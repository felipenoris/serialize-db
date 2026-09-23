"""``serialize_db.delta``: a camada de tabela sobre o delta-rs.

Os testes gravam sob ``SERIALIZE_DB_TEST_LOCAL_ROOT`` (marcador ``local``) e, com
``SERIALIZE_DB_TEST_S3_ROOT``, repetem os mesmos casos no bucket (marcador ``s3``), cada um numa raiz
nova. O modelo é o de ``Operacao``, com uma coluna de cada tipo que a camada trata à parte:
``BigInteger`` na chave, ``Date`` na origem da partição, ``Double``, ``Numeric(18, 2)``,
``DateTime``, ``to`` (palavra reservada) e a partição ``data_str``; e ``Canal``, sem partição.

Eles conferem a criação idempotente; a substituição da partição, a versão do próprio commit, a
tabela sem partição e o conflito de dois escritores; o registro de um arquivo como o ``UNLOAD``
grava, as recusas das conferências, a releitura que desfaz o commit e as estatísticas que podam; a
coluna ``Double`` com valor não finito sem mínimo e máximo; a reconciliação aditiva, a dos
comentários e a recusa da destrutiva; a reescrita num commit; a diferença de versões pelo log e a
recusa do log limpo; o arquivo de controle dos snapshots; o ``vacuum`` que preserva os snapshots; a
compactação; a exportação nos dois modos; e a pasta copiada que abre na mesma versão. A extensão
``delta`` do DuckDB precisa estar na pasta de extensões (``SERIALIZE_DB_DUCKDB_EXTENSIONS``, senão
``.duckdb/`` na raiz do repositório).
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import decimal
import json
import re
import uuid
from collections.abc import Iterator

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import pytest
import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from serialize_db import delta, schema
from serialize_db.delta import RegisteredFile
from serialize_db.errors import (
    ConflictError,
    ContractError,
    ExecutionConflict,
    LogUnavailable,
    RegistrationRefused,
    SchemaDiffRefused,
)
from serialize_db.storage import Storage


class Base(DeclarativeBase):
    pass


class Operacao(Base):
    __tablename__ = "cad_operacoes"
    __table_args__ = {
        "comment": "Operações por data-base",
        "info": {"serialize_db": {"partition_by": ["data_str"], "partition_source": "data",
                                  "sort_key": ["data", "id_operacao"]}},
    }
    id_operacao: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=False,
                                             comment="Identificador")
    data: Mapped[dt.date] = mapped_column(sa.Date)
    valor: Mapped[float | None] = mapped_column(sa.Double)
    preco: Mapped[decimal.Decimal | None] = mapped_column(sa.Numeric(18, 2))
    carimbo: Mapped[dt.datetime | None] = mapped_column(sa.DateTime)
    to: Mapped[str | None] = mapped_column(sa.String(2))
    descricao: Mapped[str | None] = mapped_column(sa.String(200))
    data_str: Mapped[str] = mapped_column(sa.String(10), comment="Partição AAAA-MM-DD")


class Canal(Base):
    __tablename__ = "dom_canais"
    id_canal: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=False)
    nome: Mapped[str] = mapped_column(sa.String(50))


OPERACOES = Operacao.__table__
CANAIS = Canal.__table__
METADATA = delta.commit_metadata("exec-2026-09-05", {"cad_contratos": 88})


@pytest.fixture(params=[pytest.param("local", marks=pytest.mark.local),
                        pytest.param("s3", marks=pytest.mark.s3)])
def storage(request: pytest.FixtureRequest) -> Iterator[Storage]:
    """Uma raiz nova por teste, sob a pasta da sessão local ou sob o prefixo da sessão no bucket."""
    location = request.getfixturevalue(f"{request.param}_location")
    yield Storage.for_uri(location.child(f"delta/{uuid.uuid4().hex[:8]}"))


@pytest.fixture
def uri(storage: Storage) -> str:
    """A pasta de ``cad_operacoes`` no ambiente ``prod``, com a tabela criada."""
    table_uri = storage.uri_of("prod/cad_operacoes")
    delta.create_table(table_uri, OPERACOES, storage)
    return table_uri


def rows(value: str, start: int, count: int, valor: list[float] | None = None) -> pa.Table:
    """``count`` operações da partição ``value`` com ids a partir de ``start``, no contrato."""
    day = dt.date.fromisoformat(value)
    ids = list(range(start, start + count))
    data = pa.table({
        "id_operacao": pa.array(ids, pa.int64()),
        "data": pa.array([day] * count, pa.date32()),
        "valor": pa.array(valor if valor is not None else [k / 4 for k in ids], pa.float64()),
        "preco": pa.array([decimal.Decimal(k) / 100 for k in ids], pa.decimal128(18, 2)),
        "carimbo": pa.array([dt.datetime(2026, 8, 31, 12, 30)] * count, pa.timestamp("us")),
        "to": pa.array(["SP"] * count),
        "descricao": pa.array([f"operação {k}" for k in ids]),
        "data_str": pa.array([value] * count),
    })
    return schema.cast(data, OPERACOES)


def log_actions(storage: Storage, uri: str, version: int) -> list[dict]:
    """As ações do commit ``version``, uma por linha do log."""
    text, _ = storage.read_text(storage.join(storage.relative(uri), "_delta_log", f"{version:020d}.json"))
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def added_stats(storage: Storage, uri: str, version: int) -> list[dict]:
    """O JSON de estatísticas de cada ação ``add`` do commit."""
    return [json.loads(action["add"]["stats"]) for action in log_actions(storage, uri, version) if "add" in action]


def write_external_file(storage: Storage, uri: str, relative: str, data: pa.Table,
                        int96: bool = True) -> RegisteredFile:
    """Um arquivo gravado pelo PyArrow dentro da pasta da tabela, como o ``UNLOAD`` grava:
    timestamp em ``INT96`` e decimal em ``FIXED_LEN_BYTE_ARRAY``; devolve o ``RegisteredFile``
    com as estatísticas da chave."""
    path = storage.join(storage.relative(uri), relative)
    storage.ensure_folder(path.rsplit("/", 1)[0])
    pq.write_table(data, f"{storage.path}/{path}", filesystem=storage.filesystem,
                   use_deprecated_int96_timestamps=int96)
    ids = data.column("id_operacao")
    stats = {"min": {"id_operacao": pc.min(ids).as_py()}, "max": {"id_operacao": pc.max(ids).as_py()},
             "null_count": {"id_operacao": 0}}
    return RegisteredFile(relative, storage.size(path), data.num_rows, stats)


def every_file(storage: Storage, prefix: str) -> list[str]:
    """Todos os arquivos sob ``prefix``, o log inclusive, relativos à raiz."""
    selector = pafs.FileSelector(f"{storage.path}/{prefix}", recursive=True)
    files = []
    for info in storage.filesystem.get_file_info(selector):
        if info.type == pafs.FileType.File:
            files.append(info.path.removeprefix(storage.path + "/"))
    return sorted(files)


def scan(storage: Storage, text: str) -> list[tuple]:
    """As linhas de uma consulta numa conexão DuckDB própria, configurada pelo armazenamento."""
    connection = storage.duckdb_connect()
    try:
        return connection.execute(text).fetchall()
    finally:
        connection.close()


# ---------------------------------------------------------------- criação e publicação


def test_create_table_is_idempotent(storage: Storage) -> None:
    """Versão 0 nas duas chamadas; o esquema do contrato, a partição, as retenções, o nome e o
    comentário da tabela em ``description`` lidos do log."""
    uri = storage.uri_of("prod/cad_operacoes")
    assert not delta.table_exists(uri, storage)
    assert delta.create_table(uri, OPERACOES, storage).version() == 0
    table = delta.create_table(uri, OPERACOES, storage)
    assert table.version() == 0 and delta.table_exists(uri, storage)
    metadata = table.metadata()
    assert (metadata.name, metadata.description, metadata.partition_columns) == (
        "cad_operacoes", "Operações por data-base", ["data_str"])
    assert metadata.configuration["delta.logRetentionDuration"] == "interval 3650 days"
    assert metadata.configuration["delta.deletedFileRetentionDuration"] == "interval 400 days"
    current = pa.schema(table.schema())
    assert current.field("id_operacao").metadata[b"comment"] == b"Identificador"
    contract = schema.arrow_schema(OPERACOES)
    assert [(f.name, f.type, f.nullable) for f in current] == [(f.name, f.type, f.nullable) for f in contract]


def test_publish_partition_replaces_only_its_partition(storage: Storage, uri: str) -> None:
    """Duas partições, a segunda republicada: a primeira intacta, uma versão por chamada, os
    metadados no ``history``; o valor fora da regra e os dados sem a coluna de partição são
    recusados antes de gravar."""
    first = delta.publish_partition(uri, OPERACOES, "2026-07-31", rows("2026-07-31", 1, 100), METADATA, storage)
    second = delta.publish_partition(uri, OPERACOES, "2026-08-31", rows("2026-08-31", 101, 100), METADATA, storage)
    again = delta.publish_partition(uri, OPERACOES, "2026-08-31", rows("2026-08-31", 201, 50), METADATA, storage)
    assert (first, second, again) == (1, 2, 3)

    table = delta.open_table(uri, storage)
    counts = pc.value_counts(table.to_pyarrow_dataset().to_table(columns=["data_str"]).column("data_str"))
    assert sorted((item["values"].as_py(), item["counts"].as_py()) for item in counts) == [
        ("2026-07-31", 100), ("2026-08-31", 50)]
    history = table.history(1)[0]
    assert history["serialize_db_execution_id"] == "exec-2026-09-05"
    assert json.loads(history["serialize_db_input_versions"]) == {"cad_contratos": 88}

    with pytest.raises(ContractError, match="regra da partição"):
        delta.publish_partition(uri, OPERACOES, "d'agua", rows("2026-08-31", 1, 1), METADATA, storage)
    with pytest.raises(ContractError, match="coluna de partição"):
        delta.publish_partition(uri, OPERACOES, "2026-08-31", rows("2026-08-31", 1, 1).drop_columns(["data_str"]), METADATA, storage)
    assert delta.open_table(uri, storage).version() == 3


class Stream:
    """Um fluxo que só expõe ``__arrow_c_stream__``, como o ``BatchStream`` de um motor."""

    def __init__(self, table: pa.Table) -> None:
        self._table = table

    def __arrow_c_stream__(self, requested_schema: object = None) -> object:
        return self._table.__arrow_c_stream__(requested_schema)


def test_publish_partition_returns_its_own_version(storage: Storage, uri: str,
                                                   monkeypatch: pytest.MonkeyPatch) -> None:
    """Com o commit de outro escritor entre a abertura e a escrita, a versão devolvida é a do
    próprio commit, lida do objeto que escreveu; os dados chegam por ``__arrow_c_stream__``."""
    delta.publish_partition(uri, OPERACOES, "2026-07-31", rows("2026-07-31", 1, 10), METADATA, storage)
    opened = delta.open_table(uri, storage)
    delta.publish_partition(uri, OPERACOES, "2026-09-30", rows("2026-09-30", 500, 10), METADATA, storage)
    monkeypatch.setattr(delta, "open_table", lambda *args, **kwargs: opened)
    version = delta.publish_partition(uri, OPERACOES, "2026-08-31", Stream(rows("2026-08-31", 11, 10)), METADATA, storage)
    monkeypatch.undo()
    assert version == 3 == delta.open_table(uri, storage).version()


def test_publish_partition_without_partition_replaces_the_table(storage: Storage) -> None:
    """``value=None`` numa tabela sem partição troca a tabela inteira; um valor nela é recusado."""
    uri = storage.uri_of("prod/dom_canais")
    delta.create_table(uri, CANAIS, storage)
    data = pa.table({"id_canal": pa.array([1, 2], pa.int64()), "nome": ["app", "web"]})
    delta.publish_partition(uri, CANAIS, None, schema.cast(data, CANAIS), METADATA, storage)
    replaced = schema.cast(pa.table({"id_canal": pa.array([3], pa.int64()), "nome": ["agência"]}), CANAIS)
    assert delta.publish_partition(uri, CANAIS, None, replaced, METADATA, storage) == 2
    current = delta.open_table(uri, storage).to_pyarrow_dataset().to_table()
    assert current.column("nome").to_pylist() == ["agência"]
    with pytest.raises(ContractError, match="sem partição"):
        delta.publish_partition(uri, CANAIS, "2026-08-31", replaced, METADATA, storage)


def test_two_writers_on_the_same_partition_conflict(storage: Storage, uri: str,
                                                   monkeypatch: pytest.MonkeyPatch) -> None:
    """Dois ``overwrite`` da mesma partição a partir da mesma versão, pelo delta-rs e pelo registro:
    o segundo é ``ExecutionConflict``; partições distintas passam."""
    delta.publish_partition(uri, OPERACOES, "2026-08-31", rows("2026-08-31", 1, 10), METADATA, storage)
    stale = delta.open_table(uri, storage)
    delta.publish_partition(uri, OPERACOES, "2026-08-31", rows("2026-08-31", 11, 10), METADATA, storage)
    file = write_external_file(storage, uri, "data_str=2026-08-31/externo.parquet",
                               rows("2026-08-31", 41, 10).drop_columns(["data_str"]))

    # A primitiva abre o objeto que outro escritor deixou para trás: o mesmo que duas execuções
    # abertas na mesma versão.
    monkeypatch.setattr(delta, "open_table", lambda *args, **kwargs: stale)
    with pytest.raises(ExecutionConflict, match="2026-08-31"):
        delta.publish_partition(uri, OPERACOES, "2026-08-31", rows("2026-08-31", 21, 10), METADATA, storage)
    with pytest.raises(ExecutionConflict, match="2026-08-31"):
        delta.register_files(uri, OPERACOES, [file], "2026-08-31", METADATA, storage)
    assert delta.publish_partition(uri, OPERACOES, "2026-09-30", rows("2026-09-30", 51, 10), METADATA, storage) == 3
    monkeypatch.undo()
    ids = delta.open_table(uri, storage).to_pyarrow_dataset().to_table(columns=["id_operacao"]).column("id_operacao")
    assert sorted(ids.to_pylist()) == list(range(11, 21)) + list(range(51, 61))


# ---------------------------------------------------------------- o registro de arquivos


def test_register_files_registers_an_unload_like_file(storage: Storage, uri: str) -> None:
    """Um arquivo em ``INT96`` e ``FIXED_LEN_BYTE_ARRAY`` entra num commit ``overwrite`` da
    partição; os dois leitores devolvem as linhas e ``timestamp[us]``; a ação leva o mínimo e o
    máximo da chave e não os do decimal nem os do timestamp."""
    delta.publish_partition(uri, OPERACOES, "2026-08-31", rows("2026-08-31", 1, 10), METADATA, storage)
    data = rows("2026-08-31", 101, 50).drop_columns(["data_str"])
    file = write_external_file(storage, uri, "data_str=2026-08-31/exec-42_ab12/0000_part_00.parquet", data)
    footer = pq.ParquetFile(f"{storage.path}/{storage.relative(uri)}/{file.path}", filesystem=storage.filesystem)
    physical = {footer.schema.column(i).name: footer.schema.column(i).physical_type for i in range(len(footer.schema))}
    assert (physical["carimbo"], physical["preco"]) == ("INT96", "FIXED_LEN_BYTE_ARRAY")

    version = delta.register_files(uri, OPERACOES, [file], "2026-08-31", METADATA, storage, expected_rows=50)
    assert version == 2
    stats = added_stats(storage, uri, 2)[0]
    assert (stats["numRecords"], stats["minValues"], stats["maxValues"]) == (50, {"id_operacao": 101}, {"id_operacao": 150})

    table = delta.open_table(uri, storage)
    read = table.to_pyarrow_dataset().to_table()
    assert read.num_rows == 50 and read.schema.field("carimbo").type == pa.timestamp("us")
    assert scan(storage, f"SELECT count(*), max(id_operacao), typeof(carimbo) FROM delta_scan('{uri}') GROUP BY ALL") == [
        (50, 150, "TIMESTAMP")]
    assert table.history(1)[0]["serialize_db_execution_id"] == "exec-2026-09-05"


def defect(storage: Storage, uri: str, name: str) -> tuple[list[RegisteredFile], str, int | None]:
    """Um registro com um defeito: os arquivos, o valor registrado e ``expected_rows``."""
    data = rows("2026-09-30", 1, 20).drop_columns(["data_str"])
    good = write_external_file(storage, uri, f"data_str=2026-09-30/{name}.parquet", data)
    cases = {
        "tamanho": ([dataclasses.replace(good, size=good.size + 1)], "2026-09-30", None),
        "linhas": ([dataclasses.replace(good, rows=19)], "2026-09-30", None),
        "partição do caminho": ([good], "2026-10-31", None),
        "esperadas": ([good], "2026-09-30", 21),
        "caminho absoluto": ([dataclasses.replace(good, path=f"/{good.path}")], "2026-09-30", None),
    }
    if name in cases:
        return cases[name]
    variants = {
        "coluna ausente": data.drop_columns(["descricao"]),
        "tipo físico": data.set_column(data.schema.get_field_index("valor"), "valor", pa.array(["x"] * 20)),
        "partição dentro": data.append_column("data_str", pa.array(["2026-09-30"] * 20)),
        "ordem": data.select(["data", "id_operacao", "valor", "preco", "carimbo", "to", "descricao"]),
    }
    return [write_external_file(storage, uri, f"data_str=2026-09-30/{name}_v.parquet", variants[name])], "2026-09-30", None


@pytest.mark.parametrize("name", ["tamanho", "linhas", "partição do caminho", "esperadas", "caminho absoluto",
                                  "coluna ausente", "tipo físico", "partição dentro", "ordem"])
def test_register_files_refuses_each_defect(storage: Storage, uri: str, name: str) -> None:
    """Cada conferência recusa com ``RegistrationRefused``: a versão não muda e o arquivo fica
    órfão na pasta."""
    files, value, expected = defect(storage, uri, name)
    with pytest.raises(RegistrationRefused):
        delta.register_files(uri, OPERACOES, files, value, METADATA, storage, expected_rows=expected)
    assert delta.open_table(uri, storage).version() == 0
    assert storage.list_files(storage.join(storage.relative(uri), "data_str=2026-09-30"), ".parquet")


def test_read_back_restores_on_a_difference(storage: Storage, uri: str) -> None:
    """Um máximo falso da chave, abaixo do real, passa pelas conferências do rodapé e faz a
    releitura voltar a versão com ``restore``: a partição fica como estava."""
    delta.publish_partition(uri, OPERACOES, "2026-08-31", rows("2026-08-31", 1, 10), METADATA, storage)
    file = write_external_file(storage, uri, "data_str=2026-08-31/falso.parquet",
                               rows("2026-08-31", 101, 10).drop_columns(["data_str"]))
    lying = dataclasses.replace(file, stats={"min": {"id_operacao": 101}, "max": {"id_operacao": 105}})
    with pytest.raises(RegistrationRefused, match="voltou à versão 1"):
        delta.register_files(uri, OPERACOES, [lying], "2026-08-31", METADATA, storage)
    table = delta.open_table(uri, storage)
    assert table.version() == 3 and table.history(1)[0]["operation"] == "RESTORE"
    ids = table.to_pyarrow_dataset().to_table(columns=["id_operacao"]).column("id_operacao")
    assert sorted(ids.to_pylist()) == list(range(1, 11))


def test_file_from_return_stats_and_registered_stats_prune(storage: Storage, uri: str) -> None:
    """O arquivo do ``COPY ... RETURN_STATS`` do DuckDB entra com o mínimo e o máximo dos tipos
    exatos, e o ``delta_scan`` pula o arquivo por eles (``Scanning Files: 0/n``); ``decimal`` e
    ``timestamp`` ficam sem mínimo e máximo."""
    delta.publish_partition(uri, OPERACOES, "2026-07-31", rows("2026-07-31", 1, 10), METADATA, storage)
    connection = storage.duckdb_connect()
    try:
        connection.register("origem", rows("2026-08-31", 101, 100).drop_columns(["data_str"]))
        storage.ensure_folder(storage.join(storage.relative(uri), "data_str=2026-08-31"))
        cursor = connection.execute(f"COPY origem TO '{uri}/data_str=2026-08-31/exec-42_ab12.parquet' (FORMAT parquet, RETURN_STATS)")
        names = [column[0] for column in cursor.description]
        row = dict(zip(names, cursor.fetchone()))
    finally:
        connection.close()
    file = delta.file_from_return_stats(row, OPERACOES, uri)
    assert (file.path, file.rows) == ("data_str=2026-08-31/exec-42_ab12.parquet", 100)
    assert file.stats["min"] == {"id_operacao": 101, "data": "2026-08-31", "valor": 25.25, "to": "SP",
                                 "descricao": "operação 101"}
    assert "preco" not in file.stats["max"] and "carimbo" not in file.stats["max"]

    delta.register_files(uri, OPERACOES, [file], "2026-08-31", METADATA, storage, expected_rows=100)
    registered = pa.table(delta.open_table(uri, storage).get_add_actions(flatten=True))
    august = registered.filter(pc.equal(registered.column("partition.data_str"), "2026-08-31"))
    assert august.column("min.preco").null_count == 1 and august.column("max.carimbo").null_count == 1
    for condition in ("id_operacao > 1000", "descricao > 'zzz'"):
        plan = scan(storage, f"EXPLAIN ANALYZE SELECT count(*) FROM delta_scan('{uri}') WHERE {condition}")[0][1]
        assert re.search(r"Scanning Files: 0/2", plan), condition


def test_nonfinite_double_columns_leave_min_max_out(storage: Storage, uri: str) -> None:
    """A coluna em ``columns_without_min_max`` sai sem mínimo e máximo no rodapé e no log de
    ``publish_partition`` e no log de ``register_files``; a outra partição sai com eles, e o
    ``delta_scan`` devolve a linha do ``NaN`` num filtro por intervalo e não abre o arquivo da
    partição sem ``NaN``."""
    with_nan = rows("2026-07-31", 1, 3, valor=[1.5, float("nan"), 2.0])
    delta.publish_partition(uri, OPERACOES, "2026-07-31", with_nan, METADATA, storage, columns_without_min_max=["valor"])
    delta.publish_partition(uri, OPERACOES, "2026-08-31", rows("2026-08-31", 11, 3, valor=[1.0, 2.0, 2.5]), METADATA, storage)
    infinite = rows("2026-09-30", 21, 3, valor=[1.0, float("inf"), 2.0]).drop_columns(["data_str"])
    file = write_external_file(storage, uri, "data_str=2026-09-30/infinito.parquet", infinite)
    file = dataclasses.replace(file, stats={"min": {"id_operacao": 21, "valor": 1.0}, "max": {"id_operacao": 23, "valor": float("inf")},
                                            "null_count": {"valor": 0}})
    delta.register_files(uri, OPERACOES, [file], "2026-09-30", METADATA, storage, columns_without_min_max=["valor"])

    july, august, september = added_stats(storage, uri, 1)[0], added_stats(storage, uri, 2)[0], added_stats(storage, uri, 3)[0]
    assert "valor" not in july["maxValues"] and "valor" not in september["maxValues"]
    assert august["maxValues"]["valor"] == 2.5 and july["maxValues"]["id_operacao"] == 3
    table_path = storage.relative(uri)
    july_file = storage.list_files(storage.join(table_path, "data_str=2026-07-31"), ".parquet")[0]
    footer = pq.ParquetFile(f"{storage.path}/{july_file}", filesystem=storage.filesystem).metadata
    valor = footer.schema.names.index("valor")
    statistics = footer.row_group(0).column(valor).statistics
    assert statistics is None or not statistics.has_min_max

    connection = storage.duckdb_connect()
    try:
        connection.execute("CALL enable_logging('FileSystem')")
        connection.execute("CALL truncate_duckdb_logs()")
        found = connection.execute(f"SELECT count(*) FROM delta_scan('{uri}') WHERE valor > 3").fetchone()[0]
        opened = set()
        for (message,) in connection.execute("SELECT message FROM duckdb_logs WHERE type = 'FileSystem'").fetchall():
            if '"op":"OPEN"' in message and ".parquet" in message:
                opened.add(re.search(r"data_str=[0-9-]+", message).group(0))
    finally:
        connection.close()
    assert found == 2  # o NaN de julho e o infinito de setembro
    assert opened == {"data_str=2026-07-31", "data_str=2026-09-30"}


def test_max_key_reads_statistics_and_scans_without_them(storage: Storage, uri: str) -> None:
    """``max_key`` lê o máximo das estatísticas e varre a coluna quando um arquivo não as tem; 0 na
    tabela vazia."""
    assert delta.max_key(delta.open_table(uri, storage), "id_operacao") == 0
    delta.publish_partition(uri, OPERACOES, "2026-07-31", rows("2026-07-31", 1, 10), METADATA, storage)
    assert delta.max_key(delta.open_table(uri, storage), "id_operacao") == 10
    file = write_external_file(storage, uri, "data_str=2026-08-31/sem_estatistica.parquet",
                               rows("2026-08-31", 500, 5).drop_columns(["data_str"]))
    delta.register_files(uri, OPERACOES, [dataclasses.replace(file, stats={})], "2026-08-31", METADATA, storage)
    actions = pa.table(delta.open_table(uri, storage).get_add_actions(flatten=True))
    assert actions.column("max.id_operacao").null_count == 1
    assert delta.max_key(delta.open_table(uri, storage), "id_operacao") == 504


# ---------------------------------------------------------------- a evolução do esquema


def evolved(**changes: object) -> sa.Table:
    """Uma cópia do modelo de ``cad_operacoes`` com colunas trocadas, acrescentadas ou removidas."""
    metadata = sa.MetaData()
    columns = []
    for column in OPERACOES.columns:
        if column.name in changes.get("drop", ()):
            continue
        copy = column._copy()
        if column.name in changes.get("nullable", ()):
            copy.nullable = True
        if column.name in changes.get("not_null", ()):
            copy.nullable = False
        if column.name in changes.get("comments", {}):
            copy.comment = changes["comments"][column.name]
        if column.name in changes.get("types", {}):
            copy.type = changes["types"][column.name]
        columns.append(copy)
    columns.extend(changes.get("add", ()))
    return sa.Table("cad_operacoes", metadata, *columns, comment=changes.get("comment", OPERACOES.comment),
                    info=OPERACOES.info)


def test_reconcile_adds_nullable_column_and_relaxes_not_null(storage: Storage, uri: str) -> None:
    """A coluna anulável entra no fim e as linhas antigas a leem nula; ``NOT NULL`` relaxado; a
    versão anterior lê o esquema antigo; a segunda chamada não commita."""
    delta.publish_partition(uri, OPERACOES, "2026-08-31", rows("2026-08-31", 1, 10), METADATA, storage)
    model = evolved(nullable=["data"], add=[sa.Column("canal", sa.String(20), comment="Canal")])
    diff = delta.reconcile(uri, model, storage)
    assert [field.name for field in diff.add] == ["canal"] and diff.relax == ("data",)
    table = delta.open_table(uri, storage)
    current = pa.schema(table.schema())
    assert current.names[-1] == "canal" and current.field("canal").metadata[b"comment"] == b"Canal"
    assert current.field("data").nullable
    assert table.to_pyarrow_dataset().to_table(columns=["canal"]).column("canal").null_count == 10
    assert "canal" not in pa.schema(delta.open_table(uri, storage, version=1).schema()).names

    version = table.version()
    assert not delta.reconcile(uri, model, storage).changes
    assert delta.open_table(uri, storage).version() == version


def test_reconcile_syncs_description_and_comments(storage: Storage, uri: str) -> None:
    """Um comentário de tabela e um de coluna alterados no modelo entram por commits só de
    ``metaData``; a segunda chamada não commita."""
    model = evolved(comment="Operações de crédito", comments={"id_operacao": "Identificador da operação"})
    diff = delta.reconcile(uri, model, storage)
    assert (diff.description, diff.comments) == ("Operações de crédito", ("id_operacao",))
    table = delta.open_table(uri, storage)
    assert table.metadata().description == "Operações de crédito"
    assert pa.schema(table.schema()).field("id_operacao").metadata[b"comment"] == "Identificador da operação".encode()
    kinds = {next(iter(action)) for action in log_actions(storage, uri, table.version())}
    assert kinds == {"commitInfo", "metaData"}
    version = table.version()
    assert not delta.reconcile(uri, model, storage).changes
    assert delta.open_table(uri, storage).version() == version


@pytest.mark.parametrize("change", ["tipo", "removida", "not null nova", "not null em anulável"])
def test_reconcile_refuses_destructive_diff(storage: Storage, uri: str, change: str) -> None:
    """Tipo trocado, coluna removida, coluna ``NOT NULL`` nova e ``NOT NULL`` numa coluna anulável,
    numa tabela com dados, são ``SchemaDiffRefused`` sem commit."""
    delta.publish_partition(uri, OPERACOES, "2026-08-31", rows("2026-08-31", 1, 10), METADATA, storage)
    models = {
        "tipo": evolved(types={"valor": sa.Numeric(18, 2)}),
        "removida": evolved(drop=["descricao"]),
        "not null nova": evolved(add=[sa.Column("canal", sa.String(20), nullable=False)]),
        "not null em anulável": evolved(not_null=["descricao"]),
    }
    with pytest.raises(SchemaDiffRefused, match="rewrite"):
        delta.reconcile(uri, models[change], storage)
    assert delta.open_table(uri, storage).version() == 1


def test_rewrite_in_one_commit_keeps_previous_version_readable(storage: Storage, uri: str) -> None:
    """Um commit com o esquema novo e as somas iguais, a versão anterior legível; a coluna
    renomeada por ``expressions`` lê os valores da antiga; o ``Double`` com ``NaN`` fica sem mínimo e
    máximo na partição dele; a coluna do contrato ausente e fora de ``expressions`` falha sem
    commit."""
    delta.publish_partition(uri, OPERACOES, "2026-07-31", rows("2026-07-31", 1, 10, valor=[float("nan")] + [1.0] * 9), METADATA, storage)
    delta.publish_partition(uri, OPERACOES, "2026-08-31", rows("2026-08-31", 11, 10), METADATA, storage)

    renamed = evolved(drop=["descricao"], add=[sa.Column("historico", sa.String(200))])
    with pytest.raises(duckdb.BinderException):
        delta.rewrite(uri, renamed, storage)
    assert delta.open_table(uri, storage).version() == 2

    version = delta.rewrite(uri, renamed, storage, expressions={"historico": '"descricao"'})
    assert version == 3
    table = delta.open_table(uri, storage)
    kinds = [next(iter(action)) for action in log_actions(storage, uri, 3)]
    assert kinds.count("metaData") == 1 and kinds.count("remove") == 2 and kinds.count("add") == 2
    current = table.to_pyarrow_dataset().to_table()
    assert "descricao" not in current.column_names and sorted(current.column("historico").to_pylist())[0] == "operação 1"
    assert current.num_rows == 20
    previous = delta.open_table(uri, storage, version=2).to_pyarrow_dataset().to_table()
    assert "descricao" in previous.column_names and previous.num_rows == 20
    july = [stats for stats in added_stats(storage, uri, 3) if stats["minValues"]["id_operacao"] == 1][0]
    august = [stats for stats in added_stats(storage, uri, 3) if stats["minValues"]["id_operacao"] == 11][0]
    assert "valor" not in july["maxValues"] and august["maxValues"]["valor"] == 5.0


# ---------------------------------------------------------------- o log, os snapshots e a operação


def test_version_diff_counts_data_changes_only(storage: Storage, uri: str) -> None:
    """Substituição e remoção contam, compactação e reconciliação não, ``published == current``
    dá vazio; na tabela sem partição, ``{None}``."""
    delta.publish_partition(uri, OPERACOES, "2026-07-31", rows("2026-07-31", 1, 10), METADATA, storage)  # 1
    delta.publish_partition(uri, OPERACOES, "2026-08-31", rows("2026-08-31", 11, 10), METADATA, storage)  # 2
    from deltalake import write_deltalake

    write_deltalake(delta.open_table(uri, storage), rows("2026-08-31", 21, 5), mode="append")  # 3
    delta.compact(uri, OPERACOES, ["2026-08-31"], storage)  # 4
    delta.reconcile(uri, evolved(comment="Outro comentário"), storage)  # 5
    delta.open_table(uri, storage).delete("\"data_str\" = '2026-07-31'")  # 6
    assert delta.open_table(uri, storage).version() == 6
    assert delta.version_diff(uri, 0, 2, OPERACOES, storage) == {"2026-07-31", "2026-08-31"}
    assert delta.version_diff(uri, 3, 5, OPERACOES, storage) == set()
    assert delta.version_diff(uri, 5, 6, OPERACOES, storage) == {"2026-07-31"}
    assert delta.version_diff(uri, 6, 6, OPERACOES, storage) == set()

    canais = storage.uri_of("prod/dom_canais")
    delta.create_table(canais, CANAIS, storage)
    data = schema.cast(pa.table({"id_canal": pa.array([1], pa.int64()), "nome": ["app"]}), CANAIS)
    delta.publish_partition(canais, CANAIS, None, data, METADATA, storage)
    assert delta.version_diff(canais, 0, 1, CANAIS, storage) == {None}


def test_version_diff_refuses_a_cleaned_log(storage: Storage, uri: str) -> None:
    """Um arquivo do log apagado entre as duas versões dá ``LogUnavailable``, com a publicação
    completa na mensagem."""
    for value in ("2026-07-31", "2026-08-31"):
        delta.publish_partition(uri, OPERACOES, value, rows(value, 1, 5), METADATA, storage)
    storage.delete([storage.join(storage.relative(uri), "_delta_log", f"{1:020d}.json")])
    with pytest.raises(LogUnavailable, match="publique a tabela inteira"):
        delta.version_diff(uri, 0, 2, OPERACOES, storage)
    assert delta.version_diff(uri, 1, 2, OPERACOES, storage) == {"2026-08-31"}


def test_snapshot_control_file_is_written_conditionally(storage: Storage) -> None:
    """O primeiro snapshot cria o arquivo, o segundo o atualiza; o nome repetido é erro, e a escrita
    concorrente é ``ConflictError``."""
    assert delta.read_snapshots(storage, "prod") == ({"snapshots": {}}, None)
    delta.snapshot(storage, "prod", "2026T2", {"cad_operacoes": 3, "dom_canais": 1})
    control = delta.snapshot(storage, "prod", "2026T3", {"cad_operacoes": 5})
    assert control == {"snapshots": {"2026T2": {"cad_operacoes": 3, "dom_canais": 1}, "2026T3": {"cad_operacoes": 5}}}
    assert delta.read_snapshots(storage, "prod")[0] == control
    with pytest.raises(ValueError, match="já existe"):
        delta.snapshot(storage, "prod", "2026T3", {"cad_operacoes": 6})

    # Outro escritor grava entre a leitura e a escrita: a impressão digital lida ficou velha.
    path = storage.join("prod", delta.CONTROL_FILE)
    _, fingerprint = storage.read_text(path)
    storage.write_text(path, json.dumps({"snapshots": {}}), if_match=fingerprint)
    with pytest.raises(ConflictError):
        storage.write_text(path, json.dumps(control), if_match=fingerprint)


def test_vacuum_keeps_snapshot_versions(storage: Storage, uri: str) -> None:
    """Com retenção zero, ``keep_versions`` das versões do snapshot preserva a versão marcada, e a
    intermediária perde o arquivo; dentro da retenção nada é listado."""
    for start in (1, 11, 21, 31):
        delta.publish_partition(uri, OPERACOES, "2026-08-31", rows("2026-08-31", start, 10), METADATA, storage)
    control = delta.snapshot(storage, "prod", "2026T3", {"cad_operacoes": 2})
    assert delta.vacuum_keeping_snapshots(uri, control, "cad_operacoes", storage) == []
    listed = delta.vacuum_keeping_snapshots(uri, control, "cad_operacoes", storage, retention_hours=0)
    assert len(listed) == 2  # as versões 1 e 3; a 2 é do snapshot, a 4 é a atual
    removed = delta.vacuum_keeping_snapshots(uri, control, "cad_operacoes", storage, retention_hours=0, apply=True)
    assert sorted(removed) == sorted(listed)
    assert delta.open_table(uri, storage, version=2).to_pyarrow_dataset().count_rows() == 10
    with pytest.raises(Exception):  # noqa: B017 - o erro vem do leitor de arquivos, e a classe varia
        delta.open_table(uri, storage, version=3).to_pyarrow_dataset().to_table()


def test_compact_before_snapshot(storage: Storage, uri: str) -> None:
    """Os arquivos pequenos de uma partição viram um; a partição com um arquivo só não commita."""
    from deltalake import write_deltalake

    delta.publish_partition(uri, OPERACOES, "2026-07-31", rows("2026-07-31", 1, 10), METADATA, storage)
    for start in (100, 200, 300):
        write_deltalake(delta.open_table(uri, storage), rows("2026-08-31", start, 5), mode="append")
    metrics = delta.compact(uri, OPERACOES, ["2026-08-31"], storage)
    assert (metrics["numFilesAdded"], metrics["numFilesRemoved"]) == (1, 3)
    version = delta.open_table(uri, storage).version()
    delta.compact(uri, OPERACOES, ["2026-07-31"], storage)
    assert delta.open_table(uri, storage).version() == version
    assert delta.open_table(uri, storage).to_pyarrow_dataset().count_rows() == 25


def test_export_snapshot_copy_and_rewrite(storage: Storage, uri: str) -> None:
    """Os dois modos produzem ``<coluna>=<valor>/`` com as mesmas linhas; ``copy`` copia só os
    arquivos que o log lista, e uma versão antiga exporta o que ela tinha."""
    delta.publish_partition(uri, OPERACOES, "2026-07-31", rows("2026-07-31", 1, 10), METADATA, storage)
    delta.publish_partition(uri, OPERACOES, "2026-08-31", rows("2026-08-31", 11, 10), METADATA, storage)
    delta.publish_partition(uri, OPERACOES, "2026-08-31", rows("2026-08-31", 21, 5), METADATA, storage)

    copied = delta.export_snapshot(uri, OPERACOES, storage.uri_of("prod/exportacao/copia"), storage)
    rewritten = delta.export_snapshot(uri, OPERACOES, storage.uri_of("prod/exportacao/reescrita"), storage, mode="rewrite")
    old = delta.export_snapshot(uri, OPERACOES, storage.uri_of("prod/exportacao/antiga"), storage, version=2)
    assert len(copied) == 2 and len(rewritten) == 2 and len(old) == 2
    assert all(re.search(r"/data_str=2026-0[78]-31/", path) for path in copied + rewritten)

    measures = "SELECT data_str, count(*), sum(id_operacao) FROM read_parquet('{}/*/*.parquet', hive_partitioning = true, hive_types_autocast = false) GROUP BY 1 ORDER BY 1"
    expected = [("2026-07-31", 10, 55), ("2026-08-31", 5, 115)]
    assert scan(storage, measures.format(storage.uri_of("prod/exportacao/copia"))) == expected
    assert scan(storage, measures.format(storage.uri_of("prod/exportacao/reescrita"))) == expected
    assert scan(storage, measures.format(storage.uri_of("prod/exportacao/antiga")))[1] == ("2026-08-31", 10, 155)


def test_copy_manifest_lists_the_files_of_a_version(storage: Storage, uri: str) -> None:
    """O manifesto do ``COPY`` leva a URL e o tamanho de cada arquivo da versão nas partições
    pedidas, com ``mandatory`` verdadeiro."""
    delta.publish_partition(uri, OPERACOES, "2026-07-31", rows("2026-07-31", 1, 10), METADATA, storage)
    delta.publish_partition(uri, OPERACOES, "2026-08-31", rows("2026-08-31", 11, 10), METADATA, storage)
    destination = storage.uri_of("prod/publicacao/exec-42/cad_operacoes/2026-08-31.manifest")
    assert delta.copy_manifest(uri, 2, ["2026-08-31"], destination, storage) == destination
    manifest = json.loads(storage.read_text(storage.relative(destination))[0])
    [entry] = manifest["entries"]
    assert entry["url"].startswith(f"{uri}/data_str=2026-08-31/") and entry["mandatory"] is True
    assert entry["meta"]["content_length"] == storage.size(storage.relative(entry["url"]))
    everything = delta.copy_manifest(uri, 2, None, storage.uri_of("prod/publicacao/tudo.manifest"), storage)
    assert len(json.loads(storage.read_text(storage.relative(everything))[0])["entries"]) == 2


def test_deep_copy_and_relocation(storage: Storage, uri: str) -> None:
    """A cópia profunda nasce na versão 0 com as linhas, o esquema e a partição da versão; a pasta
    copiada arquivo a arquivo abre na mesma versão nos dois leitores, porque o log guarda caminhos
    relativos."""
    delta.publish_partition(uri, OPERACOES, "2026-07-31", rows("2026-07-31", 1, 10), METADATA, storage)
    delta.publish_partition(uri, OPERACOES, "2026-08-31", rows("2026-08-31", 11, 10), METADATA, storage)
    archive = storage.uri_of("prod/arquivo/2026T3/cad_operacoes")
    assert delta.deep_copy(uri, 1, archive, storage) == 0
    copy = delta.open_table(archive, storage)
    assert copy.to_pyarrow_dataset().count_rows() == 10 and copy.metadata().partition_columns == ["data_str"]
    assert not pa.schema(copy.schema()).field("id_operacao").nullable

    source = storage.relative(uri)
    moved = "outro_lugar/cad_operacoes"
    for path in every_file(storage, source):
        storage.copy(path, storage.join(moved, path.removeprefix(source + "/")))
    relocated = storage.uri_of(moved)
    assert delta.open_table(relocated, storage).version() == 2
    assert scan(storage, f"SELECT count(*) FROM delta_scan('{relocated}')") == [(20,)]
