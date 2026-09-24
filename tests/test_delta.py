"""``serialize_db.delta``: a camada de tabela sobre o delta-rs.

Os testes gravam sob ``SERIALIZE_DB_TEST_LOCAL_ROOT`` (marcador ``local``) e, com
``SERIALIZE_DB_TEST_S3_ROOT``, repetem os mesmos casos no bucket (marcador ``s3``), cada um numa
raiz nova. O modelo é o de ``Operacao``, com uma coluna de cada tipo que a camada trata à parte:
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

import collections
import dataclasses
import datetime as dt
import decimal
import json
import re
import uuid
from collections.abc import Collection, Mapping, Sequence

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import pytest
import sqlalchemy as sa
from deltalake import write_deltalake
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from conftest import opened_partition_folders
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
def storage(request: pytest.FixtureRequest) -> Storage:
    """Uma raiz nova por teste, sob a pasta da sessão local ou sob o prefixo da sessão no bucket."""
    location = request.getfixturevalue(f"{request.param}_location")
    return Storage.for_uri(location.child(f"delta/{uuid.uuid4().hex[:8]}"))


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


def publish(storage: Storage, uri: str, value: str, start: int, count: int) -> int:
    """Publica ``rows(value, start, count)`` na partição ``value`` e devolve a versão do commit."""
    data = rows(value, start, count)
    return delta.publish_partition(uri, OPERACOES, value, data, METADATA, storage)


def current_values(storage: Storage, uri: str, column: str) -> list:
    """Os valores de ``column`` na versão atual da tabela."""
    dataset = delta.open_table(uri, storage).to_pyarrow_dataset()
    return dataset.to_table(columns=[column]).column(column).to_pylist()


def log_actions(storage: Storage, uri: str, version: int) -> list[dict]:
    """As ações do commit ``version``, uma por linha do log."""
    path = storage.join(storage.relative(uri), "_delta_log", f"{version:020d}.json")
    text, _ = storage.read_text(path)
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def added_stats(storage: Storage, uri: str, version: int) -> list[dict]:
    """O JSON de estatísticas de cada ação ``add`` do commit."""
    stats = []
    for action in log_actions(storage, uri, version):
        if "add" in action:
            stats.append(json.loads(action["add"]["stats"]))
    return stats


def action_kinds(storage: Storage, uri: str, version: int) -> list[str]:
    """O tipo de cada ação do commit, a única chave da linha do log: ``add``, ``remove``,
    ``metaData``, ``commitInfo``."""
    kinds = []
    for action in log_actions(storage, uri, version):
        kinds.append(list(action)[0])
    return kinds


def write_external_file(storage: Storage, uri: str, relative: str,
                        data: pa.Table) -> RegisteredFile:
    """Um arquivo gravado pelo PyArrow dentro da pasta da tabela, como o ``UNLOAD`` grava:
    timestamp em ``INT96`` e decimal em ``FIXED_LEN_BYTE_ARRAY``; devolve o ``RegisteredFile``
    com as estatísticas da chave."""
    path = storage.join(storage.relative(uri), relative)
    storage.ensure_folder(path.rsplit("/", 1)[0])
    pq.write_table(data, f"{storage.path}/{path}", filesystem=storage.filesystem,
                   use_deprecated_int96_timestamps=True)
    ids = data.column("id_operacao")
    smallest = pc.min(ids).as_py()
    largest = pc.max(ids).as_py()
    stats = {"min": {"id_operacao": smallest}, "max": {"id_operacao": largest},
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
    with storage.duckdb_connect() as connection:
        return connection.execute(text).fetchall()


# ---------------------------------------------------------------- criação e publicação


def test_create_table_is_idempotent(storage: Storage) -> None:
    """Versão 0 nas duas chamadas; o esquema do contrato, a partição, as retenções, o nome e o
    comentário da tabela em ``description`` lidos do log."""
    uri = storage.uri_of("prod/cad_operacoes")
    assert not delta.table_exists(uri, storage)
    assert delta.create_table(uri, OPERACOES, storage).version() == 0
    table = delta.create_table(uri, OPERACOES, storage)
    assert table.version() == 0
    assert delta.table_exists(uri, storage)

    metadata = table.metadata()
    assert metadata.name == "cad_operacoes"
    assert metadata.description == "Operações por data-base"
    assert metadata.partition_columns == ["data_str"]
    assert metadata.configuration["delta.logRetentionDuration"] == "interval 3650 days"
    assert metadata.configuration["delta.deletedFileRetentionDuration"] == "interval 400 days"

    current = pa.schema(table.schema())
    assert current.field("id_operacao").metadata[b"comment"] == b"Identificador"
    contract = schema.arrow_schema(OPERACOES)
    current_columns = [(field.name, field.type, field.nullable) for field in current]
    contract_columns = [(field.name, field.type, field.nullable) for field in contract]
    assert current_columns == contract_columns


def test_publish_partition_replaces_only_its_partition(storage: Storage, uri: str) -> None:
    """Duas partições, a segunda republicada: a primeira intacta, uma versão por chamada, os
    metadados no ``history``; o valor fora da regra e os dados sem a coluna de partição são
    recusados antes de gravar."""
    first = publish(storage, uri, "2026-07-31", 1, 100)
    second = publish(storage, uri, "2026-08-31", 101, 100)
    again = publish(storage, uri, "2026-08-31", 201, 50)
    assert (first, second, again) == (1, 2, 3)

    partitions = current_values(storage, uri, "data_str")
    assert collections.Counter(partitions) == {"2026-07-31": 100, "2026-08-31": 50}
    history = delta.open_table(uri, storage).history(1)[0]
    assert history["serialize_db_execution_id"] == "exec-2026-09-05"
    assert json.loads(history["serialize_db_input_versions"]) == {"cad_contratos": 88}

    # As recusas antes de gravar: a versão fica em 3.
    one_row = rows("2026-08-31", 1, 1)
    with pytest.raises(ContractError, match="regra da partição"):
        delta.publish_partition(uri, OPERACOES, "d'agua", one_row, METADATA, storage)
    without_partition = one_row.drop_columns(["data_str"])
    with pytest.raises(ContractError, match="coluna de partição"):
        delta.publish_partition(uri, OPERACOES, "2026-08-31", without_partition, METADATA,
                                storage)
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
    publish(storage, uri, "2026-07-31", 1, 10)
    opened = delta.open_table(uri, storage)
    publish(storage, uri, "2026-09-30", 500, 10)

    # A primitiva recebe o objeto aberto antes do commit do outro escritor.
    stream = Stream(rows("2026-08-31", 11, 10))
    with monkeypatch.context() as patch:
        patch.setattr(delta, "open_table", lambda *args, **kwargs: opened)
        version = delta.publish_partition(uri, OPERACOES, "2026-08-31", stream, METADATA, storage)
    assert version == 3
    assert delta.open_table(uri, storage).version() == 3


def test_publish_partition_without_partition_replaces_the_table(storage: Storage) -> None:
    """``value=None`` numa tabela sem partição troca a tabela inteira; um valor nela é recusado."""
    uri = storage.uri_of("prod/dom_canais")
    delta.create_table(uri, CANAIS, storage)
    data = pa.table({"id_canal": pa.array([1, 2], pa.int64()), "nome": ["app", "web"]})
    delta.publish_partition(uri, CANAIS, None, schema.cast(data, CANAIS), METADATA, storage)
    replacement = pa.table({"id_canal": pa.array([3], pa.int64()), "nome": ["agência"]})
    replaced = schema.cast(replacement, CANAIS)
    assert delta.publish_partition(uri, CANAIS, None, replaced, METADATA, storage) == 2
    current = delta.open_table(uri, storage).to_pyarrow_dataset().to_table()
    assert current.column("nome").to_pylist() == ["agência"]
    with pytest.raises(ContractError, match="sem partição"):
        delta.publish_partition(uri, CANAIS, "2026-08-31", replaced, METADATA, storage)


def test_two_writers_on_the_same_partition_conflict(storage: Storage, uri: str,
                                                   monkeypatch: pytest.MonkeyPatch) -> None:
    """Dois ``overwrite`` da mesma partição a partir da mesma versão, pelo delta-rs e pelo registro:
    o segundo é ``ExecutionConflict``; partições distintas passam."""
    publish(storage, uri, "2026-08-31", 1, 10)
    stale = delta.open_table(uri, storage)
    publish(storage, uri, "2026-08-31", 11, 10)
    external = rows("2026-08-31", 41, 10).drop_columns(["data_str"])
    file = write_external_file(storage, uri, "data_str=2026-08-31/externo.parquet", external)

    # A primitiva abre o objeto que outro escritor deixou para trás: o mesmo que duas execuções
    # abertas na mesma versão.
    with monkeypatch.context() as patch:
        patch.setattr(delta, "open_table", lambda *args, **kwargs: stale)
        with pytest.raises(ExecutionConflict, match="2026-08-31"):
            publish(storage, uri, "2026-08-31", 21, 10)
        with pytest.raises(ExecutionConflict, match="2026-08-31"):
            delta.register_files(uri, OPERACOES, [file], "2026-08-31", METADATA, storage)
        assert publish(storage, uri, "2026-09-30", 51, 10) == 3
    ids = current_values(storage, uri, "id_operacao")
    assert sorted(ids) == list(range(11, 21)) + list(range(51, 61))


# ---------------------------------------------------------------- o registro de arquivos


def test_register_files_registers_an_unload_like_file(storage: Storage, uri: str) -> None:
    """Um arquivo em ``INT96`` e ``FIXED_LEN_BYTE_ARRAY`` entra num commit ``overwrite`` da
    partição; os dois leitores devolvem as linhas e ``timestamp[us]``; a ação leva o mínimo e o
    máximo da chave e não os do decimal nem os do timestamp."""
    publish(storage, uri, "2026-08-31", 1, 10)
    data = rows("2026-08-31", 101, 50).drop_columns(["data_str"])
    relative = "data_str=2026-08-31/exec-42_ab12/0000_part_00.parquet"
    file = write_external_file(storage, uri, relative, data)
    footer_path = storage.join(storage.relative(uri), file.path)
    footer = pq.ParquetFile(storage.open_input_file(footer_path))
    physical = {}
    for index in range(len(footer.schema)):
        column = footer.schema.column(index)
        physical[column.name] = column.physical_type
    assert (physical["carimbo"], physical["preco"]) == ("INT96", "FIXED_LEN_BYTE_ARRAY")

    version = delta.register_files(uri, OPERACOES, [file], "2026-08-31", METADATA, storage,
                                   expected_rows=50)
    assert version == 2
    stats = added_stats(storage, uri, 2)[0]
    assert stats["numRecords"] == 50
    assert stats["minValues"] == {"id_operacao": 101}
    assert stats["maxValues"] == {"id_operacao": 150}

    table = delta.open_table(uri, storage)
    read = table.to_pyarrow_dataset().to_table()
    assert read.num_rows == 50
    assert read.schema.field("carimbo").type == pa.timestamp("us")
    query = (f"SELECT count(*), max(id_operacao), typeof(carimbo) FROM delta_scan('{uri}') "
             "GROUP BY ALL")
    assert scan(storage, query) == [(50, 150, "TIMESTAMP")]
    assert table.history(1)[0]["serialize_db_execution_id"] == "exec-2026-09-05"


# Um registro: os arquivos, o valor registrado e ``expected_rows``.
Registration = tuple[list[RegisteredFile], str, int | None]

# Os defeitos da descrição do arquivo, registrados sobre um arquivo bom.
REGISTRATION_DEFECTS = ["tamanho", "linhas", "partição do caminho", "esperadas", "caminho absoluto"]

# Os defeitos do próprio arquivo, que só o rodapé mostra.
FILE_DEFECTS = ["coluna ausente", "tipo físico", "partição dentro", "ordem", "nulo em not null"]


def defective_registration(good: RegisteredFile, name: str) -> Registration:
    """O registro do arquivo bom com o defeito ``name`` na descrição."""
    if name == "tamanho":
        return [dataclasses.replace(good, size=good.size + 1)], "2026-09-30", None
    if name == "linhas":
        return [dataclasses.replace(good, rows=19)], "2026-09-30", None
    if name == "partição do caminho":
        return [good], "2026-10-31", None
    if name == "esperadas":
        return [good], "2026-09-30", 21
    # O defeito restante: caminho absoluto.
    return [dataclasses.replace(good, path=f"/{good.path}")], "2026-09-30", None


def defective_data(data: pa.Table, name: str) -> pa.Table:
    """Os dados do arquivo com o defeito ``name``."""
    if name == "coluna ausente":
        return data.drop_columns(["descricao"])
    if name == "tipo físico":
        index = data.schema.get_field_index("valor")
        return data.set_column(index, "valor", pa.array(["x"] * 20))
    if name == "partição dentro":
        return data.append_column("data_str", pa.array(["2026-09-30"] * 20))
    if name == "ordem":
        reordered = ["data", "id_operacao", "valor", "preco", "carimbo", "to", "descricao"]
        return data.select(reordered)
    # O defeito restante: nulo em not null.
    index = data.schema.get_field_index("data")
    return data.set_column(index, "data", pa.array([None] * 20, pa.date32()))


def defect(storage: Storage, uri: str, name: str) -> Registration:
    """Um registro com o defeito ``name``: o defeito da descrição registra um arquivo bom, e o do
    arquivo grava os dados com o defeito; um arquivo gravado por caso."""
    data = rows("2026-09-30", 1, 20).drop_columns(["data_str"])
    if name in REGISTRATION_DEFECTS:
        good = write_external_file(storage, uri, f"data_str=2026-09-30/{name}.parquet", data)
        return defective_registration(good, name)
    wrong = defective_data(data, name)
    file = write_external_file(storage, uri, f"data_str=2026-09-30/{name}_v.parquet", wrong)
    return [file], "2026-09-30", None


@pytest.mark.parametrize("name", REGISTRATION_DEFECTS + FILE_DEFECTS)
def test_register_files_refuses_each_defect(storage: Storage, uri: str, name: str) -> None:
    """Cada conferência recusa com ``RegistrationRefused``: a versão não muda e o arquivo fica
    órfão na pasta."""
    files, value, expected = defect(storage, uri, name)
    folder = storage.join(storage.relative(uri), "data_str=2026-09-30")
    written = storage.list_files(folder, ".parquet")
    with pytest.raises(RegistrationRefused):
        delta.register_files(uri, OPERACOES, files, value, METADATA, storage,
                             expected_rows=expected)
    assert delta.open_table(uri, storage).version() == 0
    assert len(written) == 1
    assert storage.list_files(folder, ".parquet") == written


def test_read_back_restores_on_a_difference(storage: Storage, uri: str) -> None:
    """Um máximo falso da chave, abaixo do real, passa pelas conferências do rodapé e faz a
    releitura voltar a versão com ``restore``: a partição fica como estava."""
    publish(storage, uri, "2026-08-31", 1, 10)
    data = rows("2026-08-31", 101, 10).drop_columns(["data_str"])
    file = write_external_file(storage, uri, "data_str=2026-08-31/falso.parquet", data)
    false_stats = {"min": {"id_operacao": 101}, "max": {"id_operacao": 105}}
    lying = dataclasses.replace(file, stats=false_stats)
    with pytest.raises(RegistrationRefused, match="voltou à versão 1"):
        delta.register_files(uri, OPERACOES, [lying], "2026-08-31", METADATA, storage)
    table = delta.open_table(uri, storage)
    assert table.version() == 3
    assert table.history(1)[0]["operation"] == "RESTORE"
    ids = current_values(storage, uri, "id_operacao")
    assert sorted(ids) == list(range(1, 11))


def test_file_from_return_stats_and_registered_stats_prune(storage: Storage, uri: str) -> None:
    """O arquivo do ``COPY ... RETURN_STATS`` do DuckDB entra com o mínimo e o máximo dos tipos
    exatos, e o ``delta_scan`` pula o arquivo por eles (``Scanning Files: 0/n``); ``decimal`` e
    ``timestamp`` ficam sem mínimo e máximo."""
    publish(storage, uri, "2026-07-31", 1, 10)
    target = f"{uri}/data_str=2026-08-31/exec-42_ab12.parquet"
    with storage.duckdb_connect() as connection:
        connection.register("origem", rows("2026-08-31", 101, 100).drop_columns(["data_str"]))
        storage.ensure_folder(storage.join(storage.relative(uri), "data_str=2026-08-31"))
        cursor = connection.execute(f"COPY origem TO '{target}' (FORMAT parquet, RETURN_STATS)")
        names = [column[0] for column in cursor.description]
        row = dict(zip(names, cursor.fetchone()))
    file = delta.file_from_return_stats(row, OPERACOES, uri)
    assert (file.path, file.rows) == ("data_str=2026-08-31/exec-42_ab12.parquet", 100)
    assert file.stats["min"] == {"id_operacao": 101, "data": "2026-08-31", "valor": 25.25,
                                 "to": "SP", "descricao": "operação 101"}
    assert "preco" not in file.stats["max"]
    assert "carimbo" not in file.stats["max"]

    delta.register_files(uri, OPERACOES, [file], "2026-08-31", METADATA, storage,
                         expected_rows=100)
    registered = pa.table(delta.open_table(uri, storage).get_add_actions(flatten=True))
    august = registered.filter(pc.equal(registered.column("partition.data_str"), "2026-08-31"))
    assert august.column("min.preco").null_count == 1
    assert august.column("max.carimbo").null_count == 1
    for condition in ("id_operacao > 1000", "descricao > 'zzz'"):
        query = f"EXPLAIN ANALYZE SELECT count(*) FROM delta_scan('{uri}') WHERE {condition}"
        # O EXPLAIN ANALYZE devolve uma linha, com o plano na segunda coluna.
        plan = scan(storage, query)[0][1]
        assert "Scanning Files: 0/2" in plan, condition


def test_nonfinite_double_columns_leave_min_max_out(storage: Storage, uri: str) -> None:
    """A coluna em ``columns_without_min_max`` sai sem mínimo e máximo no rodapé e no log de
    ``publish_partition`` e no log de ``register_files``; a outra partição sai com eles, e o
    ``delta_scan`` devolve a linha do ``NaN`` num filtro por intervalo e não abre o arquivo da
    partição sem ``NaN``."""
    # Julho com NaN e setembro com infinito saem sem o mínimo e o máximo de valor; agosto, com eles.
    with_nan = rows("2026-07-31", 1, 3, valor=[1.5, float("nan"), 2.0])
    delta.publish_partition(uri, OPERACOES, "2026-07-31", with_nan, METADATA, storage,
                            columns_without_min_max=["valor"])
    finite = rows("2026-08-31", 11, 3, valor=[1.0, 2.0, 2.5])
    delta.publish_partition(uri, OPERACOES, "2026-08-31", finite, METADATA, storage)
    infinite = rows("2026-09-30", 21, 3, valor=[1.0, float("inf"), 2.0])
    external = infinite.drop_columns(["data_str"])
    file = write_external_file(storage, uri, "data_str=2026-09-30/infinito.parquet", external)
    stats = {"min": {"id_operacao": 21, "valor": 1.0},
             "max": {"id_operacao": 23, "valor": float("inf")},
             "null_count": {"valor": 0}}
    file = dataclasses.replace(file, stats=stats)
    delta.register_files(uri, OPERACOES, [file], "2026-09-30", METADATA, storage,
                         columns_without_min_max=["valor"])

    # O log dos três commits.
    july = added_stats(storage, uri, 1)[0]
    august = added_stats(storage, uri, 2)[0]
    september = added_stats(storage, uri, 3)[0]
    assert "valor" not in july["maxValues"]
    assert "valor" not in september["maxValues"]
    assert august["maxValues"]["valor"] == 2.5
    assert july["maxValues"]["id_operacao"] == 3

    # O rodapé do arquivo de julho.
    july_folder = storage.join(storage.relative(uri), "data_str=2026-07-31")
    july_file = storage.list_files(july_folder, ".parquet")[0]
    footer = pq.ParquetFile(storage.open_input_file(july_file)).metadata
    valor = footer.schema.names.index("valor")
    statistics = footer.row_group(0).column(valor).statistics
    assert statistics is None or not statistics.has_min_max

    # A poda do delta_scan, lida pelos arquivos que o DuckDB abre.
    with storage.duckdb_connect() as connection:
        connection.execute("CALL enable_logging('FileSystem')")
        connection.execute("CALL truncate_duckdb_logs()")
        query = f"SELECT count(*) FROM delta_scan('{uri}') WHERE valor > 3"
        found = connection.execute(query).fetchone()[0]
        opened = opened_partition_folders(connection, "data_str")
    assert found == 2  # o NaN de julho e o infinito de setembro
    assert opened == {"data_str=2026-07-31", "data_str=2026-09-30"}


def test_max_key_reads_statistics_and_scans_without_them(storage: Storage, uri: str) -> None:
    """``max_key`` lê o máximo das estatísticas e varre a coluna quando um arquivo não as tem; 0 na
    tabela vazia."""
    assert delta.max_key(delta.open_table(uri, storage), "id_operacao") == 0
    publish(storage, uri, "2026-07-31", 1, 10)
    assert delta.max_key(delta.open_table(uri, storage), "id_operacao") == 10
    data = rows("2026-08-31", 500, 5).drop_columns(["data_str"])
    file = write_external_file(storage, uri, "data_str=2026-08-31/sem_estatistica.parquet", data)
    without_stats = dataclasses.replace(file, stats={})
    delta.register_files(uri, OPERACOES, [without_stats], "2026-08-31", METADATA, storage)
    actions = pa.table(delta.open_table(uri, storage).get_add_actions(flatten=True))
    assert actions.column("max.id_operacao").null_count == 1
    assert delta.max_key(delta.open_table(uri, storage), "id_operacao") == 504


# ---------------------------------------------------------------- a evolução do esquema


def evolved(*, drop: Collection[str] = (), nullable: Collection[str] = (),
            not_null: Collection[str] = (), comments: Mapping[str, str] | None = None,
            types: Mapping[str, sa.types.TypeEngine] | None = None,
            add: Sequence[sa.Column] = (), comment: str | None = OPERACOES.comment) -> sa.Table:
    """Uma cópia do modelo de ``cad_operacoes`` com colunas trocadas, acrescentadas ou removidas."""
    comments = comments or {}
    types = types or {}
    columns = []
    for column in OPERACOES.columns:
        if column.name in drop:
            continue
        copy = column._copy()
        if column.name in nullable:
            copy.nullable = True
        if column.name in not_null:
            copy.nullable = False
        if column.name in comments:
            copy.comment = comments[column.name]
        if column.name in types:
            copy.type = types[column.name]
        columns.append(copy)
    columns.extend(add)
    return sa.Table("cad_operacoes", sa.MetaData(), *columns, comment=comment,
                    info=OPERACOES.info)


def test_reconcile_adds_nullable_column_and_relaxes_not_null(storage: Storage, uri: str) -> None:
    """A coluna anulável entra no fim e as linhas antigas a leem nula; ``NOT NULL`` relaxado; a
    versão anterior lê o esquema antigo; a segunda chamada não commita."""
    publish(storage, uri, "2026-08-31", 1, 10)
    model = evolved(nullable=["data"], add=[sa.Column("canal", sa.String(20), comment="Canal")])
    diff = delta.reconcile(uri, model, storage)
    assert [field.name for field in diff.add] == ["canal"]
    assert diff.relax == ("data",)
    table = delta.open_table(uri, storage)
    current = pa.schema(table.schema())
    assert current.names[-1] == "canal"
    assert current.field("canal").metadata[b"comment"] == b"Canal"
    assert current.field("data").nullable
    assert table.to_pyarrow_dataset().to_table(columns=["canal"]).column("canal").null_count == 10
    previous = pa.schema(delta.open_table(uri, storage, version=1).schema())
    assert "canal" not in previous.names

    version = table.version()
    assert not delta.reconcile(uri, model, storage).changes
    assert delta.open_table(uri, storage).version() == version


def test_reconcile_syncs_description_and_comments(storage: Storage, uri: str) -> None:
    """Um comentário de tabela e um de coluna alterados no modelo entram por commits só de
    ``metaData``; a segunda chamada não commita."""
    new_comments = {"id_operacao": "Identificador da operação"}
    model = evolved(comment="Operações de crédito", comments=new_comments)
    diff = delta.reconcile(uri, model, storage)
    assert (diff.description, diff.comments) == ("Operações de crédito", ("id_operacao",))
    table = delta.open_table(uri, storage)
    assert table.metadata().description == "Operações de crédito"
    comment = pa.schema(table.schema()).field("id_operacao").metadata[b"comment"]
    assert comment == "Identificador da operação".encode()
    assert set(action_kinds(storage, uri, table.version())) == {"commitInfo", "metaData"}
    version = table.version()
    assert not delta.reconcile(uri, model, storage).changes
    assert delta.open_table(uri, storage).version() == version


@pytest.mark.parametrize("change", ["tipo", "removida", "not null nova", "not null em anulável"])
def test_reconcile_refuses_destructive_diff(storage: Storage, uri: str, change: str) -> None:
    """Tipo trocado, coluna removida, coluna ``NOT NULL`` nova e ``NOT NULL`` numa coluna anulável,
    numa tabela com dados, são ``SchemaDiffRefused`` sem commit."""
    publish(storage, uri, "2026-08-31", 1, 10)
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
    """Um commit com o esquema novo e as somas iguais, a versão anterior legível; a coluna renomeada
    por ``expressions`` lê os valores da antiga; o ``Double`` com ``NaN`` fica sem mínimo e máximo
    na partição dele; a coluna do contrato ausente e fora de ``expressions`` falha sem commit."""
    with_nan = rows("2026-07-31", 1, 10, valor=[float("nan")] + [1.0] * 9)
    delta.publish_partition(uri, OPERACOES, "2026-07-31", with_nan, METADATA, storage)
    publish(storage, uri, "2026-08-31", 11, 10)

    # A coluna do contrato ausente e fora de expressions falha sem commit.
    renamed = evolved(drop=["descricao"], add=[sa.Column("historico", sa.String(200))])
    with pytest.raises(duckdb.BinderException):
        delta.rewrite(uri, renamed, storage)
    assert delta.open_table(uri, storage).version() == 2

    # A reescrita com expressions: um commit com o metaData, as remoções e as adições.
    version = delta.rewrite(uri, renamed, storage, expressions={"historico": '"descricao"'})
    assert version == 3
    table = delta.open_table(uri, storage)
    kinds = action_kinds(storage, uri, 3)
    assert kinds.count("metaData") == 1
    assert kinds.count("remove") == 2
    assert kinds.count("add") == 2
    current = table.to_pyarrow_dataset().to_table()
    assert "descricao" not in current.column_names
    assert min(current.column("historico").to_pylist()) == "operação 1"
    assert current.num_rows == 20

    # A versão anterior continua legível, com o esquema antigo.
    previous = delta.open_table(uri, storage, version=2).to_pyarrow_dataset().to_table()
    assert "descricao" in previous.column_names
    assert previous.num_rows == 20

    # As estatísticas dos arquivos novos, pelo primeiro id de cada partição.
    stats_by_first_id = {}
    for stats in added_stats(storage, uri, 3):
        stats_by_first_id[stats["minValues"]["id_operacao"]] = stats
    july = stats_by_first_id[1]
    august = stats_by_first_id[11]
    assert "valor" not in july["maxValues"]
    assert august["maxValues"]["valor"] == 5.0


# ---------------------------------------------------------------- o log, os snapshots e a operação


def test_version_diff_counts_data_changes_only(storage: Storage, uri: str) -> None:
    """Substituição e remoção contam, compactação e reconciliação não, ``published == current``
    dá vazio; na tabela sem partição, ``{None}``."""
    publish(storage, uri, "2026-07-31", 1, 10)  # 1
    publish(storage, uri, "2026-08-31", 11, 10)  # 2
    appended = rows("2026-08-31", 21, 5)
    write_deltalake(delta.open_table(uri, storage), appended, mode="append")  # 3
    delta.compact(uri, OPERACOES, ["2026-08-31"], storage)  # 4
    delta.reconcile(uri, evolved(comment="Outro comentário"), storage)  # 5
    delta.open_table(uri, storage).delete("\"data_str\" = '2026-07-31'")  # 6
    assert delta.open_table(uri, storage).version() == 6
    assert delta.version_diff(uri, 0, 2, OPERACOES, storage) == {"2026-07-31", "2026-08-31"}
    assert delta.version_diff(uri, 3, 5, OPERACOES, storage) == set()
    assert delta.version_diff(uri, 5, 6, OPERACOES, storage) == {"2026-07-31"}
    assert delta.version_diff(uri, 6, 6, OPERACOES, storage) == set()

    # A tabela sem partição.
    canais = storage.uri_of("prod/dom_canais")
    delta.create_table(canais, CANAIS, storage)
    data = schema.cast(pa.table({"id_canal": pa.array([1], pa.int64()), "nome": ["app"]}), CANAIS)
    delta.publish_partition(canais, CANAIS, None, data, METADATA, storage)
    assert delta.version_diff(canais, 0, 1, CANAIS, storage) == {None}


def test_version_diff_refuses_a_cleaned_log(storage: Storage, uri: str) -> None:
    """Um arquivo do log apagado entre as duas versões dá ``LogUnavailable``, com a publicação
    completa na mensagem."""
    for value in ("2026-07-31", "2026-08-31"):
        publish(storage, uri, value, 1, 5)
    storage.delete([storage.join(storage.relative(uri), "_delta_log", f"{1:020d}.json")])
    with pytest.raises(LogUnavailable, match="publique a tabela inteira"):
        delta.version_diff(uri, 0, 2, OPERACOES, storage)
    assert delta.version_diff(uri, 1, 2, OPERACOES, storage) == {"2026-08-31"}


def test_snapshot_control_file_is_written_conditionally(storage: Storage,
                                                        monkeypatch: pytest.MonkeyPatch) -> None:
    """O primeiro snapshot cria o arquivo, o segundo o atualiza; o nome repetido é erro, e a escrita
    concorrente é ``ConflictError``."""
    assert delta.read_snapshots(storage, "prod") == ({"snapshots": {}}, None)
    delta.snapshot(storage, "prod", "2026T2", {"cad_operacoes": 3, "dom_canais": 1})
    control = delta.snapshot(storage, "prod", "2026T3", {"cad_operacoes": 5})
    expected = {"snapshots": {"2026T2": {"cad_operacoes": 3, "dom_canais": 1},
                              "2026T3": {"cad_operacoes": 5}}}
    assert control == expected
    assert delta.read_snapshots(storage, "prod")[0] == control
    with pytest.raises(ValueError, match="já existe"):
        delta.snapshot(storage, "prod", "2026T3", {"cad_operacoes": 6})

    # Outro escritor grava entre a leitura e a escrita: a impressão digital lida ficou velha, e o
    # snapshot perdedor não grava nada.
    stale = delta.read_snapshots(storage, "prod")
    delta.snapshot(storage, "prod", "2026T4", {"cad_operacoes": 7})
    with monkeypatch.context() as patch:
        patch.setattr(delta, "read_snapshots", lambda *args, **kwargs: stale)
        with pytest.raises(ConflictError):
            delta.snapshot(storage, "prod", "2026T5", {"cad_operacoes": 8})
    current, _ = delta.read_snapshots(storage, "prod")
    assert list(current["snapshots"]) == ["2026T2", "2026T3", "2026T4"]


def test_vacuum_keeps_snapshot_versions(storage: Storage, uri: str) -> None:
    """Com retenção zero, ``keep_versions`` das versões do snapshot preserva a versão marcada, e a
    intermediária perde o arquivo; dentro da retenção nada é listado."""
    for start in (1, 11, 21, 31):
        publish(storage, uri, "2026-08-31", start, 10)
    control = delta.snapshot(storage, "prod", "2026T3", {"cad_operacoes": 2})
    assert delta.vacuum_keeping_snapshots(uri, control, "cad_operacoes", storage) == []
    listed = delta.vacuum_keeping_snapshots(uri, control, "cad_operacoes", storage,
                                            retention_hours=0)
    assert len(listed) == 2  # as versões 1 e 3; a 2 é do snapshot, a 4 é a atual
    removed = delta.vacuum_keeping_snapshots(uri, control, "cad_operacoes", storage,
                                             retention_hours=0, apply=True)
    assert sorted(removed) == sorted(listed)
    assert delta.open_table(uri, storage, version=2).to_pyarrow_dataset().count_rows() == 10
    # O erro vem do leitor de arquivos, e a classe varia.
    with pytest.raises(Exception):  # noqa: B017
        delta.open_table(uri, storage, version=3).to_pyarrow_dataset().to_table()


def test_compact_before_snapshot(storage: Storage, uri: str) -> None:
    """Os arquivos pequenos de uma partição viram um; a partição com um arquivo só não commita."""
    publish(storage, uri, "2026-07-31", 1, 10)
    for start in (100, 200, 300):
        write_deltalake(delta.open_table(uri, storage), rows("2026-08-31", start, 5), mode="append")
    metrics = delta.compact(uri, OPERACOES, ["2026-08-31"], storage)
    assert (metrics["numFilesAdded"], metrics["numFilesRemoved"]) == (1, 3)
    version = delta.open_table(uri, storage).version()
    delta.compact(uri, OPERACOES, ["2026-07-31"], storage)
    assert delta.open_table(uri, storage).version() == version
    assert delta.open_table(uri, storage).to_pyarrow_dataset().count_rows() == 25


def exported_measures(storage: Storage, folder: str) -> list[tuple]:
    """Linhas e soma de ``id_operacao`` por partição dos arquivos exportados em ``folder``."""
    files = (f"read_parquet('{storage.uri_of(folder)}/*/*.parquet', "
             "hive_partitioning = true, hive_types_autocast = false)")
    query = f"SELECT data_str, count(*), sum(id_operacao) FROM {files} GROUP BY 1 ORDER BY 1"
    return scan(storage, query)


def test_export_snapshot_copy_and_rewrite(storage: Storage, uri: str) -> None:
    """Os dois modos produzem ``<coluna>=<valor>/`` com as mesmas linhas; ``copy`` copia só os
    arquivos que o log lista, e uma versão antiga exporta o que ela tinha."""
    publish(storage, uri, "2026-07-31", 1, 10)
    publish(storage, uri, "2026-08-31", 11, 10)
    publish(storage, uri, "2026-08-31", 21, 5)

    copy_folder = "prod/exportacao/copia"
    rewrite_folder = "prod/exportacao/reescrita"
    old_folder = "prod/exportacao/antiga"
    copied = delta.export_snapshot(uri, OPERACOES, storage.uri_of(copy_folder), storage)
    rewritten = delta.export_snapshot(uri, OPERACOES, storage.uri_of(rewrite_folder), storage,
                                      mode="rewrite")
    old = delta.export_snapshot(uri, OPERACOES, storage.uri_of(old_folder), storage, version=2)
    assert len(copied) == 2
    assert len(rewritten) == 2
    assert len(old) == 2
    for path in copied + rewritten:
        assert re.search(r"/data_str=2026-0[78]-31/", path), path

    expected = [("2026-07-31", 10, 55), ("2026-08-31", 5, 115)]
    assert exported_measures(storage, copy_folder) == expected
    assert exported_measures(storage, rewrite_folder) == expected
    assert exported_measures(storage, old_folder)[1] == ("2026-08-31", 10, 155)


def test_copy_manifest_lists_the_files_of_a_version(storage: Storage, uri: str) -> None:
    """O manifesto do ``COPY`` leva a URL e o tamanho de cada arquivo da versão nas partições
    pedidas, com ``mandatory`` verdadeiro."""
    publish(storage, uri, "2026-07-31", 1, 10)
    publish(storage, uri, "2026-08-31", 11, 10)
    destination = storage.uri_of("prod/publicacao/exec-42/cad_operacoes/2026-08-31.manifest")
    assert delta.copy_manifest(uri, 2, ["2026-08-31"], destination, storage) == destination
    manifest = json.loads(storage.read_text(storage.relative(destination))[0])
    [entry] = manifest["entries"]
    assert entry["url"].startswith(f"{uri}/data_str=2026-08-31/")
    assert entry["mandatory"] is True
    assert entry["meta"]["content_length"] == storage.size(storage.relative(entry["url"]))
    all_partitions = storage.uri_of("prod/publicacao/tudo.manifest")
    everything = delta.copy_manifest(uri, 2, None, all_partitions, storage)
    text, _ = storage.read_text(storage.relative(everything))
    assert len(json.loads(text)["entries"]) == 2


def test_deep_copy_and_relocation(storage: Storage, uri: str) -> None:
    """A cópia profunda copia os arquivos da versão e os registra, um commit por partição, com o
    esquema, a partição, o nome e as estatísticas da origem; a repetição não commita, a cópia de
    uma versão sobre a de uma anterior copia só a partição que falta, e o destino que registra um
    arquivo fora da versão é recusado; a pasta copiada arquivo a arquivo abre na mesma versão nos
    dois leitores, porque o log guarda caminhos relativos."""
    publish(storage, uri, "2026-07-31", 1, 10)
    publish(storage, uri, "2026-08-31", 11, 10)

    # A cópia profunda da versão 1: o mesmo arquivo, no mesmo caminho relativo, com as mesmas
    # estatísticas, e a tabela com um commit por partição.
    archive = storage.uri_of("prod/arquivo/2026T3/cad_operacoes")
    assert delta.deep_copy(uri, 1, archive, storage) == 1
    copy = delta.open_table(archive, storage)
    assert copy.to_pyarrow_dataset().count_rows() == 10
    assert copy.metadata().partition_columns == ["data_str"]
    assert copy.metadata().name == "cad_operacoes"
    assert not pa.schema(copy.schema()).field("id_operacao").nullable
    source_actions = pa.table(delta.open_table(uri, storage, 1).get_add_actions(flatten=True))
    copied_actions = pa.table(copy.get_add_actions(flatten=True))
    for column in ("path", "size_bytes", "num_records", "min.id_operacao", "max.id_operacao"):
        assert copied_actions.column(column).equals(source_actions.column(column)), column
    assert scan(storage, f"SELECT count(*), sum(id_operacao) FROM delta_scan('{archive}')") == \
        [(10, 55)]

    # A repetição sobre a cópia completa não commita; a cópia da versão 2 sobre a da versão 1
    # continua de onde a primeira parou, com um commit só da partição que falta; a versão 1 sobre a
    # cópia da 2 é recusada, porque o destino registra um arquivo que ela não lista.
    assert delta.deep_copy(uri, 1, archive, storage) == 1
    assert delta.deep_copy(uri, 2, archive, storage) == 2
    added = [action["add"]["path"] for action in log_actions(storage, archive, 2)
             if "add" in action]
    source_actions = pa.table(delta.open_table(uri, storage, 2).get_add_actions(flatten=True))
    august = [path for path in source_actions.column("path").to_pylist()
              if path.startswith("data_str=2026-08-31/")]
    assert added == august and len(august) == 1
    continued = delta.open_table(archive, storage)
    assert sorted(pa.table(continued.get_add_actions(flatten=True)).column("path").to_pylist()) \
        == sorted(source_actions.column("path").to_pylist())
    assert scan(storage, f"SELECT count(*) FROM delta_scan('{archive}')") == [(20,)]
    with pytest.raises(RegistrationRefused, match="fora da versão 1"):
        delta.deep_copy(uri, 1, archive, storage)

    # A pasta copiada arquivo a arquivo.
    source = storage.relative(uri)
    moved = "outro_lugar/cad_operacoes"
    for path in every_file(storage, source):
        storage.copy(path, storage.join(moved, path.removeprefix(source + "/")))
    relocated = storage.uri_of(moved)
    assert delta.open_table(relocated, storage).version() == 2
    assert scan(storage, f"SELECT count(*) FROM delta_scan('{relocated}')") == [(20,)]
