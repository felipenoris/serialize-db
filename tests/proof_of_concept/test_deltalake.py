"""A API do delta-rs (pacote ``deltalake``) que a biblioteca usa, numa pasta local.

Cada teste exercita uma parte: a criação idempotente a partir de um esquema, os modos de escrita e a
substituição por predicado, a evolução de esquema e o ``update`` com predicado, a viagem no tempo e
o ``restore``, as ações do log e o registro de um arquivo gravado por outro programa, o ``vacuum``
com ``keep_versions``, a leitura por dataset Arrow e o conteúdo do log. Os testes seguintes cobrem
as primitivas das etapas 3, 4, 7 e 9 (``plan/PLAN-STAGE-<n>.md``) sobre o DuckDB: a view presa a
uma versão e o leitor Arrow que alimenta o ``write_deltalake``, a reescrita da tabela pelo ``COPY``
particionado registrada num commit com as estatísticas que a etapa 3 registra, a diferença de
versões pelas ações ``add`` e ``remove`` com ``dataChange`` do log, a compactação e o checkpoint, a
exportação por cópia dos arquivos e a carga inicial de pastas Parquet; e ainda
``is_deltatable`` e ``drop_column_not_null``, o que ``create_write_transaction`` não confere
(caminho, estatística, esquema do arquivo), o ``overwrite`` com ``partition_filters`` e a
compactação que normaliza arquivos de outro escritor, a descrição e os comentários que atravessam o
``overwrite`` e mudam por ``alter``, o mínimo e o máximo que o próprio delta-rs grava por tipo, o
``NaN`` e o infinito nas estatísticas registradas e nas do delta-rs, o ``Double`` sem estatística
no rodapé e no log, também por partição, dois registros concorrentes da mesma partição, a poda do
``delta_scan`` por
forma de predicado, e o valor de partição codificado na pasta e no log, com a aspa que quebra o
predicado. Os
comportamentos estão descritos em ``plan/delta.md``; aqui eles viram asserções.
"""

from __future__ import annotations

import datetime as dt
import decimal
import json
import re
import shutil
import time
from collections.abc import Callable
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pytest
from deltalake import ColumnProperties, CommitProperties, DeltaTable, QueryBuilder, Schema, WriterProperties, write_deltalake
from deltalake.exceptions import CommitFailedError, DeltaError, SchemaMismatchError
from deltalake.schema import Field, PrimitiveType
from deltalake.transaction import AddAction

from conftest import LocalLocation
from poc_delta import MONTHS, ROWS, connect_duckdb, sample_table

pytestmark = pytest.mark.local


@pytest.fixture
def folder(local_location: LocalLocation) -> Callable[[str], str]:
    """Uma pasta nova por teste sob a raiz da sessão."""
    return lambda name: local_location.child(f"deltalake/{name}")


@pytest.fixture
def two_months() -> pa.Table:
    """Mil linhas, 500 de cada mês."""
    return sample_table().slice(ROWS // 2 - 500, 1000)


def log_actions(uri: str, version: int) -> list[dict]:
    """As ações do commit ``version``, uma por linha de ``_delta_log/<versão>.json``."""
    lines = Path(uri, "_delta_log", f"{version:020d}.json").read_text(encoding="utf-8").splitlines()
    return [json.loads(line) for line in lines]


def test_create_from_schema_is_idempotent(folder: Callable[[str], str]) -> None:
    """``DeltaTable.create(mode="ignore")`` grava a versão 0 com esquema, partição, comentários e propriedades, uma vez só."""
    uri = folder("create")
    schema = pa.schema(
        [
            pa.field("id_operacao", pa.int64(), nullable=False, metadata={"comment": "Identificador"}),
            pa.field("mes", pa.string(), nullable=False),
            pa.field("valor", pa.decimal128(18, 2), nullable=False),
            pa.field("descricao", pa.string()),
        ]
    )
    configuration = {"delta.logRetentionDuration": "interval 3650 days", "delta.deletedFileRetentionDuration": "interval 400 days"}

    table = DeltaTable.create(uri, schema, mode="ignore", partition_by=["mes"], name="cad_operacoes", description="Operações", configuration=configuration)
    again = DeltaTable.create(uri, schema, mode="ignore", partition_by=["mes"], name="cad_operacoes", description="Operações", configuration=configuration)
    assert table.version() == 0 and again.version() == 0

    # Os metadados da tabela: nome, descrição, partição e propriedades ficam na ação metaData do log.
    metadata = again.metadata()
    assert metadata.name == "cad_operacoes" and metadata.partition_columns == ["mes"]
    assert metadata.configuration["delta.logRetentionDuration"] == "interval 3650 days"

    # O esquema Delta preserva nulidade e comentários; a interface PyCapsule o devolve como esquema Arrow.
    fields = {field.name: field for field in again.schema().fields}
    assert not fields["id_operacao"].nullable and fields["id_operacao"].metadata == {"comment": "Identificador"}
    assert pa.schema(again.schema()).field("valor").type == pa.decimal128(18, 2)

    assert again.history()[0]["operation"] == "CREATE TABLE"


def test_write_modes_and_predicate(folder: Callable[[str], str], two_months: pa.Table) -> None:
    """``append`` acrescenta, ``overwrite`` com ``predicate`` substitui só o mês, e o escritor valida predicado e nulidade."""
    uri = folder("write")

    # Versão 0: os dois meses, um arquivo por partição.
    write_deltalake(uri, two_months, mode="append", partition_by=["mes"])

    # Versão 1: fevereiro substituído; janeiro fica com o arquivo original.
    february = two_months.filter(pc.field("mes") == MONTHS[1])
    write_deltalake(uri, february, mode="overwrite", predicate=f"mes = '{MONTHS[1]}'")

    table = DeltaTable(uri)
    assert table.version() == 1
    assert table.to_pyarrow_table().num_rows == 1000
    assert len(table.file_uris()) == 2
    assert MONTHS[1] in table.history()[0]["operationParameters"]["predicate"]

    # Linhas fora do predicado são recusadas: o overwrite de fevereiro não aceita janeiro.
    with pytest.raises(DeltaError, match="failed validation"):
        write_deltalake(uri, two_months, mode="overwrite", predicate=f"mes = '{MONTHS[1]}'")

    # Nulo numa coluna declarada não anulável também é recusado pelo escritor.
    strict = folder("write_strict")
    DeltaTable.create(strict, pa.schema([pa.field("id", pa.int64(), nullable=False)]))
    with pytest.raises(DeltaError, match="failed validation"):
        write_deltalake(strict, pa.table({"id": pa.array([1, None], pa.int64())}), mode="append")


def test_schema_evolution_and_update(folder: Callable[[str], str], two_months: pa.Table) -> None:
    """Coluna nova entra por ``schema_mode="merge"`` ou ``alter.add_columns``; o tipo é convertido no ``append``; ``update`` preenche por predicado."""
    uri = folder("evolution")
    write_deltalake(uri, two_months, mode="append", partition_by=["mes"])

    # Uma coluna a mais sem schema_mode é erro; com merge, ela entra no fim do esquema.
    with_channel = two_months.append_column("canal", pa.array(["app"] * two_months.num_rows))
    with pytest.raises(SchemaMismatchError):
        write_deltalake(uri, with_channel, mode="append")
    write_deltalake(uri, with_channel, mode="append", schema_mode="merge")

    table = DeltaTable(uri)
    names = [field.name for field in table.schema().fields]
    assert names[-1] == "canal"
    assert table.to_pyarrow_table().column("canal").null_count == two_months.num_rows  # as linhas antigas leem nulo

    # alter.add_columns acrescenta uma coluna anulável só nos metadados; add_constraint registra um CHECK.
    table.alter.add_columns([Field("origem", PrimitiveType("string"), nullable=True)])
    table.alter.add_constraint({"valor_nao_negativo": "valor >= 0"})
    constraint = table.metadata().configuration["delta.constraints.valor_nao_negativo"]
    assert constraint.startswith("valor >=")  # a expressão é normalizada: valor >= '0'::decimal(18, 2)

    # O append converte para o tipo da tabela em vez de acusar: int32 numa coluna long entra; decimal mais largo não cabe.
    # A tabela já tem colunas a mais que a amostra, por isso o append precisa de schema_mode="merge".
    narrow = two_months.set_column(two_months.schema.get_field_index("id_operacao"), "id_operacao", two_months.column("id_operacao").cast(pa.int32()))
    write_deltalake(uri, narrow, mode="append", schema_mode="merge")
    assert pa.schema(DeltaTable(uri).schema()).field("id_operacao").type == pa.int64()

    wide = two_months.set_column(two_months.schema.get_field_index("valor"), "valor", two_months.column("valor").cast(pa.decimal128(20, 4)))
    with pytest.raises(SchemaMismatchError):
        write_deltalake(uri, wide, mode="append", schema_mode="merge")

    # update com predicado dá valor às linhas antigas na coluna nova.
    table = DeltaTable(uri)
    table.update(updates={"origem": "'legado'"}, predicate="origem IS NULL")
    assert table.to_pyarrow_table().filter(pc.field("origem") == "legado").num_rows == 3 * two_months.num_rows


def test_time_travel_and_restore(folder: Callable[[str], str], two_months: pa.Table) -> None:
    """Cada versão lê com os arquivos e o esquema dela; os metadados de commit ficam no histórico; ``restore`` é um commit novo."""
    uri = folder("history")
    february = two_months.filter(pc.field("mes") == MONTHS[1])

    def properties(execution_id: str) -> CommitProperties:
        return CommitProperties(custom_metadata={"serialize_db_execution_id": execution_id})

    write_deltalake(uri, two_months, mode="append", partition_by=["mes"], commit_properties=properties("exec-0"))
    write_deltalake(uri, february, mode="overwrite", predicate=f"mes = '{MONTHS[1]}'", commit_properties=properties("exec-1"))
    write_deltalake(uri, february.slice(0, 100), mode="overwrite", predicate=f"mes = '{MONTHS[1]}'", commit_properties=properties("exec-2"))

    # A versão atual tem 600 linhas; a versão 1 continua lendo as 1.000.
    assert DeltaTable(uri).to_pyarrow_table().num_rows == 600
    assert DeltaTable(uri, version=1).to_pyarrow_table().num_rows == 1000

    # O histórico vem do mais novo para o mais velho, com as chaves de custom_metadata no nível de cima.
    history = DeltaTable(uri).history()
    assert [entry["serialize_db_execution_id"] for entry in history] == ["exec-2", "exec-1", "exec-0"]

    # load_as_version move o mesmo objeto para outra versão.
    table = DeltaTable(uri)
    table.load_as_version(1)
    assert table.version() == 1

    # restore torna a versão 1 o estado atual num commit novo (versão 3), sem apagar a versão 2.
    table = DeltaTable(uri)
    table.restore(1)
    restored = DeltaTable(uri)
    assert restored.version() == 3 and restored.history()[0]["operation"] == "RESTORE"
    assert restored.to_pyarrow_table().num_rows == 1000
    assert DeltaTable(uri, version=2).to_pyarrow_table().num_rows == 600


def test_add_actions_and_register_external_file(folder: Callable[[str], str], two_months: pa.Table) -> None:
    """As ações ``add`` do log dão o caminho relativo, o tamanho e a partição; um Parquet de outro escritor entra por ``AddAction``."""
    uri = folder("register")
    write_deltalake(uri, two_months, mode="append", partition_by=["mes"])
    table = DeltaTable(uri)

    # get_add_actions(flatten=True) é um RecordBatch com uma linha por arquivo do snapshot.
    actions = table.get_add_actions(flatten=True)
    paths = actions.column("path").to_pylist()
    assert sorted(actions.column("partition.mes").to_pylist()) == list(MONTHS)
    assert all(path.startswith("mes=") and not path.startswith("/") for path in paths)
    assert all(size > 0 for size in actions.column("size_bytes").to_pylist())

    # Um arquivo gravado pelo PyArrow na pasta da partição, sem a coluna mes, como o delta-rs faz.
    month = "2026-04"
    partition = Path(uri) / f"mes={month}"
    partition.mkdir()
    file = partition / "externo.parquet"
    pq.write_table(two_months.slice(0, 10).drop_columns(["mes"]), file)

    action = AddAction(
        path=f"mes={month}/externo.parquet",
        size=file.stat().st_size,
        partition_values={"mes": month},
        modification_time=int(time.time() * 1000),
        data_change=True,
        stats=json.dumps({"numRecords": 10, "minValues": {}, "maxValues": {}, "nullCount": {}}),
    )
    table.create_write_transaction([action], mode="append", schema=table.schema(), partition_by=["mes"])

    # O delta-rs e o DuckDB leem o arquivo registrado com o valor de partição da ação.
    registered = DeltaTable(uri)
    assert registered.version() == 1
    assert registered.to_pyarrow_table(filters=[("mes", "=", month)]).num_rows == 10

    con = connect_duckdb(("delta",))
    assert con.execute(f"SELECT count(*) FROM delta_scan('{uri}') WHERE mes = '{month}'").fetchone()[0] == 10
    con.close()


def test_vacuum_with_keep_versions(folder: Callable[[str], str], two_months: pa.Table) -> None:
    """``vacuum`` recusa retenção abaixo da configurada; ``keep_versions`` preserva os arquivos de um snapshot."""
    uri = folder("vacuum")
    february = two_months.filter(pc.field("mes") == MONTHS[1])

    write_deltalake(uri, two_months, mode="append", partition_by=["mes"])  # versão 0
    for _ in range(3):  # versões 1, 2 e 3 substituem fevereiro
        write_deltalake(uri, february, mode="overwrite", predicate=f"mes = '{MONTHS[1]}'")

    table = DeltaTable(uri)
    with pytest.raises(DeltaError, match="retention"):
        table.vacuum(retention_hours=0, dry_run=True)

    # Sem a proteção: três arquivos de fevereiro fora do snapshot atual. Com keep_versions=[1]: o da versão 1 fica.
    candidates = table.vacuum(retention_hours=0, dry_run=True, enforce_retention_duration=False)
    kept = table.vacuum(retention_hours=0, dry_run=True, enforce_retention_duration=False, keep_versions=[1])
    assert len(candidates) == 3 and len(kept) == 2

    removed = table.vacuum(retention_hours=0, dry_run=False, enforce_retention_duration=False, keep_versions=[1])
    assert len(removed) == 2

    # A versão 1 continua legível; a versão 2 perdeu o arquivo.
    assert DeltaTable(uri, version=1).to_pyarrow_table().num_rows == 1000
    with pytest.raises(Exception):  # noqa: B017 - o erro vem do leitor de arquivos, e a classe varia
        DeltaTable(uri, version=2).to_pyarrow_table()


def test_dataset_reader_and_deep_copy(folder: Callable[[str], str], two_months: pa.Table) -> None:
    """A tabela é lida como dataset Arrow com poda por partição, e o leitor em lotes alimenta uma cópia profunda."""
    uri = folder("dataset")
    write_deltalake(uri, two_months, mode="append", partition_by=["mes"])
    table = DeltaTable(uri)

    dataset = table.to_pyarrow_dataset()
    assert dataset.to_table(filter=ds.field("mes") == MONTHS[1]).num_rows == 500
    assert table.to_pyarrow_table(filters=[("mes", "=", MONTHS[0])]).num_rows == 500

    # O leitor em lotes é a entrada de write_deltalake: a cópia nasce na versão 0 com as mesmas linhas.
    copy_uri = folder("dataset_copy")
    reader = dataset.scanner().to_reader()
    write_deltalake(copy_uri, reader, mode="overwrite", partition_by=["mes"])

    copy = DeltaTable(copy_uri)
    assert copy.version() == 0 and copy.to_pyarrow_table().num_rows == 1000


def test_log_files(folder: Callable[[str], str], two_months: pa.Table) -> None:
    """O log é um JSON por commit com uma ação por linha: ``commitInfo``, ``protocol``, ``metaData`` e ``add``."""
    uri = folder("log")
    write_deltalake(uri, two_months, mode="append", partition_by=["mes"])

    # Cada linha é um objeto com uma chave só, o tipo da ação.
    actions = log_actions(uri, 0)
    assert {next(iter(action)) for action in actions} == {"commitInfo", "protocol", "metaData", "add"}

    # Cada add traz o caminho relativo, o valor de partição e as estatísticas em JSON.
    adds = [action["add"] for action in actions if "add" in action]
    assert sorted(add["partitionValues"]["mes"] for add in adds) == list(MONTHS)
    assert all(json.loads(add["stats"])["numRecords"] == 500 for add in adds)
    assert all(not add["path"].startswith("/") for add in adds)


def test_is_deltatable_and_drop_column_not_null(folder: Callable[[str], str], two_months: pa.Table) -> None:
    """``is_deltatable`` distingue pasta vazia de tabela; ``drop_column_not_null`` relaxa a nulidade só nos metadados."""
    uri = folder("nullability")
    assert not DeltaTable.is_deltatable(uri)

    strict = pa.schema([pa.field(field.name, field.type, nullable=field.name == "descricao") for field in two_months.schema])
    DeltaTable.create(uri, strict, partition_by=["mes"])
    write_deltalake(uri, two_months, mode="append")
    assert DeltaTable.is_deltatable(uri)

    # A coluna deixa de ser obrigatória num commit de metadados; os dados não são tocados.
    table = DeltaTable(uri)
    table.alter.drop_column_not_null("id_cliente")

    changed = DeltaTable(uri)
    assert pa.schema(changed.schema()).field("id_cliente").nullable
    assert changed.version() == 2 and len(changed.file_uris()) == 2
    assert changed.history()[0]["operation"] in ("CHANGE COLUMN", "DROP COLUMN NOT NULL", "UPDATE COLUMN NULLABILITY")


def test_duckdb_view_pins_version_and_reader_feeds_write(folder: Callable[[str], str], two_months: pa.Table) -> None:
    """Uma view sobre ``delta_scan(uri, version := v)`` lê sempre ``v``; o leitor Arrow do DuckDB entra no ``write_deltalake``."""
    source = folder("source")
    write_deltalake(source, two_months, mode="append", partition_by=["mes"])

    con = connect_duckdb(("delta",))
    con.execute(f"CREATE VIEW pinned AS SELECT * FROM delta_scan('{source}', version := 0)")
    con.execute(f"CREATE VIEW current AS SELECT * FROM delta_scan('{source}')")

    # Um commit novo não muda a view presa à versão 0; a view sem versão relê o log a cada consulta.
    write_deltalake(source, two_months.slice(0, 10), mode="append")
    assert con.execute("SELECT count(*) FROM pinned").fetchone()[0] == 1000
    assert con.execute("SELECT count(*) FROM current").fetchone()[0] == 1010

    # export_partition em modo rewrite: o resultado de uma consulta sai em lotes e substitui a
    # partição na tabela publicada.
    target = folder("target")
    write_deltalake(target, two_months, mode="append", partition_by=["mes"])
    reader = con.execute(f"SELECT * FROM pinned WHERE mes = '{MONTHS[1]}'").to_arrow_reader()
    write_deltalake(target, reader, mode="overwrite", predicate=f"mes = '{MONTHS[1]}'")

    published = DeltaTable(target)
    assert published.version() == 1 and published.to_pyarrow_table().num_rows == 1000
    assert MONTHS[1] in published.history()[0]["operationParameters"]["predicate"]
    con.close()


def stat_converter(field_type: pa.DataType) -> Callable[[str], object] | None:
    """A conversão do texto do ``RETURN_STATS`` para o valor que o log guarda, ou ``None`` quando o tipo fica sem mínimo e máximo.

    É a regra do ``stat_converter`` de ``scripts/migrate_parquet_to_delta.py``, que a etapa 3 leva a
    ``register_files``: inteiro, data, ``double`` e texto transcrevem exato; o ``decimal`` vira número
    JSON no log e perde a linha na poda (``test_written_stats_lose_the_row_on_decimal``), e o
    ``timestamp`` sai truncado em milissegundos.
    """
    if pa.types.is_integer(field_type):
        return int
    if pa.types.is_floating(field_type):
        return float
    if pa.types.is_date(field_type) or pa.types.is_string(field_type):
        return str
    return None


def column_statistics(row: dict, schema: pa.Schema) -> tuple[dict, dict, dict]:
    """O mínimo, o máximo e a contagem de nulos de uma linha de ``RETURN_STATS``: ``nullCount`` de toda coluna, e mínimo e máximo só dos tipos que ``stat_converter`` transcreve."""
    minimum, maximum, nulls = {}, {}, {}
    for quoted_column, values in row["column_statistics"].items():
        column = quoted_column.strip('"')
        nulls[column] = int(values["null_count"])
        # Uma coluna só de nulos chega sem min e max.
        convert = stat_converter(schema.field(column).type)
        if convert is None or "min" not in values:
            continue
        minimum[column] = convert(values["min"])
        maximum[column] = convert(values["max"])
    return minimum, maximum, nulls


def add_actions_from_return_stats(rows: list[dict], table_uri: str, schema: pa.Schema) -> list[AddAction]:
    """Uma ``AddAction`` por linha de ``RETURN_STATS``: caminho relativo, tamanho, partição e as estatísticas de ``column_statistics``."""
    actions = []
    for row in rows:
        minimum, maximum, nulls = column_statistics(row, schema)
        actions.append(
            AddAction(
                path=row["filename"].removeprefix(f"{table_uri}/"),
                size=row["file_size_bytes"],
                partition_values=dict(row["partition_keys"]),
                modification_time=int(time.time() * 1000),
                data_change=True,
                stats=json.dumps({"numRecords": row["count"], "minValues": minimum, "maxValues": maximum, "nullCount": nulls}),
            )
        )

    return actions


def test_rewrite_by_duckdb_copy_registered_with_stats(folder: Callable[[str], str], two_months: pa.Table) -> None:
    """A tabela inteira reescrita pelo ``COPY`` particionado do DuckDB e registrada num commit ``overwrite`` com as estatísticas que a etapa 3 registra."""
    uri = folder("rewrite")
    write_deltalake(uri, two_months, mode="append", partition_by=["mes"])

    # O DuckDB grava os arquivos novos dentro da pasta da tabela, um por partição, com nome próprio e as estatísticas.
    con = connect_duckdb(("delta",))
    cursor = con.execute(
        f"COPY (SELECT id_operacao, mes, data_ref, id_cliente, valor, descricao AS descricao_nova FROM delta_scan('{uri}')) "
        f"TO '{uri}' (FORMAT parquet, PARTITION_BY (mes), APPEND true, FILENAME_PATTERN 'rewrite_{{uuid}}', RETURN_STATS)"
    )
    columns = [column[0] for column in cursor.description]
    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
    assert sorted(row["partition_keys"]["mes"] for row in rows) == list(MONTHS)

    # Um único commit troca todos os arquivos e o esquema: a coluna renomeada entra, a antiga sai.
    new_schema = two_months.schema.set(two_months.schema.get_field_index("descricao"), pa.field("descricao_nova", pa.string()))
    actions = add_actions_from_return_stats(rows, uri, new_schema)
    DeltaTable(uri).create_write_transaction(actions, mode="overwrite", schema=new_schema, partition_by=["mes"])

    rewritten = DeltaTable(uri)
    assert rewritten.version() == 1 and len(rewritten.file_uris()) == 2
    assert [field.name for field in rewritten.schema().fields][-1] == "descricao_nova"
    assert rewritten.to_pyarrow_table().num_rows == 1000

    # As colunas inteiras e a de texto têm mínimo e máximo; valor (decimal) e data_ref (timestamp) ficam sem.
    registered = pa.table(rewritten.get_add_actions(flatten=True))
    for name in ("min.id_operacao", "min.id_cliente", "min.descricao_nova"):
        assert registered.column(name).null_count == 0, name
    for name in ("min.valor", "min.data_ref"):
        assert registered.column(name).null_count == registered.num_rows, name

    # As estatísticas registradas deixam o DuckDB pular os arquivos: nenhum id acima do máximo, nenhum texto depois de 'zzz'.
    for condition in (f"id_operacao > {ROWS}", "descricao_nova > 'zzz'"):
        plan = con.execute(f"EXPLAIN ANALYZE SELECT count(*) FROM delta_scan('{uri}') WHERE {condition}").fetchone()[1]
        scanned = re.search(r"Scanning Files: (\d+)/(\d+)", plan)
        assert scanned and scanned.group(1) == "0", condition
    con.close()


def changed_partitions(actions: list[dict]) -> set[str]:
    """As partições das ações ``add`` e ``remove`` de dados de um commit; ``dataChange`` falso, o da compactação, não conta."""
    changed = set()
    for action in actions:
        # Cada linha do log traz uma ação só; as de arquivo são add e remove.
        file_action = action.get("add") or action.get("remove")
        if file_action is not None and file_action["dataChange"]:
            changed.add(file_action["partitionValues"]["mes"])
    return changed


def version_diff(uri: str, published: int, current: int) -> set[str]:
    """As partições com dados alterados nos commits depois de ``published`` até ``current``: o que a publicação recarrega.

    É a leitura do log de ``delta.version_diff`` da etapa 3, commit a commit.
    """
    changed = set()
    for version in range(published + 1, current + 1):
        changed |= changed_partitions(log_actions(uri, version))
    return changed


def test_version_diff_reads_data_changes_in_the_log(folder: Callable[[str], str], two_months: pa.Table) -> None:
    """As partições a recarregar no Redshift saem das ações ``add`` e ``remove`` com ``dataChange`` do log: a substituição, o ``append`` e a remoção contam, a compactação não."""
    uri = folder("diff")
    february = two_months.filter(pc.field("mes") == MONTHS[1])

    write_deltalake(uri, two_months, mode="append", partition_by=["mes"])  # versão 0
    write_deltalake(uri, february, mode="overwrite", predicate=f"mes = '{MONTHS[1]}'")  # versão 1: fevereiro substituído
    write_deltalake(uri, two_months.slice(0, 10), mode="append")  # versão 2: dez linhas de janeiro
    DeltaTable(uri).optimize.compact(partition_filters=[("mes", "=", MONTHS[0])])  # versão 3: janeiro compactado
    DeltaTable(uri).delete(f"mes = '{MONTHS[1]}' AND id_operacao < {ROWS // 2 + 10}")  # versão 4: dez linhas de fevereiro apagadas
    assert DeltaTable(uri).version() == 4

    assert version_diff(uri, 0, 1) == {MONTHS[1]}
    assert version_diff(uri, 1, 2) == {MONTHS[0]}
    assert version_diff(uri, 2, 3) == set()
    assert version_diff(uri, 3, 4) == {MONTHS[1]}
    assert version_diff(uri, 0, 4) == set(MONTHS)
    assert version_diff(uri, 4, 4) == set()

    # A compactação junta os dois arquivos de janeiro num, com dataChange falso nas três ações; a
    # diferença dos conjuntos de arquivos das duas versões a contaria como mudança de dados.
    compaction = log_actions(uri, 3)
    adds = [action["add"] for action in compaction if "add" in action]
    removes = [action["remove"] for action in compaction if "remove" in action]
    assert (len(adds), len(removes)) == (1, 2)
    assert [file_action["dataChange"] for file_action in adds + removes] == [False] * 3
    before = set(DeltaTable(uri, version=2).get_add_actions().column("path").to_pylist())
    after = set(DeltaTable(uri, version=3).get_add_actions().column("path").to_pylist())
    assert before != after
    assert DeltaTable(uri, version=3).to_pyarrow_table().num_rows == DeltaTable(uri, version=2).to_pyarrow_table().num_rows


def test_compact_and_checkpoint(folder: Callable[[str], str], two_months: pa.Table) -> None:
    """``optimize.compact`` junta os arquivos pequenos de um mês; ``create_checkpoint`` grava o resumo do log."""
    uri = folder("compact")
    write_deltalake(uri, two_months, mode="append", partition_by=["mes"])
    for _ in range(4):
        write_deltalake(uri, two_months.slice(600, 5), mode="append")  # cinco linhas de fevereiro por commit

    table = DeltaTable(uri)
    assert len(table.file_uris()) == 6

    # A compactação é um commit que remove os arquivos pequenos da partição e acrescenta um só.
    metrics = table.optimize.compact(partition_filters=[("mes", "=", MONTHS[1])])
    assert (metrics["numFilesAdded"], metrics["numFilesRemoved"]) == (1, 5)
    compacted = DeltaTable(uri)
    assert len(compacted.file_uris()) == 2 and compacted.to_pyarrow_table().num_rows == 1020

    # O checkpoint materializa o estado em Parquet e aponta para ele em _last_checkpoint.
    compacted.create_checkpoint()
    log = Path(uri) / "_delta_log"
    last = json.loads((log / "_last_checkpoint").read_text(encoding="utf-8"))
    assert last["version"] == compacted.version()
    assert (log / f"{last['version']:020d}.checkpoint.parquet").exists()
    assert DeltaTable(uri, version=0).to_pyarrow_table().num_rows == 1000  # o log anterior continua legível


def test_export_snapshot_by_copying_files(folder: Callable[[str], str], two_months: pa.Table) -> None:
    """``export_snapshot(mode="copy")``: os arquivos que o log lista, copiados no layout ``mes=.../``, sem ler dados."""
    uri = folder("export")
    february = two_months.filter(pc.field("mes") == MONTHS[1])
    write_deltalake(uri, two_months, mode="append", partition_by=["mes"])
    write_deltalake(uri, february, mode="overwrite", predicate=f"mes = '{MONTHS[1]}'")

    # A pasta tem três arquivos de dados; o snapshot atual lista dois.
    on_disk = [path for path in Path(uri).rglob("*.parquet") if "_delta_log" not in path.parts]
    listed = DeltaTable(uri).get_add_actions().column("path").to_pylist()
    assert len(on_disk) == 3 and len(listed) == 2

    destination = Path(folder("exported"))
    for relative in listed:
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(Path(uri) / relative, target)

    con = connect_duckdb(("delta",))
    count = con.execute(f"SELECT count(*) FROM read_parquet('{destination}/*/*.parquet', hive_partitioning = true)").fetchone()[0]
    assert count == 1000
    con.close()


def test_initial_load_from_parquet_folders(folder: Callable[[str], str], two_months: pa.Table) -> None:
    """A carga inicial: cada pasta ``mes=<valor>/`` de Parquet entra por ``overwrite`` com predicado, com o valor do caminho na coluna de partição, a chave em ``BIGINT`` e o ``double`` mantido, e recomeça de onde parou."""
    # A origem tem id_cliente em int32 e valor em double, como os modelos atuais; o contrato leva a
    # chave a int64 e mantém o double.
    source_table = two_months.set_column(two_months.schema.get_field_index("valor"), "valor", two_months.column("valor").cast(pa.float64()))
    contract = source_table.schema.set(source_table.schema.get_field_index("id_cliente"), pa.field("id_cliente", pa.int64()))
    source = Path(folder("source_parquet"))
    pq.write_to_dataset(source_table, source, partition_cols=["mes"])

    uri = folder("loaded")
    DeltaTable.create(uri, Schema.from_arrow(contract), partition_by=["mes"])
    con = connect_duckdb(("delta",))

    def load_missing_partitions() -> list[str]:
        """Carrega as pastas da origem que o log ainda não tem e devolve os valores carregados."""
        loaded = set(DeltaTable(uri).get_add_actions(flatten=True).column("partition.mes").to_pylist())
        pending = []
        for partition_folder in sorted(source.iterdir()):
            value = partition_folder.name.removeprefix("mes=")
            if value in loaded:
                continue
            # A pasta inteira sem hive_partitioning: o valor da partição vem do caminho, como texto.
            reader = con.execute(
                f"SELECT id_operacao, '{value}' AS mes, data_ref, id_cliente::BIGINT AS id_cliente, valor, descricao "
                f"FROM read_parquet('{partition_folder}/*.parquet', hive_partitioning = false)"
            ).to_arrow_reader()
            write_deltalake(uri, reader, mode="overwrite", predicate=f"mes = '{value}'")
            pending.append(value)
        return pending

    assert load_missing_partitions() == list(MONTHS)
    assert load_missing_partitions() == []  # a segunda passagem não tem o que carregar

    # O relatório: contagem e soma por partição iguais entre a origem e o Delta. O double é somado
    # como DECIMAL(38, 6) de cada valor, porque a soma em ponto flutuante depende da ordem.
    measures = "mes, count(*), sum(valor::DECIMAL(38, 6))"
    report = f"SELECT {measures} FROM read_parquet('{source}/*/*.parquet', hive_partitioning = true, hive_types_autocast = false) GROUP BY mes ORDER BY mes"
    loaded = f"SELECT {measures} FROM delta_scan('{uri}') GROUP BY mes ORDER BY mes"
    assert con.execute(report).fetchall() == con.execute(loaded).fetchall()
    loaded_schema = pa.schema(DeltaTable(uri).schema())
    assert loaded_schema.field("valor").type == pa.float64() and loaded_schema.field("id_cliente").type == pa.int64()

    # hive_types_autocast = false mantém o valor do caminho como texto: com a detecção de tipos, um
    # valor AAAA-MM-DD, como o data_str da base de origem, vira DATE.
    dated = Path(folder("source_dated")) / "data_str=2026-08-31"
    dated.mkdir(parents=True)
    pq.write_table(source_table.slice(0, 1), dated / "chunk_0.parquet")
    detected = con.execute(f"DESCRIBE SELECT data_str FROM read_parquet('{dated}/*.parquet', hive_partitioning = true)").fetchone()[1]
    kept = con.execute(f"DESCRIBE SELECT data_str FROM read_parquet('{dated}/*.parquet', hive_partitioning = true, hive_types_autocast = false)").fetchone()[1]
    assert (detected, kept) == ("DATE", "VARCHAR")
    con.close()


def test_create_write_transaction_trusts_path_and_stats(folder: Callable[[str], str], two_months: pa.Table) -> None:
    """``create_write_transaction`` não confere a ação: um caminho inexistente e uma estatística falsa commitam, e os leitores obedecem à ação."""
    uri = folder("trusts_action")
    write_deltalake(uri, two_months, mode="append", partition_by=["mes"])
    con = connect_duckdb(("delta",))

    # Um arquivo que não existe entra no log; só a leitura que toca a partição dele falha, e o restore desfaz.
    missing = AddAction(path="mes=2026-03/nada.parquet", size=1, partition_values={"mes": "2026-03"}, modification_time=1, data_change=True, stats=json.dumps({"numRecords": 1}))
    DeltaTable(uri).create_write_transaction([missing], mode="append", schema=DeltaTable(uri).schema(), partition_by=["mes"])
    assert DeltaTable(uri).version() == 1
    assert DeltaTable(uri).to_pyarrow_table(filters=[("mes", "=", MONTHS[0])]).num_rows == 500
    with pytest.raises((FileNotFoundError, pa.ArrowInvalid)):  # o leitor confia até no size da ação: 1 byte declarado dá o erro do rodapé
        DeltaTable(uri).to_pyarrow_table()
    assert con.execute(f"SELECT count(*) FROM delta_scan('{uri}') WHERE mes = '{MONTHS[0]}'").fetchone()[0] == 500
    with pytest.raises(duckdb.IOException):
        con.execute(f"SELECT count(*) FROM delta_scan('{uri}')").fetchone()
    DeltaTable(uri).restore(0)
    assert DeltaTable(uri).version() == 2 and DeltaTable(uri).to_pyarrow_table().num_rows == 1000

    # Dez linhas com id_operacao de 160000 a 160009, registradas com mínimo, máximo e numRecords falsos.
    file = Path(uri) / f"mes={MONTHS[1]}" / "externo.parquet"
    pq.write_table(sample_table().slice(160_000, 10).drop_columns(["mes"]), file)
    lying = AddAction(
        path=f"mes={MONTHS[1]}/externo.parquet",
        size=file.stat().st_size,
        partition_values={"mes": MONTHS[1]},
        modification_time=int(time.time() * 1000),
        data_change=True,
        stats=json.dumps({"numRecords": 999, "minValues": {"id_operacao": 900_000}, "maxValues": {"id_operacao": 900_010}, "nullCount": {"id_operacao": 0}}),
    )
    DeltaTable(uri).create_write_transaction([lying], mode="append", schema=DeltaTable(uri).schema(), partition_by=["mes"])

    # Os três leitores podam pela estatística da ação e devolvem zero linhas para dez que existem; sem filtro, leem as 1.010.
    where = "id_operacao BETWEEN 160000 AND 160009"
    assert DeltaTable(uri).to_pyarrow_table().num_rows == 1010
    assert DeltaTable(uri).to_pyarrow_table(filters=[("id_operacao", ">=", 160_000), ("id_operacao", "<=", 160_009)]).num_rows == 0
    assert con.execute(f"SELECT count(*) FROM delta_scan('{uri}') WHERE {where}").fetchone()[0] == 0
    plan = con.execute(f"EXPLAIN ANALYZE SELECT count(*) FROM delta_scan('{uri}') WHERE {where}").fetchone()[1]
    assert re.search(r"Scanning Files: 0/3", plan), plan
    query = QueryBuilder()
    query.register("t", DeltaTable(uri))
    assert pa.table(query.execute(f"SELECT count(*) AS n FROM t WHERE {where}").read_all()).to_pylist() == [{"n": 0}]

    # O numRecords falso vira o count(*) do DataFusion, e o máximo falso vira o max.<coluna> que max_key leria.
    assert pa.table(query.execute("SELECT count(*) AS n FROM t").read_all()).to_pylist() == [{"n": 1999}]
    actions = DeltaTable(uri).get_add_actions(flatten=True)
    assert max(actions.column("max.id_operacao").to_pylist()) == 900_010
    assert sum(actions.column("num_records").to_pylist()) == 1999
    con.close()


def test_create_write_transaction_trusts_file_schema(folder: Callable[[str], str], two_months: pa.Table) -> None:
    """Um arquivo sem uma coluna ``NOT NULL`` ou com um tipo incompatível commita; a coluna lê nulo, o tipo falha na leitura."""
    uri = folder("trusts_schema")
    not_null = pa.schema([field.with_nullable(False) for field in two_months.schema])
    DeltaTable.create(uri, Schema.from_arrow(not_null), partition_by=["mes"])
    write_deltalake(uri, two_months.cast(not_null), mode="append")
    con = connect_duckdb(("delta",))
    ten = sample_table().slice(160_000, 10).drop_columns(["mes"])

    def register(month: str, name: str, data: pa.Table) -> None:
        folder_ = Path(uri) / f"mes={month}"
        folder_.mkdir(exist_ok=True)
        pq.write_table(data, folder_ / name)
        action = AddAction(
            path=f"mes={month}/{name}",
            size=(folder_ / name).stat().st_size,
            partition_values={"mes": month},
            modification_time=int(time.time() * 1000),
            data_change=True,
            stats=json.dumps({"numRecords": data.num_rows, "minValues": {}, "maxValues": {}, "nullCount": {}}),
        )
        DeltaTable(uri).create_write_transaction([action], mode="append", schema=DeltaTable(uri).schema(), partition_by=["mes"])

    # Sem a coluna descricao, NOT NULL no contrato: o commit passa e os dois leitores devolvem nulo, o que write_deltalake recusa.
    register("2026-03", "sem_coluna.parquet", ten.drop_columns(["descricao"]))
    assert DeltaTable(uri).to_pyarrow_table(filters=[("mes", "=", "2026-03")]).column("descricao").null_count == 10
    assert con.execute(f"SELECT count(*) FROM delta_scan('{uri}') WHERE mes = '2026-03' AND descricao IS NULL").fetchone()[0] == 10
    with_nulls = ten.drop_columns(["descricao"]).append_column("descricao", pa.array([None] * 10, pa.string())).append_column("mes", pa.array(["2026-03"] * 10))
    with pytest.raises(DeltaError, match="failed validation"):
        write_deltalake(uri, with_nulls, mode="append", schema_mode="merge")

    # valor como texto não numérico numa coluna decimal: o commit passa, e a leitura da coluna falha depois dele.
    register("2026-04", "tipo_errado.parquet", ten.set_column(3, "valor", pa.array(["abc"] * 10, pa.string())))
    with pytest.raises(pa.ArrowInvalid, match="not a valid decimal128"):
        DeltaTable(uri).to_pyarrow_table(filters=[("mes", "=", "2026-04")])
    assert con.execute(f"SELECT count(*) FROM delta_scan('{uri}') WHERE mes = '2026-04'").fetchone()[0] == 10
    with pytest.raises(duckdb.Error):
        con.execute(f"SELECT sum(valor) FROM delta_scan('{uri}') WHERE mes = '2026-04'").fetchone()

    # mode="overwrite" com partition_filters troca só os arquivos da partição filtrada.
    before = set(DeltaTable(uri).get_add_actions(flatten=True).column("path").to_pylist())
    pq.write_table(ten, Path(uri) / f"mes={MONTHS[1]}" / "novo.parquet")
    replacement = AddAction(
        path=f"mes={MONTHS[1]}/novo.parquet",
        size=(Path(uri) / f"mes={MONTHS[1]}" / "novo.parquet").stat().st_size,
        partition_values={"mes": MONTHS[1]},
        modification_time=int(time.time() * 1000),
        data_change=True,
        stats=json.dumps({"numRecords": 10}),
    )
    DeltaTable(uri).create_write_transaction([replacement], mode="overwrite", schema=DeltaTable(uri).schema(), partition_by=["mes"], partition_filters=[("mes", "=", MONTHS[1])])
    after = set(DeltaTable(uri).get_add_actions(flatten=True).column("path").to_pylist())
    assert after - before == {f"mes={MONTHS[1]}/novo.parquet"}
    assert all(path.startswith(f"mes={MONTHS[1]}/") for path in before - after) and len(before - after) == 1
    assert DeltaTable(uri).to_pyarrow_table(filters=[("mes", "in", [MONTHS[0], MONTHS[1]])]).num_rows == 510
    con.close()


def physical_types(path: Path) -> dict[str, str]:
    """O tipo físico de cada coluna no rodapé de um arquivo Parquet."""
    schema = pq.ParquetFile(path).schema
    types = {}
    for index in range(len(schema)):
        types[schema.column(index).name] = schema.column(index).physical_type
    return types


def test_compact_rewrites_files_from_another_writer(folder: Callable[[str], str], two_months: pa.Table) -> None:
    """``optimize.compact`` reescreve pelo escritor do delta-rs: o ``INT96`` e o ``FIXED_LEN_BYTE_ARRAY`` registrados saem em ``INT64``, com estatística."""
    uri = folder("compact_foreign")
    write_deltalake(uri, two_months.slice(0, 500), mode="append", partition_by=["mes"])

    # Dois arquivos como o UNLOAD do Redshift grava: timestamp em INT96 e decimal em FIXED_LEN_BYTE_ARRAY, registrados por AddAction.
    partition = Path(uri) / f"mes={MONTHS[1]}"
    partition.mkdir()
    actions = []
    for index in range(2):
        file = partition / f"unload_{index}.parquet"
        pq.write_table(two_months.slice(500 + 5 * index, 5).drop_columns(["mes"]), file, use_deprecated_int96_timestamps=True)
        actions.append(AddAction(path=f"mes={MONTHS[1]}/{file.name}", size=file.stat().st_size, partition_values={"mes": MONTHS[1]}, modification_time=int(time.time() * 1000), data_change=True, stats=json.dumps({"numRecords": 5})))
    physical = physical_types(partition / "unload_0.parquet")
    assert (physical["data_ref"], physical["valor"]) == ("INT96", "FIXED_LEN_BYTE_ARRAY")
    DeltaTable(uri).create_write_transaction(actions, mode="append", schema=DeltaTable(uri).schema(), partition_by=["mes"])

    metrics = DeltaTable(uri).optimize.compact(partition_filters=[("mes", "=", MONTHS[1])])
    assert (metrics["numFilesAdded"], metrics["numFilesRemoved"]) == (1, 2)
    compacted = pa.table(DeltaTable(uri).get_add_actions(flatten=True)).filter(pc.equal(pc.field("partition.mes"), MONTHS[1]))
    assert compacted.num_rows == 1
    path = compacted.column("path").to_pylist()[0]
    physical = physical_types(Path(uri) / path)
    assert (physical["data_ref"], physical["valor"]) == ("INT64", "INT64")
    metadata = pq.ParquetFile(Path(uri) / path).metadata
    statistics = {metadata.schema.column(i).name: metadata.row_group(0).column(i).statistics.has_min_max for i in range(metadata.num_columns)}
    assert all(statistics.values()), statistics
    assert compacted.column("max.data_ref").to_pylist()[0] is not None
    assert DeltaTable(uri).to_pyarrow_table().num_rows == 510


def test_description_and_comments_survive_overwrite(folder: Callable[[str], str]) -> None:
    """A descrição, o nome e os comentários de coluna atravessam o ``overwrite`` e mudam por ``alter``."""
    uri = folder("description")
    schema = pa.schema(
        [
            pa.field("id_operacao", pa.int64(), nullable=False, metadata={"comment": "Identificador"}),
            pa.field("mes", pa.string(), nullable=False, metadata={"comment": "Partição"}),
        ]
    )
    DeltaTable.create(uri, schema, mode="ignore", partition_by=["mes"], name="cad_operacoes", description="Operações por data-base")

    def comments(table: DeltaTable) -> dict[str, str | None]:
        """O comentário de cada campo, como o esquema Delta o guarda."""
        return {field["name"]: field.get("metadata", {}).get("comment") for field in json.loads(table.schema().to_json())["fields"]}

    # O lote gravado não traz metadados de campo, e o overwrite com e sem predicado não apaga os da tabela.
    data = pa.table({"id_operacao": pa.array([1], pa.int64()), "mes": pa.array([MONTHS[0]])})
    write_deltalake(uri, data, mode="overwrite", predicate=f"mes = '{MONTHS[0]}'")
    write_deltalake(uri, data, mode="overwrite")
    table = DeltaTable(uri)
    assert table.metadata().description == "Operações por data-base" and table.metadata().name == "cad_operacoes"
    assert comments(table) == {"id_operacao": "Identificador", "mes": "Partição"}

    # A sincronização com o modelo: cada alteração é um commit só de metaData, sem tocar nos dados.
    table.alter.set_table_description("Operações de crédito por data-base")
    table.alter.set_column_metadata("id_operacao", {"comment": "Identificador da operação"})
    table = DeltaTable(uri)
    assert table.metadata().description == "Operações de crédito por data-base"
    assert comments(table) == {"id_operacao": "Identificador da operação", "mes": "Partição"}
    assert {next(iter(action)) for action in log_actions(uri, table.version())} == {"commitInfo", "metaData"}
    assert table.to_pyarrow_table().num_rows == 1


def test_written_stats_lose_the_row_on_decimal(folder: Callable[[str], str]) -> None:
    """O próprio delta-rs grava o mínimo e o máximo de ``decimal`` como float JSON, e a poda perde a linha."""
    uri = folder("stats_types")
    top = decimal.Decimal("123456789012345.21")
    assert decimal.Decimal(repr(float(top))) < top, "o valor escolhido tem de arredondar para baixo"
    top_time = dt.datetime(2026, 8, 31, 23, 59, 59, 999999)
    schema = pa.schema(
        [
            pa.field("valor", pa.decimal128(18, 2), nullable=False),
            pa.field("carimbo", pa.timestamp("us"), nullable=False),
            pa.field("taxa", pa.float64(), nullable=False),
            pa.field("descricao", pa.string(), nullable=False),
        ]
    )
    data = pa.table(
        {
            "valor": pa.array([decimal.Decimal("0.01"), top], pa.decimal128(18, 2)),
            "carimbo": pa.array([dt.datetime(2026, 7, 1), top_time], pa.timestamp("us")),
            "taxa": pa.array([0.1 + 0.2, 1234567890.123456789], pa.float64()),
            "descricao": pa.array(["a", "z" * 40]),
        },
        schema=schema,
    )
    write_deltalake(uri, data)

    # O log guarda o decimal como número JSON e o timestamp truncado em milissegundos; o double e o texto saem exatos.
    add = [action["add"] for action in log_actions(uri, 0) if "add" in action][0]
    stats = json.loads(add["stats"])
    assert decimal.Decimal(repr(stats["maxValues"]["valor"])) < top, "o máximo registrado fica abaixo do valor real"
    assert stats["maxValues"]["carimbo"] == "2026-08-31 23:59:59.999"
    assert stats["maxValues"]["taxa"] == 1234567890.123456789 and stats["maxValues"]["descricao"] == "z" * 40

    # O máximo abaixo do valor real poda o arquivo que tem a linha, sem erro, nos dois leitores.
    dataset = DeltaTable(uri).to_pyarrow_dataset()
    assert dataset.count_rows(filter=pc.field("valor") == top) == 0
    connection = connect_duckdb(["delta"])
    assert connection.execute(f"SELECT count(*) FROM delta_scan('{uri}') WHERE valor = {top}").fetchone() == (0,)

    # O timestamp truncado e os tipos exatos continuam achando a linha.
    assert dataset.count_rows(filter=pc.field("carimbo") == top_time) == 1
    assert dataset.count_rows(filter=pc.field("taxa") == 1234567890.123456789) == 1
    assert dataset.count_rows(filter=pc.field("descricao") == "z" * 40) == 1


def test_nan_statistics_hide_rows_from_delta_scan(folder: Callable[[str], str]) -> None:
    """O ``NaN`` fica fora do máximo, no ``RETURN_STATS`` e no escritor do delta-rs, e o ``delta_scan`` responde a um filtro por intervalo conforme a poda; o infinito registrado por ``stat_converter`` vira ``Infinity`` no log, e o delta-rs grava ``null`` no lugar dele.

    O ``Double`` transcreve exato os valores finitos; os especiais não. No DuckDB, ``NaN > 3`` é
    verdadeiro: a tabela do sandbox devolve a linha, e a tabela Delta a perde quando o arquivo é
    podado pelo máximo sem o ``NaN``. É a decisão do ``Double`` não finito no contrato (etapa 1).
    """
    con = connect_duckdb(("delta",))
    schema = pa.schema([("id", pa.int64()), ("valor", pa.float64())])
    outcomes = {}
    for name, values in (("nan", "(1, 1.5), (2, 'nan'::DOUBLE), (3, 2.0)"), ("infinito", "(1, 1.5), (2, 'inf'::DOUBLE), (3, 2.0)")):
        uri = folder(f"especiais_{name}")
        DeltaTable.create(uri, schema)
        con.execute(f"CREATE OR REPLACE TABLE t AS SELECT * FROM (VALUES {values}) v(id, valor)")
        cursor = con.execute(f"COPY t TO '{uri}/f.parquet' (FORMAT parquet, RETURN_STATS)")
        columns = [column[0] for column in cursor.description]
        row = dict(zip(columns, cursor.fetchone()))
        minimum, maximum, nulls = column_statistics(row, schema)
        stats = json.dumps({"numRecords": row["count"], "minValues": minimum, "maxValues": maximum, "nullCount": nulls})
        added = AddAction(path="f.parquet", size=row["file_size_bytes"], partition_values={}, modification_time=int(time.time() * 1000),
                          data_change=True, stats=stats)
        DeltaTable(uri).create_write_transaction([added], mode="append", schema=schema)
        sandbox = con.execute("SELECT count(*) FROM t WHERE valor > 3").fetchone()[0]
        registered = con.execute(f"SELECT count(*) FROM delta_scan('{uri}') WHERE valor > 3").fetchone()[0]
        maximum = pa.table(DeltaTable(uri).get_add_actions(flatten=True)).column("max.valor").to_pylist()
        outcomes[name] = (sandbox, registered, maximum, log_actions(uri, 1)[-1]["add"]["stats"])

    # NaN: o sandbox acha a linha, o delta_scan poda o arquivo pelo máximo 2.0.
    assert outcomes["nan"][:3] == (1, 0, [2.0])
    # Infinito: o log leva Infinity, que não é JSON, e o delta-rs o lê como nulo; o delta_scan acha a linha.
    assert '"valor": Infinity' in outcomes["infinito"][3]
    assert outcomes["infinito"][:3] == (1, 1, [None])

    # O escritor do delta-rs: o mesmo máximo sem o NaN, e a resposta conforme a poda; no infinito, null.
    uri = folder("especiais_delta_rs")
    write_deltalake(uri, pa.table({"id": pa.array([1, 2, 3], pa.int64()), "valor": pa.array([1.5, float("nan"), 2.0])}))
    written = json.loads(log_actions(uri, 0)[-1]["add"]["stats"])
    assert written["maxValues"]["valor"] == 2.0
    pruned = con.execute(f"SELECT count(*) FROM delta_scan('{uri}') WHERE valor > 3").fetchone()[0]
    kept = con.execute(f"SELECT count(*) FROM delta_scan('{uri}') WHERE valor >= 2").fetchone()[0]
    assert (pruned, kept) == (0, 2)  # o NaN some com o arquivo podado e aparece com ele lido
    uri = folder("infinito_delta_rs")
    write_deltalake(uri, pa.table({"id": pa.array([1, 2], pa.int64()), "valor": pa.array([1.5, float("inf")])}))
    assert json.loads(log_actions(uri, 0)[-1]["add"]["stats"])["maxValues"]["valor"] is None
    con.close()



def test_float_statistics_off_keep_the_nan_row(folder: Callable[[str], str]) -> None:
    """Sem mínimo e máximo do ``Double`` no rodapé e no log, o ``delta_scan`` devolve a linha do ``NaN``; tirar só os do log não basta, porque o leitor Parquet do DuckDB poda o grupo de linhas pelo rodapé.

    O delta-rs respeita ``delta.dataSkippingStatsColumns`` ao gravar o log, e o rodapé continua com o
    máximo sem o ``NaN``; ``ColumnProperties(statistics_enabled="NONE")`` tira a estatística da
    coluna dos dois. O arquivo do DuckDB já sai sem mínimo e máximo no grupo com ``NaN``, e
    registrado sem os dois no log devolve a linha que ``test_nan_statistics_hide_rows_from_delta_scan``
    perde. É a recomendação da issue #59.
    """
    con = connect_duckdb(("delta",))
    table = pa.table({"id": pa.array([1, 2, 3, 4, 5, 6], pa.int64()), "valor": pa.array([1.5, float("nan"), 2.0, 4.0, 5.0, 6.0])})

    def delta_scan_count(uri: str) -> int:
        return con.execute(f"SELECT count(*) FROM delta_scan('{uri}') WHERE valor > 3").fetchone()[0]

    # O log sem o valor e o rodapé com o máximo 2.0 no grupo do NaN: o grupo é podado.
    uri = folder("estatistica_so_no_rodape")
    write_deltalake(uri, table, configuration={"delta.dataSkippingStatsColumns": "id"}, writer_properties=WriterProperties(max_row_group_size=3))
    assert "valor" not in json.loads(log_actions(uri, 0)[-1]["add"]["stats"])["maxValues"]
    assert delta_scan_count(uri) == 3

    # Sem estatística do valor no rodapé nem no log: a linha volta.
    uri = folder("sem_estatistica_do_double")
    properties = WriterProperties(max_row_group_size=3, column_properties={"valor": ColumnProperties(statistics_enabled="NONE")})
    write_deltalake(uri, table, writer_properties=properties)
    stats = json.loads(log_actions(uri, 0)[-1]["add"]["stats"])
    assert stats == {"numRecords": 6, "minValues": {"id": 1}, "maxValues": {"id": 6}, "nullCount": {"id": 0}}
    assert delta_scan_count(uri) == 4

    # O arquivo do DuckDB com [1.5, NaN, 2.0], registrado sem o mínimo e o máximo do valor.
    uri = folder("registro_sem_minimo_e_maximo")
    DeltaTable.create(uri, table.schema)
    con.register("origem", table.slice(0, 3))
    cursor = con.execute(f"COPY origem TO '{uri}/f.parquet' (FORMAT parquet, RETURN_STATS)")
    columns = [column[0] for column in cursor.description]
    row = dict(zip(columns, cursor.fetchone()))
    stats = json.dumps({"numRecords": row["count"], "minValues": {"id": 1}, "maxValues": {"id": 3}, "nullCount": {"id": 0, "valor": 0}})
    added = AddAction(path="f.parquet", size=row["file_size_bytes"], partition_values={}, modification_time=int(time.time() * 1000),
                      data_change=True, stats=stats)
    DeltaTable(uri).create_write_transaction([added], mode="append", schema=table.schema)
    assert delta_scan_count(uri) == 1
    con.close()


def test_float_statistics_off_per_partition_keep_the_nan_row_and_the_pruning(folder: Callable[[str], str]) -> None:
    """A regra da issue #59 por partição: a partição com valor não finito grava o ``Double`` sem mínimo e máximo, a outra grava os dois, e o ``delta_scan`` devolve a linha do ``NaN`` e continua podando pelo valor a partição sem ele.

    ``publish_partition`` grava uma partição por chamada de ``write_deltalake``, e o
    ``writer_properties`` vale para a chamada: o arquivo de agosto sai sem a estatística do valor no
    rodapé e no log, e o de setembro sai com ela. O log ``FileSystem`` do DuckDB mostra os arquivos
    abertos.
    """
    uri = folder("estatistica_por_particao")
    schema = pa.schema([("particao", pa.string()), ("valor", pa.float64())])
    DeltaTable.create(uri, schema, partition_by=["particao"])
    without_bounds = WriterProperties(column_properties={"valor": ColumnProperties(statistics_enabled="NONE")})
    partitions = {"2026-08-31": ([1.5, float("nan"), 2.0], without_bounds), "2026-09-30": ([1.0, 2.0, 2.5], None)}
    for value, (numbers, properties) in partitions.items():
        data = pa.table({"particao": [value] * len(numbers), "valor": pa.array(numbers, pa.float64())}, schema=schema)
        write_deltalake(uri, data, mode="overwrite", predicate=f"particao = '{value}'", writer_properties=properties)

    # Uma ação add por partição: agosto sem o valor nas estatísticas, setembro com ele.
    stats = {}
    for version in (1, 2):
        added = [action["add"] for action in log_actions(uri, version) if "add" in action][0]
        stats[added["partitionValues"]["particao"]] = json.loads(added["stats"])
    assert "valor" not in stats["2026-08-31"]["maxValues"]
    assert stats["2026-09-30"]["maxValues"]["valor"] == 2.5

    con = connect_duckdb(("delta",))
    con.execute("CALL enable_logging('FileSystem')")
    con.execute("CALL truncate_duckdb_logs()")
    assert con.execute(f"SELECT count(*) FROM delta_scan('{uri}') WHERE valor > 3").fetchone()[0] == 1  # o NaN de agosto
    opened = set()
    for (message,) in con.execute("SELECT message FROM duckdb_logs WHERE type = 'FileSystem'").fetchall():
        if '"op":"OPEN"' in message and ".parquet" in message:
            opened.add(re.search(r"particao=[0-9-]+", message).group(0))
    assert opened == {"particao=2026-08-31"}  # setembro podado pelo máximo 2.5
    con.close()

def test_two_registrations_of_the_same_partition_conflict(folder: Callable[[str], str], two_months: pa.Table) -> None:
    """Dois ``create_write_transaction(mode="overwrite")`` da mesma partição, a partir da mesma versão: o segundo é ``CommitFailedError``, e a partição fica com o arquivo do primeiro.

    É a guarda que ``publish_partition`` tem no ``write_deltalake``, e que ``register_files`` e
    ``rewrite`` convertem em ``ExecutionConflict``. A transação não atualiza o objeto que a fez: a
    versão dele continua a lida.
    """
    uri = folder("registro_concorrente")
    write_deltalake(uri, two_months, mode="append", partition_by=["mes"])
    first, second = DeltaTable(uri), DeltaTable(uri)
    month = two_months.filter(pc.equal(two_months.column("mes"), MONTHS[1])).drop_columns(["mes"])

    def action(name: str) -> AddAction:
        file = Path(uri) / f"mes={MONTHS[1]}" / f"{name}.parquet"
        pq.write_table(month, file)
        return AddAction(path=f"mes={MONTHS[1]}/{name}.parquet", size=file.stat().st_size, partition_values={"mes": MONTHS[1]},
                         modification_time=int(time.time() * 1000), data_change=True, stats=json.dumps({"numRecords": month.num_rows}))

    filters = [("mes", "=", MONTHS[1])]
    first.create_write_transaction([action("primeiro")], mode="overwrite", schema=first.schema(), partition_by=["mes"], partition_filters=filters)
    assert first.version() == 0  # o objeto não avança
    with pytest.raises(CommitFailedError, match="concurrent transaction deleted data this operation read"):
        second.create_write_transaction([action("segundo")], mode="overwrite", schema=second.schema(), partition_by=["mes"], partition_filters=filters)

    current = DeltaTable(uri)
    assert current.version() == 1 and current.to_pyarrow_dataset().count_rows() == 1000
    assert sorted(Path(path).name for path in current.file_uris() if f"mes={MONTHS[1]}" in path) == ["primeiro.parquet"]


def test_delta_scan_prunes_by_equality_and_range_not_by_in_list(folder: Callable[[str], str]) -> None:
    """O ``delta_scan`` abre só os arquivos das partições de um ``=`` ou de um intervalo; um ``IN`` de mais de um valor e um ``OR`` abrem todos.

    O log ``FileSystem`` do DuckDB mostra os arquivos abertos. ``BETWEEN`` somado ao ``IN`` abre o
    intervalo, e é a forma que serve à ``ingest`` da etapa 4 com partições não contíguas. O
    ``EXPLAIN ANALYZE`` dessa forma falha na extensão ``delta`` com ``InternalException``, e a
    consulta roda: a poda dela só se lê pelo log.
    """
    uri = folder("poda_por_predicado")
    values = [f"2026-0{month}" for month in range(1, 7)]
    data = pa.table({"id": pa.array(range(600), pa.int64()), "mes": pa.array([values[k % 6] for k in range(600)])})
    write_deltalake(uri, data, partition_by=["mes"])
    con = connect_duckdb(("delta",))
    con.execute("CALL enable_logging('FileSystem')")

    def partitions_opened(predicate: str) -> int:
        con.execute("CALL truncate_duckdb_logs()")
        con.execute(f"SELECT count(*) FROM delta_scan('{uri}', version := 0) WHERE {predicate}").fetchone()
        folders = set()
        for (message,) in con.execute("SELECT message FROM duckdb_logs WHERE type = 'FileSystem'").fetchall():
            if '"op":"OPEN"' in message and ".parquet" in message:
                folders.add(re.search(r"mes=[0-9-]+", message).group(0))
        return len(folders)

    assert partitions_opened(f"mes = '{values[2]}'") == 1
    assert partitions_opened(f"mes IN ('{values[2]}')") == 1
    assert partitions_opened(f"mes IN ('{values[2]}', '{values[4]}')") == 6
    assert partitions_opened(f"mes = '{values[2]}' OR mes = '{values[4]}'") == 6
    assert partitions_opened(f"mes BETWEEN '{values[2]}' AND '{values[4]}'") == 3
    in_range = f"mes BETWEEN '{values[2]}' AND '{values[4]}' AND mes IN ('{values[2]}', '{values[4]}')"
    assert partitions_opened(in_range) == 3
    assert con.execute(f"SELECT count(*) FROM delta_scan('{uri}', version := 0) WHERE {in_range}").fetchone()[0] == 200
    with pytest.raises(duckdb.InternalException, match="total_files inconsistent"):
        con.execute(f"EXPLAIN ANALYZE SELECT count(*) FROM delta_scan('{uri}', version := 0) WHERE {in_range}")
    con.close()


def test_partition_value_is_percent_encoded_in_the_folder_and_the_log(folder: Callable[[str], str]) -> None:
    """O delta-rs grava o valor de partição codificado por porcentagem no nome da pasta, e o caminho da ação ``add`` sai codificado de novo; o valor só de letras, dígitos, ``_``, ``.`` e ``-`` sai igual nos dois. A aspa simples fecha o literal do predicado de ``publish_partition``."""
    uri = folder("particao_codificada")
    for value in ["2026-Q1", "a:b", "a%b", "ação", "d'agua"]:
        write_deltalake(uri, pa.table({"id": pa.array([1], pa.int64()), "p": [value]}), partition_by=["p"], mode="append")

    # A pasta no disco leva o valor codificado uma vez; o caminho no log, a pasta codificada de novo.
    folders = sorted(entry.name for entry in Path(uri).iterdir() if entry.name.startswith("p="))
    assert folders == ["p=2026-Q1", "p=a%25b", "p=a%3Ab", "p=a%C3%A7%C3%A3o", "p=d%27agua"]
    actions = pa.table(DeltaTable(uri).get_add_actions(flatten=True))
    log_folders = dict(zip(actions.column("partition.p").to_pylist(), [path.split("/")[0] for path in actions.column("path").to_pylist()]))
    assert log_folders == {"2026-Q1": "p=2026-Q1", "a:b": "p=a%253Ab", "a%b": "p=a%2525b", "ação": "p=a%25C3%25A7%25C3%25A3o", "d'agua": "p=d%2527agua"}

    # O predicado da substituição por partição é texto SQL: o valor com aspa quebra o literal.
    write_deltalake(uri, pa.table({"id": pa.array([2], pa.int64()), "p": ["a:b"]}), mode="overwrite", predicate="p = 'a:b'")
    with pytest.raises(DeltaError, match="Unterminated string literal"):
        write_deltalake(uri, pa.table({"id": pa.array([2], pa.int64()), "p": ["d'agua"]}), mode="overwrite", predicate="p = 'd'agua'")
