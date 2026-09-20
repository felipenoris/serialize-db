"""A API do delta-rs (pacote ``deltalake``) que a biblioteca usa, numa pasta local.

Cada teste exercita uma parte: a criação idempotente a partir de um esquema, os modos de escrita e a
substituição por predicado, a evolução de esquema e o ``update`` com predicado, a viagem no tempo e
o ``restore``, as ações do log e o registro de um arquivo gravado por outro programa, o ``vacuum``
com ``keep_versions``, a leitura por dataset Arrow e o conteúdo do log. Os testes seguintes cobrem
as primitivas das etapas 3, 4, 7 e 9 (``docs/PLAN-STAGE-<n>.md``) sobre o DuckDB: a view presa a
uma versão e o leitor Arrow que alimenta o ``write_deltalake``, a reescrita da tabela pelo ``COPY``
particionado registrada num commit com estatísticas, a diferença de versões, a compactação e o
checkpoint, a exportação por cópia dos arquivos e a carga inicial de pastas Parquet. Os
comportamentos estão descritos em ``docs/delta.md``; aqui eles viram asserções.
"""

from __future__ import annotations

import decimal
import json
import re
import shutil
import time
from collections.abc import Callable
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pytest
from deltalake import CommitProperties, DeltaTable, Schema, write_deltalake
from deltalake.exceptions import DeltaError, SchemaMismatchError
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

    log = Path(uri) / "_delta_log" / "00000000000000000000.json"
    actions = [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines()]
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

    # export_month: o resultado de uma consulta sai em lotes e substitui o mês na tabela publicada.
    target = folder("target")
    write_deltalake(target, two_months, mode="append", partition_by=["mes"])
    reader = con.execute(f"SELECT * FROM pinned WHERE mes = '{MONTHS[1]}'").to_arrow_reader()
    write_deltalake(target, reader, mode="overwrite", predicate=f"mes = '{MONTHS[1]}'")

    published = DeltaTable(target)
    assert published.version() == 1 and published.to_pyarrow_table().num_rows == 1000
    assert MONTHS[1] in published.history()[0]["operationParameters"]["predicate"]
    con.close()


def add_actions_from_return_stats(rows: list[dict], table_uri: str, integer_columns: tuple[str, ...], decimal_columns: tuple[str, ...]) -> list[AddAction]:
    """Uma ``AddAction`` por linha de ``RETURN_STATS``: caminho relativo, tamanho, partição e estatísticas tipadas."""
    actions = []
    for row in rows:
        statistics = {name.strip('"'): values for name, values in row["column_statistics"].items()}
        minimum, maximum, nulls = {}, {}, {}
        for column, values in statistics.items():
            if column in integer_columns:
                minimum[column], maximum[column] = int(values["min"]), int(values["max"])
            elif column in decimal_columns:
                minimum[column], maximum[column] = float(values["min"]), float(values["max"])
            else:
                continue
            nulls[column] = int(values["null_count"])

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
    """A tabela inteira reescrita pelo ``COPY`` particionado do DuckDB e registrada num commit ``overwrite`` com estatísticas."""
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
    actions = add_actions_from_return_stats(rows, uri, ("id_operacao", "id_cliente"), ("valor",))
    new_schema = pa.schema([field if field.name != "descricao" else pa.field("descricao_nova", pa.string()) for field in two_months.schema])
    DeltaTable(uri).create_write_transaction(actions, mode="overwrite", schema=new_schema, partition_by=["mes"])

    rewritten = DeltaTable(uri)
    assert rewritten.version() == 1 and len(rewritten.file_uris()) == 2
    assert [field.name for field in rewritten.schema().fields][-1] == "descricao_nova"
    assert rewritten.to_pyarrow_table().num_rows == 1000

    # As estatísticas registradas deixam o DuckDB pular os arquivos: nenhum id acima do máximo.
    plan = con.execute(f"EXPLAIN ANALYZE SELECT count(*) FROM delta_scan('{uri}') WHERE id_operacao > {ROWS}").fetchone()[1]
    scanned = re.search(r"Scanning Files: (\d+)/(\d+)", plan)
    assert scanned and scanned.group(1) == "0"
    con.close()


def months_between_versions(uri: str, published: int, current: int) -> set[str]:
    """Os meses com arquivos que a versão ``current`` tem e a ``published`` não tinha: o que a publicação recarrega."""
    def paths(version: int) -> set[str]:
        return set(DeltaTable(uri, version=version).get_add_actions().column("path").to_pylist())

    return {path.split("/")[0].removeprefix("mes=") for path in paths(current) - paths(published)}


def test_version_diff(folder: Callable[[str], str], two_months: pa.Table) -> None:
    """A diferença entre duas versões, pelas ações ``add``, aponta os meses a recarregar no Redshift."""
    uri = folder("diff")
    february = two_months.filter(pc.field("mes") == MONTHS[1])

    write_deltalake(uri, two_months, mode="append", partition_by=["mes"])  # versão 0
    write_deltalake(uri, february, mode="overwrite", predicate=f"mes = '{MONTHS[1]}'")  # versão 1
    write_deltalake(uri, two_months.slice(0, 10), mode="append")  # versão 2: dez linhas de janeiro

    assert months_between_versions(uri, 0, 1) == {MONTHS[1]}
    assert months_between_versions(uri, 1, 2) == {MONTHS[0]}
    assert months_between_versions(uri, 0, 2) == set(MONTHS)
    assert months_between_versions(uri, 2, 2) == set()


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
    """A carga inicial: cada pasta ``mes=.../`` de Parquet entra por mês, com o cast para o contrato, e recomeça de onde parou."""
    # A origem tem valor em DOUBLE, como os modelos atuais; o contrato pede DECIMAL(18, 2).
    source_table = two_months.set_column(two_months.schema.get_field_index("valor"), "valor", two_months.column("valor").cast(pa.float64()))
    source = Path(folder("source_parquet"))
    pq.write_to_dataset(source_table, source, partition_cols=["mes"])

    uri = folder("loaded")
    Schema.from_arrow(two_months.schema)  # o esquema Delta derivado do Arrow, o mesmo que create_table usa
    DeltaTable.create(uri, two_months.schema, partition_by=["mes"])
    con = connect_duckdb(("delta",))

    def load_missing_months() -> list[str]:
        loaded = set(DeltaTable(uri).get_add_actions(flatten=True).column("partition.mes").to_pylist())
        pending = [month for month in MONTHS if month not in loaded]
        for month in pending:
            reader = con.execute(
                f"SELECT id_operacao, mes, data_ref, id_cliente, valor::DECIMAL(18, 2) AS valor, descricao "
                f"FROM read_parquet('{source}/mes={month}/*.parquet', hive_partitioning = true)"
            ).to_arrow_reader()
            write_deltalake(uri, reader, mode="overwrite", predicate=f"mes = '{month}'")
        return pending

    assert load_missing_months() == list(MONTHS)
    assert load_missing_months() == []  # a segunda passagem não tem o que carregar

    # O relatório: contagem e soma por mês iguais entre a origem e o Delta.
    report = f"SELECT mes, count(*), sum(valor::DECIMAL(18, 2)) FROM read_parquet('{source}/*/*.parquet', hive_partitioning = true) GROUP BY mes ORDER BY mes"
    loaded = f"SELECT mes, count(*), sum(valor) FROM delta_scan('{uri}') GROUP BY mes ORDER BY mes"
    assert con.execute(report).fetchall() == con.execute(loaded).fetchall()
    assert pa.schema(DeltaTable(uri).schema()).field("valor").type == pa.decimal128(18, 2)
    con.close()
