"""A API do delta-rs (pacote ``deltalake``) que a biblioteca usa, numa pasta local.

Cada teste exercita uma parte: a criação idempotente a partir de um esquema, os modos de escrita e a
substituição por predicado, a evolução de esquema e o ``update`` com predicado, a viagem no tempo e
o ``restore``, as ações do log e o registro de um arquivo gravado por outro programa, o ``vacuum``
com ``keep_versions``, a leitura por dataset Arrow e o conteúdo do log. Os comportamentos estão
descritos em ``docs/delta.md``; aqui eles viram asserções.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pytest
from deltalake import CommitProperties, DeltaTable, write_deltalake
from deltalake.exceptions import DeltaError, SchemaMismatchError
from deltalake.schema import Field, PrimitiveType
from deltalake.transaction import AddAction

from conftest import LocalLocation
from delta import MONTHS, ROWS, connect_duckdb, sample_table

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
