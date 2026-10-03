"""Uma execução completa do pipeline mensal sobre a base de testes em Delta, no motor DuckDB.

A condição inicial é a base fictícia de ``tests/source_db_projetado.py`` carregada no Delta por
``serialize_db.parquet_import.import_table``, as 12 tabelas do modelo cliente
(``tests/client_model/``) com as quatro partições de cada tabela particionada; ela é gravada uma
vez por módulo sob ``SERIALIZE_DB_TEST_LOCAL_ROOT`` (marcador ``local``), e cada teste trabalha
numa cópia própria, para publicar a partição nova sem tocar a base dos outros.

``monthly_pipeline`` é o pipeline, no formato ``modulo:funcao`` que ``serialize-db run`` recebe,
e os comentários dele trazem as recomendações de uso do pacote. A execução roda na partição do
mês seguinte à última data-base publicada. O sistema de origem entrega os contratos do mês em um
CSV por sistema em ``<ambiente>/recebidos/<partição>/``; o pipeline os copia para a pasta da
execução, ``<ambiente>/execucoes/<execution_id>/entradas/``, e lê a cópia. A ingestão traz ao
sandbox só o que o pipeline lê: ``cad_contas`` e a última data-base das tabelas que a levam
adiante, materializada; ``cad_contratos`` nasce vazia. A partição nova de ``cad_contratos`` vem da
cópia; as de ``cad_operacoes`` e ``rel_contrato_operacao`` vêm da última por
``INSERT ... SELECT`` no sandbox; a de ``cad_lancamentos`` é gerada em pyarrow a partir da última
e acrescentada por ``run.sandbox.append``; a chave de toda linha nova vem de ``next_ids``. Os
saldos por conta e o rateio de cada contrato na partição nova vão a ``relatorios/`` na pasta da
execução, antes da auditoria com as chaves estrangeiras e da publicação no Delta. Na falha, a
partição gerada de cada tabela vai a ``geracao/`` na pasta da execução, e a exceção sobe. A pasta
da execução é código cliente: o pacote não a cria nem a lê, e o ``execution_id`` no commit de
cada tabela leva a ela.

Os testes conferem os saldos e as contagens contra a base fictícia em memória, o rateio de cada
contrato, as auditorias aprovadas com toda verificação rodada, a versão nova de cada tabela só com
a partição nova alterada, os metadados do commit, a pasta da execução e a releitura pelo leitor
Delta; a execução do mês seguinte sobre a partição publicada, pela linha de comando e pela API; a
reexecução do mês publicado; e o contrato repetido na entrega, que a auditoria reprova sem tocar o
Delta, com a partição gerada na pasta da execução. O motor ``"duckdb"`` da execução e o leitor
nascem na pasta temporária do processo, apontada para a pasta do teste. A extensão ``delta`` do
DuckDB precisa estar na pasta de extensões.
"""

from __future__ import annotations

import dataclasses
import datetime
import io
import json
import shutil
import tempfile
import uuid
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pa_csv
import pyarrow.parquet as pq
import pytest
import sqlalchemy as sa

import source_db_projetado as source
from client_model import Base, Conta, Contrato, Lancamento, Operacao, RelContratoOperacao
from client_model.statements import STATEMENTS
from conftest import LocalLocation
from serialize_db import cli, delta, parquet_import
from serialize_db.audit import AuditReport
from serialize_db.engine.duckdb import DuckDBConfig
from serialize_db.errors import AuditFailed
from serialize_db.execution import Database, Execution
from serialize_db.reader import DeltaReader
from serialize_db.schema import arrow_schema, table_options

pytestmark = pytest.mark.local

ACCOUNTS = Conta.__table__
OPERATIONS = Operacao.__table__
APPORTIONMENTS = RelContratoOperacao.__table__
CONTRACTS = Contrato.__table__
ENTRIES = Lancamento.__table__
# As tabelas que o pipeline grava, na ordem das chaves estrangeiras: a referenciada antes da que a
# referencia.
PRODUCED = (OPERATIONS, CONTRACTS, APPORTIONMENTS, ENTRIES)
# As tabelas cuja partição nova nasce da última data-base, no sandbox; a de cad_contratos vem da
# entrega.
CARRIED = (OPERATIONS, APPORTIONMENTS, ENTRIES)

LAST_BASE_DATE = source.PARTITION_VALUES[-1]
NEXT_BASE_DATE = "2026-07-31"
FOLLOWING_BASE_DATE = "2026-08-31"
EXECUTION_ID = "exec-2026-07-31"
# O carimbo dos lançamentos gerados: o instante da execução mensal.
WRITTEN_AT = datetime.datetime(2026, 8, 3, 9, 15, 0)

# A partição de referência do join do rateio, o parâmetro que a execução preenche.
REFERENCE = sa.bindparam("data_str", type_=sa.String(10))

# O rateio de cada contrato entre as operações dele na partição de referência: contratos, a
# relação e operações; fator_rateio soma 1 por contrato.
APPORTIONMENT_BY_CONTRACT = (
    sa.select(
        CONTRACTS.c.contrato,
        sa.func.count(OPERATIONS.c.id_operacao).label("operacoes"),
        sa.func.sum(APPORTIONMENTS.c.fator_rateio).label("rateio"),
    )
    .join_from(
        CONTRACTS,
        APPORTIONMENTS,
        sa.and_(
            APPORTIONMENTS.c.data == CONTRACTS.c.data,
            APPORTIONMENTS.c.sistema == CONTRACTS.c.sistema,
            APPORTIONMENTS.c.contrato == CONTRACTS.c.contrato,
        ),
    )
    .join(
        OPERATIONS,
        sa.and_(
            OPERATIONS.c.data == APPORTIONMENTS.c.data,
            OPERATIONS.c.operacao == APPORTIONMENTS.c.operacao,
        ),
    )
    .where(CONTRACTS.c.data_str == REFERENCE)
    .group_by(CONTRACTS.c.contrato)
    .order_by(CONTRACTS.c.contrato)
)


# ---------------------------------------------------------------- a pasta da execução

# A pasta da execução é do código cliente: o pacote não a cria nem a lê. Ela fica em
# <raiz>/<ambiente>/execucoes/<execution_id>/, ao lado das tabelas e das pastas que o pacote grava
# sob o ambiente (_serialize_db/, staging/, publicacao/, arquivo/), e por isso nenhuma tabela do
# modelo pode se chamar execucoes nem recebidos. Cada etapa tem uma subpasta: entradas/ com a cópia
# da entrega, relatorios/ com os resultados pequenos de toda execução, e geracao/ com a partição
# gerada, só na falha. No S3, uma regra de ciclo de vida no prefixo <ambiente>/execucoes/ apaga as
# pastas antigas.


def execution_folder(
    delta_db: Database,
    execution_id: str,
) -> str:
    """A pasta da execução, ``<ambiente>/execucoes/<execution_id>``, relativa à raiz do banco."""
    return delta_db.storage.join(delta_db.environment, "execucoes", execution_id)


def delivery_folder(
    delta_db: Database,
    partition: str,
) -> str:
    """A pasta onde o sistema de origem entrega os arquivos da partição,
    ``<ambiente>/recebidos/<partição>``, relativa à raiz do banco."""
    return delta_db.storage.join(delta_db.environment, "recebidos", partition)


def keep_inputs(
    run: Execution,
) -> list[str]:
    """Copia os arquivos entregues para a partição da execução em ``entradas/``, na pasta da
    execução, e devolve os caminhos das cópias, relativos à raiz do banco."""
    storage = run.delta_db.storage
    delivery = delivery_folder(run.delta_db, run.partition)
    delivered = storage.list_files(delivery)
    if not delivered:
        raise FileNotFoundError(f"nenhum arquivo entregue em {storage.uri_of(delivery)}")
    target = storage.join(execution_folder(run.delta_db, run.execution_id), "entradas")
    kept = []
    for path in delivered:
        copy = storage.join(target, path.removeprefix(delivery + "/"))
        # Storage.copy copia dentro da raiz do banco; no S3, sem os dados passarem pela máquina.
        # Uma entrega fora da raiz, como em outro bucket, pede o boto3, ou a leitura por
        # Storage.for_uri(<origem>).open_input_file e a escrita por open_output_stream.
        storage.copy(path, copy)
        kept.append(copy)
    return kept


def write_intermediate(
    run: Execution,
    stage: str,
    name: str,
    result: pa.Table,
) -> None:
    """Grava um resultado da execução em ``<etapa>/<nome>.parquet``, na pasta da execução."""
    storage = run.delta_db.storage
    folder = execution_folder(run.delta_db, run.execution_id)
    with storage.open_output_stream(storage.join(folder, stage, f"{name}.parquet")) as sink:
        pq.write_table(result, sink)


def write_partition(
    run: Execution,
    table: sa.Table,
    path: str,
) -> None:
    """Grava a partição da execução da tabela do sandbox num Parquet, lote a lote."""
    partition_by = table_options(table).partition_by
    statement = sa.select(table).where(table.c[partition_by] == run.partition)
    # stream entrega os lotes enquanto a consulta roda, e o ParquetWriter os grava um a um: a
    # partição inteira nunca está na memória do Python.
    with (
        run.sandbox.stream(statement) as stream,
        run.delta_db.storage.open_output_stream(path) as sink,
        pq.ParquetWriter(sink, stream.schema) as writer,
    ):
        for batch in stream:
            writer.write_batch(batch)


def keep_generated(
    run: Execution,
) -> None:
    """Grava a partição nova de cada tabela gerada, como está no sandbox, em
    ``geracao/<tabela>.parquet``, na pasta da execução."""
    storage = run.delta_db.storage
    folder = execution_folder(run.delta_db, run.execution_id)
    for table in PRODUCED:
        write_partition(run, table, storage.join(folder, "geracao", f"{table.name}.parquet"))


# ---------------------------------------------------------------- o pipeline mensal


@dataclasses.dataclass
class MonthlyReport:
    """O que a execução mensal leu e gravou, para as asserções dos testes."""

    previous: str
    """A última data-base publicada antes da partição da execução."""
    balances: pa.Table
    """Os saldos por conta na data-base nova, o resultado de ``saldos_por_conta``."""
    produced: dict[str, int]
    """As linhas geradas na partição nova, por tabela."""
    apportionment: pa.Table
    """O rateio por contrato na partição nova, o resultado de ``APPORTIONMENT_BY_CONTRACT``."""
    audits: dict[str, AuditReport]
    """O relatório aprovado de cada tabela gravada."""
    versions: dict[str, int]
    """A versão publicada de cada tabela gravada."""


def last_base_date(
    run: Execution,
) -> str:
    """A última data-base publicada antes da partição da execução."""
    # previous_partitions inclui a partição da execução quando ela já está publicada, como na
    # reexecução de um mês; das duas últimas, vale a anterior a ela.
    earlier = []
    for value in run.previous_partitions(ENTRIES, 2):
        if value < run.partition:
            earlier.append(value)
    return earlier[-1]


def ingest_model(
    run: Execution,
    previous: str,
) -> None:
    """Traz ao sandbox o que o pipeline lê: ``cad_contas`` inteira, como view, e as tabelas que
    levam a última data-base adiante só nela, materializadas; ``cad_contratos``, cuja partição nova
    vem inteira da entrega, nasce vazia, pelo DDL do modelo."""
    # Só entra no sandbox o que o pipeline lê, e das tabelas particionadas só as partições lidas: a
    # base inteira pode não caber na máquina. A auditoria com foreign_keys=True acha a tabela que
    # uma chave estrangeira aponta na versão fixada dela, sem ingestão.
    run.ingest(ACCOUNTS)
    # No DuckDB, a view lê o Delta a cada consulta, sem cópia, e a tabela materializada recebe as
    # linhas que o INSERT ... SELECT e o append gravam; no Redshift, a ingestão sempre carrega a
    # tabela. As tabelas de uma chamada entram em paralelo, cada uma numa sessão do motor.
    run.ingest(*CARRIED, partitions=[previous], materialize=True)
    run.sandbox.create_table(CONTRACTS)


def count_rows(
    run: Execution,
    table: sa.Table,
    condition: sa.ColumnElement,
) -> int:
    """Quantas linhas da tabela do sandbox satisfazem ``condition``."""
    statement = sa.select(sa.func.count()).select_from(table).where(condition)
    return run.sandbox.query(statement).column(0)[0].as_py()


def carry_forward(
    run: Execution,
    table: sa.Table,
    previous: str,
    current: str,
) -> int:
    """Leva as linhas da última data-base à nova, num ``INSERT ... SELECT`` no sandbox.

    A chave sequencial recebe os ids de ``next_ids``, a coluna de origem da partição recebe a nova
    data-base e a coluna de partição o valor dela; as demais colunas seguem iguais. Devolve quantas
    linhas entraram.
    """
    options = table_options(table)
    (key,) = table.primary_key.columns
    in_previous = table.c[options.partition_by] == previous
    count = count_rows(run, table, in_previous)
    # next_ids dá uma faixa contígua acima do maior id da versão fixada. Com os ids novos acima do
    # maior, a auditoria dispensa a junção da chave com a tabela publicada.
    ids = run.next_ids(table, count)

    # A n-ésima linha, na ordem da chave antiga, recebe o n-ésimo id da faixa.
    new_key = sa.func.row_number().over(order_by=key) + (ids.start - 1)
    replacements = {
        key.name: new_key,
        options.partition_source: sa.literal(datetime.date.fromisoformat(current)),
        options.partition_by: sa.literal(current),
    }
    selected = []
    for column in table.columns:
        selected.append(replacements.get(column.name, column).label(column.name))
    names = [column.name for column in table.columns]
    statement = sa.insert(table).from_select(names, sa.select(*selected).where(in_previous))
    run.sandbox.query(statement)
    return count


def read_contracts(
    run: Execution,
    inputs: list[str],
) -> pa.Table:
    """Os contratos dos arquivos copiados para a pasta da execução, com os tipos do contrato."""
    storage = run.delta_db.storage
    # Os tipos vêm do contrato. A inferência do pyarrow decide por arquivo, e num arquivo em que a
    # coluna "to" só tenha dígitos o código "01" viraria o inteiro 1. O write_csv do pyarrow grava
    # o nulo como campo vazio e o texto vazio como "", e as duas opções de texto os leem de volta
    # como nulo e como texto vazio; sem elas, os dois viram texto vazio, e só com a primeira, nulo.
    types = {}
    for field in arrow_schema(CONTRACTS):
        types[field.name] = field.type
    options = pa_csv.ConvertOptions(
        column_types=types,
        strings_can_be_null=True,
        quoted_strings_can_be_null=False,
    )
    tables = []
    for path in inputs:
        with storage.open_input_file(path) as source_file:
            tables.append(pa_csv.read_csv(source_file, convert_options=options))
    return pa.concat_tables(tables)


def new_contracts(
    run: Execution,
    contracts: pa.Table,
) -> pa.Table:
    """Os contratos entregues com as colunas que o pipeline preenche: a chave, com os ids de
    ``next_ids``, a data-base da execução e a coluna de partição."""
    count = contracts.num_rows
    new_month = datetime.date.fromisoformat(run.partition)
    ids = run.next_ids(CONTRACTS, count)
    return (
        contracts.append_column("id_contrato", pa.array(ids, pa.int64()))
        .append_column("data", pa.array([new_month] * count, pa.date32()))
        .append_column("data_str", pa.array([run.partition] * count, pa.string()))
    )


def previous_entries(
    run: Execution,
    previous: str,
) -> pa.Table:
    """Os lançamentos da última data-base, lidos da tabela ingerida no sandbox, na ordem da
    chave."""
    statement = (
        sa.select(ENTRIES)
        .where(ENTRIES.c.data_base_str == previous)
        .order_by(ENTRIES.c.id_lancamento)
    )
    return run.sandbox.query(statement)


def next_month_entries(
    entries: pa.Table,
    current: str,
    ids: range,
) -> pa.Table:
    """A partição nova dos lançamentos, gerada em pyarrow: a chave recebe os ids da faixa, a
    data-base e a coluna de partição recebem a nova, o carimbo é o desta execução, e as demais
    colunas seguem as dos lançamentos dados."""
    count = entries.num_rows
    new_month = datetime.date.fromisoformat(current)
    replaced = {
        "id_lancamento": pa.array(ids, pa.int64()),
        "data_base": pa.array([new_month] * count, pa.date32()),
        "data_base_str": pa.array([current] * count, pa.string()),
        "timestamp": pa.array([WRITTEN_AT] * count, pa.timestamp("us")),
    }
    columns = {}
    for name in entries.column_names:
        columns[name] = replaced.get(name, entries[name])
    return pa.table(columns)


def produce_next_month(
    run: Execution,
    previous: str,
    inputs: list[str],
) -> dict[str, int]:
    """Gera a partição nova das quatro tabelas: os contratos da cópia da entrega, acrescentados por
    ``run.sandbox.append``; as operações e a relação delas com os contratos como estão na última
    data-base, por ``INSERT ... SELECT`` no sandbox; os lançamentos em pyarrow, só os dos meses
    posteriores à nova data-base, acrescentados à tabela ingerida. Devolve as linhas geradas por
    tabela."""
    current = run.partition
    produced = {}
    # O append passa cada lote pelo cast do contrato antes de inserir: as colunas vão à ordem do
    # modelo, e um nulo em coluna NOT NULL ou um texto acima do String(n) é ContractError, sem
    # linha alguma inserida.
    contracts = new_contracts(run, read_contracts(run, inputs))
    produced[CONTRACTS.name] = run.sandbox.append(CONTRACTS, contracts)
    # A transformação que cabe em SQL roda no motor, sem trazer as linhas ao Python.
    for table in (OPERATIONS, APPORTIONMENTS):
        produced[table.name] = carry_forward(run, table, previous, current)
    # A que pede Python lê por query, que traz o resultado inteiro à memória; numa partição grande,
    # stream entrega os lotes enquanto a consulta roda, e o append aceita um iterável de lotes.
    entries = previous_entries(run, previous)
    new_month = datetime.date.fromisoformat(current)
    still_ahead = entries.filter(pc.greater(entries["data"], pa.scalar(new_month)))
    ids = run.next_ids(ENTRIES, still_ahead.num_rows)
    new_entries = next_month_entries(still_ahead, current, ids)
    produced[ENTRIES.name] = run.sandbox.append(ENTRIES, new_entries)
    return produced


def generate_and_publish(
    run: Execution,
    previous: str,
    inputs: list[str],
) -> MonthlyReport:
    """A partição nova das quatro tabelas, os relatórios dela na pasta da execução, a auditoria e
    a publicação no Delta."""
    current = run.partition
    produced = produce_next_month(run, previous, inputs)
    # Os parâmetros vão por bindparam, com os valores na chamada: o mesmo statement roda nos dois
    # motores, sem SQL montado em texto.
    balances = run.sandbox.query(STATEMENTS["saldos_por_conta"], {"data_base_str": current})
    apportionment = run.sandbox.query(APPORTIONMENT_BY_CONTRACT, {"data_str": current})
    # Os resultados pequenos vão à pasta da execução sempre, antes da auditoria, que pode
    # reprovar.
    write_intermediate(run, "relatorios", "saldos_por_conta", balances)
    write_intermediate(run, "relatorios", "rateio_por_contrato", apportionment)
    # publish_delta exige a auditoria aprovada das mesmas partições na mesma execução.
    audits = {}
    for table in PRODUCED:
        audits[table.name] = run.audit(table, [current], foreign_keys=True)
    # Um commit por tabela, com o execution_id e as versões lidas nos metadados. A partição é
    # substituída inteira, e a reexecução do mesmo mês troca a partição publicada.
    versions = run.publish_delta(*PRODUCED, partitions=[current])
    return MonthlyReport(previous, balances, produced, apportionment, audits, versions)


def monthly_pipeline(
    run: Execution,
) -> MonthlyReport:
    """O pipeline mensal sobre a partição da execução: a cópia da entrega para a pasta da
    execução, a ingestão do que ele lê e ``generate_and_publish``; na falha depois da ingestão, a
    partição gerada de cada tabela vai à pasta da execução, e a exceção sobe.

    É a função ``modulo:funcao`` de ``serialize-db run``, que ignora o relatório devolvido.
    """
    # A cópia vem antes de tudo, e o pipeline lê só a cópia: a pasta da execução guarda o que ela
    # leu, mesmo que a origem regrave a entrega depois.
    inputs = keep_inputs(run)
    previous = last_base_date(run)
    ingest_model(run, previous)
    try:
        return generate_and_publish(run, previous, inputs)
    except Exception:
        # A partição gerada, grande, vai à pasta da execução só na falha: no sucesso ela está no
        # Delta, e o sandbox que a guarda some na saída do with.
        keep_generated(run)
        raise


# ---------------------------------------------------------------- a base de testes em Delta


@pytest.fixture(scope="module")
def delta_base(
    local_location: LocalLocation,
) -> Path:
    """A base de testes na versão Delta, a condição inicial de todo teste: a base Parquet fictícia
    carregada uma vez por módulo, tabela a tabela, na ordem da carga."""
    folder = Path(local_location.child("pipeline/base"))
    parquet = source.write_source(folder / "parquet")
    root = folder / "delta"
    db = Database(str(root), "prd", Base.metadata)
    config = DuckDBConfig(temp_directory=str(folder / "carga"))
    for table in parquet_import.import_order(db.tables()):
        parquet_import.import_table(db, table, str(parquet.root), config=config)
    return root


@pytest.fixture
def folder(
    local_location: LocalLocation,
) -> Path:
    """Uma pasta nova por teste sob a raiz da sessão."""
    path = Path(local_location.child(f"pipeline/{uuid.uuid4().hex[:8]}"))
    path.mkdir(parents=True)
    return path


@pytest.fixture
def db(
    delta_base: Path,
    folder: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Database:
    """O banco do teste: uma cópia própria da base Delta, onde a partição nova é publicada; a
    pasta temporária do processo, onde o motor ``"duckdb"`` da execução e o leitor Delta nascem,
    aponta para a pasta do teste."""
    shutil.copytree(delta_base, folder / "delta")
    monkeypatch.setattr(tempfile, "tempdir", str(folder))
    return Database(str(folder / "delta"), "prd", Base.metadata)


# ---------------------------------------------------------------- a entrega e a pasta da execução

# Os relatórios que toda execução grava na pasta dela, antes da auditoria.
REPORT_FILES = ["relatorios/rateio_por_contrato.parquet", "relatorios/saldos_por_conta.parquet"]


def monthly_contracts(
    tables: dict,
) -> pa.Table:
    """Os contratos que o sistema de origem entrega a cada mês: os da última data-base da base em
    memória, sem as colunas que o pipeline preenche."""
    return tables[CONTRACTS.name][LAST_BASE_DATE].drop_columns(["id_contrato", "data"])


def with_empty_text(
    contracts: pa.Table,
) -> pa.Table:
    """Os contratos com ``fonte_familia`` vazia, e não nula, no primeiro, o texto vazio que a
    leitura do CSV separa do nulo."""
    position = contracts.schema.get_field_index("fonte_familia")
    values = contracts["fonte_familia"].to_pylist()
    values[0] = ""
    return contracts.set_column(position, "fonte_familia", pa.array(values, pa.string()))


def deliver_contracts(
    db: Database,
    partition: str,
    contracts: pa.Table,
) -> dict[str, bytes]:
    """Grava os contratos na pasta de entrega da partição, um CSV por sistema, como o sistema de
    origem faria, e devolve o conteúdo de cada arquivo pelo nome."""
    delivered = {}
    for system in sorted(set(contracts["sistema"].to_pylist())):
        sink = io.BytesIO()
        pa_csv.write_csv(contracts.filter(pc.equal(contracts["sistema"], system)), sink)
        delivered[f"contratos_sistema_{system}.csv"] = sink.getvalue()
    folder = delivery_folder(db, partition)
    for name, content in delivered.items():
        with db.storage.open_output_stream(db.storage.join(folder, name)) as file:
            file.write(content)
    return delivered


def execution_files(
    db: Database,
    execution_id: str,
) -> list[str]:
    """Os arquivos da pasta da execução, relativos a ela, em ordem."""
    folder = execution_folder(db, execution_id)
    files = []
    for path in db.storage.list_files(folder):
        files.append(path.removeprefix(folder + "/"))
    return files


def kept_inputs(
    db: Database,
    execution_id: str,
) -> dict[str, bytes]:
    """O conteúdo de cada arquivo de ``entradas/`` na pasta da execução, pelo nome."""
    folder = db.storage.join(execution_folder(db, execution_id), "entradas")
    kept = {}
    for path in db.storage.list_files(folder):
        with db.storage.open_input_file(path) as file:
            kept[path.removeprefix(folder + "/")] = file.read()
    return kept


def read_intermediate(
    db: Database,
    execution_id: str,
    path: str,
) -> pa.Table:
    """Um Parquet da pasta da execução, pelo caminho relativo a ela."""
    full = db.storage.join(execution_folder(db, execution_id), path)
    with db.storage.open_input_file(full) as file:
        return pq.read_table(file)


# ---------------------------------------------------------------- o esperado, da base em memória


def source_tables() -> dict[str, pa.Table | dict[str, pa.Table]]:
    """A base fictícia em memória, a mesma que a carga gravou."""
    return source.build_tables()


def carried_entries(
    tables: dict,
    base_date: str,
    current: str,
) -> list[dict[str, object]]:
    """Os lançamentos da data-base ``base_date`` que a partição ``current`` recebe: os que projetam
    um mês posterior a ``current``."""
    month = datetime.date.fromisoformat(current)
    carried = []
    for row in tables["cad_lancamentos"][base_date].to_pylist():
        if row["data"] > month:
            carried.append(row)
    return carried


def expected_balances(
    tables: dict,
    entries: list[dict[str, object]],
) -> list[dict[str, object]]:
    """Os saldos por conta de ``saldos_por_conta`` calculados sobre esses lançamentos da base em
    memória: a soma de ``valor`` por conta que permite lançamentos, na ordem do número."""
    accounts = {row["id_conta"]: row for row in tables["cad_contas"].to_pylist()}
    totals: dict[tuple[str, str], float] = {}
    for row in entries:
        account = accounts[row["id_conta"]]
        if not account["permite_lancamentos"]:
            continue
        key = (account["numero"], account["nome"])
        totals[key] = totals.get(key, 0.0) + row["valor"]
    balances = []
    for (numero, nome), saldo in sorted(totals.items()):
        balances.append({"numero": numero, "nome": nome, "saldo": saldo})
    return balances


def expected_produced(
    tables: dict,
    current: str,
) -> dict[str, int]:
    """As linhas que a partição ``current`` recebe de cada tabela: as da última data-base, e nos
    lançamentos só as dos meses posteriores a ``current``."""
    produced = {}
    for table in (OPERATIONS, CONTRACTS, APPORTIONMENTS):
        produced[table.name] = tables[table.name][LAST_BASE_DATE].num_rows
    produced[ENTRIES.name] = len(carried_entries(tables, LAST_BASE_DATE, current))
    return produced


def partition_counts(
    reader: DeltaReader,
    table: sa.Table,
) -> dict[str, int]:
    """As linhas de cada partição da tabela, pelo leitor Delta, em ordem de texto do valor."""
    partition_by = table_options(table).partition_by
    column = table.c[partition_by]
    statement = sa.select(column, sa.func.count().label("linhas")).group_by(column).order_by(column)
    rows = reader.query(statement).to_pylist()
    return {row[partition_by]: row["linhas"] for row in rows}


def assert_balances_match(
    found: pa.Table,
    expected: list[dict[str, object]],
) -> None:
    """As mesmas contas na mesma ordem, e cada saldo igual ao da base em memória, a menos da ordem
    da soma em ponto flutuante."""
    rows = found.to_pylist()
    found_accounts = [(row["numero"], row["nome"]) for row in rows]
    wanted_accounts = [(row["numero"], row["nome"]) for row in expected]
    assert found_accounts == wanted_accounts
    for row, wanted in zip(rows, expected, strict=True):
        assert row["saldo"] == pytest.approx(wanted["saldo"], rel=1e-12, abs=1e-3), row


def assert_apportionment_sums_to_one(
    found: pa.Table,
    contracts: int,
) -> None:
    """Todo contrato da partição nova está em uma ou duas operações, e os fatores dele somam
    exatamente 1."""
    rows = found.to_pylist()
    assert len(rows) == contracts
    for row in rows:
        assert row["operacoes"] in (1, 2), row
        assert row["rateio"] == 1.0, row


def assert_audits_ran_every_check(
    audits: dict[str, AuditReport],
    current: str,
) -> None:
    """Cada relatório aprovado cobre só a partição nova, sem verificação por rodar; as chaves
    estrangeiras compostas de ``rel_contrato_operacao`` e ``cad_lancamentos`` rodaram, e a chave
    sequencial foi dispensada da junção com a versão fixada, porque os ids novos passam do maior
    dela."""
    assert set(audits) == {table.name for table in PRODUCED}
    for report in audits.values():
        assert report.passed, report.results
        assert report.partitions == (current,)
        assert report.not_run == ()
    checks = {name: [result.name for result in report.results] for name, report in audits.items()}
    assert "orfao_data_operacao" in checks[APPORTIONMENTS.name]
    assert "orfao_data_base_sistema_contrato" in checks[ENTRIES.name]
    assert "orfao_id_conta" in checks[ENTRIES.name]
    for report in audits.values():
        against_table = []
        for result in report.results:
            if result.name.endswith("_tabela"):
                against_table.append(result)
        (table_check,) = against_table
        assert table_check.reason.startswith("dispensada"), table_check


# ---------------------------------------------------------------- a execução


def test_monthly_pipeline_publishes_the_next_base_date(
    db: Database,
) -> None:
    """A execução de 2026-07-31 lê 2026-06-30 como a última data-base, gera a partição nova com as
    contagens esperadas, os saldos por conta da base em memória e o rateio somando 1 por
    contrato, aprova as quatro auditorias com toda verificação rodada e publica uma versão a mais
    em cada tabela, só com a partição nova alterada, com o ``execution_id`` e as versões lidas no
    commit; a pasta da execução guarda a cópia da entrega e os relatórios; o leitor Delta lê as
    partições antigas intactas e a nova, com os contratos entregues, o texto vazio de um deles
    incluído."""
    tables = source_tables()
    contracts = with_empty_text(monthly_contracts(tables))
    delivered = deliver_contracts(db, NEXT_BASE_DATE, contracts)
    # O id fixo é do teste, que acha a pasta pelo nome. Fora dos testes, deixe o pacote gerar um
    # por execução: a reexecução com o mesmo id regravaria a pasta da anterior, e no motor Redshift
    # duas execuções com o mesmo id disputariam as tabelas exec_<id>_* do sandbox.
    with Execution(db, "duckdb", NEXT_BASE_DATE, execution_id=EXECUTION_ID) as run:
        pinned = dict(run.versions)
        report = monthly_pipeline(run)

    # O que a execução leu e gerou.
    assert report.previous == LAST_BASE_DATE
    carried = carried_entries(tables, LAST_BASE_DATE, NEXT_BASE_DATE)
    assert_balances_match(report.balances, expected_balances(tables, carried))
    assert report.produced == expected_produced(tables, NEXT_BASE_DATE)
    assert_apportionment_sums_to_one(report.apportionment, report.produced[CONTRACTS.name])
    assert_audits_ran_every_check(report.audits, NEXT_BASE_DATE)

    # Uma versão a mais em cada tabela gravada, só com a partição nova alterada, e os metadados
    # do commit.
    assert report.versions == {table.name: pinned[table.name] + 1 for table in PRODUCED}
    for table in PRODUCED:
        uri = db.uri(table)
        partition_by = table_options(table).partition_by
        current = delta.open_table(uri, db.storage)
        assert current.version() == report.versions[table.name]
        values = delta.partition_values(current, partition_by)
        assert values == [*source.PARTITION_VALUES, NEXT_BASE_DATE], table.name
        changed = delta.version_diff(uri, pinned[table.name], current.version(), table, db.storage)
        assert changed == {NEXT_BASE_DATE}, table.name
        commit = delta.history(uri, db.storage)[0]
        assert commit["serialize_db_execution_id"] == EXECUTION_ID
        assert json.loads(commit["serialize_db_input_versions"]) == pinned
        assert "serialize_db_snapshot" not in commit

    # A pasta da execução: a cópia da entrega, igual ao que chegou, e os relatórios, iguais aos do
    # pipeline; a partição gerada fica de fora, porque a execução não falhou.
    inputs = [f"entradas/{name}" for name in delivered]
    assert execution_files(db, EXECUTION_ID) == [*inputs, *REPORT_FILES]
    assert kept_inputs(db, EXECUTION_ID) == delivered
    balances = read_intermediate(db, EXECUTION_ID, "relatorios/saldos_por_conta.parquet")
    assert balances.equals(report.balances)
    apportionment = read_intermediate(db, EXECUTION_ID, "relatorios/rateio_por_contrato.parquet")
    assert apportionment.equals(report.apportionment)

    # O leitor Delta: as partições antigas com as linhas da carga e a nova com as geradas; os
    # contratos novos iguais aos entregues; e os lançamentos novos com a data-base nova, o carimbo
    # da execução, os ids acima do maior da base e os meses projetados todos posteriores à
    # data-base.
    new_contracts_statement = (
        sa.select(CONTRACTS)
        .where(CONTRACTS.c.data_str == NEXT_BASE_DATE)
        .order_by(CONTRACTS.c.sistema, CONTRACTS.c.contrato)
    )
    new_entries = sa.select(
        ENTRIES.c.id_lancamento, ENTRIES.c.data_base, ENTRIES.c.data, ENTRIES.c.timestamp
    ).where(ENTRIES.c.data_base_str == NEXT_BASE_DATE)
    with db.open_delta(channel=delta.CURRENT_CHANNEL) as reader:
        for table in PRODUCED:
            counts = partition_counts(reader, table)
            for value in source.PARTITION_VALUES:
                assert counts[value] == tables[table.name][value].num_rows, (table.name, value)
            assert counts[NEXT_BASE_DATE] == report.produced[table.name], table.name
        published_contracts = reader.query(new_contracts_statement)
        entries = reader.query(new_entries).to_pylist()
    filled = ["id_contrato", "data", "data_str"]
    by_contract = [("sistema", "ascending"), ("contrato", "ascending")]
    found_contracts = published_contracts.drop_columns(filled).to_pylist()
    assert found_contracts == contracts.sort_by(by_contract).to_pylist()
    new_month = datetime.date.fromisoformat(NEXT_BASE_DATE)
    assert set(published_contracts["data"].to_pylist()) == {new_month}
    assert len(entries) == report.produced[ENTRIES.name]
    assert min(row["id_lancamento"] for row in entries) > source.MAX_ENTRY_ID
    assert {row["data_base"] for row in entries} == {new_month}
    assert {row["timestamp"] for row in entries} == {WRITTEN_AT}
    assert all(row["data"] > new_month for row in entries)


def test_following_month_runs_over_the_published_partition(
    db: Database,
    capsys: pytest.CaptureFixture,
) -> None:
    """``serialize-db run`` roda o mesmo pipeline em 2026-07-31 e sai com 0, com o
    ``execution_id`` que o pacote gerou no commit de cada tabela e a pasta dele com a cópia da
    entrega; a execução seguinte, em 2026-08-31, lê a partição publicada como a última data-base,
    gera a dela com um mês projetado a menos nos lançamentos e publica mais uma versão em cada
    tabela."""
    tables = source_tables()
    contracts = monthly_contracts(tables)
    delivered = deliver_contracts(db, NEXT_BASE_DATE, contracts)
    arguments = [
        "run",
        "--root",
        db.root,
        "--environment",
        "prd",
        "--partition",
        NEXT_BASE_DATE,
        "--metadata",
        "client_model:Base.metadata",
        "test_pipeline:monthly_pipeline",
    ]
    assert cli.main(arguments) == 0
    assert "Traceback" not in capsys.readouterr().err

    # O execution_id do commit de cada tabela é um só e nomeia a pasta da execução: do commit se
    # chega à cópia do que a execução leu.
    execution_ids = set()
    for table in PRODUCED:
        commit = delta.history(db.uri(table), db.storage)[0]
        execution_ids.add(commit["serialize_db_execution_id"])
    (generated_id,) = execution_ids
    assert generated_id.startswith("exec-")
    assert kept_inputs(db, generated_id) == delivered

    deliver_contracts(db, FOLLOWING_BASE_DATE, contracts)
    with Execution(db, "duckdb", FOLLOWING_BASE_DATE) as run:
        pinned = dict(run.versions)
        report = monthly_pipeline(run)
    assert report.previous == NEXT_BASE_DATE
    expected = expected_produced(tables, FOLLOWING_BASE_DATE)
    assert expected[ENTRIES.name] < expected_produced(tables, NEXT_BASE_DATE)[ENTRIES.name]
    assert report.produced == expected
    assert_apportionment_sums_to_one(report.apportionment, report.produced[CONTRACTS.name])
    assert_audits_ran_every_check(report.audits, FOLLOWING_BASE_DATE)
    assert report.versions == {table.name: pinned[table.name] + 1 for table in PRODUCED}
    with db.open_delta(channel=delta.CURRENT_CHANNEL) as reader:
        for table in PRODUCED:
            counts = partition_counts(reader, table)
            assert list(counts) == [*source.PARTITION_VALUES, NEXT_BASE_DATE, FOLLOWING_BASE_DATE]
            assert counts[FOLLOWING_BASE_DATE] == report.produced[table.name], table.name


def test_a_rerun_of_the_published_month_replaces_its_partition(
    db: Database,
) -> None:
    """A reexecução de 2026-07-31 depois de publicado, como a do "Refazer um snapshot" do runbook,
    lê 2026-06-30 como a última data-base, e não a própria partição, e troca a partição publicada
    por outra com as mesmas contagens, numa versão a mais; cada execução tem o ``execution_id``
    gerado pelo pacote e a pasta dele com a cópia da entrega."""
    tables = source_tables()
    delivered = deliver_contracts(db, NEXT_BASE_DATE, monthly_contracts(tables))
    with Execution(db, "duckdb", NEXT_BASE_DATE) as run:
        first_id = run.execution_id
        first = monthly_pipeline(run)
    with Execution(db, "duckdb", NEXT_BASE_DATE) as run:
        rerun_id = run.execution_id
        pinned = dict(run.versions)
        rerun = monthly_pipeline(run)

    assert rerun.previous == LAST_BASE_DATE
    assert rerun.produced == first.produced
    assert_audits_ran_every_check(rerun.audits, NEXT_BASE_DATE)
    assert rerun.versions == {table.name: pinned[table.name] + 1 for table in PRODUCED}
    for table in PRODUCED:
        uri = db.uri(table)
        version = rerun.versions[table.name]
        changed = delta.version_diff(uri, pinned[table.name], version, table, db.storage)
        assert changed == {NEXT_BASE_DATE}, table.name
    with db.open_delta(channel=delta.CURRENT_CHANNEL) as reader:
        for table in PRODUCED:
            counts = partition_counts(reader, table)
            assert counts[NEXT_BASE_DATE] == rerun.produced[table.name], table.name

    # As duas execuções do mês, cada uma com a pasta dela.
    assert rerun_id != first_id
    assert kept_inputs(db, first_id) == delivered
    assert kept_inputs(db, rerun_id) == delivered


def test_a_repeated_contract_fails_the_audit_and_keeps_the_generated_partition(
    db: Database,
) -> None:
    """Um contrato repetido na entrega reprova a auditoria de ``cad_contratos`` na chave
    ``(data, sistema, contrato)``: a exceção sobe sem commit no Delta, e a pasta da execução guarda
    a cópia da entrega, os relatórios, com o rateio do contrato repetido somando 2, e a partição
    gerada de cada tabela, com o contrato duas vezes."""
    tables = source_tables()
    contracts = monthly_contracts(tables)
    repeated = contracts.slice(0, 1)
    delivered = deliver_contracts(db, NEXT_BASE_DATE, pa.concat_tables([contracts, repeated]))
    failed = r"cad_contratos em \['2026-07-31'\]: reprovada em \['chave_data_sistema_contrato'\]"
    with (
        pytest.raises(AuditFailed, match=failed),
        Execution(db, "duckdb", NEXT_BASE_DATE, execution_id=EXECUTION_ID) as run,
    ):
        pinned = dict(run.versions)
        monthly_pipeline(run)

    # Nada foi publicado: cada tabela segue na versão fixada.
    for table in PRODUCED:
        assert delta.open_table(db.uri(table), db.storage).version() == pinned[table.name]

    # A pasta da execução: a cópia da entrega, os relatórios e a partição gerada de cada tabela.
    inputs = [f"entradas/{name}" for name in delivered]
    generated = [f"geracao/{table.name}.parquet" for table in PRODUCED]
    assert execution_files(db, EXECUTION_ID) == sorted([*inputs, *generated, *REPORT_FILES])
    assert kept_inputs(db, EXECUTION_ID) == delivered

    # A partição gerada mostra o defeito: um contrato a mais, e o repetido duas vezes.
    expected = expected_produced(tables, NEXT_BASE_DATE)
    expected[CONTRACTS.name] += 1
    generated_rows = {}
    for table in PRODUCED:
        kept = read_intermediate(db, EXECUTION_ID, f"geracao/{table.name}.parquet")
        generated_rows[table.name] = kept.num_rows
    assert generated_rows == expected
    code = repeated["contrato"][0].as_py()
    kept_contracts = read_intermediate(db, EXECUTION_ID, f"geracao/{CONTRACTS.name}.parquet")
    assert kept_contracts["contrato"].to_pylist().count(code) == 2

    # O rateio gravado antes da auditoria já mostrava o contrato repetido: os fatores somam 2.
    apportionment = read_intermediate(db, EXECUTION_ID, "relatorios/rateio_por_contrato.parquet")
    apportioned = {}
    for row in apportionment.to_pylist():
        apportioned[row["contrato"]] = row["rateio"]
    assert apportioned[code] == pytest.approx(2.0)
