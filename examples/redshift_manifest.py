"""``COPY ... MANIFEST`` e ``UNLOAD ... PARTITION BY MANIFEST VERBOSE`` sobre uma tabela Delta.

Os pré-requisitos do ``export_partition`` da [etapa 5](../docs/PLAN-STAGE-5.md) e do
``COPY`` da publicação da [etapa 8](../docs/PLAN-STAGE-8.md), os dois comandos com manifesto que
ninguém exercitou no datashare ([`../docs/OPEN_QUESTIONS.md`](../docs/OPEN_QUESTIONS.md)). Ao
contrário dos outros três scripts desta pasta, este ainda **não** rodou no ambiente alvo: ele é o
próximo experimento, escrito a partir do que os três já provaram.

O ciclo, em oito passos:

1. Lê uma partição Parquet da base de origem (``cad_contratos``, Hive por ``data_str``).
2. Grava essa partição como tabela Delta, particionada pela mesma coluna. O delta-rs deixa a coluna
   de partição fora do arquivo de dados: ela vive na ação ``add``.
3. Monta o manifesto do ``COPY`` com os arquivos que o log lista, cada entrada com a URL e o
   ``meta.content_length`` que o Parquet exige.
4. Abre a sessão como ``redshift_native.py`` e roda ``USE`` no banco do datashare.
5. ``COPY ... FORMAT AS PARQUET MANIFEST`` numa staging **sem** a coluna de partição, porque o
   ``COPY`` é posicional e a coluna não está no arquivo.
6. ``INSERT INTO <destino> SELECT *, '<valor>'``, que acrescenta a coluna de partição.
7. ``UNLOAD ... PARTITION BY (<coluna>) FORMAT PARQUET MANIFEST VERBOSE``, com ``cast`` para
   ``DECIMAL`` e ``TIMESTAMP`` no ``select``: os tipos físicos que o ``UNLOAD`` grava para os dois,
   se as colunas saem ``required`` e se há estatística de mínimo e máximo são perguntas da
   [etapa 0](../docs/PLAN-STAGE-0.md) que só a leitura do rodapé responde.
8. Registra os arquivos do ``UNLOAD`` numa tabela Delta por ``create_write_transaction``, uma
   ``AddAction`` por entrada do manifesto, e lê a tabela de volta: é o ``register_files`` da etapa 5.

Por que ``cad_contratos`` e não ``cad_contas``: o ``UNLOAD ... PARTITION BY`` precisa de uma coluna
de partição, e ``cad_contas`` é uma das dez dimensões sem partição da base, com um único
``chunk_0.parquet`` na raiz da tabela ([`../docs/POC.md`](../docs/POC.md)). As quatro particionadas
são ``cad_contratos``, ``cad_operacoes`` e ``rel_contrato_operacao`` (por ``data_str``) e
``cad_lancamentos`` (por ``data_base_str``).

O que o script escreve: duas tabelas no esquema do projeto, apagadas no fim, e os objetos sob
``S3_WORK/<identificador da execução>/``, que ficam para inspeção — o endereço sai impresso no
fim, com o comando que os apaga. Nada é escrito na base de origem, que é lida.

O texto do ``COPY`` e do ``UNLOAD`` carrega ``ACCESS_KEY_ID``, ``SECRET_ACCESS_KEY`` e
``SESSION_TOKEN``: **ele nunca é impresso nem gravado**. O que sai no terminal é a versão mascarada.

Precisa de ``deltalake`` e ``pyarrow`` além de ``boto3`` e ``redshift_connector``, então roda com o
interpretador da pasta preparada:

    .venv/bin/python examples/redshift_manifest.py

Permissões: as de ``redshift_native.py``, mais ``s3:GetObject`` na base de origem e ``ListBucket``,
``GetObject``, ``PutObject`` e ``DeleteObject`` sob ``S3_WORK`` — pela identidade de quem chama, que
é quem o ``COPY`` e o ``UNLOAD`` levam no texto do comando.
"""

import datetime as dt
import io
import json
import os
import re
import time
import uuid

import boto3
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.fs
import pyarrow.parquet as pq
import redshift_connector
from deltalake import DeltaTable, write_deltalake
from deltalake.transaction import AddAction

REGION    = "sa-east-1"
WORKGROUP = "controladoria-wg"
DB_CONN   = "dev"                 # banco local onde você conecta
DB_SHARE  = "datalake_rw_shared"  # banco do datashare que guarda o esquema
SCHEMA    = "sbx_aco_decon"

BASE = "s3://bndes-aco-models-138071776059/dzd-5qqmzj3amjp657/3hpfa7636y4qor/shared"
S3_SOURCE = f"{BASE}/bndes_grupos_bases_analise_financeira/databases/dsv/db_projetado/cad_contratos"
S3_WORK   = f"{BASE}/serialize-db-exemplo"

TABLE            = "cad_contratos"
PARTITION_COLUMN = "data_str"
PARTITION        = "2026-02-28"   # uma das três partições de cad_contratos
ROW_LIMIT        = 500_000        # None lê a partição inteira (cerca de 2,2 milhões de linhas)

# As colunas do arquivo Parquet, na ordem em que estão nele: o COPY é posicional, e a staging
# precisa ter exatamente estas, sem a coluna de partição. "to" é palavra reservada e vai entre aspas.
# A nulidade aqui é a dos arquivos, não a do modelo: sete colunas de cad_contratos são anuláveis na
# origem e NOT NULL no contrato (docs/POC.md), e quem resolve isso é o cast da etapa 1.
FILE_COLUMNS = """
    id_contrato BIGINT NOT NULL,
    data DATE NOT NULL,
    sistema INTEGER,
    contrato VARCHAR(100) NOT NULL,
    legado BOOLEAN NOT NULL,
    um INTEGER,
    "to" VARCHAR(100),
    fonte INTEGER,
    fonte_familia VARCHAR(100),
    estagio INTEGER,
    taxa_juros_fixos DOUBLE PRECISION,
    data_assinatura DATE,
    data_primeira_amortizacao DATE,
    data_ultima_amortizacao DATE
"""

# O esquema Arrow do que o UNLOAD grava, para a tabela Delta do passo 8.
UNLOAD_SCHEMA = pa.schema(
    [
        pa.field("id_contrato", pa.int64(), nullable=False),
        pa.field("data", pa.date32(), nullable=False),
        pa.field("contrato", pa.string(), nullable=False),
        pa.field("taxa_juros_fixos", pa.float64()),
        pa.field("taxa_decimal", pa.decimal128(18, 2)),
        pa.field("data_hora", pa.timestamp("us")),
        pa.field(PARTITION_COLUMN, pa.string(), nullable=False),
    ]
)

run_id = uuid.uuid4().hex[:8]
work = f"{S3_WORK}/{run_id}"
delta_uri = f"{work}/delta/{TABLE}"
manifest_uri = f"{work}/manifesto.json"
unload_uri = f"{work}/unload"
staging_table = f"exemplo_manifesto_{run_id}_staging"
target_table = f"exemplo_manifesto_{run_id}_{TABLE}"

# O cliente HTTP do delta-rs lê NO_PROXY antes de no_proxy, e uma NO_PROXY vazia anula as exceções:
# a chamada ao endpoint de credenciais vai pelo proxy e volta 403 (docs/POC.md, 2026-09-20).
if not os.environ.get("NO_PROXY") and os.environ.get("no_proxy"):
    os.environ["NO_PROXY"] = os.environ["no_proxy"]

s3 = boto3.client("s3", region_name=REGION)
fs = pyarrow.fs.S3FileSystem(region=REGION)
bucket, _, work_prefix = work.removeprefix("s3://").partition("/")
unloaded = False  # o UNLOAD ... PARTITION BY é a pergunta; a limpeza roda mesmo se ele falhar


def mask(sql: str) -> str:
    """O comando sem as credenciais, para o terminal: o texto original nunca é impresso."""
    return re.sub(r"(ACCESS_KEY_ID|SECRET_ACCESS_KEY|SESSION_TOKEN) '[^']*'", r"\1 '<oculto>'", sql)


def credentials_clause() -> str:
    """Como o ``COPY`` e o ``UNLOAD`` alcançam o S3: as credenciais de quem chama, pedidas a cada comando.

    O namespace do ambiente alvo não tem papel IAM associado, e sem papel associado nem um ARN
    explícito funciona (``RS-6``). Elas expiram em cerca de uma hora, por isso a cláusula é montada
    por comando e nunca guardada.
    """
    found = boto3.Session().get_credentials().get_frozen_credentials()
    clause = f"ACCESS_KEY_ID '{found.access_key}'\nSECRET_ACCESS_KEY '{found.secret_key}'"
    return clause + (f"\nSESSION_TOKEN '{found.token}'" if found.token else "")


def execute(cursor, sql: str) -> list[tuple]:
    """Roda o comando, ecoa a versão mascarada e devolve as linhas, ou uma lista vazia."""
    print(f"\n$ {mask(sql).strip()}")
    started = time.perf_counter()
    cursor.execute(sql)
    rows = cursor.fetchall() if cursor.description else []
    print(f"({time.perf_counter() - started:.1f} s)")
    return rows


# ------------------------------------------------------------------------------------------------
# 1. A partição Parquet da origem

source = f"{S3_SOURCE}/{PARTITION_COLUMN}={PARTITION}"
print(f"1. lendo {source}")
data = ds.dataset(source.removeprefix("s3://"), filesystem=fs, format="parquet")
table = data.head(ROW_LIMIT) if ROW_LIMIT else data.to_table()
print(f"   {table.num_rows} linhas, {len(table.schema)} colunas")

# A coluna de partição vive só no caminho, e o valor é igual a `data` em toda linha da partição: é o
# que o initial_load da etapa 7 confere antes de acrescentar a coluna.
assert pc.all(pc.equal(table.column("data"), dt.date.fromisoformat(PARTITION))).as_py(), (
    f"alguma linha tem `data` diferente de {PARTITION}"
)

# As chaves inteiras passam a int64 na migração para o Delta (decisão de 2026-09-20).
index = table.schema.get_field_index("id_contrato")
table = table.set_column(index, pa.field("id_contrato", pa.int64(), nullable=False), table.column("id_contrato").cast(pa.int64()))
table = table.append_column(
    pa.field(PARTITION_COLUMN, pa.string(), nullable=False),
    pa.array([PARTITION] * table.num_rows, pa.string()),
)

# ------------------------------------------------------------------------------------------------
# 2. A tabela Delta, particionada pela mesma coluna

print(f"\n2. gravando {delta_uri}")
write_deltalake(delta_uri, table, mode="overwrite", partition_by=[PARTITION_COLUMN])
delta = DeltaTable(delta_uri)
# get_add_actions devolve uma tabela arro3; pa.table a converte pelo PyCapsule Interface, sem cópia.
actions = pa.table(delta.get_add_actions(flatten=True)).to_pylist()
print(f"   versão {delta.version()}, {len(actions)} arquivo(s)")

# O delta-rs não grava a coluna de partição dentro do arquivo: ela está na ação `add`. É por isso
# que a staging do COPY não a tem, e o INSERT do passo 6 a acrescenta.
written = pq.read_schema(f"{delta_uri.removeprefix('s3://')}/{actions[0]['path']}", filesystem=fs)
print(f"   colunas no arquivo: {', '.join(written.names)}")
assert PARTITION_COLUMN not in written.names, "o delta-rs gravou a coluna de partição dentro do arquivo"

# ------------------------------------------------------------------------------------------------
# 3. O manifesto do COPY

entries = [
    {"url": f"{delta_uri}/{action['path']}", "mandatory": True, "meta": {"content_length": action["size_bytes"]}}
    for action in actions
]
s3.put_object(
    Bucket=bucket,
    Key=f"{work_prefix}/manifesto.json",
    Body=json.dumps({"entries": entries}).encode(),
)
print(f"\n3. manifesto em {manifest_uri} com {len(entries)} entrada(s)")

# ------------------------------------------------------------------------------------------------
# 4. A sessão, como em redshift_native.py

serverless = boto3.client("redshift-serverless", region_name=REGION)
endpoint = serverless.get_workgroup(workgroupName=WORKGROUP)["workgroup"]["endpoint"]
credentials = serverless.get_credentials(workgroupName=WORKGROUP, dbName=DB_CONN, durationSeconds=3600)
print(f"\n4. {endpoint['address']}:{endpoint['port']} como {credentials['dbUser']}")

connection = redshift_connector.connect(
    host=endpoint["address"],
    port=endpoint["port"],
    database=DB_CONN,
    user=credentials["dbUser"],
    password=credentials["dbPassword"],
)
connection.autocommit = True

try:
    with connection.cursor() as cursor:
        execute(cursor, f"USE {DB_SHARE};")

        # --------------------------------------------------------------------------------------
        # 5. COPY ... MANIFEST na staging, sem a coluna de partição

        execute(cursor, f"CREATE TABLE {SCHEMA}.{staging_table} ({FILE_COLUMNS});")
        execute(
            cursor,
            f"COPY {SCHEMA}.{staging_table}\nFROM '{manifest_uri}'\n{credentials_clause()}\nFORMAT AS PARQUET MANIFEST;",
        )
        print(f"   staging: {execute(cursor, f'SELECT count(*) FROM {SCHEMA}.{staging_table};')[0][0]} linhas")

        # --------------------------------------------------------------------------------------
        # 6. O destino com a coluna de partição, acrescentada pelo INSERT

        execute(
            cursor,
            f"CREATE TABLE {SCHEMA}.{target_table} (\n{FILE_COLUMNS.rstrip()},\n    {PARTITION_COLUMN} VARCHAR(10) NOT NULL\n)\n"
            f"DISTSTYLE KEY DISTKEY (id_contrato) SORTKEY ({PARTITION_COLUMN}, id_contrato);",
        )
        execute(
            cursor,
            f"INSERT INTO {SCHEMA}.{target_table}\nSELECT *, '{PARTITION}' FROM {SCHEMA}.{staging_table};",
        )
        print(f"   destino: {execute(cursor, f'SELECT count(*) FROM {SCHEMA}.{target_table};')[0][0]} linhas")

        # --------------------------------------------------------------------------------------
        # 7. UNLOAD ... PARTITION BY MANIFEST VERBOSE
        #
        # Os dois `cast` respondem perguntas da etapa 0 que a base de origem não tem como responder,
        # porque ela não tem coluna DECIMAL nem TIMESTAMP: qual tipo físico o UNLOAD grava para cada
        # um. A coluna de partição entra no `select` e o Redshift a tira dos arquivos, pondo o valor
        # no caminho, que é o layout que o Delta espera.
        select = (
            f"select id_contrato, data, contrato, taxa_juros_fixos, "
            f"cast(taxa_juros_fixos as decimal(18,2)) as taxa_decimal, "
            f"cast(data as timestamp) as data_hora, {PARTITION_COLUMN} "
            f"from {SCHEMA}.{target_table}"
        )
        unload = (
            f"UNLOAD ('{select}')\nTO '{unload_uri}/'\n{credentials_clause()}\n"
            f"FORMAT AS PARQUET PARTITION BY ({PARTITION_COLUMN}) MANIFEST VERBOSE;"
        )
        try:
            execute(cursor, unload)
            unloaded = True
        except Exception as error:  # noqa: BLE001 - a recusa é a resposta da pergunta, não defeito
            print(f"!! UNLOAD ... PARTITION BY recusado: {type(error).__name__}: {error}")
            print("   é a questão em aberto; a alternativa da etapa 5 é um UNLOAD por partição")
            unloaded = False
finally:
    # A limpeza nunca esconde o erro que trouxe o script até aqui, e um comando por execute: num
    # datashare, um comando múltiplo fora de um bloco de transação não é aceito.
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"USE {DB_SHARE};")
            for name in (staging_table, target_table):
                cursor.execute(f"DROP TABLE IF EXISTS {SCHEMA}.{name};")
        print(f"\n   tabelas apagadas: {staging_table}, {target_table}")
    except Exception as error:  # noqa: BLE001 - a limpeza falhada é aviso, não o resultado
        print(f"!! limpeza falhou, apague à mão {SCHEMA}.{staging_table} e {SCHEMA}.{target_table}: {error}")
    connection.close()

if not unloaded:
    raise SystemExit(f"\nobjetos em {work}; apague com: aws s3 rm --recursive {work}")

# ------------------------------------------------------------------------------------------------
# 8. O manifesto do UNLOAD, o rodapé de um arquivo e o registro no Delta

manifest = json.loads(s3.get_object(Bucket=bucket, Key=f"{work_prefix}/unload/manifest")["Body"].read())
entries = manifest["entries"]
print(f"\n8. manifesto do UNLOAD: {len(entries)} arquivo(s), {sum(entry['meta']['record_count'] for entry in entries)} linhas")

body = s3.get_object(Bucket=bucket, Key=entries[0]["url"].removeprefix(f"s3://{bucket}/"))["Body"].read()
parquet = pq.ParquetFile(io.BytesIO(body))
print("   tipos físicos:", {parquet.schema.column(i).name: parquet.schema.column(i).physical_type for i in range(len(parquet.schema))})
print("   esquema:", " ".join(str(parquet.schema).split()))
statistics = parquet.metadata.row_group(0).column(0).statistics
print(f"   estatística de mínimo e máximo: {bool(statistics and statistics.has_min_max)}")

registered = DeltaTable.create(unload_uri, UNLOAD_SCHEMA, partition_by=[PARTITION_COLUMN], mode="ignore")
registered.create_write_transaction(
    [
        AddAction(
            path=entry["url"].removeprefix(f"{unload_uri}/"),
            size=entry["meta"]["content_length"],
            partition_values=dict([entry["url"].removeprefix(f"{unload_uri}/").split("/")[0].split("=", 1)]),
            modification_time=int(time.time() * 1000),
            data_change=True,
            stats=json.dumps({"numRecords": entry["meta"]["record_count"], "minValues": {}, "maxValues": {}, "nullCount": {}}),
        )
        for entry in entries
    ],
    mode="append",
    schema=registered.schema(),
    partition_by=[PARTITION_COLUMN],
)
print(f"   registrados no Delta {unload_uri}: {DeltaTable(unload_uri).to_pyarrow_table().num_rows} linhas de volta")

print(f"\nobjetos em {work}; apague com: aws s3 rm --recursive {work}")
