"""``COPY`` e ``UNLOAD`` no esquema do datashare, com as credenciais de quem chama.

Exemplo executado com sucesso no ambiente alvo em 2026-09-20 e guardado como foi executado. Ele
responde quatro perguntas que a documentação e os probes não fechavam
([`../docs/POC.md`](../docs/POC.md), [`../docs/OPEN_QUESTIONS.md`](../docs/OPEN_QUESTIONS.md)):

1. **O produtor concedeu escrita no datashare.** ``CREATE TABLE``, ``COPY``, ``SELECT`` e ``UNLOAD``
   passaram em ``sbx_aco_decon``, no banco ``datalake_rw_shared``.
2. **``USE <banco>`` troca o banco da sessão**, e a partir daí o nome em duas partes
   ``esquema.tabela`` basta. A restrição documentada, de que só o nome em três partes vale, se
   aplica a quem não está conectado ao banco compartilhado.
3. **O ``COPY`` e o ``UNLOAD`` levam as credenciais de quem chama**, por ``ACCESS_KEY_ID``,
   ``SECRET_ACCESS_KEY`` e ``SESSION_TOKEN``, em vez de ``IAM_ROLE``. É o que destrava o ambiente
   alvo, onde o namespace não tem papel IAM associado: quem alcança o S3 é a identidade da sessão.
   O texto com as credenciais nunca é registrado em log nem gravado em arquivo.
4. **O ``COPY`` lê um prefixo de pasta Parquet direto**, sem manifesto, e converte ``int32`` da
   origem para a coluna ``BIGINT`` do contrato.

O ``UNLOAD`` também passou a partir de uma tabela do datashare, o que a lista de comandos
suportados da AWS não afirmava.

O DDL de ``cad_contas`` é o do contrato, gerado do esquema lido da base de origem
([`../tests/source_db_projetado.py`](../tests/source_db_projetado.py)): a chave em ``BIGINT``, o
texto com tamanho medido, ``NOT NULL`` em todas as colunas, chave primária e unicidade informativas,
``DISTSTYLE ALL`` por ser dimensão.
"""

import boto3
import redshift_connector

# Credenciais
session = boto3.Session()
credentials = session.get_credentials().get_frozen_credentials()

REGION    = "sa-east-1"
WORKGROUP = "controladoria-wg"
DB_CONN   = "dev"                 # banco local onde você conecta
DB_SHARE  = "datalake_rw_shared"  # banco do datashare (só no nome da tabela)
SCHEMA    = "sbx_aco_decon"
S3_DB_TABLE_PATH = "s3://bndes-aco-models-138071776059/dzd-5qqmzj3amjp657/3hpfa7636y4qor/shared/bndes_grupos_bases_analise_financeira/databases/dsv/db_projetado/cad_contas/"
S3_UNLOAD_PATH = "s3://bndes-aco-models-138071776059/dzd-5qqmzj3amjp657/3hpfa7636y4qor/shared/unload/"

rs = boto3.client("redshift-serverless", region_name=REGION)

# 1. endpoint do workgroup
wg   = rs.get_workgroup(workgroupName=WORKGROUP)["workgroup"]
host = wg["endpoint"]["address"]
port = wg["endpoint"]["port"]
print(f"endpoint: {host}:{port}")

# 2. credencial temporária derivada da sua role IAM
cred = rs.get_credentials(
    workgroupName=WORKGROUP,
    dbName=DB_CONN,
    durationSeconds=3600,
)
print(f"dbUser: {cred['dbUser']} | expira: {cred['expiration']}")

# 3. conexão via protocolo nativo (porta 5439)
conn = redshift_connector.connect(
    host=host,
    port=port,
    database=DB_CONN,
    user=cred["dbUser"],
    password=cred["dbPassword"],
)

try:
    with conn.cursor() as cur:

        # muda banco atual para o DB_SHARE
        cur.execute(f"USE {DB_SHARE};")

        # testa SELECT neste database
        cur.execute(f"SELECT * FROM {SCHEMA}.teste;")
        df = cur.fetch_dataframe()
        print(df)

        # cria tabela cad_contas
        ddl = f"""
CREATE TABLE {SCHEMA}.cad_contas (
	id_conta BIGINT NOT NULL,
	nome VARCHAR(100) NOT NULL,
	numero VARCHAR(20) NOT NULL,
	permite_lancamentos BOOLEAN NOT NULL,
	CONSTRAINT pk_cad_contas PRIMARY KEY (id_conta),
	CONSTRAINT uq_cad_contas_numero UNIQUE (numero)
) DISTSTYLE ALL SORTKEY (id_conta);
"""

        cur.execute(ddl)

        sql = f"""
COPY {SCHEMA}.cad_contas
FROM '{S3_DB_TABLE_PATH}'
ACCESS_KEY_ID '{credentials.access_key}'
SECRET_ACCESS_KEY '{credentials.secret_key}'
SESSION_TOKEN '{credentials.token}'
FORMAT AS PARQUET;
"""

        print(sql)
        cur.execute(sql)

        cur.execute(f"SELECT * FROM {SCHEMA}.cad_contas LIMIT 10;")
        df = cur.fetch_dataframe()

        print(df)
        cur.execute("COMMIT;")

    with conn.cursor() as cur:

        # muda banco atual para o DB_SHARE
        cur.execute(f"USE {DB_SHARE};")

        sql = f"""
UNLOAD ('SELECT * FROM {SCHEMA}.cad_contas')
TO '{S3_UNLOAD_PATH}'
ACCESS_KEY_ID '{credentials.access_key}'
SECRET_ACCESS_KEY '{credentials.secret_key}'
SESSION_TOKEN '{credentials.token}'
FORMAT AS PARQUET;
"""
        cur.execute(sql)

finally:
    conn.close()
