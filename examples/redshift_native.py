"""Redshift pelo protocolo nativo: credencial temporária do workgroup e conexão na porta 5439.

Exemplo executado com sucesso no ambiente alvo em 2026-09-20 e guardado como está: os valores
literais (região, workgroup, banco, esquema, tabela) são os do ambiente, e o que ele prova está em
[`../docs/POC.md`](../docs/POC.md). É o caminho que a biblioteca usa
([`../docs/PLAN-STAGE-5.md`](../docs/PLAN-STAGE-5.md)), e o que ``probes/redshift.py`` (``RS-15``) e
``tests/conftest.py`` (``connect_redshift``) repetem.

São três chamadas: ``get_workgroup`` dá o endereço e a porta do endpoint, ``get_credentials``
devolve um par usuário e senha derivado da identidade IAM (no máximo 3600 segundos; sem
``durationSeconds`` são 900), e ``redshift_connector.connect`` abre a sessão com esse par. O usuário
sai como ``IAMR:<papel>`` ou ``IAM:<usuário>`` e entra em ``PUBLIC``: os privilégios do esquema
precisam alcançá-lo.

O nome em três partes ``banco.esquema.tabela`` cita a tabela do datashare a partir do banco local da
conexão. A biblioteca escreve nesse esquema, e o que o Redshift permite escrever num datashare está
em [`../docs/redshift.md`](../docs/redshift.md).

Permissões: ``redshift-serverless:GetWorkgroup`` e ``GetCredentials`` (com o recurso ``dbname``
quando ``dbName`` é informado).
"""

import boto3
import redshift_connector

REGION    = "sa-east-1"
WORKGROUP = "controladoria-wg"
DB_CONN   = "dev"                 # banco local onde você conecta
DB_SHARE  = "datalake_rw_shared"  # banco do datashare (só no nome da tabela)
SCHEMA    = "sbx_aco_decon"
TABELA    = "teste"

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
        # diagnóstico: quem você é dentro do banco
        cur.execute("SELECT current_user, current_database();")
        print("sessão:", cur.fetchone())

        # 4. o SELECT, com nome em três partes
        cur.execute(f'SELECT * FROM {DB_SHARE}.{SCHEMA}.{TABELA} LIMIT 10;')
        df = cur.fetch_dataframe()

    print(df)
finally:
    conn.close()
