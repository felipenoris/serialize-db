"""Redshift pela Data API: execução assíncrona por HTTPS, sem conexão na porta 5439.

Exemplo executado com sucesso no ambiente alvo em 2026-09-20 e guardado como está: os valores
literais (região, workgroup, banco, tabela) são os do ambiente, e o que ele prova está em
[`../plan/POC.md`](../plan/POC.md).

O ciclo tem três passos, e é o que ``probes/redshift.py`` (``RS-10``) repete com ``select 1``:
``execute_statement`` devolve na hora um identificador, ``describe_statement`` é consultado até o
estado ser ``FINISHED``, ``FAILED`` ou ``ABORTED``, e ``get_statement_result`` devolve o resultado
paginado. Cada célula é um dicionário de um item (``{"stringValue": "x"}``, ``{"longValue": 1}``,
``{"isNull": True}``), e ``DECIMAL``, data e hora chegam como texto: a Data API serve a comandos e
a diagnóstico, não à troca de dados da biblioteca, que usa o protocolo nativo
([`redshift_native.py`](redshift_native.py)) e o S3.

Permissões: ``redshift-data:ExecuteStatement``, ``DescribeStatement`` e ``GetStatementResult``, mais
``redshift-serverless:GetCredentials`` no workgroup, porque o usuário do banco sai da identidade IAM.
"""

import time
import boto3
import pandas as pd

REGION    = "sa-east-1"
WORKGROUP = "controladoria-wg"
DB_CONN   = "dev"
SQL       = "SELECT * FROM datalake_rw_shared.sbx_aco_decon.teste LIMIT 10;"

rd = boto3.client("redshift-data", region_name=REGION)

# 1. dispara a query (assíncrono — retorna na hora, sem esperar)
resp = rd.execute_statement(
    WorkgroupName=WORKGROUP,
    Database=DB_CONN,
    Sql=SQL,
)
stmt_id = resp["Id"]
print("statement:", stmt_id)

# 2. polling até terminar
while True:
    desc = rd.describe_statement(Id=stmt_id)
    status = desc["Status"]
    if status in ("FINISHED", "FAILED", "ABORTED"):
        break
    time.sleep(1)

print("status:", status, "| duração(ms):", desc.get("Duration", 0) // 1_000_000)

if status != "FINISHED":
    raise RuntimeError(desc.get("Error", "erro sem detalhe"))

# 3. resultado (paginado)
colunas, linhas = None, []
paginator = rd.get_paginator("get_statement_result")
for page in paginator.paginate(Id=stmt_id):
    if colunas is None:
        colunas = [c["name"] for c in page["ColumnMetadata"]]
    for registro in page["Records"]:
        # cada célula é um dict de um item: {"stringValue": "x"} ou {"isNull": True}
        linhas.append([
            None if celula.get("isNull") else list(celula.values())[0]
            for celula in registro
        ])

print(pd.DataFrame(linhas, columns=colunas))
