# Etapa 8: publicação para clientes

A entrega e o critério de aceite desta etapa estão na tabela de etapas de [`PLAN.md`](PLAN.md), que
também fixa as decisões, as regras que toda etapa obedece e a ordem do trabalho.

As tabelas publicadas, `<ambiente>_<tabela>` no esquema único, são derivadas do Delta; nada é
escrito nelas por outro caminho. O esquema único é `sbx_aco_decon` no banco de datashare
`datalake_rw_shared` (decisão do usuário de 2026-09-20), então todo comando cita a tabela por nome
em duas partes depois do `USE <banco>` que a conexão roda, e a escrita obedece ao que um datashare
aceita ([`redshift.md`](redshift.md)): `COPY` sem cláusula `COMPUPDATE`, a escrita de uma transação
num banco só, e um comando múltiplo apenas dentro de um bloco de transação. A tabela de controle
mora no mesmo banco das tabelas publicadas, e a transação da publicação abre com `BEGIN` explícito.
O `COPY` e o `UNLOAD` levam a cláusula de credenciais da [etapa 5](PLAN-STAGE-5.md).

O `COPY ... MANIFEST` numa tabela de datashare passou no ambiente alvo em 2026-09-21, 500.000 linhas
em 4,6 s a partir de um arquivo gravado pelo delta-rs
([`../examples/redshift_manifest.py`](../examples/redshift_manifest.py), [`POC.md`](POC.md)): é o
caminho da publicação, e o manifesto é o que impede o `COPY` de ler também os arquivos das versões
anteriores, que o prefixo da partição guarda até o `vacuum` ([`delta.md`](delta.md)). A alternativa
que existia enquanto a pergunta estava aberta, copiar os arquivos da versão para
`staging/<execution_id>/` por `storage.copy` e carregar esse prefixo, deixa de ser necessária.

| Primitiva | O que faz |
| --- | --- |
| `serialize_db_publications` | `CREATE TABLE IF NOT EXISTS serialize_db_publications (table_name VARCHAR(127), delta_version BIGINT, execution_id VARCHAR(127), published_at TIMESTAMP)`. |
| `run.publish_redshift(*tables)` | Para cada tabela, `version_diff` entre a versão em `serialize_db_publications` e a atual (na primeira publicação, todas as partições); a reconciliação da tabela publicada (`ALTER TABLE ADD COLUMN` no fim, porque o `COPY` é posicional; recriação e recarga no diff destrutivo); numa única transação aberta por `BEGIN`, por tabela e partição: `DELETE` da partição, `COPY ... MANIFEST` na staging, `INSERT ... SELECT *, '<valor>'` e a linha de controle. |
| `publication_status(db)` | A versão publicada contra a atual de cada tabela, para o operador. |
| `serialize-db publish` | A publicação fora de uma execução, por exemplo depois de uma correção. |

Testes: o SQL da transação comparado com texto esperado, sem conexão, com o nome em duas partes e
a cláusula de credenciais mascarada; integração marcada `redshift`. Provas de conceito:
`test_deltalake.py::test_version_diff_reads_data_changes_in_the_log`,
`test_stdlib.py::test_group_log_actions_by_partition` e
`test_redshift.py::test_copy_manifest_from_delta_files` (a transação da publicação repete o `COPY`
na staging e o `INSERT` com a partição).

## Interface

O módulo é `serialize_db.publication`, novo na organização do pacote: a publicação usa o motor
Redshift e a camada Delta, e `Execution.publish_redshift` a chama.

```python
"""Assinaturas de serialize_db.publication; os corpos estão no rascunho abaixo."""
import dataclasses

import sqlalchemy as sa


@dataclasses.dataclass(frozen=True)
class PublicationStatus:
    table: str                      # <ambiente>_<tabela>
    published_version: int | None   # None: nunca publicada
    current_version: int
    pending_partitions: tuple[str | None, ...]


def control_ddl(schema: str) -> str: ...
def publication_transaction(schema: str, environment: str, table: sa.Table, partitions: list[str | None], manifests: dict[str | None, str],
                            delta_version: int, execution_id: str, credentials: str) -> list[str]: ...
def reconcile_published(schema: str, environment: str, table: sa.Table, diff: object) -> list[str]: ...
def publish_redshift(db: object, engine: object, tables: list[sa.Table], execution_id: str, max_workers: int = 1) -> dict[str, int]: ...
def publication_status(db: object, engine: object) -> list[PublicationStatus]: ...
```

## Estratégia de implementação

- **`control_ddl`** é `CREATE TABLE IF NOT EXISTS <esquema>.serialize_db_publications (table_name
  VARCHAR(127), delta_version BIGINT, execution_id VARCHAR(127), published_at TIMESTAMP)`, a mesma
  conferência do `USE` que o motor roda ao conectar ([etapa 5](PLAN-STAGE-5.md)).
- **`publish_redshift`** lê, por tabela, a versão publicada em `serialize_db_publications` e a
  atual, chama `delta.version_diff` (todas as partições na primeira publicação), grava um
  `copy_manifest` por partição em `publicacao/<execution_id>/<tabela>/<valor>.manifest`, aplica
  `reconcile_published` quando o esquema Delta ganhou colunas, e roda `publication_transaction`,
  um comando por `execute`, com `BEGIN` e `COMMIT` explícitos; as tabelas correm num pool com uma
  conexão por thread, limitadas pelas slots do WLM.
- **`publication_transaction`** devolve a lista de comandos: `BEGIN`; `CREATE TABLE
  <esquema>.<ambiente>_<tabela>_staging` sem a coluna de partição; por partição, `DELETE FROM
  <publicada> WHERE <coluna> = '<valor>'`, `DELETE FROM <staging>`, `COPY <staging> FROM '<manifesto>'
  <credenciais> FORMAT AS PARQUET MANIFEST FILLRECORD` (a proposta da seção "Decisões pendentes"),
  `INSERT INTO <publicada> SELECT *, '<valor>' FROM
  <staging>` (com `JSON_PARSE` nas colunas `SUPER`); depois `DELETE` e `INSERT` da linha de controle,
  `DROP TABLE <staging>` e `COMMIT`. `TRUNCATE` não entra: numa tabela local ele confirma a
  transação sozinho, e `DELETE` sem `WHERE` é transacional nos dois casos. Uma partição removida no
  Delta (`version_diff` a devolve pelo `remove`) recebe só o `DELETE`.
- **`reconcile_published`** repete o diff aditivo com `ALTER TABLE ADD COLUMN <coluna> <tipo>` no
  fim da tabela, porque o `COPY` é posicional e recusa um arquivo com colunas a menos
  (`Unmatched number of columns`, 2026-09-21), e a staging nasce do esquema Delta; um diff destrutivo
  devolve `DROP TABLE IF EXISTS` mais `published_ddl`, o `ddl` da [etapa 1](PLAN-STAGE-1.md) com a
  chave primária informativa, escrito nesta etapa sobre `column_ddl` e `quoted`,
  e a publicação seguinte recarrega todas as partições.
- **`publication_status`** compara a versão em `serialize_db_publications` com a atual e lista as
  partições pendentes por `version_diff`, para `serialize-db publish --status`.

## Pré-requisitos e pós-condições

| Primitiva | Pré-requisitos | Pós-condições |
| --- | --- | --- |
| `publish_redshift` | A versão do Delta relida por `read_back`; a sessão no banco do datashare; `s3:GetObject` pela identidade da sessão sobre a pasta da tabela. | Por tabela, uma transação: as partições alteradas trocadas, a linha de controle com a versão do Delta; na falha, `ROLLBACK` implícito e o controle intacto. |
| `publication_transaction` | Manifestos gravados; staging inexistente. | Comandos que o Redshift aceita num bloco de transação, sem `COMPUPDATE`, sem `TRUNCATE`, com nomes em duas partes e a cláusula de credenciais só no `COPY`. |
| `reconcile_published` | Diff calculado contra o esquema Delta publicado. | `ADD COLUMN` no fim; no destrutivo, a recriação e a recarga completa na publicação seguinte. |

## Testes por caso

| Caso | Teste | O que confere |
| --- | --- | --- |
| Texto da transação | `test_publication_transaction_text` (sem conexão) | A sequência de comandos, um por item, com `BEGIN` e `COMMIT`, sem `TRUNCATE` nem `COMPUPDATE`, nomes em duas partes, credenciais mascaradas no que vai a log. |
| Reconciliação | `test_reconcile_published_add_column_and_recreate` | `ADD COLUMN` no aditivo; `DROP TABLE` mais DDL com chave no destrutivo. |
| Diferença | `test_publish_only_changed_partitions` (`redshift`) | Duas publicações: a segunda, depois de uma partição alterada, emite um `DELETE` e um `COPY` só dela. |
| Primeira publicação | `test_first_publication_loads_every_partition` (`redshift`) | Sem linha de controle, todas as partições; a linha de controle escrita na mesma transação. |
| Falha no meio | `test_failed_copy_leaves_control_row_untouched` (`redshift`) | Um manifesto inválido na segunda partição: nenhuma partição trocada, controle intacto. |
| Estado | `test_publication_status_lists_pending_partitions` | A versão publicada, a atual e as partições pendentes por tabela. |
| Distribuição atribuída | `test_published_tables_distribution_is_read` (`redshift`) | `svv_table_info` depois da primeira publicação: `diststyle`, `sortkey1`, `tbl_rows` e `skew_rows` de cada tabela publicada, como leitura, nunca como reprovação; a visão negada também é leitura, porque `RS-8` ainda não foi lida no ambiente alvo. O modelo cliente não declara `redshift` e a distribuição é `AUTO` (decisão do usuário de 2026-09-21): é esta leitura que diz se uma `distkey` explícita se paga, e ela entraria por `ALTER TABLE`. |

## Rascunhos executados

O rascunho monta o texto da transação e da reconciliação; compilado, não executado num cluster
(2026-09-21).

```python
"""Etapa 8: a transação da publicação como texto, a partir do diff de versões e das ações add; compilado, não executado."""
import json
import re

SCHEMA, ENVIRONMENT = "sbx_aco_decon", "prod"
CONTROL = "serialize_db_publications"


def mask(sql: str) -> str:
    return re.sub(r"(ACCESS_KEY_ID|SECRET_ACCESS_KEY|SESSION_TOKEN)\s+'[^']*'", r"\1 '***'", sql)


def control_ddl() -> str:
    return f"CREATE TABLE IF NOT EXISTS {SCHEMA}.{CONTROL} (table_name VARCHAR(127) NOT NULL, delta_version BIGINT NOT NULL, execution_id VARCHAR(127) NOT NULL, published_at TIMESTAMP NOT NULL);"


def publication_transaction(table: str, partition_by: str, partitions: list[str], manifests: dict[str, str], delta_version: int, execution_id: str,
                            staging_ddl: str, credentials: str, super_columns: list[str] = ()) -> list[str]:
    """Um comando por chamada, dentro de BEGIN ... COMMIT: DELETE da partição, COPY MANIFEST na staging, INSERT com a partição, a linha de controle."""
    published, staging = f"{SCHEMA}.{ENVIRONMENT}_{table}", f"{SCHEMA}.{ENVIRONMENT}_{table}_staging"
    commands = ["BEGIN;", staging_ddl.format(name=staging)]
    for value in partitions:
        commands += [
            f"DELETE FROM {published} WHERE {partition_by} = '{value}';",
            f"DELETE FROM {staging};",                       # não TRUNCATE: o local confirma a transação sozinho
            f"COPY {staging}\nFROM '{manifests[value]}'\n{credentials}\nFORMAT AS PARQUET MANIFEST;",
            f"INSERT INTO {published}\nSELECT *, '{value}' AS {partition_by} FROM {staging};",
        ]
    commands += [
        f"DELETE FROM {SCHEMA}.{CONTROL} WHERE table_name = '{ENVIRONMENT}_{table}';",
        f"INSERT INTO {SCHEMA}.{CONTROL} VALUES ('{ENVIRONMENT}_{table}', {delta_version}, '{execution_id}', getdate());",
        f"DROP TABLE {staging};",
        "COMMIT;",
    ]
    return commands


def reconcile_published_ddl(table: str, added: list[tuple[str, str]], destructive: bool) -> list[str]:
    published = f"{SCHEMA}.{ENVIRONMENT}_{table}"
    if destructive:
        return [f"DROP TABLE IF EXISTS {published};", "-- recriação pelo DDL do contrato e recarga de todas as partições"]
    return [f"ALTER TABLE {published} ADD COLUMN {name} {kind};" for name, kind in added]


credentials = "ACCESS_KEY_ID 'AKIA' SECRET_ACCESS_KEY 'segredo' SESSION_TOKEN 'token'"
staging_ddl = "CREATE TABLE {name} (id_lancamento BIGINT NOT NULL, data_base DATE NOT NULL, valor DOUBLE PRECISION NOT NULL);"
manifests = {"2026-08-31": "s3://bucket/prod/publicacao/exec-2026-09-05/cad_lancamentos/2026-08-31.manifest"}
print(control_ddl())
for command in publication_transaction("cad_lancamentos", "data_base_str", ["2026-08-31"], manifests, 58, "exec-2026-09-05", staging_ddl, credentials):
    print(mask(command))
print("\n".join(reconcile_published_ddl("cad_lancamentos", [("canal", "VARCHAR(20)")], destructive=False)))
print("\n".join(reconcile_published_ddl("cad_lancamentos", [], destructive=True)))
print("segredo fora do texto impresso:", all("segredo" not in mask(c) for c in publication_transaction("t", "p", ["v"], {"v": "m"}, 1, "e", staging_ddl, credentials)))
```

Saída:

```
CREATE TABLE IF NOT EXISTS sbx_aco_decon.serialize_db_publications (table_name VARCHAR(127) NOT NULL, delta_version BIGINT NOT NULL, execution_id VARCHAR(127) NOT NULL, published_at TIMESTAMP NOT NULL);
BEGIN;
CREATE TABLE sbx_aco_decon.prod_cad_lancamentos_staging (id_lancamento BIGINT NOT NULL, data_base DATE NOT NULL, valor DOUBLE PRECISION NOT NULL);
DELETE FROM sbx_aco_decon.prod_cad_lancamentos WHERE data_base_str = '2026-08-31';
DELETE FROM sbx_aco_decon.prod_cad_lancamentos_staging;
COPY sbx_aco_decon.prod_cad_lancamentos_staging
FROM 's3://bucket/prod/publicacao/exec-2026-09-05/cad_lancamentos/2026-08-31.manifest'
ACCESS_KEY_ID '***' SECRET_ACCESS_KEY '***' SESSION_TOKEN '***'
FORMAT AS PARQUET MANIFEST;
INSERT INTO sbx_aco_decon.prod_cad_lancamentos
SELECT *, '2026-08-31' AS data_base_str FROM sbx_aco_decon.prod_cad_lancamentos_staging;
DELETE FROM sbx_aco_decon.serialize_db_publications WHERE table_name = 'prod_cad_lancamentos';
INSERT INTO sbx_aco_decon.serialize_db_publications VALUES ('prod_cad_lancamentos', 58, 'exec-2026-09-05', getdate());
DROP TABLE sbx_aco_decon.prod_cad_lancamentos_staging;
COMMIT;
ALTER TABLE sbx_aco_decon.prod_cad_lancamentos ADD COLUMN canal VARCHAR(20);
DROP TABLE IF EXISTS sbx_aco_decon.prod_cad_lancamentos;
-- recriação pelo DDL do contrato e recarga de todas as partições
segredo fora do texto impresso: True
```

## Decisões pendentes

- **[decisão] A staging da publicação como tabela comum no datashare ou temporária no banco da
  conexão** ([`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md)); o rascunho a cria e apaga dentro da
  transação, no esquema do datashare.
- **[decisão] `FILLRECORD` em todo `COPY` da biblioteca, ou a lista de colunas**, para arquivos
  anteriores a uma coluna nova: os dois carregaram um arquivo de cinco colunas numa tabela de seis
  com a coluna nova nula (2026-09-21; a lista em quatro execuções, o `FILLRECORD` em duas). A
  proposta é `FILLRECORD`, porque o
  manifesto de uma partição pode listar arquivos anteriores e posteriores à coluna e um `COPY` só os
  carrega todos; a lista de colunas exigiria um `COPY` por contagem de colunas. Até a decisão, a
  reconciliação destrutiva recarrega tudo, e o rascunho acima emite o `COPY` sem a cláusula.
- **[decisão] O teto do campo JSON no Redshift.** Um Parquet com o documento em texto não leva um
  documento acima de 65.535 bytes a `SUPER`: o `COPY` exige `SERIALIZETOJSON` e, com ela, recusa a
  string (`1224 String value exceeds the max size of 65535 bytes`, 2026-09-21), e a staging
  `VARCHAR(65535)` tem o mesmo teto. A proposta é o teto de 65.535 bytes por documento no contrato,
  conferido pela auditoria da [etapa 4](PLAN-STAGE-4.md) como `String(n)`; os caminhos para um
  documento maior, se uma tabela precisar, são `COPY ... FORMAT JSON 'auto'` de um arquivo JSON com
  uma linha por registro, que carregou um objeto de 80.901 bytes, e `INSERT ... JSON_PARSE(%s)`
  linha a linha, que carregou o mesmo documento.
- **[decisão] A largura de `VARCHAR(n)` da tabela publicada quando `String(n)` cresce no modelo.**
  `schema_diff` compara esquemas Arrow, que não têm `n`, então o Delta e a reconciliação aditiva
  não veem a mudança; só o `<tabela>.redshift.sql` versionado da [etapa 1](PLAN-STAGE-1.md) a
  mostra, e a tabela publicada continua com o `VARCHAR(n)` antigo até um
  `ALTER TABLE ... ALTER COLUMN ... TYPE VARCHAR(n)`, que o Redshift aceita fora de transação e sem
  descer abaixo do maior valor existente ([`redshift.md`](redshift.md)). A proposta é
  `reconcile_published` ler a largura da tabela publicada (`svv_all_columns`, que a suíte lê desde
  2026-09-21) e emitir o comando quando o modelo cresce; a diminuição é destrutiva.
