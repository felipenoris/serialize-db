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
| `serialize_db_publications` | Uma só para todos os ambientes, criada uma vez no esquema pelo usuário, antes da primeira publicação: `CREATE TABLE <esquema>.serialize_db_publications (table_name VARCHAR(127), delta_version BIGINT, execution_id VARCHAR(127), published_at TIMESTAMP)`, sem `IF NOT EXISTS`. Nenhum caminho do pipeline a cria (decisão do usuário de 2026-09-23). |
| `create_publications_table(config)` | A inicialização da tabela de controle: abre uma sessão pelo `connect` da [etapa 5](PLAN-STAGE-5.md) e roda `control_ddl`; a segunda chamada falha com a mensagem do servidor, porque a tabela já existe. |
| `run.publish_redshift(*tables)` | Começa conferindo que a tabela de controle existe, e sem ela levanta `PublicationError`, que aponta `serialize-db publish --init`, antes de qualquer escrita. Depois, para cada tabela, `version_diff` entre a versão em `serialize_db_publications` e a atual (na primeira publicação, todas as partições); a reconciliação da tabela publicada (`ALTER TABLE ADD COLUMN` no fim, porque o `COPY` é posicional; recriação e recarga no diff destrutivo); numa única transação aberta por `BEGIN`, por tabela e partição: `DELETE` da partição, `COPY ... MANIFEST` na staging, `INSERT ... SELECT *, '<valor>'` e a linha de controle. |
| `publication_status(db)` | A versão publicada contra a atual de cada tabela, para o operador, com a mesma conferência da tabela de controle. |
| `serialize-db publish` | A publicação fora de uma execução, por exemplo depois de uma correção; `--status` mostra `publication_status`, e `--init` roda `create_publications_table`. |

Testes: o SQL da transação comparado com texto esperado, sem conexão, com o nome em duas partes e
a cláusula de credenciais mascarada; integração marcada `redshift`. Provas de conceito:
`test_deltalake.py::test_version_diff_reads_data_changes_in_the_log`,
`test_stdlib.py::test_group_log_actions_by_partition` e
`test_redshift.py::test_copy_manifest_from_delta_files` (a transação da publicação repete o `COPY`
na staging e o `INSERT` com a partição) e `test_redshift_transactions.py` (duas publicações
simultâneas no esquema do datashare, à espera do ambiente alvo).

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
def create_publications_table(config: object) -> None: ...   # uma vez no esquema, pelo usuário; RedshiftConfig da etapa 5
def publication_transaction(schema: str, environment: str, table: sa.Table, partitions: list[str | None], manifests: dict[str | None, str],
                            delta_version: int, execution_id: str, credentials: str) -> list[str]: ...
def reconcile_published(schema: str, environment: str, table: sa.Table, diff: object) -> list[str]: ...
def publish_redshift(db: object, engine: object, tables: list[sa.Table], execution_id: str, max_workers: int = 1) -> dict[str, int]: ...
def publication_status(db: object, engine: object) -> list[PublicationStatus]: ...
```

## Estratégia de implementação

- **`control_ddl`** é `CREATE TABLE <esquema>.serialize_db_publications (table_name VARCHAR(127),
  delta_version BIGINT, execution_id VARCHAR(127), published_at TIMESTAMP)`, sem `IF NOT EXISTS`, e
  só `create_publications_table` o roda (decisão do usuário de 2026-09-23): o usuário cria a tabela
  uma vez no esquema, e nenhum caminho do pipeline exercita o `CREATE TABLE` dela. O motor da
  [etapa 5](PLAN-STAGE-5.md) não a cria ao conectar.
- **`publish_redshift`** confere a tabela de controle antes de tudo, por `select 1 from
  <esquema>.serialize_db_publications limit 0` sem transação aberta, porque um erro dentro de uma
  transação aborta os comandos seguintes (25P02, leitura de 2026-09-21): o erro de relação
  inexistente vira `PublicationError`, com o comando de inicialização na mensagem, e a publicação
  para sem ter escrito nada. Depois lê, por tabela, a versão publicada em `serialize_db_publications`
  e a atual, chama `delta.version_diff` (todas as partições na primeira publicação), grava um
  `copy_manifest` por partição na URI `storage.uri_of(<ambiente>/publicacao/<execution_id>/<tabela>/<valor>.manifest)`, aplica
  `reconcile_published` quando o esquema Delta ganhou colunas, e roda `publication_transaction`,
  um comando por `execute`, com `BEGIN` e `COMMIT` explícitos; as tabelas correm num pool com uma
  conexão por thread, limitadas pelas slots do WLM, com a política do `publish` da
  [etapa 6](PLAN-STAGE-6.md): uma tabela entra no pool só com um worker livre e nenhuma falha, e a
  falha sobe com o seu tipo e o resultado de cada tabela numa nota (`add_note`), porque entregar
  todas de uma vez deixou o worker único pegar a tabela seguinte à que falhou (leitura de
  2026-09-23, [`POC.md`](POC.md)). `run.publish_redshift` entra em `Execution` com esta etapa.
- **`publication_transaction`** devolve a lista de comandos: `BEGIN`; `CREATE TABLE
  <esquema>.<ambiente>_<tabela>_staging` sem a coluna de partição; por partição, `DELETE FROM
  <publicada> WHERE <coluna> = '<valor>'`, `DELETE FROM <staging>`, `COPY <staging> FROM '<manifesto>'
  <credenciais> FORMAT AS PARQUET MANIFEST FILLRECORD` (decisão do usuário de 2026-09-23: o
  manifesto de uma partição pode listar arquivos anteriores e posteriores a uma coluna nova, e um
  `COPY` só os carrega todos, com a coluna nova nula nos anteriores, como o Delta os lê),
  `INSERT INTO <publicada> SELECT *, '<valor>' FROM
  <staging>` (com `JSON_PARSE` nas colunas `SUPER`); depois `DELETE` e `INSERT` da linha de controle,
  `DROP TABLE <staging>` e `COMMIT`. `TRUNCATE` não entra: numa tabela local ele confirma a
  transação sozinho, e `DELETE` sem `WHERE` é transacional nos dois casos. Uma partição removida no
  Delta (`version_diff` a devolve pelo `remove`) recebe só o `DELETE`.
- **As publicações simultâneas.** A tabela de controle é a única que dois ambientes escrevem, e duas
  publicações do mesmo ambiente podem tocar a mesma tabela (`serialize-db publish` ao lado de uma
  execução). A documentação do Redshift prevê que o `DELETE` da linha de controle da segunda
  transação espere a primeira terminar e que, sob isolamento de snapshot, linhas distintas
  confirmem as duas, enquanto sob o serializável a segunda recebe `1023`
  ([`redshift.md`](redshift.md), seção "Transações concorrentes").
  `tests/proof_of_concept/test_redshift_transactions.py` mede no esquema do datashare as escritas em
  tabelas distintas, dev e prod gravando linhas distintas da tabela de controle, duas publicações da
  mesma tabela com a staging de nome fixo, o `LOCK` da tabela de controle e o `UPDATE` condicionado
  à versão lida; a primeira execução no ambiente alvo decide se a transação fica como está
  ([`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md)).
- **`reconcile_published`** repete o diff aditivo com `ALTER TABLE ADD COLUMN <coluna> <tipo>` no
  fim da tabela, porque o `COPY` é posicional e recusa um arquivo com colunas a menos
  (`Unmatched number of columns`, 2026-09-21), e a staging nasce do esquema Delta; um diff destrutivo
  devolve `DROP TABLE IF EXISTS` mais `published_ddl`, o `ddl` da [etapa 1](PLAN-STAGE-1.md) com a
  chave primária informativa, escrito nesta etapa sobre `column_ddl` e `quoted`,
  e a publicação seguinte recarrega todas as partições. A largura de `VARCHAR(n)` que muda no
  modelo é diff destrutivo (decisão do usuário de 2026-09-23): `schema_diff` compara esquemas
  Arrow, que não têm `n`, então `reconcile_published` lê a largura de cada coluna da tabela
  publicada em `svv_all_columns` e a compara com a do modelo. O `ALTER TABLE ... ALTER COLUMN ...
  TYPE VARCHAR(n)` não está na lista do que a escrita por datashare aceita, recusa coluna com
  chave e as codificações `BYTEDICT`, `RUNLENGTH`, `TEXT255` e `TEXT32K`, e roda só fora de
  transação ([`redshift.md`](redshift.md)); `test_redshift.py::test_alter_column_type_on_the_share`
  o lê no esquema do datashare, e ele entra como atalho da recriação só depois dessa leitura.
- **O documento JSON** tem o teto de 65.535 bytes no contrato (decisão do usuário de 2026-09-23):
  o `COPY` de Parquet com `SERIALIZETOJSON` recusa uma string maior em `SUPER` (`1224 String value
  exceeds the max size of 65535 bytes`, 2026-09-21), e a staging `VARCHAR(65535)` tem o mesmo teto.
  O `cast` da [etapa 1](PLAN-STAGE-1.md) recusa o documento maior, e a auditoria da
  [etapa 4](PLAN-STAGE-4.md) o conta. Os caminhos para um documento maior, `COPY ... FORMAT JSON
  'auto'` de um arquivo com uma linha por registro e `INSERT ... JSON_PARSE(%s)` linha a linha,
  carregaram um objeto de 80.901 bytes em 2026-09-21 e ficam fora do plano até uma tabela precisar.
- **`publication_status`** confere a tabela de controle como `publish_redshift`, compara a versão
  em `serialize_db_publications` com a atual e lista as partições pendentes por `version_diff`, para
  `serialize-db publish --status`.

## Pré-requisitos e pós-condições

| Primitiva | Pré-requisitos | Pós-condições |
| --- | --- | --- |
| `publish_redshift` | A tabela de controle criada por `create_publications_table`; a versão do Delta relida por `read_back`; a sessão no banco do datashare; `s3:GetObject` pela identidade da sessão sobre a pasta da tabela. | Por tabela, uma transação: as partições alteradas trocadas, a linha de controle com a versão do Delta; na falha, `ROLLBACK` implícito e o controle intacto. |
| `publication_transaction` | Manifestos gravados; staging inexistente. | Comandos que o Redshift aceita num bloco de transação, sem `COMPUPDATE`, sem `TRUNCATE`, com nomes em duas partes e a cláusula de credenciais só no `COPY`. |
| `reconcile_published` | Diff calculado contra o esquema Delta publicado; a largura de cada `VARCHAR` da tabela publicada lida em `svv_all_columns`. | `ADD COLUMN` no fim; no destrutivo, que inclui a largura de `VARCHAR(n)` mudada, a recriação e a recarga completa na publicação seguinte. |

## Testes por caso

| Caso | Teste | O que confere |
| --- | --- | --- |
| Tabela de controle | `test_publish_requires_the_control_table` (sem conexão) | Uma conexão de mentira em que o `select ... limit 0` falha com relação inexistente: `publish_redshift` e `publication_status` levantam `PublicationError` com o comando de inicialização e não rodam outro comando; `control_ddl` sem `IF NOT EXISTS`. |
| Texto da transação | `test_publication_transaction_text` (sem conexão) | A sequência de comandos, um por item, com `BEGIN` e `COMMIT`, sem `TRUNCATE` nem `COMPUPDATE`, nomes em duas partes, credenciais mascaradas no que vai a log. |
| Reconciliação | `test_reconcile_published_add_column_and_recreate` | `ADD COLUMN` no aditivo; `DROP TABLE` mais DDL com chave no destrutivo e na largura de `VARCHAR(n)` que muda no modelo. |
| Diferença | `test_publish_only_changed_partitions` (`redshift`) | Duas publicações: a segunda, depois de uma partição alterada, emite um `DELETE` e um `COPY` só dela. |
| Primeira publicação | `test_first_publication_loads_every_partition` (`redshift`) | Sem linha de controle, todas as partições; a linha de controle escrita na mesma transação. |
| Falha no meio | `test_failed_copy_leaves_control_row_untouched` (`redshift`) | Um manifesto inválido na segunda partição: nenhuma partição trocada, controle intacto. |
| Estado | `test_publication_status_lists_pending_partitions` | A versão publicada, a atual e as partições pendentes por tabela. |
| Distribuição atribuída | `test_published_tables_distribution_is_read` (`redshift`) | `svv_table_info` depois da primeira publicação: `diststyle`, `sortkey1`, `tbl_rows` e `skew_rows` de cada tabela publicada, como leitura, nunca como reprovação; a visão negada também é leitura: o probe de 2026-09-23 (`RS-8`) recebeu `permission denied` (42501) nela depois do `USE`, e a fonte da leitura é decisão pendente. O modelo cliente não declara `redshift` e a distribuição é `AUTO` (decisão do usuário de 2026-09-21): é esta leitura que diz se uma `distkey` explícita se paga, e ela entraria por `ALTER TABLE`. |

## Rascunhos executados

O rascunho monta o texto da transação e da reconciliação; compilado, não executado num cluster
(2026-09-21). O `IF NOT EXISTS` do `control_ddl` dele saiu com a decisão do usuário de 2026-09-23.

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
- **[decisão] A fonte da leitura da distribuição atribuída.** O papel do projeto não lê
  `svv_table_info` depois do `USE` (`permission denied`, 42501, probe de 2026-09-23,
  [`POC.md`](POC.md)), e `test_published_tables_distribution_is_read` fica sem a visão que diria se
  uma `distkey` explícita se paga (a distribuição é `AUTO`, decisão do usuário de 2026-09-21). Uma
  fonte que o papel leia no esquema do datashare ainda não foi medida no ambiente alvo.

As decisões do usuário de 2026-09-23 sobre o `FILLRECORD`, o teto do documento JSON e a largura
de `VARCHAR(n)` estão escritas nas seções que as descrevem.
