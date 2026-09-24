# Etapa 6: execução e linha de comando

A entrega e o critério de aceite desta etapa estão na tabela de etapas de [`PLAN.md`](PLAN.md), que
também fixa as decisões, as regras que toda etapa obedece e a ordem do trabalho.

`serialize_db.execution` é o ciclo de uma execução; `serialize_db.cli` o expõe.

A [etapa 10](PLAN-STAGE-10.md) tira desta etapa `run.publish_redshift` e `serialize-db run
--redshift` (decisão do usuário de 2026-09-24): a publicação no Redshift passa a ser só
`serialize-db publish`, depois da execução, por snapshot ou canal. As linhas abaixo descrevem o
código até essa implementação.

| Primitiva | O que faz |
| --- | --- |
| `Database(root, environment, metadata)` | A raiz do banco, o ambiente (`prd`, `dev`, pela regra da partição, porque vira nome de pasta) e o `MetaData` dos modelos; `uri(table)` é `<root>/<ambiente>/<tabela>` a partir da raiz que `Storage.for_uri` normalizou, sem barra final, porque os caminhos das etapas 3 a 5 acrescentam `/<coluna>=<valor>/`, mais o arquivo de controle e os prefixos `staging/`, `publicacao/` e `arquivo/`; chama `prepare_environment`, e `storage` é o `Storage.for_uri(root)`, criado no primeiro uso. As opções do delta-rs saem de `storage.storage_options()` a cada chamada, sem credencial ([etapa 3](PLAN-STAGE-3.md)). |
| `Execution(db, engine, partition, execution_id=None, redshift=None)` | Gerenciador de contexto: na entrada abre as tabelas de entrada, fixa `versions` e cria o sandbox (`"duckdb"` é o `DuckDBEngine` da [etapa 4](PLAN-STAGE-4.md); `"redshift"` é o `RedshiftEngine` da [etapa 5](PLAN-STAGE-5.md), com a configuração `redshift`, uma `RedshiftConfig`, ou a das variáveis `SERIALIZE_DB_REDSHIFT_*` sem ela); `redshift` é também a configuração de `publish_redshift`, que sem ela é `PublicationError` (decisão do usuário de 2026-09-24); na saída descarta o sandbox, grava o snapshot marcado quando a execução termina sem erro e grava o resumo no log. As primitivas podem ser chamadas de qualquer thread, cada uma na sessão única do motor, sob o lock dele, e o estado mutável (`versions`, auditorias aprovadas, o alocador) fica sob lock. |
| `run.previous_partitions(table, n)` | Os `n` últimos valores de partição da tabela na versão fixada até `run.partition`, inclusive, lidos das ações `add`, na ordem de texto dos valores, que nos valores `AAAA-MM-DD` é a do calendário; o calendário é do cliente, não da biblioteca. |
| `run.ingest(*tables, partitions=None, materialize=False)` | `engine.ingest` de cada tabela na versão fixada; sem `partitions`, a tabela inteira. Uma tabela entra na sessão principal; mais de uma entram todas em paralelo, cada uma numa sessão a mais do motor (`new_session`), e a chamada volta quando todas terminam. Cada comando também usa o paralelismo do motor (as `threads` do DuckDB, as slices do Redshift). Em disco local, quatro tabelas de 150.000 linhas entraram em 0,017 s em quatro sessões e em 0,066 s em série (`test_parallel.py`, 2026-09-23). |
| `run.published(table)` | A versão fixada da tabela como origem de consulta, por `engine.published(table, db.uri(table), versions[table])`: `delta_scan('<uri>', version := <v>)` no DuckDB, a staging `exec_<id>_<tabela>_publicado` no Redshift ([etapa 4](PLAN-STAGE-4.md), [etapa 5](PLAN-STAGE-5.md)). É por ela que o pipeline lê as partições já publicadas da tabela que ele mesmo grava, cujo nome no sandbox pertence ao `loader` (decisão do usuário de 2026-09-22); numa tabela que ainda não existe, levanta `SandboxError`. |
| `run.sandbox` | O motor, onde o pipeline chama `stream` e `loader`, os lotes na saída e na entrada, e `query` e `load`, as formas por `pa.Table`, de qualquer thread; nos dois motores todo comando passa pela sessão única da execução, sob o lock que as primitivas tomam e soltam ([etapa 4](PLAN-STAGE-4.md), [etapa 5](PLAN-STAGE-5.md)); `with run.sandbox.session() as connection:` dá a conexão crua para o que as primitivas não cobrem, com o lock tomado pelo bloco e reentrante na mesma thread, e `with run.sandbox.new_session() as other:` abre uma sessão a mais para o que roda em paralelo, sem as tabelas temporárias da principal. |
| `run.next_ids(table, n)` | Um `range` de `n` inteiros contíguos da chave sequencial, a chave primária inteira de uma coluna, sob lock, a partir de `max_key(chave) + 1` na versão fixada da tabela, lido uma vez por tabela; as faixas de threads paralelas não se sobrepõem, e os ids de uma reexecução diferem. Numa tabela de chave primária composta o cliente decide os ids e não chama `next_ids` (decisão do usuário de 2026-09-23). |
| `run.audit(table, partitions, foreign_keys=False, key_scope=None)` | `engine.audit` com a URI e a versão fixada da tabela e, em `referenced`, as de cada tabela que uma chave estrangeira aponta; devolve o `AuditReport` aprovado e o guarda; a reprovação levanta `AuditFailed` e encerra sem tocar o Delta, e o relatório, com o SQL de cada verificação e até 20 linhas de amostra por verificação reprovada, vai para o log. |
| `run.publish(*tables, partitions=None, audit=True, max_workers=1)` | Exige a auditoria aprovada de cada tabela nessas partições na própria execução, e `audit=False` dispensa a exigência e fica no log; confere por `version_diff` que nenhuma alteração de dados entrou em cada tabela desde a versão fixada e aborta com `ExecutionConflict` quando entrou (um avanço só de metadados ou de manutenção passa e atualiza a versão fixada); depois `create_table` se não existir, `reconcile` e `export_partition` por partição com `commit_metadata`, tabela a tabela num `ThreadPoolExecutor(max_workers)`: na primeira falha as tarefas em curso terminam, as não iniciadas são canceladas, e a exceção lista o resultado de cada tabela, porque os commits feitos ficam; avança `versions[table]` sob lock. O padrão 1 vem da memória por escrita (`delta.md`). O `export_partition` dos dois motores registra por `register_files` o arquivo que o motor gravou, depois das conferências da [etapa 3](PLAN-STAGE-3.md), e o motor Redshift troca para `publish_partition` a partição com `Double` não finito ([etapa 4](PLAN-STAGE-4.md), [etapa 5](PLAN-STAGE-5.md)); a execução não escolhe o modo, porque o `rewrite` saiu das etapas 4 e 7 (decisão do usuário de 2026-09-24). A cada partição, `run.publish` passa a `export_partition`, em `columns_without_min_max`, as colunas `Double` com valor não finito que a auditoria da execução contou (`AuditReport.nonfinite_columns`), e todas as colunas `Double` da tabela com `audit=False`; a lista supõe a tabela sem mudança entre a auditoria e a publicação, como a própria aprovação (decisão do usuário de 2026-09-23, [issue #59](https://github.com/felipenoris/serialize-db/issues/59)). |
| `run.publish_redshift(*tables, max_workers=1)` | A publicação da [etapa 8](PLAN-STAGE-8.md) da versão fixada de cada tabela, com a configuração `redshift` da execução: `publication.publish_redshift(db, redshift, tables, execution_id, max_workers, versions)`, uma conexão por tabela em paralelo, limitada pelas slots do WLM; dois `COPY` e dois `UNLOAD` em conexões distintas passaram em paralelo no ambiente alvo em 2026-09-21 ([`POC.md`](POC.md)). Sem a configuração, `PublicationError`, sem tocar o Redshift. |
| `run.snapshot(name)` | Marca a execução: `serialize_db_snapshot` nos commits e `delta.snapshot(storage, environment, name, versions)` no encerramento ([etapa 3](PLAN-STAGE-3.md)). |
| `serialize-db run` | `--root`, `--environment`, `--engine`, `--partition`, `--execution-id`, `--redshift` (a configuração das variáveis `SERIALIZE_DB_REDSHIFT_*` para `run.publish_redshift` numa execução no motor DuckDB; com `--engine redshift` ela já entra), `--metadata modulo:atributo` (o `MetaData` dos modelos, como em `schema` e `sql`) e `modulo:funcao` do pipeline, que recebe `run`; código de saída 0, 1 na reprovação da auditoria, 2 no conflito. |
| `serialize-db audit` | `--metadata`, `--table`, `--partitions`, `--foreign-keys`, `--key-scope` e `--engine`, que escolhe o dialeto; com `--sql` imprime, para depuração, o texto das verificações desse dialeto e não abre conexão nem armazenamento. Sem `--sql`, com `--root` e `--environment`, roda a auditoria sobre a versão publicada, no motor de `--engine`, e imprime o relatório. Os padrões vêm das variáveis `SERIALIZE_DB_*`, como no `run`. |

O log é o `logging` padrão com um resumo por execução: identificador, partição, versões lidas, versões
gravadas e tempo por passo. Testes: `tests/test_execution.py` sob a raiz local com o motor DuckDB:
a reexecução com o mesmo `execution_id` produz as mesmas linhas, com ids que podem diferir;
`next_ids` de duas threads devolve faixas disjuntas; `ingest` de três tabelas as carrega em três
sessões a mais, a sessão principal lê as três, e a falha de uma lista o resultado das outras; a auditoria reprovada deixa a versão da tabela
como estava; de duas execuções publicando a mesma partição, a segunda aborta com `ExecutionConflict`, e
também a que publica uma tabela em que outra execução gravou dados desde a abertura, e não a que só foi compactada; `publish` com
`max_workers=2` dá o mesmo resultado que com 1. Provas de conceito: `test_stdlib.py` (`test_partition_values_in_text_order`,
`test_execution_identifiers`, `test_frozen_dataclass_derives_an_attribute_by_cached_property`,
`test_context_manager_cleans_up_on_failure`,
`test_entry_point_by_import_string`, `test_command_line_parsing`, `test_execution_log`,
`test_prepare_environment`, `test_json_control_file_and_commit_metadata`) e
`test_deltalake.py::test_time_travel_and_restore` (os metadados de commit no histórico) e
`test_concurrency.py` (os leitores Delta presos à versão carregada durante um `append`) e
`test_deltalake.py::test_partition_value_is_percent_encoded_in_the_folder_and_the_log` (a
codificação do valor de partição que a regra da partição evita).

`Database` exige o `MetaData`: sem ele a execução não sabe que tabelas abrir nem que esquema
reconciliar, e o exemplo de [`PLAN.md`](PLAN.md) passou a informá-lo. `Execution` aceita o nome do
motor (`"duckdb"`, `"redshift"`), ou um motor já construído, para os testes.

## Estratégia de implementação

- **`Database`** chama `prepare_environment` no `__post_init__` e monta os caminhos; não toca o
  armazenamento. `uri(table)` é `storage.uri_of("<ambiente>/<tabela>")`, a partir da raiz que
  `Storage.for_uri` normalizou: uma pasta local relativa vira absoluta, sem barra final, e é essa a
  URI que o delta-rs, o DuckDB e `storage.relative` recebem; os prefixos (`control_path`,
  `staging_prefix`, `publication_prefix`, `archive_prefix`) são caminhos relativos à raiz, os que os
  métodos de `Storage` recebem ([etapa 3](PLAN-STAGE-3.md)). `storage` é um
  `functools.cached_property` que cria `Storage.for_uri(root)` no primeiro uso: um campo `init=False` de um dataclass congelado não aceita a atribuição do
  `__post_init__` (`FrozenInstanceError`, leitura de 2026-09-23), e o `object.__setattr__` que a
  contornaria é a atribuição dinâmica que o estilo do projeto evita.
- **`Execution.__enter__`** abre toda tabela do `metadata` que existe no ambiente
  (`delta.table_exists`, e depois `delta.open_table`) e guarda a `DeltaTable` e a versão; a tabela ausente fica com `None`
  e nasce em `publish`. Toda leitura da execução usa esses objetos: `previous_partitions`, `ingest`,
  `next_ids`, `audit`. O `execution_id` ausente vira `exec-<AAAA-MM-DD>-<uuid8>`. A partição e o
  `execution_id` passam pela regra da partição, `re.fullmatch(r"[0-9A-Za-z][0-9A-Za-z_.-]*",
  valor)`: letra ou dígito no início, depois letras, dígitos, `_`, `.` e `-` (decisão do usuário de
  2026-09-23). Os dois viram nome de pasta e literal SQL: a aspa simples fecha o literal
  `'<valor>'` das etapas 3, 4, 5 e 8, e o delta-rs grava `:`, `%` ou um acento codificados no nome
  da pasta, que o `COPY` do `register` gravaria sem codificar ([`POC.md`](POC.md)); com a regra, o
  valor codificado é o próprio valor. A partição também cabe no `String(n)` da coluna de partição
  de cada tabela particionada do modelo, e cada valor de `partitions` que `ingest`, `audit` e
  `publish` recebem passa pela mesma regra. A data `AAAA-MM-DD`, o caso da base atual, e `2026-Q1`
  passam. O motor é construído aqui, com o `execution_id` e o `Storage`: `"duckdb"` é
  `DuckDBEngine(DuckDBConfig(), execution_id, db.storage)` da [etapa 4](PLAN-STAGE-4.md), que também
  confere o `execution_id` pela regra da partição e nasce numa pasta de `tempfile.mkdtemp`.
- **`__exit__`** chama `sandbox.cleanup()` aconteça o que acontecer e grava o resumo no log
  (`serialize_db.execution`): identificador, partição, versões lidas, versões gravadas, tempo por
  passo.
- **`previous_partitions`** lê `partition.<coluna>` de `get_add_actions(flatten=True)` da versão
  fixada, convertido para PyArrow por `pa.table(...)` (o delta-rs devolve uma tabela `arro3`),
  filtra `<= run.partition` e devolve os `n` maiores; o calendário é do cliente.
- **`ingest`** chama `sandbox.ingest(table, uri, versions[table], partitions, materialize)` por
  tabela. Com uma tabela, na sessão principal; com mais, um `ThreadPoolExecutor` com uma thread por
  tabela, e cada thread roda `with sandbox.new_session() as session: session.ingest(...)`. As
  tabelas do sandbox são tabelas comuns, e a sessão principal as vê no comando seguinte. Uma falha
  não cancela as ingestões que já rodam: todas terminam, e a exceção da primeira falha sobe com o
  resultado de cada tabela numa nota (`add_note`), mantendo o seu tipo; o que entrou fica para o
  `cleanup`. A tabela que não existe no ambiente é `SandboxError`.
- **`published`** devolve `sandbox.published(table, db.uri(table), versions[table])` e não cria
  objeto com o nome do modelo, que fica para o `loader` da tabela que a execução grava.
- **`next_ids`** guarda um contador por tabela sob `threading.Lock`, iniciado em
  `delta.max_key(dt, chave) + 1` na versão fixada, ou 1 numa tabela nova, e devolve `range(inicio,
  inicio + n)`. A chave é a chave primária inteira de uma coluna, a sequencial; numa chave composta
  o cliente decide os ids e não chama `next_ids` (decisão do usuário de 2026-09-23), e a chamada
  numa chave composta ou não inteira levanta `ContractError`.
- **`audit`** chama `sandbox.audit(table, partitions, uri, versions[table], foreign_keys,
  key_scope, referenced)`, com `referenced` montado de `versions` para cada tabela que uma chave
  estrangeira aponta, guarda sob lock o relatório aprovado por `(tabela, partições)`, escreve
  `report.sql()` no log e levanta `AuditFailed` na reprovação. Do relatório saem, por partição, a
  contagem (`report.rows(valor)`), que `publish` passa como `expected_rows`, e o
  `nonfinite_columns` ([etapa 4](PLAN-STAGE-4.md)): uma linha escrita no sandbox entre a auditoria e
  a publicação faz a exportação recusar com `RegistrationRefused`.
- **`publish`** exige o par aprovado (ou `audit=False`, que fica no log); para cada tabela, cria
  (`create_table`) quando `versions[table]` é `None`, senão confere que nenhuma **alteração de
  dados** entrou desde a versão fixada: `delta.version_diff(uri, versions[table], atual)` vazio.
  Um avanço só de metadados ou de manutenção (`reconcile` de outra execução, `compact`, `vacuum`,
  que gravam commits) não é conflito, e `versions[table]` passa para a versão atual; um avanço com
  dados é `ExecutionConflict`, porque duas execuções abertas na mesma versão publicariam a mesma
  faixa de `next_ids`. Depois `reconcile`, e `sandbox.export_partition(table, uri, valor,
  commit_metadata(...), expected_rows, columns_without_min_max)` por partição
  (`partitions=None` numa tabela sem partição é `[None]`), com, em `columns_without_min_max`, o `nonfinite_columns[valor]` da auditoria aprovada, ou todas as colunas
  `Double` da tabela com `audit=False`, e em `expected_rows` a contagem da auditoria, ou `None` com
  `audit=False`; as tabelas correm num `ThreadPoolExecutor(max_workers)`, a mesma função de pool
  que `ingest` usa com um worker por tabela: na primeira falha nada novo começa, o que está em curso
  termina, e a exceção da falha sobe com o resultado de cada tabela numa nota (concluída, falhou,
  cancelada), mantendo o seu tipo, que a linha de comando traduz no código de saída. Uma tabela só
  é entregue ao pool quando um worker está livre e nenhuma falha chegou: com todas entregues de uma
  vez, o worker único pegou a terceira tabela antes de o laço principal ver a falha da segunda
  (leitura de 2026-09-23, [`POC.md`](POC.md)). `versions[table]` avança sob lock a cada commit, e o
  dicionário devolvido é `{tabela: versão}`.
- **`publish_redshift`** chama `publication.publish_redshift` da [etapa 8](PLAN-STAGE-8.md) com a
  configuração `redshift` da execução e as versões fixadas, uma conexão por tabela no pool de
  `serialize_db._pool`, o mesmo de `publish` e de `ingest`.
- **`snapshot`** marca o nome, pela regra da partição, para `commit_metadata` dos commits
  seguintes e, no `__exit__` de uma execução sem erro, grava `delta.snapshot(storage, environment,
  name, versions)` com todas as tabelas do ambiente; a execução que falha não grava o snapshot.
- **`cli.main`** usa `argparse` com um subcomando por primitiva; `run` recebe `--root`,
  `--environment`, `--engine`, `--partition`, `--execution-id`, `--metadata modulo:atributo` (o
  `MetaData` dos modelos do pipeline) e o `modulo:funcao` do pipeline, com as variáveis
  `SERIALIZE_DB_*` como padrão; `--partition` e `--execution-id` passam pela regra da partição como `type` do `argparse`, e o
  `String(n)` fica para `Execution`, que tem o modelo. O código de saída é 0, 1 em `AuditFailed`,
  2 em `ExecutionConflict`, no `ContractError` da abertura (o motor que não serve, o `String(n)`) e
  no erro de uso do `argparse`, que também cobre o `modulo:atributo` que não importa. `audit` com
  `--sql` imprime o texto de `audit_sql` no dialeto de `--engine`, com o sentinela `{prefix}`; sem
  `--sql`, abre um `DuckDBEngine` próprio, ingere a versão atual da tabela nas partições pedidas e
  roda a auditoria contra ela, com código 0 na aprovação e 1 na reprovação. `run` e `audit`
  configuram o `logging` em `INFO`.
  O `run` importa os módulos antes de abrir a execução, para a importação preguiçosa não correr ao
  lado das threads da biblioteca.

## Pré-requisitos e pós-condições

| Primitiva | Pré-requisitos | Pós-condições |
| --- | --- | --- |
| `Database` | Raiz alcançável; `metadata` aprovado por `check_models`. | Caminhos montados; ambiente normalizado. |
| `Execution.__enter__` | Partição e `execution_id` pela regra da partição, a partição dentro do `String(n)`; motor configurável. | `versions` com toda tabela do ambiente; o sandbox aberto; o log com as versões lidas. |
| `previous_partitions` | Tabela existente e particionada. | Os `n` últimos valores até a partição da execução, inclusive, em ordem crescente. |
| `next_ids` | Tabela com chave primária inteira de uma coluna; outra chave é `ContractError`. | Faixas contíguas e disjuntas entre threads, a primeira acima do máximo da versão fixada. |
| `audit` | Sandbox com a tabela. | O par aprovado registrado, ou `AuditFailed` com o relatório no log; o Delta intocado. |
| `publish` | Auditoria aprovada; nenhuma alteração de dados na tabela desde a versão fixada. | Uma versão por partição com os metadados, sem mínimo e máximo nas colunas `Double` com valor não finito; `versions` avançado; `ExecutionConflict` sem commit no conflito; na falha de uma tabela, as demais terminam ou são canceladas e a exceção lista cada resultado. |
| `__exit__` | Nenhum. | Sandbox descartado, staging apagado, resumo no log, snapshot gravado quando marcado. |
| `serialize-db run` | `--metadata` e o pipeline importáveis. | Código de saída pelo resultado; nenhum traceback no erro de uso. |

## Testes por caso

`tests/test_execution.py` sob a raiz local com o motor DuckDB e com um motor de mentira que registra
as chamadas.

| Caso | Teste | O que confere |
| --- | --- | --- |
| Abertura | `test_execution_opens_every_table_and_fixes_versions` | `versions` com as tabelas existentes e `None` nas ausentes; o log com as versões. |
| Partição | `test_execution_refuses_an_invalid_partition` | O valor vazio, com `/`, `=`, espaço, `'`, `:`, `%` ou acento, começado por `.`, `_` ou `-`, ou acima do `String(n)` da coluna de partição é recusado antes de o sandbox abrir, e o `execution_id` fora da regra também; `2026-08-31` e `2026-Q1` passam. |
| Partições anteriores | `test_previous_partitions_up_to_the_execution_partition` | Só valores até a partição da execução, os `n` últimos, em ordem. |
| Identificadores | `test_next_ids_are_disjoint_across_threads` | Duas threads, faixas disjuntas, o início acima do máximo lido das estatísticas; a tabela nova começa em 1; a chave composta é `ContractError`. |
| Auditoria exigida | `test_publish_requires_the_audit` | `publish` sem `audit` é `AuditFailed`; com `audit=False` passa e o log registra. |
| Reprovação | `test_failed_audit_leaves_the_delta_untouched` | A versão da tabela não muda; `__exit__` descarta o sandbox. |
| Reexecução | `test_rerun_with_the_same_execution_id_produces_the_same_rows` | As mesmas linhas, ids que podem diferir, uma versão a mais. |
| Conflito | `test_publish_aborts_when_data_changed_since_open` | Um `append` de outra execução depois da abertura é `ExecutionConflict`; um `compact` não é. |
| Mesma partição | `test_two_executions_on_the_same_partition_conflict` | A segunda aborta no `CommitFailedError`. |
| Paralelismo | `test_publish_with_two_workers_matches_one` | O mesmo resultado com `max_workers=1` e `2`; com um worker, a falha da segunda tabela deixa a primeira concluída e a terceira cancelada, e a nota lista cada resultado. |
| `Double` não finito | `test_publish_passes_the_nonfinite_columns_to_the_export` | O motor de mentira recebe, por partição, as colunas `Double` com valor não finito que a auditoria contou, a partição sem elas recebe a lista vazia, e com `audit=False` recebe todas as colunas `Double`; no motor DuckDB, a partição com `NaN` sai sem o mínimo e o máximo da coluna. |
| Metadados | `test_commit_metadata_in_history` | `serialize_db_execution_id` e `serialize_db_input_versions` no `history`; `serialize_db_snapshot` só na execução marcada. |
| Snapshot | `test_snapshot_writes_the_control_file_at_exit` | Todas as tabelas do ambiente na entrada, gravada uma vez. |
| Linha de comando | `test_cli_run_parses_and_exits_by_result` | `--partition` e `--execution-id` fora da regra saem com 2; `--metadata` obrigatório; o pipeline que não importa sai com 2; `AuditFailed` sai com 1; `ExecutionConflict` com 2; o `--export-mode` é recusado com 2; nenhum traceback. |
| Auditoria avulsa | `test_cli_audit_prints_the_sql_and_audits_the_published_version` | `--sql` imprime o texto no dialeto pedido, sem armazenamento; sem ele, a auditoria da versão publicada sai com 0; a tabela fora dos modelos sai com 2. |
| Ingestão paralela | `test_ingest_of_several_tables_uses_extra_sessions` | Várias tabelas em sessões a mais, lidas pela sessão principal; a falha de uma leva o resultado das outras na nota. |

## A implementação

O módulo `serialize_db.execution` (`Database`, `Execution`), `AuditFailed` em
`serialize_db.errors`, os subcomandos `run` e `audit` de `serialize_db.cli` e os casos de
`tests/test_execution.py` substituem a interface e o rascunho executado em 2026-09-21 e em
2026-09-22: as assinaturas e as docstrings estão no código e na documentação do `pdoc`, e
`Database` e `Execution` são importados de `serialize_db`. O que a implementação mostrou está em
[`POC.md`](POC.md), seção "O que a implementação da etapa 6 mostrou".

## Decisões pendentes

Nenhuma. As decisões que o usuário tomou em 2026-09-23 estão escritas nas seções que as descrevem:
`--metadata modulo:atributo` no `serialize-db run`, como em `schema` e `sql`; `next_ids` só na
chave sequencial, a chave primária inteira de uma coluna, porque numa chave composta o cliente
decide os ids; e a regra da partição, `[0-9A-Za-z][0-9A-Za-z_.-]*`, no valor da partição e no
`execution_id`. A barreira por tabela não entra nas etapas: um `load` esquecido numa thread faz a
leitura concorrente falhar, porque o `loader` cria a tabela no `close` e recusa o nome ocupado nos
dois motores ([etapa 4](PLAN-STAGE-4.md), [etapa 5](PLAN-STAGE-5.md)).
