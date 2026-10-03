# Etapa 6: execução e linha de comando

A entrega e o critério de aceite desta etapa estão na tabela de etapas de [`PLAN.md`](PLAN.md), que
também fixa as decisões, as regras que toda etapa obedece e a ordem do trabalho.

`serialize_db.execution` é o ciclo de uma execução; `serialize_db.cli` o expõe.

A [etapa 10](PLAN-STAGE-10.md) tirou desta etapa `run.publish_redshift` e
`serialize-db run --redshift` em 2026-09-25 (decisão do usuário de 2026-09-24): a publicação no
Redshift é só `serialize-db publish_redshift`, depois da execução, por snapshot ou canal; `Database`
ganhou `open_delta` e `open_redshift`, descritos no arquivo dessa etapa.

| Primitiva | O que faz |
| --- | --- |
| `Database(root, environment, metadata)` | A raiz do banco, o ambiente (`prd`, `dsv`, pela regra da partição, porque vira nome de pasta) e o `MetaData` dos modelos; `uri(table)` é `<root>/<ambiente>/<tabela>` a partir da raiz que `Storage.for_uri` normalizou, sem barra final, porque os caminhos das etapas 3 a 5 acrescentam `/<coluna>=<valor>/`, mais o arquivo de controle e os prefixos `staging/`, `publicacao/` e `arquivo/`; chama `prepare_environment`, e `storage` é o `Storage.for_uri(root)`, criado no primeiro uso. As opções do delta-rs saem de `storage.storage_options()` a cada chamada, sem credencial ([etapa 3](PLAN-STAGE-3.md)). |
| `Execution(delta_db, engine, partition=None, execution_id=None, redshift=None)` | Gerenciador de contexto: na entrada abre as tabelas de entrada, fixa `versions` e cria o sandbox (`"duckdb"` é o `DuckDBEngine` da [etapa 4](PLAN-STAGE-4.md); `"redshift"` é o `RedshiftEngine` da [etapa 5](PLAN-STAGE-5.md), com a configuração `redshift`, uma `RedshiftConfig`, ou a das variáveis `SERIALIZE_DB_REDSHIFT_*` sem ela, e a `SERIALIZE_DB_REDSHIFT_PORT` que não é número é `ContractError` na entrada); na saída descarta o sandbox, grava o snapshot marcado quando a execução termina sem erro e grava o resumo no log, que diz `concluída` só quando o descarte e a gravação do snapshot terminam, e `com erro` quando a execução ou um dos dois falha, com a exceção subindo ao cliente. As primitivas podem ser chamadas de qualquer thread, cada uma na sessão única do motor, sob o lock dele, e o estado mutável (`versions`, auditorias aprovadas, o alocador) fica sob lock. Sem `partition`, a execução não tem partição, como a do pipeline que só atualiza tabelas sem partição (decisão do usuário de 2026-09-27). |
| `run.previous_partitions(table, n)` | Os `n` últimos valores de partição da tabela na versão fixada, a da abertura ou a que `publish_delta` avançou, até `run.partition`, inclusive, lidos das ações `add`, na ordem de texto dos valores, que nos valores `AAAA-MM-DD` é a do calendário; o calendário é do cliente, não da biblioteca. A execução sem partição é `ContractError`. |
| `run.ingest(*tables, partitions=None, materialize=False)` | `engine.ingest` de cada tabela na versão fixada; sem `partitions`, a tabela inteira. Uma tabela entra na sessão principal; mais de uma entram todas em paralelo, cada uma numa sessão a mais do motor (`new_session`), e a chamada volta quando todas terminam. Cada comando também usa o paralelismo do motor (as `threads` do DuckDB, as slices do Redshift). Em disco local, quatro tabelas de 150.000 linhas entraram em 0,017 s em quatro sessões e em 0,066 s em série (`test_parallel.py`, 2026-09-23). |
| `run.pinned_delta(table)` | A versão fixada da tabela como origem de consulta, por `engine.pinned_delta(table, db.uri(table), versions[table])`: `delta_scan('<uri>', version := <v>)` no DuckDB, a staging `exec_<id>_<tabela>_versao_<versão>` no Redshift, uma por versão pedida (decisão do usuário de 2026-09-25) ([etapa 4](PLAN-STAGE-4.md), [etapa 5](PLAN-STAGE-5.md)). É por ela que o pipeline lê a versão fixada da tabela que ele mesmo grava, cujo nome no sandbox a execução cria por `create_table` e preenche pelo `appender` (decisões do usuário de 2026-09-22 e de 2026-09-28); numa tabela que ainda não existe, levanta `SandboxError`. |
| `run.sandbox` | O motor, onde o pipeline chama `stream` e `appender`, os lotes na saída e na entrada, `query` e `append`, as formas por `pa.Table`, e `create_table`, a tabela vazia do modelo, de qualquer thread; nos dois motores todo comando passa pela sessão única da execução, sob o lock que as primitivas tomam e soltam ([etapa 4](PLAN-STAGE-4.md), [etapa 5](PLAN-STAGE-5.md)); `with run.sandbox.session() as connection:` dá a conexão crua para o que as primitivas não cobrem, com o lock tomado pelo bloco e reentrante na mesma thread, e `with run.sandbox.new_session() as other:` abre uma sessão a mais para o que roda em paralelo, sem as tabelas temporárias da principal. |
| `run.next_ids(table, n)` | Um `range` de `n` inteiros contíguos da chave sequencial, a chave primária inteira de uma coluna, sob lock, a partir de `max_key(chave) + 1` na versão fixada da tabela, lido uma vez por tabela, no primeiro pedido, na versão fixada desse momento, a da abertura ou a que `publish_delta` avançou; as faixas de threads paralelas não se sobrepõem, e os ids de uma reexecução diferem. Numa tabela de chave primária composta o cliente decide os ids e não chama `next_ids` (decisão do usuário de 2026-09-23). |
| `run.audit(table, partitions, foreign_keys=False, key_scope=None)` | `engine.audit` com a URI e a versão fixada da tabela e, em `referenced`, as de cada tabela que uma chave estrangeira aponta; devolve o `AuditReport` aprovado e o guarda; a reprovação levanta `AuditFailed` e encerra sem tocar o Delta, e o relatório, com o SQL de cada verificação e até 20 linhas de amostra por verificação reprovada, vai para o log. Numa tabela sem partição, a lista não filtra, a auditoria é da tabela inteira e a aprovação fica sob a lista, que `publish_delta`, que só aceita `None` nessa tabela, não procura: documentado, sem recusa (decisão do usuário de 2026-10-01). A lista vazia é `ContractError`, porque o `IN ()` falso aprovaria sem auditar (decisão do usuário de 2026-10-02). |
| `run.publish_delta(*tables, partitions=None, audit=True, max_workers=1)` | Exige a auditoria aprovada de cada tabela nessas partições na própria execução, e `audit=False` dispensa a exigência e fica no log; confere por `version_diff` que nenhuma alteração de dados entrou em cada tabela desde a versão fixada e aborta com `ExecutionConflict` quando entrou (um avanço só de metadados ou de manutenção passa e atualiza a versão fixada); depois `create_table` se não existir, `reconcile` e `export_partition` por partição com `commit_metadata`, tabela a tabela num `ThreadPoolExecutor(max_workers)`: na primeira falha as tarefas em curso terminam, as não iniciadas são canceladas, e a exceção lista o resultado de cada tabela, porque os commits feitos ficam; avança `versions[table]` sob lock. O padrão 1 vem da memória por escrita (`delta.md`). O `export_partition` dos dois motores registra por `register_files` o arquivo que o motor gravou, depois das conferências da [etapa 3](PLAN-STAGE-3.md), e o motor Redshift troca para `publish_partition` a partição com `Double` não finito ([etapa 4](PLAN-STAGE-4.md), [etapa 5](PLAN-STAGE-5.md)); a execução não escolhe o modo, porque o `rewrite` saiu das etapas 4 e 7 (decisão do usuário de 2026-09-24). A cada partição, `run.publish_delta` passa a `export_partition`, em `columns_without_min_max`, as colunas `Double` com valor não finito que a auditoria da execução contou (`AuditReport.nonfinite_columns`), e todas as colunas `Double` da tabela com `audit=False`; a lista supõe a tabela sem mudança entre a auditoria e a publicação, como a própria aprovação (decisão do usuário de 2026-09-23, [issue #59](https://github.com/felipenoris/serialize-db/issues/59)). A lista vazia em `partitions` é `ContractError`, porque reconciliaria a tabela sem exportar (decisão do usuário de 2026-10-02). |
| `run.snapshot(name)` | Marca a execução: `serialize_db_snapshot` nos commits e `delta.snapshot(storage, environment, name, versions)` no encerramento ([etapa 3](PLAN-STAGE-3.md)); o nome já presente em `snapshots` ou em `archived` do arquivo de controle é `ContractError` na chamada, antes de qualquer commit, com a mensagem que manda marcar outro nome e apontar o canal para ele com `serialize-db channel` (decisão do usuário de 2026-09-28); a segunda chamada na mesma execução é `ContractError` com o nome marcado, porque os commits entre as chamadas levariam o primeiro nome e a entrada do snapshot o último (decisão do usuário de 2026-10-02). |
| `serialize-db run` | `--root`, `--environment`, `--engine`, `--partition` (opcional, como a partição do `Execution`), `--execution-id`, `--metadata modulo:atributo` (o `MetaData` dos modelos, como em `schema` e `sql`) e `modulo:funcao` do pipeline, que recebe `run`; código de saída 0, 1 na reprovação da auditoria, 2 no conflito na tabela e no arquivo de controle, no `ContractError` da abertura, do pipeline ou da saída, como a configuração do Redshift sem conexão ou com a `SERIALIZE_DB_REDSHIFT_PORT` que não é número, o `execution_id` longo demais para o prefixo do sandbox e o nome de snapshot já usado, e no erro de uso, como o motor fora de `duckdb` e `redshift`, em `--engine` ou em `SERIALIZE_DB_ENGINE`. |
| `serialize-db audit` | `--metadata`, `--table`, `--partitions`, `--foreign-keys`, `--key-scope` e `--engine`, que escolhe o dialeto; com `--sql` imprime, para depuração, o texto das verificações desse dialeto e não abre conexão nem armazenamento. Sem `--sql`, com `--root` e `--environment`, roda a auditoria sobre a versão atual do Delta, no motor de `--engine`, e imprime o relatório; sai com 1 na reprovação, também quando o `ingest` recusa um valor fora do contrato (no DuckDB, o nulo numa coluna `NOT NULL` e o JSON malformado; no Redshift, qualquer erro do driver), impresso como `ingestão: reprovada (<erro do banco>)` sem as outras contagens, e com 2 na tabela fora dos modelos ou sem Delta, em `--partitions` numa tabela sem partição, antes de abrir um motor e também com `--sql`, no motor fora de `duckdb` e `redshift` e na configuração do Redshift sem conexão ou com a porta que não é número. Os padrões vêm das variáveis `SERIALIZE_DB_*`, como no `run`. |

O log é o `logging` padrão com um resumo por execução: identificador, partição (ou `sem partição`),
versões lidas, versões gravadas e tempo por passo. Testes: `tests/test_execution.py` sob a raiz local com o motor DuckDB:
a reexecução com o mesmo `execution_id` produz as mesmas linhas, com ids que podem diferir;
`next_ids` de duas threads devolve faixas disjuntas; `ingest` de uma tabela a carrega na sessão
principal, e de duas as carrega em duas sessões a mais, a sessão principal lê as duas, e a falha
de uma lista o resultado das outras; a auditoria reprovada deixa a versão da tabela
como estava; de duas execuções publicando a mesma partição, a segunda aborta com `ExecutionConflict`, e
também a que publica uma tabela em que outra execução gravou dados desde a abertura, e não a que só foi compactada; `publish_delta` com
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
  (`delta.table_exists`, e depois `delta.open_table`) e guarda a `DeltaTable` e a versão; a tabela
  ausente fica com `None` e nasce em `publish_delta`. Toda leitura da execução usa a versão fixada:
  `ingest` e `audit` pelo número em `versions`, e `previous_partitions` e `next_ids` pela
  `DeltaTable` guardada, reaberta na versão fixada quando `publish_delta` a avançou. O
  `execution_id` ausente vira
  `exec-<AAAA-MM-DD>-<uuid8>`. A partição e o `execution_id` passam pela regra da partição,
  `re.fullmatch(r"[0-9A-Za-z][0-9A-Za-z_.-]*", valor)`: letra ou dígito no início, depois letras,
  dígitos, `_`, `.` e `-` (decisão do usuário de 2026-09-23). Os dois viram nome de pasta e literal
  SQL: a aspa simples fecha o literal `'<valor>'` das etapas 3, 4, 5 e 8, e o delta-rs grava `:`,
  `%` ou um acento codificados no nome da pasta, que o `COPY` do `register` gravaria sem codificar
  ([`POC.md`](POC.md)); com a regra, o valor codificado é o próprio valor. A partição também cabe no
  `String(n)` da coluna de partição de cada tabela particionada do modelo, e cada valor de
  `partitions` que `ingest`, `audit` e `publish_delta` recebem passa pela mesma regra. A data
  `AAAA-MM-DD`, o caso da base atual, e `2026-Q1` passam. A partição é opcional (decisão do usuário
  de 2026-09-27): sem ela, `run.partition` é `None` e as duas conferências da partição não rodam. O
  motor é construído aqui, com o `execution_id` e o `Storage`: `"duckdb"` é
  `DuckDBEngine(DuckDBConfig(), execution_id, db.storage)` da [etapa 4](PLAN-STAGE-4.md), que também
  confere o `execution_id` pela regra da partição e nasce numa pasta de `tempfile.mkdtemp`.
- **`__exit__`** chama `sandbox.cleanup()` aconteça o que acontecer e grava o resumo no log
  (`serialize_db.execution`): identificador, partição (ou `sem partição`), versões lidas, versões
  gravadas, tempo por passo. O resumo diz `concluída` só quando o descarte do sandbox e a gravação
  do snapshot marcado terminam, e `com erro` quando a execução ou um dos dois falha; a exceção sobe
  ao cliente.
- **`previous_partitions`** lê `partition.<coluna>` de `get_add_actions(flatten=True)` da versão
  fixada atual, a da abertura ou a que `publish_delta` avançou, com a `DeltaTable` reaberta nessa
  versão, convertido para PyArrow por `pa.table(...)` (o delta-rs devolve uma tabela `arro3`),
  filtra `<= run.partition` e devolve os `n` maiores; o calendário é do cliente. Na execução sem
  partição, levanta `ContractError`.
- **`ingest`** chama `sandbox.ingest(table, uri, versions[table], partitions, materialize)` por
  tabela. Com uma tabela, na sessão principal; com mais, um `ThreadPoolExecutor` com uma thread por
  tabela, e cada thread roda `with sandbox.new_session() as session: session.ingest(...)`. As
  tabelas do sandbox são tabelas comuns, e a sessão principal as vê no comando seguinte. Uma falha
  não cancela as ingestões que já rodam: todas terminam, e a exceção da primeira falha sobe com o
  resultado de cada tabela numa nota (`add_note`), mantendo o seu tipo; o que entrou fica para o
  `cleanup`. A tabela que não existe no ambiente é `SandboxError`.
- **`pinned_delta`** devolve `sandbox.pinned_delta(table, db.uri(table), versions[table])` e não
  cria objeto com o nome do modelo, que fica para `create_table` da tabela que a execução grava.
- **`next_ids`** guarda um contador por tabela sob `threading.Lock`, iniciado no primeiro pedido em
  `delta.max_key(dt, chave) + 1` na versão fixada desse momento, a que `publish_delta` pode ter
  avançado, ou 1 numa tabela sem versão fixada, e devolve `range(inicio,
  inicio + n)`. A chave é a chave primária inteira de uma coluna, a sequencial; numa chave composta
  o cliente decide os ids e não chama `next_ids` (decisão do usuário de 2026-09-23), e a chamada
  numa chave composta ou não inteira levanta `ContractError`.
- **`audit`** chama `sandbox.audit(table, partitions, uri, versions[table], foreign_keys,
  key_scope, referenced)`, com `referenced` montado de `versions` para cada tabela que uma chave
  estrangeira aponta, guarda sob lock o relatório aprovado por `(tabela, partições)`, escreve
  `report.sql()` no log e levanta `AuditFailed` na reprovação. Do relatório saem, por partição, a
  contagem (`report.rows(valor)`), que `publish_delta` passa como `expected_rows`, e o
  `nonfinite_columns` ([etapa 4](PLAN-STAGE-4.md)): uma linha escrita no sandbox entre a auditoria e
  a publicação faz a exportação recusar com `RegistrationRefused`. O log e as mensagens de `audit`
  e de `publish_delta` dizem `em [<valores>]`, ou `na tabela inteira` sem partições.
- **`publish_delta`** exige o par aprovado (ou `audit=False`, que fica no log); para cada tabela, cria
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
- **`snapshot`** confere o nome pela regra da partição, lê o arquivo de controle
  (`delta.read_snapshots`) e recusa com `ContractError` o nome já presente em `snapshots` ou em
  `archived`, antes de qualquer commit; depois marca o nome para `commit_metadata` dos commits
  seguintes e, no `__exit__` de uma execução sem erro, grava `delta.snapshot(storage, environment,
  name, versions)` com todas as tabelas do ambiente; a execução que falha não grava o snapshot, e o
  nome que outro escritor grava depois da marcação é o `ContractError` de `delta.snapshot` na
  saída, depois dos commits.
- **`cli.main`** usa `argparse` com um subcomando por primitiva; `run` recebe `--root`,
  `--environment`, `--engine`, `--partition` (opcional), `--execution-id`,
  `--metadata modulo:atributo` (o `MetaData` dos modelos do pipeline) e o `modulo:funcao` do
  pipeline, com as variáveis `SERIALIZE_DB_*` como padrão; `--partition` e `--execution-id` passam
  pela regra da partição como `type` do `argparse`, e o `String(n)` fica para `Execution`, que tem o
  modelo. O código de saída é 0, 1 em `AuditFailed`, 2 em `ExecutionConflict`, no `ConflictError`
  do arquivo de controle, que outro escritor mudou antes da gravação do snapshot na saída, no
  `ContractError` da abertura (o `String(n)`, a configuração do Redshift sem conexão, a
  `SERIALIZE_DB_REDSHIFT_PORT` que não é número, recusada por `RedshiftConfig.from_environment`), do
  pipeline e da saída (o nome de snapshot já usado, na chamada ou na saída) e no erro de uso do
  `argparse`, que também cobre o `modulo:atributo` que não importa e o motor fora de `duckdb` e
  `redshift`: o `type` de `--engine` confere também o padrão de `SERIALIZE_DB_ENGINE`, que o
  `choices` não confere. `audit` recusa com 2 `--partitions` numa tabela sem partição antes de abrir
  um motor, com e sem `--sql`; com `--sql` imprime o texto de `audit_sql` no dialeto de `--engine`,
  com o sentinela `{prefix}`; sem `--sql`, abre um sandbox próprio do motor de `--engine`, e outro
  nome é `ContractError`, nunca o DuckDB; ingere materializada a versão atual da tabela nas
  partições pedidas e roda a auditoria contra ela, com a versão atual de cada tabela referenciada,
  com código 0 na aprovação e 1 na reprovação; o relatório impresso diz `tabela inteira` nos totais
  da tabela sem partição. O `ingest` pelo DDL do modelo recusa o nulo numa coluna `NOT NULL` e o
  JSON malformado antes das verificações que os contariam: `ConstraintException` e
  `ConversionException` no DuckDB, e no Redshift um erro do driver que a classe não separa dos
  outros; a linha de comando imprime a recusa como `ingestão: reprovada (<erro do banco>)`, sem as
  outras contagens, e sai com 1, e qualquer outro erro do DuckDB, como o acesso negado a um arquivo,
  sobe com o traceback (decisão do usuário de 2026-09-28). `run` e `audit` configuram o `logging` em
  `INFO`. O `run` importa os módulos antes de abrir a execução, para a importação preguiçosa não
  correr ao lado das threads da biblioteca.

## Pré-requisitos e pós-condições

| Primitiva | Pré-requisitos | Pós-condições |
| --- | --- | --- |
| `Database` | Raiz alcançável; `metadata` aprovado por `check_models`. | Caminhos montados; ambiente normalizado. |
| `Execution.__enter__` | `execution_id` pela regra da partição; a partição, quando há, pela regra e dentro do `String(n)`; motor configurável. | `versions` com toda tabela do ambiente; o sandbox aberto; o log com as versões lidas. |
| `previous_partitions` | Tabela existente e particionada; execução com partição. | Os `n` últimos valores até a partição da execução, inclusive, em ordem crescente. |
| `next_ids` | Tabela com chave primária inteira de uma coluna; outra chave é `ContractError`. | Faixas contíguas e disjuntas entre threads, a primeira acima do máximo da versão fixada. |
| `audit` | Sandbox com a tabela. | O par aprovado registrado, ou `AuditFailed` com o relatório no log; o Delta intocado. |
| `publish_delta` | Auditoria aprovada; nenhuma alteração de dados na tabela desde a versão fixada. | Uma versão por partição com os metadados, sem mínimo e máximo nas colunas `Double` com valor não finito; `versions` avançado; `ExecutionConflict` sem commit no conflito; na falha de uma tabela, as demais terminam ou são canceladas e a exceção lista cada resultado. |
| `__exit__` | Nenhum. | Sandbox descartado, staging apagado, resumo no log, snapshot gravado quando marcado; o resumo diz `com erro` quando a execução, o descarte ou a gravação do snapshot falha. |
| `serialize-db run` | `--metadata` e o pipeline importáveis. | Código de saída pelo resultado; nenhum traceback no erro de uso. |

## Testes por caso

`tests/test_execution.py` sob a raiz local com o motor DuckDB e com um motor de mentira que registra
as chamadas.

| Caso | Teste | O que confere |
| --- | --- | --- |
| Abertura | `test_execution_opens_every_table_and_fixes_versions` | `versions` com as tabelas existentes e `None` nas ausentes; o log com as versões. |
| Partição | `test_execution_refuses_an_invalid_partition` | O valor vazio, com `/`, `=`, espaço, `'`, `:`, `%` ou acento, começado por `.`, `_` ou `-`, ou acima do `String(n)` da coluna de partição é recusado antes de o sandbox abrir, e o `execution_id` fora da regra também; `2026-08-31` e `2026-Q1` passam. |
| Partições anteriores | `test_previous_partitions_up_to_the_execution_partition` | Só valores até a partição da execução, os `n` últimos, em ordem; a tabela ausente e a tabela sem arquivos dão a lista vazia; a tabela sem partição e a execução sem partição são `ContractError`. |
| Execução sem partição | `test_execution_without_partition_publishes_a_table_without_partition` | A execução abre sem a conferência da partição, e o log diz `sem partição` e a auditoria `na tabela inteira`; a tabela sem partição nasce na primeira publicação, e a execução seguinte a traz inteira ao sandbox, acrescenta uma linha e a publica inteira. |
| Identificadores | `test_next_ids_are_disjoint_across_threads` | Duas threads, faixas disjuntas, o início acima do máximo lido das estatísticas; a tabela nova começa em 1; a chave composta é `ContractError`. |
| Versão avançada | `test_previous_partitions_and_next_ids_read_the_version_publish_delta_advanced` | Depois de `publish_delta`, `previous_partitions` e `next_ids` leem a versão fixada que ele avançou: a tabela ausente na abertura mostra a partição publicada e dá ids acima dos gravados, e a tabela aberta na versão 4 mostra a partição nova e o maior id dela. |
| Lista numa tabela sem partição | `test_audit_with_partitions_on_a_table_without_partition_audits_it_whole` | `run.audit` com uma lista numa tabela sem partição audita a tabela inteira e guarda a aprovação sob a lista; `publish_delta` dessa tabela recusa a lista com `ContractError` e, sem a auditoria com `None`, é `AuditFailed`; com ela, publica. |
| Auditoria exigida | `test_publish_delta_requires_the_audit` | `publish_delta` sem `audit` é `AuditFailed`; com `audit=False` passa e o log registra. |
| Lista de partições vazia | `test_the_empty_list_of_partitions_is_refused` | `audit` e `publish_delta` recusam a lista vazia com `ContractError`, sem chamar o motor nem criar a tabela. |
| Mensagens da tabela inteira | `test_messages_of_the_whole_table_name_it` | Sem partições, o log e as mensagens de `audit` e `publish_delta` dizem `na tabela inteira`, e não `None`: a auditoria reprovada, a publicação sem a auditoria aprovada e a publicação com `audit=False`. |
| Reprovação | `test_failed_audit_leaves_the_delta_untouched` | A versão da tabela não muda; `__exit__` descarta o sandbox. |
| Reexecução | `test_rerun_with_the_same_execution_id_produces_the_same_rows` | As mesmas linhas, ids que podem diferir, uma versão a mais. |
| Conflito | `test_publish_delta_aborts_when_data_changed_since_open` | Um `append` de outra execução depois da abertura é `ExecutionConflict`; um `compact` não é. |
| Mesma partição | `test_two_executions_on_the_same_partition_conflict` | A segunda aborta no `CommitFailedError`. |
| Paralelismo | `test_publish_delta_with_two_workers_matches_one` | O mesmo resultado com `max_workers=1` e `2`; com um worker, a falha da segunda tabela deixa a primeira concluída e a terceira cancelada, e a nota lista cada resultado. |
| `Double` não finito | `test_publish_delta_passes_the_nonfinite_columns_to_the_export` | O motor de mentira recebe, por partição, as colunas `Double` com valor não finito que a auditoria contou, a partição sem elas recebe a lista vazia, e com `audit=False` recebe todas as colunas `Double`; no motor DuckDB, a partição com `NaN` sai sem o mínimo e o máximo da coluna. |
| Metadados | `test_commit_metadata_in_history` | `serialize_db_execution_id` e `serialize_db_input_versions` no `history`; `serialize_db_snapshot` só na execução marcada. |
| Snapshot | `test_snapshot_writes_the_control_file_at_exit` | Todas as tabelas do ambiente na entrada, gravada uma vez; a execução que falha não a grava; o nome que outro escritor grava depois da marcação é `ContractError` na saída, depois do commit, e o resumo no log diz `com erro`. |
| Nome de snapshot usado | `test_snapshot_refuses_a_used_name_before_any_commit` | O nome em `snapshots` e o nome em `archived` são `ContractError` na chamada de `run.snapshot`, com `serialize-db channel` na mensagem; a tabela não é criada e o arquivo de controle não muda. |
| Snapshot marcado uma vez | `test_snapshot_is_marked_once_per_execution` | A segunda chamada de `run.snapshot`, com outro nome ou o mesmo, é `ContractError` com o nome marcado na mensagem; a execução segue marcada com o primeiro, gravado nos commits e na saída. |
| Nome de snapshot na linha de comando | `test_cli_run_exits_with_2_on_a_used_snapshot_name` | `serialize-db run` sai com 2 no nome em `snapshots` e em `archived`, com a versão do Delta igual, e também quando outro escritor grava o nome depois da marcação; nenhum traceback. |
| Ingestão recusada na auditoria | `test_cli_audit_prints_the_refused_ingest_as_a_failed_audit` | No DuckDB, o nulo numa coluna `NOT NULL` e o JSON malformado saem com 1 e a linha `ingestão: reprovada (...)`, sem traceback. |
| Ingestão recusada no Redshift | `test_cli_audit_on_redshift_prints_the_refused_ingest` | No substituto, com `--engine redshift`, as duas recusas saem com 1 e a mesma linha, que o relatório da sessão guarda para a bateria no alvo (`redshift.audit.refused_ingest.<mês>`). |
| Erro de acesso na auditoria | `test_cli_audit_lets_an_access_error_of_the_ingest_propagate` | Um `duckdb.IOException` do `ingest` sobe de `cli.main`, sem a linha de reprovação. |
| Linha de comando | `test_cli_run_parses_and_exits_by_result` | `--partition` e `--execution-id` fora da regra saem com 2; `--metadata` obrigatório; o pipeline que não importa sai com 2; `AuditFailed` sai com 1; `ExecutionConflict` com 2, e o `ConflictError` do arquivo de controle, que outro escritor grava entre a leitura e a escrita condicional do snapshot na saída, também; o `--export-mode` é recusado com 2; nenhum traceback. |
| Linha de comando sem partição | `test_cli_run_without_partition` | Sem `--partition`, o pipeline que publica a tabela sem partição sai com 0, e o que pede as partições anteriores com 2, sem traceback. |
| Configuração da execução | `test_cli_run_hands_the_redshift_config_to_the_execution` | `--engine redshift` constrói o motor Redshift com as variáveis `SERIALIZE_DB_REDSHIFT_*` e dá a configuração à execução; `--redshift` é erro de uso, com 2; no motor DuckDB a execução não tem a configuração nem `publish_redshift`. |
| Configuração do Redshift | `test_cli_exits_with_2_on_the_redshift_config_without_connection` | `run` e `audit` com `--engine redshift` e `publish_redshift --init` saem com 2 na configuração sem conexão e na `SERIALIZE_DB_REDSHIFT_PORT` que não é número, com a variável na mensagem nos três caminhos; com a conexão, `run` sai com 2 no `--execution-id` que não deixa 63 bytes ao nome; nenhum traceback. |
| Auditoria avulsa | `test_cli_audit_prints_the_sql_and_audits_the_current_version` | `--sql` imprime o texto no dialeto pedido, sem armazenamento; sem ele, a auditoria da versão atual do Delta sai com 0 e imprime os totais da partição; a tabela fora dos modelos sai com 2. |
| Auditoria de uma tabela sem partição | `test_cli_audit_of_a_table_without_partition` | `serialize-db audit` de uma tabela sem partição imprime os totais da `tabela inteira`; com `--partitions`, com ou sem `--sql`, sai com 2 antes de abrir um motor, sem traceback. |
| Motor desconhecido | `test_cli_refuses_an_unknown_engine` | `SERIALIZE_DB_ENGINE` fora de `duckdb` e `redshift` é erro de uso em `run` e em `audit`, com e sem `--sql`, como o mesmo valor em `--engine`; o motor da auditoria recusa o nome desconhecido com `ContractError` em vez de abrir o DuckDB. |
| Ingestão paralela | `test_ingest_of_several_tables_uses_extra_sessions` | Uma tabela na sessão principal, na thread de quem chama, sem `new_session`; várias em sessões a mais, fora da thread principal, lidas pela sessão principal; a falha de uma leva o resultado das outras na nota. |

`tests/test_pipeline.py` roda a execução completa sobre a base de testes em Delta, a base fictícia
de `tests/source_db_projetado.py` carregada por `import_table` com o modelo cliente:
`monthly_pipeline(run)`, no formato `modulo:funcao` de `serialize-db run`, ingere as tabelas do
modelo (as particionadas só na última data-base, materializadas pela DDL e pelo `INSERT` do
`delta_scan`), gera a partição do mês seguinte de `cad_operacoes`, `cad_contratos` e
`rel_contrato_operacao` por `INSERT ... SELECT` com os ids de `next_ids`, e a de `cad_lancamentos`
em pyarrow, a partir da última partição ingerida, acrescentada por `run.sandbox.append` à tabela
do `ingest`; roda `saldos_por_conta` de `client_model.statements` sobre a partição nova, confere o
rateio por contrato num `join` de três tabelas, audita as quatro com `foreign_keys=True` e as
publica por `publish_delta`. `test_monthly_pipeline_publishes_the_next_base_date` confere os saldos
e as contagens contra a base em memória, as auditorias sem verificação por rodar, a versão a mais de
cada tabela só com a partição nova alterada, os metadados do commit e a releitura pelo leitor Delta;
`test_following_month_runs_over_the_published_partition` roda o mesmo pipeline por
`serialize-db run` e, em seguida, a execução do mês seguinte sobre a partição publicada.

## A implementação

O módulo `serialize_db.execution` (`Database`, `Execution`), `AuditFailed` em
`serialize_db.errors`, os subcomandos `run` e `audit` de `serialize_db.cli` e os casos de
`tests/test_execution.py` substituem a interface e o rascunho executado em 2026-09-21 e em
2026-09-22: as assinaturas e as docstrings estão no código e na documentação do `pdoc`, e
`Database` e `Execution` são importados de `serialize_db`. O que a implementação mostrou está em
[`POC.md`](POC.md), seção "O que a implementação da etapa 6 mostrou".

## Decisões pendentes

Nenhuma. O `run.audit` com `partitions` numa tabela sem partição, que audita a tabela inteira
enquanto `ingest`, `publish_delta` e a linha de comando recusam, ficou documentado, sem recusa,
pela decisão do usuário de 2026-10-01, na linha de `run.audit`. As decisões que o usuário tomou
em 2026-09-23 estão escritas nas seções que as descrevem:
`--metadata modulo:atributo` no `serialize-db run`, como em `schema` e `sql`; `next_ids` só na
chave sequencial, a chave primária inteira de uma coluna, porque numa chave composta o cliente
decide os ids; e a regra da partição, `[0-9A-Za-z][0-9A-Za-z_.-]*`, no valor da partição e no
`execution_id`. A barreira por tabela não entra nas etapas: um `append` esquecido numa thread
deixa a leitura concorrente ver a tabela sem as linhas novas, que entram de uma vez no `close` do
`appender`, o efeito que o usuário aceitou em 2026-09-28 no lugar da falha com `CatalogException`
do `loader` que criava a tabela no `close`, a decisão de 2026-09-23 substituída
([etapa 4](PLAN-STAGE-4.md), [etapa 5](PLAN-STAGE-5.md)). A decisão do usuário de
2026-09-27 separa pelo nome a publicação no Delta da publicação no Redshift: `run.publish_delta` e
`run.pinned_delta`, com o `pinned_delta` dos motores, de um lado, e `serialize-db publish_redshift`
e `serialize_db.publication.publish_redshift` do outro.
