# Etapa 6: execução e linha de comando

A entrega e o critério de aceite desta etapa estão na tabela de etapas de [`PLAN.md`](PLAN.md), que
também fixa as decisões, as regras que toda etapa obedece e a ordem do trabalho.

`serialize_db.execution` é o ciclo de uma execução; `serialize_db.cli` o expõe.

| Primitiva | O que faz |
| --- | --- |
| `Database(root, environment, metadata, storage_options=None)` | A raiz do banco, o ambiente (`prod`, `dev`) e o `MetaData` dos modelos; `uri(table)` é `<root>/<ambiente>/<tabela>/`, mais o arquivo de controle e os prefixos `staging/`, `publicacao/` e `arquivo/`; chama `prepare_environment` e cria `Storage.for_uri(root)`. |
| `Execution(db, engine, partition, execution_id)` | Gerenciador de contexto: na entrada abre as tabelas de entrada, fixa `versions` e cria o sandbox; na saída descarta o sandbox e grava o resumo no log. As primitivas podem ser chamadas de qualquer thread, cada uma na conexão da sua thread, e o estado mutável (`versions`, auditorias aprovadas, o alocador) fica sob lock. |
| `run.previous_partitions(table, n)` | Os `n` últimos valores de partição da tabela na versão fixada até `run.partition`, inclusive, lidos das ações `add`; o calendário é do cliente, não da biblioteca. |
| `run.ingest(*tables, partitions=None, materialize=False, max_workers=1)` | `engine.ingest` de cada tabela na versão fixada, num `ThreadPoolExecutor(max_workers)` quando `max_workers > 1`, cada tarefa na conexão da sua thread; sem `partitions`, a tabela inteira. O ganho é no S3, onde a latência domina; em disco local o pool da instância já usa os núcleos. |
| `run.sandbox` | O motor, onde o pipeline chama `stream` e `loader`, os lotes na saída e na entrada, e `query`, `execute` e `load`, as formas por `pa.Table`, de qualquer thread; cada stream e cada loader roda num cursor próprio; `run.sandbox.connection` é a conexão crua da thread, para o que as primitivas não cobrem. |
| `run.next_ids(table, n)` | Um `range` de `n` inteiros contíguos, sob lock, a partir de `max_key(chave) + 1` na versão fixada da tabela, lido uma vez por tabela; as faixas de threads paralelas não se sobrepõem, e os ids de uma reexecução diferem. |
| `run.audit(table, partitions, foreign_keys=False, key_scope=None)` | `engine.audit`; a reprovação levanta `AuditFailed` e encerra sem tocar o Delta, e o relatório, com o SQL de cada verificação, vai para o log. |
| `run.publish(*tables, partitions, audit=True, max_workers=1, export_mode=None)` | Exige a auditoria aprovada de cada tabela nessas partições na própria execução, e `audit=False` dispensa a exigência e fica no log; confere que a versão atual de cada tabela é a fixada e aborta com `ExecutionConflict` quando outra execução a avançou; depois `create_table` se não existir, `reconcile`, `export_partition` e `publish_partition` por partição com `commit_metadata`, tabela a tabela num `ThreadPoolExecutor(max_workers)`: na primeira falha as tarefas em curso terminam, as não iniciadas são canceladas, e a exceção lista o resultado de cada tabela, porque os commits feitos ficam; avança `versions[table]` sob lock. O padrão 1 vem da memória por escrita (`delta.md`). `export_mode` (`"register"` ou `"rewrite"`, `SERIALIZE_DB_EXPORT_MODE` por omissão) vai ao `export_partition` do motor Redshift ([etapa 5](PLAN-STAGE-5.md)); o motor DuckDB escolhe pela regra da [etapa 4](PLAN-STAGE-4.md). |
| `run.publish_redshift(*tables, max_workers=1)` | A publicação da [etapa 8](PLAN-STAGE-8.md), uma conexão por tabela em paralelo, limitada pelas slots do WLM. |
| `run.snapshot(name)` | Marca a execução: `serialize_db_snapshot` nos commits e `snapshot(root, name, versions)` no encerramento. |
| `serialize-db run` | `--root`, `--environment`, `--engine`, `--partition`, `--execution-id` e `modulo:funcao` do pipeline, que recebe `run`; código de saída 0, 1 na reprovação da auditoria, 2 no conflito. |
| `serialize-db audit` | `--table`, `--partitions`, `--foreign-keys` e `--key-scope`; com `--sql` imprime o texto das verificações do dialeto escolhido e não abre conexão nem armazenamento, e `--write <pasta>` grava os arquivos das duas variantes. Sem `--sql`, roda a auditoria sobre a versão publicada e imprime o relatório. |

O log é o `logging` padrão com um resumo por execução: identificador, partição, versões lidas, versões
gravadas e tempo por passo. Testes: `tests/test_execution.py` sob a raiz local com o motor DuckDB:
a reexecução com o mesmo `execution_id` produz as mesmas linhas, com ids que podem diferir;
`next_ids` de duas threads devolve faixas disjuntas; a auditoria reprovada deixa a versão da tabela
como estava; de duas execuções publicando a mesma partição, a segunda aborta com `ExecutionConflict`, e
também a que publica uma tabela cuja versão avançou desde a abertura; `ingest` e `publish` com
`max_workers=2` dão o mesmo resultado que com 1. Provas de conceito: `test_stdlib.py` (`test_month_arithmetic`,
`test_execution_identifiers`, `test_context_manager_cleans_up_on_failure`,
`test_entry_point_by_import_string`, `test_command_line_parsing`, `test_execution_log`,
`test_prepare_environment`, `test_json_control_file_and_commit_metadata`) e
`test_deltalake.py::test_time_travel_and_restore` (os metadados de commit no histórico) e
`test_concurrency.py` (os leitores Delta presos à versão carregada durante um `append`, as faixas de
identificadores de um contador sob `Lock`) e `test_parallel.py` (o pool que termina o que está em
curso e cancela o resto, a barreira por tabela, `max_key` pelas estatísticas).
