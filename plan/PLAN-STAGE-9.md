# Etapa 9: operação

A entrega e o critério de aceite desta etapa estão na tabela de etapas de [`PLAN.md`](PLAN.md), que
também fixa as decisões, as regras que toda etapa obedece e a ordem do trabalho.

As primitivas são as da [etapa 3](PLAN-STAGE-3.md), mais `history` e `archive_snapshot` em
`serialize_db.delta`, e a inicialização da tabela de controle da [etapa 8](PLAN-STAGE-8.md); a
etapa entrega os subcomandos de `serialize_db.cli` e o runbook `docs/operacao.md`, implementados em
2026-09-24 na pasta local.

| Rotina | Quando | Comando |
| --- | --- | --- |
| Tabela de controle da publicação | Uma vez no esquema do Redshift, antes da primeira publicação de qualquer ambiente: `publish_redshift` recusa publicar sem ela ([etapa 8](PLAN-STAGE-8.md), decisão do usuário de 2026-09-23). | `serialize-db publish --init`. |
| Snapshot do banco | Na periodicidade do processo, por exemplo o fim do trimestre: dentro da execução marcada (`run.snapshot("2026T3")`, gravado no encerramento sem erro) ou fora dela, com a versão atual de cada tabela do ambiente que existe. | `serialize-db snapshot --name 2026T3`. |
| Compactação | Antes de um snapshot, nunca depois: o comando confere o arquivo de controle e recusa a tabela cujo snapshot está na versão atual. Também normaliza os arquivos que o `UNLOAD` gravou: `INT64` no lugar de `INT96` e de `FIXED_LEN_BYTE_ARRAY`, estatística em toda coluna ([etapa 3](PLAN-STAGE-3.md)). | `serialize-db compact --table ... --partitions ...`. |
| `vacuum` | Mensal: a lista com `keep_versions` do arquivo de controle, revisada, depois aplicada; `--full` de tempos em tempos para os órfãos. A retenção é de 400 dias (decisão do usuário de 2026-09-23), e `docs/index.md`, seção "Retenção dos arquivos removidos", diz como mudá-la. Num bucket versionado o espaço só é liberado pela regra `NoncurrentVersionExpiration`; `probes/bucket.py` (`BK-14`) mostra o acumulado. | `serialize-db vacuum [--apply] [--full] [--retention-hours 9600]`. |
| Arquivo | Anual: cada tabela do snapshot copiada para `arquivo/<nome>/<tabela>/`, pela cópia dos arquivos de cada partição e o registro deles (decisão do usuário de 2026-09-24), a entrada movida de `snapshots` para `archived` no mesmo arquivo de controle, a pasta com a regra de ciclo de vida. | `serialize-db archive --name 2026T3`. |
| Exportação | Sob demanda: as pastas Parquet por partição de uma versão da tabela, `copy` ou `rewrite`, num destino vazio sob a raiz. | `serialize-db export --table ... --destination ... [--version N] [--mode copy]`. |
| Auditoria avulsa | Depois de uma correção, e quando o SQL de uma verificação precisa ser lido. | `serialize-db audit --table ... [--sql]`. |
| Monitoração | Os commits de cada tabela com os metadados da biblioteca. | `serialize-db history --table ...`. |

Todos os subcomandos da etapa recebem `--metadata`, `--root` e `--environment` com os padrões
`SERIALIZE_DB_*` da [etapa 6](PLAN-STAGE-6.md), e saem com 0 ao terminar e 2 no erro de uso, no
nome repetido ou ausente, na compactação recusada, no destino inválido e no conflito de escrita do
arquivo de controle. A documentação da API sai do `pdoc`; o runbook, `docs/operacao.md`, lista cada
rotina com o comando, o que conferir antes e o que esperar depois, e entra na página de
`serialize_db.cli` pela docstring do módulo (decisão do usuário de 2026-09-23), ao lado da seção
"Retenção dos arquivos removidos" de `docs/index.md`. Testes: `tests/test_operation.py` sob a raiz
local (6 casos) e `tests/test_delta.py::test_deep_copy_and_relocation` para a cópia por registro.
Provas de conceito: `test_deltalake.py` (`test_vacuum_with_keep_versions`,
`test_compact_and_checkpoint`, `test_dataset_reader_and_deep_copy`,
`test_export_snapshot_by_copying_files`, `test_log_files`) e
`test_stdlib.py::test_exclusive_create_atomic_replace_and_fingerprint` (o arquivo de controle).

## A implementação

Os subcomandos `snapshot`, `vacuum`, `compact`, `archive`, `export` e `history` de
`serialize_db.cli`, `history`, `archive_snapshot` e o `deep_copy` novo de `serialize_db.delta`, o
runbook `docs/operacao.md` e os casos de `tests/test_operation.py` substituem a interface e o
rascunho executado em 2026-09-21: as assinaturas e as docstrings estão no código e na documentação
do `pdoc`. O que a implementação mudou do plano:

- **`deep_copy`** copia por `Storage.copy` cada arquivo que o log da versão lista, para o mesmo
  caminho relativo, e o registra com o tamanho, as linhas e as estatísticas da própria ação de
  origem (as dos tipos exatos, como `register_files`), num commit `overwrite` por partição sobre a
  tabela criada por `DeltaTable.create` com o esquema, o nome, a descrição e as propriedades da
  versão. A releitura é a contagem da cópia pelos dois leitores contra a soma das ações, e não
  `read_back`: a versão arquivada pode ter um esquema anterior ao modelo atual, e `register_files`
  conferiria o rodapé contra o contrato de hoje. A cópia termina numa versão por partição, não na
  0, e `_commit_actions` recebe o nome e a coluna de partição no lugar da tabela do SQLAlchemy.
  A repetição continua uma cópia interrompida: com tabela no destino, a partição cujos arquivos
  ela já registra é pulada, sem commit, e um destino que registra um arquivo fora da versão é
  `RegistrationRefused` (a falha de 2026-09-24 no alvo, [`POC.md`](POC.md)). A cópia reabre a
  tabela de destino antes do commit de cada partição, o que o commit não exige, porque ele
  resolve a versão pelo log no armazenamento (a sonda de 2026-09-21 em [`POC.md`](POC.md)); a
  reabertura custa uma leitura do log por partição e fica (decisão do usuário de 2026-09-25).
- **`compact`** recusa também a tabela sem partição cujo snapshot está na versão atual, e exige
  `--partitions` numa tabela particionada; a tabela sem partição com um arquivo só não commita.
- **`archive`** chama `deep_copy` em toda tabela do snapshot, e a repetição de um arquivamento
  interrompido continua de onde parou, porque `deep_copy` pula as partições já registradas; a
  entrada se move só depois da última cópia. A primeira versão pulava a tabela presente em
  `arquivo/<nome>/`, e teria dado por arquivada a `cad_lancamentos` pela metade que a falha de
  2026-09-24 deixou no alvo ([`POC.md`](POC.md)).
- **`export`** exige o destino sob a raiz (`Storage.relative`) e vazio (`list_files`), e
  `export_snapshot` confere a origem e o destino sob a raiz nos dois modos antes de gravar,
  porque o `COPY` particionado do DuckDB grava onde recebe.
- **`history`** devolve o instante do commit como `datetime` em UTC, e a linha de comando o imprime
  em ISO 8601; `get_add_actions(flatten=False)` traz `path`, `size_bytes`, `modification_time`,
  `num_records` e os structs `null_count`, `min`, `max` e `partition`, e `history()` do delta-rs
  traz os metadados do commit como chaves de primeiro nível ([`POC.md`](POC.md)).
- `snapshot` recusa o nome presente em `archived`, e `Execution.snapshot` herda a recusa no
  encerramento da execução marcada.

## Estratégia de implementação

- **`snapshot`** fora de uma execução lê a versão atual de cada tabela do ambiente que existe e
  grava a entrada; dentro da execução, `run.snapshot(name)` a grava no encerramento. O nome presente
  em `snapshots` ou em `archived` é recusado.
- **`vacuum`** roda `vacuum_keeping_snapshots(uri, control, table.name, storage, retention_hours,
  apply, full)` da [etapa 3](PLAN-STAGE-3.md) para cada tabela, com `keep_versions` do arquivo de
  controle; sem `--apply` imprime a lista por tabela; `--full` inclui os órfãos das escritas
  interrompidas e dos registros recusados. Dentro da retenção de 400 dias nada é listado, porque a
  retenção é a janela em que toda versão continua legível; a lista aparece quando um arquivo
  removido passa dela.
- **`compact`** confere o arquivo de controle e recusa a tabela cujo snapshot é a versão atual,
  porque a compactação depois do snapshot dobra os arquivos que ele referencia; depois roda
  `optimize.compact` das partições pedidas e imprime `numFilesAdded` e `numFilesRemoved` com o
  tempo e o pico de RSS do processo (decisão do usuário de 2026-09-24); uma partição com um só
  arquivo não commita. A compactação roda no escritor do delta-rs, fora do
  `memory_limit` do DuckDB, com as tarefas paralelas do padrão do delta-rs; a memória dela numa
  partição de `cad_lancamentos` não foi medida ([`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md)).
  O `optimize.compact` grava mínimo e máximo em toda coluna, e por isso `delta.compact` lê as ações
  `add` da versão atual e junta, por partição, as colunas `Double` que algum arquivo traz sem
  mínimo e máximo no log; cada grupo de partições com o mesmo conjunto é compactado numa chamada,
  com as propriedades de escrita de `_writer_properties`, as de `publish_partition`, e a partição
  compactada fica sem a estatística que a issue #59 tira (decisão do usuário de 2026-09-25). Uma
  coluna `Double` só de nulos num arquivo também sai do log sem mínimo e máximo, e perde a
  estatística na partição compactada. O caso chega ao `compact` depois de um `rewrite` de uma
  tabela com mais de 100 partições e partições de vários arquivos ([`POC.md`](POC.md)); o código
  ainda não segue a decisão, que espera a escolha da issue #85 sobre as colunas sem mínimo e
  máximo (decisão do usuário de 2026-09-25, [`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md),
  [`CURRENT_STATE.md`](CURRENT_STATE.md)).
- **`archive`** lê a entrada do snapshot, roda `deep_copy(<ambiente>/<tabela>, versão,
  <ambiente>/arquivo/<nome>/<tabela>, storage)` de cada tabela na versão registrada, imprime
  por tabela o tempo e o pico de RSS do processo, com o tempo de cada partição no log de
  `deep_copy` (decisão do usuário de 2026-09-24), e move a entrada de `snapshots` para a chave
  irmã `archived` do mesmo arquivo por `archive_snapshot`, na escrita condicional (decisão do
  usuário de 2026-09-23):
  `vacuum_keeping_snapshots` lê só `snapshots`, então a entrada arquivada deixa de prender as
  versões e o registro do snapshot fica no controle. Os dados não passam pela máquina, a memória é a
  dos rodapés e do log, e os arquivos ficam idênticos aos da origem, com o `INT96` e o
  `FIXED_LEN_BYTE_ARRAY` do `UNLOAD` inclusive; a normalização fica com `export --mode rewrite` e
  com a compactação, que reescrevem.
- **`export`** chama `export_snapshot` com `--mode copy` ou `rewrite` e imprime o tempo e o pico
  de RSS do processo; `--version` exporta uma versão antiga, com o DDL tirado do esquema daquela
  versão.
- **No ambiente alvo** (2026-09-24, sobre a raiz da carga pelo pacote): na bateria das 12:38,
  `history`, `snapshot` e `vacuum` rodaram, e `archive` copiou três tabelas e morreu no
  `CopyObject` do arquivo da partição 2026-06-30 de `cad_lancamentos`, abandonado pelo SDK da AWS
  depois de 3 segundos sem resposta ([`POC.md`](POC.md)), o que levou `Storage.copy` à
  transferência gerenciada do `boto3` ([etapa 3](PLAN-STAGE-3.md)); na das 16:51, sobre a raiz
  recarregada, os quatro rodaram inteiros, e `archive` copiou os 21 arquivos das 12 tabelas, um
  commit por partição, e moveu a entrada para `archived`, sem imprimir a duração, que as rotinas
  imprimem desde a decisão do usuário do mesmo dia; `export`, `compact` e a leitura das medidas lá
  esperam ([`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md)).
- **`history`** imprime, por tabela, versão, operação, carimbo e os metadados
  `serialize_db_execution_id`, `serialize_db_input_versions` e `serialize_db_snapshot`; os commits
  de `vacuum` (`VACUUM START`, `VACUUM END`) e de `OPTIMIZE` aparecem sem metadados.
- **O runbook** está em `docs/operacao.md`, publicado pelo `pdoc` com a docstring de
  `serialize_db.cli` que o inclui: uma seção por rotina com o comando, o que conferir antes (o
  arquivo de controle, a memória da máquina contra a medida da rotina na maior tabela, o destino) e
  o que esperar depois (a versão, a lista do `vacuum`, o `history`).

## Pré-requisitos e pós-condições

| Rotina | Pré-requisitos | Pós-condições |
| --- | --- | --- |
| `snapshot` | Nome inédito em `snapshots` e em `archived`; nenhuma execução aberta no ambiente. | A entrada com todas as tabelas existentes; 2 se outro escritor mudou o controle. |
| `vacuum` | Controle legível. | Sem `--apply`, nenhuma exclusão; com ele, a versão de cada snapshot continua legível e as intermediárias fora da retenção somem. |
| `compact` | Nenhum snapshot na versão atual da tabela; `--partitions` numa tabela particionada. | Menos arquivos na partição; o mesmo conteúdo; um commit `OPTIMIZE` com `dataChange` falso; as colunas `Double` que o log da partição trazia sem mínimo e máximo continuam sem eles. |
| `archive` | Snapshot registrado em `snapshots`. | Uma tabela nova por tabela do snapshot, uma versão por partição, com os mesmos arquivos e as mesmas somas; a entrada movida de `snapshots` para `archived`. |
| `export` | Destino vazio sob a raiz. | Pastas `<coluna>=<valor>/` sem `_delta_log`; em `copy`, os mesmos bytes; em `rewrite`, o esquema atual em todos os arquivos. |
| `history` | Tabela existente. | Uma linha por commit, sem credencial. |

## Testes por caso

`tests/test_operation.py` sob a raiz local, sobre o modelo de `tests/lancamentos_model.py`.

| Caso | Teste | O que confere |
| --- | --- | --- |
| Snapshot e histórico | `test_snapshot_records_every_table_and_history_shows_the_metadata` | A entrada com a versão atual de cada tabela existente; o nome repetido recusado; o `history` do mais recente ao mais antigo, com os três metadados nos commits da biblioteca e nenhum no `CREATE TABLE`. |
| Snapshot preso | `test_vacuum_keeps_the_snapshot_version` | Dentro da retenção nada é listado; com retenção zero, o arquivo da versão anterior ao snapshot é listado e, com `--apply`, apagado; a versão do snapshot lê e a anterior falha. |
| Compactação antes | `test_compact_refuses_after_a_snapshot_on_the_current_version` | Recusa quando o snapshot é a versão atual, também na tabela sem partição; compacta depois de uma versão nova, num commit `OPTIMIZE` que `version_diff` não conta; `--partitions` exigido na tabela particionada; a tabela fora do modelo é erro de uso; a linha impressa com o tempo e o pico de RSS. |
| Compactação sem estatística | `tests/test_delta.py::test_compact_keeps_the_columns_without_min_max` | Numa partição de dois arquivos, um sem mínimo e máximo de `valor` no log, o arquivo compactado sem eles no log e no rodapé e com os das outras colunas, com as mesmas linhas, o `NaN` incluído; outra partição, compactada na mesma chamada sem essa marca, com a estatística de `valor`. |
| Arquivo | `test_archive_copies_each_table_with_the_same_sums` | Uma versão por partição no arquivo, os mesmos arquivos e as mesmas somas da versão registrada, a entrada em `archived` e fora de `snapshots`, o `vacuum` sem a versão arquivada em `keep_versions`, `snapshot` recusando o mesmo nome, `archive` recusando o nome ausente e pulando a tabela já arquivada, com o tempo e o pico de RSS na linha de cada tabela. |
| Exportação | `test_export_by_copy_and_by_rewrite` | Os dois modos e uma versão antiga; o destino não vazio e o destino fora da raiz recusados; a linha impressa com o tempo e o pico de RSS. |
| Erros de uso | `test_cli_operation_usage_errors` | O nome ausente, o modo desconhecido, a tabela fora do modelo e a tabela sem Delta, sem traceback. |
| Ambiente padrão | `test_empty_environment_variable_counts_as_absent` | `SERIALIZE_DB_ENVIRONMENT` vazia vale `dsv`, como em `run`, `audit` e `publish`, e não um erro de uso. |
| Cópia profunda | `tests/test_delta.py::test_deep_copy_and_relocation` | O mesmo arquivo, caminho, tamanho e extremos da origem na cópia, uma versão por partição, as mesmas somas; a repetição sem commit, a cópia da versão 2 sobre a da 1 só com a partição que falta, e a versão 1 sobre a cópia da 2 recusada. |

## Decisões pendentes

Nenhuma. As decisões do usuário de 2026-09-23 sobre o lugar do runbook, `docs/operacao.md`, a
retenção de 400 dias do `vacuum` mensal e a entrada do snapshot arquivado, movida para a chave
irmã `archived`, a de 2026-09-24 sobre o `archive` pela cópia dos arquivos de cada partição e o
registro deles, e a de 2026-09-25 sobre as colunas `Double` sem mínimo e máximo na compactação,
estão escritas nas seções que as descrevem.
