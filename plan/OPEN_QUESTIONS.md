# Questões em aberto

Este documento registra o que ainda não tem resposta: as pendências do projeto, as decisões que
esperam o usuário e as perguntas que uma leitura ou um teste vai fechar. Um item sai daqui quando a
resposta entra no documento que a guarda, e a saída nomeia esse documento. O plano está em
[`PLAN.md`](PLAN.md), o estado da implementação em [`CURRENT_STATE.md`](CURRENT_STATE.md) e o que já
foi medido em [`POC.md`](POC.md).

- **Versões não correntes.** O bucket é versionado e o papel não lê o ciclo de vida: cada exclusão
  (o `vacuum`, a limpeza da suíte S3) deixa uma versão não corrente invisível à listagem. `BK-14`
  conta o acumulado (219 versões não correntes, 1.388.530 bytes, e 219 marcadores de exclusão
  sob a raiz dos probes em 2026-09-23; 366 versões, 16.345.479 bytes, com 358 marcadores sob a
  raiz nova em 2026-09-24 às 01:42, depois das três sessões de 2026-09-23; e 907 versões,
  32.966.477 bytes, com 859 marcadores às 12:39 do mesmo dia; e 1.943 versões, 50.394.018 bytes,
  com 1.823 marcadores às 23:26, [`POC.md`](POC.md)), e a regra
  `NoncurrentVersionExpiration` sob a raiz, junto com `AbortIncompleteMultipartUpload`, é pergunta
  para quem administra o bucket. Sem ela, o `vacuum` da retenção de 400 dias não libera espaço;
  `docs/index.md`, seção "Retenção dos arquivos removidos", traz a regra de exemplo e como mudar a
  retenção.
- **Credenciais de uma hora.** Nenhuma execução mais longa que uma emissão rodou ainda; a
  [etapa 3](PLAN-STAGE-3.md) resolve `storage_options` a cada chamada e não põe credencial nele
  (decisão do usuário de 2026-09-22), e a primeira execução longa no espaço confirma que o delta-rs
  renova pela cadeia padrão o `DeltaTable` que a execução segura. O secret `credential_chain` do
  DuckDB, de `storage.duckdb_setup` e do script de migração, guarda a chave e o token resolvidos no
  `CREATE SECRET`, e a documentação da extensão `aws` pede `REFRESH auto` para a credencial que
  expira ([`POC.md`](POC.md), sonda de 2026-09-24): os dois secrets levam `REFRESH auto` desde a
  decisão do usuário de 2026-09-24, e a renovação numa conexão que atravessa a rotação da
  credencial do contêiner, que às 22:50 de 2026-09-23 expirava em 46 minutos (`RS-18`), só uma
  execução longa no alvo mostra. O
  `S3FileSystem` do `Storage`, que o `stream` e o `loader` do motor Redshift e a leitura dos
  rodapés usam, guarda a cadeia de credenciais do SDK da AWS, que renova a credencial do contêiner
  por conta própria; nenhuma execução mediu essa renovação. A credencial do Redshift
  tem o mesmo teto (`GetCredentials`, 3600 segundos), e o serverless encerra a sessão ociosa há
  3.600 s e a transação inativa há 21.600 s ([`redshift.md`](redshift.md)): o que acontece com uma
  conexão aberta quando a senha expira, e se ela cai no meio de um `COPY`, ainda não foi medido; o
  motor da [etapa 5](PLAN-STAGE-5.md) reconecta uma vez por comando e perde só a tabela temporária
  que o pipeline tenha criado na sessão. As credenciais que o `COPY`
  e o `UNLOAD` levam no texto do comando expiram com as do espaço, e `RS-18` imprime quando; um
  `COPY` mais longo que isso também não foi medido. `probes/credentials.py` segura esses
  clientes, menos o `COPY`, até passar a expiração da credencial do contêiner e a da senha do
  Redshift, em cerca de uma hora, e lê cada um a cada cinco minutos. No substituto de
  2026-09-25, com chaves de 70 s, o `delta_scan` falhou com a chave vencida do secret, e só o
  `read_parquet` a renovou, numa renovação que a consulta seguinte desfaz quando o resultado
  fica aberto ([`POC.md`](POC.md)); se o alvo repetir isso, a biblioteca precisa renovar o
  secret do DuckDB por conta própria, e a forma de renovar espera o usuário.
- **O `PARALLEL OFF` e a reconexão do motor Redshift.** As suítes do motor e da publicação rodaram
  no ambiente alvo em 2026-09-24, duas vezes cada, e leram o que esperavam ([`POC.md`](POC.md)):
  ficam sem medida o `PARALLEL OFF` até 5.000.000 linhas na exportação e a reconexão depois de uma
  queda do servidor, que nenhum teste provoca lá ([etapa 5](PLAN-STAGE-5.md)).
- **A versão retirada do `deltalake`.** O `uv` avisou em 2026-09-25, ao instalar numa sonda a
  versão fixada em `pyproject.toml`, que `deltalake==1.6.4` está retirada (yanked) do PyPI, com o
  motivo "Issue: #4784", e a instalou assim mesmo ([`POC.md`](POC.md)). A issue #4784 do delta-rs,
  lida em 2026-09-25, é um `MERGE` numa tabela com o change data feed ligado que insere uma linha
  toda nula para cada linha que o predicado de `when_not_matched_insert` recusa, e afeta a 1.6.4 e a
  1.6.5; o pacote não usa `MERGE` nem o change data feed (busca por `.merge(`,
  `enableChangeDataFeed`, `change_data_feed` e `load_cdf` em `src/`, `scripts/`, `tests/` e
  `probes/`). A 1.6.5 também está retirada, e a 1.6.6, de 2026-09-24, traz a correção (PR #4785) e
  não está retirada ([`POC.md`](POC.md)). Espera o usuário: trocar a versão fixada pela 1.6.6, com
  as suítes rodadas nela.
- **A memória da compactação.** O `optimize.compact` do delta-rs roda fora do `memory_limit` do
  DuckDB, com as tarefas paralelas do padrão do delta-rs, e a memória dele numa partição de
  `cad_lancamentos` não foi medida ([etapa 9](PLAN-STAGE-9.md)); o `archive` saiu desse risco pela
  cópia dos arquivos de cada partição e o registro deles (decisão do usuário de 2026-09-24).
- **O filtro do dataset do delta-rs nas colunas sem mínimo e máximo.** O
  `DeltaTable.to_pyarrow_dataset()` do delta-rs, e com ele o `to_pyarrow_table` e o `to_pandas`
  com `filters`, perde as linhas de um filtro sobre uma coluna que o log deixa sem mínimo e máximo:
  o `filestats_to_expression_next` do delta-rs põe `coluna >= null` e `coluna <= null` na garantia
  de cada arquivo, e o PyArrow pula o arquivo. Na sonda de 2026-09-25 ([`POC.md`](POC.md)), um
  arquivo registrado pelo motor DuckDB leu 0 linhas em `valor > 1` (`Numeric(18, 2)`),
  `quando > '2025-12-31'` (`DateTime`) e `legado = true` (`Boolean`), contra 2, 2 e 1 pelo
  `delta_scan`. A perda é por arquivo, e o `IS NULL` e o `IS NOT NULL` só erram no arquivo que
  também não traz o `nullCount` da coluna: o primeiro perde os nulos, e o segundo os traz. Ficam
  sem os dois no log os tipos que o registro omite ([etapa 3](PLAN-STAGE-3.md)), entre eles as
  colunas `DateTime` e `Boolean` do modelo cliente; o texto dos arquivos do `UNLOAD`; e as
  `Double` com valor não finito da issue #59, em todo caminho de escrita, e sem o `nullCount` na
  troca do motor Redshift e no `compact` da [etapa 9](PLAN-STAGE-9.md). O `delta_scan` dos
  motores e do leitor Delta e o `COPY` do Redshift leem certo, e o pacote filtra o dataset do
  delta-rs só pela coluna da partição (`read_back`), fora da perda. Com
  `delta.dataSkippingStatsColumns` sem as três colunas, a mesma sonda leu 2, 2 e 1, e o `delta_scan`
  seguiu podando pelas estatísticas que o log já guarda; a propriedade faz o `write_deltalake`
  gravar só as estatísticas das colunas dela e o `get_add_actions` esconder as outras. A issue #3032
  do delta-rs, aberta em 2024-11-25 e fechada com o rótulo `mre-needed`, relata o mesmo sintoma num
  filtro fora da partição, sem a causa; nenhuma issue aberta trata do caso, e o defeito segue na
  1.6.6 e no `main`. O `DeltaTable.scan`, o `QueryBuilder` e o `scan_delta` do Polars leem certo, e
  `docs/index.md`, seção "Ler a base com o modelo", lista os leitores ([`POC.md`](POC.md)). Espera o
  usuário: pôr a propriedade nas tabelas, com as colunas inteiras e de data, que todo caminho de
  escrita grava com mínimo e máximo; gravar no log mínimo e máximo largos, que o protocolo aceita
  com `tightBounds` falso, nos tipos que o registro omite e nas `Double` da issue #59; ou deixar o
  pacote como está. A issue #85 acompanha o item, com um exemplo autocontido que reproduz a perda.
  O defeito vai ao delta-rs numa issue com o exemplo mínimo, que o usuário abre.

- **A operação no ambiente alvo.** Em 2026-09-24, nas baterias das 16:51 e das 23:25, a carga, a
  auditoria, `history`, `snapshot`, `vacuum`, `archive`, a publicação da base inteira e `export`
  rodaram sem erro, com o tempo e o pico de RSS de `archive`, `export` e da publicação lidos às
  23:25 ([`POC.md`](POC.md)). O `compact` rodou só sobre a partição 2026-03-31 de
  `cad_lancamentos`, que tem um arquivo só e não commita: a compactação de uma partição de vários
  arquivos e a memória dela (o item acima) esperam uma partição com mais de um arquivo; a
  continuação de uma cópia interrompida do `archive` só o substituto exercitou.

- **O acesso de leitura no ambiente alvo.** A [etapa 10](PLAN-STAGE-10.md), implementada em
  2026-09-25 na pasta local e no substituto, espera as leituras que só o alvo dá, com os comandos
  em `SUITE.md`: o tempo de abertura do leitor Delta sobre as 12 tabelas da raiz carregada, com
  uma view por tabela (8,7 ms por view na pasta local, [`POC.md`](POC.md)); a publicação por
  `--channel default` e a volta a um snapshot anterior ao publicado, com o tempo e o pico de RSS
  por tabela; e o `UNLOAD` de um cliente com usuário só de leitura para um bucket próprio, com o
  caminho de credencial que serve a ele, que precisa de um papel de cliente no alvo.

## Achados da revisão dos comentários e da documentação

A revisão de 2026-09-25 (PR #82) mudou só comentários, docstrings e prosa de `src/`, `scripts/`,
`probes/`, dos READMEs e de `docs/`. Os itens abaixo pedem mudança de código, de texto impresso ou
de arquivo fora dela, e cada um espera o usuário: corrigir, ou aceitar como está. Foram lidos no
código da `main` e, onde o item diz, sondados na pasta local em 2026-09-25 ([`POC.md`](POC.md)).

- **Os códigos de saída da CLI.** O `ContractError` da entrada da execução escapa do `try` de
  `cli._run` e sai com traceback e código 1, sondado em `run --engine redshift` sem conexão e,
  com ela, num `--execution-id` de 60 caracteres, que não deixa os 63 bytes do prefixo; a
  docstring de `_run` e a [etapa 6](PLAN-STAGE-6.md) dizem 2. Na sonda, saem do mesmo jeito
  `publish --init` e `audit --engine redshift` sem conexão, e `load --source gs://...`, pelo
  `ValueError` de `initial_load`, que documenta ainda `duckdb.Error`, `RegistrationRefused` e
  `ExecutionConflict` sem captura na CLI; `publish --status` sem conexão sai com 2. No `audit`,
  esse 1 se confunde com o da auditoria reprovada. A abertura de `docs/operacao.md` diz que todo
  subcomando recebe `--metadata` e sai com 0 ou 2, e `publish --init` dispensa `--metadata`, e
  `audit` sai com 1 na reprovação.
- **O `nullCount` de `file_from_footer`.** A coluna sem estatística no rodapé, como o timestamp
  `INT96` do `UNLOAD`, entra no log com `nullCount` 0, e a docstring, `RegisteredFile.stats` e
  [`parquet.md`](parquet.md) a deixam de fora; com esse 0, o dataset do delta-rs leu 0 linhas em
  `IS NULL`, contra 3 pelo `delta_scan` (sonda). O `statistics_enabled="NONE"` de
  `delta._writer_properties` tira também o `nullCount` da coluna, com a mesma leitura.
- **As bordas da API.** `delta.export_snapshot(mode="rewrite")` numa tabela particionada grava fora
  da raiz sem erro, e só a CLI confere o destino antes. `DuckDBEngine.export_partition` com um
  valor numa tabela sem partição grava o arquivo e só então levanta o `ContractError` de
  `register_files`, e o arquivo fica na pasta da tabela, fora do log (sonda). A mensagem de
  `delta.channel_snapshot(control, "current")` sugere `serialize-db channel --name current`, que
  `set_channel` recusa; só a chamada direta chega a ela. `publish_redshift` com uma tabela fora de
  `versions` diz que a tabela não existe no ambiente. `RedshiftReader` aceita `unload_to` numa
  pasta local, e o `UNLOAD` real grava só no S3; o caminho local serve ao substituto.
- **As credenciais da publicação.** `publication._publication_transaction` monta
  `credentials_clause` uma vez por tabela e a põe em todo `COPY` da transação, e a
  [etapa 5](PLAN-STAGE-5.md) pede a cláusula montada por comando, porque as credenciais expiram; a
  publicação de `cad_lancamentos` levou 153,9 s em 2026-09-24.
- **Os nomes, as anotações e as guardas.** `PublishedColumn` fica fora do `__all__` de
  `serialize_db.publication` e está na assinatura pública de `reconcile_published`;
  `published_name` se diz protegida, e só o módulo a usa. O `version: int` de `ingest` aceita
  `None` nos dois motores, que o recusam com `SandboxError`. `RedshiftEngine._reconnect` usa
  `contextlib.suppress(Exception)`, contra a regra do `except Exception` sem relançar.
  `deep_copy` reabre a tabela de destino a cada partição, e o commit não precisa disso (a revisão
  de 2026-09-21 em [`POC.md`](POC.md)). A guarda `if actions.num_rows:` de
  `Execution.previous_partitions` não muda o resultado: numa tabela sem arquivos,
  `get_add_actions(flatten=True)` já traz a coluna `partition.<coluna>` vazia (sonda).
- **As docstrings e as páginas que o código desmente.** A docstring de `serialize_db.errors` diz
  que a execução captura as exceções de `serialize_db.delta`, e [`PLAN.md`](PLAN.md) dá o mesmo
  motivo ao módulo; a execução não captura nenhuma, a CLI captura `ConflictError` e
  `ExecutionConflict`, e nenhum módulo captura `RegistrationRefused`, `SchemaDiffRefused` e
  `LogUnavailable`. Os resumos de `ContractError` e de `SandboxError` não cobrem a configuração
  sem conexão, o `execution_id` longo demais para o prefixo e as credenciais ausentes do `COPY`,
  que o código levanta com elas. A prosa de `Execution.publish` diz que `export_partition`
  registra o arquivo, e no motor Redshift a partição com `Double` não finito passa por
  `publish_partition`. As docstrings de `engine/redshift.py` citam `staging/<execution_id>/...`,
  que não é o layout do `unload_to` do leitor, e o log do construtor chama de sandbox a sessão do
  leitor. O pdoc liga "o `stream` do motor" de `reader.py` a `DeltaReader.stream`, e "o `close`"
  do `:return:` dos dois `stream` ao `close` do leitor. O `'7306MiB'` do exemplo de
  `environment_limits` não tem leitura que o confirme. `docs/index.md` diz que cada execução cria
  as tabelas "a partir do DDL do modelo", e o `ingest` do DuckDB abre as de entrada como view ou
  `CREATE ... AS SELECT` de `delta_scan`.
- **Os probes.** Uma correção num probe espera uma rodada no alvo que compare o relatório de antes
  com o de depois. `Report.finish` de `probelib.py` e `redshift.py` têm ternários aninhados;
  `redshift.py` usa `getattr` dinâmico, trabalha antes de um retorno antecipado e, com `space.py`,
  lê o `_expiry_time` privado das credenciais do botocore; `bucket.py` e `redshift.py` usam os
  identificadores `alcance` e `leitura`. O `RS-14` de `redshift.py` julga o host do Redshift como
  endpoint de API quando não há região, e o rótulo de `pg_settings` omite `wlm_query_slot_count`,
  que a consulta lê. O resumo impresso de `diagnose_aws.py` diz que a suíte não passa
  `AWS_ENDPOINT_URL` ao DuckDB, com "manutenção necessária", e a suíte passa
  (`duckdb_s3_secret` de `tests/conftest.py`); `describe` rotula um erro local como "sem
  resposta", e o arquivo repete helpers de `probelib.py`. `main` de `duckdb_threads.py` não tem as
  guardas de seção, e um `--metadata` que não importa sai com traceback e código 1. As
  justificativas dos `noqa` não terminam em ponto.
- **Os arquivos fora da revisão.** [`PLAN-STAGE-8.md`](PLAN-STAGE-8.md) ficou atrás da etapa 10: o
  parágrafo da transação e a pós-condição de `publish_redshift` tratam a versão publicada acima
  da pedida como `ExecutionConflict`, e a publicação volta a ela por `version_diff(min, max)`; a
  tabela dos testes cita `test_execution_publishes_to_redshift_and_the_cli`, que não existe mais,
  e o fim do arquivo cita a seção "Rascunhos executados", que saiu. `tests/test_probes.py`
  escreve "verdicto" num comentário, e o comentário "A migração adiantada" de `pyproject.toml`
  narra história.

## Decisões de API pendentes por etapa

Cada arquivo de etapa fecha com a seção "Decisões pendentes"; a lista abaixo as reúne, e uma decisão
tomada sai daqui e do arquivo da etapa no mesmo commit. Nenhuma etapa tem decisão pendente; os
itens que esperam o usuário fora dos arquivos de etapa estão na lista acima.
