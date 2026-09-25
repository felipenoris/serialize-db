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
  com 1.823 marcadores às 23:26; e 2.980 versões, 86.695.363 bytes, com 2.788 marcadores em
  2026-09-25 às 17:26, [`POC.md`](POC.md)), e a regra
  `NoncurrentVersionExpiration` sob a raiz, junto com `AbortIncompleteMultipartUpload`, é pergunta
  para quem administra o bucket. Sem ela, o `vacuum` da retenção de 400 dias não libera espaço;
  `docs/index.md`, seção "Retenção dos arquivos removidos", traz a regra de exemplo e como mudar a
  retenção.
- **Credenciais de uma hora.** A primeira execução mais longa que uma emissão,
  `probes/credentials.py` no alvo em 2026-09-25 ([`POC.md`](POC.md)), leu o delta-rs, o
  `S3FileSystem` e o `boto3` renovando a credencial do contêiner, a conexão Redshift aberta
  seguindo depois da expiração da senha de `GetCredentials` (3.600 s) e o `delta_scan` do DuckDB
  falhando uma vez depois que a chave do secret `credential_chain` expirou. Por decisão do usuário
  de 2026-09-25, o secret leva desde então a chave da credencial do `boto3`, e o motor DuckDB o
  recria na entrada de cada sessão quando ela troca ([etapa 3](PLAN-STAGE-3.md),
  [etapa 4](PLAN-STAGE-4.md)); no substituto de chaves de 70 s, o motor leu em todas as rodadas
  ([`POC.md`](POC.md)). Espera uma rodada no alvo: `probes/credentials.py`, que lê o DuckDB pelo
  motor desde então, com o comando em `SUITE.md`. Seguem sem medida um comando do DuckDB mais longo
  que os 15 minutos que a chave tem pela frente, no mínimo, na entrada da sessão (o botocore a
  renova entre 15 e 10 minutos antes da expiração), o `COPY` mais longo que a credencial que ele
  leva, a queda de uma conexão Redshift no meio de um `COPY` e a sessão ociosa e a transação
  inativa do serverless, encerradas depois de 3.600 s e 21.600 s ([`redshift.md`](redshift.md)).
  A cláusula do `COPY` e do `UNLOAD`, montada uma vez por tabela na publicação, leva uma chave com
  cerca de 29 minutos ou mais pela frente, contra os 153,9 s da publicação de `cad_lancamentos` em
  2026-09-24, e a decisão de 2026-09-25 da [etapa 8](PLAN-STAGE-8.md) fica; o motor da
  [etapa 5](PLAN-STAGE-5.md) reconecta uma vez por comando e perde só a tabela temporária que o
  pipeline tenha criado na sessão.
- **O `PARALLEL OFF` e a reconexão do motor Redshift.** As suítes do motor e da publicação rodaram
  no ambiente alvo em 2026-09-24, duas vezes cada, e leram o que esperavam ([`POC.md`](POC.md)):
  ficam sem medida o `PARALLEL OFF` até 5.000.000 linhas na exportação e a reconexão depois de uma
  queda do servidor, que nenhum teste provoca lá ([etapa 5](PLAN-STAGE-5.md)).
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
  O defeito vai ao delta-rs numa issue com o exemplo mínimo, que o usuário abre. O `compact` das
  `Double` sem mínimo e máximo da [etapa 9](PLAN-STAGE-9.md) espera esta escolha (decisão do
  usuário de 2026-09-25).

- **A operação no ambiente alvo.** Em 2026-09-24, nas baterias das 16:51 e das 23:25, a carga, a
  auditoria, `history`, `snapshot`, `vacuum`, `archive`, a publicação da base inteira e `export`
  rodaram sem erro, com o tempo e o pico de RSS de `archive`, `export` e da publicação lidos às
  23:25 ([`POC.md`](POC.md)). O `compact` rodou só sobre a partição 2026-03-31 de
  `cad_lancamentos`, que tem um arquivo só e não commita, e em 2026-09-25 saiu com a recusa
  prevista, porque `SUITE.md` o roda depois de um snapshot na versão atual: a compactação de uma
  partição de vários arquivos e a memória dela (o item acima) esperam uma partição com mais de um
  arquivo, que a carga não grava, e um `compact` antes do snapshot; a continuação de uma cópia
  interrompida do `archive` só o substituto exercitou.

- **O acesso de leitura no ambiente alvo.** A [etapa 10](PLAN-STAGE-10.md) rodou no alvo na
  bateria de 2026-09-25 ([`POC.md`](POC.md)): o leitor Delta abriu as 12 views da raiz carregada
  em 0,645 s, e as suítes passaram a publicação por canal e por snapshot, com a volta a um
  snapshot anterior, e a comparação dos dois leitores, com o `stream` do leitor Redshift pelo
  `UNLOAD`. Esperam: a publicação da base inteira por `--channel default` e a volta a um snapshot
  anterior ao publicado, com o tempo e o pico de RSS por tabela, com os comandos em `SUITE.md`; e
  o `UNLOAD` de um cliente com usuário só de leitura para um bucket próprio, com o caminho de
  credencial que serve a ele, que precisa de um papel de cliente no alvo.

- **A SQLAlchemy 2.1.** A 2.1.0, publicada em 2026-09-24, quebrou o pacote na sessão de testes
  de 2026-09-25 ([`POC.md`](POC.md)), e o pino fica em 2.0.54. O `params()` novo guarda os
  valores no statement: a compilação com `literal_binds` os escreve como `NULL`, e o `stream` do
  motor Redshift pelo `UNLOAD` roda com eles nulos, sem erro; o `construct_params()` de um `IN`
  expansível compilado com `render_postcompile` levanta `InvalidRequestError` no motor DuckDB e
  no cursor do motor Redshift. O `Double` deixou de derivar de `Numeric`, e o `load_report` para
  de somar as colunas `Double`. A reflexão do duckdb-engine 0.17.0 também falha na 2.1, fora do
  pacote. Espera o usuário: adaptar o pacote à 2.1 (`bound_statement`, de `serialize_db.sql`, que
  passa os valores por `params()`, e as colunas que o `load_report` soma) ou manter a 2.0.54.

## Achados da revisão dos comentários e da documentação

A revisão de 2026-09-25 (PR #82) mudou só comentários, docstrings e prosa de `src/`, `scripts/`,
`probes/`, dos READMEs e de `docs/`, e os achados que pediam mudança de código, de texto impresso
ou de arquivo fora dela foram lidos no código da `main` e sondados na pasta local em 2026-09-25
([`POC.md`](POC.md)). Os corrigidos e os aceitos como estão saíram daqui para o código, os testes
e os arquivos das etapas; o item abaixo espera uma rodada no alvo.

- **Os probes.** Uma correção num probe espera uma rodada no alvo que compare o relatório de antes
  com o de depois. `Report.finish` de `probelib.py` e `redshift.py` têm ternários aninhados;
  `redshift.py` usa `getattr` dinâmico, trabalha antes de um retorno antecipado e, com `space.py`,
  lê o `_expiry_time` privado das credenciais do botocore; `bucket.py` e `redshift.py` usam os
  identificadores `alcance` e `leitura`. O `RS-14` de `redshift.py` julga o host do Redshift como
  endpoint de API quando não há região, e o rótulo de `pg_settings` omite `wlm_query_slot_count`,
  que a consulta lê. O resumo impresso de `diagnose_aws.py` diz que a suíte não passa
  `AWS_ENDPOINT_URL` ao DuckDB, com "manutenção necessária", e a suíte passa
  (`create_duckdb_s3_secret` de `tests/conftest.py`); `describe` rotula um erro local como "sem
  resposta", e o arquivo repete helpers de `probelib.py`. `main` de `duckdb_threads.py` não tem as
  guardas de seção, e um `--metadata` que não importa sai com traceback e código 1. As
  justificativas dos `noqa` não terminam em ponto.

## Decisões de API pendentes por etapa

Cada arquivo de etapa fecha com a seção "Decisões pendentes"; a lista abaixo as reúne, e uma decisão
tomada sai daqui e do arquivo da etapa no mesmo commit. Nenhuma etapa tem decisão pendente; os itens
que esperam o usuário estão na lista acima.
