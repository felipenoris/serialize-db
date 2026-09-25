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
  `COPY` mais longo que isso também não foi medido. A publicação monta a cláusula uma vez por tabela
  e a repete em todo `COPY` da transação, o que o usuário aceitou até a rodada de
  `probes/credentials.py` no alvo (decisão de 2026-09-25, [etapa 8](PLAN-STAGE-8.md)): o relatório
  diz quando a chave da cláusula troca e quando expira, para comparar com os 153,9 s da publicação
  de `cad_lancamentos` em 2026-09-24. `probes/credentials.py` segura esses clientes, menos o `COPY`,
  até passar a expiração da credencial do contêiner e a da senha do Redshift, em cerca de uma hora,
  e lê cada um a cada cinco minutos. No substituto de 2026-09-25, com chaves de 70 s, o `delta_scan`
  falhou com a chave vencida do secret, e só o `read_parquet` a renovou, numa renovação que a
  consulta seguinte desfaz quando o resultado fica aberto ([`POC.md`](POC.md)); se o alvo repetir
  isso, a biblioteca precisa renovar o secret do DuckDB por conta própria, e a forma de renovar
  espera o usuário.
- **O `PARALLEL OFF` e a reconexão do motor Redshift.** As suítes do motor e da publicação rodaram
  no ambiente alvo em 2026-09-24, duas vezes cada, e leram o que esperavam ([`POC.md`](POC.md)):
  ficam sem medida o `PARALLEL OFF` até 5.000.000 linhas na exportação e a reconexão depois de uma
  queda do servidor, que nenhum teste provoca lá ([etapa 5](PLAN-STAGE-5.md)).
- **O `deltalake` 1.6.6 no ambiente alvo.** `pyproject.toml` fixa `deltalake==1.6.6` desde
  2026-09-25, e as sessões locais e o substituto leram nela os mesmos números da 1.6.4
  ([`POC.md`](POC.md)). A pasta do ambiente alvo rodou as baterias de 2026-09-24 com a 1.6.4 e só
  recebe a 1.6.6, com o boto3 1.43.102 e o sqlglot 30.19.0, quando `prepare_offline.sh` roda de
  novo. O substituto dá chaves estáticas ao delta-rs, e a cadeia de credenciais do contêiner, cujas
  crates da AWS mudaram na 1.6.6, só a suíte S3 no alvo exercita. Espera o usuário: preparar a pasta
  de novo e rodar a suíte S3 no alvo.
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

- **A SQLAlchemy 2.1.** A 2.1.0, publicada em 2026-09-24, quebrou o pacote na sessão de testes
  de 2026-09-25 ([`POC.md`](POC.md)), e o pino fica em 2.0.54. O `params()` novo guarda os
  valores no statement: a compilação com `literal_binds` os escreve como `NULL`, e o `stream` do
  motor Redshift pelo `UNLOAD` roda com eles nulos, sem erro; o `construct_params()` de um `IN`
  expansível compilado com `render_postcompile` levanta `InvalidRequestError` no motor DuckDB e
  no cursor do motor Redshift. O `Double` deixou de derivar de `Numeric`, e o `load_report` para
  de somar as colunas `Double`. A reflexão do duckdb-engine 0.17.0 também falha na 2.1, fora do
  pacote. Espera o usuário: adaptar o pacote à 2.1 (`bound_statement`, de `serialize_db.sql`, que
  passa os valores por `params()`, e as colunas que o `load_report` soma) ou manter a 2.0.54.

- **Os tipos que o contrato aceita sem conferir.** A revisão da tabela de tipos de 2026-09-25
  ([`POC.md`](POC.md)) achou três casos no código de `serialize_db.schema`, que
  `docs/index.md` descreve como estão. O `cast` converte pelos 16 bytes o `arrow.uuid` que o
  PyArrow e o pandas inferem de um `uuid.UUID`, e quase todo UUID sai recusado com
  `Invalid UTF8 payload`, sem a instrução ao cliente; nem `cast` nem a auditoria medem os 36 bytes
  do `VARCHAR(36)`. O `Enum` entra como `String(n)`, com `n` do maior valor, e nada confere se o
  valor está na lista. O `Numeric` de precisão acima de 38 levanta o `ValueError` do PyArrow em
  `arrow_type`, e `check_models` o levanta em vez de listar a violação. Espera o usuário: levar o
  `arrow.uuid` ao texto canônico ou recusá-lo com a instrução, e medir o `Uuid` no `cast` e na
  auditoria; recusar o `Enum` em `check_models` ou conferir a lista; e listar o `Numeric` acima de
  38 como violação.

## Achados da revisão dos comentários e da documentação

A revisão de 2026-09-25 (PR #82) mudou só comentários, docstrings e prosa de `src/`, `scripts/`,
`probes/`, dos READMEs e de `docs/`, e os achados que pediam mudança de código, de texto impresso
ou de arquivo fora dela foram lidos no código da `main` e sondados na pasta local em 2026-09-25
([`POC.md`](POC.md)). Os corrigidos saíram daqui para o código, os testes e os arquivos das
etapas; os itens abaixo esperam o usuário: corrigir, ou aceitar como está.

- **Os erros sem tratamento de `serialize-db load`.** O `duckdb.Error` e o `RegistrationRefused`
  que `initial_load` documenta saem com o traceback e o código 1, como todo erro sem tratamento
  dos subcomandos (`docs/operacao.md`), enquanto a origem fora dos armazenamentos da biblioteca e
  o `ExecutionConflict` saem com 2 e uma linha. Espera o usuário: uma linha com a mensagem, sem o
  traceback que mostra onde a carga parou, ou o traceback como está.
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

## Decisões de API pendentes por etapa

Cada arquivo de etapa fecha com a seção "Decisões pendentes"; a lista abaixo as reúne, e uma decisão
tomada sai daqui e do arquivo da etapa no mesmo commit. Nenhuma etapa tem decisão pendente; os
itens que esperam o usuário fora dos arquivos de etapa estão na lista acima.
