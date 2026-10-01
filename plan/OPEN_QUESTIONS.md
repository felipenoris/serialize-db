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
  2026-09-25 às 17:26; e 5.269 versões, 159.538.248 bytes, com 4.883 marcadores em 2026-09-26 às
  15:14; e 7.619 versões, 233.165.927 bytes, com 7.030 marcadores em 2026-09-27 às 15:59; e 8.822
  versões, 270.369.639 bytes, com 8.127 marcadores em 2026-09-28 às 20:14; e 10.301 versões,
  307.133.320 bytes, com 9.504 marcadores às 23:10; e ao menos 10.434 versões, 283.570.979 bytes,
  com 9.565 marcadores em 2026-09-29 às 13:32, quando a listagem de `BK-14` parou no limite de
  20.000 entradas, [`POC.md`](POC.md)), e a regra `NoncurrentVersionExpiration` sob a raiz, junto
  com `AbortIncompleteMultipartUpload`, é pergunta para quem administra o bucket. Sem ela, o
  `vacuum` da retenção de 400 dias não libera espaço; `docs/index.md`, seção "Retenção dos arquivos
  removidos", traz a regra de exemplo e como mudar a retenção.
- **Credenciais de uma hora.** `probes/credentials.py` leu no alvo, em 2026-09-25, em 2026-09-26, em
  2026-09-27 e em 2026-09-29 ([`POC.md`](POC.md)), o delta-rs, o `S3FileSystem` e o `boto3`
  renovando a credencial do contêiner, que troca de chave a cada cerca de 30 minutos, e a conexão
  Redshift aberta seguindo depois da expiração da senha de `GetCredentials` (3.600 s). O
  `delta_scan` do DuckDB, que falhou uma vez em 2026-09-25 com a chave vencida do secret
  `credential_chain`, leu em todas as rodadas de 2026-09-26, de 2026-09-27 e de 2026-09-29 pelo
  secret que leva a chave da credencial do `boto3` e que o motor recria na entrada de cada sessão
  quando ela troca (decisão do usuário de 2026-09-25, [etapa 3](PLAN-STAGE-3.md),
  [etapa 4](PLAN-STAGE-4.md)). Seguem sem medida um comando do DuckDB mais longo que os 15 minutos
  que a chave tem pela frente, no mínimo, na entrada da sessão (o botocore a renova entre 15 e 10
  minutos antes da expiração), o `COPY` mais longo que a credencial que ele leva, a queda de uma
  conexão Redshift no meio de um `COPY` e a sessão ociosa e a transação inativa do serverless,
  encerradas depois de 3.600 s e 21.600 s ([`redshift.md`](redshift.md)). A cláusula do `COPY` e do
  `UNLOAD` é montada a cada comando, no motor da [etapa 5](PLAN-STAGE-5.md) e, desde a decisão do
  usuário de 2026-09-26, na publicação da [etapa 8](PLAN-STAGE-8.md), que passou assim no alvo em
  2026-09-27, e leva uma chave com cerca de 29 minutos ou mais pela frente; o motor reconecta uma
  vez por comando e perde só a tabela temporária que o pipeline tenha criado na sessão.
- **O `PARALLEL OFF` e a reconexão do motor Redshift.** As suítes do motor e da publicação rodaram
  no ambiente alvo em 2026-09-24, duas vezes cada, e leram o que esperavam ([`POC.md`](POC.md)):
  ficam sem medida o `PARALLEL OFF` até 5.000.000 linhas na exportação e a reconexão depois de uma
  queda do servidor, que nenhum teste provoca lá ([etapa 5](PLAN-STAGE-5.md)).
  `probes/operacao/probe_unload_parallel.py` mede o primeiro, o `UNLOAD` da exportação com
  `PARALLEL OFF` e em paralelo de 1 a 20 milhões de linhas, e espera a rodada no alvo pelos
  comandos de `SUITE.md`, seção "Sondas da operação".
- **A memória da compactação.** O `optimize.compact` do delta-rs roda fora do `memory_limit` do
  DuckDB, com as tarefas paralelas do padrão do delta-rs, e a memória dele numa partição de
  `cad_lancamentos` não foi medida ([etapa 9](PLAN-STAGE-9.md)); o `archive` saiu desse risco pela
  cópia dos arquivos de cada partição e o registro deles (decisão do usuário de 2026-09-24).
  `probes/operacao/probe_compact_memory.py` a mede na primeira partição da origem repartida em
  cerca de 32 arquivos, e espera a rodada no alvo.
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
  23:25 ([`POC.md`](POC.md)). O `compact` rodou só sobre a partição 2026-03-31 de `cad_lancamentos`,
  que tem um arquivo só e não commita, e em 2026-09-25, em 2026-09-26, em 2026-09-27 e em 2026-09-29
  saiu com a recusa prevista, porque `SUITE.md` o roda depois de um snapshot na versão atual: a
  compactação de uma partição de vários arquivos e a memória dela (o item acima) esperam uma
  partição com mais de um arquivo, que a carga não grava, e um `compact` antes do snapshot; a
  continuação de uma cópia interrompida do `archive` só o substituto exercitou. Em 2026-09-28, a
  carga parou em `cad_lancamentos` 2026-07-31 com o `RegistrationRefused` de uma origem que mudava
  durante a leitura, e em 2026-09-29, com a origem estável, passou inteira numa raiz recarregada
  ([`POC.md`](POC.md)): a continuação de uma carga parada, que `tests/test_load.py` cobre na pasta
  local, e o `vacuum --full` de um arquivo fora do log seguem sem leitura no alvo. As sondas de
  `probes/operacao/` rodam essas leituras sobre as primeiras partições de `cad_lancamentos` da
  origem, a continuação do `archive` inclusive, e passaram na pasta local e no substituto em
  2026-09-30 ([`POC.md`](POC.md)); esperam a rodada no alvo, pelos comandos de `SUITE.md`, seção
  "Sondas da operação". O `COPY` da publicação de uma partição compactada, que o `compact`
  regrava em ZSTD, também não rodou lá.

- **O acesso de leitura no ambiente alvo.** A [etapa 10](PLAN-STAGE-10.md) rodou no alvo nas
  baterias de 2026-09-25, de 2026-09-26, de 2026-09-27 e de 2026-09-28 às 23:09
  ([`POC.md`](POC.md)): o leitor Delta abriu as 12 views da raiz carregada em 0,645 s, em 0,582 s,
  em 0,571 s e em 0,556 s; as suítes passaram a publicação por canal e por snapshot, com a volta a
  um snapshot anterior, e a comparação dos dois leitores, com o `stream` do leitor Redshift pelo
  `UNLOAD`, e na bateria de 2026-09-30 o runbook de refazer um snapshot, com a volta pelo canal; e
  em 2026-09-26 a base inteira foi publicada por `--channel default`, `cad_lancamentos` em 295,1 s
  com o pico do processo em 266 MB, e de novo em 2026-09-27, em 328,5 s com 270 MB, e em
  2026-09-29, em 335,2 s com 286 MB. Esperam: a volta a um snapshot anterior ao publicado sobre a
  base, com o tempo e o pico de RSS por tabela, que pede um commit depois do snapshot, fora do fluxo
  de `SUITE.md`, cujo passo 6 leu em cada bateria que cada versão já estava publicada; e o `UNLOAD`
  de um cliente com usuário só de leitura para um bucket próprio, com o caminho de credencial que
  serve a ele, que precisa de um papel de cliente no alvo.

- **A SQLAlchemy 2.1.** A 2.1.0, publicada em 2026-09-24, quebrou o pacote na sessão de testes
  de 2026-09-25 ([`POC.md`](POC.md)), e o pino fica em 2.0.54. O `params()` novo guarda os
  valores no statement: a compilação com `literal_binds` os escreve como `NULL`, e o `stream` do
  motor Redshift pelo `UNLOAD` roda com eles nulos, sem erro; o `construct_params()` de um `IN`
  expansível compilado com `render_postcompile` levanta `InvalidRequestError` no motor DuckDB e
  no cursor do motor Redshift. O `Double` deixou de derivar de `Numeric`, e o `load_report` para
  de somar as colunas `Double`. A reflexão do duckdb-engine 0.17.0 também falha na 2.1, fora do
  pacote. Espera o usuário: adaptar o pacote à 2.1 (`bound_statement`, de `serialize_db.sql`, que
  passa os valores por `params()`, e as colunas que o `load_report` soma) ou manter a 2.0.54.
- **O dialeto do DuckDB.** O `duckdb-engine` 0.17.0, o compilador do `render` e do motor DuckDB
  fixado em `pyproject.toml`, é de 2025-03-29, sem lançamento desde então, com 55 issues e 43 PRs
  abertos e a correção da reflexão da `pg_collation` parada num PR de 2026-03-28; o
  `duckdb-sqlalchemy` 1.5.5.9, a bifurcação de 2025-12-24 mantida por um autor, com 17 estrelas e
  8.973 downloads no mês contra 1.841.220, compila os mesmos statements byte a byte, passa todos
  os testes do pacote no lugar dele com a SQLAlchemy 2.0.54 e livra a reflexão da `pg_collation`
  na 2.1.0 ([`POC.md`](POC.md)). Espera o usuário: trocar a dependência (a fixação em
  `pyproject.toml`, `import duckdb_sqlalchemy` em `serialize_db.sql`, `serialize_db.engine.duckdb`
  e `tests/proof_of_concept/test_sqlalchemy.py`, cuja asserção da chave primária não refletida
  passa a refleti-la, a lista de `probes/space.py` e a prosa que nomeia o dialeto em `README.md`,
  `docs/index.md`, `plan/` e `CLAUDE.md`) ou manter o `duckdb-engine` enquanto a 2.0.54 o serve.

## Achados das sondas de consistência de leitura e escrita

As sondas de 2026-09-25 ([`POC.md`](POC.md), seção "O que as sondas de consistência de leitura e
escrita mostraram") atravessaram cada fronteira de leitura e escrita com valores de borda e
trabalho paralelo, e acharam o que segue, reproduzido sem mudar `src/`; as rodadas delas no ambiente
alvo, em 2026-09-26, em 2026-09-27 e em 2026-09-29, repetiram os achados sem reprovar checagem
([`POC.md`](POC.md)). Cada item espera o usuário: corrigir, ou aceitar como está.

- **O sinal do zero pelo `COPY` do DuckDB.** O escritor Parquet do DuckDB codifica a coluna
  `DOUBLE` por dicionário e trata `-0.0` e `0.0` como o mesmo valor: numa partição com os dois,
  todos saem com o sinal do primeiro que apareceu. Atinge o `export_partition` do motor DuckDB,
  `initial_load`, `rewrite` e `export_snapshot(mode="rewrite")`; `publish_partition` e `compact`,
  pelo escritor do delta-rs, guardam o sinal, e o `UNLOAD` do Redshift não foi lido. A diferença
  aparece em `1 / x`, em `math.copysign` e no texto do valor, nunca numa comparação ou numa soma.
  Opções: `DICTIONARY_SIZE_LIMIT 0` no `COPY` (sem dicionário em coluna alguma, arquivo maior), ou
  registrar a perda na linha do `Double` da tabela de tipos de `docs/index.md`.
- **A soma de controle da auditoria acima de 1e32.** `audit` soma cada `Double` e `Numeric` como
  `DECIMAL(38, 6)` (`_totals` de `serialize_db.audit`): um valor finito de magnitude 1e32 ou mais
  falha no `CAST` (`ConversionException`), e uma soma acima disso estoura (`OutOfRangeException`),
  o que derruba `audit` e impede `publish_delta` (`audit=False` dispensa, com aviso). A base de
  produção fica em 1e18. Opções: somar o `Double` como `DOUBLE` (a soma de controle deixa de ser
  exata, como já é a coluna), ou capturar o estouro e registrar a soma como não lida.
- **A escrita condicional do arquivo de controle entre threads.** Na pasta local,
  `Storage.write_text(if_match=...)` confere a impressão digital e faz o `os.replace` fora de um
  lock: oito threads somando 50 cada perderam 293 de 400 atualizações, e 321 na máquina do alvo. A
  docstring diz que a escrita não é atômica entre processos; entre threads do mesmo processo ela
  também não é, e `snapshot`, `archive_snapshot` e `set_channel` chamados em paralelo numa raiz
  local (duas `Execution` com `snapshot` encerrando ao mesmo tempo, por exemplo) podem perder uma
  entrada. No S3 o `IfMatch` é do servidor. Opções: um `threading.Lock` de `Storage` em volta de
  `_replace_local` e `_create_local`, ou só a nota na docstring.
- **O `NaN` que o pandas entrega como nulo.** `pa.Table.from_pandas`, o caminho que a documentação
  dá ao `DataFrame`, transforma o `NaN` de uma coluna `float64` em nulo, e a linha do `Double` na
  tabela de tipos de `docs/index.md` diz que ele entra como chega, com `NaN`; isso vale para o
  Arrow, e pelo pandas o `NaN` vira nulo antes de `cast`, que numa coluna `NOT NULL` o recusa.
  Opção: uma frase na seção do `DataFrame` de `docs/index.md`.
- **A janela entre a conferência da versão fixada e o commit de `publish_delta`.**
  `_check_no_data_change` confere por `version_diff` que nenhuma alteração de dados entrou na tabela
  desde a versão fixada, e `register_files` abre a tabela de novo, na versão atual, logo antes do
  `create_write_transaction` (`publish_partition` abre do mesmo jeito): um commit de dados de outra
  execução na mesma partição entre a conferência e essa abertura, durante o `reconcile` e o `COPY`
  de `export_partition`, passa sem `ExecutionConflict`, e o commit seguinte substitui a partição da
  outra execução sem aviso, quando a docstring de `publish_delta` e a [etapa 6](PLAN-STAGE-6.md)
  prometem `ExecutionConflict`. A sonda da execução reproduz a janela na seção D (`exec-e` parada em
  `export_partition` enquanto `exec-f` publica) e a viu na disputa da seção C sob carga. No delta-rs
  1.6.6, `create_write_transaction(mode="overwrite", partition_filters=...)` sobre um `DeltaTable`
  aberto na versão fixada falha com `CommitFailedError` quando um `overwrite` da mesma partição
  entrou depois dela (`a concurrent transaction deleted data this operation read`) e quando o
  esquema mudou (`Metadata changed since last commit`), e passa com uma gravação em outra partição,
  uma compactação da mesma partição e um commit só de metadados no meio. Opções: `register_files` e
  `publish_partition` abrirem a tabela na versão fixada pela execução, atualizada depois do
  `reconcile`, que commita a mudança de esquema, o que entrega o `ExecutionConflict` prometido pelo
  próprio delta-rs; ou a docstring de `publish_delta` dizer que a conferência não cobre a janela,
  com uma execução por ambiente de cada vez.

## Decisões de API pendentes por etapa
- **O `ruff format` sobre o resto do código.** As assinaturas das funções seguem o padrão de um
  parâmetro por linha desde 2026-10-01, e `uvx ruff format --check` ainda reformataria 74 dos 89
  arquivos Python que o Ruff lê (8.995 linhas trocadas, retiradas e acrescentadas somadas, em
  2026-10-01: 3.777 em `probes/`, 3.618 em `tests/`, 1.521 em `src/` e 79 em `scripts/`): chamadas
  e expressões longas reabertas um argumento por linha com vírgula final, condições longas entre
  parênteses, listas recolhidas numa linha, aspas simples trocadas por duplas e alinhamentos por
  espaços desfeitos. Formatar tudo de uma vez, ou deixar o formatador só para as assinaturas, é
  decisão do usuário; [`CURRENT_STATE.md`](CURRENT_STATE.md) registra o estado.
- **O Ruff no grupo `dev` e na esteira.** O usuário recusou os dois em 2026-09-21, e a configuração
  entrou em `pyproject.toml` em 2026-10-01 para rodar por `uvx ruff`; se `ruff check` e
  `ruff format --check` entram em `tests.yml`, com o `ruff` fixado no grupo `dev`, e se os comandos
  entram em `README.md`, é pergunta para o usuário.

Cada arquivo de etapa fecha com a seção "Decisões pendentes"; a lista abaixo as reúne, e uma decisão
tomada sai daqui e do arquivo da etapa no mesmo commit. A [etapa 9](PLAN-STAGE-9.md) espera a
escolha da issue #85 para o `compact` das colunas `Double` sem mínimo e máximo, o item acima; as
outras etapas não têm decisão pendente, e os demais itens que esperam o usuário estão na lista
acima.
