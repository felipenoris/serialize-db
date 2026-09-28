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
  15:14; e 7.619 versões, 233.165.927 bytes, com 7.030 marcadores em 2026-09-27 às 15:59,
  [`POC.md`](POC.md)), e a regra
  `NoncurrentVersionExpiration` sob a raiz, junto com `AbortIncompleteMultipartUpload`, é pergunta
  para quem administra o bucket. Sem ela, o `vacuum` da retenção de 400 dias não libera espaço;
  `docs/index.md`, seção "Retenção dos arquivos removidos", traz a regra de exemplo e como mudar a
  retenção.
- **Credenciais de uma hora.** `probes/credentials.py` leu no alvo, em 2026-09-25, em 2026-09-26 e
  em 2026-09-27 ([`POC.md`](POC.md)), o delta-rs, o `S3FileSystem` e o `boto3` renovando a
  credencial do contêiner, que troca de chave a cada cerca de 30 minutos, e a conexão Redshift
  aberta seguindo depois da expiração da senha de `GetCredentials` (3.600 s). O `delta_scan` do
  DuckDB, que falhou uma vez em 2026-09-25 com a chave vencida do secret `credential_chain`, leu em
  todas as rodadas de 2026-09-26 e de 2026-09-27 pelo secret que leva a chave da credencial do
  `boto3` e que o motor recria na entrada de cada sessão quando ela troca (decisão do usuário de
  2026-09-25, [etapa 3](PLAN-STAGE-3.md), [etapa 4](PLAN-STAGE-4.md)). Seguem sem medida um comando
  do DuckDB mais longo que os 15 minutos que a chave tem pela frente, no mínimo, na entrada da
  sessão (o botocore a renova entre 15 e 10 minutos antes da expiração), o `COPY` mais longo que a
  credencial que ele leva, a queda de uma conexão Redshift no meio de um `COPY` e a sessão ociosa e
  a transação inativa do serverless, encerradas depois de 3.600 s e 21.600 s
  ([`redshift.md`](redshift.md)). A cláusula do `COPY` e do `UNLOAD` é montada a cada comando, no
  motor da [etapa 5](PLAN-STAGE-5.md) e, desde a decisão do usuário de 2026-09-26, na publicação da
  [etapa 8](PLAN-STAGE-8.md), que passou assim no alvo em 2026-09-27, e leva uma chave com cerca de
  29 minutos ou mais pela frente; o motor reconecta uma vez por comando e perde só a tabela
  temporária que o pipeline tenha criado na sessão.
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
  23:25 ([`POC.md`](POC.md)). O `compact` rodou só sobre a partição 2026-03-31 de `cad_lancamentos`,
  que tem um arquivo só e não commita, e em 2026-09-25, em 2026-09-26 e em 2026-09-27 saiu com a
  recusa prevista, porque `SUITE.md` o roda depois de um snapshot na versão atual: a compactação de
  uma partição de vários arquivos e a memória dela (o item acima) esperam uma partição com mais de
  um arquivo, que a carga não grava, e um `compact` antes do snapshot; a continuação de uma cópia
  interrompida do `archive` só o substituto exercitou.

- **O acesso de leitura no ambiente alvo.** A [etapa 10](PLAN-STAGE-10.md) rodou no alvo nas
  baterias de 2026-09-25, de 2026-09-26 e de 2026-09-27 ([`POC.md`](POC.md)): o leitor Delta abriu
  as 12 views da raiz carregada em 0,645 s, em 0,582 s e em 0,571 s; as suítes passaram a publicação
  por canal e por snapshot, com a volta a um snapshot anterior, e a comparação dos dois leitores,
  com o `stream` do leitor Redshift pelo `UNLOAD`; e em 2026-09-26 a base inteira foi publicada por
  `--channel default`, `cad_lancamentos` em 295,1 s com o pico do processo em 266 MB, e de novo em
  2026-09-27, em 328,5 s com 270 MB. Esperam: a volta a um snapshot anterior ao publicado sobre a
  base, com o tempo e o pico de RSS por tabela, que pede um commit depois do snapshot, fora do fluxo
  de `SUITE.md`, cujo passo 6 leu nas duas baterias que cada versão já estava publicada; e o
  `UNLOAD` de um cliente com usuário só de leitura para um bucket próprio, com o caminho de
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
trabalho paralelo, e acharam o que segue, reproduzido sem mudar `src/`; as rodadas delas no
ambiente alvo, em 2026-09-26 e em 2026-09-27, repetiram os achados sem reprovar checagem
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

## A API de escrita no sandbox

- **Dois escritores na mesma tabela do Redshift.** `probes/consistencia/probe_append_test.py`
  com `-m redshift` no ambiente alvo lê o que dois `COPY` na mesma tabela ao mesmo tempo, um
  `COPY` ao lado de um `UPDATE` e o `CREATE TABLE` durante um `UNLOAD` fazem sob o isolamento
  serializável (o `1023` lido em 2026-09-24); no substituto a sonda passou em 2026-09-28, e a
  leitura vale só para o código dela. Desde a implementação de 2026-09-28 a sonda chama
  `create_table` e `append` dos motores, e a docstring do `appender` nada diz sobre dois escritores
  na mesma tabela até a rodada.

## Achados da revisão do repositório

A revisão de 2026-09-28 ([`POC.md`](POC.md), seção "O que a revisão do repositório de 2026-09-28
reproduziu") corrigiu o que não dependia de decisão; cada item abaixo espera o usuário.

- **O `COPY` posicional do Redshift depois de uma coluna nova no meio do modelo.** O `COPY ...
  FORMAT AS PARQUET` liga as colunas do arquivo às da tabela pela posição, e as stagings do `ingest`
  e do `pinned_delta` do motor Redshift e da publicação nascem na ordem do modelo (`staging_ddl`,
  chamado por `_load_from_delta` e por `publication_statements`). O `reconcile` põe a coluna nova no
  fim do esquema Delta, os arquivos gravados antes dela não a têm, e o `FILLRECORD` completa com
  nulo a última coluna da staging: com a coluna nova no meio do modelo, ou com colunas reordenadas,
  cada valor dos arquivos antigos cai na coluna vizinha, sem erro. No substituto, a coluna `novo`
  entre `id` e `a` recebeu os valores de `a`, `a` os de `b`, e `b` ficou nula, no `ingest` e na
  publicação. A tabela "Regras do COPY para Parquet" de [`redshift.md`](redshift.md) supõe a ordem
  do arquivo igual à do modelo. Espera o usuário: o `COPY` com a lista de colunas lida do rodapé de
  cada grupo de arquivos, que o alvo aceitou em 2026-09-21 e que pede um `COPY` por lista distinta;
  ou a recusa, em `schema_diff` e em `check_models`, de uma coluna nova fora do fim do modelo e de
  uma reordenação.
- **As conversões da carga inicial que o `cast` recusa.** A consulta de cada partição de
  `initial_load` converte cada coluna por `CAST` do DuckDB para o tipo do modelo
  (`partition_query`), e a conferência (`_check_partition`) conta só nulos, textos longos e a coluna
  de origem da partição: um `double` com mais casas que a escala de um `Numeric` entra arredondado,
  um `timestamp` com hora numa coluna `Date` perde a hora, e um `timestamptz` numa `DateTime` sem
  fuso entra na hora do `TimeZone` da máquina. O `cast` recusa os três com `ContractError` ([etapa
  1](PLAN-STAGE-1.md)). A base de produção carregou sem diferença de contagem e soma em 2026-09-24,
  e a próxima carga no alvo passa pelas mesmas conversões. Espera o usuário: as conferências
  equivalentes às do `cast` na carga, com o `TimeZone` da conexão em UTC, que recusam a partição; ou
  a carga como está, com as três conversões escritas em `docs/index.md`.
- **A auditoria de `serialize-db audit` com o contrato no `ingest`.** Desde 2026-09-28,
  `ingest(materialize=True)` do motor DuckDB cria a tabela pelo DDL do modelo e a enche por `INSERT
  ... BY NAME`, e `serialize-db audit` ingere assim: um JSON malformado ou um nulo numa coluna `NOT
  NULL`, os defeitos que as verificações `json_` e `nulo_` contam, derrubam o `ingest` com
  `ConversionException` ou `ConstraintException` e saem com 1 e o traceback, sem relatório. O teste
  da linha de comando só cobre a auditoria aprovada. Espera o usuário: a auditoria da linha de
  comando sobre a view (`materialize=False`), que relê o Delta em cada verificação; uma
  materialização sem o contrato só para ela; ou a recusa do `ingest` impressa como reprovação.
- **O nome de snapshot repetido em `serialize-db run`.** `run.snapshot(nome)` só confere a regra do
  nome, e o nome já presente no arquivo de controle é `ValueError` na saída do `with`, depois dos
  commits; `serialize-db run` captura só `ContractError`, `AuditFailed`, `ExecutionConflict` e
  `ConflictError`, e sai com 1 e o traceback, enquanto `docs/operacao.md` e a docstring de
  `serialize_db.cli` prometem 2 no nome repetido. Espera o usuário: `delta.snapshot` levantar
  `ContractError`, que deriva de `ValueError`, no nome repetido; conferir o nome já em
  `run.snapshot`, antes dos commits; ou a documentação dizer 1.
- **As chaves `serialize_db_version` e `serialize_db_execution_id` do rodapé Parquet.** A convenção
  de nomes do `CLAUDE.md` e [`serialize-db.md`](serialize-db.md) as descrevem como gravadas pela
  biblioteca no rodapé de cada arquivo, e nenhum escritor do pacote as grava: o `COPY` do DuckDB, o
  `UNLOAD` do Redshift e o `write_deltalake` gravam os arquivos sem metadados da biblioteca. Espera
  o usuário: gravá-las, onde o escritor aceita metadados de rodapé; ou tirá-las dos dois documentos.
- **O DuckDB de `probes/diagnose_aws.py`.** A checagem do DuckDB carrega a extensão `aws` e cria o
  secret com `PROVIDER credential_chain`, que a biblioteca deixou em 2026-09-25 pela chave do
  `boto3`, e sem `URL_STYLE 'path'` nem `USE_SSL false` um endpoint `http`, o do substituto, reprova
  a checagem sem que o acesso da biblioteca falhe. O probe só roda no alvo, e a regra do instrumento
  validado pede o relatório dele antes e depois da mudança. Espera o usuário: a checagem pelo
  `Storage.duckdb_connect` da biblioteca, validada numa rodada no alvo; ou a checagem como está, uma
  leitura do caminho do `credential_chain`.
- **A simulação do `RS-11` em `probes/redshift.py`.** O `RS-11` simula `ListBucket`, `GetObject` e
  `PutObject` contra o bucket e o prefixo numa chamada só de `simulate_principal_policy`, e uma
  política de privilégio mínimo, com `ListBucket` só no bucket e as outras duas só nos objetos, pode
  ler negada (inferido da documentação do IAM, sem rodada); o `BK-8` de `probes/bucket.py` simula
  cada ação contra o recurso dela. Espera o usuário: uma simulação por ação e recurso, validada numa
  rodada no alvo; ou o `RS-11` como está.

## Decisões de API pendentes por etapa

Cada arquivo de etapa fecha com a seção "Decisões pendentes"; a lista abaixo as reúne, e uma decisão
tomada sai daqui e do arquivo da etapa no mesmo commit. Nenhuma etapa tem decisão pendente; os itens
que esperam o usuário estão na lista acima.
