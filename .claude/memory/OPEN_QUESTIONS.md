# Questões em aberto

Leia antes de planejar uma sessão e antes de dar uma pergunta por aberta. Este documento registra
o que ainda não tem resposta: as pendências do projeto, as decisões que esperam o usuário e as
perguntas que uma leitura ou um teste vai fechar. Um item sai daqui quando a resposta entra onde ela
fica (a docstring ou o teste do código, `decisions.md` ou outro arquivo desta pasta), e a saída
nomeia esse lugar. É o único documento do plano que o trabalho atualiza: o plano, os arquivos de
etapa, o estado da implementação e o que foi medido até 2026-10-05 estão na pasta `plan/` da
biblioteca do projeto Claude, fora do repositório, e o `POC.md`, o `estrategia.md` e a etapa N (o
`PLAN-STAGE-N.md`) citados abaixo são os de lá.

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
  20.000 entradas, `POC.md`; e, com a listagem no mesmo limite, ao menos 10.419 versões,
  26.705.558.141 bytes, com 9.580 marcadores em 2026-10-07 às 03:04,
  `.claude/memory/environments.md`), e a regra `NoncurrentVersionExpiration` sob a raiz, junto
  com `AbortIncompleteMultipartUpload`, é pergunta para quem administra o bucket. Sem ela, o
  `vacuum` da retenção de 400 dias não libera espaço; `docs/index.md`, seção "Retenção dos arquivos
  removidos", traz a regra de exemplo e como mudar a retenção. A mesma pergunta vale para a regra
  de ciclo de vida que `docs/operacao.md`, seção "Arquivo", espera na pasta `arquivo/`: a passagem
  dos arquivos dela à classe de armazenamento mais barata. As sondas da operação também apagam a
  pasta delas sob a raiz da suíte no fim de cada rodada: 19.293 MB em 2026-10-05 e 19.284 MB na
  segunda rodada de 2026-10-07, em MB de 2^20 bytes, pelas linhas `raiz apagada` dos relatórios.
  Cada rodada deixa ao menos isso em versões não correntes, cerca de três quartos do salto de
  `BK-14` de 249.621.376 bytes em 2026-10-05 para 26.705.708.023 em 2026-10-06 [inferido].
- **Credenciais de uma hora.** `probes/credentials.py` leu no alvo, em 2026-09-25, em 2026-09-26, em
  2026-09-27 e em 2026-09-29 (`POC.md`), o delta-rs, o `S3FileSystem` e o `boto3`
  renovando a credencial do contêiner, que troca de chave a cada cerca de 30 minutos, e a conexão
  Redshift aberta seguindo depois da expiração da senha de `GetCredentials` (3.600 s). O
  `delta_scan` do DuckDB, que falhou uma vez em 2026-09-25 com a chave vencida do secret
  `credential_chain`, leu em todas as rodadas de 2026-09-26, de 2026-09-27 e de 2026-09-29 pelo
  secret que leva a chave da credencial do `boto3` e que o motor recria na entrada de cada sessão
  quando ela troca (decisão do usuário de 2026-09-25, etapa 3,
  etapa 4). Seguem sem medida um comando do DuckDB mais longo que os 15 minutos
  que a chave tem pela frente, no mínimo, na entrada da sessão (o botocore a renova entre 15 e 10
  minutos antes da expiração) ou na abertura das conexões de `rewrite`, `read_back`,
  `export_parquet` e da troca do motor Redshift, que duram uma tabela ou uma partição e ficam com a
  chave da abertura; o `COPY` mais longo que a credencial que ele leva; a queda de uma conexão
  Redshift no meio de um `COPY`; e a sessão ociosa e a transação inativa do serverless, encerradas
  depois de 3.600 s e 21.600 s
  ([`docs/tecnologias.md`, Redshift](../../docs/tecnologias.md#redshift)).
  A cláusula do `COPY` e do `UNLOAD` é montada a cada comando, no motor da etapa 5 e, desde a
  decisão do usuário de 2026-09-26, na publicação da etapa 8, que passou assim no alvo em
  2026-09-27, e leva uma chave com cerca de 29 minutos ou mais pela frente; o motor reconecta uma
  vez por comando fora de transação e perde só a tabela temporária que o pipeline tenha criado na
  sessão, e a carga de cada partição de `ingest` e de `pinned_delta` roda numa transação desde
  2026-10-04, para a queda no meio do `COPY` subir sem repetição (`POC.md`).
- **A reconexão do motor Redshift.** As suítes do motor e da publicação rodaram no ambiente alvo
  em 2026-09-24, duas vezes cada, e leram o que esperavam (`POC.md`): fica sem medida a reconexão
  depois de uma queda do servidor, que nenhum teste provoca lá (etapa 5).
- **A transação do motor Redshift dentro de um `BEGIN` do cliente.** `transaction()` do motor
  Redshift, que o appender e, desde 2026-10-04, a carga de cada partição de `ingest` e de
  `pinned_delta` usam, não é reentrante: um `append` dentro de um `BEGIN` que o cliente abriu em
  `session()` leva no `COMMIT` dele o trabalho do cliente (leitura do código na revisão de
  2026-10-01; o Redshift não foi lido com um `BEGIN` dentro de outro). A docstring de `session()`
  do motor DuckDB descreve o caso dele, e as de `session()` e `transaction()` do motor Redshift não
  falam do caso. Espera a leitura no alvo de
  `test_engine_redshift.py::test_append_inside_a_client_transaction_is_read`
  (`redshift.engine.append_inside_client_transaction`: o desfecho do `append` e do `ROLLBACK` do
  cliente, os avisos do servidor e os ids que ficam; no substituto local, o `BEGIN` aninhado é
  recusado) e a frase nas docstrings que a revisão de 2026-10-01 propôs.
- **A distribuição das tabelas publicadas.** As tabelas publicadas ficam em `DISTSTYLE AUTO`
  (decisão do usuário de 2026-09-21), e uma chave de distribuição só entra, por
  `ALTER TABLE ... ALTER DISTKEY`, quando o `EXPLAIN` de um join típico entre elas,
  `cad_lancamentos` com `cad_contas` por `id_conta`, mostra `DS_BCAST_INNER` ou `DS_DIST_BOTH`
  (decisão do usuário de 2026-09-23); o papel do projeto não lê `svv_table_info` depois do `USE`
  (42501, probe de 2026-09-23). O `EXPLAIN` só rodou sobre as tabelas pequenas das suítes, com
  `DS_DIST_ALL_NONE` (`test_publication.py::test_published_join_redistribution_is_read`,
  2026-09-24), e espera a leitura sobre a base publicada no alvo, que
  `probes/operacao/probe_published_base.py` faz depois da publicação pelo canal `default`.
- **O filtro do dataset do delta-rs nas colunas sem mínimo e máximo.** O
  `DeltaTable.to_pyarrow_dataset()` do delta-rs, e com ele o `to_pyarrow_table` e o `to_pandas`
  com `filters`, perde as linhas de um filtro sobre uma coluna que o log deixa sem mínimo e máximo:
  o `filestats_to_expression_next` do delta-rs põe `coluna >= null` e `coluna <= null` na garantia
  de cada arquivo, e o PyArrow pula o arquivo. Na sonda de 2026-09-25 (`POC.md`), um
  arquivo registrado pelo motor DuckDB leu 0 linhas em `valor > 1` (`Numeric(18, 2)`),
  `quando > '2025-12-31'` (`DateTime`) e `legado = true` (`Boolean`), contra 2, 2 e 1 pelo
  `delta_scan`. A perda é por arquivo, e o `IS NULL` e o `IS NOT NULL` só erram no arquivo que
  também não traz o `nullCount` da coluna: o primeiro perde os nulos, e o segundo os traz. Ficam
  sem os dois no log os tipos que o registro omite (etapa 3), entre eles as
  colunas `DateTime` e `Boolean` do modelo cliente; o texto dos arquivos do `UNLOAD`; e as
  `Double` com valor não finito da issue #59, em todo caminho de escrita menos o `compact`, que
  grava mínimo, máximo e `nullCount` de toda coluna (docstring de `delta.compact`), e sem o
  `nullCount` na troca do motor Redshift. O `delta_scan` dos
  motores e do leitor Delta e o `COPY` do Redshift leem certo, e o pacote filtra o dataset do
  delta-rs só pela coluna da partição (`read_back`), fora da perda. O texto de um arquivo gravado
  pelo DuckDB, pelo export do motor ou pelo import, também fica sem mínimo e máximo quando o
  prefixo de 256 bytes do mínimo ou do máximo não tem byte ASCII (docstring de
  `delta._stat_converter`), e o filtro do dataset por essa coluna perde o arquivo inteiro (leitura
  de 2026-10-04, `POC.md`). Com
  `delta.dataSkippingStatsColumns` sem as três colunas, a mesma sonda leu 2, 2 e 1, e o `delta_scan`
  seguiu podando pelas estatísticas que o log já guarda; a propriedade faz o `write_deltalake`
  gravar só as estatísticas das colunas dela e o `get_add_actions` esconder as outras. A issue #3032
  do delta-rs, aberta em 2024-11-25 e fechada com o rótulo `mre-needed`, relata o mesmo sintoma num
  filtro fora da partição, sem a causa; nenhuma issue aberta trata do caso, e o defeito segue na
  1.6.6 e no `main`. O `DeltaTable.scan`, o `QueryBuilder` e o `scan_delta` do Polars leem certo, e
  `docs/index.md`, seção "Ler a base com o modelo", lista os leitores (`POC.md`). Espera o
  usuário: pôr a propriedade nas tabelas, com as colunas inteiras e de data, que todo caminho de
  escrita grava com mínimo e máximo; gravar no log mínimo e máximo largos, que o protocolo aceita
  com `tightBounds` falso, nos tipos que o registro omite e nas `Double` da issue #59; ou deixar o
  pacote como está. A issue #85 acompanha o item, com um exemplo autocontido que reproduz a perda.
  O defeito vai ao delta-rs numa issue com o exemplo mínimo, que o usuário abre. O `compact` das
  `Double` sem mínimo e máximo da etapa 9 espera esta escolha (decisão do usuário
  de 2026-09-25): o código da decisão compacta sem a estatística cada coluna `Double` que algum
  arquivo da partição traz sem mínimo e máximo no log, e uma coluna só de nulos num arquivo, que
  também sai do log sem os dois, perde a estatística na partição compactada; o teste
  `tests/test_delta.py::test_compact_keeps_the_columns_without_min_max` entra com esse código, sobre
  uma partição de dois arquivos, um sem mínimo e máximo de `valor`, e outra, compactada na mesma
  chamada, que os mantém.

- **A passagem da produção para o Delta.** A carga e a publicação rodaram no alvo sobre uma cópia da
  base de produção, num sandbox (declaração do usuário de 2026-09-23). Os tipos do modelo cliente
  ficam fechados antes da carga da produção, porque mudá-los depois é reescrever o Delta: `valor`
  segue `Double` (decisão do usuário de 2026-09-20), e a troca por `Numeric(18, 2)`, mais adequada a
  dados contábeis, ficou como melhoria futura, por `rewrite` com o `cast` que recusa o `double` fora
  da escala. Um `Numeric` largo leva um defeito do escritor do delta-rs, que grava no log o mínimo e
  o máximo como número JSON: `123456789012345.21` num `decimal(18, 2)` saiu `123456789012345.2`, e
  `valor = 123456789012345.21` não achou a linha no delta-rs nem no `delta_scan` (2026-09-22,
  `test_deltalake.py::test_written_stats_lose_the_row_on_decimal`); `register_files` deixa o
  `decimal` sem mínimo e máximo, e `publish_partition` grava pelo delta-rs. Depois da carga, os
  leitores passam a abrir o Delta, e as pastas de origem ficam como cópia até a primeira publicação
  no Redshift. Espera o usuário: a troca de `valor`, se vier, antes da carga da produção.
- **O acesso de leitura no ambiente alvo.** A etapa 10 rodou no alvo nas baterias de 2026-09-25, de
  2026-09-26, de 2026-09-27, de 2026-09-28 às 23:09, de 2026-10-05 e de 2026-10-07 (`POC.md`,
  `.claude/memory/environments.md`): o leitor Delta abriu as 12 views da raiz carregada em 0,645 s,
  em 0,582 s, em 0,571 s, em 0,556 s, em 0,607 s e, com 2 vCPUs, em 1,440 s; as suítes passaram a
  publicação por canal e por snapshot, com a volta a um snapshot anterior, e a comparação dos dois
  leitores, com o `stream` do leitor Redshift pelo `UNLOAD`, e na bateria de 2026-09-30 o runbook de
  refazer um snapshot, com a volta pelo canal; e em 2026-09-26 a base inteira foi publicada por
  `--channel default`, `cad_lancamentos` em 295,1 s com o pico do processo em 266 MB, e de novo em
  2026-09-27, em 328,5 s com 270 MB, em 2026-09-29, em 335,2 s com 286 MB, em 2026-10-05, em 324,9 s
  com 285 MB, e em 2026-10-07, com 2 vCPUs, em 300,2 s com 274 MB. Esperam: a volta a um snapshot
  anterior ao publicado sobre a base, com o tempo e o pico de RSS por tabela, que pede um commit
  depois do snapshot e que a seção "Sonda da base publicada" do `suite_alvo.sh` faz com
  `probes/operacao/probe_published_base.py`, refazendo a primeira partição de `cad_lancamentos` (o
  passo 6 da publicação leu em cada bateria que cada versão já estava publicada); e o `UNLOAD` de um
  cliente com usuário só de leitura para um bucket próprio, com o caminho de credencial que serve a
  ele, que precisa de um papel de cliente no alvo.

- **O ganho das APIs com threads numa máquina maior.** `probes/operacao/probe_parallel_gain.py`
  mediu o ganho de cada API com threads sobre a série no ambiente alvo em 2026-10-05, numa máquina
  de 8 vCPUs, com as tabelas no S3 e o Redshift (`docs/index.md`, seção "Multithreading"), e em
  2026-10-07, duas vezes, numa de 2 vCPUs (`.claude/memory/concurrency.md`): no DuckDB os pools
  ganharam a metade do que em 8 vCPUs, e no Redshift o ganho não dependeu da máquina; o ganho com
  mais CPUs segue sem medida e espera a sonda numa máquina maior.

- **A SQLAlchemy 2.1.** A 2.1.0, publicada em 2026-09-24, quebrou o pacote na sessão de testes
  de 2026-09-25, e a 2.1.3, de 2026-10-02, ainda o quebra na de 2026-10-03 (`POC.md`);
  o pino fica em 2.0.54. A 2.1.3 corrigiu os valores de `params()` que saíam `NULL` sob
  `literal_binds`. Seguem: o `construct_params()` de um statement com valores de `params()`,
  compilado com `render_postcompile`, levanta `InvalidRequestError` no motor DuckDB e no cursor do
  motor Redshift; o dialeto deixou de dobrar a contrabarra dos literais, e no substituto o
  `stream` do motor Redshift pelo `UNLOAD` leu 0 linhas onde o `query` leu 1, sem erro; o `Double`
  deixou de derivar de `Numeric`, e o `import_report` para de somar as colunas `Double`; e um
  statement que o próprio cliente passou por `params()` é recusado com `SqlError`. A reflexão do
  duckdb-engine 0.17.0 também falha na 2.1, fora do pacote. O ajuste que passou os testes do
  pacote na 2.1.3 e na 2.0.54 é `construct_expanded_state()` nos dois motores, `sa.Float` ao lado
  de `sa.Numeric` nas somas do `import_report` e `_backslash_escapes` verdadeiro e explícito nos
  dialetos do Redshift; os do DuckDB já o fixam em falso. O usuário decidiu manter a 2.0.54 em
  2026-10-03 (`.claude/memory/decisions.md`); a leitura se repete com uma 2.1.x nova, como a 2.1.4,
  de 2026-10-07, que ainda não passou pela sessão de testes, ou com a troca do dialeto do DuckDB.
- **O dialeto do DuckDB.** O `duckdb-engine` 0.17.0, o compilador do `render` e do motor DuckDB
  fixado em `pyproject.toml`, é de 2025-03-29, sem lançamento desde então, com 55 issues e 43 PRs
  abertos e a correção da reflexão da `pg_collation` parada num PR de 2026-03-28; o
  `duckdb-sqlalchemy` 1.5.5.9, a bifurcação de 2025-12-24 mantida por um autor, com 17 estrelas e
  8.973 downloads no mês contra 1.841.220, compila os mesmos statements byte a byte, passa todos
  os testes do pacote no lugar dele com a SQLAlchemy 2.0.54 e livra a reflexão da `pg_collation`
  na 2.1.0 (`POC.md`); a versão atual do `duckdb-sqlalchemy` é a 1.5.6, de 2026-10-05, sem
  leitura. Espera o usuário: trocar a dependência (a fixação e o comentário em `pyproject.toml`,
  `import duckdb_sqlalchemy` em `serialize_db.sql`, `serialize_db.engine.duckdb` e
  `tests/proof_of_concept/test_sqlalchemy.py`, cuja asserção da chave primária não refletida
  passa a refleti-la, a lista de `probes/space.py`, a prosa que nomeia o dialeto em `README.md`,
  `docs/index.md` e `docs/tecnologias.md` e as docstrings de `tests/test_schema.py` e
  `tests/test_sql.py`) ou manter o `duckdb-engine` enquanto a 2.0.54 o serve.
- **O DuckDB 1.5.6.** Os clientes instalam as extensões do DuckDB pelos wheels
  `duckdb-extension-delta` e `duckdb-extension-httpfs` do PyPI, que exigem o `duckdb` da mesma
  versão e paravam na 1.5.5 em 2026-10-04 (`POC.md`); o pino fica em 1.5.5. A troca
  para a 1.5.6, que passou os testes do pacote em 2026-10-03, espera os wheels da 1.5.6 no PyPI.
- **O pacote no Windows.** A esteira roda os testes do pacote num runner Windows desde 2026-10-01
  (`POC.md`), com a pasta local e o DuckDB em memória, e a suíte inteira, com os testes
  dos probes e as provas de conceito, passou no runner em 2026-10-04; o S3, o Redshift, os próprios
  probes, `prepare_offline.sh` e o projeto cliente não rodaram no Windows. Lá, o `os.environ` passa
  o nome da variável para maiúsculas (o `encodekey` do `os.py` do Python 3.13; no runner, `no_proxy`
  e `NO_PROXY` foram uma variável só), e `_proxy_settings`, de `serialize_db.storage`, lê na
  variável `username`, a do usuário do proxy no espaço SageMaker, o `USERNAME` do login: com
  `HTTP_PROXY` definido, o DuckDB recebe o login como usuário do proxy, com ou sem usuário no
  endereço. Espera o usuário: no Windows, ler o usuário e a senha do proxy só do endereço, ou de
  variáveis `SERIALIZE_DB_`, ou manter a leitura enquanto o Windows é só a máquina de quem
  desenvolve.
- **O caso de estudo do GIL.** `test_gil_reacquisition_waits_the_switch_interval`, em
  `tests/proof_of_concept/test_concurrency.py`, reprovou em sessões locais da suíte inteira em
  2026-09-25, em 2026-10-03 e em 2026-10-07 e passou isolado e nas demais: a asserção pede que os
  200 `os.stat` ao lado do laço Python levem mais que o dobro do tempo que levam com o intervalo de
  troca dez vezes menor, e nas sessões reprovadas levaram 0,011 s contra 0,018 s e 0,005 s contra
  0,006 s (isolado em 2026-10-07, 0,278 s a 0,444 s contra 0,006 s a 0,024 s). No alvo, a sessão
  `-m "not redshift"` do `suite_alvo.sh` roda o caso, e ele passou nas 13 sessões com relatório, de
  2026-09-24 a 2026-10-07, com 0,381 s a 0,893 s ao lado do laço contra 0,012 s a 0,074 s com o
  intervalo menor (`concurrency.gil.os_stat_200`); a de 2026-10-07, com 2 vCPUs, leu 0,381 s
  contra 0,012 s. A esteira não roda `tests/proof_of_concept/`, e o caso só atrapalha a sessão
  local antes do commit. Espera o usuário: tornar a medida robusta ou aceitar a reprovação
  ocasional.
- **A pasta da execução no pacote.** `tests/test_pipeline.py` guarda, em código cliente, a cópia
  da entrega e os resultados de cada execução em `<ambiente>/execucoes/<execution_id>/`
  (`POC.md`), sem API do pacote; `Storage.copy` só copia dentro da raiz do banco, e uma
  entrega em outro bucket fica com o `boto3` do cliente. Espera o usuário: levar a pasta e a cópia
  de fora da raiz ao pacote quando um segundo pipeline repetir o código, ou mantê-las no cliente.
- **As funções do pipeline que diferem entre os motores.** Uma função com nome ou semântica
  diferente no DuckDB e no Redshift ganha uma regra `@compiles` por dialeto, como as da auditoria em
  `serialize_db.audit`; a lista sai do SQL do pipeline mensal, fora deste repositório, e o
  levantamento não foi feito. Espera o código do pipeline.
- **O teste do pipeline no S3 e no Redshift.** `tests/test_pipeline.py` roda o pipeline mensal ponta
  a ponta no motor DuckDB, sobre a base fictícia numa pasta local; a adaptação ao S3 e ao Redshift
  fica com o usuário (mensagem de 2026-09-27).
- **O Delta diante de um catálogo.** O Delta Lake pelo delta-rs é a camada de tabela porque nenhum
  serviço de catálogo está habilitado (premissa do usuário de 2026-09-19): o Iceberg sem catálogo
  fica fora da especificação, que exige a troca atômica do ponteiro no catálogo, e o Redshift lê a
  base só por `COPY ... MANIFEST`, porque um esquema externo exige `CREATE` no banco, negado ao
  projeto (diagnóstico de 2026-09-13). A avaliação de 2026-09-19 fixou o gatilho de reavaliação: com
  o Glue, o S3 Tables ou um catálogo que o Redshift alcance disponível no ambiente alvo, a
  comparação das camadas de tabela do `estrategia.md` é refeita com o Iceberg registrável. A troca
  não reescreve os dados: um Iceberg registra os Parquet do Delta por `add_files`, e o Apache XTable
  converte os metadados sem tocar nos arquivos. O risco a observar no protocolo Delta é o recurso
  `catalogManaged`, que leva o commit para um catálogo. `probes/catalog.py` mede o gatilho, uma
  tabela Iceberg no Glue ou um table bucket no S3 Tables: nas leituras das baterias de 2026-10-05
  a 2026-10-07, a última às 03:04 de 2026-10-07, o Glue seguia com um banco e uma tabela Parquet, e
  o Lake Formation e o S3 Tables não responderam ao papel do projeto
  (`.claude/memory/environments.md`). Espera um catálogo no ambiente alvo.

## Achados das sondas de consistência de leitura e escrita

As sondas de 2026-09-25 (`POC.md`, seção "O que as sondas de consistência de leitura e escrita
mostraram") atravessaram cada fronteira de leitura e escrita com valores de borda e trabalho
paralelo, e acharam o que segue, reproduzido sem mudar `src/`; as rodadas delas no ambiente alvo, em
2026-09-26, em 2026-09-27, em 2026-09-29, em 2026-10-05 e em 2026-10-07, repetiram os achados sem
reprovar checagem (`POC.md`, `.claude/memory/environments.md`). Cada item espera o usuário:
corrigir, ou aceitar como está.

- **O sinal do zero pelo `COPY` do DuckDB.** O escritor Parquet do DuckDB codifica a coluna
  `DOUBLE` por dicionário e trata `-0.0` e `0.0` como o mesmo valor: num grupo de linhas com os
  dois, todos saem com o sinal do primeiro que apareceu, e um grupo de 4 linhas, gravado em `PLAIN`,
  guarda o sinal (`.claude/memory/duckdb.md`). Atinge o `export_partition` do motor DuckDB,
  `import_table`, `rewrite` e `export_parquet(mode="rewrite")`; `publish_partition` e `compact`,
  pelo escritor do delta-rs, guardam o sinal, e o `UNLOAD` do Redshift espera a leitura no alvo de
  `test_engine_redshift.py::test_zero_sign_through_copy_query_and_unload_is_read`
  (`redshift.engine.zero_sign`: o `COPY`, o cursor e o `UNLOAD`, com o que o servidor guarda lido
  no texto do valor e por `atan2`). A diferença aparece em `1 / x`, em `math.copysign` e no texto
  do valor, nunca numa comparação ou numa soma.
  Opções: `DICTIONARY_SIZE_LIMIT 0` no `COPY` (sem dicionário em coluna alguma, arquivo maior), ou
  registrar a perda na linha do `Double` da tabela de tipos de `docs/index.md`.
- **A soma de controle da auditoria acima de 1e32.** `audit` soma cada `Double` e `Numeric` como
  `DECIMAL(38, 6)` (`_totals` de `serialize_db.audit`): um valor finito de magnitude 1e32 ou mais
  falha no `CAST` (`ConversionException`), e uma soma acima disso estoura (`OutOfRangeException`),
  o que derruba `audit` e impede `publish_delta` (`audit=False` dispensa, com aviso). O
  `import_report` soma as mesmas colunas como `DECIMAL(38, 6)` (`_total_measures` de
  `serialize_db.parquet_import`) e tem o mesmo teto. A base de produção fica em 1e18. Opções:
  somar o `Double` como `DOUBLE` (a soma de controle deixa de ser exata, como já é a coluna), ou
  capturar o estouro e registrar a soma como não lida.
- **A escrita condicional do arquivo de controle entre threads.** Na pasta local,
  `Storage.write_text(if_match=...)` confere a impressão digital e faz o `os.replace` fora de um
  lock: oito threads somando 50 cada perderam 293 de 400 atualizações, e de 312 a 335 nas rodadas do
  alvo, de 2026-09-26 a 2026-10-07. A docstring diz que a escrita não é atômica entre processos;
  entre threads do mesmo processo ela também não é, e `snapshot`, `archive_snapshot` e `set_channel`
  chamados em paralelo numa raiz local (duas `Execution` com `snapshot` encerrando ao mesmo tempo,
  por exemplo) podem perder uma entrada. No S3 o `IfMatch` é do servidor. Opções: um
  `threading.Lock` de `Storage` em volta de `_replace_local` e `_create_local`, ou só a nota na
  docstring.
- **O `NaN` que o pandas entrega como nulo.** `pa.Table.from_pandas`, o caminho que a documentação
  dá ao `DataFrame`, transforma o `NaN` de uma coluna `float64` em nulo, e a linha do `Double` na
  tabela de tipos de `docs/index.md` diz que ele entra como chega, com `NaN`; isso vale para o
  Arrow, e pelo pandas o `NaN` vira nulo antes de `cast`, que numa coluna `NOT NULL` o recusa.
  Opção: uma frase em `docs/index.md`, seção "Rodar o pipeline no sandbox DuckDB", no parágrafo
  que dá a conversão do `DataFrame`.
- **A janela entre a conferência da versão fixada e o commit de `publish_delta`.**
  `_check_no_data_change` confere por `version_diff` que nenhuma alteração de dados entrou na tabela
  desde a versão fixada, e `register_files` abre a tabela de novo, na versão atual, logo antes do
  `create_write_transaction` (`publish_partition` abre do mesmo jeito): um commit de dados de outra
  execução na mesma partição entre a conferência e essa abertura, durante o `reconcile` e o `COPY`
  de `export_partition`, passa sem `ExecutionConflict`, e o commit seguinte substitui a partição da
  outra execução sem aviso, quando a docstring de `publish_delta`, `docs/index.md` (seção "Rodar
  uma execução") e a etapa 6 prometem `ExecutionConflict`. A sonda da execução reproduz a janela
  na seção D (`exec-e` parada em `export_partition` enquanto `exec-f` publica) e a viu na disputa
  da seção C sob carga. No delta-rs
  1.6.6, `create_write_transaction(mode="overwrite", partition_filters=...)` sobre um `DeltaTable`
  aberto na versão fixada falha com `CommitFailedError` quando um `overwrite` da mesma partição
  entrou depois dela (`a concurrent transaction deleted data this operation read`) e quando o
  esquema mudou (`Metadata changed since last commit`), e passa com uma gravação em outra partição,
  uma compactação da mesma partição e um commit só de metadados no meio. Opções: `register_files` e
  `publish_partition` abrirem a tabela na versão fixada pela execução, atualizada depois do
  `reconcile`, que commita a mudança de esquema, o que entrega o `ExecutionConflict` prometido pelo
  próprio delta-rs; ou a docstring de `publish_delta` e `docs/index.md` dizerem que a conferência
  não cobre a janela, com uma execução por ambiente de cada vez.

## Achados da revisão de bugs de 2026-10-04

A revisão de `src/serialize_db` na `main` de 2026-10-04, a pedido do usuário, com atenção à
consistência das leituras, das escritas e da publicação, corrigiu no mesmo PR o `COPY` repetido
pela reconexão do motor Redshift (`POC.md`, etapa 5) e deixa dois
itens que pedem decisão:

- **O `Double` não finito nas constantes do `render`.** `render` escreve `nan`, `inf` e `-inf`
  para um `Double` não finito embutido como constante, e o DuckDB recusa o texto com
  `BinderException` (`Referenced column "nan" was not found`), e o `literal_text` do `stream` do
  Redshift compila os parâmetros do cliente com o mesmo `literal_binds`. O Redshift espera a
  leitura no alvo de `test_engine_redshift.py::test_nonfinite_double_constant_is_read`
  (`redshift.engine.nonfinite_double_constant`: a constante do `render` pelo `query`, o valor do
  cliente pelo `stream` e pelo parâmetro do driver). Opções: um `literal_processor` do `Double`
  nos dois dialetos, que escreva `CAST('NaN' AS DOUBLE)` e `CAST('Infinity' AS DOUBLE)`; ou um
  `SqlError` na constante não finita, antes de o motor recusar o texto.
- **A ordem do `Execution.__exit__`.** O `__exit__` roda `sandbox.cleanup()` antes de
  `_write_snapshot()`: um descarte que falha, por uma conexão derrubada ou um `DROP` recusado,
  sobe ao cliente e deixa a execução sem a entrada do snapshot que `run.snapshot(...)` pediu,
  com as partições já publicadas no Delta e as versões no log da execução. Opções: gravar
  a entrada antes do descarte, com o descarte em `finally`, para a falha do sandbox não apagar o
  registro do que já está publicado; ou a docstring de `Execution` dizer que o snapshot só entra
  com o descarte concluído, e o operador o cria por `serialize-db snapshot`.
