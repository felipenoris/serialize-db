# Questões em aberto

Este documento registra o que ainda não tem resposta: as pendências do projeto, as decisões que
esperam o usuário e as perguntas que uma leitura ou um teste vai fechar. Um item sai daqui quando a
resposta entra no documento que a guarda, e a saída nomeia esse documento. O plano está em
[`PLAN.md`](PLAN.md), o estado da implementação em [`CURRENT_STATE.md`](CURRENT_STATE.md) e o que já
foi medido em [`POC.md`](POC.md).

- **O que `svv_table_info` responde depois do `USE`.** Antes do `USE` ela enxerga só o banco local,
  como `has_schema_privilege` e `information_schema.columns`, que a suíte leu depois do `USE` em
  2026-09-21, seis vezes a primeira e cinco a segunda: `false` e vazio para o esquema do datashare,
  com o `CREATE TABLE` passando nele ([`POC.md`](POC.md), [`redshift.md`](redshift.md)). A execução do
  probe de 2026-09-21 não leu `RS-8` porque `RS-19` reprovou pelo critério errado
  (`current_database()` continuou `dev` depois do `USE`, que vale mesmo assim; o critério passou a
  ser a resolução de um nome em duas partes), e a execução seguinte do probe no ambiente alvo o lê.
  A biblioteca não lê a visão, e etapa alguma depende da leitura: a [etapa 0](PLAN-STAGE-0.md)
  fechou sem ela.
- **Versões não correntes.** O bucket é versionado e o papel não lê o ciclo de vida: cada exclusão
  (o `vacuum`, a limpeza da suíte S3) deixa uma versão não corrente invisível à listagem. `BK-14`
  conta o acumulado, e a regra `NoncurrentVersionExpiration` sob a raiz, junto com
  `AbortIncompleteMultipartUpload`, é pergunta para quem administra o bucket.
- **Credenciais de uma hora.** Nenhuma execução mais longa que uma emissão rodou ainda; a
  [etapa 3](PLAN-STAGE-3.md) resolve `storage_options` a cada chamada e não põe credencial nele
  (decisão do usuário de 2026-09-22), e a primeira execução longa no espaço confirma que o delta-rs
  renova pela cadeia padrão o `DeltaTable` que a execução segura. A credencial do Redshift
  tem o mesmo teto (`GetCredentials`, 3600 segundos), e o serverless encerra a sessão ociosa há
  3.600 s e a transação inativa há 21.600 s ([`redshift.md`](redshift.md)): o que acontece com uma
  conexão aberta quando a senha expira, e se ela cai no meio de um `COPY`, ainda não foi medido; o
  motor da [etapa 5](PLAN-STAGE-5.md) reconecta uma vez por comando e perde só a tabela temporária
  que o pipeline tenha criado na sessão. As credenciais que o `COPY`
  e o `UNLOAD` levam no texto do comando expiram com as do espaço, e `RS-18` imprime quando; um
  `COPY` mais longo que isso também não foi medido.
- **Os relatórios dos probes de 2026-09-21.** O usuário os guardou em `secrets/probes-aws-bn/`, fora
  do git; [`POC.md`](POC.md) os interpreta, e `plan/readings/` não os tem. Copiá-los para
  `plan/readings/`, como os de 2026-09-20, é decisão do usuário: eles trazem os mesmos
  identificadores (conta, papel, usuário do banco) que os relatórios já versionados.
- **Quanto o teste de alcance poupa no ambiente alvo.** O IAM (`iam.amazonaws.com`) e o KMS não têm
  endpoint VPC lá, e `simulate_principal_policy` esperou 10 s e `describe_key` 80 s por nada em
  2026-09-21. As duas passaram a `short_config(2, 5, 1)` atrás de um teste TCP de 2 s num endereço
  (`probelib.endpoint_reachable`), que no macOS no mesmo dia baixou de 10,0 s para 2,0 s a espera por
  um endereço sem rota ([`POC.md`](POC.md)). A próxima execução dos probes no alvo diz o que sobra;
  a permissão sobre a raiz fica provada pela primeira escrita.
- **A memória da partição de `cad_lancamentos`.** Cerca de 700 MB de Parquet e 35 milhões de
  linhas por partição; a primeira carga real mede o `write_deltalake` de um leitor e o `COPY ...
  RETURN_STATS` mais `register_files` antes de fixar o padrão ([etapa 7](PLAN-STAGE-7.md)); a
  migração adiantada (`scripts/migrate_parquet_to_delta.py`, logo depois da etapa 1) é essa carga. `export_mode="rewrite"` e `"register"` medem os dois caminhos em cada motor e na carga inicial
  (etapas [4](PLAN-STAGE-4.md), [5](PLAN-STAGE-5.md) e [7](PLAN-STAGE-7.md)). O relatório da
  migração com essa medição é o gatilho de revisão de [`PLAN.md`](PLAN.md): ele decide o padrão da
  flag e se o outro modo sai, em cada motor e na carga inicial.
- **O `threads` do DuckDB na leitura do S3.** O DuckDB lê arquivos remotos com E/S síncrona, uma
  requisição HTTP por thread, e a documentação recomenda `threads` de 2 a 5 vezes os núcleos para
  essa leitura ([`duckdb.md`](duckdb.md)); o padrão é um por núcleo, 2 no ambiente alvo, e a sessão a
  mais de cada tabela de `run.ingest` só acrescenta a thread que a chama. A primeira execução no
  ambiente alvo mede a ingestão por `delta_scan` do S3 com o padrão e com `threads` acima dos
  núcleos, e a medição decide o padrão de `DuckDBConfig.threads` para uma raiz no S3
  ([etapa 4](PLAN-STAGE-4.md)). A mesma execução mede a ingestão de várias tabelas em sessões a
  mais: em disco local, num macOS de 11 núcleos, quatro tabelas de 8.000.000 de linhas entraram em
  1,629 s contra 3,498 s em série com `threads = 2`, e parte do ganho veio das threads que chamam
  cada sessão, que o ambiente alvo, com 2 vCPUs, não tem de sobra (2026-09-23,
  [`POC.md`](POC.md)).
- **Duas transações simultâneas no esquema do datashare.** A publicação da
  [etapa 8](PLAN-STAGE-8.md) grava a linha de controle em `serialize_db_publications`, a única
  tabela que dois ambientes escrevem, e usa uma staging de nome fixo por ambiente e tabela. A
  documentação prevê que o segundo `DELETE` espere o primeiro terminar e que, sob isolamento de
  snapshot, linhas distintas confirmem as duas transações ([`redshift.md`](redshift.md)); o banco do
  datashare informa isolamento `UNKNOWN`, e o `LOCK` não está na lista de comandos da escrita por
  datashare. `tests/proof_of_concept/test_redshift_transactions.py` (`-m redshift`) mede os cenários
  e espera uma execução no ambiente alvo; o resultado decide se a transação da etapa 8 fica como
  está ou ganha o `LOCK`, a nova tentativa, o `UPDATE` condicionado à versão lida ou a staging por
  execução.
- **O `Double` não finito nas estatísticas do Delta**, a
  [issue #59](https://github.com/felipenoris/serialize-db/issues/59). O `cast` aceita `NaN` e
  infinito numa coluna `Double`, e a biblioteca grava sem mínimo e máximo, no rodapé Parquet e no
  log Delta, as colunas `Double` com valor não finito em cada partição, pela contagem da auditoria
  (decisões do usuário de 2026-09-23, [etapa 3](PLAN-STAGE-3.md)). Continuam abertos:
  - O rodapé que o `UNLOAD` do Redshift grava num grupo de linhas com `NaN`, que só o ambiente alvo
    mede. O Redshift aceita `NaN` em `DOUBLE PRECISION`, e um rodapé com o máximo sem o `NaN`, como
    o do pyarrow, faz o leitor Parquet do DuckDB perder a linha mesmo com o log sem estatística
    ([duckdb/duckdb#25521](https://github.com/duckdb/duckdb/issues/25521), leituras de 2026-09-23,
    [`POC.md`](POC.md)). `test_redshift.py::test_unload_footer_statistics_with_nan` lê o rodapé
    com o `NaN` no início, no meio e no fim do grupo de linhas e com os infinitos, e o que o
    `read_parquet` do DuckDB devolve para `valor > 3` sobre cada arquivo.
  - As tabelas que a migração adiantada gravou no ambiente alvo, com o mínimo e o máximo do
    `Double` registrados. O relatório da versão que rodou lá somava cada coluna `Double` por `CAST`
    para `DECIMAL(38, 6)`, que falha com `NaN` e infinito, e só `ContractError` era tratado: uma
    execução completa sem erro indica tabelas sem valor não finito. O script já segue a regra. Os relatórios da execução, ainda não
    disponíveis, dizem quais tabelas rodaram; uma tabela fora deles pede a contagem de `isnan` e
    `isinf`.
  - Se a contagem de não finitos da auditoria reprova.
- **O `pytest` sem variável grava na pasta temporária do pytest.** A premissa de
  [`PLAN.md`](PLAN.md) diz que `pytest` sem variável não grava arquivo algum, e o cabeçalho de
  `tests/conftest.py` diz que fora das raízes informadas a sessão grava só `.pytest_cache/`; mas
  `tests/test_source_db_projetado.py` grava a base fictícia inteira em `tmp_path_factory`, e
  `tests/test_probes.py` grava relatórios em `tmp_path`, sem variável (revisão de 2026-09-22). A
  decisão é do usuário: admitir a pasta temporária do pytest na premissa e no cabeçalho, ou marcar
  esses testes `local`, e a esteira do GitHub deixa de rodá-los.
- **As correções das suítes S3 e Redshift que esperam o ambiente alvo.** A revisão de 2026-09-22
  achou asserções que não reprovam e leituras que se perdem, em suítes que só rodam no bucket e no
  Redshift e que por isso não mudaram sem uma execução lá:
  `test_redshift.py::outcome` pega toda exceção, e a asserção de `test_copy_varchar_overflow` passa
  com um `TypeError` do próprio teste (pegar `redshift_connector.Error`); o `UNLOAD` recusado de
  `test_unload_partition_by_and_register`, que o comentário chama de regressão, é pulado com
  `share_database` em vez de reprovar; `test_s3.py::test_boto3_list_copy_delete` compara a
  listagem do `list_objects_v2` com `storage.data_files()`, que sai do mesmo paginador (comparar
  com `DeltaTable(uri).file_uris()`), `test_data_file_encryption` aceita qualquer criptografia, e
  `test_boto3_credential_source` chama o STS antes de registrar a origem das credenciais, que se
  perde quando o STS não responde. A revisão do código de 2026-09-23 mudou essas suítes só na forma
  e em extrações mecânicas, e achou mais correções que mudam o comportamento na falha:
  `test_redshift.py` trata o manifesto ausente de três modos (asserção, reserva e nenhuma
  conferência), `on_own_connection` usa `count_from` como chave de modo, a fixture
  `duckdb_connection` só recebe `AWS_REGION` pela ordem dos argumentos do teste, e a limpeza do
  Redshift não registra nada quando dá certo; em `test_redshift_transactions.py`, a participante A
  que falha antes do `COMMIT` não desfaz a transação antes de `finish`, e B pode esperar os 120 s
  presa nos bloqueios de A, a chave `statement_timeout` do relatório é regravada por cada
  participante, um `SELECT pg_backend_pid()` que falha faz `.result[0][0]` levantar `TypeError`, A
  não é fechada quando B não conecta, e o erro sai cru do driver onde `test_redshift.py` usa
  `describe`; em `test_s3.py`, a docstring ainda diz que a suíte é pulada sem internet. A próxima
  execução das duas suítes no ambiente alvo vem com essas correções.
- **As relações de `rel_contas_hierarquias` em que a conta é pai de si mesma.** Na base fictícia
  (`tests/source_db_projetado.py`), as 32 primeiras das 93 relações da hierarquia 1 têm `id_parent`
  igual a `id_child`, efeito de montar os pais pelas mesmas 32 primeiras contas dos filhos; as
  leituras das duas bases reais contaram linhas e tipos, não essa relação. Pergunta ao usuário: a
  base real tem essas linhas? Sem elas, a base fictícia troca os pais, e os testes que leem a
  hierarquia conferem os valores novos.
- **O texto da auditoria no Redshift.** O texto de `serialize_db.audit.audit_sql(..., "redshift")`
  nunca rodou no Redshift: a contagem por `count(CASE WHEN ... THEN 1 END)`, que a documentação do
  `COUNT` sustenta, o `to_char(x, 'YYYY-MM-DD')`, o `is_valid_json`, o `octet_length`, o operador
  POSIX `~` da regra da partição, e o `is_finite` como `x NOT IN ('NaN'::float8, 'Infinity'::float8,
  '-Infinity'::float8)`, que supõe o `NaN` igual a si mesmo, como no PostgreSQL
  ([etapa 4](PLAN-STAGE-4.md)). A [etapa 5](PLAN-STAGE-5.md) roda esse texto, e
  `test_redshift.py::test_audit_sql_under_search_path_and_nan_comparison` o confere antes dela, pelo
  caminho do motor: o `ddl` da etapa 1 e o `audit_sql` citam as tabelas sem esquema, e o caso roda
  numa conexão com `SET search_path` no esquema do datashare depois do `USE`, que também nunca
  rodou lá. Ele lê a comparação do `NaN` que separa o PostgreSQL do IEEE (`'NaN'::float8 =
  'NaN'::float8`, o `is_finite` do `NaN`, dos infinitos, de um número e do nulo, e o `CAST` do `NaN`
  para `NUMERIC(38, 6)`), uma tabela com uma linha de cada defeito, um `NaN` e um infinito, com o
  esperado de cada contador ao lado, cada medida da verificação de linhas isolada (uma medida
  recusada derruba a consulta inteira, e a coluna JSON é `SUPER` no DDL do Redshift, que o
  `is_valid_json` talvez recuse [uncertain]), e os textos do modelo cliente sobre as tabelas vazias.
  Passou pelo emulador local em 2026-09-23 ([`POC.md`](POC.md)).
- **As leituras da etapa 5 na próxima execução da suíte Redshift.** As decisões do usuário de
  2026-09-23 ([etapa 5](PLAN-STAGE-5.md)) supõem comportamentos que ninguém executou no ambiente
  alvo. Os casos estão em `tests/proof_of_concept/test_redshift.py` e passaram, em 2026-09-23, por
  um emulador local com o DuckDB no lugar do Redshift, que confere só o código dos testes
  ([`POC.md`](POC.md)):
  - `test_unload_to_a_hive_prefix_and_register`: o `UNLOAD` sem `PARTITION BY` para um prefixo com
    `=`, `<uri>/<coluna>=<valor>/<execution_id>_<uuid>/`, as colunas do `schema.elements` do
    manifesto dele, o registro dos arquivos, a releitura pelo delta-rs e pelo `delta_scan`, e o
    segundo `UNLOAD` no mesmo destino e num `uuid` novo;
  - `test_stream_by_unload_with_literal_values`: a aspa, a contrabarra, o `%`, o `LIKE`, uma data,
    um número, um `IN` de lista, um timestamp e um `Float`, cada caso pelos parâmetros do driver,
    pelo texto com os literais direto no cursor e pelo mesmo texto dentro do `UNLOAD`; a
    contrabarra, que o dialeto dobra, é a leitura que decide o caminho;
  - `test_unload_limit_empty_result_temp_table_and_super`: a mensagem que recusa o `LIMIT` externo,
    o que o `UNLOAD` grava para um resultado vazio, de que depende o esquema do lote vazio, a tabela
    temporária da sessão lida pelo `UNLOAD` e a coluna `SUPER` no Parquet;
  - `test_row_description_oids_and_type_modifier`: o `row_desc` de um `select` com uma coluna de
    cada tipo do contrato e `SUPER`, e de outro com `count(*)`, `sum` e `avg` de `DECIMAL(18, 2)`,
    `sum` de `DOUBLE PRECISION`, um `DECIMAL(38, 6)` e os literais de texto e de número, que fecha
    a tabela de OIDs de `schema_from_row_description`;
  - `test_small_load_copy_cost`: o melhor de três de um `load` de 10 linhas pelo `COPY` e pelo
    `INSERT` de várias linhas, como leitura: é o que traria de volta o `INSERT` multilinha.

## Decisões de API pendentes por etapa

Cada arquivo de etapa fecha com a seção "Decisões pendentes"; a lista abaixo as reúne, e uma decisão
tomada sai daqui e do arquivo da etapa no mesmo commit.

- [Etapa 1](PLAN-STAGE-1.md): o timestamp com fuso numa coluna `DateTime` sem fuso, e o inverso,
  que o `cast` aceita em silêncio: o instante UTC vira hora local, e a hora local vira UTC (leitura
  de 2026-09-22, [`POC.md`](POC.md)). Proposto: recusar os dois com `ContractError`, porque a
  conversão muda o valor que o cliente vê e nenhuma coluna do modelo cliente tem fuso.
- [Etapa 7](PLAN-STAGE-7.md): a `sort_key` na consulta da carga; o padrão de `export_mode` na carga;
  antes da migração adiantada, o `COPY ... TO 's3://...' (RETURN_STATS)` do DuckDB no ambiente alvo
  (ou gravar em disco e subir pelo `boto3`) e a medição da partição de `cad_lancamentos`.
- [Etapa 8](PLAN-STAGE-8.md): a `distkey` de cada tabela publicada, decidida pela leitura de
  `svv_table_info` depois da primeira publicação (a distribuição é `AUTO` desde a decisão do
  usuário de 2026-09-21); a staging da publicação no datashare ou temporária; `FILLRECORD` em
  todo `COPY` da biblioteca (proposto: um manifesto pode listar arquivos anteriores e posteriores a
  uma coluna nova) ou a lista de colunas, os dois lidos em 2026-09-21; o teto de 65.535 bytes do
  campo JSON no Redshift conferido pela auditoria (proposto), ou um caminho por
  `COPY ... FORMAT JSON 'auto'` ou `INSERT ... JSON_PARSE` para os documentos maiores, os dois lidos;
  a largura de `VARCHAR(n)` da tabela publicada quando `String(n)` cresce no modelo: o diff do Delta
  não a vê, porque o Arrow não tem `n`, só o `<tabela>.redshift.sql` versionado a mostra, e
  `reconcile_published` precisaria de `ALTER TABLE ... ALTER COLUMN ... TYPE VARCHAR(n)`, que o
  Redshift aceita fora de transação e sem descer abaixo do maior valor existente
  ([`redshift.md`](redshift.md)).
- [Etapa 9](PLAN-STAGE-9.md): o nome do runbook; a marca de arquivamento no controle; a retenção do
  `vacuum` mensal.
