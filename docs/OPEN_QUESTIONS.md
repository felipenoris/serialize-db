# Questões em aberto

Este documento registra o que ainda não tem resposta: as pendências do projeto, as decisões que
esperam o usuário e as perguntas que uma leitura ou um teste vai fechar. Um item sai daqui quando a
resposta entra no documento que a guarda, e a saída nomeia esse documento. O plano está em
[`PLAN.md`](PLAN.md), o estado da implementação em [`CURRENT_STATE.md`](CURRENT_STATE.md) e o que já
foi medido em [`POC.md`](POC.md).

- **Onde ficam as tabelas de execução.** O sandbox `exec_<id>_*` da [etapa 4](PLAN-STAGE-4.md) e as
  stagings do `COPY` nascem no banco do datashare, onde o `CREATE TABLE` passou, e herdam as
  restrições dele: escrita num banco por transação, sem `VIEW`. A alternativa da tabela temporária
  (`TEMP` é verdadeiro no banco da conexão, e `CREATE` não) custa o sandbox morrer com a sessão. A
  [etapa 5](PLAN-STAGE-5.md) decide quando o primeiro pipeline rodar lá.
- **O que `svv_table_info` responde depois do `USE`.** Antes do `USE` ela enxerga só o banco local,
  como `has_schema_privilege` e `information_schema.columns`, que a suíte leu depois do `USE` em
  2026-09-21, quatro vezes a primeira e três a segunda: `false` e vazio para o esquema do datashare,
  com o `CREATE TABLE` passando nele ([`POC.md`](POC.md), [`redshift.md`](redshift.md)). A execução do
  probe de 2026-09-21 não leu `RS-8` porque `RS-19` reprovou pelo critério errado
  (`current_database()` continuou `dev` depois do `USE`, que vale mesmo assim; o critério passou a
  ser a resolução de um nome em duas partes), e a próxima execução no ambiente alvo o lê.
- **Versões não correntes.** O bucket é versionado e o papel não lê o ciclo de vida: cada exclusão
  (o `vacuum`, a limpeza da suíte S3) deixa uma versão não corrente invisível à listagem. `BK-14`
  conta o acumulado, e a regra `NoncurrentVersionExpiration` sob a raiz, junto com
  `AbortIncompleteMultipartUpload`, é pergunta para quem administra o bucket.
- **Credenciais de uma hora.** Nenhuma execução mais longa que uma emissão rodou ainda; a
  [etapa 3](PLAN-STAGE-3.md) renova `storage_options` a cada chamada, e a primeira execução longa
  no espaço confirma que o delta-rs e o `boto3` renovam pela cadeia padrão. A credencial do Redshift
  tem o mesmo teto (`GetCredentials`, 3600 segundos): o que acontece com uma conexão aberta quando a
  senha expira, e se ela cai no meio de um `COPY`, ainda não foi medido. As credenciais que o `COPY`
  e o `UNLOAD` levam no texto do comando expiram com as do espaço, e `RS-18` imprime quando; um
  `COPY` mais longo que isso também não foi medido.
- **Os relatórios dos probes de 2026-09-21.** O usuário os guardou em `secrets/probes-aws-bn/`, fora
  do git; [`POC.md`](POC.md) os interpreta, e `docs/readings/` não os tem. Copiá-los para
  `docs/readings/`, como os de 2026-09-20, é decisão do usuário: eles trazem os mesmos
  identificadores (conta, papel, usuário do banco) que os relatórios já versionados.
- **Quanto o teste de alcance poupa no ambiente alvo.** O IAM (`iam.amazonaws.com`) e o KMS não têm
  endpoint VPC lá, e `simulate_principal_policy` esperou 10 s e `describe_key` 80 s por nada em
  2026-09-21. As duas passaram a `short_config(2, 5, 1)` atrás de um teste TCP de 2 s num endereço
  (`probelib.endpoint_reachable`), que no macOS no mesmo dia baixou de 10,0 s para 2,0 s a espera por
  um endereço sem rota ([`POC.md`](POC.md)). A próxima execução dos probes no alvo diz o que sobra;
  a permissão sobre a raiz fica provada pela primeira escrita.
- **`Text` no Redshift.** O `sqlalchemy-redshift` compila `Text` como `TEXT`, que o Redshift guarda
  como `VARCHAR(256)`. A [etapa 1](PLAN-STAGE-1.md) emite `VARCHAR(65535)` por uma regra
  `@compiles(Text, "redshift")` em `ddl`, em vez de exigir `String(65535)` nos modelos; a escolha
  ainda não foi confirmada pelo usuário.
- **Barreira por tabela.** Um cliente que dispara `load` numa thread e esquece o `result()` lê o
  estado anterior em silêncio, porque o DuckDB não espera. A guarda: `load` marca a tabela em voo,
  e `query` e `execute` esperam as tabelas em voo que o statement referencia, tiradas por
  `find_tables` do statement Core ou do sentinela `{prefix}` do texto gerado
  (`test_parallel.py::test_table_barrier_delays_the_read_until_the_load_lands`). Fica fora das
  etapas até existir um pipeline paralelo real.
- **A memória da partição de `cad_lancamentos`.** Cerca de 700 MB de Parquet e 35 milhões de
  linhas por partição; a primeira carga real mede o `write_deltalake` de um leitor e o `COPY ...
  RETURN_STATS` mais `register_files` antes de fixar o padrão ([etapa 7](PLAN-STAGE-7.md)). `export_mode="rewrite"` e `"register"` medem os dois caminhos em cada motor e na carga inicial
  (etapas [4](PLAN-STAGE-4.md), [5](PLAN-STAGE-5.md) e [7](PLAN-STAGE-7.md)), e a medição decide o
  padrão da flag.

## O que a próxima execução da suíte Redshift lê

As perguntas do `COPY` que a documentação oficial não responde foram fechadas pelas execuções de
2026-09-21 às 12:08 e às 12:10 no ambiente alvo ([`POC.md`](POC.md), [`redshift.md`](redshift.md)):
o `COPY` de Parquet aceita lista de colunas, carrega `DECIMAL(18, 2)` em `INT64` e `timestamp_ntz`
em `INT64` de microssegundos, aborta numa string maior que o `VARCHAR` de destino, associa as colunas
por posição e recusa um arquivo com colunas a menos. O que resta é leitura da próxima execução, com
o teste que a faz:

- **As linhas que `FILLRECORD` carrega** de um arquivo anterior a uma coluna nova: o `COPY` com a
  cláusula passou nas duas execuções, e a contagem que vinha depois recebeu o `34510` do driver
  (`test_copy_column_list_and_fillrecord`); a [etapa 8](PLAN-STAGE-8.md) escolhe entre a lista de
  colunas, confirmada, e o `FILLRECORD`.
- **O cache de prepared statements desligado.** `connect_redshift` passa `max_prepared_statements=0`
  desde as execuções das 12:08 e das 12:10; `test_repeated_statement_after_truncate_and_the_driver_cache`
  afirma que o mesmo comando passa antes e depois de um `TRUNCATE` na conexão da sessão e registra o
  que uma conexão com o cache do driver recebe: na repetição depois do `TRUNCATE`, na segunda
  repetição, depois de um `ALTER` e numa tabela temporária do banco da conexão. O `connect` da
  [etapa 5](PLAN-STAGE-5.md) leva a mesma opção.
- **`SUPER` acima de 65.535 bytes pelo `COPY`.** O `INSERT ... JSON_PARSE(%s)` de 80.901 bytes
  passou; o `COPY` de Parquet com a coluna em texto exige `SERIALIZETOJSON`. A suíte lê o que a
  cláusula grava (`json_typeof`, `json_size` e o `JSON_PARSE` do texto de volta) e o
  `COPY ... FORMAT JSON 'auto'` de um documento como objeto (`test_super_and_json_parse`). A decisão
  da [etapa 8](PLAN-STAGE-8.md): o teto de 65.535 bytes no contrato do campo JSON, aplicado pela
  auditoria, ou um caminho por JSON para os documentos maiores.
- **`TRUNCATECOLUMNS` no `COPY` de Parquet**, que a lista de opções aceitas não inclui
  (`test_copy_varchar_overflow`): se for aceito, é a degradação que a auditoria da
  [etapa 4](PLAN-STAGE-4.md) dispensa.
- **As linhas na fila do cursor antes do primeiro `fetchmany`**
  (`test_cursor_fetchmany_feeds_record_batches`, `redshift.driver.rows_cached_after_execute`): o
  código do driver lê o resultado inteiro no `execute` ([`redshift.md`](redshift.md)), e a leitura
  confirma; o limite de linhas a partir do qual `stream` passa a `UNLOAD` é decisão da
  [etapa 5](PLAN-STAGE-5.md).

## Decisões de API pendentes por etapa

Cada arquivo de etapa fecha com a seção "Decisões pendentes"; a lista abaixo as reúne, e uma decisão
tomada sai daqui e do arquivo da etapa no mesmo commit.

- [Etapa 1](PLAN-STAGE-1.md): `Text` como `VARCHAR(65535)` por `@compiles`; `String(n)` medido em
  bytes; a coluna sem comentário como violação de `check_models`; `duckdb-engine` e
  `sqlalchemy-redshift` como dependências de execução enquanto `ddl` compilar pelo dialeto.
- [Etapa 2](PLAN-STAGE-2.md): identificadores entre aspas duplas em `bind`; o `sqlglot` no grupo
  `dev`.
- [Etapa 3](PLAN-STAGE-3.md): a reserva de credenciais do `boto3` em `storage_options`; as colunas
  com estatística registrada; `version_diff` quando o log foi limpo.
- [Etapa 4](PLAN-STAGE-4.md): `loader` numa tabela que já existe; o padrão de `memory_limit`; o
  banco em arquivo como padrão; a amostra do `AuditReport`.
- [Etapa 5](PLAN-STAGE-5.md): onde as tabelas `exec_<id>_*` nascem; a confirmação do `USE` pela
  criação da tabela de controle; os limites entre `fetchmany` e `UNLOAD` e entre `INSERT` e `COPY`;
  a tabela de OIDs de `schema_from_description`; o destino de `export_partition` por partição
  (`<uri>/<execution_id>/<valor>/` com `PARTITION BY`, ou `<uri>/<coluna>=<valor>/<execution_id>/`
  sem ele), porque o `UNLOAD` confere o destino como prefixo.
- [Etapa 6](PLAN-STAGE-6.md): `--metadata` na linha de comando; a chave de `next_ids` numa chave
  composta; a barreira por tabela.
- [Etapa 7](PLAN-STAGE-7.md): a `sort_key` na consulta da carga; o padrão de `export_mode` na carga.
- [Etapa 8](PLAN-STAGE-8.md): a staging da publicação no datashare ou temporária; a lista de
  colunas do `COPY`, confirmada, ou `FILLRECORD`, com a contagem por ler; o teto de 65.535 bytes do
  campo JSON no Redshift ou um caminho por JSON para os documentos maiores.
- [Etapa 9](PLAN-STAGE-9.md): o nome do runbook; a marca de arquivamento no controle; a retenção do
  `vacuum` mensal.
