# Questões em aberto

Este documento registra o que ainda não tem resposta: as pendências do projeto, as decisões que
esperam o usuário e as perguntas que uma leitura ou um teste vai fechar. Um item sai daqui quando a
resposta entra no documento que a guarda, e a saída nomeia esse documento. O plano está em
[`PLAN.md`](PLAN.md), o estado da implementação em [`CURRENT_STATE.md`](CURRENT_STATE.md) e o que já
foi medido em [`POC.md`](POC.md).

- **Onde ficam as tabelas de execução.** O sandbox `exec_<id>_*` da [etapa 4](PLAN-STAGE-4.md) e as
  stagings do `COPY` nascem no banco do datashare, onde o `CREATE TABLE` passou, e herdam as
  restrições dele: escrita num banco por transação, sem `VIEW`. A alternativa da tabela temporária
  (`TEMP` é verdadeiro no banco da conexão, e `CREATE` não; a suíte criou, consultou e truncou uma
  depois do `USE` em 2026-09-21) custa o sandbox morrer com a sessão. A
  [etapa 5](PLAN-STAGE-5.md) decide quando o primeiro pipeline rodar lá.
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
  [etapa 3](PLAN-STAGE-3.md) renova `storage_options` a cada chamada, e a primeira execução longa
  no espaço confirma que o delta-rs e o `boto3` renovam pela cadeia padrão. A credencial do Redshift
  tem o mesmo teto (`GetCredentials`, 3600 segundos): o que acontece com uma conexão aberta quando a
  senha expira, e se ela cai no meio de um `COPY`, ainda não foi medido. As credenciais que o `COPY`
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
- **Barreira por tabela.** Um cliente que dispara `load` numa thread e esquece o `result()` lê o
  estado anterior em silêncio, porque o DuckDB não espera. A guarda: `load` marca a tabela em voo,
  e `query` e `execute` esperam as tabelas em voo que o statement referencia, tiradas por
  `find_tables` do statement Core ou do sentinela `{prefix}` do texto gerado
  (`test_parallel.py::test_table_barrier_delays_the_read_until_the_load_lands`). Fica fora das
  etapas até existir um pipeline paralelo real.
- **A memória da partição de `cad_lancamentos`.** Cerca de 700 MB de Parquet e 35 milhões de
  linhas por partição; a primeira carga real mede o `write_deltalake` de um leitor e o `COPY ...
  RETURN_STATS` mais `register_files` antes de fixar o padrão ([etapa 7](PLAN-STAGE-7.md)); a
  migração adiantada (`scripts/migrate_parquet_to_delta.py`, logo depois da etapa 1) é essa carga. `export_mode="rewrite"` e `"register"` medem os dois caminhos em cada motor e na carga inicial
  (etapas [4](PLAN-STAGE-4.md), [5](PLAN-STAGE-5.md) e [7](PLAN-STAGE-7.md)), e a medição decide o
  padrão da flag.

## Decisões de API pendentes por etapa

Cada arquivo de etapa fecha com a seção "Decisões pendentes"; a lista abaixo as reúne, e uma decisão
tomada sai daqui e do arquivo da etapa no mesmo commit.

- [Etapa 3](PLAN-STAGE-3.md): o comentário da tabela como `description` da tabela Delta; a reserva de credenciais do `boto3` em `storage_options`; as colunas
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
