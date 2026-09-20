# Questões em aberto

Este documento registra o que ainda não tem resposta: as pendências do projeto, as decisões que
esperam o usuário e as perguntas que uma leitura ou um teste vai fechar. Um item sai daqui quando a
resposta entra no documento que a guarda, e a saída nomeia esse documento. O plano está em
[`PLAN.md`](PLAN.md), o estado da implementação em [`CURRENT_STATE.md`](CURRENT_STATE.md) e o que já
foi medido em [`POC.md`](POC.md).

- **`UNLOAD ... PARTITION BY` a partir de uma tabela do datashare.** O `UNLOAD` simples passou no
  ambiente alvo em 2026-09-20 ([`../examples/redshift_copy_unload.py`](../examples/redshift_copy_unload.py)),
  e a documentação não o lista nem entre os comandos aceitos nem entre os recusados. O que o
  `export_partition` da [etapa 5](PLAN-STAGE-5.md) precisa é `PARTITION BY (<coluna>) MANIFEST
  VERBOSE`, que ninguém exercitou lá; `test_unload_partition_by_and_register` registra o resultado e
  pula o resto quando a recusa vem do datashare.
- **`COMPUPDATE` explícito no `COPY` de um datashare.** O `COPY` sem cláusula alguma passou, e é o
  que a biblioteca emite. Se `COMPUPDATE OFF` explícito é aceito, ou se a frase da documentação
  ("`COPY` sem `COMPUPDATE`") quer dizer que a análise de compressão precisa estar desligada, nenhum
  teste respondeu.
- **Onde ficam as tabelas de execução.** O sandbox `exec_<id>_*` da [etapa 4](PLAN-STAGE-4.md) e as
  stagings do `COPY` nascem no banco do datashare, onde o `CREATE TABLE` passou, e herdam as
  restrições dele: escrita num banco por transação, sem `VIEW`. A alternativa da tabela temporária
  (`TEMP` é verdadeiro no banco da conexão, e `CREATE` não) custa o sandbox morrer com a sessão. A
  [etapa 5](PLAN-STAGE-5.md) decide quando o primeiro pipeline rodar lá.
- **Versões não correntes.** O bucket é versionado e o papel não lê o ciclo de vida: cada exclusão
  (o `vacuum`, a limpeza da suíte S3) deixa uma versão não corrente invisível à listagem. `BK-14`
  conta o acumulado, e a regra `NoncurrentVersionExpiration` sob a raiz, junto com
  `AbortIncompleteMultipartUpload`, é pergunta para quem administra o bucket.
- **Credenciais de uma hora.** Nenhuma execução mais longa que uma emissão rodou ainda; a
  [etapa 3](PLAN-STAGE-3.md) renova `storage_options` a cada chamada, e a primeira execução longa
  no espaço confirma que o delta-rs e o `boto3` renovam pela cadeia padrão. A credencial do Redshift
  tem o mesmo teto (`GetCredentials`, 3600 segundos): o que acontece com uma conexão aberta quando a
  senha expira, e se ela cai no meio de um `COPY`, ainda não foi medido.
- **Manutenção da suíte S3.** Se `diagnose_aws.py` confirmar o cenário sem proxy: exportar
  `AWS_DEFAULT_REGION` a partir de `AWS_REGION`, tornar a chamada ao STS opcional com espera curta e
  passar `AWS_ENDPOINT_URL` ao secret do DuckDB. A [etapa 3](PLAN-STAGE-3.md) implementa o mesmo
  na biblioteca.
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
- **`fetchmany` do `redshift_connector`.** O `stream` do motor Redshift monta cada lote de
  `cursor.fetchmany(batch_size)`; se o driver lê as linhas do socket a cada chamada ou materializa o
  resultado inteiro no `execute` decide se `stream` limita a memória sem `UNLOAD`.
  `test_redshift.py::test_cursor_fetchmany_feeds_record_batches` exercita o caminho, e a primeira
  execução com uma consulta grande mede a memória ([etapa 5](PLAN-STAGE-5.md)).
- **A memória da partição de `cad_lancamentos`.** Cerca de 700 MB de Parquet e 35 milhões de
  linhas por partição; a primeira carga real mede o `write_deltalake` de um leitor e o `COPY ...
  RETURN_STATS` mais `register_files` antes de fixar o padrão ([etapa 7](PLAN-STAGE-7.md)).

## O que a documentação oficial do Redshift não responde

Todas dependem de uma execução da suíte contra o Redshift do ambiente alvo, que ainda não houve; o
caminho de conexão está fixado desde 2026-09-20 ([`../examples/`](../examples/)), e
`tests/proof_of_concept/test_redshift.py` tem um teste por pergunta. As perguntas de S3 foram
respondidas em 2026-09-19. A [etapa 0](PLAN-STAGE-0.md) as agrupa por comando.

- Se o `COPY` de Parquet aceita lista de colunas: o `awswrangler` emite
  `COPY tabela (colunas) ... FORMAT AS PARQUET`, e a referência descreve a lista só para arquivos
  planos.
- A correspondência entre os tipos físicos do Parquet e as colunas do Redshift no `COPY`, em
  especial `TIMESTAMP` como `INT64` em microssegundos e o tipo físico de `DECIMAL`.
- Se o `COPY` trunca ou aborta numa string maior que o `VARCHAR` de destino.
- Se o Spectrum mapeia colunas Parquet soltas por nome ou por posição.
- Os tipos físicos que o `UNLOAD` grava para `TIMESTAMP` e `DECIMAL`, se as suas colunas saem
  `required` e se ele grava estatísticas de mínimo e máximo; os três afetam a `AddAction` de
  `register_file` em [`delta.md`](delta.md).
- Se o `FILLRECORD` deixa o `COPY` carregar arquivos antigos, sem as colunas acrescentadas depois,
  que a evolução do Delta e do DuckLake produz.
- Se o `COPY ... MANIFEST FORMAT AS PARQUET` numa tabela de datashare se comporta como numa tabela
  local: o `COPY` de um prefixo de pasta passou em 2026-09-20, o de um manifesto ainda não.
