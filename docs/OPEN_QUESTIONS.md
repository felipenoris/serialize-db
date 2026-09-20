# Questões em aberto

Este documento registra o que ainda não tem resposta: as pendências do projeto, as decisões que
esperam o usuário e as perguntas que uma leitura ou um teste vai fechar. Um item sai daqui quando a
resposta entra no documento que a guarda, e a saída nomeia esse documento. O plano está em
[`PLAN.md`](PLAN.md), o estado da implementação em [`CURRENT_STATE.md`](CURRENT_STATE.md) e o que já
foi medido em [`POC.md`](POC.md).

- **Redshift.** A prova de conceito espera uma conexão no projeto; os itens estão na
  [etapa 0](PLAN-STAGE-0.md). A primeira leitura de `probes/redshift.py` fixa como a
  [etapa 5](PLAN-STAGE-5.md) conecta (senha ou IAM, cluster ou serverless) e qual papel o `COPY`
  usa; no laboratório as APIs do Redshift não têm endpoint VPC (`RS-14`), então a etapa 5 nasce
  com a conexão por senha e trata IAM e Data API como opcionais.
- **Versões não correntes.** O bucket é versionado e o papel não lê o ciclo de vida: cada exclusão
  (o `vacuum`, a limpeza da suíte S3) deixa uma versão não corrente invisível à listagem. `BK-14`
  conta o acumulado, e a regra `NoncurrentVersionExpiration` sob a raiz, junto com
  `AbortIncompleteMultipartUpload`, é pergunta para quem administra o bucket.
- **Credenciais de uma hora.** Nenhuma execução mais longa que uma emissão rodou ainda; a
  [etapa 3](PLAN-STAGE-3.md) renova `storage_options` a cada chamada, e a primeira execução longa
  no espaço confirma que o delta-rs e o `boto3` renovam pela cadeia padrão.
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
- **O proxy do DuckDB fora do `prepare_offline.sh`.** O DuckDB recusa o endereço com as
  credenciais embutidas, e só o script separa endereço e credenciais hoje ([`POC.md`](POC.md)).
  Toda chamada HTTP do DuckDB na rede corporativa cai no mesmo erro: os probes `space.py` e
  `diagnose_aws.py`, e o acesso ao S3 pelo `httpfs` na [etapa 3](PLAN-STAGE-3.md). No destino
  não há proxy, e é decisão do usuário se a separação vira função compartilhada dos probes e da
  biblioteca ou continua só no script.
- **A memória da partição de `cad_lancamentos`.** Cerca de 700 MB de Parquet e 35 milhões de
  linhas por partição; a primeira carga real mede o `write_deltalake` de um leitor e o `COPY ...
  RETURN_STATS` mais `register_files` antes de fixar o padrão ([etapa 7](PLAN-STAGE-7.md)).

## O que a documentação oficial do Redshift não responde

Todas dependem da conexão, que o projeto ainda não tem, e `tests/proof_of_concept/test_redshift.py`
tem um teste por pergunta; as perguntas de S3 foram respondidas em 2026-09-19. A
[etapa 0](PLAN-STAGE-0.md) as agrupa por comando.

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
