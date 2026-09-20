# Questões em aberto

Este documento registra o que ainda não tem resposta: as pendências do projeto, as decisões que
esperam o usuário e as perguntas que uma leitura ou um teste vai fechar. Um item sai daqui quando a
resposta entra no documento que a guarda, e a saída nomeia esse documento. O plano está em
[`PLAN.md`](PLAN.md), o estado da implementação em [`CURRENT_STATE.md`](CURRENT_STATE.md) e o que já
foi medido em [`POC.md`](POC.md).

- **Papel IAM para o `COPY` e o `UNLOAD`.** O namespace `controladoria-ns` não tem papel IAM
  padrão nem papel associado (leitura de 2026-09-20, `RS-6`), e o Redshift só usa papel associado ao
  namespace, com ou sem ARN explícito: enquanto isso não mudar, nenhum `COPY` do S3 e nenhum
  `UNLOAD` rodam, o que bloqueia as etapas [5](PLAN-STAGE-5.md) e [8](PLAN-STAGE-8.md). É pedido
  para quem administra o namespace: associar um papel com `s3:ListBucket`, `GetObject` e `PutObject`
  sob a raiz do projeto, e de preferência torná-lo o padrão. Depois disso, `RS-6` e `RS-11` medem.
- **Escrita no banco do datashare.** O esquema do projeto é `sbx_aco_decon` no banco
  `datalake_rw_shared`, do datashare `controladoria_rw_datashare` (leitura de 2026-09-20), e um
  objeto de datashare só aceita escrita quando o produtor concede `INSERT` e `CREATE`. A sessão lê o
  esquema (três tabelas em `svv_all_tables`), mas a ACL do banco nomeia só a role administrativa do
  SSO, e ninguém criou tabela lá pela biblioteca. Dos três requisitos do consumidor, a leitura fecha
  um: o patch `1.0.436211` atende. O isolamento do banco que recebe a escrita fica no produtor e
  chega como `UNKNOWN`, e `stv_slices` é negada a um usuário comum, então os 64 slices não são
  verificáveis daqui — a capacidade base do workgroup é 8 RPU. Quem responde é
  `test_schema_location_and_three_part_name`, que cria, insere e lê pelo nome em três partes; a
  resposta entra em [`redshift.md`](redshift.md), e uma recusa muda a [etapa 8](PLAN-STAGE-8.md).
- **`UNLOAD` a partir de uma tabela do datashare.** A documentação lista os comandos que a escrita
  num datashare aceita e o `UNLOAD` não está entre eles, sem dizer que ele é recusado. O
  `export_partition` da [etapa 5](PLAN-STAGE-5.md) depende dele;
  `test_unload_partition_by_and_register` registra o resultado e pula o resto quando o esquema vem
  de um datashare.
- **Onde ficam as tabelas de execução.** O sandbox `exec_<id>_*` da [etapa 4](PLAN-STAGE-4.md) e as
  stagings do `COPY` herdam as restrições do datashare se nascerem lá. A leitura de 2026-09-20
  eliminou uma das alternativas: `has_database_privilege(dev, CREATE)` é falso, então o banco local
  da conexão não recebe tabela nenhuma. Restam a tabela temporária (`TEMP` é verdadeiro) e o próprio
  datashare, se o produtor tiver concedido `CREATE`. A tabela temporária custa o sandbox morrer com
  a sessão, o que a [etapa 5](PLAN-STAGE-5.md) precisa acomodar se for o caminho.
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
- Se o `COPY ... MANIFEST FORMAT AS PARQUET COMPUPDATE OFF` numa tabela de datashare se comporta
  como numa tabela local, e em que visão o motivo de uma recusa aparece: `stl_load_errors` cobre só
  clusters provisionados, e `sys_load_error_detail` cobre os dois.
