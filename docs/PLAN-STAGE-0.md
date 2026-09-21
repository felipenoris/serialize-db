# Etapa 0: prova de conceito na AWS

A entrega e o critério de aceite desta etapa estão na tabela de etapas de [`PLAN.md`](PLAN.md), que
também fixa as decisões, as regras que toda etapa obedece e a ordem do trabalho.

Os itens de S3 estão verificados ([`POC.md`](POC.md)). Os de Redshift estão em
`tests/proof_of_concept/test_redshift.py`, marcador `redshift`, ainda não executado; ele roda com os
arquivos sob `SERIALIZE_DB_TEST_S3_ROOT` e as tabelas no esquema de
`SERIALIZE_DB_TEST_REDSHIFT_SCHEMA`, no banco de `SERIALIZE_DB_REDSHIFT_SHARE_DATABASE`. O caminho
de conexão já está fixado: a credencial temporária do workgroup serverless, de
[`../examples/redshift_native.py`](../examples/redshift_native.py), executado no ambiente alvo em
2026-09-20.

- `COPY ... FORMAT AS PARQUET MANIFEST` de arquivos gravados pelo delta-rs: o comando passou no
  datashare em 2026-09-21, com 500.000 linhas em `INT64`, `INT32` de data, `BYTE_ARRAY` e `DOUBLE`
  ([`../examples/redshift_manifest.py`](../examples/redshift_manifest.py)). Faltam as colunas que a
  base de origem não tem nem gera: `DECIMAL(18, 2)` em `INT64`, `timestamp_ntz` em `INT64` de
  microssegundos, o que acontece com uma string acima do `VARCHAR` de destino (truncar ou abortar),
  a lista de colunas no `COPY`, `FILLRECORD` para arquivos anteriores a uma coluna nova, e `SUPER`
  direto do `COPY` para documentos acima de 65.535 bytes.
- `UNLOAD ... PARTITION BY (<coluna de partição>) MANIFEST VERBOSE`: **verificado** em 2026-09-21
  por [`../examples/redshift_manifest.py`](../examples/redshift_manifest.py), que exercita este
  item e o `COPY ... MANIFEST` do item anterior num script só, com `cast` para `DECIMAL` e
  `TIMESTAMP` no `select` porque a base de origem não tem coluna de nenhum dos dois tipos. O
  comando é aceito a partir de uma tabela do datashare, grava na convenção Hive com a coluna de
  partição fora dos arquivos, e `create_write_transaction` registrou os arquivos numa tabela Delta
  que devolveu as linhas. Os tipos físicos, a obrigatoriedade das colunas e as estatísticas estão em
  [`POC.md`](POC.md) e [`redshift.md`](redshift.md); o `TIMESTAMP` sai em `INT96`, o
  `DECIMAL(18, 2)` em `FIXED_LEN_BYTE_ARRAY(8)`, toda coluna sai `optional` e há mínimo e máximo.
- Se o Redshift Spectrum mapeia colunas Parquet por nome ou por posição, só para registro; o
  projeto não cria esquemas externos.
- O banco do esquema do projeto: a sessão enxerga `datalake_rw_shared.sbx_aco_decon` (`RS-16`,
  2026-09-20), e depois de `USE datalake_rw_shared` o `CREATE TABLE`, o `COPY` de uma pasta, o
  `SELECT` e o `UNLOAD` passaram por `sbx_aco_decon.<tabela>`
  ([`../examples/redshift_copy_unload.py`](../examples/redshift_copy_unload.py)); o `SELECT` em três
  partes passou de `dev`. Em 2026-09-21 passaram também o `COPY ... MANIFEST`, o `INSERT ... SELECT`
  e o `UNLOAD ... PARTITION BY ... MANIFEST VERBOSE`
  ([`../examples/redshift_manifest.py`](../examples/redshift_manifest.py)). Faltam o `DELETE` e o
  `MERGE` da publicação. Os requisitos da escrita num datashare que a sessão não lê (isolamento do
  produtor, slices) não impediram a escrita.
- O ciclo da Data API com `select`, que devolve `DECIMAL` como texto: a prova de que existe caminho
  sem a porta 5439, e a razão de ela ficar fora da biblioteca.

Antes de qualquer etapa na AWS, os probes rodam no ambiente e o resultado é colado na conversa:
`space.py` e `diagnose_aws.py` para a suíte S3, `bucket.py` para a raiz escolhida, `redshift.py`
para a [etapa 5](PLAN-STAGE-5.md) e `parquet_source.py` para a base de origem da
[etapa 7](PLAN-STAGE-7.md). O `redshift.py` rodou no ambiente alvo em 2026-09-20 e respondeu o papel
IAM do `COPY` (`RS-6`: nenhum, e as credenciais de quem chama o substituem) e o que a sessão lê dos
requisitos do datashare (`RS-17`); a execução seguinte, com o probe revisto, confere o `USE`
(`RS-19`), o que `has_schema_privilege` e `svv_table_info` respondem depois dele (`RS-5`, `RS-8`) e
onde as tabelas de execução podem nascer (`RS-9`), a questão de
[`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md) que a suíte não alcança sem escrever.
