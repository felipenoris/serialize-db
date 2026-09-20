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

- `COPY ... FORMAT AS PARQUET MANIFEST` de arquivos gravados pelo delta-rs: `DECIMAL(18, 2)` em
  `INT64`, `timestamp_ntz` em `INT64` de microssegundos, o que acontece com uma string acima do
  `VARCHAR` de destino (truncar ou abortar), a lista de colunas no `COPY`, `FILLRECORD` para
  arquivos anteriores a uma coluna nova, e `SUPER` direto do `COPY` para documentos acima de
  65.535 bytes.
- `UNLOAD ... PARTITION BY (mes) MANIFEST VERBOSE`: os tipos físicos de `TIMESTAMP` e `DECIMAL`, se
  as colunas saem `required`, se há estatísticas de mínimo e máximo, e o registro dos arquivos por
  `create_write_transaction`, lido pelo DuckDB.
- Se o Redshift Spectrum mapeia colunas Parquet por nome ou por posição, só para registro; o
  projeto não cria esquemas externos.
- O banco do esquema do projeto: a sessão enxerga `datalake_rw_shared.sbx_aco_decon` (`RS-16`,
  2026-09-20), e depois de `USE datalake_rw_shared` o `CREATE TABLE`, o `COPY` de uma pasta, o
  `SELECT` e o `UNLOAD` passaram por `sbx_aco_decon.<tabela>`
  ([`../examples/redshift_copy_unload.py`](../examples/redshift_copy_unload.py)); o `SELECT` em três
  partes passou de `dev`. Faltam o `INSERT`, o `DELETE` e o `MERGE` da publicação, o `COPY ...
  MANIFEST` e o `UNLOAD ... PARTITION BY`. Os requisitos da escrita num datashare que a sessão não lê
  (isolamento do produtor, slices) não impediram a escrita.
- O ciclo da Data API com `select`, que devolve `DECIMAL` como texto: a prova de que existe caminho
  sem a porta 5439, e a razão de ela ficar fora da biblioteca.

Antes de qualquer etapa na AWS, os probes rodam no ambiente e o resultado é colado na conversa:
`space.py` e `diagnose_aws.py` para a suíte S3, `bucket.py` para a raiz escolhida, `redshift.py`
para a [etapa 5](PLAN-STAGE-5.md). O `redshift.py` rodou no ambiente alvo em 2026-09-20 e respondeu o
papel IAM do `COPY` (`RS-6`: nenhum, e as credenciais de quem chama o substituem) e o que a sessão
lê dos requisitos do datashare (`RS-17`); a execução seguinte, com o probe revisto, confere o `USE`
(`RS-19`), o que `has_schema_privilege` e `svv_table_info` respondem depois dele (`RS-5`, `RS-8`) e
onde as tabelas de execução podem nascer (`RS-9`), a questão de
[`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md) que a suíte não alcança sem escrever.
