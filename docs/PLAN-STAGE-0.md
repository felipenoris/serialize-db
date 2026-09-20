# Etapa 0: prova de conceito na AWS

A entrega e o critério de aceite desta etapa estão na tabela de etapas de [`PLAN.md`](PLAN.md), que
também fixa as decisões, as regras que toda etapa obedece e a ordem do trabalho.

Os itens de S3 estão verificados ([`POC.md`](POC.md)). Os de Redshift estão em
`tests/proof_of_concept/test_redshift.py`, marcador
`redshift`, escrito antes de haver conexão e ainda não executado; ele roda quando
`probes/redshift.py` mostrar a conexão, com os arquivos sob `SERIALIZE_DB_TEST_S3_ROOT` e as
tabelas no esquema de `SERIALIZE_DB_TEST_REDSHIFT_SCHEMA`:

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

Antes de qualquer etapa na AWS, os probes rodam no ambiente e o resultado é colado na conversa:
`space.py` e `diagnose_aws.py` para a suíte S3, `bucket.py` para a raiz escolhida, `redshift.py`
para a [etapa 5](PLAN-STAGE-5.md).
