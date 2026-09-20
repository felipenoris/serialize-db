# Etapa 1: `schema`

A entrega e o critério de aceite desta etapa estão na tabela de etapas de [`PLAN.md`](PLAN.md), que
também fixa as decisões, as regras que toda etapa obedece e a ordem do trabalho.

O módulo `serialize_db.schema` deriva dos modelos tudo o que os outros módulos precisam saber sobre
uma tabela. A etapa começa pelo modelo de referência de `tests/model/`, corrigido como a biblioteca
cliente o escreveria: `Base` importável de um módulo só, `Numeric(18, 2)` nas colunas monetárias, `autoincrement=False` nas chaves inteiras,
chaves estrangeiras sem `DEFERRABLE`, a coluna `mes` (`String(7)`) nas tabelas particionadas,
comentários de tabela e de coluna, e `Table.info["serialize_db"]` com `partition_by`, `sort_key` e
`redshift`, como em `schema.md`.

O modelo de referência bate com a base de origem lida em 2026-09-20 ([`POC.md`](POC.md)): as 12
tabelas existem nos arquivos com as mesmas colunas, na mesma ordem e com os tipos da tabela de
`schema.md` (`Integer` em `int32`, `String` em `string`, `Date` em `date32`, `Double` em `double`,
`Boolean` em `bool`, `DateTime` em `timestamp`), e a nulidade declarada é a mesma, exceto em sete
colunas de `cad_contratos` (`sistema`, `um`, `to`, `fonte`, `taxa_juros_fixos`,
`data_primeira_amortizacao`, `data_ultima_amortizacao`), anuláveis nos arquivos e `NOT NULL` no
modelo, sem nulo algum nos dados. A origem tem duas tabelas fora do modelo, `alembic_version` e
`meta_update_status`. A coluna `mes` deriva de `data` em `cad_contratos`, `cad_operacoes` e
`rel_contrato_operacao` e de `data_base` em `cad_lancamentos` (a partição da origem e
`meta_update_status` dizem isso), então o modelo corrigido declara em `Table.info["serialize_db"]`
a coluna de data de que `mes` deriva, que a auditoria e a carga inicial leem; o nome da chave
espera o usuário ([`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md)). A única coluna monetária é `valor`
de `cad_lancamentos`, com três casas decimais na origem; se ela vira `Numeric(18, 2)` ou continua
`Double` espera o usuário, e as taxas e os fatores (`fator`, `fator_rateio`, `taxa_*`, `spread_*`,
`custo_adicional`, `taxa_juros_fixos`) continuam `Double`. `id_lancamento` chega a 1.113.599.996
em `int32`, e `BigInteger` nessas chaves também espera o usuário.

| Primitiva | O que faz |
| --- | --- |
| `arrow_schema(table)` | O `pa.Schema` do `Table`: os tipos da tabela de `schema.md`, a nulidade, o comentário de cada coluna em `metadata` do campo, `PARQUET:field_id`; um campo JSON é `string`, sem a extensão `arrow.json`. |
| `delta_schema(table)` | O `deltalake.Schema` derivado do Arrow: `decimal(18,2)`, `timestamp_ntz` para `DateTime` sem fuso e `timestamp` para o com fuso, `string` para JSON e UUID, comentários preservados. |
| `ddl(table, dialect, prefix="")` | `CREATE TABLE` para `duckdb` ou `redshift`: sem `DEFERRABLE` nem `Identity`, `CHECK` só no DuckDB, chaves só quando `table_options` as pede, `SORTKEY`, `DISTSTYLE` e `DISTKEY` no Redshift, `Text` como `VARCHAR(65535)`, `Uuid` como `VARCHAR(36)`, `JSON` como `SUPER`; `prefix` renomeia a tabela para o sandbox. |
| `table_options(table)` | O `TableOptions` (`partition_by`, `sort_key`, `redshift`, `keys` e a coluna de data de que `mes` deriva) lido de `Table.info["serialize_db"]` com os padrões da biblioteca: partição por `mes` quando a coluna existe e as chaves do próprio modelo, `table.primary_key` e os `UniqueConstraint`; `keys` só acrescenta uma chave de negócio ou exclui uma delas, sempre de forma explícita. |
| `cast(data, table)` | A `pa.Table`, ou o `RecordBatchReader` lote a lote, convertida para `arrow_schema(table)` com `safe=True` e devolvida no mesmo tipo: as colunas do contrato presentes, na ordem do contrato; `large_string` para `string`, timestamps a microssegundos e UTC, inteiro em `Numeric` por `decimal128(21, 2)`; recusa perda de precisão, `double` fora da escala e `timestamp` com hora numa coluna `Date` (as duas perdas que `safe=True` não acusa), `struct` numa coluna JSON, texto acima de `String(n)` e nulo em coluna `NOT NULL`, com a mensagem que diz o que o cliente faz antes de chamar. |
| `check_models(metadata)` | A lista de violações do contrato nos modelos: tipo fora da tabela de tipos, `Double` em coluna monetária, `autoincrement` em chave inteira, `DEFERRABLE`, `Identity`, tabela particionada sem `mes`, tabela sem chave primária e sem `keys`, coluna sem comentário. Vazia nos modelos corrigidos. |
| `schema_files(metadata)` | `{"<tabela>.delta.json": ..., "<tabela>.duckdb.sql": ..., "<tabela>.redshift.sql": ...}` em memória. |
| `write_schema_files(metadata, directory)` | Grava `schema_files` em `directory`; `serialize-db schema write` grava e `serialize-db schema check` compara sem gravar. |

Testes: `tests/test_schema.py`, sem gravar, sobre o modelo de referência e sobre um modelo de teste
com todos os tipos; `create_all` no DuckDB em memória com o DDL de cada modelo; o diff dos arquivos
`tests/model/schema/` versionados; `write_schema_files` sob a raiz local. Dependências: `sqlalchemy`, `pyarrow`,
`deltalake`, `duckdb`, `duckdb-engine` e `sqlalchemy-redshift`. Provas de conceito:
`test_sqlalchemy.py` (`test_declarative_model_exposes_table`, `test_ddl_per_dialect`,
`test_create_all_and_reflection`, `test_arrow_and_delta_schema_from_table`, com o mapa de tipos e
`Schema.from_arrow().to_json()`, `test_sandbox_copy_of_table_and_schema_files_diff`),
`test_pyarrow.py` (`test_schema_metadata_and_from_pylist`, `test_safe_cast_refuses_data_loss`, com as
perdas que o cast seguro não acusa, e `test_arrow_table_round_trips_through_pandas_without_copy`) e
`test_stdlib.py` (`test_generated_files_diff`, `test_decimal_totals`).
