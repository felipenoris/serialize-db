# Etapa 2: `sql`

A entrega e o critério de aceite desta etapa estão na tabela de etapas de [`PLAN.md`](PLAN.md), que
também fixa as decisões, as regras que toda etapa obedece e a ordem do trabalho.

O módulo `serialize_db.sql` gera o texto SQL de cada dialeto a partir de um statement Core, para a
substituição gradual da compilação em tempo de execução descrita em `sqlalchemy.md`.

| Primitiva | O que faz |
| --- | --- |
| `param(name, type_=None)` | `literal_column(":nome", type_)`: o parâmetro de execução, que atravessa `literal_binds` e chega ao texto como `:nome`. |
| `prefixed(statement, metadata, prefix="{prefix}")` | A cópia do statement com cada tabela do contrato trocada pela cópia com o prefixo, por `replacement_traverse`; o nome sai sem aspas (`quoted_name(quote=False)`). |
| `render(statement, dialect, metadata, prefix="{prefix}")` | O texto de `duckdb` ou `redshift` com as constantes embutidas e os parâmetros como `:nome`, compilado por um dialeto com `paramstyle="named"`, que não dobra o `%` dos literais; um `bindparam` sem valor é erro, porque o compilador o renderia como `NULL`. |
| `bind(sql, params, style)` | O texto com `:nome` reescrito para o estilo do motor (`$nome` no DuckDB; inalterado no `redshift_connector` com `paramstyle = "named"`) e o dicionário conferido: parâmetro faltante ou sobrando é erro. |
| `sql_files(statements, metadata)` | `{"<nome>.duckdb.sql": ..., "<nome>.redshift.sql": ...}` de um dicionário `{nome: statement}`. |
| `write_sql_files(statements, metadata, directory)` | Grava `sql_files`; `serialize-db sql write` e `serialize-db sql check`. |
| `read_sql(directory, name, dialect)` | O texto versionado, para o `execute` dos motores. |

Testes: `tests/test_sql.py`, sem gravar: o statement de `sqlalchemy.md` (parâmetro, `%` em literal,
prefixo) renderizado nos dois dialetos e executado no DuckDB em memória com `$mes`; `bindparam` sem
valor e parâmetro faltante como erros; o diff de `tests/model/sql/`. Opcional: `sqlglot.parse_one(texto,
dialect)` como teste de que o texto do Redshift analisa. Provas de conceito:
`test_sqlalchemy.py` (`test_generated_sql_text_per_dialect`, `test_redshift_dialect_compiles_dml`) e
`test_stdlib.py::test_generated_files_diff`.
