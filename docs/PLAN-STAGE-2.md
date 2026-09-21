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
valor e parâmetro faltante como erros; o diff de `tests/client_model/sql/`. Opcional: `sqlglot.parse_one(texto,
dialect)` como teste de que o texto do Redshift analisa. Provas de conceito:
`test_sqlalchemy.py` (`test_generated_sql_text_per_dialect`, `test_redshift_dialect_compiles_dml`) e
`test_stdlib.py::test_generated_files_diff`.

## Interface

```python
"""Assinaturas de serialize_db.sql; os corpos estão no rascunho abaixo."""
from typing import Literal

import sqlalchemy as sa

Dialect = Literal["duckdb", "redshift"]


class SqlError(ValueError):
    """Um statement que não vira texto executável, ou um texto cujos parâmetros não fecham com o dicionário."""


def param(name: str, type_: sa.types.TypeEngine | None = None) -> sa.ColumnElement: ...
def prefixed(statement: sa.sql.ClauseElement, metadata: sa.MetaData, prefix: str = "{prefix}") -> sa.sql.ClauseElement: ...
def render(statement: sa.sql.ClauseElement, dialect: Dialect, metadata: sa.MetaData, prefix: str = "{prefix}") -> str: ...
def bind(sql: str, params: dict[str, object], style: Dialect) -> tuple[str, dict[str, object]]: ...
def referenced_tables(statement_or_sql: sa.sql.ClauseElement | str) -> set[str]: ...
def sql_files(statements: dict[str, sa.sql.ClauseElement], metadata: sa.MetaData) -> dict[str, str]: ...
def write_sql_files(statements: dict[str, sa.sql.ClauseElement], metadata: sa.MetaData, directory: str) -> list[str]: ...
def check_sql_files(statements: dict[str, sa.sql.ClauseElement], metadata: sa.MetaData, directory: str) -> list[str]: ...
def read_sql(directory: str, name: str, dialect: Dialect) -> str: ...
```

`referenced_tables` entra aqui, e não na [etapa 6](PLAN-STAGE-6.md): as tabelas de um statement
Core saem de `find_tables(include_crud=True)`, as de um texto gerado saem do sentinela `{prefix}`,
e o log da execução as registra por comando. É o esboço de `test_parallel.py`, e a barreira por
tabela de [`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md) a usaria.

## Estratégia de implementação

- **`param`** valida o nome (`[a-z_][a-z0-9_]*`) e devolve `literal_column(":nome", type_)`; o
  tipo serve à compilação de comparações com colunas tipadas.
- **`prefixed`** copia cada `Table` do contrato por `to_metadata` com o nome
  `quoted_name(prefix + name, quote=False)` e troca tabelas e colunas por `replacement_traverse`.
  O statement original não muda.
- **`render`** compila com um dialeto avulso de `paramstyle="named"` e `literal_binds=True`, dentro
  de `warnings.catch_warnings()` com `SAWarning` como erro: o `bindparam` sem valor, que renderizaria
  `NULL`, vira `SqlError` com a instrução de usar `param`.
- **`bind`** reescreve `:nome` por uma expressão regular que consome primeiro os literais entre
  aspas simples (com `''` escapado) e só depois reconhece `:nome` fora de `::cast`; um `:` dentro de
  um literal fica intacto. O conjunto de nomes encontrados precisa ser igual ao do dicionário:
  faltante e sobrando são `SqlError`. No estilo `duckdb` o marcador vira `$nome`; no `redshift`
  fica `:nome`, que o `redshift_connector` lê com `cursor.paramstyle = "named"`.
- **`sql_files`** renderiza cada statement nos dois dialetos com `\n` final; `write_sql_files`,
  `check_sql_files` e `read_sql` repetem o padrão da [etapa 1](PLAN-STAGE-1.md).

## Pré-requisitos e pós-condições

| Primitiva | Pré-requisitos | Pós-condições |
| --- | --- | --- |
| `param` | Nome válido como identificador. | Um `literal_column` que atravessa `literal_binds` como `:nome`. |
| `prefixed` | Toda tabela do statement pertence ao `metadata` informado. | Uma cópia com cada tabela e coluna trocadas; o original intacto. |
| `render` | Statement sem `bindparam` sem valor. | Texto com as constantes embutidas, `%` dos literais simples e `{prefix}` sem aspas; o mesmo texto nos dois dialetos para o SQL portável. |
| `bind` | Texto com `:nome` fora de literais. | O texto no estilo do motor e o dicionário conferido; `SqlError` quando os nomes não fecham. |
| `write_sql_files` | Pasta gravável. | Dois arquivos por statement; `check_sql_files` vazio quando nada mudou. |

## Testes por caso

`tests/test_sql.py`, sem gravar, com o statement de [`sqlalchemy.md`](sqlalchemy.md) e os do
rascunho.

| Caso | Teste | O que confere |
| --- | --- | --- |
| Texto por dialeto | `test_render_embeds_constants_and_keeps_parameters` | Constantes embutidas, `:nome` preservado, `%` simples em `LIKE`, `{prefix}` sem aspas; DuckDB e Redshift iguais para o statement portável. |
| Execução | `test_rendered_text_runs_in_duckdb` | O texto com `prefix=""` passa por `bind` e roda num DuckDB em memória com `$nome`. |
| Literais com dois-pontos | `test_bind_leaves_quoted_literals_and_casts_alone` | `'TI:%'`, `'12:30'` e `valor::DECIMAL(18, 2)` intactos; só `:nome` do dicionário muda. |
| Parâmetros | `test_bind_refuses_missing_and_extra_parameters` | Faltante e sobrando são `SqlError` com os dois conjuntos na mensagem. |
| `bindparam` sem valor | `test_render_refuses_bindparam_without_value` | `SqlError` em vez de `NULL` silencioso. |
| Prefixo | `test_prefixed_replaces_every_contract_table` | Tabelas e colunas trocadas em `select`, `insert ... from_select` e `join`; o statement original intacto. |
| Tabelas referenciadas | `test_referenced_tables_from_core_and_text` | `find_tables` e o sentinela dão o mesmo conjunto para o mesmo comando. |
| Arquivos gerados | `test_sql_files_match_versioned`, `test_check_sql_files_reports_a_changed_statement` | Diff vazio contra `tests/client_model/sql/`; uma coluna nova no statement aparece no diff. |
| Redshift analisável | `test_redshift_text_parses_with_sqlglot` (opcional) | `sqlglot.parse_one(texto, dialect="redshift")` aceita o texto. |

## Rascunhos executados

Rodou em 2026-09-21 com as versões fixadas.

```python
"""Etapa 2: param, prefixed, render, bind e sql_files sobre um statement Core, executado no DuckDB com $nome."""
import re
import warnings

import duckdb
import duckdb_engine
import sqlalchemy as sa
from sqlalchemy.exc import SAWarning
from sqlalchemy.sql import quoted_name
from sqlalchemy.sql.visitors import replacement_traverse
from sqlalchemy_redshift.dialect import RedshiftDialect_redshift_connector

DIALECTS = {"duckdb": duckdb_engine.Dialect(paramstyle="named"), "redshift": RedshiftDialect_redshift_connector(paramstyle="named")}
PARAM_STYLES = {"duckdb": "$", "redshift": ":"}     # o redshift_connector com cursor.paramstyle = "named" lê :nome


class SqlError(ValueError):
    """Um statement que não vira texto executável, ou um texto com parâmetros que não fecham com o dicionário."""


def param(name: str, type_=None) -> sa.ColumnElement:
    """O parâmetro de execução: literal_column(':nome') atravessa literal_binds e chega ao texto como :nome."""
    if not re.fullmatch(r"[a-z_][a-z0-9_]*", name):
        raise SqlError(f"nome de parâmetro inválido: {name!r}")
    return sa.literal_column(f":{name}", type_=type_)


def prefixed(statement, metadata: sa.MetaData, prefix: str = "{prefix}"):
    """A cópia do statement com cada tabela do contrato trocada pela cópia com o prefixo, sem aspas."""
    copies = {t: t.to_metadata(sa.MetaData(), name=quoted_name(f"{prefix}{t.name}", quote=False)) for t in metadata.tables.values()}

    def replace(element):
        if isinstance(element, sa.Table):
            return copies.get(element)
        if isinstance(element, sa.Column) and element.table in copies:
            return copies[element.table].c[element.name]
        return None

    return replacement_traverse(statement, {}, replace)


def render(statement, dialect: str, metadata: sa.MetaData, prefix: str = "{prefix}") -> str:
    """O texto do dialeto com as constantes embutidas e os parâmetros como :nome; um bindparam sem valor é erro."""
    with warnings.catch_warnings():
        warnings.simplefilter("error", SAWarning)
        try:
            compiled = prefixed(statement, metadata, prefix).compile(dialect=DIALECTS[dialect], compile_kwargs={"literal_binds": True})
        except SAWarning as warning:
            raise SqlError(f"parâmetro sem valor no statement: {warning}; use param('nome')") from None
    return str(compiled)


PLACEHOLDER = re.compile(r"'(?:[^']|'')*'|(?<![:\w]):([a-z_][a-z0-9_]*)")   # literal entre aspas, ou :nome fora de ::cast


def bind(sql: str, params: dict, style: str) -> tuple[str, dict]:
    """O texto com :nome reescrito para o estilo do motor e o dicionário conferido; literais entre aspas ficam intactos."""
    found = set()

    def rewrite(match: re.Match) -> str:
        if match.group(1) is None:
            return match.group(0)               # um literal '...' passa como está
        found.add(match.group(1))
        return f"{PARAM_STYLES[style]}{match.group(1)}"

    text = PLACEHOLDER.sub(rewrite, sql)
    if found != set(params):
        raise SqlError(f"parâmetros do texto {sorted(found)} e do dicionário {sorted(params)} não fecham")
    return text, dict(params)


def sql_files(statements: dict, metadata: sa.MetaData) -> dict[str, str]:
    return {f"{name}.{dialect}.sql": render(statement, dialect, metadata) + "\n" for name, statement in statements.items() for dialect in DIALECTS}


metadata = sa.MetaData()
entries = sa.Table("cad_lancamentos", metadata, sa.Column("id_lancamento", sa.BigInteger), sa.Column("id_conta", sa.BigInteger),
                   sa.Column("valor", sa.Double), sa.Column("area", sa.String(50)), sa.Column("data_base_str", sa.String(10)))
accounts = sa.Table("cad_contas", metadata, sa.Column("id_conta", sa.BigInteger), sa.Column("numero", sa.String(30)))

statement = (
    sa.select(accounts.c.numero, sa.func.sum(entries.c.valor).label("total"))
    .join_from(entries, accounts, entries.c.id_conta == accounts.c.id_conta)
    .where(entries.c.data_base_str == param("data_base_str", sa.String(10)), entries.c.area.like("TI:%"), accounts.c.numero != "1:2")
    .group_by(accounts.c.numero).order_by(accounts.c.numero)
)
texts = {dialect: render(statement, dialect, metadata) for dialect in DIALECTS}
print(texts["duckdb"])
print("redshift igual ao duckdb:", texts["redshift"] == texts["duckdb"])

sql, values = bind(render(statement, "duckdb", metadata, prefix=""), {"data_base_str": "2026-08-31"}, "duckdb")
print(sql.splitlines()[-1])                    # 'TI:%' e '1:2' ficaram intactos; :data_base_str virou $data_base_str
con = duckdb.connect()
con.execute("CREATE TABLE cad_lancamentos (id_lancamento BIGINT, id_conta BIGINT, valor DOUBLE, area VARCHAR, data_base_str VARCHAR)")
con.execute("CREATE TABLE cad_contas (id_conta BIGINT, numero VARCHAR)")
con.execute("INSERT INTO cad_lancamentos VALUES (1, 7, 150, 'TI:infra', '2026-08-31'), (2, 7, 50, 'RH', '2026-08-31'), (3, 9, 200, 'TI:dados', '2026-07-31')")
con.execute("INSERT INTO cad_contas VALUES (7, '1.1'), (9, '1:2')")
print(con.execute(sql, values).fetchall())

for description, action in {
    "bindparam sem valor": lambda: render(sa.select(entries.c.id_conta).where(entries.c.area == sa.bindparam("area")), "duckdb", metadata),
    "parâmetro faltante": lambda: bind("SELECT 1 WHERE x = :a AND y = :b", {"a": 1}, "duckdb"),
    "parâmetro sobrando": lambda: bind("SELECT 1 WHERE x = :a", {"a": 1, "b": 2}, "redshift"),
}.items():
    try:
        action()
    except SqlError as error:
        print(f"{description}: {str(error)[:100]}")

print(sorted(sql_files({"total_por_conta": statement}, metadata)))
print(bind("SELECT valor::DECIMAL(18, 2), '12:30' FROM t WHERE k = :k", {"k": 1}, "redshift")[0])   # ::cast e '12:30' preservados
```

Saída:

```
SELECT {prefix}cad_contas.numero, sum({prefix}cad_lancamentos.valor) AS total
FROM {prefix}cad_lancamentos JOIN {prefix}cad_contas ON {prefix}cad_lancamentos.id_conta = {prefix}cad_contas.id_conta
WHERE {prefix}cad_lancamentos.data_base_str = :data_base_str AND {prefix}cad_lancamentos.area LIKE 'TI:%' AND {prefix}cad_contas.numero != '1:2' GROUP BY {prefix}cad_contas.numero ORDER BY {prefix}cad_contas.numero
redshift igual ao duckdb: True
WHERE cad_lancamentos.data_base_str = $data_base_str AND cad_lancamentos.area LIKE 'TI:%' AND cad_contas.numero != '1:2' GROUP BY cad_contas.numero ORDER BY cad_contas.numero
[('1.1', 150.0)]
bindparam sem valor: parâmetro sem valor no statement: Bound parameter 'area' rendering literal NULL in a SQL expression;
parâmetro faltante: parâmetros do texto ['a', 'b'] e do dicionário ['a'] não fecham
parâmetro sobrando: parâmetros do texto ['a'] e do dicionário ['a', 'b'] não fecham
['total_por_conta.duckdb.sql', 'total_por_conta.redshift.sql']
SELECT valor::DECIMAL(18, 2), '12:30' FROM t WHERE k = :k
```

## Decisões pendentes

- **[decisão] Aspas duplas nos identificadores do texto gerado.** `bind` preserva só literais entre
  aspas simples; um identificador entre aspas duplas que contenha `:nome` seria reescrito. O texto
  gerado tem identificadores entre aspas: o DDL da [etapa 1](PLAN-STAGE-1.md) cita todos, e os dois
  dialetos citam `"to"` e o do Redshift `"timestamp"`, as duas colunas reservadas do modelo
  cliente (`cad_contratos."to"` num `select` compilado em 2026-09-21). O contrato não tem
  identificador com `:`, e a alternativa é estender a expressão a `"..."`.
- **[decisão] O `sqlglot` no grupo `dev`** para o teste opcional que analisa o texto do Redshift; ele
  só entra depois de um ensaio em venv avulsa, pela regra de dependências.
