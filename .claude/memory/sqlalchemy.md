# SQLAlchemy and the SQL layer

Read before `serialize_db.schema` and `serialize_db.sql` (stages 1 and 2), a DDL rule per dialect or generated SQL text. Each fact ends with the `plan/` file that details it, and `tests/proof_of_concept/` holds the API details as assertions. A fact found in a session is appended here, under the heading it belongs to.

## Dialects and compilation

- The generic `Identity()` disappears from the Redshift DDL, and DuckDB rejects it; business keys
  generated on the client avoid the problem on both. `plan/sqlalchemy.md`
- A `@compiles(CreateTable, "redshift")` hook reading `Table.info` produced the same
  `DISTSTYLE KEY DISTKEY (...) SORTKEY (...)` as the `redshift_*` arguments. Without the dialect
  installed, `redshift_*` arguments are accepted with a `Can't validate argument` warning; with it,
  unknown ones raise `ArgumentError`. `plan/redshift.md`
- `duckdb_engine` reflects columns, types and comments, not primary keys or indexes; Python-scope
  variables are invisible to queries through the engine, so a DataFrame needs `register` on the raw
  connection. `plan/duckdb.md`
- `compile()` needs the right dialect object: `duckdb_engine.Dialect()` renders `%(name)s`, the
  dialect of a created engine renders `$1` (`numeric_dollar`, set when the engine loads the DBAPI)
  and `RedshiftDialect_redshift_connector()` renders `%s` with `positiontup`. The Redshift
  `CREATE TABLE` compiler drops `CHECK`, but `AddConstraint` and `CREATE INDEX` still render, so
  `ddl_if(dialect="duckdb")` gates them; `create_mock_engine` dumps the whole `create_all` sequence
  with `checkfirst=False`. `plan/sqlalchemy.md`
- Both dialects declare `supports_native_decimal = False`: `Numeric` parameters go through
  `to_float` and results through a float-formatting `DecimalResultProcessor`. Through duckdb_engine,
  `Numeric(18, 2)` is exact up to 15 significant digits, 16 digits lose the last cent, 17 round to
  10^15 and 18 fail the `INSERT`; the Arrow path keeps 18 digits and `literal_binds` text keeps the
  decimal. `plan/sqlalchemy.md`
- `literal_column(":mes")` survives `literal_binds` in both dialects and is the execution parameter
  of generated SQL; `bindparam("mes")` without a value and `text("mes = :mes")` render `mes = NULL`
  with a `SAWarning` instead of failing. Stand-alone dialect instances (`pyformat`, `format`) double
  `%` in literals under `literal_binds` (`LIKE 'A%'` becomes `'A%%'`); `Dialect(paramstyle="named")`
  does not. A table name with `{` is quoted unless `quoted_name(quote=False)`;
  `replacement_traverse` swaps the contract tables of a finished statement for prefixed copies.
  DuckDB runs the text with `$name` and a dict; `redshift_connector` with
  `cursor.paramstyle = "named"`. The Redshift dialect derives from `PGDialect` and compiles
  `DISTINCT ON`, `ON CONFLICT DO NOTHING` and array subscripts without error. `plan/sqlalchemy.md`
- A SQLAlchemy schema with a dot is one quoted identifier (`"db.schema".tabela`);
  `MetaData(schema=quoted_name("db.schema", False))` renders the three-part name in DDL, `select`
  and `insert` (2026-09-20, Redshift dialect). `plan/sqlalchemy.md`,
  `tests/proof_of_concept/test_sqlalchemy.py`
- The `DEFERRABLE` and `SERIAL` behavior of each dialect is in `plan/sqlalchemy.md` and `plan/duckdb.md`.
- Two client-model columns are reserved words: `to` (`cad_contratos`) in DuckDB (`duckdb_keywords()`
  category `reserved`; `CREATE TABLE t (to VARCHAR(2))` is a parser error) and in Redshift, and
  `timestamp` (`cad_lancamentos`) in Redshift (`column_name` in DuckDB, accepted bare). Both
  SQLAlchemy dialects quote `"to"` on their own in DDL and DML, the Redshift one also `"timestamp"`,
  and `redshift_distkey="to"` renders `DISTKEY ("to") SORTKEY ("to", data)`; the library's own
  text generation quotes every identifier (2026-09-21). `plan/PLAN-STAGE-1.md`, `plan/POC.md`
- Stage 1 generates the DDL from the type table without the dialect packages (user decision of
  2026-09-21): `sql_type` spells `DECIMAL(p, s)`, `VARCHAR(n)` on both engines
  (DuckDB ignores the length), `VARCHAR` / `VARCHAR(65535)` for `Text`, `VARCHAR(36)` for `Uuid`,
  `JSON` / `SUPER`, `DOUBLE` / `DOUBLE PRECISION`, `TIMESTAMP` / `TIMESTAMPTZ`; DuckDB read the
  fully quoted `CREATE TABLE` back as `DECIMAL(18,2)`, `TIMESTAMP WITH TIME ZONE`, `VARCHAR` and
  `JSON`. The `with_variant(SUPER(), "redshift")` on a JSON column stays optional: `isinstance(kind,
  sa.JSON)` holds with or without it. `plan/PLAN-STAGE-1.md`, `plan/schema.md`
- A `bindparam` without value shows in the plain compilation as `compiled.binds[name]` with
  `required=True` (`value=None`); one with a value has `required=False`; constants and `in_` lists
  enter under anonymous names with `required=False`; a `literal_column(":nome")` never appears.
  With `literal_binds=True`, `binds` is empty, the text carries `= NULL` and a `SAWarning` fires.
  Stage 2 reads `binds` instead of catching the warning: `warnings.catch_warnings` swaps the
  process-wide filter and the `warnings` docs call it unsafe with threads below Python 3.14's
  `context_aware_warnings`; the project runs 3.13 (2026-09-21). `plan/PLAN-STAGE-2.md`, `plan/POC.md`
- For a portable `SELECT`, an `INSERT ... SELECT` and a `CAST`, `duckdb_engine.Dialect`,
  `RedshiftDialect_redshift_connector` and SQLAlchemy's own `postgresql.dialect`, all with
  `paramstyle="named"`, compile byte-identical text; the only difference measured is the quoting
  of `"timestamp"`, which only the Redshift dialect does. A table copy built with
  `quoted_name(name, quote=True)` on the prefixed table name and on every column makes the
  `postgresql` dialect quote every identifier, sentinel inside the quotes
  (`"{prefix}cad_contas"."numero"`), and the text runs in DuckDB over the quoted DDL of stage 1
  (2026-09-21). The user kept the two third-party dialects as `render`'s compilers the same day, so
  they become runtime dependencies with stage 2. `plan/PLAN-STAGE-2.md`, `plan/POC.md`

## SQL tooling

- SQLGlot transpiles function names and syntax between DuckDB and Redshift but passes through
  constructs the target lacks (`INSERT ... BY NAME`, `list_aggregate`) and turned DuckDB
  `VARCHAR(200)` into Redshift `VARCHAR(MAX)`; Redshift integration tests remain necessary.
  `plan/estrategia.md`
