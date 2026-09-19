
# User Instructions

Do not edit this section. You're free to edit all other sections of this file.

These are the instructions written by the user.

## Project Goals

The main goal is to build a library that takes as input a database stored as a set of Parquet files, ingests the necessary files into DuckDB or Redshift while enforcing a schema, runs a pipeline on this data, and exports the updated database back to Parquet files, ideally incrementally. Only the data required by the pipeline will be ingested, since the full database may not fit on the local machine (in RAM or on disk). DuckDB or Redshift is used as a sandbox to generate new data, and a tool to perform complex queries.

- The source of the truth of this database lives in those parquet files.

- But the set of parquet files conform to a schema.

- A core feature is the ability to import and export the database in Parquet format,
ideally incrementally.

- Use SQLAlchemy with ORM for modeling the database schema using Python.

- Support schema migration. Alembic is an option.

- Pipeline execution engine will be: DuckDB or Redshift. Must support both.

- Data will be published on Redshift for clients.

- The project has access to a single Redshift schema. We could use a prefix on table names to implement a namespace, to separate production/development/pipeline execution.

- Parquet/DuckDB/Redshift are column-based, with small focus on table constraints:

  - Parquet: isolates data for a single table, with no contepts of primary key, foreign key, autoincrement columns. Parquet metadata must be 

  - DuckDB: enforces constraints, but at a performance cost;

  - Redshift: constraints are informational only.

## References

    - <https://duckdb.org/docs/lts/guides/performance/schema#constraints>
    - <https://docs.aws.amazon.com/redshift/latest/dg/t_Defining_constraints.html>
    - <https://github.com/felipenoris/etl-cookbook-tutorial>

# Guidelines

## Project

- This repo is a Python library.

- Use `uv` to manage python version and dependencies.

- Use python version 3.13.

- use `pytest` to write tests.

- use [`pdoc`](https://pdoc.dev/) to generate documentation as static HTML format.

## Tools installed in the current environment

- `uv`.

- `gh`

- `cargo`

- `git`

## git

When you need to commit in this repo, do it in a new branch prefixed with `claude/` and open a PR. While a PR you opened is still open, every new commit goes to that PR's branch; do not open another branch or PR. Submit your commits incrementally. The user will merge when needed. After merge, sync the local repo copy to the `main` branch (sometimes the user will do that for you).

## AWS (Target Environment)

- SageMaker Unified Studio (Linux ubuntu, amd64)

- S3 (project bucket)

- Redshift: read/write permissions to a single schema

## Language convention (important)

**All prose in this repo is Brazilian Portuguese (pt-BR)**: README files,
code comments, docstrings, printed output, test messages, and shell-script
comments. When editing or adding content, **keep writing in pt-BR** to match.
Identifiers (variable/function names) are in English.

This `CLAUDE.md` is in English, for AI-assistant tooling.

## `REFERENCES.md`

Organize in this file all the websites you access to search for information.

## Chat

Chat with user in Portuguese (Brazil). Mark your answers with tags [guess], [uncertain] when you are not confident.

Challenge the user with questions when you find that the user is proposing something bad or wrong or when you have a better idea.

Be direct and sucint.

## Writing style

Applies to every text in the repository: Markdown, the comments in `.tf`, `.py`, `.sh`, `.tftpl` and
the Makefile, and this file. The test for a sentence is whether the reader does something with it.
Facts, identifiers, measurements and their dates, commands, quoted output and a log entry's provenance
are never cut; the words around them are.

1. **A heading names its subject**, in the file's own vocabulary: "Account tree", "The identities the
   battery runs as". Not a label that needs the body to be understood ("The map"), and not a
   rhetorical tail ("and why each one is the one it is").
2. **No counts in headings or titles.** "The two identities", "Four rules for reading this picture",
   "ONE MATCHER, TWO INPUTS" go stale the day an item is added, and the number is not the information.
   Write "The identities", "Rules for reading this picture", "The DNS Firewall coverage matcher".
3. **Every sentence has a subject, headings included.** "True now, and expected to change" becomes
   "Readings a later stage changes".
4. **Lead with the subject**, then its status: "The WireGuard Elastic IP is not allocated here; it is
   transferred from Sandbox." Never a preamble that reveals the subject at its end ("WHAT IS NOT HERE,
   AND THE ABSENCE IS THE STEP AFTER THIS ONE: the WireGuard Elastic IP").
5. **No capitals for emphasis.** The plain sentence carries the same fact. Bold marks the one term a
   reader scans for, never a whole sentence.
6. **No revision artifacts in headings or prose.** Edit dates, "(revised 2026-08-17, by the user)",
   "row four, inverted", "REWRITTEN 2026-09-06", "the list grew one entry at step 3" belong to git
   history and the stage log. A date stays when it dates a measurement.
7. **A code comment states what the code does and the constraint it obeys.** It does not narrate how the
   file got here, argue that something is "expected rather than a finding", or cite a lesson in place of
   the fact. A comment the reader of the code does not need is deleted.
8. **Delete what carries no information**: rhetorical connectives ("and that is the point", "which is
   the whole reason", "said out loud"), a second phrasing of the same fact, and a justification a
   decision file already records. One short sentence beats a chain of clauses joined by dashes.
9. **Shorter is the goal; a fact removed is a defect.** When a cut would drop a measurement, an
   identifier or a verdict, keep the sentence.

## `prompts.md`

Never read the file `prompts.md`.

# Claude Memory

Use this section to store you memory for this project. Use a "size budget" of 50KB for this file.

## Repository index

Read the file listed here before researching its subject again. Each document names the pages it
came from, and `REFERENCES.md` collects every URL consulted so far, grouped by subject: Parquet
format, Redshift, DuckDB, PyArrow, SQLAlchemy, pandas, PyIceberg, Glue Data Catalog, Athena, Lake
Formation, SageMaker Unified Studio, S3 and Python packages. New research appends to the matching
group.

| File | Subject |
| --- | --- |
| `README.md` | Initialization with `uv init --python 3.13`. |
| `docs/guia.md` | ETL practices the pipeline follows: immutable monthly partitions with idempotent replacement, write-audit-publish, the schema contract, and the open question about committing metadata atomically on S3. |
| `docs/schema.md` | DDL generated from the ORM models, physical options carried in `Table.info`, constraint policy per backend, the type table that maps SQLAlchemy to Arrow, Iceberg, DuckDB and Redshift, and SQL portability between the two engines. |
| `docs/parquet.md` | Parquet file layout and every metadata structure (`FileMetaData`, schema, row group, `ColumnMetaData`, page index, Bloom filters, page headers, key-value pairs, size and geospatial statistics, encryption, summary files), inspection with DuckDB and with PyArrow, partitioning, query optimization by layer, and import and export in DuckDB and in Redshift. |
| `docs/duckdb.md` | DuckDB as the execution sandbox. |
| `docs/redshift.md` | Redshift as the publication database and the second execution engine. It opens with the diagnostic queries for a session. |
| `docs/sqlalchemy.md` | SQLAlchemy as the schema contract: metadata, reflection and customization, deferrable constraints, Core statements, the ORM for DDL and for DML, keys generated on the server, and what the Redshift dialect, the DuckDB dialect and Parquet files each support. |
| `src/serialize_db/model/` | Declarative ORM models of the accounting, management and projection tables. |

`docs/duckdb.md` and `docs/redshift.md` share a section order: data organization and the differences
from PostgreSQL, supported types with `DECIMAL` and JSON, DDL, `SELECT`/`INSERT`/`UPDATE`/`DELETE`,
ingestion, export to Parquet, performance recommendations, SQLAlchemy support, references.

## What the documents establish

The load-bearing facts, each detailed in the file named at the end of the line.

- Arrow is the ingestion path for both engines. Loading 300,000 rows into DuckDB from an Arrow table
  took 0.008 s, against 5.2 s for 50,000 rows through `executemany`. `docs/duckdb.md`
- A pandas column of `object` holding `Decimal` gets its DuckDB type from a sample of 1,000 values:
  the sample column became `DECIMAL(7, 2)`, which a larger value in a later batch would reject.
  Casting to Arrow with the contract schema first fixes the type. `docs/duckdb.md`
- `pandas.read_sql` turns `Decimal` into `float` unless `coerce_float=False`, and
  `dtype_backend="pyarrow"` returns `double` for `DECIMAL` and `string` for `DATE`. Only the Arrow
  path preserves `decimal128(18, 2)` and `date32`. `docs/duckdb.md`
- With `redshift_connector`, `executemany` makes one round trip per row, and the dialect does not
  rewrite it into a multi-row `VALUES`. Volume goes through Parquet on S3 and `COPY`; small batches
  go through `insert(Modelo).values(lista)`. `docs/redshift.md`
- The generic `Identity()` disappears from the Redshift DDL, and DuckDB rejects it outright. Business
  keys generated on the client avoid the problem on both. `docs/sqlalchemy.md`
- A `@compiles(CreateTable, "redshift")` hook that reads `Table.info` produced the same
  `DISTSTYLE KEY DISTKEY (...) SORTKEY (...)` as the `redshift_*` arguments. Without the dialect
  installed, `redshift_*` arguments are accepted with a `Can't validate argument` warning; with it
  installed, unknown ones raise `ArgumentError`. `docs/redshift.md`
- `duckdb_engine` reflects columns, types and comments, but not primary keys or indexes; variables
  in the Python scope are invisible to queries issued through the engine, so a DataFrame needs
  `register` on the raw connection. `docs/duckdb.md`
- The DuckDB Parquet writer marks every column `optional`, even `NOT NULL`, writes `DECIMAL(18, 2)`
  as `INT64` and writes no page index. PyArrow writes `required` and `FIXED_LEN_BYTE_ARRAY(8)`.
  `docs/parquet.md`
- `COPY ... FROM` in DuckDB is positional and casts convertible types in silence, so type
  enforcement belongs on the metadata, before the load. `docs/parquet.md`
- Constraints cost on load and do not help queries in either engine: a DuckDB load of 300,000 rows
  went from 0.008 s to 0.073 s with a composite primary key, and Redshift keys are informational.
  `docs/schema.md`, `docs/duckdb.md`

## The state of the code

`src/serialize_db/model/` holds three modules of declarative models: `model_base_contabil.py`
(`dom_veiculos`, `dom_hierarquias_contas`, `cad_contas`, `rel_contas_hierarquias`,
`cad_lancamentos`), `model_base_gerencial.py` (`dom_mensuracoes`, `dom_segmentos`, `dom_negocios`,
`cad_operacoes`, `rel_contrato_operacao`, `cad_contratos`, `cad_lancamentos`) and
`model_db_projetado.py` (`cad_contratos`, `cad_aliquotas`).

Two of them fail to import: `model_base_gerencial.py` runs `from lib_base_contabil import Base` and
`model_db_projetado.py` runs `from lib_base_gerencial import Base`, module names the repository does
not have. Only `model_base_contabil.py` declares `Base`. The models carry no dialect options and no
`info` dictionaries, which `docs/schema.md` proposes, and they use `Double` where the contract
expects `Numeric(18, 2)`. Their single-column integer primary keys keep the default `autoincrement`,
which duckdb_engine renders as `SERIAL` and DuckDB rejects with `Type with name SERIAL does not
exist!`; `autoincrement=False` avoids it. Foreign keys are declared `deferrable=True, initially='DEFERRED'`, which
both dialects render as `DEFERRABLE INITIALLY DEFERRED`: DuckDB rejects that DDL with
`Constraint not implemented!`, and the Redshift `CREATE TABLE` syntax has no such clause
(`docs/sqlalchemy.md`, section on deferrable constraints).

`REFERENCES.md` links `docs/plano-de-implementacao.md`, which is not in the repository.

## Environment of the measurements

The examples in `docs/parquet.md`, `docs/duckdb.md` and `docs/sqlalchemy.md` ran on 2026-09-18 with
Python 3.13, DuckDB 1.5.5, PyArrow 25.0.1, pandas 3.0.6, polars 1.44.2, SQLAlchemy 2.0.54,
duckdb_engine 0.17.0, sqlalchemy-redshift 1.0.0 and redshift_connector 2.1.16. The sample is 300,000
rows of `operacoes` with the columns `id_operacao`, `data_ref`, `id_cliente`, `valor` and
`descricao`. The Redshift statements were compiled only; nothing ran against a cluster.

## Questions the official documentation does not answer

The documents mark these as pending the proof of concept.

- Whether `COPY` from Parquet accepts a column list. `awswrangler` emits
  `COPY tabela (colunas) ... FORMAT AS PARQUET`, while the `COPY` reference describes the column list
  only for flat files.
- The correspondence between Parquet physical types and Redshift columns in `COPY`, in particular
  `TIMESTAMP` as `INT64` in microseconds and the physical type of `DECIMAL`.
- What `COPY` from Parquet does when a string exceeds the target `VARCHAR`: truncate or abort.
- Whether Redshift Spectrum maps plain Parquet columns by name or by position.
- The physical types `UNLOAD` writes for `TIMESTAMP` and `DECIMAL`, whether its columns are
  required, and whether it writes minimum and maximum statistics. The three affect `add_files`.

One gap is already understood and needs a decision rather than a test: `sqlalchemy-redshift` compiles
`Text` as `TEXT`, which Redshift stores as `VARCHAR(256)`, so the `VARCHAR(65535)` of the contract
needs `String(65535)` or a `@compiles(Text, "redshift")` rule.
