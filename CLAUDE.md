
# User Instructions

Do not edit this section. You're free to edit all other sections of this file.

These are the instructions written by the user.

- This repo is a Python library.

- Use `uv` to manage python version and dependencies.

- Use python version 3.13.

## Motivation

I have another project that implements an ETL pipeline based on a relational database.
That project builds on a base Python package that provides routines to import and
export data in Parquet format, and it uses SQLAlchemy's ORM for database schema
management.

The pipeline works as follows:

1 - A previous version of the database is persisted in Parquet format, together with
a JSON schema file, in a folder.

2 - The pipeline first rebuilds the relational database from scratch: it reads the
JSON schema, creates the empty tables, and then imports all Parquet files into the
database.

3 - The pipeline runs, producing a few new partitions in a few tables.

4 - The pipeline then decides which Parquet files should be exported incrementally.

The original idea was to use the model defined with SQLAlchemy ORM as the single
source of truth for the database schema. The project also uses Alembic for schema
versioning.

Although each table maps to a Python class, instances of these classes are never
created, for performance reasons: the model classes serve only to build the database
schema and to generate SELECT statements. All data input and output goes through
dataframes, both when querying the database and when inserting into it.

The major drawback is performance, and it comes from the relational database: as a
prerequisite to running the pipeline, the database must be rebuilt from scratch from
the Parquet files. The machine it runs on will eventually run out of volume space.
Because it relies on a relational database (PostgreSQL), importing the data from
Parquet into the database takes about 7 hours before the pipeline can even start —
even though the pipeline does not depend on all the data, only on a few partitions.

More recently, we experimented with DuckDB and Amazon Redshift: with both, ingesting
the whole database takes only a few minutes.

## Project Goals

The goal of this project is to implement a Python library to replace the one described
in the previous section.

What is settled:

- The new package will not use PostgreSQL as the relational database backing the ETL
pipeline.

- Available technologies: Redshift, DuckDB, Parquet.

- I would like to support both Redshift and DuckDB as replacements for PostgreSQL.

- A core feature is the ability to import and export the database in Parquet format,
ideally incrementally.

- The new package must support SQLAlchemy ORM for modeling the database schema, since
all the pipeline logic depends on it.

Still open:

- Whether the database schema should be generated from the SQLAlchemy ORM models, or
whether hand-written DDL statements would be a better solution for performance reasons:
these columnar database technologies usually do not rely on primary keys, foreign keys,
or constraints.

    - DuckDB: enforces constraints, but at a performance cost;
    - Redshift: constraints are informational only.

- Pipeline structure:

    - Option 1: Parquet files are the single source of truth (how it works today). The
    relational database is always rebuilt from scratch, and the pipeline exports new
    Parquet partitions.

    - Option 2: Parquet files are just backups. The new database (Redshift/DuckDB) is
    the single source of truth and periodically exports its state to Parquet files.

- In the previous version, new data was inserted into the relational database with a
single large `INSERT` statement. With the new technology this may change. I would also
like to explore what `pyarrow` has to offer, based on
<https://github.com/felipenoris/etl-cookbook-tutorial>.

## References

    - <https://duckdb.org/docs/lts/guides/performance/schema#constraints>
    - <https://docs.aws.amazon.com/redshift/latest/dg/t_Defining_constraints.html>
    - <https://github.com/felipenoris/etl-cookbook-tutorial>

# Guidelines

## Tools installed in the current environment

- `uv`.

- `cargo`

## Target Environment

- Linux ubuntu, amd64.

## Language convention (important)

**All prose in this repo is Brazilian Portuguese (pt-BR)**: README files,
code comments, docstrings, printed output, test messages, and shell-script
comments. When editing or adding content, **keep writing in pt-BR** to match.
Identifiers (variable/function names) are a mix of English and Portuguese —
follow the convention of the file you are editing. This `CLAUDE.md` is the one
intentional exception (English, for AI-assistant tooling).

## `REFERENCES.md`



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

# Claude Memory

Use this section to store you memory for this project. Use a "size budget" of 10KB for this file.
