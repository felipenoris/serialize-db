
# User Instructions

Do not edit this section. You're free to edit all other sections of this file.

These are the instructions written by the user.

## Project Goals

- A core feature is the ability to import and export the database in Parquet format,
ideally incrementally.

- Use SQLAlchemy ORM for modeling the database schema.

- Support schema migration. Alembic is an option.

- Parquet is the source of truth.

- Pipeline execution engine will be: DuckDB or Redshift. Must support both.

- Data will be published on Redshift for clients.

- Project has access to a single Redshift schema in the current state. We could use a prefix on table names to implement a namespace, to separate production/development/pipeline execution.

- Parquet/DuckDB/Redshift are column-based, with small focus on table constraints:

    - Parquet: isolates data for a single table, with no contepts of primary key, foreign key, autoincrement columns. 

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

When you need to edit files in this repo, do it in a new branch prefixed with `claude/` and open a PR. While a PR you opened is still open, every new commit goes to that PR's branch; do not open another branch or PR. Submit your commits incrementally. The user will merge when needed. After merge, sync the local repo copy to the `main` branch (sometimes the user will do that for you).

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

# Claude Memory

Use this section to store you memory for this project. Use a "size budget" of 50KB for this file.
