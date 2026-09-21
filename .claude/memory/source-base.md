# The Parquet source base and its fixture

Read before stage 7 (`serialize_db.load`), `tests/source_db_projetado.py` or `probes/parquet_source.py`; the user's decisions on the load are in `decisions.md`. Each fact ends with the `docs/` file that details it, and `tests/proof_of_concept/` holds the API details as assertions. A fact found in a session is appended here, under the heading it belongs to.

## The development base, read on 2026-09-20

- The source base (dev, 2026-09-20): 14 tables, 205 files, 3.76 GB, 187,340,644 rows, written by
  pandas through parquet-cpp-arrow with format 1.0, no dictionary, `INT96` timestamps and the
  `pandas` footer key; Hive partitions `data_str=<YYYY-MM-DD>` (`data_base_str` for
  `cad_lancamentos`) whose value is a month end, lives only in the path and equals `data` or
  `data_base` in every row; up to 36 `chunk_<n>.parquet` files of 1,000,000 rows and one row group
  per partition, not zero-padded. PyArrow reads `INT96` as `timestamp[ns]` without min/max;
  `coerce_int96_timestamp_unit="us"` and DuckDB's `TIMESTAMP` truncate silently and the safe cast
  refuses a non-zero sub-microsecond part; DuckDB `hive_partitioning=true` casts `data_str` to
  `DATE` unless `hive_types_autocast=false`, and a PyArrow dataset keeps it `string`. The 12 model
  tables match the files in columns, order, types and nullability except seven `cad_contratos`
  columns nullable in the files and `NOT NULL` in the model with no null in the data. `docs/POC.md`,
  `docs/PLAN-STAGE-7.md`, `tests/source_db_projetado.py`

## The fixture

The fictitious Parquet source base `db_projetado`, reproducing the structure `probes/parquet_source.py` read in the dev base on 2026-09-20, which the source-base fact below states in full: the 14 tables with the read columns, types and nullability (12 match the reference model; `alembic_version` and `meta_update_status` are outside it), the Hive partitions, the `chunk_<n>` files, the `INT96` timestamps, and the previous library's real `schema.json` at the root (`source_db_projetado_schema.json`). The values the load handles (`valor` with three decimals, `fator` with five, `id_lancamento` up to 1,113,599,996). The data is consistent with the reference model (unique keys, every foreign key satisfied, the four dates in every partitioned table, the N×N `rel_contrato_operacao` with dyadic `fator_rateio` summing to 1 per operation). `write_source(root)` returns the files and row counts; `tests/test_source_db_projetado.py` checks the written files against the transcribed section 3 of the report, the model's keys and the schema control. The material of the stage 7 test.
