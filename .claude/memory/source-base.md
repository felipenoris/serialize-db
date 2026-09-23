# The Parquet source base and its fixture

Read before stage 7 (`serialize_db.load`), `tests/source_db_projetado.py`, `tests/reference_model/` or `probes/parquet_source.py`; the user's decisions on the load are in `decisions.md`. Each fact ends with the `plan/` file that details it, and `tests/proof_of_concept/` holds the API details as assertions. A fact found in a session is appended here, under the heading it belongs to.

## The development base, read on 2026-09-20

- The source base (dev, 2026-09-20, `/mnt/bndes_grupos_bases_analise_financeira/databases/dsv/db_projetado`
  in the space): 14 tables, 205 files, 3,757,237,689 bytes, 187,340,644 rows, written by pandas
  through parquet-cpp-arrow with format 1.0, no dictionary, `INT96` timestamps and the `pandas`
  footer key in every file; Hive partitions `data_str=<YYYY-MM-DD>` (`data_base_str` for
  `cad_lancamentos`) whose value is a month end, lives only in the path and equals `data` or
  `data_base` in every row; up to 36 `chunk_<n>.parquet` files of 1,000,000 rows and one row group
  per partition, not zero-padded. PyArrow reads `INT96` as `timestamp[ns]` without min/max;
  `coerce_int96_timestamp_unit="us"` and DuckDB's `TIMESTAMP` truncate silently and the safe cast
  refuses a non-zero sub-microsecond part; DuckDB `hive_partitioning=true` casts `data_str` to
  `DATE` unless `hive_types_autocast=false`, and a PyArrow dataset keeps it `string`. The 12 model
  tables match the files in columns, order, types and nullability except seven `cad_contratos`
  columns nullable in the files and `NOT NULL` in the model with no null in the data. The report was
  pasted in the conversation, never saved; its sections 2, 6, 7 and 9 survive in the transcript of
  session `1b1640bf` on this machine, and the numbers below come from there. `plan/POC.md`,
  `plan/PLAN-STAGE-7.md`, `tests/source_db_projetado.py`

## The production base, read on 2026-09-21

- The production base (`s3://bndes-aco-models-<conta>/dzd-<domínio>/<projeto>/shared/bndes_grupos_bases_analise_financeira/databases/prd/db_projetado`),
  read in the target through `S3FileSystem` at 13:54 UTC with `--sample 5000`, the listing in 0.1 s
  (the report is in `plan/readings/parquet_source-2026-09-21-1354.txt`, committed by the user): the
  same structure as the dev base, section 3 identical column by column (checked by script against
  the transcription in `tests/test_source_db_projetado.py`), the same partitions, layout, seven
  columns without statistics and `schema.json`; 14 tables, 205 files, 3,771,538,655 bytes,
  187,340,531 rows. The differences are data: 113 rows fewer (`cad_contas` 97 for 102 and `numero`
  up to `T.4` for `T.5`, `rel_contas_hierarquias` 89 for 93, `cad_lancamentos` 141,901,795 for
  141,901,899, the 104 missing rows with null `sistema` and `contrato`); smaller maximum ids
  (`id_contrato` 78,342,969 for 88,853,864, `id_operacao` 136,235,442 for 154,461,887,
  `id_lancamento` 952,517,158 for 1,113,599,996, `id_rel_contrato_operacao` 490,576,085 for
  556,941,030, the minimums equal); `cad_aliquotas.id` 1 to 26 for 2 to 16 with the same pairs and
  factors; `valor` extremes `±11846195394.62801` for `±11846195394.628`; `meta_update_status` ids up
  to 161 and the last load on 2026-09-14 (dev: 182 and 2026-09-03); and the `pandas` footer key in
  part of the files (`cad_contratos` 5/8, `cad_lancamentos` 111/144, `cad_operacoes` 9/13,
  `rel_contrato_operacao` 16/30, none in `alembic_version` and `meta_update_status`; the files
  without it are 3, 4 and 14 in the three `data_str` tables, the size of one partition each, and
  the report does not say which). The reference model matches it as it matches dev, no `NOT NULL`
  column of the model has a null, and the composite foreign-key orphans repeat (`data_base`
  2026-01-31 without `cad_contratos`, `desemb-999`). `plan/POC.md`, `plan/PLAN-STAGE-7.md`

## The fixture

On 2026-09-21 the user grouped the production `rel_contrato_operacao` by the contract key
`(sistema, contrato)` and read `sum(fator_rateio) = 1`: each contract is apportioned among its N
operations, not each operation among its contracts, and the client's query runs the same way, from
the contract to its operations. The fixture built it the other way and was corrected the same day
(`tests/source_db_projetado.py`, `tests/test_source_db_projetado.py`, the `fator_rateio` and table
comments of the client model, the regenerated `rel_contrato_operacao.delta.json`). The structure
did not change: still N×N, the pair `(data, operacao, sistema, contrato)` still unique, the same
row count per partition. It does not rescue the foreign key `cad_contratos` declares to
`rel_contrato_operacao`: with N operations per contract, `(data, sistema, contrato)` is still not
unique at the target. `plan/POC.md`

The fictitious Parquet source base `db_projetado`, reproducing the structure common to the two readings (section 3, the partitions, the `chunk_<n>` files, the `INT96` timestamps, the layout): the 14 tables with the read columns, types and nullability (12 match the reference model; `alembic_version` and `meta_update_status` are outside it), the Hive partitions, and the previous library's real `schema.json` at the root (`source_db_projetado_schema.json`). The values that differ between the bases follow the dev reading (`valor` with three decimals, `fator` with five, `id_lancamento` up to 1,113,599,996, the `meta_update_status` ids). The `pandas` footer key follows the production base: `written_by_pandas` leaves the last partition of each partitioned table and the two control tables without it, so every partitioned table has files of both kinds; the probe run on the fixture on 2026-09-21 printed section 3 identical to the transcription and the footer table with the mix. The data is consistent with the reference model (unique keys, every foreign key satisfied, the four dates in every partitioned table, `rel_contas_hierarquias` a tree of accounting accounts, one root and five levels, with no account its own parent (the user stated on 2026-09-23 that in the real base `id_parent` and `id_child` always differ and that the table implements a tree of accounting accounts), the N×N `rel_contrato_operacao` with `fator_rateio` 1 or 1/2, dyadic, summing to 1 per contract, since a contract is in one or two operations; the builders make one dict per row since the review of 2026-09-23, with the written base byte-identical). `write_source(root)` returns the files and row counts; `tests/test_source_db_projetado.py` checks the written files against the transcribed section 3 of the report, the model's keys and the schema control, and `tests/test_reference_model.py` reads the reference model through SQLAlchemy (with `tests/lib_base_contabil.py` and `tests/lib_base_gerencial.py` standing in for the pipeline's modules) and checks it against `SCHEMAS` and the transcribed keys. The material of the stage 7 test.

## The early migration in the target

- `scripts/migrate_parquet_to_delta.py` ran in the target over the copy of the production base in
  the sandbox (`databases/prd/db_projetado`, the `--source` of `SUITE.md`), one process and one
  `--report` JSON per table, in a version from d2c545b (2026-09-21) to 8de3c8b
  (2026-09-22), before the issue #59 rule of e2ed614: the user reported the success in the session
  of cea8a51 (2026-09-22) and handed the reports over on 2026-09-23. The JSON records neither the
  mode, the sort, the roots nor the machine; the script prints the DuckDB `threads` and
  `memory_limit` outside it. `decisions.md`
- Every model table loaded every partition, with counts and sums equal between the source and the
  Delta: 187,340,509 rows, the production reading's 187,340,531 minus `alembic_version` (1) and
  `meta_update_status` (21), which the reports list outside the model with `schema.json`.
  `cad_lancamentos` has 2026-01-31 (33,239,719 rows), 2026-02-28 (23,789,279), 2026-03-31
  (52,654,607) and 2026-06-30 (32,218,190); `cad_contratos` (6,543,408 rows), `cad_operacoes`
  (10,984,434) and `rel_contrato_operacao` (27,910,654) have the last three dates. The conversions
  reported are the `int32` keys to `int64` and `INT96` to `timestamp[us]`. `decisions.md`
- The production copy has no `NaN` or infinity in any `Double` column: that version summed every
  `Double` (a `sa.Numeric` subclass) as `CAST(... AS DECIMAL(38, 6))` over the whole source and
  the whole Delta table, the cast raises `ConversionException` on `NaN` and ±infinity in DuckDB
  1.5.5 (probed again on 2026-09-23), and `main` caught only `ContractError`, so a non-finite value
  would have ended the process before its JSON. Any `Double` min and max it logged is correct, and
  the issue #59 item on the migrated tables needs no `isnan`/`isinf` count. `decisions.md`
- Each partition's time runs from the check query to the commit, beside the process's peak RSS so
  far: `cad_lancamentos` 5.3 s (11,548 MB), 4.5 s (11,548 MB), 7.1 s (19,595 MB) and 5.3 s
  (19,595 MB) in date order; `rel_contrato_operacao` up to 4.2 s and 4,587 MB, `cad_operacoes` up
  to 3.2 s and 3,130 MB, `cad_contratos` up to 3.0 s and 2,000 MB, the unpartitioned tables under
  0.6 s at about 258 MB; about 53 s of loading in all. A 19,595 MB peak exceeds the 7.6 GiB of the
  2026-09-21 reading and the 15.4 GiB of 2026-09-23 (`environments.md`), so the run used a larger
  instance, whose default DuckDB
  `memory_limit` is 80% of its RAM: the peaks do not say whether a partition fits in 7.6 GiB, and
  only the first partition of each process is an isolated peak. `decisions.md`
- The `export_mode` default and the load's sort still need `cad_lancamentos` in `rewrite` and with
  `--no-sort`: the user declined separate runs, and the script now measures, before each
  partitioned table's load, every requested partition in the four variants (`register` and
  `rewrite`, with and without the `sort_key` order), each in a new process with its own peak and a
  scratch table under `<root>/_medicao_<table>/`, also when the partition is already in the log
  (the script resumes from the log and would skip a loaded partition); the report carries the
  machine. The user reruns the `SUITE.md` commands, every table with the same parameters. Nothing
  else needs a rerun: since d2c545b the Delta schema changed
  only in the comments of `rel_contrato_operacao` (7261f0a), which `delta.reconcile` applies as
  additive, and the `register` versions before 8de3c8b (2026-09-22) logged min and max only for
  integers and dates, which costs pruning on `Double` and text columns, not correctness.
  `decisions.md`
