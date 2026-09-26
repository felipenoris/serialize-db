# The Parquet source base and its fixture

Read before stage 7 (`serialize_db.load`), `tests/source_db_projetado.py`, `tests/reference_model/` or `probes/parquet_source.py`; the user's decisions on the load are in `decisions.md`. Each fact ends with the `plan/` file that details it, and `tests/proof_of_concept/` holds the API details as assertions. A fact found in a session is appended here, under the heading it belongs to.

## The development base, read on 2026-09-20

- The source base (dsv, 2026-09-20, `/mnt/bndes_grupos_bases_analise_financeira/databases/dsv/db_projetado`
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
  same structure as the dsv base, section 3 identical column by column (checked by script against
  the transcription in `tests/test_source_db_projetado.py`), the same partitions, layout, seven
  columns without statistics and `schema.json`; 14 tables, 205 files, 3,771,538,655 bytes,
  187,340,531 rows. The differences are data: 113 rows fewer (`cad_contas` 97 for 102 and `numero`
  up to `T.4` for `T.5`, `rel_contas_hierarquias` 89 for 93, `cad_lancamentos` 141,901,795 for
  141,901,899, the 104 missing rows with null `sistema` and `contrato`); smaller maximum ids
  (`id_contrato` 78,342,969 for 88,853,864, `id_operacao` 136,235,442 for 154,461,887,
  `id_lancamento` 952,517,158 for 1,113,599,996, `id_rel_contrato_operacao` 490,576,085 for
  556,941,030, the minimums equal); `cad_aliquotas.id` 1 to 26 for 2 to 16 with the same pairs and
  factors; `valor` extremes `±11846195394.62801` for `±11846195394.628`; `meta_update_status` ids up
  to 161 and the last load on 2026-09-14 (dsv: 182 and 2026-09-03); and the `pandas` footer key in
  part of the files (`cad_contratos` 5/8, `cad_lancamentos` 111/144, `cad_operacoes` 9/13,
  `rel_contrato_operacao` 16/30, none in `alembic_version` and `meta_update_status`; the files
  without it are 3, 4 and 14 in the three `data_str` tables, the size of one partition each, and
  the report does not say which). The reference model matches it as it matches dsv, no `NOT NULL`
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

The fictitious Parquet source base `db_projetado`, reproducing the structure common to the two readings (section 3, the partitions, the `chunk_<n>` files, the `INT96` timestamps, the layout): the 14 tables with the read columns, types and nullability (12 match the reference model; `alembic_version` and `meta_update_status` are outside it), the Hive partitions, and the previous library's real `schema.json` at the root (`source_db_projetado_schema.json`). The values that differ between the bases follow the dsv reading (`valor` with three decimals, `fator` with five, `id_lancamento` up to 1,113,599,996, the `meta_update_status` ids). The `pandas` footer key follows the production base: `written_by_pandas` leaves the last partition of each partitioned table and the two control tables without it, so every partitioned table has files of both kinds; the probe run on the fixture on 2026-09-21 printed section 3 identical to the transcription and the footer table with the mix. The data is consistent with the reference model (unique keys, every foreign key satisfied, the four dates in every partitioned table, `rel_contas_hierarquias` a tree of accounting accounts, one root and five levels, with no account its own parent (the user stated on 2026-09-23 that in the real base `id_parent` and `id_child` always differ and that the table implements a tree of accounting accounts), the N×N `rel_contrato_operacao` with `fator_rateio` 1 or 1/2, dyadic, summing to 1 per contract, since a contract is in one or two operations; the builders make one dict per row since the review of 2026-09-23, with the written base byte-identical). `write_source(root)` returns the files and row counts; `tests/test_source_db_projetado.py` checks the written files against the transcribed section 3 of the report, the model's keys and the schema control, and `tests/test_reference_model.py` reads the reference model through SQLAlchemy (with `tests/lib_base_contabil.py` and `tests/lib_base_gerencial.py` standing in for the pipeline's modules) and checks it against `SCHEMAS` and the transcribed keys. The material of the stage 7 test.

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
  `--no-sort`: the user declined separate runs, and the script measured, before each
  partitioned table's load, every requested partition in the four variants (`register` and
  `rewrite`, with and without the `sort_key` order), each in a new process with its own peak and a
  scratch table under `<root>/_medicao_<table>/`, also when the partition was already in the log
  (the script resumes from the log and would skip a loaded partition); the report carried the
  machine. The user reran the `SUITE.md` commands, every table with the same parameters, on
  2026-09-23 and 2026-09-24, and the measurement left the script on 2026-09-24 (user decision,
  `decisions.md`). Nothing
  else needs a rerun: since d2c545b the Delta schema changed
  only in the comments of `rel_contrato_operacao` (7261f0a), which `delta.reconcile` applies as
  additive, and the `register` versions before 8de3c8b (2026-09-22) logged min and max only for
  integers and dates, which costs pruning on `Double` and text columns, not correctness.
  `decisions.md`
- The second migration (2026-09-23, 23:05 to 23:19 UTC, the script from `main` with #67, a new root
  under the personal folder, `register`, sorted, measured, a 4 vCPU and 15,786 MB machine with
  DuckDB `memory_limit` 12.3 GiB): eleven tables loaded every partition with counts, sums and
  non-finite counts equal to the source and no non-finite `Double`; `cad_lancamentos` left no
  report and its table ended at version 2 with 2026-01-31 and 2026-02-28, the process ending in
  2026-03-31 (52,654,607 rows) for a cause the files do not show. The measured partitions:
  `rewrite` took 1.14 to 1.52 times the `register` time sorted and 1.14 to 1.46 unsorted, the sort
  1.23 to 1.63 times the `register` time with files at 74% to 92% of the size; `rel_contrato_operacao`
  2026-03-31 (13,637,568 rows) took 13.1 s and 2,442 MB sorted in `register`. The raw reports stay
  out of git; the numbers are in `plan/POC.md`. `decisions.md`
- The `cad_lancamentos` load of the second migration died for lack of memory: the user did not keep
  the terminal output but saw `Killed` several times in that part (2026-09-24), the kernel's OOM
  killer, under DuckDB's default `memory_limit` of 12.3 GiB on the 15,786 MB machine. The script
  now opens every DuckDB connection with half the memory still available and gives each table its
  own connection; the rerun of `cad_lancamentos` confirms the partition fits. `plan/POC.md`,
  `plan/PLAN-STAGE-7.md`
- The second migration ran one process per table in the order of `SUITE.md` (alphabetical):
  `cad_lancamentos` ran alone between `cad_contratos`, whose report was written at 23:06 UTC, and
  `cad_operacoes`, which started at 23:14:58, and the threads probe came after, at 23:21: by the
  sequence of `SUITE.md`, the kernel's kill owes nothing to the probe. The loads in the main
  process, one connection across
  the whole table: `rel_contrato_operacao` 2.2 s, 13.3 s and 11.3 s with the process peak at 508,
  2,459 and 2,712 MB, growing on the third partition although it is smaller than the second;
  `cad_operacoes` 4.3 s to 5.1 s and up to 1,801 MB; `cad_contratos` 3.2 s to 3.6 s and up to
  1,142 MB; the unpartitioned tables 0.4 s to 0.6 s at about 270 MB. `plan/POC.md`

- The third migration (2026-09-24, 01:53 to 02:02 UTC, `main` with #69, one process per table on
  a 16 vCPU and 31,159 MB machine, `memory_limit` 13.1 to 13.6 GiB and 16 threads, `register`
  sorted with the four-variant measurement): every table matched, `cad_lancamentos` included, with
  no non-finite `Double`; its loads took 6.4 s, 5.4 s, 10.1 s and 6.4 s for 2026-01-31, 2026-02-28,
  2026-03-31 and 2026-06-30, the process peak 9,678 MB after the first two and 16,430 MB after
  2026-03-31 (20% above the 13.4 GiB limit, 53% of the machine). Its measured partitions:
  `register` sorted 7.0, 5.4, 10.6 and 6.6 s with peaks of 10,190, 7,413, 15,126 and 9,782 MB;
  unsorted 8.8, 6.7, 12.5 and 8.9 s with 8,650, 6,539, 7,540 and 8,214 MB; `rewrite` 1.4 to 2.1
  times the `register` time; sorted and unsorted files the same size (903,7 MB
  against 905,7 MB for 2026-03-31), unlike the other three partitioned tables, where
  the sort still costs 1.1 to 1.4 times and shrinks the files to 74% to 92%. With 16 threads
  `rel_contrato_operacao` 2026-03-31 loaded in 4.7 s with a 3,251 MB peak (13.3 s and 2,459 MB with
  4 vCPUs). The raw reports stay out of git. `plan/POC.md`, `plan/PLAN-STAGE-7.md`

- The load through the package (2026-09-24, 14:16 to 14:19 UTC, `scripts/migrate_parquet_to_delta.py
  --environment prod` from `main` with #72, one process, an engine per partition with 16 threads
  and a 14,030 MiB `memory_limit` on a 16 vCPU and 31,383 MB machine with 28,061 MB available):
  every table matched in counts and sums, into `<root>/prod/<table>`; `cad_lancamentos` 22.7 s,
  16.8 s, 36.9 s and 22.7 s with the process peak at 11,419 MB after the first two partitions and
  16,198 MB after 2026-03-31 (against 10.1 s and 16,430 MB in the 01:53 run: the package checks
  the source partition before the `COPY` and re-reads the copy through both readers);
  `rel_contrato_operacao` up to 11.9 s and 3,691 MB, `cad_operacoes` up to 6.5 s and 2,376 MB,
  `cad_contratos` up to 5.5 s, the unpartitioned tables 2.1 s to 5.5 s each. `serialize-db audit
  --table cad_lancamentos --partitions 2026-01-31 --foreign-keys` approved the row and key checks
  and five foreign keys and failed `orfao_data_base_sistema_contrato` with 989,852 distinct keys,
  the known absence of a 2026-01-31 partition in `cad_contratos`; `total_valor`
  117,667,407,519.194421, no non-finite `Double`. `history`, `snapshot carga-2026-09-24` (12
  tables) and `vacuum` (0 files) ran; `archive` died in the copy of the 2026-06-30 file
  (`aws-s3.md`). The raw report stays out of git. `plan/POC.md`
- The load through the package again (2026-09-24, `started_at` 16:51:12 UTC, from `main` with
  #73, the same machine and limits, `memory_limit` 14,036 MiB, the root loaded anew): every table
  matched, 187,340,509 rows in 21 files; `cad_lancamentos` 19.9 s, 15.3 s, 31.8 s and 19.4 s (9%
  to 15% less than at 14:16, nothing separates the cause) with the peak at 10,766 MB after the
  first two partitions and 16,355 MB after 2026-03-31 (17% above the limit, 52% of the machine);
  `rel_contrato_operacao` up to 11.0 s and 3,780 MB, `cad_operacoes` up to 6.5 s and 2,420 MB,
  `cad_contratos` up to 5.1 s, the unpartitioned tables 2.1 s to 2.7 s; the audit read the same
  as at 14:16. `history`, `snapshot`, `vacuum`, the whole `archive` (21 files, `aws-s3.md`) and
  the publication of the 12 tables (`redshift.md`) followed. The raw report stays out of git.
  `plan/POC.md`
- The load of the grown production base (2026-09-26, `started_at` 15:55:29 UTC, from `main` of
  2026-09-25 at 22:35 or later, on 8 vCPUs and 15,617 MB with 12,768 MB available, 8 threads and
  a 6,384 MiB `memory_limit`, the root of 2026-09-25 loaded anew): the source gained the month
  2026-07-31 in the four partitioned tables, `cad_lancamentos` 141,933,948 rows (the four earlier
  partitions hold 141,901,795), `rel_contrato_operacao` 15,209,141, `cad_operacoes` 5,579,536 and
  `cad_contratos` 3,985,447, and `cad_contas` went from 97 to 101 rows, `rel_contas_hierarquias`
  from 89 to 93 and `cad_aliquotas` from 15 to 22 (`fator` summing 3.454950 against 3.873450);
  the earlier partitions kept their counts and sums. Every table matched, 354,048,596 rows in 25
  partitions, 521.1 s summed; `cad_lancamentos` 2026-07-31 took 268.2 s (0.53 million rows per
  second against about 1 million for the others) with the process peak at 9,161 MB. The audit
  read the same 989,852 orphans of 2026-01-31; `snapshot carga-2026-09-24`, `vacuum` (0 files)
  and `archive` (25 files, `cad_lancamentos` 19.1 s at 348 MB) ran. The raw report stays out of
  git. `plan/POC.md`
