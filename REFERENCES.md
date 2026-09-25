# Referências

Este arquivo lista os sites consultados na pesquisa do
plano de implementação e dos documentos sobre
[arquivos Parquet](plan/parquet.md), [DuckDB](plan/duckdb.md), [Redshift](plan/redshift.md),
[SQLAlchemy](plan/sqlalchemy.md), [Delta Lake](plan/delta.md) e a
[estratégia de implementação](plan/estrategia.md), agrupados por assunto.

## Projetos de referência

- <https://github.com/felipenoris/etl-cookbook-tutorial>

## Formato Parquet

Especificação, no repositório `apache/parquet-format`:

- <https://github.com/apache/parquet-format/blob/master/README.md>
- <https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift>
- <https://github.com/apache/parquet-format/blob/master/PageIndex.md>
- <https://github.com/apache/parquet-format/blob/master/BloomFilter.md>
- <https://github.com/apache/parquet-format/blob/master/LogicalTypes.md>
- <https://github.com/apache/parquet-format/blob/master/Encodings.md>
- <https://github.com/apache/parquet-format/blob/master/Compression.md>
- <https://github.com/apache/parquet-format/blob/master/Encryption.md>
- <https://github.com/apache/parquet-format/blob/master/Geospatial.md>
- <https://github.com/apache/parquet-format/blob/master/CHANGES.md>

O `NaN` nas estatísticas, consultado em 2026-09-23 (os tickets pela API REST do JIRA):

- <https://issues.apache.org/jira/browse/PARQUET-1246>
- <https://issues.apache.org/jira/browse/PARQUET-1222>
- <https://github.com/apache/parquet-format/commits/master/src/main/thrift/parquet.thrift>: o commit
  do PARQUET-2249, a ordem IEEE 754 e o `nan_count` (2026-05-26)

O CRC da página, o único checksum do formato, consultado em 2026-09-25 nos escritores que o pacote
usa:

- <https://github.com/apache/arrow-rs/blob/59.3.0/parquet/src/column/page.rs>: o `crc: None` fixo
  do cabeçalho da página, o escritor do delta-rs
- <https://github.com/duckdb/duckdb/blob/v1.5.5/extension/parquet/writer/primitive_column_writer.cpp>,
  `extension/parquet/column_reader.cpp` e `extension/parquet/parquet_writer.cpp` na mesma tag: o
  `COPY` do DuckDB sem `crc`

Site do projeto:

- <https://parquet.apache.org/docs/file-format/implementationstatus/>

Artigos:

- <https://arrow.apache.org/blog/2022/12/26/querying-parquet-with-millisecond-latency/>
- <https://www.influxdata.com/blog/using-parquets-bloom-filters/>
- <https://pydantic.dev/articles/bloom-filter-folding-parquet-logfire>
- <https://arxiv.org/abs/2304.05028>

## Amazon Redshift

Documentação:

- <https://docs.aws.amazon.com/redshift/latest/dg/t_Defining_constraints.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/c_best-practices-defining-constraints.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/c-optimizing-query-performance.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/c_loading-data-best-practices.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/copy-usage_notes-copy-from-columnar.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/t_updating-inserting-using-staging-tables-.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/c_best-practices-time-series-tables.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_UNLOAD.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_UNLOAD_command_examples.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/t_Unloading_tables.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_ALTER_TABLE.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_ALTER_TABLE_APPEND.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_Character_types.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_NEW.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_ALTER_USER.html>
- <https://docs.aws.amazon.com/redshift/latest/mgmt/serverless-usage-considerations.html>
- <https://docs.aws.amazon.com/redshift/latest/mgmt/serverless-billing.html>
- <https://repost.aws/knowledge-center/redshift-temporary-tables-memory> (respondeu 403 em 2026-09-22)
- <https://repost.aws/questions/QUIk7F9VwdRqmhQrjRfuVQ0w/redshift-removes-a-backslash-from-string-stored-in-a-varchar-field> (respondeu 403 em 2026-09-23)
- <https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_examples.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/performing-a-deep-copy.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_SCHEMA.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_DROP_SCHEMA.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_SVV_REDSHIFT_SCHEMA_QUOTA.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_names.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_DATABASE.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_ALTER_DATABASE.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/c_Concurrent_writes.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/c_serial_isolation.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/c_write_readwrite.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_Serializable_isolation_example.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/c_serial_isolation-serializable-isolation-troubleshooting.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_LOCK.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-data-format.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-data-load.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-column-mapping.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-data-source-s3.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-data-conversion.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_COPY_command_examples.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/c-using-spectrum.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_STL_LOAD_ERRORS.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/SYS_LOAD_ERROR_DETAIL.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/ingest-super.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/cm_chap_ConfigurationRef.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/querying-iceberg.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/querying-iceberg-supported-data-types.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/referencing-iceberg-tables.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/iceberg-integration-querying.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/iceberg-writes.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/iceberg-writes-sql-syntax.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/iceberg-writes-best-practices.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/iceberg-writes-transaction-semantics.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/iceberg-alter-table.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/iceberg-v3-features.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_EXTERNAL_SCHEMA.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/spectrum-lake-formation.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/c-spectrum-external-tables.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_EXTERNAL_TABLE.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_EXTERNAL_TABLE_usage.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_EXTERNAL_TABLE_examples.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_ALTER_TABLE_external-table.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/c-spectrum-troubleshooting.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/c-spectrum-data-files.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/c-spectrum-considerations.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/c-spectrum-external-performance.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/tutorial-query-nested-data.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/nested-data-use-cases.html>
- <https://docs.aws.amazon.com/redshift/latest/mgmt/query-editor-v2-glue.html>
- <https://docs.aws.amazon.com/redshift/latest/mgmt/amazon-redshift-limits.html>
- <https://docs.aws.amazon.com/redshift/latest/mgmt/data-api.html>
- <https://docs.aws.amazon.com/redshift/latest/mgmt/sagemaker-unified-studio.html>

Compartilhamento de dados (datashare), autorização e permissões:

- <https://docs.aws.amazon.com/redshift/latest/dg/datashare-overview.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/datashare-creation.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/datashare-considerations.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/considerations-datashare-general.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/considerations-datashare-reads-writes.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/considerations-datashare-datalake.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/lake-formation-considerations.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/getting-started-datashare-writes.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/connect-database-console-writes.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/multi-warehouse-writes-sql-statements.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/multi-warehouse-writes-sql-statements-unsupported.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/writes-producer-new.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/writes-consumer-new.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/writes-producer-existing.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/writes-consumer-existing.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/writes-creating-datashare.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/writes-adding-datashare.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/writes-authorizing.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/writes-associating.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/writes-creating-database.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/writes-granting.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/writes-querying.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/writes-managing-permissions.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/permissions-datashares.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/t_scoped-permissions.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/database-direct-connect.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/cross-database_usage.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/cross-database_limitation.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/cross-database_example.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/federated-permissions-considerations.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/glue-irc-federated-catalogs.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/querying-s3Tables.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/c-spectrum-external-schemas.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_USE_command.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_Privileges.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_GRANT.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_GRANT-usage-notes.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_GRANT-examples.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_TRUNCATE.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-authorization.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_DEFAULT_IAM_ROLE.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_SVV_USER_INFO.html>
- <https://docs.aws.amazon.com/cli/latest/reference/redshift-serverless/get-namespace.html>
- <https://docs.aws.amazon.com/cli/latest/reference/redshift-serverless/update-namespace.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_SHOW_DATASHARES.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_SHOW_DATABASES.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_SHOW_GRANTS.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_DESC_DATASHARE.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_SVV_DATASHARES.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_SVV_DATASHARE_OBJECTS.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_SVV_DATASHARE_CONSUMERS.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_SVV_DATASHARE_PRIVILEGES.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_SVV_REDSHIFT_DATABASES.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_SVV_ALL_SCHEMAS.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_SVV_DATABASE_PRIVILEGES.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_SVV_SCHEMA_PRIVILEGES.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_SVV_RELATION_PRIVILEGES.html>
- <https://docs.aws.amazon.com/redshift/latest/mgmt/query-editor-v2-connecting.html>
- <https://docs.aws.amazon.com/redshift/latest/mgmt/authorization-fas-spectrum.html>
- <https://docs.aws.amazon.com/redshift/latest/mgmt/default-iam-role.html>
- <https://docs.aws.amazon.com/redshift/latest/mgmt/copy-unload-iam-role.html>
- <https://docs.aws.amazon.com/redshift/latest/mgmt/data-api-secrets.html>
- <https://docs.aws.amazon.com/redshift/latest/mgmt/data-api-access.html>

Conectividade do ambiente alvo (credencial temporária do workgroup, ciclo da Data API, escrita num
banco de datashare), consultadas em 2026-09-20 a partir dos exemplos de `examples/`:

- <https://docs.aws.amazon.com/redshift-serverless/latest/APIReference/API_GetCredentials.html>
- <https://docs.aws.amazon.com/redshift/latest/mgmt/data-api.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/data-sharing-intro.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/considerations-datashare-reads-writes.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/multi-warehouse-writes-sql-statements.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/multi-warehouse-writes-sql-statements-unsupported.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_SVV_REDSHIFT_DATABASES.html>

- <https://docs.aws.amazon.com/redshift/latest/dg/c_columnar_storage_disk_mem_mgmnt.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/c_redshift-and-postgres-sql.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/c_redshift-sql-implementated-differently.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/c_unsupported-postgresql-features.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/c_unsupported-postgresql-datatypes.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/c_unsupported-postgresql-functions.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/c_Supported_data_types.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_Numeric_types201.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_Datetime_types.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_Boolean_type.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_SUPER_type.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/super-overview.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/query-super.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/limitations-super.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/json-functions.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/JSON_PARSE.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/JSON_SERIALIZE.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/JSON_EXTRACT_PATH_TEXT.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_AS.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_DROP_TABLE.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_COMMENT.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/t_Sorting_data.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/t_Distributing_data.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/c_choosing_dist_sort.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/c_best-practices-sort-key.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/c_best-practices-best-dist-key.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/c_Compression_encodings.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/c_Loading_tables_auto_compress.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_ANALYZE_COMPRESSION.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_SELECT_synopsis.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_INSERT_30.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_UPDATE.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_DELETE.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_MERGE.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/c_best-practices-single-copy-command.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/c_best-practices-use-multiple-files.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/c_best-practices-multi-row-inserts.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/c_best-practices-sort-key-order.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/t_Loading_tables_with_the_COPY_command.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/t_Loading_data.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/t_loading-tables-from-s3.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_VACUUM_command.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/t_Reclaiming_storage_space202.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_ANALYZE.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/t_Analyzing_tables.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/c-query-performance.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/c_designing-queries-best-practices.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/c_data_redistribution.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/c-query-planning.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_EXPLAIN.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/c-the-query-plan.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/t_Creating_tables.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/PG_LAST_COPY_COUNT.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/PG_LAST_UNLOAD_COUNT.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_QUOTE_LITERAL.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_SVV_TABLE_INFO.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_SVV_ALTER_TABLE_RECOMMENDATIONS.html>
- <https://docs.aws.amazon.com/redshift/latest/mgmt/python-redshift-driver.html>
- <https://docs.aws.amazon.com/redshift/latest/mgmt/python-connect-examples.html>
- <https://docs.aws.amazon.com/redshift/latest/mgmt/python-configuration-options.html>
- <https://docs.aws.amazon.com/redshift/latest/mgmt/python-api-reference.html>

Novidades, blog e base de conhecimento:

- <https://aws.amazon.com/about-aws/whats-new/2022/05/amazon-redshift-snapshot-isolation-level-support-concurrent-transactions/>
- <https://aws.amazon.com/about-aws/whats-new/2024/05/amazon-redshift-snapshot-isolation-provisioned-clusters/>
- <https://aws.amazon.com/about-aws/whats-new/2025/11/aws-redshift-iceberg-writes-m1>
- <https://aws.amazon.com/about-aws/whats-new/2026/04/redshift-update-delete-merge-iceberg-tables/>
- <https://aws.amazon.com/about-aws/whats-new/2026/05/amazon-redshift-alter-table-iceberg/>
- <https://aws.amazon.com/about-aws/whats-new/2026/08/amazon-redshift-supports-apache-iceberg-v3/>
- <https://aws.amazon.com/blogs/big-data/getting-started-with-apache-iceberg-write-support-in-amazon-redshift/>
- <https://aws.amazon.com/blogs/big-data/getting-started-with-apache-iceberg-write-support-in-amazon-redshift-part-1/>
- <https://aws.amazon.com/blogs/big-data/getting-started-with-apache-iceberg-write-support-in-amazon-redshift-part-2/>
- <https://aws.amazon.com/blogs/big-data/simplify-external-object-access-in-amazon-redshift-using-automatic-mounting-of-the-aws-glue-data-catalog/>
- <https://aws.amazon.com/blogs/big-data/best-practices-for-querying-apache-iceberg-data-with-amazon-redshift/>
- <https://aws.amazon.com/blogs/big-data/achieve-2x-faster-data-lake-query-performance-with-apache-iceberg-on-amazon-redshift/>
- <https://aws.amazon.com/blogs/big-data/10-best-practices-for-amazon-redshift-spectrum/>
- <https://aws.amazon.com/blogs/big-data/build-an-analytics-pipeline-that-is-resilient-to-schema-changes-using-amazon-redshift-spectrum/>
- <https://repost.aws/knowledge-center/redshift-spectrum-data-errors>
- <https://aws.amazon.com/about-aws/whats-new/2023/11/amazon-redshift-multi-data-warehouse-writes-data-sharing-preview/>
- <https://aws.amazon.com/about-aws/whats-new/2024/11/amazon-redshift-multi-data-warehouse-through-data-sharing/>
- <https://aws.amazon.com/blogs/big-data/improve-your-etl-performance-using-multiple-redshift-warehouses-for-writes/>
- <https://aws.amazon.com/blogs/big-data/develop-a-business-chargeback-model-within-your-organization-using-amazon-redshift-multi-warehouse-writes/>

Consultadas para a estratégia de implementação:

- <https://docs.aws.amazon.com/redshift/latest/dg/c-spectrum-external-tables.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_TRY_CAST.html>

Delta Lake no Redshift Spectrum (tabelas de manifesto simbólico), consultado em 2026-09-19:

- <https://docs.aws.amazon.com/redshift/latest/dg/c-spectrum-external-tables.html>
- <https://aws.amazon.com/blogs/big-data/introducing-native-delta-lake-table-support-with-aws-glue-crawlers>
- <https://aws.amazon.com/about-aws/whats-new/2022/12/aws-glue-crawlers-support-delta-lake-tables>

O comprimento em bytes de um `VARCHAR`, para a verificação de texto da auditoria, consultado em
2026-09-22:

- <https://docs.aws.amazon.com/redshift/latest/dg/r_OCTET_LENGTH.html>

A sintaxe do `COUNT`, sem a cláusula `FILTER` nos agregados, para os contadores da auditoria,
consultada em 2026-09-23 (a busca achou também a página sobre `FILTER` do Modern SQL):

- <https://docs.aws.amazon.com/redshift/latest/dg/r_COUNT.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/c_Aggregate_Functions.html>
- <https://modern-sql.com/feature/filter>

## DuckDB

Documentação:

- <https://duckdb.org/docs/lts/guides/performance/schema#constraints>
- <https://duckdb.org/docs/current/data/partitioning/partitioned_writes.html>
- <https://duckdb.org/docs/current/data/multiple_files/combining_schemas.html>
- <https://duckdb.org/docs/current/data/parquet/overview.html>
- <https://duckdb.org/docs/current/data/parquet/tips.html>
- <https://duckdb.org/docs/current/sql/indexes.html>
- <https://duckdb.org/docs/current/sql/statements/alter_table.html>
- <https://duckdb.org/docs/current/sql/statements/copy.html>
- <https://duckdb.org/docs/current/sql/data_types/text.html>
- <https://duckdb.org/docs/current/guides/python/import_arrow.html>
- <https://duckdb.org/docs/current/connect/concurrency.html>
- <https://duckdb.org/docs/current/core_extensions/aws.html>
- <https://duckdb.org/docs/current/core_extensions/httpfs/https.html>
- <https://duckdb.org/docs/current/core_extensions/iceberg/overview.html>
- <https://duckdb.org/docs/current/core_extensions/iceberg/catalogs.html>
- <https://duckdb.org/docs/current/core_extensions/iceberg/writing_to_iceberg.html>
- <https://duckdb.org/docs/current/core_extensions/iceberg/iceberg_options.html>
- <https://duckdb.org/docs/current/core_extensions/iceberg/iceberg_functions.html>
- <https://duckdb.org/docs/current/core_extensions/iceberg/troubleshooting.html>
- <https://ducklake.select/>
- <https://duckdb.org/docs/current/data/parquet/metadata.html>
- <https://duckdb.org/docs/current/data/parquet/encryption.html>
- <https://duckdb.org/docs/current/data/partitioning/hive_partitioning.html>
- <https://duckdb.org/docs/current/data/multiple_files/overview.html>
- <https://duckdb.org/docs/current/sql/statements/insert.html>
- <https://duckdb.org/docs/current/sql/statements/create_table.html>
- <https://duckdb.org/docs/lts/sql/statements/create_table>
- <https://duckdb.org/docs/current/guides/performance/file_formats.html>
- <https://duckdb.org/docs/current/guides/performance/indexing.html>
- <https://duckdb.org/docs/current/guides/performance/how_to_tune_workloads.html>
- <https://duckdb.org/docs/stable/guides/performance/how_to_tune_workloads> (redireciona para a anterior)
- <https://duckdb.org/docs/current/guides/performance/join_operations.html>
- <https://duckdb.org/docs/current/guides/file_formats/parquet_import.html>
- <https://duckdb.org/docs/current/guides/file_formats/parquet_export.html>
- <https://duckdb.org/docs/current/configuration/overview.html>

- <https://duckdb.org/docs/current/internals/overview.html>
- <https://duckdb.org/docs/current/internals/storage.html>
- <https://duckdb.org/docs/current/internals/vector.html>
- <https://duckdb.org/docs/current/sql/data_types/overview.html>
- <https://duckdb.org/docs/current/sql/data_types/numeric.html>
- <https://duckdb.org/docs/current/sql/data_types/timestamp.html>
- <https://duckdb.org/docs/current/sql/data_types/typecasting.html>
- <https://duckdb.org/docs/current/sql/data_types/nulls.html>
- <https://duckdb.org/docs/current/data/json/overview.html>
- <https://duckdb.org/docs/current/data/json/json_type.html>
- <https://duckdb.org/docs/current/data/json/json_functions.html>
- <https://duckdb.org/docs/current/data/json/loading_json.html>
- <https://duckdb.org/docs/current/data/json/creating_json.html>
- <https://duckdb.org/docs/current/data/json/caveats.html>
- <https://duckdb.org/docs/current/data/json/sql_to_and_from_json.html>
- <https://duckdb.org/docs/current/sql/constraints.html>
- <https://duckdb.org/docs/current/sql/statements/drop.html>
- <https://duckdb.org/docs/current/sql/statements/comment_on.html>
- <https://duckdb.org/docs/current/sql/statements/create_index.html>
- <https://duckdb.org/docs/current/sql/statements/create_sequence.html>
- <https://duckdb.org/docs/current/sql/statements/select.html>
- <https://duckdb.org/docs/current/sql/statements/update.html>
- <https://duckdb.org/docs/current/sql/statements/delete.html>
- <https://duckdb.org/docs/current/sql/statements/merge_into.html>
- <https://duckdb.org/docs/current/sql/statements/transactions.html>
- <https://duckdb.org/docs/current/sql/statements/checkpoint.html>
- <https://duckdb.org/docs/current/sql/statements/vacuum.html>
- <https://duckdb.org/docs/current/sql/statements/analyze.html>
- <https://duckdb.org/docs/current/sql/dialect/postgresql_compatibility.html>
- <https://duckdb.org/docs/current/sql/dialect/friendly_sql.html>
- <https://duckdb.org/docs/current/sql/dialect/indexing.html>
- <https://duckdb.org/docs/current/sql/dialect/order_preservation.html>
- <https://duckdb.org/docs/current/sql/dialect/sql_quirks.html>
- <https://duckdb.org/docs/current/clients/python/overview.html>
- <https://duckdb.org/docs/current/clients/python/data_ingestion.html>
- <https://duckdb.org/docs/current/clients/python/conversion.html>
- <https://duckdb.org/docs/current/clients/python/dbapi.html>
- <https://duckdb.org/docs/current/clients/python/relational_api.html>
- <https://duckdb.org/docs/current/clients/python/types.html>
- <https://duckdb.org/docs/current/clients/python/known_issues.html>
- <https://duckdb.org/docs/current/guides/python/import_pandas.html>
- <https://duckdb.org/docs/current/guides/python/export_pandas.html>
- <https://duckdb.org/docs/current/guides/python/export_arrow.html>
- <https://duckdb.org/docs/current/guides/python/sql_on_pandas.html>
- <https://duckdb.org/docs/current/guides/python/sql_on_arrow.html>
- <https://duckdb.org/docs/current/guides/python/polars.html>
- <https://duckdb.org/docs/current/guides/python/multiple_threads.html>
- <https://duckdb.org/docs/current/guides/python/execute_sql.html>
- <https://duckdb.org/docs/current/guides/performance/overview.html>
- <https://duckdb.org/docs/current/guides/performance/import.html>
- <https://duckdb.org/docs/current/guides/performance/schema.html>
- <https://duckdb.org/docs/current/guides/performance/environment.html>
- <https://duckdb.org/docs/current/guides/performance/my_workload_is_slow.html>
- <https://duckdb.org/docs/current/guides/performance/working_with_huge_databases.html>
- <https://duckdb.org/docs/current/guides/performance/oom.html>
- <https://duckdb.org/docs/current/guides/performance/benchmarks.html>
- <https://duckdb.org/docs/current/data/overview.html>
- <https://duckdb.org/docs/current/data/insert.html>
- <https://duckdb.org/docs/current/data/appender.html>
- <https://duckdb.org/docs/current/data/data_sources.html>
- <https://duckdb.org/docs/current/connect/overview.html>
- <https://duckdb.org/docs/current/guides/sql_features/merge.html>
- <https://duckdb.org/docs/current/operations_manual/footprint_of_duckdb/reclaiming_space.html>
- <https://github.com/duckdb/duckdb-web/tree/main/docs/current>: fonte em Markdown das páginas acima

Blog:

- <https://duckdb.org/news/>
- <https://duckdb.org/2025/09/16/announcing-duckdb-140.html>
- <https://duckdb.org/2025/10/07/announcing-duckdb-141.html>
- <https://duckdb.org/2025/11/12/announcing-duckdb-142.html>
- <https://duckdb.org/2025/11/28/iceberg-writes-in-duckdb.html>
- <https://duckdb.org/2025/12/09/announcing-duckdb-143.html>
- <https://duckdb.org/2026/01/26/announcing-duckdb-144.html>
- <https://duckdb.org/2026/03/09/announcing-duckdb-150.html>
- <https://duckdb.org/2026/03/23/announcing-duckdb-151.html>
- <https://duckdb.org/2026/04/13/announcing-duckdb-152.html>
- <https://duckdb.org/2026/05/20/announcing-duckdb-153.html>
- <https://duckdb.org/2026/05/29/new-iceberg-features.html>
- <https://duckdb.org/2026/06/17/announcing-duckdb-145.html>
- <https://duckdb.org/2026/06/17/announcing-duckdb-154.html>
- <https://duckdb.org/2026/07/22/announcing-duckdb-155.html>
- <https://duckdb.org/2026/08/17/duckdb-20-highlights.html>
- <https://duckdb.org/2026/09/02/try-duckdb-20-alpha.html>
- <https://duckdb.org/2021/06/25/querying-parquet.html>
- <https://duckdb.org/2025/01/22/parquet-encodings.html>
- <https://duckdb.org/2025/02/05/announcing-duckdb-120.html>
- <https://duckdb.org/2025/03/07/parquet-bloom-filters-in-duckdb.html>

Repositório `duckdb` no GitHub:

- <https://github.com/duckdb/duckdb/issues/2755>
- <https://github.com/duckdb/duckdb/issues/25521>
- <https://github.com/duckdb/duckdb/issues/23921>
- A busca de issues por `NaN` em `duckdb/duckdb` e `duckdb/duckdb-delta` (2026-09-23)

Extensão `iceberg` no GitHub:

- <https://github.com/duckdb/duckdb-iceberg>
- <https://github.com/duckdb/duckdb-iceberg/tree/45163a28>: código da versão usada pelo DuckDB 1.5.5, em
  `src/catalog/rest/` (transações, catálogo, API e autenticação SigV4), `src/core/expression/`,
  `src/execution/operator/` e testes `.test` de partições temporais e tabelas ordenadas
- <https://github.com/duckdb/duckdb/blob/v1.5.5/.github/config/extensions/iceberg.cmake>
- <https://github.com/duckdb/duckdb-iceberg/issues/33>
- <https://github.com/duckdb/duckdb-iceberg/issues/40>
- <https://github.com/duckdb/duckdb-iceberg/issues/418>
- <https://github.com/duckdb/duckdb-iceberg/issues/454>
- <https://github.com/duckdb/duckdb-iceberg/issues/520>
- <https://github.com/duckdb/duckdb-iceberg/issues/555>
- <https://github.com/duckdb/duckdb-iceberg/issues/624>
- <https://github.com/duckdb/duckdb-iceberg/issues/626>
- <https://github.com/duckdb/duckdb-iceberg/issues/631>
- <https://github.com/duckdb/duckdb-iceberg/issues/660>
- <https://github.com/duckdb/duckdb-iceberg/issues/699>
- <https://github.com/duckdb/duckdb-iceberg/issues/786>
- <https://github.com/duckdb/duckdb-iceberg/issues/790>
- <https://github.com/duckdb/duckdb-iceberg/issues/810>
- <https://github.com/duckdb/duckdb-iceberg/issues/814>
- <https://github.com/duckdb/duckdb-iceberg/issues/890>
- <https://github.com/duckdb/duckdb-iceberg/issues/941>
- <https://github.com/duckdb/duckdb-iceberg/issues/942>
- <https://github.com/duckdb/duckdb-iceberg/issues/955>
- <https://github.com/duckdb/duckdb-iceberg/issues/984>
- <https://github.com/duckdb/duckdb-iceberg/issues/995>
- <https://github.com/duckdb/duckdb-iceberg/issues/1057>
- <https://github.com/duckdb/duckdb-iceberg/issues/1128>
- <https://github.com/duckdb/duckdb-iceberg/issues/1162>
- <https://github.com/duckdb/duckdb-iceberg/issues/1290>
- <https://github.com/duckdb/duckdb-iceberg/issues/1299>
- <https://github.com/duckdb/duckdb-iceberg/issues/1322>
- <https://github.com/duckdb/duckdb-iceberg/issues/1369>
- <https://github.com/duckdb/duckdb-iceberg/pull/563>
- <https://github.com/duckdb/duckdb-iceberg/pull/700>
- <https://github.com/duckdb/duckdb-iceberg/pull/948>
- <https://github.com/duckdb/duckdb-iceberg/pull/1056>
- <https://github.com/duckdb/duckdb-iceberg/pull/1115>
- <https://github.com/duckdb/duckdb-iceberg/pull/1163>
- <https://github.com/duckdb/duckdb-iceberg/pull/1170>
- <https://github.com/duckdb/duckdb-iceberg/pull/1181>
- <https://github.com/duckdb/duckdb-iceberg/pull/1256>
- <https://github.com/duckdb/duckdb-iceberg/pull/1310>
- <https://github.com/duckdb/duckdb-iceberg/pull/1351>

Extensões `delta`, `iceberg` e `ducklake`, e `ATTACH` remoto:

- <https://duckdb.org/docs/current/core_extensions/delta.html>
- <https://duckdb.org/docs/current/core_extensions/iceberg/overview.html>
- <https://duckdb.org/docs/current/core_extensions/ducklake>
- <https://duckdb.org/docs/current/sql/statements/attach.html>
- <https://duckdb.org/2026/04/13/announcing-duckdb-152>

Escrita particionada (`COPY ... PARTITION_BY`), consultada em 2026-09-19:

- <https://duckdb.org/docs/stable/data/partitioning/partitioned_writes>
- <https://duckdb.org/docs/current/data/partitioning/partitioned_writes.html>

A memória, as threads e o cgroup, consultados em 2026-09-24 (os limites lidos do ambiente):

- <https://duckdb.org/2024/07/09/memory-management>
- <https://duckdb.org/docs/current/guides/performance/oom>
- <https://duckdb.org/docs/lts/guides/troubleshooting/oom_errors>
- <https://duckdb.org/docs/current/guides/performance/environment>
- <https://duckdb.org/docs/current/guides/performance/how_to_tune_workloads>
- <https://duckdb.org/docs/current/configuration/overview>
- <https://duckdb.org/2025/05/21/announcing-duckdb-130>
- <https://duckdb.org/2025/09/24/sorting-again>
- <https://github.com/duckdb/duckdb/issues/15080> (o limite do cgroup v2 lido errado na 1.1.3)
- <https://github.com/duckdb/duckdb/pull/16608> (a leitura do cgroup v1 e v2, memória e CPU, na 1.3)
- <https://raw.githubusercontent.com/duckdb/duckdb/v1.5.0/src/common/cgroups.cpp>
- <https://github.com/duckdb/duckdb/issues/7651>
- <https://github.com/duckdb/duckdb/discussions/16781>
- <https://github.com/milaboratory/platforma/pull/1848> (o limite do DuckDB tirado da memória livre)

## PyArrow

- <https://arrow.apache.org/docs/python/parquet.html>
- <https://arrow.apache.org/docs/python/dataset.html>
- <https://github.com/apache/arrow/tree/main/docs/source/python/parquet>: fonte das páginas
  `parquet.rst`, `parquet_datasets.rst`, `parquet_type_handling.rst` e `parquet_encryption.rst`
- <https://arrow.apache.org/docs/python/generated/pyarrow.parquet.write_table.html>:
  `write_page_checksum`, o CRC da página, desligado por padrão (2026-09-25)

A garantia do fragmento e a simplificação do filtro por ela, consultadas em 2026-09-25:

- <https://arrow.apache.org/docs/python/generated/pyarrow.dataset.FileSystemDataset.html>
- <https://arrow.apache.org/docs/python/generated/pyarrow.dataset.Fragment.html>
- <https://arrow.apache.org/docs/python/generated/pyarrow.dataset.ParquetFileFormat.html>
- <https://raw.githubusercontent.com/apache/arrow/main/python/pyarrow/_dataset.pyx>, e na tag
  `apache-arrow-25.0.1`
- <https://raw.githubusercontent.com/apache/arrow/apache-arrow-25.0.1/cpp/src/arrow/compute/expression.cc>
  e `expression_internal.h` na mesma pasta
- <https://github.com/apache/arrow/issues/51491>
- <https://github.com/apache/arrow/pull/48716>

## SQLAlchemy

Documentação da versão 2.0:

- <https://docs.sqlalchemy.org/en/20/core/metadata.html>
- <https://docs.sqlalchemy.org/en/20/core/reflection.html>
- <https://docs.sqlalchemy.org/en/20/core/ddl.html>
- <https://docs.sqlalchemy.org/en/20/core/constraints.html>
- <https://docs.sqlalchemy.org/en/20/core/defaults.html>
- <https://docs.sqlalchemy.org/en/20/core/type_basics.html>
- <https://docs.sqlalchemy.org/en/20/core/custom_types.html>
- <https://docs.sqlalchemy.org/en/20/core/compiler.html>
- <https://docs.sqlalchemy.org/en/20/core/dml.html>
- <https://docs.sqlalchemy.org/en/20/core/connections.html>
- <https://docs.sqlalchemy.org/en/20/faq/sqlexpressions.html>
- <https://docs.sqlalchemy.org/en/20/core/engines.html>
- <https://docs.sqlalchemy.org/en/20/core/event.html>
- <https://docs.sqlalchemy.org/en/20/core/events.html>
- <https://docs.sqlalchemy.org/en/20/core/inspection.html>
- <https://docs.sqlalchemy.org/en/20/core/schema.html>
- <https://docs.sqlalchemy.org/en/20/tutorial/metadata.html>
- <https://docs.sqlalchemy.org/en/20/tutorial/data_insert.html>
- <https://docs.sqlalchemy.org/en/20/tutorial/data_select.html>
- <https://docs.sqlalchemy.org/en/20/tutorial/data_update.html>
- <https://docs.sqlalchemy.org/en/20/tutorial/orm_data_manipulation.html>
- <https://docs.sqlalchemy.org/en/20/tutorial/dbapi_transactions.html>
- <https://docs.sqlalchemy.org/en/20/tutorial/engine.html>
- <https://docs.sqlalchemy.org/en/20/orm/declarative_tables.html>
- <https://docs.sqlalchemy.org/en/20/orm/declarative_config.html>
- <https://docs.sqlalchemy.org/en/20/orm/declarative_styles.html>
- <https://docs.sqlalchemy.org/en/20/orm/declarative_mixins.html>
- <https://docs.sqlalchemy.org/en/20/orm/mapping_styles.html>
- <https://docs.sqlalchemy.org/en/20/orm/mapping_api.html>
- <https://docs.sqlalchemy.org/en/20/orm/mapped_attributes.html>
- <https://docs.sqlalchemy.org/en/20/orm/dataclasses.html>
- <https://docs.sqlalchemy.org/en/20/orm/queryguide/dml.html>
- <https://docs.sqlalchemy.org/en/20/orm/queryguide/select.html>
- <https://docs.sqlalchemy.org/en/20/orm/persistence_techniques.html>
- <https://docs.sqlalchemy.org/en/20/orm/session_basics.html>
- <https://docs.sqlalchemy.org/en/20/orm/quickstart.html>
- <https://docs.sqlalchemy.org/en/20/orm/basic_relationships.html>
- <https://docs.sqlalchemy.org/en/20/faq/performance.html>
- <https://docs.sqlalchemy.org/en/20/dialects/index.html>
- <https://docs.sqlalchemy.org/en/20/dialects/postgresql.html>
- <https://docs.sqlalchemy.org/en/20/orm/relationship_persistence.html>

Dialeto do Redshift, no repositório `sqlalchemy-redshift/sqlalchemy-redshift`:

- <https://github.com/sqlalchemy-redshift/sqlalchemy-redshift/blob/main/README.rst>
- <https://github.com/sqlalchemy-redshift/sqlalchemy-redshift/blob/main/CHANGES.rst>
- <https://github.com/sqlalchemy-redshift/sqlalchemy-redshift/blob/main/docs/ddl-compiler.rst>
- <https://github.com/sqlalchemy-redshift/sqlalchemy-redshift/blob/main/docs/commands.rst>
- <https://github.com/sqlalchemy-redshift/sqlalchemy-redshift/blob/main/sqlalchemy_redshift/dialect.py>
- <https://github.com/sqlalchemy-redshift/sqlalchemy-redshift/blob/main/sqlalchemy_redshift/ddl.py>
- <https://github.com/sqlalchemy-redshift/sqlalchemy-redshift/blob/main/sqlalchemy_redshift/commands.py>
- <https://sqlalchemy-redshift.readthedocs.io/en/latest/>

Dialeto do DuckDB, no repositório `Mause/duckdb_engine`:

- <https://github.com/Mause/duckdb_engine/blob/main/README.md>
- <https://github.com/Mause/duckdb_engine/blob/main/duckdb_engine/__init__.py>
- <https://github.com/Mause/duckdb_engine/blob/main/duckdb_engine/datatypes.py>
- <https://github.com/Mause/duckdb_engine/blob/main/duckdb_engine/config.py>

Documentação da versão 2.1, lida em 2026-09-25 para a troca da versão fixada:

- <https://docs.sqlalchemy.org/en/21/changelog/migration_21.html>
- <https://docs.sqlalchemy.org/en/21/changelog/changelog_21.html>

## pandas

- <https://pandas.pydata.org/docs/reference/api/pandas.read_sql.html>
- <https://pandas.pydata.org/docs/reference/api/pandas.read_sql_query.html>
- <https://pandas.pydata.org/docs/reference/api/pandas.read_sql_table.html>
- <https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_sql.html>
- <https://pandas.pydata.org/docs/reference/api/pandas.ArrowDtype.html>
- <https://pandas.pydata.org/docs/user_guide/pyarrow.html>

## PostgreSQL

Semântica de restrições adiáveis, herdada pelos dialetos do DuckDB e do Redshift:

- <https://www.postgresql.org/docs/current/sql-createtable.html>
- <https://www.postgresql.org/docs/current/sql-set-constraints.html>

O `NaN` igual a si mesmo e acima de todo número:

- <https://www.postgresql.org/docs/current/datatype-numeric.html>

## PyIceberg

- <https://py.iceberg.apache.org/api/>
- <https://py.iceberg.apache.org/configuration/>
- <https://github.com/apache/iceberg-python/releases>
- <https://github.com/apache/iceberg-python/tree/pyiceberg-0.12.0>: `pyiceberg/catalog/` (`glue.py`,
  `sql.py`, `memory.py`, `rest/`), `pyiceberg/table/` (`__init__.py`, `inspect.py`, `maintenance.py`,
  `metadata.py`, `name_mapping.py`, `update/`), `pyiceberg/io/`, `pyiceberg/schema.py`,
  `mkdocs/docs/` e testes de `add_files`, `name_mapping` e escrita
- Versões anteriores de `pyiceberg/table/__init__.py`, `pyiceberg/io/pyarrow.py` e
  `mkdocs/docs/configuration.md`, das tags `pyiceberg-0.5.0` a `pyiceberg-0.11.1`
- <https://github.com/apache/iceberg-python/issues/809>
- <https://github.com/apache/iceberg-python/issues/1551>
- <https://github.com/apache/iceberg-python/issues/2131>
- <https://github.com/apache/iceberg-python/issues/2201>
- <https://github.com/apache/iceberg-python/issues/2203>
- <https://github.com/apache/iceberg-python/issues/2604>
- <https://github.com/apache/iceberg-python/issues/3008>
- <https://github.com/apache/iceberg-python/issues/3556>
- <https://github.com/apache/iceberg-python/pull/1925>
- <https://github.com/apache/iceberg-python/pull/3320>

Consultadas para a estratégia de implementação:

- <https://py.iceberg.apache.org/api/>
- <https://dlthub.com/docs/dlt-ecosystem/destinations/iceberg>

Especificação do catálogo REST do Iceberg (namespaces e commit de várias tabelas), consultada em
2026-09-19:

- <https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml>
- <https://iceberg.apache.org/spec/>
- <https://github.com/apache/iceberg/blob/main/format/spec.md>
- <https://github.com/apache/iceberg-python> (`pyiceberg/catalog/__init__.py` e `pyiceberg/catalog/rest/__init__.py`)

## AWS Glue Data Catalog

- <https://docs.aws.amazon.com/glue/latest/dg/table-optimizers.html>
- <https://docs.aws.amazon.com/glue/latest/dg/compaction-management.html>
- <https://docs.aws.amazon.com/glue/latest/dg/enable-compaction.html>
- <https://docs.aws.amazon.com/glue/latest/dg/snapshot-retention-management.html>
- <https://docs.aws.amazon.com/glue/latest/dg/enable-snapshot-retention.html>
- <https://docs.aws.amazon.com/glue/latest/dg/orphan-file-deletion.html>
- <https://docs.aws.amazon.com/glue/latest/dg/enable-orphan-file-deletion.html>
- <https://docs.aws.amazon.com/glue/latest/dg/optimization-prerequisites.html>
- <https://docs.aws.amazon.com/glue/latest/dg/optimizer-notes.html>
- <https://docs.aws.amazon.com/glue/latest/dg/view-optimization-metrics.html>
- <https://docs.aws.amazon.com/glue/latest/dg/connect-glu-iceberg-rest.html>
- <https://docs.aws.amazon.com/glue/latest/dg/connect-glue-iceberg-rest-ext.html>
- <https://docs.aws.amazon.com/glue/latest/dg/iceberg-rest-apis.html>
- <https://docs.aws.amazon.com/glue/latest/dg/limitation-glue-iceberg-rest-api.html>
- <https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-iceberg.html>
- <https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog-tables.html>
- <https://docs.aws.amazon.com/glue/latest/dg/access_catalog.html>
- <https://docs.aws.amazon.com/glue/latest/webapi/API_UpdateTable.html>
- <https://docs.aws.amazon.com/glue/latest/webapi/API_UpdatePartition.html>
- <https://docs.aws.amazon.com/glue/latest/webapi/API_BatchUpdatePartition.html>
- <https://docs.aws.amazon.com/glue/latest/webapi/API_IcebergCompactionConfiguration.html>
- <https://docs.aws.amazon.com/glue/latest/webapi/API_IcebergRetentionConfiguration.html>
- <https://docs.aws.amazon.com/glue/latest/webapi/API_IcebergOrphanFileDeletionConfiguration.html>
- <https://docs.aws.amazon.com/general/latest/gr/glue.html>
- <https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-integrating-glue-endpoint.html>
- <https://aws.amazon.com/glue/pricing/>
- <https://aws.amazon.com/about-aws/whats-new/2023/11/aws-glue-data-catalog-compaction-iceberg-tables/>
- <https://aws.amazon.com/about-aws/whats-new/2024/09/aws-glue-data-catalog-optimization-apache-iceberg-tables/>
- <https://aws.amazon.com/about-aws/whats-new/2025/06/amazon-s3-sort-z-order-compaction-apache-iceberg-tables/>
- <https://aws.amazon.com/blogs/aws/aws-glue-data-catalog-now-supports-automatic-compaction-of-apache-iceberg-tables/>
- <https://aws.amazon.com/blogs/big-data/the-aws-glue-data-catalog-now-supports-storage-optimization-of-apache-iceberg-tables/>
- <https://aws.amazon.com/blogs/big-data/manage-concurrent-write-conflicts-in-apache-iceberg-on-the-aws-glue-data-catalog/>
- <https://aws.amazon.com/blogs/big-data/read-and-write-s3-iceberg-table-using-aws-glue-iceberg-rest-catalog-from-open-source-apache-spark/>
- <https://aws.amazon.com/blogs/big-data/access-amazon-s3-iceberg-tables-from-databricks-using-aws-glue-iceberg-rest-catalog-in-amazon-sagemaker-lakehouse/>
- <https://aws.amazon.com/blogs/big-data/zero-copy-access-to-apache-iceberg-tables-in-amazon-s3-from-salesforce-data-360-using-the-iceberg-rest-endpoint-from-aws-glue-data-catalog/>
- <https://aws.amazon.com/blogs/big-data/break-down-data-silos-and-seamlessly-query-iceberg-tables-in-amazon-sagemaker-from-snowflake/>
- <https://aws.amazon.com/blogs/big-data/accelerate-lightweight-analytics-using-pyiceberg-with-aws-lambda-and-an-aws-glue-iceberg-rest-endpoint/>

Crawler de tabelas Delta Lake (tabelas nativas e de manifesto simbólico), consultado em 2026-09-19:

- <https://docs.aws.amazon.com/glue/latest/dg/crawler-delta-lake.html>

## Amazon Athena

- <https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg.html>
- <https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-creating-tables.html>
- <https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-managing-tables.html>
- <https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-updating-iceberg-table-data.html>
- <https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-insert-into.html>
- <https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-merge-into.html>
- <https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-delete.html>
- <https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-update.html>
- <https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-data-optimization.html>
- <https://docs.aws.amazon.com/athena/latest/ug/optimize-statement.html>
- <https://docs.aws.amazon.com/athena/latest/ug/vacuum-statement.html>
- <https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-time-travel-and-version-travel-queries.html>
- <https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-evolving-table-schema.html>
- <https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-alter-table-add-columns.html>
- <https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-alter-table-drop-column.html>
- <https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-alter-table-change-column.html>
- <https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-additional-operations.html>
- <https://docs.aws.amazon.com/athena/latest/ug/insert-into.html>
- <https://docs.aws.amazon.com/athena/latest/ug/merge-into-statement.html>
- <https://docs.aws.amazon.com/athena/latest/ug/ctas-insert-into.html>
- <https://docs.aws.amazon.com/athena/latest/ug/unsupported-ddl.html>
- <https://docs.aws.amazon.com/prescriptive-guidance/latest/apache-iceberg-on-aws/iceberg-athena.html>
- <https://aws.amazon.com/blogs/big-data/perform-upserts-in-a-data-lake-using-amazon-athena-and-apache-iceberg/>

## AWS Lake Formation

- <https://docs.aws.amazon.com/lake-formation/latest/dg/creating-iceberg-tables.html>
- <https://docs.aws.amazon.com/lake-formation/latest/dg/lf-permissions-reference.html>
- <https://docs.aws.amazon.com/lake-formation/latest/dg/access-control-underlying-data.html>
- <https://docs.aws.amazon.com/lake-formation/latest/dg/hybrid-access-mode.html>
- <https://docs.aws.amazon.com/lake-formation/latest/dg/notes-hybrid.html>
- <https://docs.aws.amazon.com/lake-formation/latest/dg/full-table-credential-vending.html>
- <https://docs.aws.amazon.com/lake-formation/latest/dg/using-cred-vending.html>
- <https://docs.aws.amazon.com/lake-formation/latest/dg/how-vending-works.html>
- <https://docs.aws.amazon.com/lake-formation/latest/dg/RSPC-lf.html>
- <https://docs.aws.amazon.com/cli/latest/reference/lakeformation/index.html>
- <https://docs.aws.amazon.com/cli/latest/reference/lakeformation/get-data-lake-settings.html>
- <https://docs.aws.amazon.com/cli/latest/reference/lakeformation/list-resources.html>
- <https://docs.aws.amazon.com/cli/latest/reference/lakeformation/describe-resource.html>
- <https://docs.aws.amazon.com/cli/latest/reference/lakeformation/list-permissions.html>
- <https://docs.aws.amazon.com/cli/latest/reference/lakeformation/list-lake-formation-opt-ins.html>
- <https://aws.amazon.com/blogs/big-data/access-amazon-s3-data-files-directly-using-aws-lake-formation-permissions/>
- <https://aws.amazon.com/blogs/big-data/amazon-datazone-announces-integration-with-aws-lake-formation-hybrid-access-mode-for-the-aws-glue-data-catalog/>

## Amazon SageMaker Unified Studio

Guia do usuário:

- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/ide-spaces.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/code-spaces-idc.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/code-spaces-iam.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/jupyterlab.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/notebooks.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/notebooks-schedule-runs.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/work-with-cells.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/sql-utilities.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/compute.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/manage-compute-environments.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/compute-permission-mode.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/storage.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/s3-path.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/managing-configurations.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/sagemaker-build-models.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/workflow-orchestration.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/workflow-environments.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/serverless-workflows.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/using-project-tip.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/lake-formation-permissions-for-amazon-sagemaker-unified-studio.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/lakehouse.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/bring-resources-scripts.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/adding-a-existing-athena-connection.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/adding-a-existing-compute-connection.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/compute-prerequisite-athena.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/compute-redshift.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/compute-prerequisite-redshift.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/connecting-amazon-redshift.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/compute-add-new-redshift-serverless.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/service-quotas.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/release-notes.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/troubleshooting-issues.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/grant-access-to-redshift-asset.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/grant-access-to-glue-asset.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/approve-reject-subscription-request.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/data-s3-publish.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/data.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/data-source-glue.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/catalog-iam-use-case-glue-tables-discoverable.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/concepts.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/gs-sql.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/python-library.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/project.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/connections.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/connection-data.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/connection-clients.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/secrets.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/using-client-config.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/accessing-metadata.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/jupyterlab-data-sharing-across-compute.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/sql-query.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/sql-query-write-run.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/connect-data-sources.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/data-connections.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/connecting-new-data-source.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/jupyterlab-sql-spark.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/notebooks-spark-connect.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/removing-compute-redshift.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/query-with-jdbc.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/byoi-how-to.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/byoi-launch-custom-image.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/sagemaker-xgboost-recipe.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/sagemaker-train-models.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/create-monitor-training-jobs.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/sagemaker-deploy-models.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/sagemaker-pipelines.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/strands-agents.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/gs-ml.html>

Guia do administrador e outras documentações:

- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/adminguide/manage-tooling-blueprint.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/adminguide/supported-blueprints.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/adminguide/security-iam-roles.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/adminguide/security-accesss-control-patterns.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/adminguide/security-authorization.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/adminguide/trusted-identity-propagation.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/adminguide/iam-based-domains.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/adminguide/iam-based-domains-overview.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/adminguide/projects-iam-based-domains.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/adminguide/setup-iam-based-domains.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/adminguide/setup-projects-idc-based-domains.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/adminguide/security-iam-awsmanpol-SageMakerStudioProjectUserRolePolicy.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/adminguide/security-iam-awsmanpol-SageMakerStudioUserIAMDefaultExecutionPolicy.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/adminguide/quotas.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/adminguide/all-capabilities.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/adminguide/sql-analytics.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/adminguide/blueprints.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/adminguide/custom-blueprint.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/adminguide/AmazonSageMakerManageAccess.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/adminguide/security-iam-awsmanpol-SageMakerStudioProjectProvisioningRolePolicy.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/adminguide/setup-projects-iam-based-domains.html>
- <https://docs.aws.amazon.com/sagemaker-unified-studio/latest/adminguide/data-onboarding.html>
- <https://docs.aws.amazon.com/aws-managed-policy/latest/reference/SageMakerStudioProjectProvisioningRolePolicy.html>
- <https://docs.aws.amazon.com/aws-managed-policy/latest/reference/SageMakerStudioProjectUserRolePolicy.html>
- <https://docs.aws.amazon.com/datazone/latest/userguide/grant-access-to-redshift-asset.html>
- <https://docs.aws.amazon.com/datazone/latest/userguide/working-with-blueprints.html>
- <https://docs.aws.amazon.com/datazone/latest/userguide/hybrid-mode.html>
- <https://docs.aws.amazon.com/next-generation-sagemaker/latest/userguide/upload-query.html>
- <https://docs.aws.amazon.com/sagemaker-lakehouse-architecture/latest/userguide/lakehouse-add-new-database.html>
- <https://docs.aws.amazon.com/sagemaker-lakehouse-architecture/latest/userguide/onboarding-data-sagemaker-lakehouse.html>
- <https://docs.aws.amazon.com/sagemaker-lakehouse-architecture/latest/userguide/s3-data-lakes.html>
- <https://docs.aws.amazon.com/sagemaker-lakehouse-architecture/latest/userguide/lakehouse-upload-data.html>
- <https://docs.aws.amazon.com/sagemaker-lakehouse-architecture/latest/userguide/lakehouse-data-connection.html>
- <https://docs.aws.amazon.com/sagemaker-lakehouse-architecture/latest/userguide/lakehouse-create-connection.html>
- <https://docs.aws.amazon.com/sagemaker-lakehouse-architecture/latest/userguide/rms-integration.html>
- <https://docs.aws.amazon.com/next-generation-sagemaker/latest/userguide/getting-started-sagemaker-gdc-s3.html>
- <https://docs.aws.amazon.com/sagemaker-lakehouse-architecture/latest/userguide/lakehouse-add-catalog.html>
- <https://docs.aws.amazon.com/sagemaker-lakehouse-architecture/latest/userguide/lakehouse-how.html>
- <https://docs.aws.amazon.com/sagemaker-lakehouse-architecture/latest/userguide/s3-tables-integration.html>

Novidades e blog:

- <https://aws.amazon.com/about-aws/whats-new/2025/03/amazon-sagemaker-lakehouse-integration-s3-tables-generally-available/>
- <https://aws.amazon.com/about-aws/whats-new/2026/04/amazon-sagemaker-serverless-workflows/>
- <https://aws.amazon.com/about-aws/whats-new/2026/05/smus-identity-user-management/>
- <https://aws.amazon.com/about-aws/whats-new/2026/06/amazon-sagemaker-unified-studio/>
- <https://aws.amazon.com/about-aws/whats-new/2026/08/amazon-sagemaker/>
- <https://aws.amazon.com/blogs/big-data/schedule-notebook-runs-in-amazon-sagemaker-unified-studio/>
- <https://aws.amazon.com/blogs/big-data/use-apache-airflow-workflows-to-orchestrate-data-processing-on-amazon-sagemaker-unified-studio/>
- <https://aws.amazon.com/blogs/big-data/get-to-insights-faster-using-notebooks-in-amazon-sagemaker-unified-studio/>
- <https://aws.amazon.com/blogs/big-data/access-your-existing-data-and-resources-through-amazon-sagemaker-unified-studio-part-1-aws-glue-data-catalog-and-amazon-redshift/>
- <https://aws.amazon.com/blogs/big-data/access-your-existing-data-and-resources-through-amazon-sagemaker-unified-studio-part-2-amazon-s3-amazon-rds-amazon-dynamodb-and-amazon-emr/>
- <https://aws.amazon.com/blogs/big-data/guide-to-adopting-amazon-sagemaker-unified-studio-from-atpcos-journey/>
- <https://aws.amazon.com/blogs/big-data/navigating-architectural-choices-for-a-lakehouse-using-amazon-sagemaker/>
- <https://aws.amazon.com/blogs/big-data/connect-share-and-query-where-your-data-sits-using-amazon-sagemaker-unified-studio/>
- <https://aws.amazon.com/blogs/big-data/foundational-blocks-of-amazon-sagemaker-unified-studio-an-admins-guide-to-implement-unified-access-to-all-your-data-analytics-and-ai/>
- <https://aws.amazon.com/blogs/big-data/scaling-fine-grained-access-control-for-enterprise-lakehouse-using-sagemaker-unified-studio-and-aws-lake-formation/>
- <https://aws.amazon.com/blogs/big-data/govern-amazon-redshift-data-across-accounts-with-sagemaker-unified-studio/>
- <https://aws.amazon.com/blogs/big-data/tailor-amazon-sagemaker-unified-studio-project-environments-to-your-needs-using-custom-blueprints/>
- <https://aws.amazon.com/about-aws/whats-new/2026/01/sagemaker-unified-studio-adds-cross-region-iam/>

## Amazon S3

- <https://aws.amazon.com/about-aws/whats-new/2024/11/amazon-s3-functionality-conditional-writes>
- <https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html>

Escritas condicionais (`If-None-Match`, `If-Match`):

- <https://aws.amazon.com/about-aws/whats-new/2024/08/amazon-s3-conditional-writes>
- <https://aws.amazon.com/about-aws/whats-new/2024/11/amazon-s3-functionality-conditional-writes>
- <https://aws.amazon.com/about-aws/whats-new/2025/10/amazon-s3-conditional-write-functionality-copy-operations>
- <https://aws.amazon.com/blogs/storage/building-multi-writer-applications-on-amazon-s3-using-native-controls/>
- <https://docs.aws.amazon.com/boto3/latest/reference/services/s3/client/put_object.html>
- <https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes-enforce.html>
- <https://aws.amazon.com/about-aws/whats-new/2024/11/amazon-s3-enforcement-conditional-write-operations-general-purpose-buckets/>

A integridade dos objetos, consultada em 2026-09-25 (o CRC64NVME do objeto inteiro e o ETag que
não é MD5 sob SSE-KMS nem no multipart):

- <https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html>
- <https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity-upload.html>

## Biblioteca padrão do Python

- <https://docs.python.org/3/library/warnings.html> (`catch_warnings` altera o estado global do
  módulo e não é seguro com threads sem `context_aware_warnings`, do Python 3.14)
- <https://docs.python.org/3.13/library/os.html> (a página veio cortada antes de
  `process_cpu_count`, 2026-09-24)
- <https://docs.python.org/3.13/whatsnew/3.13.html> (`os.process_cpu_count` e `PYTHON_CPU_COUNT`)

## Linux e EC2

As CPUs e a memória de um processo, consultadas em 2026-09-24:

- <https://docs.kernel.org/admin-guide/cgroup-v2.html> (`cpu.max`, `memory.max`, `memory.current`,
  `memory.stat` e `/proc/self/cgroup`)
- <https://docs.kernel.org/filesystems/proc.html> (`MemAvailable`)
- <https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instance-optimize-cpu.html> (cada vCPU é uma
  thread de um núcleo nas instâncias com SMT)

## Pacotes Python

- <https://github.com/Mause/duckdb_engine>
- <https://pypi.org/project/sqlalchemy-redshift/>
- <https://github.com/aws/amazon-redshift-python-driver>
- <https://aws-sdk-pandas.readthedocs.io/en/stable/stubs/awswrangler.redshift.copy.html>
- <https://adbc-drivers.org/drivers/redshift/>

- <https://github.com/aws/amazon-redshift-python-driver/blob/master/README.rst>
- <https://github.com/aws/amazon-redshift-python-driver/blob/master/redshift_connector/cursor.py>
- <https://github.com/aws/amazon-redshift-python-driver/blob/master/CHANGELOG.md>
- <https://github.com/aws/aws-sdk-pandas/blob/main/awswrangler/redshift/_write.py>
- <https://github.com/aws/aws-sdk-pandas/blob/main/awswrangler/redshift/_read.py>
- <https://github.com/aws/aws-sdk-pandas/blob/main/awswrangler/redshift/_utils.py>
- <https://github.com/aws/aws-sdk-pandas/blob/main/awswrangler/redshift/_connect.py>
- <https://github.com/aws/aws-sdk-pandas/blob/main/awswrangler/_data_types.py>

API JSON do PyPI, consultada para versões e datas de lançamento:

- <https://pypi.org/pypi/SQLAlchemy/json>
- <https://pypi.org/pypi/redshift-connector/json>
- <https://pypi.org/pypi/duckdb/json>
- <https://pypi.org/pypi/duckdb/1.5.5/json>
- <https://pypi.org/pypi/alembic/json>
- <https://pypi.org/pypi/pyarrow/json>
- <https://pypi.org/pypi/awswrangler/json>
- <https://pypi.org/pypi/pyiceberg/json>
- <https://pypi.org/pypi/adbc-driver-manager/json>
- <https://pypi.org/pypi/pandas/json>
- <https://pypi.org/pypi/polars/json>
- <https://pypi.org/pypi/boto3/json>
- <https://pypi.org/pypi/sagemaker-studio/json>

API JSON do PyPI, consultada em 2026-09-19 para a estratégia de implementação:

- <https://pypi.org/pypi/deltalake/json>, <https://pypi.org/pypi/deltalake/1.0.0/json>
- <https://pypi.org/pypi/duckdb/1.5.2/json>
- <https://pypi.org/pypi/sqlglot/json>, <https://pypi.org/pypi/sqlmesh/json>,
  <https://pypi.org/pypi/sqlmesh/0.236.2/json>
- <https://pypi.org/pypi/dbt-core/json>, <https://pypi.org/pypi/dbt-duckdb/json>,
  <https://pypi.org/pypi/dbt-redshift/json>
- <https://pypi.org/pypi/datafusion/json>, <https://pypi.org/pypi/ibis-framework/json>,
  <https://pypi.org/pypi/narwhals/json>, <https://pypi.org/pypi/daft/json>
- <https://pypi.org/pypi/pandera/json>, <https://pypi.org/pypi/patito/json>,
  <https://pypi.org/pypi/pydantic/json>, <https://pypi.org/pypi/great-expectations/json>,
  <https://pypi.org/pypi/datacontract-cli/json>
- <https://pypi.org/pypi/lancedb/json>, <https://pypi.org/pypi/pylance/json>,
  <https://pypi.org/pypi/hudi/json>, <https://pypi.org/pypi/ducklake/json>
- <https://pypi.org/pypi/maturin/json>, <https://pypi.org/pypi/sqlmodel/json>,
  <https://pypi.org/pypi/pypika/json>, <https://pypi.org/pypi/dlt/json>,
  <https://pypi.org/pypi/dagster/json>, <https://pypi.org/pypi/prefect/json>

API JSON do PyPI, consultada em 2026-09-25 para as versões retiradas do deltalake:
<https://pypi.org/pypi/deltalake/json>.

API JSON do PyPI e os pacotes-fonte, consultados em 2026-09-25 para a troca do deltalake pela
1.6.6:

- <https://pypi.org/pypi/deltalake/json>, <https://pypi.org/pypi/deltalake/1.6.4/json>,
  <https://pypi.org/pypi/deltalake/1.6.5/json> e <https://pypi.org/pypi/deltalake/1.6.6/json>
- <https://files.pythonhosted.org/packages/b0/68/29c9e7c9fcf8b5f03ec300566e974fca7065fd84709f34ef1b85ec74e385/deltalake-1.6.4.tar.gz>
- <https://files.pythonhosted.org/packages/24/7e/817984d82cec757f6f3a3dbb84afcd85027e7ae02e0a354702c2127f6777/deltalake-1.6.6.tar.gz>

API do crates.io (`https://crates.io/api/v1/crates/<crate>`), consultada em 2026-09-19 para
`object_store`, `parquet`, `arrow`, `pyo3`, `pyo3-arrow`, `deltalake`, `delta_kernel`, `duckdb`,
`datafusion`, `hudi`, `iceberg`, `lance`, `maturin`, `sqlparser` e `polars`.

API JSON do PyPI, consultada em 2026-09-25 para as versões novas das dependências diretas:
<https://pypi.org/pypi/boto3/json>, <https://pypi.org/pypi/botocore/json>,
<https://pypi.org/pypi/s3transfer/json>, <https://pypi.org/pypi/deltalake/json>,
<https://pypi.org/pypi/duckdb/json>, <https://pypi.org/pypi/duckdb-engine/json>,
<https://pypi.org/pypi/pyarrow/json>, <https://pypi.org/pypi/sqlalchemy/json>,
<https://pypi.org/pypi/sqlalchemy-redshift/json>, <https://pypi.org/pypi/redshift-connector/json>,
<https://pypi.org/pypi/pandas/json>, <https://pypi.org/pypi/pytest/json>,
<https://pypi.org/pypi/sqlglot/json>, <https://pypi.org/pypi/flask/json>,
<https://pypi.org/pypi/flask-cors/json>, <https://pypi.org/pypi/moto/json>,
<https://pypi.org/pypi/pdoc/json>, <https://pypi.org/pypi/ipykernel/json> e
<https://pypi.org/pypi/uv-build/json>.

## Delta Lake

Documentação do delta-rs e repositórios:

- <https://delta-io.github.io/delta-rs/usage/writing/>
- <https://delta-io.github.io/delta-rs/usage/writing/writing-to-s3-with-locking-provider/>
- <https://delta-io.github.io/delta-rs/integrations/object-storage/s3/>
- <https://delta-io.github.io/delta-rs/integrations/object-storage/s3-like/>
- <https://delta-io.github.io/delta-rs/integrations/object-storage/hdfs/>
- <https://delta-io.github.io/delta-rs/api/delta_table/>
- <https://delta-io.github.io/delta-rs/api/delta_table/delta_table_alterer/>
- <https://github.com/delta-io/delta-rs/releases>
- <https://github.com/delta-io/delta-rs/discussions/4482>
- <https://github.com/delta-io/delta-rs/issues/4464>
- <https://github.com/delta-io/delta-rs/issues/2843>
- <https://github.com/delta-io/delta-rs/pull/4732>
- <https://github.com/delta-io/delta-rs/issues/3936>
- <https://github.com/delta-io/delta/blob/master/PROTOCOL.md>
- <https://docs.pola.rs/api/python/stable/reference/api/polars.DataFrame.write_delta.html>
- <https://docs.pola.rs/user-guide/io/hive/>
- <https://dlthub.com/docs/dlt-ecosystem/destinations/delta-iceberg>

Implementações do protocolo (delta-spark, delta-rs, Delta Kernel) e uso sem Spark, consultadas em
2026-09-19:

- <https://delta.io/learn/getting-started/>
- <https://delta.io/blog/delta-lake-without-spark/>
- <https://delta.io/integrations/>
- <https://docs.delta.io/latest/delta-storage.html>
- <https://pypi.org/project/delta-spark/>
- <https://delta-io.github.io/delta-rs/>
- <https://delta-io.github.io/delta-rs/feature-table/>
- <https://github.com/delta-io/delta-rs> (`README.md`, `Cargo.toml` e `crates/core/Cargo.toml`)
- <https://github.com/delta-io/delta-rs/issues/4512>
- <https://github.com/delta-io/delta-rs/issues/4613>
- <https://github.com/delta-io/delta-rs/issues/4657>
- <https://github.com/delta-io/delta-kernel-rs>
- <https://github.com/duckdb/duckdb-delta>

Interoperabilidade entre Delta e Iceberg (UniForm, XTable, convergência dos formatos), consultada em
2026-09-19:

- <https://docs.delta.io/latest/delta-uniform.html>
- <https://xtable.apache.org/>
- <https://xtable.apache.org/docs/how-to>
- <https://www.databricks.com/blog/databricks-tabular>
- <https://www.databricks.com/company/newsroom/press-releases/databricks-agrees-acquire-tabular-company-founded-original-creators-apache-iceberg> (404 em 2026-09-19)

O `NaN` nas estatísticas do log, consultado em 2026-09-23 (o protocolo, acima, na seção "Per-file
Statistics"):

- <https://github.com/delta-io/delta-kernel-rs/blob/main/default-engine/src/stats.rs>
- <https://github.com/delta-io/delta-kernel-rs/blob/main/kernel/src/scan/data_skipping.rs>
- <https://github.com/delta-io/delta/blob/master/spark/src/main/scala/org/apache/spark/sql/delta/stats/StatsCollectionUtils.scala>
- <https://github.com/delta-io/delta/blob/master/spark/src/main/scala/org/apache/spark/sql/delta/stats/StatisticsCollection.scala>
- <https://github.com/delta-io/delta/blob/master/spark/src/main/scala/org/apache/spark/sql/delta/stats/DataSkippingReader.scala>
- <https://github.com/delta-io/delta/blob/master/spark/src/main/scala/org/apache/spark/sql/delta/sources/DeltaSQLConf.scala>
- <https://github.com/delta-io/delta/pull/7101>
- A busca de issues e PRs por `NaN` em `delta-io/delta-rs` e `delta-io/delta-kernel-rs`

O motivo da retirada do `deltalake` 1.6.4 do PyPI, lido em 2026-09-25:

- <https://github.com/delta-io/delta-rs/issues/4784>

As versões do delta-rs entre a 1.6.4 e a 1.6.6, lidas em 2026-09-25 para a troca da versão fixada:

- <https://github.com/delta-io/delta-rs/releases/tag/python-v1.6.5> e
  <https://github.com/delta-io/delta-rs/releases/tag/python-v1.6.6>
- <https://github.com/delta-io/delta-rs/pull/4747>
- <https://raw.githubusercontent.com/delta-io/delta-rs/main/CHANGELOG.md>, cuja entrada mais nova é
  a `rust-v0.32.3`, de 2026-05-19
- <https://api.github.com/repos/delta-io/delta-rs/releases/tags/python-v1.6.5> e a mesma rota com
  `python-v1.6.6`, que o proxy do contêiner recusou

O filtro do dataset do delta-rs sobre as colunas sem mínimo e máximo no log, consultado em
2026-09-25:

- <https://github.com/delta-io/delta-rs/blob/main/python/src/lib.rs>
- <https://raw.githubusercontent.com/delta-io/delta-rs/main/python/src/lib.rs>
  (`filestats_to_expression_next`), e o mesmo caminho nas tags `python-v0.24.0`, `python-v0.25.0`,
  `python-v1.6.4`, `python-v1.6.5` e `python-v1.6.6`
- <https://raw.githubusercontent.com/delta-io/delta-rs/main/python/deltalake/table.py>, e nas tags
  `python-v1.6.4` e `python-v1.6.6`
- <https://raw.githubusercontent.com/delta-io/delta-rs/main/python/deltalake/query.py>
- <https://raw.githubusercontent.com/delta-io/delta-rs/main/crates/core/src/kernel/snapshot/log_data.rs>,
  e na tag `python-v1.6.4`
- <https://raw.githubusercontent.com/delta-io/delta-rs/main/crates/core/src/kernel/snapshot/iterators.rs>
  (`iterators/mod.rs` deu 404), `snapshot/mod.rs` e `kernel/mod.rs`
- <https://raw.githubusercontent.com/delta-io/delta-rs/main/crates/core/src/writer/stats.rs>
- <https://raw.githubusercontent.com/delta-io/delta-rs/main/crates/core/src/table/config.rs>
- <https://raw.githubusercontent.com/delta-io/delta-rs/main/python/tests/test_table_read.py>, e
  `test_stats.py`, `test_writer.py` e `test_writerproperties.py` na mesma pasta
- <https://raw.githubusercontent.com/delta-io/delta-rs/main/CHANGELOG.md>
- <https://raw.githubusercontent.com/delta-io/delta-rs/main/mkdocs.yml>
- <https://raw.githubusercontent.com/delta-io/delta-rs/main/docs/usage/querying-delta-tables.md>, e
  sob `docs/`: `how-delta-lake-works/delta-lake-file-skipping.md`,
  `usage/delta-lake-best-practices.md`, `integrations/delta-lake-polars.md`,
  `integrations/delta-lake-arrow.md`, `integrations/delta-lake-pandas.md`,
  `integrations/delta-lake-datafusion.md` e `api/query.md`
- <https://raw.githubusercontent.com/delta-io/delta/master/PROTOCOL.md> (a seção "Per-file
  Statistics")
- <https://docs.delta.io/latest/table-properties.html>
- <https://docs.delta.io/latest/optimizations-oss.html>
- <https://github.com/delta-io/delta-rs/issues/3032>, também pela API do GitHub
  (<https://api.github.com/repos/delta-io/delta-rs/issues/3032> e o `/comments` dela)
- As issues `1496`, `2427`, `3014`, `3173`, `3201`, `3905`, `4398`, `4490` e `4659` de
  <https://github.com/delta-io/delta-rs/issues>
- <https://github.com/delta-io/delta-rs/pull/3210>; o `.diff` dela em
  <https://patch-diff.githubusercontent.com/raw/delta-io/delta-rs/pull/3210.diff> respondeu "The
  server is unavailable at this time."
- As PRs `1520` (e o `/files` dela), `3288`, `4499`, `4666`, `4675`, `4690`, `4693` e `4757` de
  <https://github.com/delta-io/delta-rs/pulls>
- As versões <https://github.com/delta-io/delta-rs/releases/tag/python-v1.6.6>, `python-v1.6.5`,
  `python-v1.6.4` e `python-v0.25.0`
- O modelo de issue do delta-rs,
  <https://raw.githubusercontent.com/delta-io/delta-rs/main/.github/ISSUE_TEMPLATE/bug_report.yml> e
  `config.yml` na mesma pasta (`bug_report.md` deu 404)
- As buscas de issues do GitHub em `delta-io/delta-rs` por `to_pyarrow_dataset stats`,
  `partition_expression`, `pyarrow filter` entre as abertas, `filestats_to_expression`,
  `to_pyarrow_dataset` entre as abertas e `"is_not_null" OR "is not null" pyarrow`, e a de PRs por
  `dataset_partitions`
- A lista de issues e PRs do delta-rs pela API do ecosyste.ms,
  <https://issues.ecosyste.ms/api/v1/hosts/GitHub/repositories/delta-io%2Fdelta-rs/issues?per_page=100&page=1>,
  das páginas 1 a 56, e a issue 3032 por
  <https://issues.ecosyste.ms/api/v1/hosts/GitHub/repositories/delta-io%2Fdelta-rs/issues/3032>
- <https://docs.pola.rs/api/python/stable/reference/api/polars.scan_delta.html>
- <https://raw.githubusercontent.com/pola-rs/polars/main/py-polars/src/polars/io/delta/__init__.py>,
  e `functions.py`, `_dataset.py` e `_utils.py` na mesma pasta; `py-polars/polars/io/delta.py`,
  `py-polars/src/polars/io/delta.py` e `py-polars/polars/io/delta/__init__.py` deram 404
- <https://raw.githubusercontent.com/pola-rs/polars/main/crates/polars-plan/src/plans/aexpr/predicates/skip_batches.rs>
- A busca na web por `filestats_to_expression`, pelo filtro do `to_pyarrow_dataset` sem linhas com
  as estatísticas ausentes, pela issue #3032, pela garantia com `null[double]`, pelas propriedades
  `dataSkippingNumIndexedCols` e `dataSkippingStatsColumns`, pelo `SimplifyWithGuarantee` do Arrow
  com o literal nulo e pelo `statistics_enabled="NONE"` com o `to_pyarrow_table` vazio

## DuckLake

- <https://ducklake.select/docs/stable/>
- <https://ducklake.select/docs/stable/duckdb/introduction>
- <https://ducklake.select/docs/stable/duckdb/usage/connecting>
- <https://ducklake.select/docs/stable/duckdb/usage/choosing_a_catalog_database>
- <https://ducklake.select/docs/stable/duckdb/usage/schema_evolution>
- <https://ducklake.select/docs/stable/duckdb/metadata/adding_files>
- <https://ducklake.select/docs/stable/duckdb/advanced_features/partitioning>
- <https://ducklake.select/docs/stable/duckdb/advanced_features/conflict_resolution>
- <https://ducklake.select/docs/stable/duckdb/advanced_features/data_inlining>
- <https://ducklake.select/docs/stable/specification/tables/ducklake_data_file>
- <https://ducklake.select/2026/04/13/ducklake-10/>
- <https://github.com/duckdb/ducklake/pull/175>
- <https://github.com/duckdb/ducklake/discussions/519>
- <https://motherduck.com/blog/announcing-ducklake-1-0-on-motherduck/>
- <https://dlthub.com/docs/dlt-ecosystem/destinations/ducklake>

Manifesto do DuckLake (transações entre tabelas e schemas como grupos de tabelas), consultado em
2026-09-19:

- <https://ducklake.select/manifesto/>

## Hudi

- <https://github.com/apache/hudi-rs/blob/main/README.md>

## Apache Hive

Leitura de Parquet por nome e a propriedade `parquet.column.index.access`, consultada em 2026-09-19:

- <https://cwiki.apache.org/confluence/display/Hive/Parquet>

## SQLGlot, SQLMesh, dbt, Ibis e dlt

- <https://github.com/tobymao/sqlglot/blob/main/README.md>
- <https://sqlmesh.readthedocs.io/en/stable/guides/configuration/>
- <https://sqlmesh.readthedocs.io/en/stable/reference/configuration/>
- <https://sqlmesh.readthedocs.io/en/stable/integrations/engines/redshift/>
- <https://sqlmesh.readthedocs.io/en/stable/concepts/models/python_models/>
- <https://github.com/TobikoData/sqlmesh/blob/main/sqlmesh/core/engine_adapter/base.py>
- <https://github.com/TobikoData/sqlmesh/blob/main/sqlmesh/core/engine_adapter/redshift.py>
- <https://github.com/duckdb/dbt-duckdb/blob/master/README.md>
- <https://docs.getdbt.com/docs/build/python-models>
- <https://ibis-project.org/support_matrix>
- <https://dlthub.com/docs/dlt-ecosystem/destinations/redshift>
- <https://datafusion.apache.org/python/>

## Contrato de dados, auditoria e migração

- <https://pandera.readthedocs.io/en/stable/>
- <https://github.com/datacontract/datacontract-cli/blob/main/README.md>
- <https://docs.soda.io/reference/data-source-reference-for-soda-core>
- <https://github.com/sodadata/soda-core>
- <https://atlasgo.io/features>
- <https://atlasgo.io/guides/redshift>
- <https://atlasgo.io/blog/2024/05/21/atlas-v-0-23>

## Rust e PyO3

- <https://docs.astral.sh/uv/concepts/build-backend/>
- <https://docs.astral.sh/uv/concepts/projects/init/>
- <https://docs.rs/pyo3-arrow/latest/pyo3_arrow/>
- <https://docs.rs/object_store/latest/object_store/enum.PutMode.html>
- <https://docs.rs/object_store/latest/object_store/aws/enum.S3ConditionalPut.html>
- <https://docs.rs/object_store/latest/object_store/aws/struct.AmazonS3Builder.html>
- <https://docs.rs/object_store/latest/object_store/aws/enum.AmazonS3ConfigKey.html>
- <https://docs.rs/object_store/latest/src/object_store/aws/builder.rs.html>

## Fontes

Redshift:

- [Table constraints](https://docs.aws.amazon.com/redshift/latest/dg/t_Defining_constraints.html)
- [Define primary key and foreign key constraints](https://docs.aws.amazon.com/redshift/latest/dg/c_best-practices-defining-constraints.html)
- [Query performance tuning](https://docs.aws.amazon.com/redshift/latest/dg/c-optimizing-query-performance.html)
- [Best practices for loading data](https://docs.aws.amazon.com/redshift/latest/dg/c_loading-data-best-practices.html)
- [COPY from columnar data formats](https://docs.aws.amazon.com/redshift/latest/dg/copy-usage_notes-copy-from-columnar.html)
- [UNLOAD](https://docs.aws.amazon.com/redshift/latest/dg/r_UNLOAD.html)
- [Character types](https://docs.aws.amazon.com/redshift/latest/dg/r_Character_types.html)
- [CREATE TABLE](https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_NEW.html)
- [CREATE SCHEMA](https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_SCHEMA.html)
- [DROP SCHEMA](https://docs.aws.amazon.com/redshift/latest/dg/r_DROP_SCHEMA.html)
- [Names and identifiers](https://docs.aws.amazon.com/redshift/latest/dg/r_names.html)
- [Quotas and limits in Amazon Redshift](https://docs.aws.amazon.com/redshift/latest/mgmt/amazon-redshift-limits.html)
- [Using the Amazon Redshift Data API](https://docs.aws.amazon.com/redshift/latest/mgmt/data-api.html)
- [Storing database credentials in AWS Secrets Manager](https://docs.aws.amazon.com/redshift/latest/mgmt/data-api-secrets.html)
- [Amazon Redshift multi-data warehouse writes through data sharing is now generally available](https://aws.amazon.com/about-aws/whats-new/2024/11/amazon-redshift-multi-data-warehouse-through-data-sharing/)
- [Considerations for data sharing reads and writes in Amazon Redshift](https://docs.aws.amazon.com/redshift/latest/dg/considerations-datashare-reads-writes.html)
- [Supported SQL statements for data sharing writes on consumers](https://docs.aws.amazon.com/redshift/latest/dg/multi-warehouse-writes-sql-statements.html)
- [Unsupported SQL statements for data sharing writes on consumers](https://docs.aws.amazon.com/redshift/latest/dg/multi-warehouse-writes-sql-statements-unsupported.html)
- [USE](https://docs.aws.amazon.com/redshift/latest/dg/r_USE_command.html)
- [SVV_REDSHIFT_DATABASES](https://docs.aws.amazon.com/redshift/latest/dg/r_SVV_REDSHIFT_DATABASES.html)
- [SVV_ALL_SCHEMAS](https://docs.aws.amazon.com/redshift/latest/dg/r_SVV_ALL_SCHEMAS.html)
- [SHOW GRANTS](https://docs.aws.amazon.com/redshift/latest/dg/r_SHOW_GRANTS.html)
- [DEFAULT_IAM_ROLE](https://docs.aws.amazon.com/redshift/latest/dg/r_DEFAULT_IAM_ROLE.html)
- [SVV_USER_INFO](https://docs.aws.amazon.com/redshift/latest/dg/r_SVV_USER_INFO.html)
- [AWS CLI: redshift-serverless get-namespace](https://docs.aws.amazon.com/cli/latest/reference/redshift-serverless/get-namespace.html)
- [AWS CLI: redshift-serverless update-namespace](https://docs.aws.amazon.com/cli/latest/reference/redshift-serverless/update-namespace.html)
- [COPY: Authorization parameters](https://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-authorization.html)
- [GRANT: Usage notes](https://docs.aws.amazon.com/redshift/latest/dg/r_GRANT-usage-notes.html)
- [CREATE EXTERNAL SCHEMA](https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_EXTERNAL_SCHEMA.html)
- [Querying AWS Glue IRC federated catalogs with Amazon Redshift](https://docs.aws.amazon.com/redshift/latest/dg/glue-irc-federated-catalogs.html)
- [Isolation levels in Amazon Redshift](https://docs.aws.amazon.com/redshift/latest/dg/c_serial_isolation.html)
- [Amazon Redshift announces Snapshot Isolation as the default for new cluster creates and restores](https://aws.amazon.com/about-aws/whats-new/2024/05/amazon-redshift-snapshot-isolation-provisioned-clusters/)
- [Using Apache Iceberg tables with Amazon Redshift](https://docs.aws.amazon.com/redshift/latest/dg/querying-iceberg.html)
- [Referencing Iceberg tables in Amazon Redshift](https://docs.aws.amazon.com/redshift/latest/dg/referencing-iceberg-tables.html)
- [Querying the AWS Glue Data Catalog](https://docs.aws.amazon.com/redshift/latest/mgmt/query-editor-v2-glue.html)
- [Redshift Spectrum and AWS Lake Formation](https://docs.aws.amazon.com/redshift/latest/dg/spectrum-lake-formation.html)
- [Iceberg: SQL commands](https://docs.aws.amazon.com/redshift/latest/dg/iceberg-writes-sql-syntax.html)
- [Iceberg: Transaction semantics](https://docs.aws.amazon.com/redshift/latest/dg/iceberg-writes-transaction-semantics.html)
- [Iceberg: Altering table definitions](https://docs.aws.amazon.com/redshift/latest/dg/iceberg-alter-table.html)
- [Apache Iceberg v3 features in Amazon Redshift](https://docs.aws.amazon.com/redshift/latest/dg/iceberg-v3-features.html)
- [Amazon Redshift now supports writing to Apache Iceberg tables](https://aws.amazon.com/about-aws/whats-new/2025/11/aws-redshift-iceberg-writes-m1)
- [Amazon Redshift supports UPDATE, DELETE, MERGE for Apache Iceberg tables](https://aws.amazon.com/about-aws/whats-new/2026/04/redshift-update-delete-merge-iceberg-tables/)
- [Amazon Redshift adds ALTER TABLE for Iceberg tables](https://aws.amazon.com/about-aws/whats-new/2026/05/amazon-redshift-alter-table-iceberg/)
- [Amazon Redshift now supports Apache Iceberg v3 tables](https://aws.amazon.com/about-aws/whats-new/2026/08/amazon-redshift-supports-apache-iceberg-v3/)
- [Achieve 2x faster data lake query performance with Apache Iceberg on Amazon Redshift](https://aws.amazon.com/blogs/big-data/achieve-2x-faster-data-lake-query-performance-with-apache-iceberg-on-amazon-redshift/)
- [CREATE EXTERNAL TABLE](https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_EXTERNAL_TABLE.html)

DuckDB:

- [Schema performance: constraints](https://duckdb.org/docs/lts/guides/performance/schema#constraints)
- [Indexes](https://duckdb.org/docs/current/sql/indexes.html)
- [Text types](https://duckdb.org/docs/current/sql/data_types/text.html)
- [Parquet tips](https://duckdb.org/docs/current/data/parquet/tips.html)
- [Reading and Writing Parquet Files](https://duckdb.org/docs/current/data/parquet/overview.html)
- [COPY Statement](https://duckdb.org/docs/current/sql/statements/copy.html)
- [Import from Apache Arrow](https://duckdb.org/docs/current/guides/python/import_arrow.html)
- [Concurrency](https://duckdb.org/docs/current/connect/concurrency.html)
- [Iceberg extension](https://duckdb.org/docs/current/core_extensions/iceberg/overview.html)
- [Iceberg catalogs](https://duckdb.org/docs/current/core_extensions/iceberg/catalogs.html)
- [Writing to Iceberg](https://duckdb.org/docs/current/core_extensions/iceberg/writing_to_iceberg.html)
- [Iceberg Options](https://duckdb.org/docs/current/core_extensions/iceberg/iceberg_options.html)
- [Writes in DuckDB-Iceberg](https://duckdb.org/2025/11/28/iceberg-writes-in-duckdb.html)
- [New DuckDB-Iceberg Features in v1.5.3](https://duckdb.org/2026/05/29/new-iceberg-features.html)
- Issues da extensão `iceberg`: [#660](https://github.com/duckdb/duckdb-iceberg/issues/660),
  [#786](https://github.com/duckdb/duckdb-iceberg/issues/786),
  [#995](https://github.com/duckdb/duckdb-iceberg/issues/995),
  [#1162](https://github.com/duckdb/duckdb-iceberg/issues/1162),
  [#1290](https://github.com/duckdb/duckdb-iceberg/issues/1290),
  [#1369](https://github.com/duckdb/duckdb-iceberg/issues/1369),
  [#454](https://github.com/duckdb/duckdb-iceberg/issues/454),
  [#810](https://github.com/duckdb/duckdb-iceberg/issues/810)
- [DuckLake](https://ducklake.select/)

PyIceberg:

- [API](https://py.iceberg.apache.org/api/)
- [Configuration](https://py.iceberg.apache.org/configuration/)
- [Código-fonte na versão 0.12.0](https://github.com/apache/iceberg-python/tree/pyiceberg-0.12.0)
- [PR #3320: Add commit retry and concurrency validation for writes](https://github.com/apache/iceberg-python/pull/3320)
- [Issue #3008: Retry Behavior for SigV4Adapter in REST Catalog](https://github.com/apache/iceberg-python/issues/3008)
- [Issue #1551: Support writing V3 tables](https://github.com/apache/iceberg-python/issues/1551)

AWS Glue, Athena e Lake Formation:

- [Optimizing Iceberg tables](https://docs.aws.amazon.com/glue/latest/dg/table-optimizers.html)
- [Compaction optimization](https://docs.aws.amazon.com/glue/latest/dg/compaction-management.html)
- [Snapshot retention optimization](https://docs.aws.amazon.com/glue/latest/dg/snapshot-retention-management.html)
- [Deleting orphan files](https://docs.aws.amazon.com/glue/latest/dg/orphan-file-deletion.html)
- [Table optimization prerequisites](https://docs.aws.amazon.com/glue/latest/dg/optimization-prerequisites.html)
- [Connecting to the Data Catalog using AWS Glue Iceberg REST endpoint](https://docs.aws.amazon.com/glue/latest/dg/connect-glu-iceberg-rest.html)
- [Considerations and limitations when using AWS Glue Iceberg REST Catalog APIs](https://docs.aws.amazon.com/glue/latest/dg/limitation-glue-iceberg-rest-api.html)
- [UpdatePartition](https://docs.aws.amazon.com/glue/latest/webapi/API_UpdatePartition.html)
- [BatchUpdatePartition](https://docs.aws.amazon.com/glue/latest/webapi/API_BatchUpdatePartition.html)
- [AWS Glue endpoints and quotas](https://docs.aws.amazon.com/general/latest/gr/glue.html)
- [AWS Glue Pricing](https://aws.amazon.com/glue/pricing/)
- [Manage concurrent write conflicts in Apache Iceberg on the AWS Glue Data Catalog](https://aws.amazon.com/blogs/big-data/manage-concurrent-write-conflicts-in-apache-iceberg-on-the-aws-glue-data-catalog/)
- [Query Apache Iceberg tables](https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg.html)
- [Update Iceberg table data](https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-updating-iceberg-table-data.html)
- [OPTIMIZE](https://docs.aws.amazon.com/athena/latest/ug/optimize-statement.html)
- [VACUUM](https://docs.aws.amazon.com/athena/latest/ug/vacuum-statement.html)
- [Creating Apache Iceberg tables](https://docs.aws.amazon.com/lake-formation/latest/dg/creating-iceberg-tables.html)
- [Underlying data access control](https://docs.aws.amazon.com/lake-formation/latest/dg/access-control-underlying-data.html)
- [Application integration for full table access](https://docs.aws.amazon.com/lake-formation/latest/dg/full-table-credential-vending.html)
- [Hybrid access mode](https://docs.aws.amazon.com/lake-formation/latest/dg/hybrid-access-mode.html)
- [AWS CLI: lakeformation](https://docs.aws.amazon.com/cli/latest/reference/lakeformation/index.html)

SageMaker Unified Studio:

- [Code spaces in Identity Center domains](https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/code-spaces-idc.html)
- [Manage Tooling blueprint parameters](https://docs.aws.amazon.com/sagemaker-unified-studio/latest/adminguide/manage-tooling-blueprint.html)
- [Using the JupyterLab IDE in Amazon SageMaker Unified Studio](https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/jupyterlab.html)
- [Schedule and automate notebook runs](https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/notebooks-schedule-runs.html)
- [Using workflows in Amazon SageMaker Unified Studio](https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/workflow-orchestration.html)
- [Access control patterns](https://docs.aws.amazon.com/sagemaker-unified-studio/latest/adminguide/security-accesss-control-patterns.html)
- [Configure Lake Formation permissions for Amazon SageMaker Unified Studio](https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/lake-formation-permissions-for-amazon-sagemaker-unified-studio.html)
- [Gaining access to Amazon Redshift resources](https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/compute-prerequisite-redshift.html)
- [Connecting to Amazon Redshift](https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/connecting-amazon-redshift.html)
- [Amazon SageMaker Unified Studio terminology and concepts](https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/concepts.html)
- [S3 Path](https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/s3-path.html)
- [Using Amazon SageMaker Unified Studio Library for Python](https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/python-library.html)
- [Connections](https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/connections.html)
- [SageMakerStudioProjectUserRolePolicy](https://docs.aws.amazon.com/aws-managed-policy/latest/reference/SageMakerStudioProjectUserRolePolicy.html)
- [Connect, share, and query where your data sits using Amazon SageMaker Unified Studio](https://aws.amazon.com/blogs/big-data/connect-share-and-query-where-your-data-sits-using-amazon-sagemaker-unified-studio/)

Python:

- [duckdb_engine](https://github.com/Mause/duckdb_engine)
- [sqlalchemy-redshift](https://pypi.org/project/sqlalchemy-redshift/)
- [Amazon Redshift Python connector](https://github.com/aws/amazon-redshift-python-driver)
- [awswrangler.redshift.copy](https://aws-sdk-pandas.readthedocs.io/en/stable/stubs/awswrangler.redshift.copy.html)
- [ADBC Driver for Amazon Redshift](https://adbc-drivers.org/drivers/redshift/)
- [etl-cookbook-tutorial](https://github.com/felipenoris/etl-cookbook-tutorial)

## Repositórios consultados

- <https://github.com/felipenoris/AWS-DataScience> (2026-09-19): a pasta `aws/` (`INDEX.md`, `awslib/report.py`, `awslib/context.py`, `studio.py`, `probes/README.md`), o formato dos scripts de leitura que `probes/` deste repositório reproduz: só leitura, seções numeradas, chamada ecoada acima do resultado, identificadores `NOME=valor`, checagens `pass`/`fail`/`note` e a seção final de chamadas que falharam.
