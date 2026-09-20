# Etapa 3: `storage` e `delta`

A entrega e o critério de aceite desta etapa estão na tabela de etapas de [`PLAN.md`](PLAN.md), que
também fixa as decisões, as regras que toda etapa obedece e a ordem do trabalho.

`serialize_db.storage` esconde a diferença entre a pasta local e o S3; é a divisão de
`tests/conftest.py` (`LocalLocation`, `S3Location`) levada à biblioteca.

| Primitiva | O que faz |
| --- | --- |
| `Storage.for_uri(uri)` | `LocalStorage` para um caminho ou `file://`, `S3Storage` para `s3://bucket/prefixo`. |
| `join(*parts)`, `exists(path)`, `list_files(prefix, suffix)`, `delete(paths)` | Caminhos relativos à raiz; a listagem exclui `_delta_log/`. |
| `read_text(path)`, `write_text(path, text, if_match=None, if_none_match=False)` | Escrita condicional: `IfMatch` e `IfNoneMatch` no S3 (412 vira `ConflictError`); `O_EXCL` e `os.replace` na pasta local. É a escrita de `_serialize_db/snapshots.json`. |
| `copy(source, destination)` | `CopyObject` no S3, `shutil.copy2` na pasta local; a exportação sem ler dados. |
| `storage_options()` | As opções do delta-rs: região, `AWS_ENDPOINT_URL`, `max_retries`, `retry_timeout`, `timeout`, as chaves de SSE quando configuradas, e as credenciais do `boto3` só na reserva; resolvidas a cada chamada, nunca guardadas, porque as credenciais do contêiner duram cerca de uma hora. |
| `duckdb_setup(connection)` | `LOAD httpfs; LOAD delta; LOAD aws` e o secret `credential_chain` com `REGION` e `ENDPOINT`; só `LOAD delta` na pasta local. |
| `prepare_environment()` | Exporta `NO_PROXY` a partir de `no_proxy` quando a maiúscula está ausente ou vazia, copia a região entre `AWS_REGION` e `AWS_DEFAULT_REGION` nos dois sentidos, respeita `AWS_ENDPOINT_URL`; devolve o que mudou, para o log. Chamada por `Database`. |

`serialize_db.delta` é a camada de tabela; `uri` é a pasta da tabela, `table` o `Table` do modelo,
`data` uma `pa.Table` ou um `RecordBatchReader`.

| Primitiva | O que faz |
| --- | --- |
| `create_table(uri, table)` | `DeltaTable.create(mode="ignore")` com `delta_schema`, `partition_by`, nome, descrição e as propriedades `delta.logRetentionDuration = interval 3650 days` e `delta.deletedFileRetentionDuration = interval 400 days`; sem vetores de exclusão nem column mapping. |
| `open(uri, version=None)` | A `DeltaTable` numa versão; a execução abre cada tabela uma vez e guarda a versão. |
| `max_key(dt, column)` | O maior valor de `column` na versão carregada: o máximo de `max.<coluna>` de `get_add_actions(flatten=True)`, sem ler dados, ou a varredura da coluna quando um arquivo não tem a estatística; 0 na tabela vazia. O início de `run.next_ids`. |
| `commit_metadata(execution_id, input_versions, snapshot=None)` | O dicionário de `CommitProperties(custom_metadata=...)`: `serialize_db_execution_id`, `serialize_db_input_versions` e `serialize_db_snapshot`. |
| `publish_month(uri, month, data, metadata)` | `write_deltalake(mode="overwrite", predicate="mes = '<mes>'")` de `data` já passado por `cast`; `CommitFailedError` sobe como `ExecutionConflict`. |
| `register_files(uri, files, months, metadata)` | `create_write_transaction(mode="overwrite", partition_filters=...)` com uma `AddAction` por arquivo: caminho relativo à pasta da tabela, tamanho, valores de partição e estatísticas do `RETURN_STATS` do DuckDB ou do rodapé Parquet. |
| `schema_diff(table, dt)` | O `SchemaDiff` entre `arrow_schema(table)` e `dt.schema()`: coluna nova anulável, `NOT NULL` relaxado e `CHECK` são aditivos; coluna `NOT NULL` nova em tabela com dados, renomeação, remoção e mudança de tipo são destrutivos. |
| `reconcile(uri, table)` | Aplica o diff aditivo (`add_columns`, `drop_column_not_null`, `add_constraint`) e recusa o destrutivo com a mensagem que aponta `rewrite`. |
| `rewrite(uri, table)` | A tabela inteira com o esquema do contrato num único commit e sem predicado: `COPY ... PARTITION_BY (mes) ... RETURN_STATS` do DuckDB a partir de `delta_scan` mais `create_write_transaction(mode="overwrite", schema=...)`, com memória constante. |
| `copy_manifest(uri, version, months, destination)` | O manifesto do `COPY` do Redshift (`url` e `meta.content_length` de `get_add_actions()`), gravado sob `publicacao/`. |
| `version_diff(uri, published, current)` | Os meses com ações `add` entre as duas versões. |
| `snapshot(root, name, versions)` | A entrada `{name: versions}` em `_serialize_db/snapshots.json`, gravada com `write_text(if_match=...)`. |
| `vacuum_keeping_snapshots(uri, control, retention_hours=9600, apply=False)` | `vacuum` com `keep_versions` das versões do arquivo de controle; lista por padrão e apaga com `apply=True`. |
| `compact(uri, months)` | `optimize.compact` dos meses com arquivos pequenos, antes de um snapshot. |
| `deep_copy(uri, version, destination)` | Tabela nova na versão 0 com os dados de uma versão, para a pasta de arquivo. |
| `export_snapshot(uri, destination, version=None, mode="copy")` | Pastas `mes=.../` sem o log: `copy` copia os arquivos que o log lista; `rewrite` reescreve pelo `COPY` particionado do DuckDB. |

Testes: `tests/test_storage.py` e `tests/test_delta.py` sob a raiz local, com os mesmos casos no
bucket por `-m s3`: substituição do mês e idempotência, conflito entre dois escritores,
reconciliação aditiva e recusa da destrutiva, `rewrite` num commit sem predicado com a versão
anterior legível, `keep_versions`, `export_snapshot` nos dois modos, realocação da pasta e a escrita
condicional do arquivo de controle. Provas de conceito: `test_stdlib.py` (`test_storage_uris`,
`test_exclusive_create_atomic_replace_and_fingerprint`, `test_json_control_file_and_commit_metadata`,
`test_group_log_actions_by_month`, `test_prepare_environment`), `test_s3.py` (`test_conditional_put`,
`test_boto3_list_copy_delete`, `test_delta_rs_storage_options_fallback`), `test_local.py`
(`test_commit_is_atomic_on_disk`, `test_folder_relocates`) e `test_deltalake.py` inteiro: criação
idempotente, predicado e nulidade, evolução com `drop_column_not_null` (recebe o nome da coluna),
`restore`, `AddAction`, `vacuum`, `version_diff`, compactação e checkpoint, exportação por cópia e a
reescrita pelo `COPY ... APPEND true, FILENAME_PATTERN, RETURN_STATS` do DuckDB registrada num
commit `overwrite` com esquema novo e estatísticas tipadas, que o DuckDB usa para podar;
`test_parallel.py` (quatro tabelas lidas em paralelo, escritas em paralelo por tabela e por mês da
mesma tabela com o conflito no mesmo mês, e `max_key` pelas estatísticas com a varredura de reserva).
