# Etapa 3: `storage` e `delta`

A entrega e o critério de aceite desta etapa estão na tabela de etapas de [`PLAN.md`](PLAN.md), que
também fixa as decisões, as regras que toda etapa obedece e a ordem do trabalho.

`serialize_db.storage` esconde a diferença entre a pasta local e o S3 pelo sistema de arquivos do
PyArrow (`pyarrow.fs`), que lista, lê, copia e apaga nos dois armazenamentos e é o que
`scripts/migrate_parquet_to_delta.py` (`Location`) e `probes/parquet_source.py` usaram no ambiente
alvo (decisão do usuário de 2026-09-22). `Storage` é um `dataclass` com a URI, o sistema de
arquivos e o caminho nele, sem uma classe por armazenamento; só a escrita condicional do arquivo de
controle tem um ramo por armazenamento. Toda primitiva de `serialize_db.delta` que toca o
armazenamento recebe o `Storage`, que resolve `storage_options()` a cada chamada, e a URI da pasta
da tabela, que `relative` leva ao caminho relativo à raiz.

| Primitiva | O que faz |
| --- | --- |
| `Storage.for_uri(uri)` | `pafs.S3FileSystem` com a região de `AWS_REGION` ou `AWS_DEFAULT_REGION` e o `endpoint_override` de `AWS_ENDPOINT_URL` para `s3://bucket/prefixo`, sem rede na construção; `pafs.LocalFileSystem` com o caminho resolvido para um caminho ou `file://`. |
| `join(*parts)`, `relative(uri)`, `uri_of(path)` | `join` monta com `/` um caminho relativo à raiz, sem barras nas pontas; `relative` leva uma URI sob a raiz ao caminho relativo e recusa a de fora com `ValueError`; `uri_of` faz a volta. |
| `exists(path)`, `size(path)`, `list_files(prefix, suffix)`, `delete(paths)`, `ensure_folder(path)`, `open_input_file(path)` | Caminhos relativos à raiz, pelo `get_file_info`, pelo `delete_file` e pelo `open_input_file` do sistema de arquivos; a listagem desce as pastas e exclui `_delta_log/`; `delete` de um caminho ausente não é erro; `ensure_folder` cria a pasta local que o `COPY` do DuckDB para um arquivo não cria, e no S3 não faz nada. |
| `read_text(path)`, `create_text(path, text)`, `write_text(path, text, if_match=None)` | Escrita condicional, o único ramo por armazenamento: `put_object` do `boto3` com `IfNoneMatch` (`create_text`) ou `IfMatch` (`write_text`) no S3 (412 vira `ConflictError`), porque o `pyarrow.fs` não tem a condição nem devolve a etag; `O_EXCL` e `os.replace` na pasta local. É a escrita de `_serialize_db/snapshots.json` e do manifesto, e a leitura do log por `version_diff`; o arquivo ausente é `FileNotFoundError` nos dois armazenamentos. |
| `copy(source, destination)` | Na pasta local, `copy_file`; no S3, a transferência gerenciada do `boto3`, `CopyObject` até 8 MiB e `UploadPartCopy` em partes de 8 MiB acima, com a repetição por parte, porque o `CopyObject` único do `copy_file` é abandonado pelo SDK da AWS depois de 3 s sem resposta (leitura de 2026-09-24, [`POC.md`](POC.md)); a exportação e o arquivo sem ler dados. |
| `storage_options()` | As opções do delta-rs: região, `AWS_ENDPOINT_URL`, `max_retries` 3 e `retry_timeout` 10 s, e as chaves de SSE das variáveis do object_store (`AWS_SERVER_SIDE_ENCRYPTION`, `AWS_SSE_KMS_KEY_ID`, `AWS_SSE_BUCKET_KEY_ENABLED`) quando configuradas; vazias na pasta local; nunca credenciais (decisão do usuário de 2026-09-22). A cadeia padrão do delta-rs as resolve e as renova sozinha no `DeltaTable` que a execução guarda, enquanto um trio congelado expiraria em cerca de uma hora e circularia num dicionário que um log ou uma exceção imprime. Resolvidas a cada chamada, nunca guardadas. |
| `duckdb_connect(database=":memory:", config=None)` | A conexão do DuckDB com `extension_directory` de `SERIALIZE_DB_DUCKDB_EXTENSIONS`, ou de `.duckdb/` ao lado do ambiente virtual (a pasta que `prepare_offline.sh` cria), `autoinstall_known_extensions` e `autoload_known_extensions` desligados, as opções de `config` e `duckdb_setup` aplicado; é a conexão de `rewrite`, `read_back` e `export_snapshot`, e a do motor da [etapa 4](PLAN-STAGE-4.md). |
| `duckdb_setup(connection)` | `LOAD httpfs; LOAD delta` e o secret `serialize_db_s3` com `KEY_ID`, `SECRET` e `SESSION_TOKEN` da credencial que a cadeia do `boto3` resolve naquele momento, passados como parâmetros do comando, fora do texto que o erro de sintaxe do DuckDB repete (decisão do usuário de 2026-09-25: o secret guarda a chave até ser recriado, e o motor DuckDB o recria quando a chave troca) e `REGION` e, com `AWS_ENDPOINT_URL`, `ENDPOINT` sem o esquema, `URL_STYLE 'path'` e, num endpoint `http`, `USE_SSL false`; só `LOAD delta` na pasta local; sem credencial na cadeia, `botocore.exceptions.NoCredentialsError`. Aplica `http_proxy`, `http_proxy_username` e `http_proxy_password` a partir de `HTTP_PROXY`, `username` e `password`, como `probelib.duckdb_proxy` faz nos probes: o DuckDB recusa o endereço com as credenciais embutidas, e o erro atinge o acesso ao S3, não só o download de extensão ([`POC.md`](POC.md)). |
| `prepare_environment()` | Exporta `NO_PROXY` a partir de `no_proxy` quando a maiúscula está ausente ou vazia, copia a região entre `AWS_REGION` e `AWS_DEFAULT_REGION` nos dois sentidos, respeita `AWS_ENDPOINT_URL`; devolve o que mudou, para o log. Chamada por `Database`. |

`serialize_db.delta` é a camada de tabela; `uri` é a pasta da tabela, `table` o `Table` do modelo,
`data` uma `pa.Table`, um `RecordBatchReader` ou um objeto com `__arrow_c_stream__`, como o
`BatchStream` de um motor. A coluna de partição sai de `table_options(table)`, nunca de um argumento
solto, e `value` é o valor de uma partição, `None` numa tabela sem partição.

| Primitiva | O que faz |
| --- | --- |
| `create_table(uri, table, storage)` | `DeltaTable.create(mode="ignore")` com `delta_schema`, `partition_by`, o nome da tabela, o comentário da tabela em `description` (decisão do usuário de 2026-09-22) e as propriedades `delta.logRetentionDuration = interval 3650 days` e `delta.deletedFileRetentionDuration = interval 400 days`; sem vetores de exclusão nem column mapping. |
| `open_table(uri, storage, version=None)` | A `DeltaTable` numa versão; a execução abre cada tabela uma vez e guarda a versão. O nome não é `open`, que sombrearia a função embutida dentro do módulo. |
| `table_exists(uri, storage)` | `DeltaTable.is_deltatable`: a pasta sem `_delta_log/` não é tabela; é o que `Execution.__enter__` consulta. |
| `max_key(dt, column)` | O maior valor de `column` na versão carregada: o máximo de `max.<coluna>` de `get_add_actions(flatten=True)`, sem ler dados, ou a varredura da coluna quando um arquivo não tem a estatística; 0 na tabela vazia. O início de `run.next_ids`, e por isso a estatística registrada é verdadeira ou omitida (seção "As conferências do registro de arquivos"): a omitida cai na varredura, a falsa daria chaves repetidas. |
| `commit_metadata(execution_id, input_versions, snapshot=None)` | O dicionário de `CommitProperties(custom_metadata=...)`: `serialize_db_execution_id`, `serialize_db_input_versions` e `serialize_db_snapshot`. |
| `publish_partition(uri, table, value, data, metadata, storage, columns_without_min_max=())` | `write_deltalake(dt, data, mode="overwrite", predicate="<coluna de partição> = '<valor>'")` de `data` já passado por `cast`, pelo objeto `DeltaTable`, e devolve `dt.version()`, a versão do próprio commit; as colunas de `columns_without_min_max` recebem `ColumnProperties(statistics_enabled="NONE")` no `writer_properties` e saem sem mínimo e máximo no rodapé e no log, pela regra do `Double` não finito da seção "As conferências do registro de arquivos"; `value=None` numa tabela sem partição substitui a tabela inteira; `CommitFailedError` sobe como `ExecutionConflict`. |
| `register_files(uri, table, files, value, metadata, storage, expected_rows=None, columns_without_min_max=())` | Arquivos que outro escritor gravou dentro da pasta da tabela entram no log por `create_write_transaction(mode="overwrite", partition_filters=...)`, uma `AddAction` por arquivo: caminho relativo à pasta da tabela, tamanho, valores de partição e estatísticas do `RETURN_STATS` do DuckDB ou do rodapé Parquet. O `create_write_transaction` grava a ação como a recebe, e os leitores obedecem à ação, não ao arquivo ([`POC.md`](POC.md)); a primitiva faz as conferências da seção "As conferências do registro de arquivos" antes do commit e a releitura depois dele. Os arquivos do `UNLOAD` do Redshift têm mínimo e máximo, menos nas colunas de timestamp, que saem em `INT96` e não carregam estatística: a coluna fica fora de `minValues` e `maxValues` sem falhar o registro ([`redshift.md`](redshift.md)). O `schema.elements` do manifesto verboso listou a coluna de partição, que os arquivos não têm, no `UNLOAD` com `PARTITION BY` (suíte de 2026-09-21); o da [etapa 5](PLAN-STAGE-5.md) grava sem ele e com a coluna fora do `select` (decisão do usuário de 2026-09-23), e a lista esperada da conferência é a do `select`, que a próxima execução da suíte confere. O segundo registro da mesma partição a partir da mesma versão é `CommitFailedError` (leitura de 2026-09-23), que sobe como `ExecutionConflict`, como em `publish_partition`. |
| `read_back(uri, table, value, expected_rows, storage)` | A releitura da versão recém-commitada pelos dois leitores, em conexão DuckDB própria, na partição ou, com `value=None`, na tabela inteira: as linhas iguais à soma de `numRecords` do log e a `expected_rows`, o menor e o maior valor de cada coluna da primeira chave iguais nos dois leitores, e o mínimo e o máximo registrados no log como limites dos lidos; uma diferença volta a versão e levanta `RegistrationRefused`. |
| `file_from_return_stats(row, table, uri)` | O `RegisteredFile` de uma linha do `RETURN_STATS` do DuckDB, com o caminho relativo à pasta da tabela, o `null_count` de toda coluna e o mínimo e o máximo dos tipos que transcrevem exato, convertidos do texto; protegida, usada por `rewrite` e pelo `export_partition` do motor DuckDB. |
| `schema_diff(table, dt)` | O `SchemaDiff` entre `arrow_schema(table)` e `dt.schema()`: coluna nova anulável, `NOT NULL` relaxado, `CHECK` e comentário divergente são aditivos; coluna `NOT NULL` nova em tabela com dados, renomeação, remoção e mudança de tipo são destrutivos. |
| `reconcile(uri, table, storage)` | Aplica o diff aditivo (`add_columns`, `drop_column_not_null`, `add_constraint`, `set_table_description` e `set_column_metadata`) e recusa o destrutivo com a mensagem que aponta `rewrite`. |
| `rewrite(uri, table, storage, expressions=None)` | A tabela inteira com o esquema do contrato num único commit e sem predicado: `COPY ... PARTITION_BY (<coluna de partição>) ... RETURN_STATS` do DuckDB a partir de `delta_scan` mais `create_write_transaction(mode="overwrite", schema=...)`, com memória constante. `expressions` dá, por coluna do contrato, a expressão sobre a versão atual que a preenche: o nome antigo numa renomeação, o valor de uma coluna `NOT NULL` nova (decisão do usuário de 2026-09-22). A conexão DuckDB é aberta aqui e configurada por `storage.duckdb_setup`, sem o motor da [etapa 4](PLAN-STAGE-4.md): `delta` não depende de `engine`. `CommitFailedError` sobe como `ExecutionConflict`. |
| `copy_manifest(uri, version, partitions, destination, storage)` | O manifesto do `COPY` do Redshift (`url` e `meta.content_length` de `get_add_actions()`, `mandatory` verdadeiro), gravado na URI `destination` sob `publicacao/` ou `staging/`; devolve a URI. |
| `version_diff(uri, published, current, table, storage)` | As partições com ações `add` ou `remove` de dados entre as duas versões, lidas do log; a compactação (`dataChange` falso) não conta. Um arquivo do log ausente é `LogUnavailable`, com a instrução de publicar a tabela inteira (decisão do usuário de 2026-09-22). |
| `read_snapshots(storage, environment)`, `snapshot(storage, environment, name, versions)` | O arquivo de controle `_serialize_db/snapshots.json` do ambiente com a impressão digital, `({"snapshots": {}}, None)` quando ele ainda não existe, e a entrada `{name: versions}` gravada nele com `write_text(if_match=...)`, ou `create_text` no primeiro; o nome presente em `snapshots` ou em `archived` é `ValueError`. `archive_snapshot(storage, environment, name)` move a entrada de `snapshots` para a chave irmã `archived` na mesma escrita condicional, e `history(uri, storage)` lista os commits com os metadados da biblioteca ([etapa 9](PLAN-STAGE-9.md)). |
| `vacuum_keeping_snapshots(uri, control, table_name, storage, retention_hours=9600, apply=False, full=False)` | `vacuum` com `keep_versions` das versões do arquivo de controle; lista por padrão e apaga com `apply=True`. |
| `compact(uri, table, partitions, storage)` | `optimize.compact` das partições com arquivos pequenos, antes de um snapshot. A reescrita sai pelo escritor do delta-rs: os arquivos do `UNLOAD` que ela junta perdem o `INT96` e o `FIXED_LEN_BYTE_ARRAY` e ganham estatística em toda coluna (`test_deltalake.py::test_compact_rewrites_files_from_another_writer`). |
| `deep_copy(uri, version, destination, storage)` | Tabela nova na URI `destination` com os arquivos, o esquema (nulidade e comentários inclusive), a partição, o nome, a descrição e as propriedades de uma versão, para a pasta de arquivo: cada arquivo que o log da versão lista copiado por `Storage.copy` para o mesmo caminho relativo e registrado com as estatísticas da própria ação de origem, num commit por partição, e a contagem da cópia conferida pelos dois leitores (decisão do usuário de 2026-09-24, [etapa 9](PLAN-STAGE-9.md)), porque a memória do `write_deltalake` do leitor da tabela inteira cresce com a tabela; a cópia termina numa versão por partição. |
| `export_snapshot(uri, table, destination, storage, version=None, mode="copy")` | Pastas `<coluna de partição>=<valor>/` sem o log na URI `destination`: `copy` copia os arquivos que o log lista; `rewrite` reescreve pelo `COPY` particionado do DuckDB; devolve as URIs gravadas. |

`scripts/migrate_parquet_to_delta.py`, a migração adiantada da [etapa 7](PLAN-STAGE-7.md), já tem
em forma de módulo, testada e executada no ambiente alvo, a metade de `register_files` que monta a
ação: `stat_converter` (os quatro tipos que transcrevem exato), `delta_stats` (o JSON de
estatísticas a partir do `RETURN_STATS`), `copy_partition_file` e `register_partition` (o
`COPY ... RETURN_STATS` na pasta da partição e o `create_write_transaction` da ação). A etapa as
trouxe para `serialize_db.delta` (`_stat_converter`, `file_from_return_stats`, `_action_stats` e
`register_files`); o script mantém as suas cópias até a [etapa 7](PLAN-STAGE-7.md) absorvê-lo.

## As conferências do registro de arquivos

`create_write_transaction` grava a ação como a recebe, e a sondagem de 2026-09-21 ([`POC.md`](POC.md),
`test_deltalake.py::test_create_write_transaction_trusts_path_and_stats` e
`::test_create_write_transaction_trusts_file_schema`) mostrou o que cada campo errado faz: um
caminho inexistente commita, e toda leitura que toca a partição falha até um `restore`; uma
estatística falsa faz o delta-rs, o `delta_scan` e o DataFusion podarem o arquivo que tem as linhas
e devolverem zero sem erro, o `numRecords` falso vira o `count(*)` do DataFusion, e o `max.<coluna>`
falso viraria o `max_key` de `next_ids`; um arquivo sem uma coluna `NOT NULL` commita e lê nulo nos
dois leitores, o que o `write_deltalake` recusa; um valor que não converte para o tipo da coluna
commita e falha quando a coluna é lida. `register_files` repõe a conferência antes do commit, só com
o rodapé de cada arquivo, um GET por arquivo:

1. O caminho é relativo à pasta da tabela, sem `/` inicial, URI nem `..`, e o arquivo existe em
   `uri/path`, o caminho que o leitor resolve, com o tamanho da ação.
2. O esquema do rodapé contra o da tabela, nome a nome: nenhuma coluna do contrato ausente, e o tipo
   físico entre os admitidos para o lógico (`INT96` e `INT64` para `timestamp_ntz`,
   `FIXED_LEN_BYTE_ARRAY`, `INT64` e `INT32` para `decimal`, `INT32` para `long`); a coluna de
   partição fora do arquivo, porque ela vive na ação e o `COPY` posicional do Redshift a leria como
   a coluna seguinte; e as colunas do contrato na ordem dele, pelo mesmo `COPY`. Uma coluna fora do
   contrato passa, porque os leitores a ignoram. Nenhum nulo numa coluna `NOT NULL`, pela contagem
   de nulos de cada grupo de linhas do rodapé: o DuckDB grava toda coluna como `optional`, e o leitor
   devolveria o nulo que o `write_deltalake` recusa.
3. O valor de partição do caminho Hive igual ao de `value`.
4. A soma de `num_records` dos rodapés igual à que `files` declara e a `expected_rows`, quando o
   chamador tem a contagem da fonte.
5. `numRecords` do rodapé; `minValues` e `maxValues` das colunas inteiras, de data, `Double` e
   `String`, omitidos nas demais e nas que o rodapé não traz: no `delta_scan`, uma estatística
   ausente só deixa de podar, e uma errada poda o arquivo certo. O dataset do delta-rs
   (`to_pyarrow_dataset`) perde as linhas do arquivo sem mínimo e máximo de uma coluna em todo
   filtro de valor sobre ela, e com o `nullCount` do registro lê certo o `IS NULL` e o `IS NOT NULL`
   (sonda de 2026-09-25, [`POC.md`](POC.md), [`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md)). Os quatro
   tipos são os que a sondagem de 2026-09-22 mediu transcrevendo exato ([`POC.md`](POC.md),
   decisão do usuário do mesmo dia); `decimal` fica de fora porque o próprio delta-rs grava o
   mínimo e o máximo como número JSON e perde a linha na poda, e `timestamp` porque o valor sai
   truncado em milissegundos. O `Double` transcreve exato só os valores finitos, e as colunas de
   `columns_without_min_max`, as `Double` com valor não finito na partição, ficam sem mínimo e
   máximo (decisão do usuário de 2026-09-23,
   [issue #59](https://github.com/felipenoris/serialize-db/issues/59)). Com `NaN` na coluna, o máximo do
   `RETURN_STATS` fica sem ele, e o `delta_scan ... WHERE valor > 3` perdeu a linha que o DuckDB
   ordena acima de todo número; o infinito entraria no JSON do log como
   `Infinity`, que não é JSON válido; e o `has_nan` só vê o último grupo de linhas do arquivo, por
   isso a lista vem da contagem da auditoria, não do `RETURN_STATS` (leituras de 2026-09-23,
   [`POC.md`](POC.md)). O rodapé do `COPY` do DuckDB já sai sem mínimo e máximo no grupo de linhas
   com `NaN`. O texto não tem exceção: o `RETURN_STATS` trunca o máximo para cima, e omite o texto
   multibyte longo.

A reprovação recusa o commit com o arquivo e a conferência na mensagem, e os arquivos ficam órfãos
na pasta até `vacuum(full=True)`. Depois do commit, `read_back` lê a versão nova pelo delta-rs e
pelo `delta_scan`, `count(*)` e mínimo e máximo da chave por partição, confere as linhas contra a
soma de `numRecords` do log e o mínimo e o máximo registrados da chave como limites dos lidos, e uma
diferença chama `restore(version - 1)` e sobe a mesma exceção. A releitura pega o que as
conferências do rodapé não veem, um leitor que não lê o arquivo como o outro: o `parquet.field.id`
no esquema Delta fez o `delta_scan` ler toda coluna como nula com a contagem certa (2026-09-21,
[`POC.md`](POC.md)), e o mínimo e o máximo da chave o acusariam. Uma versão entra num snapshot ou em
`publish_redshift` só depois da releitura.

Testes: `tests/test_storage.py` e `tests/test_delta.py` sob a raiz local, com os mesmos casos no
bucket por `-m s3`, e os de `test_storage.py` que não gravam sem variável: substituição da partição
e idempotência, conflito entre dois escritores, as recusas do registro e a releitura que desfaz o
commit, reconciliação aditiva e recusa da destrutiva, `rewrite` num commit sem predicado com a
versão anterior legível, `keep_versions`, `export_snapshot` nos dois modos, o manifesto, a cópia
profunda, realocação da pasta e a escrita condicional do arquivo de controle. Provas de conceito:
`test_stdlib.py` (`test_storage_uris`, `test_exclusive_create_atomic_replace_and_fingerprint`,
`test_json_control_file_and_commit_metadata`, `test_group_log_actions_by_partition`,
`test_prepare_environment`), `test_s3.py` (`test_conditional_put`, `test_boto3_list_copy_delete`, e
`test_delta_rs_storage_options_fallback`, que mede a forma das credenciais em `storage_options` sem
que a biblioteca a use), `test_local.py` (`test_commit_is_atomic_on_disk`, `test_folder_relocates`)
e `test_deltalake.py` inteiro: criação idempotente, predicado e nulidade, evolução com
`drop_column_not_null` (recebe o nome da coluna), `restore`, `AddAction`, o que
`create_write_transaction` não confere (caminho, estatística, esquema do arquivo) e o `overwrite`
com `partition_filters`, a compactação que normaliza arquivos de outro escritor, `vacuum`,
`version_diff` pelas ações `add` e `remove` com `dataChange` do log, compactação e checkpoint,
e exportação por cópia; `test_parallel.py` (quatro tabelas lidas em paralelo, escritas em paralelo
por tabela e por mês da mesma tabela com o conflito no mesmo mês);
`tests/test_migrate_parquet_to_delta.py` (o registro com as estatísticas dos quatro tipos). A
reescrita pelo `COPY ... APPEND true, FILENAME_PATTERN, RETURN_STATS` do DuckDB e `max_key` pelas
estatísticas com a varredura de reserva são os casos de `tests/test_delta.py`.

## Estratégia de implementação

- **A forma do código** segue o `CLAUDE.md`, não a dos rascunhos de 2026-09-21, que eram provas
  densas: laços explícitos no lugar das compreensões com condição composta, uma função por
  conferência de `register_files` (`_check_relative_path`, `_check_file_size`,
  `_check_footer_schema`, `_check_not_null`, `_check_partition_path`, `_check_file_rows`,
  `_check_row_counts`), o
  `SchemaDiff` montado por laço e não pelas
  expressões condicionais que escolhem a lista e o valor ao mesmo tempo, dicionários de ação
  montados em variáveis nomeadas, e a versão lida do objeto `DeltaTable` que escreveu, nunca de
  `DeltaTable(uri).version()`, que devolve o último commit do log, talvez de outro escritor
  (leitura de 2026-09-22, [`POC.md`](POC.md)).
- **`Storage.for_uri`** constrói o `pafs.S3FileSystem` para `s3://` com a região de `AWS_REGION` ou
  `AWS_DEFAULT_REGION`, a mesma que `storage_options` passa ao delta-rs, e o `endpoint_override` de
  `AWS_ENDPOINT_URL`, e para um caminho ou `file://` o `pafs.LocalFileSystem()` com o caminho
  resolvido. O `pafs.FileSystem.from_uri(uri)` de `open_location`, no script, consulta a região do
  bucket na rede quando a URI não a traz (0,49 s num ambiente despido, leitura de 2026-09-23,
  [`POC.md`](POC.md)), e `test_storage_for_uri` roda sem rede na esteira do GitHub. Os caminhos
  das primitivas são relativos à raiz e `join` os monta com `/`; `list_files` é
  `get_file_info(FileSelector(prefixo, recursive=True))`,
  `delete` é `delete_file`, `copy` é `copy_file` na pasta local e a transferência gerenciada do
  `boto3` no S3, e os rodapés de `register_files` saem de
  `pq.ParquetFile` sobre `open_input_file` do mesmo sistema de arquivos.
- **`create_text`** e **`write_text`** são a escrita de `_serialize_db/snapshots.json`. Na pasta
  local, `create_text` é `os.open(O_CREAT | O_EXCL)` e `if_match` compara a impressão digital (`sha256` do conteúdo)
  antes de gravar num arquivo temporário e trocar por `os.replace`; a comparação e a troca não são
  atômicas entre processos, o que basta à pasta local, o ambiente dos testes e do desenvolvimento.
  No S3, `put_object` do `boto3` com `IfNoneMatch="*"` (`create_text`) ou `IfMatch=<etag>`, atômico no servidor, e
  o 412 vira `ConflictError`; `read_text` lê pelo `get_object`, que devolve a etag. `read_text`
  devolve o texto e a impressão para a escrita seguinte. É o único uso do `boto3` na etapa, que o
  leva às dependências de execução, fixado em `pyproject.toml`, e roda
  `prepare_offline.sh` de novo.
- **`storage_options`** monta as opções do delta-rs a cada chamada: `AWS_REGION` de `AWS_REGION` ou
  `AWS_DEFAULT_REGION`, `AWS_ENDPOINT_URL` quando presente, `max_retries` 3 e `retry_timeout` 10 s,
  e as chaves de SSE quando configuradas. Contra um endereço que não responde, o delta-rs desistiu
  em 10,3 s com essas opções, contra 57,0 s no padrão, e contra uma porta fechada em 0,6 s;
  `max_retries` 1 deu os mesmos 10,3 s (leitura de 2026-09-23, [`POC.md`](POC.md)), e as três
  tentativas cobrem o erro passageiro do S3 dentro dos 10 s. Credencial alguma entra no dicionário:
  a cadeia padrão do delta-rs as resolve e as renova enquanto a execução segura o `DeltaTable`, que
  nasce com as opções recebidas; um trio congelado do `boto3` expiraria em cerca de uma hora no meio
  de uma execução longa e circularia num dicionário que um log ou uma mensagem de exceção imprime. A
  cadeia depende do `NO_PROXY` que `prepare_environment` exporta, e o ambiente alvo não tem proxy
  ([`POC.md`](POC.md), leitura de 2026-09-21). `test_delta_rs_storage_options_fallback` continua
  medindo a forma das credenciais congeladas, para o dia em que um ambiente quebrar a cadeia.
- **`duckdb_setup`** roda `LOAD httpfs; LOAD delta` e cria o secret `serialize_db_s3` com a chave,
  o segredo e o token da credencial do `boto3`, a região e o endpoint quando a raiz é S3, e só
  `LOAD delta` na pasta local. O DuckDB não lê
  `AWS_ENDPOINT_URL`, e com um endpoint o secret leva o endereço sem o esquema, o endereço por
  caminho (`URL_STYLE 'path'`) que o delta-rs e o PyArrow usam, e `USE_SSL false` num endpoint
  `http`: sem as duas opções, o moto em `127.0.0.1` não respondeu ao DuckDB ([`POC.md`](POC.md),
  sonda de 2026-09-23). Aplica `http_proxy`, `http_proxy_username` e `http_proxy_password`
  separados de `HTTP_PROXY` como `probelib.duckdb_proxy`. No ambiente alvo não há variável de
  proxy, e o bloco é vazio ([`POC.md`](POC.md), leitura de 2026-09-21). O secret guarda a chave
  até ser recriado. O `credential_chain` com `REFRESH auto`, a forma de 2026-09-24, só era renovado
  pelo `httpfs`: no substituto e no alvo, em 2026-09-25, o `delta_scan` de uma conexão aberta
  falhou uma vez depois que a chave guardada no secret expirou ([`POC.md`](POC.md)). O usuário
  decidiu em 2026-09-25 o secret com a chave do `boto3`: `renew_duckdb_secret(connection,
  credentials)`, protegida, lê o `key_id` que `duckdb_secrets()` mostra sem redação e recria o
  secret quando ele difere da chave de `credentials.get_frozen_credentials()`, que o botocore
  troca quando faltam menos de 15 minutos para a expiração. O motor DuckDB da
  [etapa 4](PLAN-STAGE-4.md) segura a credencial de `aws_credentials()`, também protegida, e chama
  a recriação na entrada de cada sessão, o que cobre a execução, o leitor Delta e a carga da
  [etapa 7](PLAN-STAGE-7.md). As conexões de `rewrite`, `read_back`, `export_snapshot` e da troca
  da [etapa 5](PLAN-STAGE-5.md) duram uma tabela ou uma partição e ficam com a chave da abertura.
  A extensão `aws`, que servia ao `credential_chain`, saiu de `duckdb_setup`.
- **`prepare_environment`** exporta `NO_PROXY` de `no_proxy` quando a maiúscula está ausente ou
  vazia, copia a região nos dois sentidos e devolve o dicionário do que mudou, para o log.
- **`create_table`** é `DeltaTable.create(mode="ignore")` com `delta_schema(table)`,
  `partition_by` de `table_options`, o nome da tabela, o comentário da tabela em `description` e as
  duas propriedades de retenção. A descrição, o nome e os comentários de coluna atravessam todo
  `write_deltalake(mode="overwrite")`, com e sem predicado
  (`test_deltalake.py::test_description_and_comments_survive_overwrite`).
- **`publish_partition`** monta o predicado `<coluna> = '<valor>'` de `table_options(table)`, com o
  nome da coluna entre aspas duplas, e chama `write_deltalake(dt, data, mode="overwrite",
  predicate=..., commit_properties=CommitProperties(custom_metadata=metadata))` sobre o objeto
  aberto por `open_table`, que depois da escrita está na versão do próprio commit. O valor entra
  entre aspas simples e chega validado pela regra da partição da [etapa 6](PLAN-STAGE-6.md),
  `[0-9A-Za-z][0-9A-Za-z_.-]*`, que exclui a aspa simples que quebraria o predicado.
  `value=None` numa tabela sem partição substitui a tabela; `CommitFailedError` vira
  `ExecutionConflict`. Ela recebe `data` já passado por `cast`, e recusa um lote sem a coluna de
  partição antes de gravar. As colunas de `columns_without_min_max` entram no `writer_properties`
  com `ColumnProperties(statistics_enabled="NONE")`, que vale para a chamada inteira, uma por
  partição (`test_deltalake.py::test_float_statistics_off_per_partition_keep_the_nan_row_and_the_pruning`).
- **`register_files`** traz de `scripts/migrate_parquet_to_delta.py` a montagem da ação
  (`stat_converter`, `delta_stats`), sem o mínimo e o máximo das colunas de
  `columns_without_min_max`, e faz as conferências da seção "As conferências do registro de
  arquivos" com um `pq.ParquetFile` por arquivo (um `GET` de rodapé no S3, pelo `pyarrow.fs` do
  `Storage`), cada conferência numa função que levanta `RegistrationRefused`; depois, um único
  `create_write_transaction(mode="overwrite", partition_filters=[(coluna, "=", valor)])` com uma
  `AddAction` por arquivo. A tabela de tipos físicos admitidos por tipo lógico é a do rascunho,
  com o `INT32` do decimal de até 9 dígitos que o DuckDB grava (`INT96` e `INT64` para
  `timestamp_ntz`, `FIXED_LEN_BYTE_ARRAY`, `INT64` e `INT32` para `decimal`, `INT32` e `INT64` para
  `long`). O mínimo e o máximo entram das colunas inteiras, de data, `Double` e
  `String`, os quatro tipos que transcrevem exato, e `nullCount` de todas as que o rodapé traz. O
  inteiro converte por `int`, a data e o texto saem como o texto do `RETURN_STATS`, e o `Double` por
  `float`, que faz o percurso de ida e volta na representação mais curta.
  `create_write_transaction` devolve `None` e não atualiza o objeto `DeltaTable` (leituras de
  2026-09-21 a 2026-09-23), então a primitiva relê a versão depois do commit; com uma execução por
  ambiente, a versão relida é a do próprio commit. O commit confere conflito como o
  `write_deltalake`: um segundo registro da mesma partição, a partir da versão que o primeiro
  substituiu, é `CommitFailedError`, e a primitiva o converte em `ExecutionConflict`.
- **`read_back`** roda depois do commit, numa conexão de `storage.duckdb_connect`, como `rewrite`:
  `count(*)` e mínimo e máximo de cada coluna da primeira chave do modelo por partição no delta-rs
  (o scanner de `to_pyarrow_dataset`, lote a lote, com a memória de um lote) e no `delta_scan` do
  DuckDB, a soma de `numRecords` das ações `add` da partição, e o mínimo e o máximo que o log
  registra da chave, nos tipos exatos, como limites dos lidos: um máximo registrado abaixo do lido
  podaria o arquivo que tem a linha. Uma diferença chama `restore(version - 1)` e levanta
  `RegistrationRefused` com as leituras.
- **`version_diff`** lê os arquivos `_delta_log/<versão>.json` de `published + 1` a `current` pelo
  `Storage` e recolhe `partitionValues` das ações `add` e `remove` com `dataChange` verdadeiro; a
  compactação grava `dataChange` falso e não conta (rascunho de 2026-09-21), o que evita recarregar no
  Redshift uma partição só compactada. Um arquivo do log ausente é `LogUnavailable`, com a
  instrução de publicar a tabela inteira: `create_table` fixa `delta.logRetentionDuration` em 3.650
  dias, e a limpeza que apagaria o arquivo também torna ilegível a versão publicada ([`delta.md`](delta.md),
  `No files in log segment`), então a diferença de conjuntos de `get_add_actions` entre as duas
  versões não teria como rodar. Os dois consumidores — a guarda de conflito da
  [etapa 6](PLAN-STAGE-6.md) e a publicação da [etapa 8](PLAN-STAGE-8.md) — param em vez de
  adivinhar o que o cliente recebe.
- **`schema_diff` e `reconcile`** comparam `arrow_schema(table)` com `pa.schema(dt.schema())`:
  coluna anulável nova entra por `add_columns` com o tipo Delta derivado do campo Arrow (`Field(name,
  DeltaSchema.from_arrow(pa.schema([field])).fields[0].type, nullable=True)`); `NOT NULL` relaxado
  por `drop_column_not_null`; `CHECK` por `add_constraint`; o destrutivo é `SchemaDiffRefused` com a
  lista e a instrução de `rewrite`. A documentação também é aditiva: o comentário da tabela vai para
  `set_table_description` e o da coluna para `set_column_metadata`, cada um num commit só de
  `metaData`, e a comparação lê a chave `comment` do campo, porque o esquema Arrow traz o
  `PARQUET:field_id` que o Delta não tem.
- **`rewrite`** abre uma conexão de `storage.duckdb_connect`, roda `COPY (SELECT <expressão de cada
  coluna do contrato> FROM delta_scan(uri, version := <atual>)) TO uri (FORMAT parquet, PARTITION_BY
  (<coluna>), APPEND true, FILENAME_PATTERN 'rewrite_{uuid}', RETURN_STATS)`, que tira a coluna de
  partição dos arquivos, ou `TO '<uri>/rewrite_<uuid>.parquet'` numa tabela sem partição,
  e registra tudo num `create_write_transaction(mode="overwrite", schema=delta_schema(table))`; é o
  caminho medido em [`delta.md`](delta.md) com memória constante, e a medição de lá renomeou
  `valor` por `valor AS valor_bruto` no `SELECT`. A expressão de cada coluna é
  `CAST(<expressão> AS <sql_type(coluna, "duckdb")>) AS "<coluna>"`, com a expressão de
  `expressions` ou o nome da coluna entre aspas: a renomeação é `{"valor_bruto": '"valor"'}`, a
  coluna `NOT NULL` nova leva o seu valor (`{"canal": "'web'"}`), a mudança de tipo sai do `CAST`, e
  a remoção é a coluna que o contrato não tem mais. Uma chave de `expressions` fora do contrato é
  `ContractError`, e uma coluna do contrato ausente da versão atual e fora de `expressions` falha
  no DuckDB (`BinderException`), antes de qualquer commit. Uma consulta sobre o mesmo `SELECT`
  conta os não finitos de cada coluna `Double` por partição, e essas colunas saem sem mínimo e
  máximo no log de cada arquivo da partição (issue #59). As ações passam pelas mesmas conferências
  de `register_files`, e `read_back` roda depois, na tabela inteira.
- **`snapshot`** lê o arquivo de controle com a impressão, recusa um nome presente em `snapshots` ou em `archived`, grava com
  `if_match` (ou `create_text` no primeiro) e devolve o controle novo; `ConflictError` sobe.
- **`vacuum_keeping_snapshots`** monta `keep_versions` das versões do controle para a tabela e
  chama `vacuum(retention_hours, enforce_retention_duration=False, dry_run=not apply,
  keep_versions=...)`; `full=True` inclui os órfãos. Dentro da retenção nada é listado, mesmo com
  versões intermediárias: a retenção de 400 dias é a janela em que toda versão continua legível.
- **`compact`** é `optimize.compact(partition_filters=[(coluna, "in", partitions)])`; a operação
  com um só arquivo na partição não commita, e o chamador lê a versão antes e depois.
- **`deep_copy`** cria o destino por `DeltaTable.create` com o esquema, o nome, a descrição e as
  propriedades da versão (`mode="error"` recusa um destino que já tem tabela), copia cada arquivo
  que `get_add_actions(flatten=False)` lista por `Storage.copy`, para o mesmo caminho relativo, e
  o registra num commit `overwrite` por partição com a `AddAction` montada da ação de origem
  (tamanho, linhas, `nullCount` e o mínimo e o máximo dos tipos exatos), sem os dados passarem
  pela máquina; no fim confere a contagem da cópia pelos dois leitores contra a soma das ações,
  e a diferença é `RegistrationRefused`. É a troca que a [etapa 9](PLAN-STAGE-9.md) fez em
  2026-09-24 (decisão do usuário) no lugar do `write_deltalake` do leitor da tabela inteira, cuja
  memória cresce com a tabela, fora do `memory_limit` do DuckDB (1.140 MB para 12.000.000 de
  linhas, [`delta.md`](delta.md)).
- **`export_snapshot`** copia os arquivos que `get_add_actions()` lista no layout
  `<coluna>=<valor>/` por `Storage.copy` (`mode="copy"`), ou reescreve pelo `COPY` particionado do
  DuckDB (`mode="rewrite"`); um snapshot antigo usa `DeltaTable(uri, version=v)`.

## Pré-requisitos e pós-condições

| Primitiva | Pré-requisitos | Pós-condições |
| --- | --- | --- |
| `create_text`, `write_text` | `create_text` num caminho ausente, ou `write_text` com `if_match` e a impressão da última leitura. | O arquivo gravado por inteiro e a impressão nova devolvida; `ConflictError` sem alteração quando a condição falha. |
| `storage_options`, `duckdb_setup` | Região em `AWS_REGION` ou `AWS_DEFAULT_REGION` para o S3; credencial na cadeia do `boto3`; extensões na pasta configurada. | Opções sem credencial alguma e o secret com a chave da credencial do `boto3`; nenhum download de extensão. |
| `create_table` | Modelo aprovado por `check_models`. | Tabela na versão 0 com o esquema Delta do contrato, a partição, as retenções, o nome e o comentário da tabela em `description`; a chamada repetida não muda a versão. |
| `publish_partition` | `data` passado por `cast`, com a coluna de partição; a versão atual da tabela sem dados novos desde a fixada (conferido por `Execution.publish`); o valor validado pela etapa 6. | Uma versão nova com os arquivos da partição e os metadados de commit, devolvida pelo objeto que escreveu; as demais partições intactas; `ExecutionConflict` sem commit no conflito. |
| `register_files` | Arquivos gravados dentro da pasta da tabela, com o rodapé legível. | Um commit `overwrite` da partição com uma ação por arquivo, ou `RegistrationRefused` sem commit e com o arquivo e a conferência na mensagem. |
| `read_back` | Um commit recém-feito. | Contagem igual nos dois leitores, no log e em `expected_rows`, extremos da chave iguais nos dois leitores e dentro dos limites do log, ou `restore(version - 1)` e `RegistrationRefused`. |
| `reconcile` | Tabela existente. | O diff aditivo aplicado em commits de metadados, comentários incluídos; `SchemaDiffRefused` sem alteração no destrutivo; a versão anterior continua legível com o esquema antigo. |
| `rewrite` | Ordem explícita fora da execução mensal; em `expressions`, a expressão de toda coluna do contrato que a versão atual não tem. | Um commit com `remove` de todos os arquivos vivos, `add` dos novos e `metaData`; memória constante; a releitura feita. |
| `version_diff` | `published <= current`; os arquivos do log das duas versões presentes. | O conjunto das partições com dados alterados; vazio para `published == current` e para uma compactação; `LogUnavailable` quando um arquivo do log falta. |
| `snapshot` | Nome inédito. | A entrada gravada com escrita condicional; `ConflictError` quando outro escritor mudou o arquivo entre a leitura e a escrita. |
| `vacuum_keeping_snapshots` | Controle lido. | A lista dos arquivos fora da retenção e fora das versões presas; com `apply=True`, apagados, e a versão do snapshot continua legível. |

## Testes por caso

`tests/test_storage.py` e `tests/test_delta.py` sob a raiz local; os mesmos casos no bucket por
`-m s3`.

| Caso | Teste | O que confere |
| --- | --- | --- |
| Armazenamento por URI | `test_storage_for_uri` (sem gravar e sem rede) | `s3://`, `file://` e caminho dão o sistema de arquivos certo (`S3FileSystem` com a região da variável, `LocalFileSystem`) e o caminho nele; outra URI e o S3 sem região são erro. |
| Caminhos | `test_paths_relative_to_the_root` (sem gravar) | `join` sem barras nas pontas, `relative` da URI sob a raiz e a recusa da de fora, `uri_of`. |
| Escrita condicional | `test_create_text_and_write_text_if_match` | A segunda `create_text` e o `if_match` velho são `ConflictError`; o conteúdo final é o da escrita que venceu; o arquivo ausente é `FileNotFoundError`. |
| Listagem, cópia e exclusão | `test_list_copy_delete` | `list_files` desce as pastas e exclui `_delta_log/`; `copy` preserva bytes; `delete` de caminho ausente não falha. |
| Opções do delta-rs | `test_storage_options_resolved_per_call` (sem gravar) | Duas chamadas devolvem dicionários novos; a região vem da variável; `max_retries` presente; as chaves de SSE configuradas; nenhuma chave de credencial no dicionário. |
| Ambiente | `test_prepare_environment` (sem gravar) | `NO_PROXY` sai de `no_proxy` quando ausente ou vazia, a região vai nos dois sentidos, e a segunda chamada não muda nada. |
| Proxy do DuckDB | `test_duckdb_proxy_settings_without_credentials_in_the_address` (sem gravar) | O endereço sem as credenciais, o usuário e a senha das variáveis ou do endereço, sem URL-encode. |
| Secret do DuckDB | `test_duckdb_secret_options_for_an_endpoint` (sem gravar) | A região; com `AWS_ENDPOINT_URL`, o endereço sem o esquema e `URL_STYLE 'path'`, e `USE_SSL false` só num endpoint `http`. |
| Recriação do secret | `test_renew_duckdb_secret_follows_the_key` (sem gravar; pulado sem a extensão `httpfs` na pasta de extensões) | O secret criado sem um anterior, mantido com a mesma chave e recriado quando a chave da credencial troca, com a região, sem o segredo nem o token no `secret_string`, e a chave, o segredo e o token fora do texto dos comandos. |
| Conexão do DuckDB | `test_duckdb_connect_loads_delta` | A extensão `delta` carregada da pasta configurada, sem instalação automática, e, no S3, o secret com a chave que a cadeia do `boto3` resolve. |
| Criação | `test_create_table_is_idempotent` | Versão 0 nas duas chamadas; esquema, partição, retenções, nome e o comentário da tabela em `description` lidos do log. |
| Substituição | `test_publish_partition_replaces_only_its_partition` | Duas partições, a segunda republicada: a primeira intacta, uma versão por chamada, os metadados no `history`; o valor fora da regra e os dados sem a coluna de partição recusados antes de gravar. |
| Versão devolvida | `test_publish_partition_returns_its_own_version` | Com o commit de outro escritor entre a abertura e a escrita, a versão devolvida é a do próprio commit; os dados chegam por `__arrow_c_stream__`. |
| Sem partição | `test_publish_partition_without_partition_replaces_the_table` | `value=None` troca a tabela inteira; um valor numa tabela sem partição é recusado. |
| Conflito | `test_two_writers_on_the_same_partition_conflict` | Dois `overwrite` da mesma partição na mesma versão, por `publish_partition` e por `register_files`: o segundo é `ExecutionConflict`; partições distintas passam. |
| Registro | `test_register_files_registers_an_unload_like_file` | Um arquivo `INT96` e `FIXED_LEN_BYTE_ARRAY` registrado; os dois leitores devolvem as linhas e `timestamp[us]`; o mínimo e o máximo da chave na ação. |
| Contagem de nulos do rodapé | `test_file_from_footer_leaves_out_the_null_count_the_footer_lacks` | A coluna sem estatística no rodapé, o timestamp `INT96`, fica fora do `null_count` de `file_from_footer` e do `nullCount` do log, e o `IS NULL` pelo `DeltaTable.scan` lê os nulos dela; a chave entra com o zero. |
| Conferências | `test_register_files_refuses_each_defect`, parametrizado | Tamanho, linhas, partição do caminho, `expected_rows` diferente, caminho absoluto, coluna do contrato ausente, tipo físico fora dos admitidos, coluna de partição dentro do arquivo, colunas fora de ordem e nulo numa coluna `NOT NULL`; a versão não muda e o arquivo fica órfão. |
| Releitura | `test_read_back_restores_on_a_difference` | Um máximo falso da chave, abaixo do real, faz `read_back` voltar a versão por `restore`. |
| `Double` não finito | `test_nonfinite_double_columns_leave_min_max_out` | Uma coluna em `columns_without_min_max` numa partição: `publish_partition` grava o rodapé e o log sem o mínimo e o máximo dela, `register_files` grava o log sem os dois (o infinito inclusive), a outra partição sai com eles, e o `delta_scan` devolve as linhas do `NaN` e do infinito num filtro por intervalo e não abre o arquivo da outra partição. |
| Estatísticas | `test_file_from_return_stats_and_registered_stats_prune` | O arquivo do `COPY ... RETURN_STATS` do DuckDB entra com o mínimo e o máximo dos tipos exatos; `EXPLAIN ANALYZE` mostra `Scanning Files: 0/n` para uma chave acima do máximo e um texto acima do máximo; as colunas `decimal` e `timestamp` entram sem mínimo e máximo. |
| Chave máxima | `test_max_key_reads_statistics_and_scans_without_them` | O máximo das estatísticas, a varredura quando um arquivo registrado não as tem, 0 na tabela vazia. |
| Reconciliação aditiva | `test_reconcile_adds_nullable_column_and_relaxes_not_null` | A coluna entra no fim, com o comentário; as linhas antigas leem nulo; a versão anterior lê o esquema antigo; a segunda chamada não commita. |
| Documentação | `test_reconcile_syncs_description_and_comments` | Um comentário de tabela e um de coluna alterados no modelo entram por commit de `metaData`; a segunda chamada não commita. |
| Reconciliação destrutiva | `test_reconcile_refuses_destructive_diff`, parametrizado | Tipo trocado, coluna removida, `NOT NULL` nova e `NOT NULL` numa coluna anulável, numa tabela com dados, são `SchemaDiffRefused`, sem commit. |
| Reescrita | `test_rewrite_in_one_commit_keeps_previous_version_readable` | Um commit, o esquema novo, as linhas iguais e a versão anterior legível; uma coluna renomeada por `expressions` lê os valores da antiga; o `Double` com `NaN` sem mínimo e máximo na partição dele; uma coluna do contrato ausente e fora de `expressions` falha sem commit. |
| Diferença de versões | `test_version_diff_counts_data_changes_only` | Substituição e remoção contam, compactação e reconciliação não, `published == current` dá vazio; `{None}` na tabela sem partição. |
| Log ausente | `test_version_diff_refuses_a_cleaned_log` | Um arquivo do log apagado entre as duas versões dá `LogUnavailable`, com a publicação completa na mensagem. |
| Snapshot | `test_snapshot_control_file_is_written_conditionally` | O nome repetido é erro; o nome fora da regra da partição (`2026 T4`, `release/2026`) é `ContractError`, sem gravar; a escrita concorrente é `ConflictError`. |
| `vacuum` | `test_vacuum_keeps_snapshot_versions` | Com retenção zero e `keep_versions`, a versão do snapshot lê e a intermediária falha; dentro da retenção nada é listado. |
| Compactação | `test_compact_before_snapshot` | Arquivos pequenos de uma partição virando um; a partição com um arquivo não commita. |
| Exportação | `test_export_snapshot_copy_and_rewrite` | Os dois modos produzem `<coluna>=<valor>/` com as mesmas linhas; `copy` copia só o que o log lista; uma versão antiga exporta o que ela tinha; o destino fora da raiz recusa nos dois modos, sem gravar. |
| Manifesto | `test_copy_manifest_lists_the_files_of_a_version` | A URL, o tamanho e `mandatory` de cada arquivo da versão nas partições pedidas, e de todos sem elas. |
| Cópia e realocação | `test_deep_copy_and_relocation` | A cópia profunda tem o mesmo arquivo, caminho, tamanho e extremos da versão, uma versão por partição e a nulidade do esquema, e recusa o destino com tabela; a pasta copiada abre na mesma versão nos dois leitores. |

## A implementação

Os módulos `serialize_db.storage` e `serialize_db.delta` e os casos de `tests/test_storage.py` e
`tests/test_delta.py` substituem a interface e o rascunho executado em 2026-09-21: as assinaturas
e as docstrings estão no código e na documentação do `pdoc`, as exceções da etapa em
`serialize_db.errors`, e a regra da partição em `serialize_db.schema` (`PARTITION_VALUE`,
`check_partition_value`). O que o rascunho mostrou (o `dataChange` falso da compactação, a
compactação de um arquivo que não commita, o tipo Delta que `add_columns` recebe) e o que a
implementação mostrou estão em [`POC.md`](POC.md), seções "O que os rascunhos das etapas
mostraram" e "O que a implementação da etapa 3 mostrou".

## Decisões pendentes

Nenhuma. A renovação do secret do DuckDB, decidida pelo usuário em 2026-09-25, está na descrição
de `duckdb_setup`.

As seis decisões da etapa tomadas pelo usuário em 2026-09-22 estão escritas na seção que
descreve cada uma: o comentário da tabela em `description`, com `reconcile` sincronizando a
descrição e os comentários de coluna; `storage_options` sem credencial alguma, pela cadeia padrão do
delta-rs; o mínimo e o máximo das colunas inteiras, de data, `Double` e `String` em
`register_files`; `version_diff` recusando com `LogUnavailable` o log limpo; `expressions` em
`rewrite`, para a renomeação e a coluna `NOT NULL` nova; e `Storage` sobre `pyarrow.fs`, sem uma
classe por armazenamento. A decisão de 2026-09-23 sobre o `Double` não finito, a lista
`columns_without_min_max` por partição, está no item 5 das conferências do registro e em
`publish_partition`. As sondagens que mediram as quatro primeiras estão em [`POC.md`](POC.md) e
em `tests/proof_of_concept/test_deltalake.py`.
