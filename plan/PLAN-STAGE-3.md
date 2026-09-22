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
| `storage_options()` | As opções do delta-rs: região, `AWS_ENDPOINT_URL`, `max_retries`, `retry_timeout`, `timeout` e as chaves de SSE quando configuradas; nunca credenciais (decisão do usuário de 2026-09-22). A cadeia padrão do delta-rs as resolve e as renova sozinha no `DeltaTable` que a execução guarda, enquanto um trio congelado expiraria em cerca de uma hora e circularia num dicionário que um log ou uma exceção imprime. Resolvidas a cada chamada, nunca guardadas. |
| `duckdb_setup(connection)` | `LOAD httpfs; LOAD delta; LOAD aws` e o secret `credential_chain` com `REGION` e `ENDPOINT`; só `LOAD delta` na pasta local. Aplica `http_proxy`, `http_proxy_username` e `http_proxy_password` a partir de `HTTP_PROXY`, `username` e `password`, como `probelib.duckdb_proxy` faz nos probes: o DuckDB recusa o endereço com as credenciais embutidas, e o erro atinge o acesso ao S3, não só o download de extensão ([`POC.md`](POC.md)). |
| `prepare_environment()` | Exporta `NO_PROXY` a partir de `no_proxy` quando a maiúscula está ausente ou vazia, copia a região entre `AWS_REGION` e `AWS_DEFAULT_REGION` nos dois sentidos, respeita `AWS_ENDPOINT_URL`; devolve o que mudou, para o log. Chamada por `Database`. |

`serialize_db.delta` é a camada de tabela; `uri` é a pasta da tabela, `table` o `Table` do modelo,
`data` uma `pa.Table`, um `RecordBatchReader` ou um objeto com `__arrow_c_stream__`, como o `BatchStream`
de um motor.

| Primitiva | O que faz |
| --- | --- |
| `create_table(uri, table)` | `DeltaTable.create(mode="ignore")` com `delta_schema`, `partition_by`, o nome da tabela, o comentário da tabela em `description` (decisão do usuário de 2026-09-22) e as propriedades `delta.logRetentionDuration = interval 3650 days` e `delta.deletedFileRetentionDuration = interval 400 days`; sem vetores de exclusão nem column mapping. |
| `open(uri, version=None)` | A `DeltaTable` numa versão; a execução abre cada tabela uma vez e guarda a versão. |
| `max_key(dt, column)` | O maior valor de `column` na versão carregada: o máximo de `max.<coluna>` de `get_add_actions(flatten=True)`, sem ler dados, ou a varredura da coluna quando um arquivo não tem a estatística; 0 na tabela vazia. O início de `run.next_ids`, e por isso a estatística registrada é verdadeira ou omitida (seção "As conferências do registro de arquivos"): a omitida cai na varredura, a falsa daria chaves repetidas. |
| `commit_metadata(execution_id, input_versions, snapshot=None)` | O dicionário de `CommitProperties(custom_metadata=...)`: `serialize_db_execution_id`, `serialize_db_input_versions` e `serialize_db_snapshot`. |
| `publish_partition(uri, table, value, data, metadata, storage)` | `write_deltalake(mode="overwrite", predicate="<coluna de partição> = '<valor>'")` de `data` já passado por `cast`; `value=None` numa tabela sem partição substitui a tabela inteira; `CommitFailedError` sobe como `ExecutionConflict`. |
| `register_files(uri, table, files, value, metadata, storage, expected_rows=None)` | Arquivos que outro escritor gravou dentro da pasta da tabela entram no log por `create_write_transaction(mode="overwrite", partition_filters=...)`, uma `AddAction` por arquivo: caminho relativo à pasta da tabela, tamanho, valores de partição e estatísticas do `RETURN_STATS` do DuckDB ou do rodapé Parquet. O `create_write_transaction` grava a ação como a recebe, e os leitores obedecem à ação, não ao arquivo ([`POC.md`](POC.md)); a primitiva faz as conferências da seção "As conferências do registro de arquivos" antes do commit e a releitura depois dele. Os arquivos do `UNLOAD` do Redshift têm mínimo e máximo, menos nas colunas de timestamp, que saem em `INT96` e não carregam estatística: a coluna fica fora de `minValues` e `maxValues` sem falhar o registro ([`redshift.md`](redshift.md)). O `schema.elements` do manifesto verboso lista a coluna de partição, que os arquivos não têm (suíte de 2026-09-21): a lista esperada da conferência a inclui. |
| `schema_diff(table, dt)` | O `SchemaDiff` entre `arrow_schema(table)` e `dt.schema()`: coluna nova anulável, `NOT NULL` relaxado, `CHECK` e comentário divergente são aditivos; coluna `NOT NULL` nova em tabela com dados, renomeação, remoção e mudança de tipo são destrutivos. |
| `reconcile(uri, table)` | Aplica o diff aditivo (`add_columns`, `drop_column_not_null`, `add_constraint`, `set_table_description` e `set_column_metadata`) e recusa o destrutivo com a mensagem que aponta `rewrite`. |
| `rewrite(uri, table)` | A tabela inteira com o esquema do contrato num único commit e sem predicado: `COPY ... PARTITION_BY (<coluna de partição>) ... RETURN_STATS` do DuckDB a partir de `delta_scan` mais `create_write_transaction(mode="overwrite", schema=...)`, com memória constante. A conexão DuckDB é aberta aqui e configurada por `storage.duckdb_setup`, sem o motor da [etapa 4](PLAN-STAGE-4.md): `delta` não depende de `engine`. |
| `copy_manifest(uri, version, partitions, destination)` | O manifesto do `COPY` do Redshift (`url` e `meta.content_length` de `get_add_actions()`), gravado sob `publicacao/`. |
| `version_diff(uri, published, current, table, storage)` | As partições com ações `add` ou `remove` de dados entre as duas versões, lidas do log; a compactação (`dataChange` falso) não conta. Um arquivo do log ausente é `LogUnavailable`, com a instrução de publicar a tabela inteira (decisão do usuário de 2026-09-22). |
| `snapshot(root, name, versions)` | A entrada `{name: versions}` em `_serialize_db/snapshots.json`, gravada com `write_text(if_match=...)`. |
| `vacuum_keeping_snapshots(uri, control, retention_hours=9600, apply=False)` | `vacuum` com `keep_versions` das versões do arquivo de controle; lista por padrão e apaga com `apply=True`. |
| `compact(uri, partitions)` | `optimize.compact` das partições com arquivos pequenos, antes de um snapshot. A reescrita sai pelo escritor do delta-rs: os arquivos do `UNLOAD` que ela junta perdem o `INT96` e o `FIXED_LEN_BYTE_ARRAY` e ganham estatística em toda coluna (`test_deltalake.py::test_compact_rewrites_files_from_another_writer`). |
| `deep_copy(uri, version, destination)` | Tabela nova na versão 0 com os dados de uma versão, para a pasta de arquivo. |
| `export_snapshot(uri, destination, version=None, mode="copy")` | Pastas `<coluna de partição>=<valor>/` sem o log: `copy` copia os arquivos que o log lista; `rewrite` reescreve pelo `COPY` particionado do DuckDB. |

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

1. O arquivo existe em `uri/path`, o caminho que o leitor resolve, com o tamanho da ação.
2. O esquema do rodapé contra o da tabela, nome a nome: nenhuma coluna do contrato ausente, e o tipo
   físico entre os admitidos para o lógico (`INT96` e `timestamp[ns]` para `timestamp_ntz`,
   `FIXED_LEN_BYTE_ARRAY` e `INT64` para `decimal`, `int32` para `long`); uma coluna a mais passa,
   porque os leitores a ignoram.
3. O valor de partição do caminho Hive igual ao de `partitions`.
4. A soma de `num_records` dos rodapés igual à que `files` declara e a `expected_rows`, quando o
   chamador tem a contagem da fonte.
5. `numRecords` do rodapé; `minValues` e `maxValues` das colunas inteiras, de data, `Double` e
   `String`, omitidos nas demais e nas que o rodapé não traz: uma estatística ausente só deixa de
   podar, uma errada poda o arquivo certo. Os quatro tipos são os que a sondagem de 2026-09-22
   mediu transcrevendo exato ([`POC.md`](POC.md), decisão do usuário do mesmo dia); `decimal` fica
   de fora porque o próprio delta-rs grava o mínimo e o máximo como número JSON e perde a linha na
   poda, e `timestamp` porque o valor sai truncado em milissegundos.

A reprovação recusa o commit com o arquivo e a conferência na mensagem, e os arquivos ficam órfãos na
pasta até `vacuum(full=True)`. Depois do commit, `read_back(uri, partitions, expected)` lê a versão
nova pelo delta-rs e pelo `delta_scan`, `count(*)` e mínimo e máximo da chave por partição, e uma
diferença chama `restore(version - 1)` e sobe a mesma exceção. Uma versão entra num snapshot ou em
`publish_redshift` só depois da releitura.

Testes: `tests/test_storage.py` e `tests/test_delta.py` sob a raiz local, com os mesmos casos no
bucket por `-m s3`: substituição da partição e idempotência, conflito entre dois escritores,
reconciliação aditiva e recusa da destrutiva, `rewrite` num commit sem predicado com a versão
anterior legível, `keep_versions`, `export_snapshot` nos dois modos, realocação da pasta e a escrita
condicional do arquivo de controle. Provas de conceito: `test_stdlib.py` (`test_storage_uris`,
`test_exclusive_create_atomic_replace_and_fingerprint`, `test_json_control_file_and_commit_metadata`,
`test_group_log_actions_by_month`, `test_prepare_environment`), `test_s3.py` (`test_conditional_put`,
`test_boto3_list_copy_delete`, e `test_delta_rs_storage_options_fallback`, que mede a forma das
credenciais em `storage_options` sem que a biblioteca a use), `test_local.py`
(`test_commit_is_atomic_on_disk`, `test_folder_relocates`) e `test_deltalake.py` inteiro: criação
idempotente, predicado e nulidade, evolução com `drop_column_not_null` (recebe o nome da coluna),
`restore`, `AddAction`, o que `create_write_transaction` não confere (caminho, estatística, esquema
do arquivo) e o `overwrite` com `partition_filters`, a compactação que normaliza arquivos de outro
escritor, `vacuum`, `version_diff`, compactação e checkpoint, exportação por cópia e a
reescrita pelo `COPY ... APPEND true, FILENAME_PATTERN, RETURN_STATS` do DuckDB registrada num
commit `overwrite` com esquema novo e estatísticas tipadas, que o DuckDB usa para podar;
`test_parallel.py` (quatro tabelas lidas em paralelo, escritas em paralelo por tabela e por mês da
mesma tabela com o conflito no mesmo mês, e `max_key` pelas estatísticas com a varredura de reserva).

## Interface

```python
"""Assinaturas de serialize_db.storage e serialize_db.delta; os corpos estão nos rascunhos abaixo."""
import dataclasses
import os
from collections.abc import Mapping, MutableMapping
from typing import Literal

import duckdb
import pyarrow as pa
import sqlalchemy as sa
from deltalake import DeltaTable


class ConflictError(Exception):
    """A escrita condicional perdeu: outro escritor mudou o objeto (412 no S3, impressão digital diferente na pasta local)."""


class ExecutionConflict(Exception):
    """Outra execução gravou a mesma partição ou avançou a tabela com dados: CommitFailedError do delta-rs, ou a versão fixada ficou para trás."""


class RegistrationRefused(Exception):
    """Uma conferência de register_files reprovou; nada foi commitado e o arquivo fica órfão até vacuum(full=True)."""


class SchemaDiffRefused(Exception):
    """O diff entre o modelo e a tabela é destrutivo; a mensagem aponta rewrite."""


class LogUnavailable(Exception):
    """Um arquivo do log entre as duas versões não existe; a mensagem manda publicar a tabela inteira."""


class Storage:
    root: str

    @staticmethod
    def for_uri(uri: str) -> "Storage": ...
    def join(self, *parts: str) -> str: ...
    def exists(self, path: str) -> bool: ...
    def list_files(self, prefix: str, suffix: str = "") -> list[str]: ...
    def read_text(self, path: str) -> tuple[str, str]: ...                      # o texto e a impressão digital (etag no S3)
    def write_text(self, path: str, text: str, if_match: str | None = None, if_none_match: bool = False) -> str: ...
    def copy(self, source: str, destination: str) -> None: ...
    def delete(self, paths: list[str]) -> None: ...
    def storage_options(self) -> dict[str, str]: ...                            # região, endpoint, retry e SSE; nunca credenciais
    def duckdb_setup(self, connection: duckdb.DuckDBPyConnection) -> None: ...


class LocalStorage(Storage): ...
class S3Storage(Storage): ...


def prepare_environment(environ: MutableMapping[str, str] = os.environ) -> dict[str, str]: ...


@dataclasses.dataclass(frozen=True)
class RegisteredFile:
    """Um arquivo que outro escritor gravou dentro da pasta da tabela, como o COPY do DuckDB ou o manifesto do UNLOAD o descrevem."""
    path: str                        # relativo à pasta da tabela, como o log guarda
    size: int
    rows: int
    stats: Mapping[str, Mapping[str, object]]   # {"min": {coluna: valor}, "max": {...}, "null_count": {...}}, tipados; ausente é omitido


@dataclasses.dataclass(frozen=True)
class SchemaDiff:
    add: tuple[pa.Field, ...]        # colunas anuláveis novas
    relax: tuple[str, ...]           # NOT NULL relaxado
    checks: tuple[str, ...]          # CHECK novos
    description: str | None          # o comentário da tabela quando difere da description do Delta
    comments: tuple[str, ...]        # as colunas cujo comentário difere do do esquema Delta
    destructive: tuple[str, ...]     # NOT NULL nova em tabela com dados, renomeação, remoção, mudança de tipo


ExportMode = Literal["copy", "rewrite"]


def create_table(uri: str, table: sa.Table, storage: Storage) -> DeltaTable: ...
def open(uri: str, storage: Storage, version: int | None = None) -> DeltaTable: ...
def max_key(dt: DeltaTable, column: str) -> int: ...
def commit_metadata(execution_id: str, input_versions: Mapping[str, int], snapshot: str | None = None) -> dict[str, str]: ...
def publish_partition(uri: str, table: sa.Table, value: str | None, data: object, metadata: Mapping[str, str], storage: Storage) -> int: ...
def register_files(uri: str, table: sa.Table, files: list[RegisteredFile], value: str | None, metadata: Mapping[str, str], storage: Storage, expected_rows: int | None = None) -> int: ...
def read_back(uri: str, table: sa.Table, value: str | None, expected_rows: int, connection: duckdb.DuckDBPyConnection, storage: Storage) -> None: ...
def schema_diff(table: sa.Table, dt: DeltaTable) -> SchemaDiff: ...
def reconcile(uri: str, table: sa.Table, storage: Storage) -> SchemaDiff: ...
def rewrite(uri: str, table: sa.Table, storage: Storage) -> int: ...
def copy_manifest(uri: str, version: int, partitions: list[str] | None, destination: str, storage: Storage) -> str: ...
def version_diff(uri: str, published: int, current: int, table: sa.Table, storage: Storage) -> set[str | None]: ...
def read_snapshots(storage: Storage, environment: str) -> tuple[dict, str]: ...
def snapshot(storage: Storage, environment: str, name: str, versions: Mapping[str, int]) -> dict: ...
def vacuum_keeping_snapshots(uri: str, control: Mapping, table_name: str, storage: Storage, retention_hours: int = 9600, apply: bool = False, full: bool = False) -> list[str]: ...
def compact(uri: str, table: sa.Table, partitions: list[str], storage: Storage) -> dict: ...
def deep_copy(uri: str, version: int, destination: str, storage: Storage) -> int: ...
def export_snapshot(uri: str, table: sa.Table, destination: str, storage: Storage, version: int | None = None, mode: ExportMode = "copy") -> list[str]: ...
```

As assinaturas mudaram em relação à tabela acima: `publish_partition`, `register_files`,
`compact` e `export_snapshot` recebem o `Table`, porque a coluna de partição sai de
`table_options(table)` e não de um argumento solto; toda primitiva que toca o armazenamento recebe
o `Storage`, que resolve `storage_options()` a cada chamada; `register_files` recebe o valor da
partição (`value`) em vez de um dicionário `partitions`; `version_diff` devolve `None` no conjunto
quando a tabela não tem partição e foi alterada.

## Estratégia de implementação

- **`Storage.for_uri`** escolhe pela URI: `s3://` dá `S3Storage`, caminho ou `file://` dá
  `LocalStorage`. Os caminhos das primitivas são relativos à raiz e `join` os monta com `/`.
- **`write_text`** é a escrita de `_serialize_db/snapshots.json`. Na pasta local, `if_none_match`
  é `os.open(O_CREAT | O_EXCL)` e `if_match` compara a impressão digital (`sha256` do conteúdo)
  antes de gravar num arquivo temporário e trocar por `os.replace`. No S3, `PutObject` com
  `IfNoneMatch="*"` ou `IfMatch=<etag>`, e o 412 vira `ConflictError`. `read_text` devolve o texto e
  a impressão para a escrita seguinte.
- **`storage_options`** monta as opções do delta-rs a cada chamada: `AWS_REGION` de
  `AWS_REGION` ou `AWS_DEFAULT_REGION`, `AWS_ENDPOINT_URL` quando presente, `max_retries` e
  `retry_timeout` para uma rede morta falhar em segundos, e as chaves de SSE quando configuradas.
  Credencial alguma entra no dicionário: a cadeia padrão do delta-rs as resolve e as renova enquanto
  a execução segura o `DeltaTable`, que nasce com as opções recebidas; um trio congelado do `boto3`
  expiraria em cerca de uma hora no meio de uma execução longa e circularia num dicionário que um
  log ou uma mensagem de exceção imprime. A cadeia depende do `NO_PROXY` que `prepare_environment`
  exporta, e o ambiente alvo não tem proxy ([`POC.md`](POC.md), leitura de 2026-09-21).
  `test_delta_rs_storage_options_fallback` continua medindo a forma das credenciais congeladas, para
  o dia em que um ambiente quebrar a cadeia.
- **`duckdb_setup`** roda `LOAD httpfs; LOAD delta; LOAD aws` e cria o secret `credential_chain`
  com a região e o endpoint quando a raiz é S3, e só `LOAD delta` na pasta local; aplica
  `http_proxy`, `http_proxy_username` e `http_proxy_password` separados de `HTTP_PROXY` como
  `probelib.duckdb_proxy`. No ambiente alvo não há variável de proxy, e o bloco é vazio
  ([`POC.md`](POC.md), leitura de 2026-09-21).
- **`prepare_environment`** é a função de `test_stdlib.py`: `NO_PROXY` de `no_proxy` quando a
  maiúscula está ausente ou vazia, a região copiada nos dois sentidos, e o dicionário do que mudou
  para o log.
- **`create_table`** é `DeltaTable.create(mode="ignore")` com `delta_schema(table)`,
  `partition_by` de `table_options`, o nome da tabela, o comentário da tabela em `description` e as
  duas propriedades de retenção. A descrição, o nome e os comentários de coluna atravessam todo
  `write_deltalake(mode="overwrite")`, com e sem predicado
  (`test_deltalake.py::test_description_and_comments_survive_overwrite`).
- **`publish_partition`** monta o predicado `<coluna> = '<valor>'` de `table_options(table)` e chama
  `write_deltalake(mode="overwrite", predicate=..., commit_properties=CommitProperties(custom_metadata=metadata))`;
  `value=None` numa tabela sem partição substitui a tabela; `CommitFailedError` vira
  `ExecutionConflict`. Ela recebe `data` já passado por `cast`, e recusa um lote sem a coluna de
  partição antes de gravar.
- **`register_files`** faz as conferências da seção "As conferências do registro de arquivos" com
  um `pq.ParquetFile` por arquivo (um `GET` de rodapé no S3, pelo `pyarrow.fs` do `Storage`), depois
  um único `create_write_transaction(mode="overwrite", partition_filters=[(coluna, "=", valor)])` com
  uma `AddAction` por arquivo. A tabela de tipos físicos admitidos por tipo lógico é a do rascunho
  (`INT96` e `INT64` para `timestamp_ntz`, `FIXED_LEN_BYTE_ARRAY` e `INT64` para `decimal`, `INT32` e
  `INT64` para `long`). O mínimo e o máximo entram das colunas inteiras, de data, `Double` e
  `String`, os quatro tipos que transcrevem exato, e `nullCount` de todas as que o rodapé traz. O
  inteiro converte por `int`, a data e o texto saem como o texto do `RETURN_STATS`, e o `Double` por
  `float`, que faz o percurso de ida e volta na representação mais curta.
- **`read_back`** roda depois do commit: `count(*)` e mínimo e máximo da chave por partição no
  delta-rs (`to_pyarrow_dataset`) e no `delta_scan` do DuckDB; uma diferença chama
  `restore(version - 1)` e levanta `RegistrationRefused`.
- **`version_diff`** lê os arquivos `_delta_log/<versão>.json` de `published + 1` a `current` pelo
  `Storage` e recolhe `partitionValues` das ações `add` e `remove` com `dataChange` verdadeiro; a
  compactação grava `dataChange` falso e não conta (rascunho abaixo), o que evita recarregar no
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
- **`rewrite`** abre uma conexão DuckDB própria, configurada por `duckdb_setup`, roda `COPY (SELECT
  <colunas do contrato, sem a de partição> FROM delta_scan(uri)) TO uri (FORMAT parquet, PARTITION_BY
  (<coluna>), APPEND true, FILENAME_PATTERN 'rewrite_{uuid}', RETURN_STATS)` e registra tudo num
  `create_write_transaction(mode="overwrite", schema=delta_schema(table))`; é o caminho medido em
  [`delta.md`](delta.md) com memória constante.
- **`snapshot`** lê o arquivo de controle com a impressão, recusa um nome repetido, grava com
  `if_match` (ou `if_none_match` no primeiro) e devolve o controle novo; `ConflictError` sobe.
- **`vacuum_keeping_snapshots`** monta `keep_versions` das versões do controle para a tabela e
  chama `vacuum(retention_hours, enforce_retention_duration=False, dry_run=not apply,
  keep_versions=...)`; `full=True` inclui os órfãos. Dentro da retenção nada é listado, mesmo com
  versões intermediárias: a retenção de 400 dias é a janela em que toda versão continua legível.
- **`compact`** é `optimize.compact(partition_filters=[(coluna, "in", partitions)])`; a operação
  com um só arquivo na partição não commita, e o chamador lê a versão antes e depois.
- **`deep_copy`** grava `write_deltalake(destination, DeltaTable(uri, version=v).to_pyarrow_dataset().scanner().to_reader(), mode="overwrite", partition_by=...)`,
  nunca `to_pyarrow_table`, pela regra de encerramento de [`PLAN.md`](PLAN.md).
- **`export_snapshot`** copia os arquivos que `get_add_actions()` lista no layout
  `<coluna>=<valor>/` por `Storage.copy` (`mode="copy"`), ou reescreve pelo `COPY` particionado do
  DuckDB (`mode="rewrite"`); um snapshot antigo usa `DeltaTable(uri, version=v)`.

## Pré-requisitos e pós-condições

| Primitiva | Pré-requisitos | Pós-condições |
| --- | --- | --- |
| `write_text` | `if_match` com a impressão da última leitura, ou `if_none_match` num caminho ausente. | O arquivo gravado por inteiro e a impressão nova devolvida; `ConflictError` sem alteração quando a condição falha. |
| `storage_options`, `duckdb_setup` | Região em `AWS_REGION` ou `AWS_DEFAULT_REGION` para o S3; extensões na pasta configurada. | Opções sem credencial alguma e secret de cadeia; nenhum download de extensão. |
| `create_table` | Modelo aprovado por `check_models`. | Tabela na versão 0 com o esquema Delta do contrato, a partição, as retenções, o nome e o comentário da tabela em `description`; a chamada repetida não muda a versão. |
| `publish_partition` | `data` passado por `cast`, com a coluna de partição; a versão atual da tabela sem dados novos desde a fixada (conferido por `Execution.publish`). | Uma versão nova com os arquivos da partição e os metadados de commit; as demais partições intactas; `ExecutionConflict` sem commit no conflito. |
| `register_files` | Arquivos gravados dentro da pasta da tabela, com o rodapé legível. | Um commit `overwrite` da partição com uma ação por arquivo, ou `RegistrationRefused` sem commit e com o arquivo e a conferência na mensagem. |
| `read_back` | Um commit recém-feito. | Contagem e extremos da chave iguais nos dois leitores, ou `restore(version - 1)` e `RegistrationRefused`. |
| `reconcile` | Tabela existente. | O diff aditivo aplicado em commits de metadados, comentários incluídos; `SchemaDiffRefused` sem alteração no destrutivo; a versão anterior continua legível com o esquema antigo. |
| `rewrite` | Ordem explícita fora da execução mensal. | Um commit com `remove` de todos os arquivos vivos, `add` dos novos e `metaData`; memória constante. |
| `version_diff` | `published <= current`; os arquivos do log das duas versões presentes. | O conjunto das partições com dados alterados; vazio para `published == current` e para uma compactação; `LogUnavailable` quando um arquivo do log falta. |
| `snapshot` | Nome inédito. | A entrada gravada com escrita condicional; `ConflictError` quando outro escritor mudou o arquivo entre a leitura e a escrita. |
| `vacuum_keeping_snapshots` | Controle lido. | A lista dos arquivos fora da retenção e fora das versões presas; com `apply=True`, apagados, e a versão do snapshot continua legível. |

## Testes por caso

`tests/test_storage.py` e `tests/test_delta.py` sob a raiz local; os mesmos casos no bucket por
`-m s3`.

| Caso | Teste | O que confere |
| --- | --- | --- |
| Armazenamento por URI | `test_storage_for_uri` (sem gravar) | `s3://`, `file://` e caminho dão a classe certa; outra URI é erro. |
| Escrita condicional | `test_write_text_exclusive_create_and_if_match` | A segunda criação exclusiva e o `if_match` velho são `ConflictError`; o conteúdo final é o da escrita que venceu. |
| Listagem, cópia e exclusão | `test_list_copy_delete` | `list_files` exclui `_delta_log/`; `copy` preserva bytes; `delete` de caminho ausente não falha. |
| Opções do delta-rs | `test_storage_options_resolved_per_call` | Duas chamadas devolvem dicionários novos; a região vem da variável; `max_retries` presente; nenhuma chave de credencial no dicionário. |
| Ambiente | `test_prepare_environment` | Os casos de `test_stdlib.py`, com `NO_PROXY` vazia tratada como ausente. |
| Criação | `test_create_table_is_idempotent` | Versão 0 nas duas chamadas; esquema, partição, retenções, nome e o comentário da tabela em `description` lidos do log. |
| Substituição | `test_publish_partition_replaces_only_its_partition` | Duas partições, a segunda republicada: a primeira intacta, uma versão por chamada, os metadados no `history`. |
| Sem partição | `test_publish_partition_without_partition_replaces_the_table` | `value=None` troca a tabela inteira. |
| Conflito | `test_two_writers_on_the_same_partition_conflict` | Dois `overwrite` da mesma partição na mesma versão: o segundo é `ExecutionConflict`; partições distintas passam. |
| Registro | `test_register_files_registers_an_unload_like_file` | Um arquivo `INT96` e `FIXED_LEN_BYTE_ARRAY` registrado; os dois leitores devolvem as linhas e `timestamp[us]`. |
| Conferências | `test_register_files_refuses_each_defect`, parametrizado | Tamanho, contagem, partição do caminho, coluna `NOT NULL` ausente, tipo físico fora dos admitidos e `expected_rows` diferente; a versão não muda e o arquivo fica órfão. |
| Releitura | `test_read_back_restores_on_a_difference` | Uma estatística falsa injetada faz `read_back` voltar a versão. |
| Estatísticas | `test_registered_stats_prune_files` | `EXPLAIN ANALYZE` do DuckDB mostra `Scanning Files: 0/n` para uma chave acima do máximo registrado; as colunas `decimal` e `timestamp` entram sem mínimo e máximo. |
| Reconciliação aditiva | `test_reconcile_adds_nullable_column_and_relaxes_not_null` | A coluna entra no fim; as linhas antigas leem nulo; a versão anterior lê o esquema antigo. |
| Documentação | `test_reconcile_syncs_description_and_comments` | Um comentário de tabela e um de coluna alterados no modelo entram por commit de `metaData`; a segunda chamada não commita. |
| Reconciliação destrutiva | `test_reconcile_refuses_destructive_diff` | Tipo trocado, coluna removida e `NOT NULL` nova em tabela com dados são `SchemaDiffRefused`, sem commit. |
| Reescrita | `test_rewrite_in_one_commit_keeps_previous_version_readable` | Um commit, o esquema novo, as somas iguais e a versão anterior legível. |
| Diferença de versões | `test_version_diff_counts_data_changes_only` | Substituição e remoção contam, compactação não, `published == current` dá vazio. |
| Log ausente | `test_version_diff_refuses_a_cleaned_log` | Um arquivo do log apagado entre as duas versões dá `LogUnavailable`, com a publicação completa na mensagem. |
| Snapshot | `test_snapshot_control_file_is_written_conditionally` | O nome repetido é erro; a escrita concorrente é `ConflictError`. |
| `vacuum` | `test_vacuum_keeps_snapshot_versions` | Com retenção zero e `keep_versions`, a versão do snapshot lê e a intermediária falha; dentro da retenção nada é listado. |
| Compactação | `test_compact_before_snapshot` | Arquivos pequenos de uma partição virando um; a partição com um arquivo não commita. |
| Exportação | `test_export_snapshot_copy_and_rewrite` | Os dois modos produzem `<coluna>=<valor>/` com as mesmas linhas; `copy` não lê dados. |
| Realocação | `test_folder_relocates` | A pasta copiada abre na mesma versão, nos dois leitores. |

## Rascunhos executados

Os dois rascunhos rodaram em 2026-09-21 com as versões fixadas, numa pasta temporária. O primeiro
é o armazenamento local; o segundo, a camada Delta com o registro de um arquivo gravado como o
`UNLOAD` grava (`INT96`, `FIXED_LEN_BYTE_ARRAY`), as recusas, `version_diff` pelo log e a
reconciliação. O `delta_scan` precisa da extensão `delta` na pasta de `SERIALIZE_DB_DUCKDB_EXTENSIONS`,
ou em `.duckdb/` da raiz do repositório.

```python
"""Etapa 3: o armazenamento local com a escrita condicional que o arquivo de controle exige, e a escolha por URI."""
import dataclasses
import hashlib
import os
import shutil
import tempfile
from pathlib import Path
from urllib.parse import urlparse


class ConflictError(Exception):
    """A escrita condicional perdeu: outro escritor mudou o objeto (412 no S3, impressão digital diferente na pasta local)."""


@dataclasses.dataclass(frozen=True)
class Storage:
    root: str

    @staticmethod
    def for_uri(uri: str) -> "Storage":
        parsed = urlparse(uri)
        if parsed.scheme == "s3":
            return S3Storage(uri.rstrip("/"))
        if parsed.scheme in ("", "file"):
            return LocalStorage(str(Path(parsed.path if parsed.scheme else uri).expanduser().resolve()))
        raise ValueError(f"URI sem armazenamento conhecido: {uri}")

    def join(self, *parts: str) -> str:
        return "/".join([self.root, *[part.strip("/") for part in parts if part]])


class S3Storage(Storage):
    """PutObject com IfNoneMatch='*' ou IfMatch=<etag>; 412 vira ConflictError; a etag é a impressão digital."""


class LocalStorage(Storage):
    @staticmethod
    def fingerprint(path: str) -> str:
        return hashlib.sha256(Path(path).read_bytes()).hexdigest()

    def read_text(self, path: str) -> tuple[str, str]:
        """O texto e a impressão digital que write_text(if_match=...) exige de volta."""
        target = self.join(path)
        return Path(target).read_text(encoding="utf-8"), self.fingerprint(target)

    def exists(self, path: str) -> bool:
        return Path(self.join(path)).exists()

    def write_text(self, path: str, text: str, if_match: str | None = None, if_none_match: bool = False) -> str:
        target = Path(self.join(path))
        target.parent.mkdir(parents=True, exist_ok=True)
        if if_none_match:
            try:
                descriptor = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL)   # criação exclusiva: falha se existe
            except FileExistsError:
                raise ConflictError(f"{path} já existe") from None
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                handle.write(text)
            return self.fingerprint(target)
        if if_match is not None and (not target.exists() or self.fingerprint(target) != if_match):
            raise ConflictError(f"{path} mudou desde a leitura")
        with tempfile.NamedTemporaryFile("w", dir=target.parent, delete=False, encoding="utf-8") as handle:
            handle.write(text)
        os.replace(handle.name, target)        # substituição atômica no mesmo sistema de arquivos
        return self.fingerprint(target)

    def list_files(self, prefix: str, suffix: str = "") -> list[str]:
        base = Path(self.join(prefix))
        return sorted(str(p.relative_to(self.root)) for p in base.rglob(f"*{suffix}") if p.is_file() and "_delta_log" not in p.relative_to(base).parts)

    def copy(self, source: str, destination: str) -> None:
        target = Path(self.join(destination))
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(self.join(source), target)

    def delete(self, paths: list[str]) -> None:
        for path in paths:
            Path(self.join(path)).unlink(missing_ok=True)


print(type(Storage.for_uri("s3://bucket/projeto/delta")).__name__, type(Storage.for_uri("file:///dados/prod")).__name__, type(Storage.for_uri("/dados/prod")).__name__)
with tempfile.TemporaryDirectory() as folder:
    storage = Storage.for_uri(folder)
    control = "prod/_serialize_db/snapshots.json"
    first = storage.write_text(control, '{"snapshots": {}}', if_none_match=True)
    try:
        storage.write_text(control, "{}", if_none_match=True)
    except ConflictError as error:
        print("segunda criação exclusiva:", error)
    text, fingerprint = storage.read_text(control)
    assert fingerprint == first
    second = storage.write_text(control, '{"snapshots": {"2026T3": {"cad_contas": 1}}}', if_match=fingerprint)
    try:
        storage.write_text(control, "{}", if_match=fingerprint)   # a impressão digital ficou velha
    except ConflictError as error:
        print("escrita com if_match velho:", error)
    print("conteúdo final:", storage.read_text(control)[0], "| impressão nova difere:", second != first)
    storage.copy(control, "arquivo/snapshots.json")
    print("listagem:", storage.list_files("", ".json"))
    storage.delete(["arquivo/snapshots.json"])
    print("depois do delete:", storage.list_files("", ".json"))
```

Saída:

```
S3Storage LocalStorage LocalStorage
segunda criação exclusiva: prod/_serialize_db/snapshots.json já existe
escrita com if_match velho: prod/_serialize_db/snapshots.json mudou desde a leitura
conteúdo final: {"snapshots": {"2026T3": {"cad_contas": 1}}} | impressão nova difere: True
listagem: ['arquivo/snapshots.json', 'prod/_serialize_db/snapshots.json']
depois do delete: ['prod/_serialize_db/snapshots.json']
```

```python
"""Etapa 3: create_table, publish_partition, register_files com as conferências, version_diff pelo log e a reconciliação."""
import dataclasses
import datetime as dt
import decimal
import json
import os
import tempfile
import time
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from deltalake import CommitProperties, DeltaTable, Schema as DeltaSchema, write_deltalake
from deltalake.exceptions import CommitFailedError
from deltalake.schema import Field
from deltalake.transaction import AddAction

PARTITION, SOURCE = "data_str", "data"
CONTRACT = pa.schema([
    pa.field("id_operacao", pa.int64(), nullable=False), pa.field(SOURCE, pa.date32(), nullable=False),
    pa.field("valor", pa.decimal128(18, 2), nullable=False), pa.field("carimbo", pa.timestamp("us")),
    pa.field("descricao", pa.string()), pa.field(PARTITION, pa.string(), nullable=False)])
PHYSICAL_ALLOWED = {"timestamp[us]": {"INT64", "INT96"}, "decimal128(18, 2)": {"INT64", "FIXED_LEN_BYTE_ARRAY"}, "int64": {"INT64", "INT32"},
                    "date32[day]": {"INT32"}, "string": {"BYTE_ARRAY"}}


class ExecutionConflict(Exception):
    """Outra execução gravou a mesma partição ou avançou a tabela: CommitFailedError do delta-rs."""


class RegistrationRefused(Exception):
    """Uma conferência de register_files reprovou; nada foi commitado e o arquivo fica órfão até vacuum(full=True)."""


@dataclasses.dataclass(frozen=True)
class RegisteredFile:
    """Um arquivo que outro escritor gravou dentro da pasta da tabela, como o COPY do DuckDB ou o UNLOAD o descrevem."""
    path: str                      # relativo à pasta da tabela, como o log guarda
    size: int
    rows: int
    stats: dict[str, dict]         # {"min": {col: v}, "max": {col: v}, "null_count": {col: n}}, tipados; ausentes são omitidos


def create_table(uri: str, schema: pa.Schema, partition_by: str | None, name: str) -> DeltaTable:
    return DeltaTable.create(uri, DeltaSchema.from_arrow(schema), partition_by=[partition_by] if partition_by else None, name=name, mode="ignore",
                             configuration={"delta.logRetentionDuration": "interval 3650 days", "delta.deletedFileRetentionDuration": "interval 400 days"})


def commit_metadata(execution_id: str, input_versions: dict[str, int], snapshot: str | None = None) -> dict[str, str]:
    metadata = {"serialize_db_execution_id": execution_id, "serialize_db_input_versions": json.dumps(input_versions, sort_keys=True)}
    return metadata | ({"serialize_db_snapshot": snapshot} if snapshot else {})


def publish_partition(uri: str, partition_by: str | None, value: str | None, data, metadata: dict[str, str]) -> int:
    """overwrite da partição (ou da tabela, sem partição) num commit com os metadados; devolve a versão nova."""
    predicate = f"{partition_by} = '{value}'" if partition_by else None
    try:
        write_deltalake(uri, data, mode="overwrite", predicate=predicate, commit_properties=CommitProperties(custom_metadata=metadata))
    except CommitFailedError as error:
        raise ExecutionConflict(f"{uri} partição {value}: {error}") from None
    return DeltaTable(uri).version()


def register_files(uri: str, table: DeltaTable, files: list[RegisteredFile], partition_by: str | None, value: str | None,
                   metadata: dict[str, str], expected_rows: int | None = None) -> int:
    """As conferências da etapa 3 sobre o rodapé de cada arquivo, depois um único commit overwrite da partição."""
    contract = pa.schema(table.schema())
    total = 0
    for file in files:
        full = os.path.join(uri, file.path)
        if not os.path.exists(full) or os.path.getsize(full) != file.size:                                   # 1. existe, com o tamanho da ação
            raise RegistrationRefused(f"{file.path}: ausente ou com tamanho diferente de {file.size}")
        footer = pq.ParquetFile(full)
        physical = {footer.schema.column(i).name: footer.schema.column(i).physical_type for i in range(len(footer.schema))}
        for field in contract:                                                                                 # 2. esquema nome a nome
            if field.name == partition_by:
                continue
            if field.name not in physical:
                raise RegistrationRefused(f"{file.path}: coluna {field.name} do contrato ausente do arquivo")
            if physical[field.name] not in PHYSICAL_ALLOWED[str(field.type)]:
                raise RegistrationRefused(f"{file.path}: {field.name} em {physical[field.name]}, fora de {PHYSICAL_ALLOWED[str(field.type)]}")
        if partition_by and f"{partition_by}={value}" not in file.path.split("/"):                             # 3. a partição do caminho
            raise RegistrationRefused(f"{file.path}: caminho fora da partição {partition_by}={value}")
        if footer.metadata.num_rows != file.rows:                                                              # 4. as linhas do rodapé
            raise RegistrationRefused(f"{file.path}: {footer.metadata.num_rows} linhas no rodapé, {file.rows} declaradas")
        total += file.rows
    if expected_rows is not None and total != expected_rows:
        raise RegistrationRefused(f"{total} linhas nos arquivos, {expected_rows} na fonte")
    actions = [AddAction(path=f.path, size=f.size, partition_values={partition_by: value} if partition_by else {}, modification_time=int(time.time() * 1000),
                         data_change=True, stats=json.dumps({"numRecords": f.rows, "minValues": f.stats.get("min", {}), "maxValues": f.stats.get("max", {}),
                                                              "nullCount": f.stats.get("null_count", {})}, default=str)) for f in files]
    table.create_write_transaction(actions, mode="overwrite", schema=table.schema(), partition_by=[partition_by] if partition_by else None,
                                   partition_filters=[(partition_by, "=", value)] if partition_by else None, commit_properties=CommitProperties(custom_metadata=metadata))
    return DeltaTable(uri).version()


def version_diff(uri: str, published: int, current: int) -> set[str]:
    """As partições com add ou remove de dados entre as duas versões, pelo log; a compactação (dataChange falso) não conta."""
    touched = set()
    for version in range(published + 1, current + 1):
        for line in Path(uri, "_delta_log", f"{version:020d}.json").read_text().splitlines():
            action = json.loads(line)
            for kind in ("add", "remove"):
                if kind in action and action[kind].get("dataChange", True):
                    touched.add(action[kind].get("partitionValues", {}).get(PARTITION))
    return touched - {None}


def schema_diff(contract: pa.Schema, current: pa.Schema) -> dict[str, list]:
    diff = {"add": [], "relax": [], "destructive": []}
    for field in contract:
        existing = current.field(field.name) if field.name in current.names else None
        if existing is None:
            (diff["destructive"] if not field.nullable else diff["add"]).append(field.name if field.nullable else f"{field.name} NOT NULL nova")
        elif existing.type != field.type:
            diff["destructive"].append(f"{field.name}: {existing.type} -> {field.type}")
        elif not existing.nullable and field.nullable:
            diff["relax"].append(field.name)
    diff["destructive"] += [f"{name} removida" for name in current.names if name not in contract.names]
    return diff


def reconcile(uri: str, contract: pa.Schema) -> dict[str, list]:
    table = DeltaTable(uri)
    diff = schema_diff(contract, pa.schema(table.schema()))
    if diff["destructive"]:
        raise ValueError(f"diff destrutivo, só por rewrite: {diff['destructive']}")
    if diff["add"]:
        table.alter.add_columns([Field(name, DeltaSchema.from_arrow(pa.schema([contract.field(name)])).fields[0].type, nullable=True) for name in diff["add"]])
    for name in diff["relax"]:
        table.alter.drop_column_not_null(name)
    return diff


def rows(value: str, start: int, n: int) -> pa.Table:
    day = dt.date.fromisoformat(value)
    return pa.table({"id_operacao": pa.array(range(start, start + n), pa.int64()), SOURCE: pa.array([day] * n, pa.date32()),
                     "valor": pa.array([decimal.Decimal(k) / 100 for k in range(start, start + n)], pa.decimal128(18, 2)),
                     "carimbo": pa.array([dt.datetime(2026, 8, 31, 12)] * n, pa.timestamp("us")), "descricao": pa.array([f"op {k}" for k in range(start, start + n)]),
                     PARTITION: pa.array([value] * n)}, schema=CONTRACT)


with tempfile.TemporaryDirectory() as folder:
    uri = os.path.join(folder, "prod", "cad_operacoes")
    create_table(uri, CONTRACT, PARTITION, "cad_operacoes")
    assert create_table(uri, CONTRACT, PARTITION, "cad_operacoes").version() == 0      # idempotente
    metadata = commit_metadata("exec-2026-09-05", {"cad_contratos": 88})
    v1 = publish_partition(uri, PARTITION, "2026-07-31", rows("2026-07-31", 1, 100), metadata)
    v2 = publish_partition(uri, PARTITION, "2026-08-31", rows("2026-08-31", 101, 100), metadata)
    v3 = publish_partition(uri, PARTITION, "2026-08-31", rows("2026-08-31", 101, 100), metadata)   # a repetição substitui a mesma partição
    print("versões:", v1, v2, v3, "| linhas:", DeltaTable(uri).to_pyarrow_dataset().count_rows(), "| metadados:", DeltaTable(uri).history(1)[0].get("serialize_db_execution_id"))

    # Um arquivo gravado por outro escritor: INT96 no timestamp e FIXED_LEN_BYTE_ARRAY no decimal, como o UNLOAD grava.
    external = rows("2026-09-30", 201, 50).drop_columns([PARTITION])
    relative = f"exec-2026-09-05/{PARTITION}=2026-09-30/0000_part_00.parquet"
    Path(uri, relative).parent.mkdir(parents=True)
    pq.write_table(external, Path(uri, relative), use_deprecated_int96_timestamps=True)
    physical = {pq.ParquetFile(Path(uri, relative)).schema.column(i).physical_type for i in range(6 - 1)}
    print("tipos físicos do arquivo externo:", sorted(physical))
    good = RegisteredFile(relative, Path(uri, relative).stat().st_size, 50, {"min": {"id_operacao": 201}, "max": {"id_operacao": 250}, "null_count": {"id_operacao": 0}})
    v4 = register_files(uri, DeltaTable(uri), [good], PARTITION, "2026-09-30", metadata, expected_rows=50)
    read_back = duckdb.connect(config={"extension_directory": os.environ.get("SERIALIZE_DB_DUCKDB_EXTENSIONS", ".duckdb"), "autoinstall_known_extensions": False, "autoload_known_extensions": False})
    read_back.execute("LOAD delta")
    print("versão", v4, "| delta-rs:", DeltaTable(uri).to_pyarrow_dataset().count_rows(), "| delta_scan:", read_back.execute(f"SELECT count(*), max(id_operacao), typeof(carimbo) FROM delta_scan('{uri}') GROUP BY ALL").fetchall())

    for description, bad in {
        "tamanho errado": dataclasses.replace(good, size=good.size + 1),
        "linhas erradas": dataclasses.replace(good, rows=49),
        "partição do caminho": good,                                                 # o arquivo existe sob 2026-09-30 e é registrado como 2026-10-31
    }.items():
        try:
            register_files(uri, DeltaTable(uri), [bad], PARTITION, "2026-10-31" if "partição" in description else "2026-09-30", metadata)
        except RegistrationRefused as error:
            print(f"recusa ({description}): {str(error)[:90]}")
    missing = external.drop_columns(["valor"])                                       # sem uma coluna NOT NULL do contrato
    other = f"exec-2026-09-05/{PARTITION}=2026-09-30/0001_part_00.parquet"
    pq.write_table(missing, Path(uri, other))
    try:
        register_files(uri, DeltaTable(uri), [RegisteredFile(other, Path(uri, other).stat().st_size, 50, {})], PARTITION, "2026-09-30", metadata)
    except RegistrationRefused as error:
        print(f"recusa (coluna ausente): {str(error)[:90]}")
    print("versão depois das recusas:", DeltaTable(uri).version(), "| órfãos na pasta:", len([p for p in Path(uri).rglob('*.parquet') if '_delta_log' not in p.parts]) - len(DeltaTable(uri).file_uris()))

    # version_diff pelo log: a compactação (dataChange falso) não conta, a substituição e a remoção contam.
    write_deltalake(uri, rows("2026-08-31", 301, 10), mode="append")                                  # um segundo arquivo na partição, para haver o que compactar
    v4b = DeltaTable(uri).version()
    DeltaTable(uri).optimize.compact(partition_filters=[(PARTITION, "=", "2026-08-31")])
    v5 = DeltaTable(uri).version()
    compaction = [json.loads(line) for line in Path(uri, "_delta_log", f"{v5:020d}.json").read_text().splitlines()]
    print("dataChange na compactação:", sorted({str(a.get("add", a.get("remove", {})).get("dataChange")) for a in compaction if "add" in a or "remove" in a}))
    DeltaTable(uri).delete(f"{PARTITION} = '2026-07-31'")
    v6 = DeltaTable(uri).version()
    print("version_diff:", sorted(version_diff(uri, v3, v4)), sorted(version_diff(uri, v4b, v5)), sorted(version_diff(uri, v5, v6)), sorted(version_diff(uri, v1, v6)))

    # A reconciliação: coluna anulável nova e NOT NULL relaxado entram; tipo trocado e NOT NULL nova são recusados.
    evolved = pa.schema([f if f.name != "descricao" else pa.field("descricao", pa.string(), nullable=True) for f in CONTRACT] + [pa.field("canal", pa.string())])
    print("reconcile:", reconcile(uri, evolved), "| colunas:", pa.schema(DeltaTable(uri).schema()).names[-2:])
    try:
        reconcile(uri, pa.schema([pa.field("id_operacao", pa.int32(), nullable=False), pa.field("nova", pa.int64(), nullable=False)] + [f for f in evolved if f.name != "id_operacao"]))
    except ValueError as error:
        print("reconcile recusa:", str(error)[:120])
    try:
        publish_partition(uri, PARTITION, "2026-08-31", rows("2026-08-31", 1, 10).append_column("canal", pa.array(["x"] * 10)), metadata)
        DeltaTable(uri, version=v6)   # a versão anterior continua legível
        print("versão anterior legível:", DeltaTable(uri, version=v6).to_pyarrow_dataset().count_rows())
    except ExecutionConflict as error:
        print(error)
```

Saída:

```
versões: 1 2 3 | linhas: 200 | metadados: exec-2026-09-05
tipos físicos do arquivo externo: ['BYTE_ARRAY', 'FIXED_LEN_BYTE_ARRAY', 'INT32', 'INT64', 'INT96']
versão 4 | delta-rs: 250 | delta_scan: [(250, 250, 'TIMESTAMP')]
recusa (tamanho errado): exec-2026-09-05/data_str=2026-09-30/0000_part_00.parquet: ausente ou com tamanho diferente
recusa (linhas erradas): exec-2026-09-05/data_str=2026-09-30/0000_part_00.parquet: 50 linhas no rodapé, 49 declarad
recusa (partição do caminho): exec-2026-09-05/data_str=2026-09-30/0000_part_00.parquet: caminho fora da partição data_st
recusa (coluna ausente): exec-2026-09-05/data_str=2026-09-30/0001_part_00.parquet: coluna valor do contrato ausente
versão depois das recusas: 4 | órfãos na pasta: 2
dataChange na compactação: ['False']
version_diff: ['2026-09-30'] [] ['2026-07-31'] ['2026-07-31', '2026-08-31', '2026-09-30']
reconcile: {'add': ['canal'], 'relax': [], 'destructive': []} | colunas: ['data_str', 'canal']
reconcile recusa: diff destrutivo, só por rewrite: ['id_operacao: int64 -> int32', 'nova NOT NULL nova']
versão anterior legível: 160
```

O rascunho mostra três fatos que entram no plano: o commit da compactação grava `dataChange` falso
nas ações `add` e `remove`, o que `version_diff` usa; uma compactação de partição com um só arquivo
não commita; e `alter.add_columns` recebe o tipo Delta do campo (`Field(name, <PrimitiveType>)`),
não o seu texto.

## Decisões pendentes

Nenhuma: as quatro decisões da etapa foram tomadas pelo usuário em 2026-09-22 — o comentário da
tabela em `description`, com `reconcile` sincronizando a descrição e os comentários de coluna;
`storage_options` sem credencial alguma, pela cadeia padrão do delta-rs; o mínimo e o máximo das
colunas inteiras, de data, `Double` e `String` em `register_files`; e `version_diff` recusando com
`LogUnavailable` o log limpo —, e cada uma está escrita na seção que a descreve. As duas sondagens
que as mediram estão em [`POC.md`](POC.md) e em
`tests/proof_of_concept/test_deltalake.py`.
