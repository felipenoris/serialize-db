# Etapa 4: `audit` e motor DuckDB

A entrega e o critério de aceite desta etapa estão na tabela de etapas de [`PLAN.md`](PLAN.md), que
também fixa as decisões, as regras que toda etapa obedece e a ordem do trabalho.

`serialize_db.audit` monta as verificações a partir do contrato e não sabe qual motor as roda;
`serialize_db.engine` declara o protocolo `Engine`, que a execução também não conhece, e
`serialize_db.engine.duckdb` o implementa. O protocolo fixa os tipos da fronteira: `stream` devolve
um `BatchStream` de `pa.RecordBatch` e `loader` recebe lotes por `write`; `query` e `execute` devolvem
a `pa.Table` que `stream` montou, e `load` entrega ao `loader` os lotes de uma `pa.Table`, de um
`RecordBatch`, de um `RecordBatchReader` ou de um iterável ([`PLAN.md`](PLAN.md), seção "A troca de
dados com o código cliente"). Os esboços `BatchStream` e `Loader` de `test_parallel.py` são a
referência da implementação.

Chave primária, unicidade e chave estrangeira ficam fora do DDL dos dois sandboxes (`schema.md`), e
a auditoria é onde elas são aplicadas. Cada verificação sai do `Table`, sem declaração adicional no
modelo:

| Verificação | De onde sai | Escopo |
| --- | --- | --- |
| Nulo em coluna `NOT NULL` | `column.nullable` | As partições da execução. |
| Chave repetida | `table.primary_key` e os `UniqueConstraint`, mais o que `keys` acrescenta | As partições da execução quando as colunas da chave incluem a coluna de partição; a tabela inteira quando não incluem. |
| Órfão de chave estrangeira | `table.foreign_keys` | Só com `foreign_keys=True`; a tabela referenciada entra na versão fixada pela execução. |
| Partição fora da data | `partition_by` e `partition_source` de `table_options`: a coluna de partição diferente de `strftime(<coluna de data>, '%Y-%m-%d')` | As partições da execução. |
| Texto acima de `String(n)` e valor fora do `Numeric(18, 2)` | os tipos de `schema.md`; no Redshift, o `COPY` de uma string maior que o `VARCHAR` aborta (`Spectrum Scan Error` 15007, 2026-09-21), e esta verificação é a barreira | As partições da execução. |
| Documento JSON inválido | as colunas JSON, que nem o Arrow nem o Delta validam | As partições da execução. |
| Totais de controle | as colunas `Numeric` e `Double`; as `Double` somadas como `DECIMAL(38, 6)` de cada valor, porque a soma em ponto flutuante depende da ordem | As partições da execução. |

Uma chave primária não é por partição: conferi-la só nas partições da execução não é unicidade.
Quando as colunas da chave não incluem a coluna de partição, a verificação compara o sandbox com as
demais partições da versão fixada — `delta_scan(uri, version := v) WHERE data_str NOT IN (...)` no DuckDB, uma
staging só com as colunas da chave, carregada por `COPY ... MANIFEST`, no Redshift. Custa uma
passagem nas colunas da chave da tabela inteira; `key_scope="partition"` a reduz às partições da execução,
e a escolha entra no relatório.

A chave estrangeira precisa da tabela referenciada, e só o que o pipeline usa é ingerido:
`foreign_keys=True` ingere a coluna referenciada das tabelas que faltarem no sandbox, na versão
fixada, e o anti-join roda contra ela. Sem o argumento, órfão nenhum é procurado, e o relatório
registra a verificação como não executada.

| Primitiva | O que faz |
| --- | --- |
| `checks(table, partitions=None, foreign_keys=False, key_scope=None)` | A lista de `Check` (nome, statement Core, o que reprova): os defeitos de linha num `count(*) FILTER` por coluna na mesma passagem, uma consulta por chave e uma por chave estrangeira. |
| `audit_sql(table, dialect, **opcoes)` | `{nome: texto}` por `sql.render`, sem conexão e sem motor: o SQL que a auditoria vai rodar, para depuração. |
| `audit_files(metadata, **opcoes)` e `write_audit_files(metadata, directory, **opcoes)` | `{"<tabela>.audit.duckdb.sql": ..., "<tabela>.audit.redshift.sql": ...}` em memória e gravados, como os arquivos de `schema` e de `sql`; versionar a pasta é opcional e faz um modelo alterado aparecer no diff. |
| `AuditReport` | Por verificação: nome, o SQL rodado, a contagem de defeitos, uma amostra das linhas reprovadas e o veredito; `passed` é a conjunção, e `report.sql()` devolve o texto de todas. |

| Primitiva | DuckDB |
| --- | --- |
| `connect(config)` | Banco em arquivo `<pasta temporária>/<execution_id>.duckdb` ou em memória; `extension_directory` de `SERIALIZE_DB_DUCKDB_EXTENSIONS` (ou `.duckdb/` da pasta preparada), `autoinstall_known_extensions` e `autoload_known_extensions` desligados; `storage.duckdb_setup`; `threads`, `memory_limit`, `temp_directory` e `preserve_insertion_order = false`; `temp_directory` sempre explícito, com o espaço livre conferido e registrado no log, porque o padrão `.tmp` é relativo à pasta corrente; uma conexão por thread, o `cursor()` da raiz guardado num `threading.local` no primeiro uso e fechado em `cleanup`, e `connection` devolve a da thread. |
| `ingest(table, uri, version, partitions=None, materialize=False)` | View com o nome do modelo sobre `delta_scan(uri, version := v)`, ou `CREATE TABLE ... AS SELECT ... FROM delta_scan(...) WHERE data_str IN (...)` com `materialize=True`. |
| `stream(statement_or_sql, params=None, batch_size=100_000, prefetch=2)` | Um statement Core compilado para o dialeto, com as tabelas do contrato trocadas pelas do sandbox por `sql.prefixed`, ou o texto gerado por `render` ou escrito pelo pipeline, com `{prefix}` vazio e `:nome` em `$nome` por `sql.bind`; roda num `cursor()` próprio, sem `Session`, e devolve o `BatchStream`: iterável de `pa.RecordBatch` com os tipos do motor (`decimal128(18, 2)`, `date32`, JSON como `string`), `schema`, `read_next_batch`, `read_all`, `close`, gerenciador de contexto e `__arrow_c_stream__` (para `write_deltalake` e `RecordBatchReader.from_stream`, nunca para o `register` do DuckDB). Uma thread auxiliar puxa `prefetch` lotes de `to_arrow_reader(batch_size)` para uma fila limitada, com esperas com prazo e sem referência ao stream; `prefetch=0` dispensa a thread; `close` a interrompe e fecha o cursor; o erro da consulta chega na construção ou na leitura seguinte. |
| `query(statement, **params)` | `stream(statement, params).read_all()`: a `pa.Table` com os tipos do motor. |
| `execute(sql, params)` | `stream(sql, params).read_all()`; um comando sem resultado devolve a tabela `Count` ou `Success` do DuckDB. |
| `loader(table, queue_depth=2)` | O gerenciador de contexto que grava lotes numa tabela do sandbox criada por `ddl(table, "duckdb")`: `write(data)` aceita `pa.RecordBatch` ou `pa.Table`, faz `cast(batch, table)` na thread do cliente e põe o lote numa fila limitada; uma thread auxiliar, num `cursor()` próprio e numa transação explícita, registra cada lote e roda `INSERT ... BY NAME SELECT * FROM <lote>`, `commit` no `close` e `rollback` em qualquer erro, inclusive uma exceção dentro do `with` ou um loader abandonado; `close` relança o erro da thread; `rows` conta as linhas gravadas. Nada é visível antes do `commit`, e nenhum gerador Python é entregue ao DuckDB. |
| `load(table, data)` | `data` é uma `pa.Table`, um `pa.RecordBatch`, um `RecordBatchReader` ou um iterável de lotes: `with loader(table) as l: for batch in ...: l.write(batch)`; outro tipo é recusado com a mensagem que aponta `pa.Table.from_pandas` e `pa.RecordBatch.from_pandas`. |
| `audit(table, partitions, **opcoes)` | Roda o texto de `audit.audit_sql(table, "duckdb")` — `json_valid` e `strftime(data, '%Y-%m-%d')` são as funções do dialeto — e monta o `AuditReport`; a comparação com as demais partições sai de `delta_scan` na versão fixada, e `passed` falso interrompe a execução. |
| `export_partition(table, uri, value, metadata, mode=None, expected_rows=None)` | `mode` é a flag `export_mode` da [etapa 6](PLAN-STAGE-6.md) (`register` ou `rewrite`, `SERIALIZE_DB_EXPORT_MODE` por omissão, `register` sem ela), a mesma do motor Redshift ([etapa 5](PLAN-STAGE-5.md)) e da carga inicial ([etapa 7](PLAN-STAGE-7.md)). **`register`**: `COPY (SELECT <colunas do contrato, sem a de partição> FROM <sandbox> WHERE <coluna de partição> = '<valor>') TO '<uri>/<coluna de partição>=<valor>/<execution_id>_<uuid>.parquet' (FORMAT parquet, RETURN_STATS)` mais `register_files` com as estatísticas de `RETURN_STATS`, as conferências da [etapa 3](PLAN-STAGE-3.md) antes do commit e a releitura depois; a memória é constante (600 MB na reescrita medida em [`delta.md`](delta.md)). O `uuid` no nome impede uma reexecução com o mesmo `execution_id` de sobrescrever o arquivo que a versão anterior referencia. **`rewrite`**: o `RecordBatchReader` da partição, passado por `cast`, para `publish_partition`, num cursor próprio; o `write_deltalake` calcula estatística e nulidade, e a memória cresce com a partição (1.140 MB para 135 MB de Parquet na mesma medição). A mesma partição sai igual pelos dois, e o teste os compara. |
| `cleanup()` | Fecha a conexão e apaga o arquivo do banco e a pasta de transbordo. |

Testes: `tests/test_audit.py`, sem gravar: o texto de cada verificação nos dois dialetos, o diff dos
arquivos gerados, a chave lida do `primary_key` do modelo e o escopo escolhido pelas colunas da
chave. `tests/test_engine_duckdb.py` sob a raiz local: o pipeline de exemplo (doze partições
materializadas e dimensões em view, um `select` com `join`, auditoria, exportação da partição nos dois modos com as mesmas linhas) sobre um
Delta local criado no teste; o ciclo `query`, `to_pandas(types_mapper=pd.ArrowDtype)`, `from_pandas` e
`load` com `decimal128(18, 2)` e `date32` mantidos, e `load` recusando um DataFrame; o ciclo por
lotes: `stream` com `prefetch` enquanto o cursor da thread roda outros comandos, `stream` fechado no
meio, `loader` que desfaz na exceção do cliente e no lote recusado pelo `cast`, `load` de um
iterável e de uma `pa.Table` com o mesmo resultado, e `query` igual a `stream(...).read_all()`;
auditoria que reprova a chave repetida dentro da partição e a repetida contra uma partição já
publicada, e o órfão de chave estrangeira com a tabela referenciada fora do sandbox; `execute` com
`%` em literal. Provas de conceito: `test_duckdb.py` (configuração, Arrow na entrada e na saída, o
leitor esvaziado pelo comando seguinte e preservado num cursor próprio, a consulta em streaming com
a memória medida em subprocesso, o `INSERT` sobre um leitor de gerador com a leitura antecipada e
os lotes que o leitor não confere, `DECIMAL`, JSON, `executemany`, `COPY` com `RETURN_STATS` e
particionado, banco em arquivo, `test_audit_queries`), `poc_delta.py`
(`delta_scan`, poda, tempos, `ATTACH ... PIN_SNAPSHOT`),
`test_deltalake.py::test_duckdb_view_pins_version_and_reader_feeds_write`
(a view presa a `version := v` e o `to_arrow_reader()` no `write_deltalake` com predicado),
`test_sqlalchemy.py::test_arrow_path_on_raw_connection`,
`test_stdlib.py::test_engine_protocol_and_config_dataclass` e `test_concurrency.py` (o GIL liberado
pelo DuckDB, pelo delta-rs e pelo PyArrow, um `cursor()` por thread, a conexão compartilhada que troca os
resultados das threads, o arquivo do DuckDB recusado com outra configuração, dois comandos em dois cursores,
o intervalo de troca do GIL pago por cada retomada ao lado de uma thread Python ocupada) e
`test_parallel.py` (o motor com um cursor por thread, os esboços `BatchStream` e `Loader` com o
pipeline de três estágios, a ingestão de quatro tabelas por `delta_scan` em paralelo, cargas Arrow e
`COPY ... TO` em paralelo) e `test_pyarrow.py::test_record_batch_cast_and_conversions_share_buffers`.

## Interface

```python
"""Assinaturas de serialize_db.audit, serialize_db.engine e serialize_db.engine.duckdb; os corpos estão nos rascunhos abaixo."""
import dataclasses
from collections.abc import Iterable, Iterator, Mapping
from typing import Literal, Protocol, runtime_checkable

import duckdb
import pyarrow as pa
import sqlalchemy as sa

Dialect = Literal["duckdb", "redshift"]
ExportMode = Literal["register", "rewrite"]


@dataclasses.dataclass(frozen=True)
class Check:
    name: str                        # "linhas", "chave_<colunas>", "chave_<colunas>_publicada", "orfao_<colunas>"
    statement: sa.Select
    fails_when: str                  # "algum contador acima de zero" ou "alguma linha"


@dataclasses.dataclass(frozen=True)
class CheckResult:
    name: str
    sql: str
    defects: int
    sample: pa.Table                 # até 20 linhas reprovadas
    passed: bool


@dataclasses.dataclass(frozen=True)
class AuditReport:
    table: str
    partitions: tuple[str, ...]
    results: tuple[CheckResult, ...]
    not_run: tuple[str, ...]         # "orfao_*" sem foreign_keys=True

    @property
    def passed(self) -> bool: ...
    def sql(self) -> str: ...


def checks(table: sa.Table, partitions: list[str] | None = None, foreign_keys: bool = False, key_scope: Literal["partition", "table"] | None = None,
           prefix: str = "{prefix}", published: sa.FromClause | None = None, referenced: Mapping[str, sa.FromClause] | None = None) -> list[Check]: ...
def audit_sql(table: sa.Table, dialect: Dialect, **options: object) -> dict[str, str]: ...
def audit_files(metadata: sa.MetaData, **options: object) -> dict[str, str]: ...
def write_audit_files(metadata: sa.MetaData, directory: str, **options: object) -> list[str]: ...


class BatchStream(Protocol):
    schema: pa.Schema
    def read_next_batch(self) -> pa.RecordBatch: ...
    def __iter__(self) -> Iterator[pa.RecordBatch]: ...
    def read_all(self) -> pa.Table: ...
    def close(self) -> None: ...
    def __enter__(self) -> "BatchStream": ...
    def __exit__(self, *exc: object) -> None: ...
    def __arrow_c_stream__(self, requested_schema: object = None) -> object: ...


class Loader(Protocol):
    rows: int
    def write(self, data: pa.RecordBatch | pa.Table) -> None: ...
    def close(self) -> None: ...
    def __enter__(self) -> "Loader": ...
    def __exit__(self, *exc: object) -> None: ...


@runtime_checkable
class Engine(Protocol):
    """A interface dos dois motores; Execution só depende dela."""
    @property
    def connection(self) -> object: ...
    def ingest(self, table: sa.Table, uri: str, version: int, partitions: list[str] | None = None, materialize: bool = False) -> None: ...
    def stream(self, statement_or_sql: sa.sql.ClauseElement | str, params: Mapping[str, object] | None = None, batch_size: int = 100_000, prefetch: int = 2) -> BatchStream: ...
    def query(self, statement: sa.sql.ClauseElement | str, **params: object) -> pa.Table: ...
    def execute(self, sql: str, params: Mapping[str, object] | None = None) -> pa.Table: ...
    def loader(self, table: sa.Table, queue_depth: int = 2) -> Loader: ...
    def load(self, table: sa.Table, data: pa.Table | pa.RecordBatch | pa.RecordBatchReader | Iterable[pa.RecordBatch]) -> int: ...
    def audit(self, table: sa.Table, partitions: list[str] | None, uri: str | None = None, version: int | None = None, **options: object) -> AuditReport: ...
    def export_partition(self, table: sa.Table, uri: str, value: str | None, metadata: Mapping[str, str], mode: ExportMode | None = None, expected_rows: int | None = None) -> int: ...
    def cleanup(self) -> None: ...


@dataclasses.dataclass(frozen=True, kw_only=True)
class DuckDBConfig:
    database: str | None = None      # None: <temp_directory>/<execution_id>.duckdb; ":memory:" só por pedido
    threads: int | None = None
    memory_limit: str | None = None  # None: 60% da memória da máquina, para o Python ter o resto
    temp_directory: str | None = None
    extension_directory: str | None = None


class DuckDBEngine:
    def __init__(self, config: DuckDBConfig, execution_id: str, storage: object) -> None: ...
```

Duas mudanças em relação à tabela acima: `export_partition` recebe `uri`, `metadata` e
`expected_rows`, porque o motor não conhece o `Database` e o commit precisa dos metadados; e
`audit` recebe `uri` e `version` da tabela fixada, para montar `published` (`delta_scan(uri,
version := v)`) e as tabelas referenciadas sem depender de `Execution`.

## Estratégia de implementação

- **`checks`** monta os statements Core sobre a cópia prefixada da tabela (`sql.prefixed`). Uma
  consulta de linhas reúne num `count(*) FILTER` por coluna: nulo em `NOT NULL`, texto acima de
  `String(n)`, JSON inválido, a coluna de partição diferente de `partition_text(partition_source)`,
  e a soma de controle de cada coluna `Double` e `Numeric` como `DECIMAL(38, 6)`. Duas funções
  genéricas com `@compiles` por dialeto fazem a portabilidade: `partition_text` é
  `strftime(x, '%Y-%m-%d')` no DuckDB e `to_char(x, 'YYYY-MM-DD')` no Redshift; `json_valid` fica
  no DuckDB e vira `is_valid_json` no Redshift. Uma consulta por chave (`GROUP BY ... HAVING count(*)
  > 1`) dentro das partições da execução, e, quando a chave não inclui a coluna de partição e
  `key_scope != "partition"`, uma segunda consulta que junta o sandbox com `published` (o
  `delta_scan` da versão fixada, ou a staging no Redshift) fora das partições da execução. Uma
  consulta por chave estrangeira (anti-join contra `referenced[tabela]`) só com
  `foreign_keys=True`; sem ele, o nome entra em `not_run`.
- **`audit_sql`** renderiza cada `Check` por `sql.render` com o prefixo pedido; é o texto que
  `serialize-db audit --sql` imprime e que `audit_files` grava.
- **`AuditReport`** guarda por verificação o SQL rodado, a contagem e uma amostra; `passed` é a
  conjunção; `sql()` concatena os textos para o log da execução.
- **`DuckDBEngine.__init__`** abre a conexão raiz com `extension_directory`,
  `autoinstall_known_extensions` e `autoload_known_extensions` desligados, `threads`,
  `memory_limit`, `temp_directory`, `preserve_insertion_order = false`, e chama
  `storage.duckdb_setup`. O banco é um arquivo em `<temp_directory>/<execution_id>.duckdb` por
  padrão: no ambiente alvo a máquina tem 7,6 GiB de memória, 2 vCPUs e 29,8 GiB livres num só
  disco, e uma tabela materializada de doze partições de `cad_lancamentos` não cabe em memória, cabe
  em disco ([`POC.md`](POC.md), leitura de 2026-09-21). O espaço livre de `temp_directory` é lido por
  `shutil.disk_usage` e vai para o log. `connection` devolve o `cursor()` da thread, criado no
  primeiro uso e guardado num `threading.local`.
- **`ingest`** cria `VIEW <nome do modelo> AS SELECT * FROM delta_scan('<uri>', version := <v>)`,
  ou `TABLE ... WHERE <coluna> IN (...)` com `materialize=True`, na conexão da thread.
- **`stream`** é o `BatchStream` de `test_parallel.py`: um statement Core vira texto por
  `sql.render(prefix="")` e `sql.bind(style="duckdb")`, um texto pronto passa por `bind`; a consulta
  roda num `cursor()` próprio com `to_arrow_reader(batch_size)`, e a thread auxiliar puxa `prefetch`
  lotes para uma fila limitada, com esperas com prazo, sem referência ao stream, encerrada em `close`.
- **`query` e `execute`** são `stream(...).read_all()`.
- **`loader`** é o `Loader` de `test_parallel.py`: `write` faz `cast(batch, table)` na thread do
  cliente e enfileira; a thread auxiliar, num `cursor()` próprio, roda `CREATE TABLE IF NOT EXISTS
  <nome> (ddl)`, `BEGIN`, um `INSERT INTO <nome> BY NAME SELECT * FROM lote` por lote e `COMMIT` no
  `close`; qualquer erro faz `ROLLBACK` e sobe em `close` ou no `write` seguinte. `load` embrulha
  `loader` para `pa.Table`, `RecordBatch`, `RecordBatchReader` e iteráveis, e recusa o resto com a
  mensagem que aponta `pa.Table.from_pandas`.
- **`audit`** roda `audit_sql(table, "duckdb", published=delta_scan(uri, version), ...)` e monta o
  `AuditReport`; `passed` falso não levanta aqui, levanta em `Execution.audit`.
- **`export_partition`** com `mode="register"`: `COPY (SELECT <colunas sem a de partição> FROM
  <sandbox> WHERE <coluna> = '<valor>' ORDER BY <sort_key>) TO '<uri>/<coluna>=<valor>/<execution_id>_<uuid>.parquet'
  (FORMAT parquet, RETURN_STATS)`, a linha do `RETURN_STATS` vira um `RegisteredFile` (contagem,
  tamanho, `null_count` de todas as colunas, `min` e `max` das colunas com transcrição testada), e
  `delta.register_files` faz as conferências e o commit, com `expected_rows` do `count(*)` do
  sandbox. Com `mode="rewrite"`: `stream` da partição, `cast`, `delta.publish_partition`. O modo
  vem do argumento, de `SERIALIZE_DB_EXPORT_MODE` ou de `"register"`.
- **`cleanup`** fecha os cursores das threads e a conexão raiz e apaga o arquivo do banco e a pasta
  de transbordo.

## Pré-requisitos e pós-condições

| Primitiva | Pré-requisitos | Pós-condições |
| --- | --- | --- |
| `checks`, `audit_sql` | Modelo aprovado por `check_models`; `published` informado quando alguma chave não inclui a coluna de partição. | Um `Check` por consulta, com o texto renderizável nos dois dialetos; sem conexão. |
| `DuckDBEngine` | Extensões na pasta configurada; `temp_directory` com espaço conferido. | Uma conexão raiz; um cursor por thread no primeiro uso; nenhum download. |
| `ingest` | Tabela Delta legível na versão pedida. | Uma view ou tabela com o nome do modelo, presa à versão; a tabela atual pode avançar sem afetar a leitura. |
| `stream` | Texto ou statement válido; parâmetros que fecham com o dicionário. | Lotes na ordem da consulta; o cursor da thread do cliente livre; a thread auxiliar encerrada em `close`; o erro da consulta no construtor ou no lote seguinte. |
| `loader`, `load` | Lotes que passam por `cast`. | Nada visível antes do `commit`; tudo desfeito na exceção; `rows` igual às linhas escritas. |
| `audit` | Sandbox com a tabela; `uri` e `version` para as chaves fora da partição. | Um `AuditReport` com o SQL de cada verificação; nenhuma escrita. |
| `export_partition` | Auditoria aprovada (conferida por `Execution.publish`); a pasta da partição gravável. | Uma versão nova no Delta com a partição substituída e as estatísticas registradas; a mesma partição sai igual pelos dois modos. |
| `cleanup` | Nenhum. | Arquivo do banco e pasta de transbordo apagados; chamadas seguintes falham. |

## Testes por caso

`tests/test_audit.py` sem gravar; `tests/test_engine_duckdb.py` sob a raiz local, sobre um Delta
criado no teste a partir do modelo de referência.

| Caso | Teste | O que confere |
| --- | --- | --- |
| Texto das verificações | `test_audit_sql_per_dialect` | Cada `Check` do modelo de referência renderiza nos dois dialetos; `strftime` no DuckDB e `to_char` no Redshift; `json_valid` e `is_valid_json`. |
| Escopo da chave | `test_key_scope_follows_the_partition_column` | Chave com a coluna de partição gera uma consulta; sem ela, gera a segunda contra `published`; `key_scope="partition"` a suprime e o relatório registra. |
| Chave estrangeira | `test_foreign_key_check_only_on_request` | Sem `foreign_keys=True` o nome está em `not_run`; com ele, o anti-join contra `referenced`. |
| Arquivos | `test_audit_files_match_versioned` | Diff vazio contra a pasta versionada. |
| Defeitos plantados | `test_audit_finds_each_defect` | O sandbox do rascunho: nulo, texto longo, partição errada, chave repetida dentro da partição e contra a publicada, chave única repetida contra a publicada. |
| Conexão | `test_engine_config_and_per_thread_cursor` | `duckdb_settings()` com os valores pedidos; três threads recebem três cursores; as tabelas criadas por uma thread são visíveis às outras. |
| Ingestão presa | `test_ingest_pins_the_version` | Um `append` na tabela depois da abertura não aparece na view nem na tabela materializada. |
| Stream | `test_stream_prefetches_on_its_own_cursor` | Os casos de `test_parallel.py`: ordem, `prefetch`, `close` antecipado, erro da consulta, cursor do cliente livre. |
| Loader | `test_loader_commits_at_close_and_rolls_back_on_error` | Nada visível antes do `close`; exceção do cliente, lote recusado pelo `cast` e erro do `INSERT` deixam a tabela como estava; `rows`. |
| Formas por tabela | `test_query_and_load_match_stream_and_loader` | `query` igual a `stream(...).read_all()`; `load` de `pa.Table`, `RecordBatch`, leitor e iterável com o mesmo resultado; DataFrame recusado com a mensagem. |
| Ciclo pandas | `test_pandas_round_trip_keeps_contract_types` | `to_pandas(types_mapper=pd.ArrowDtype)` e `from_pandas` mantêm `decimal128(18, 2)` e `date32`. |
| Exportação | `test_export_partition_modes_produce_the_same_partition` | `register` e `rewrite` sobre a mesma partição dão as mesmas linhas e somas; `register` poda pela estatística (`Scanning Files: 0/n`); `expected_rows` diferente recusa. |
| Pipeline de exemplo | `test_example_pipeline_in_a_file_backed_database` | Doze partições materializadas, dimensões em view, um `select` com `join`, auditoria, exportação; o arquivo `.duckdb` apagado por `cleanup`. |
| Texto com `%` | `test_execute_keeps_percent_literals` | `LIKE 'A%'` chega ao DuckDB como está. |

## Rascunhos executados

Os dois rascunhos rodaram em 2026-09-21 com as versões fixadas. O primeiro monta as verificações a
partir de um `Table` com chave primária e chave única, imprime o texto de cada uma nos dois dialetos
e as executa num DuckDB em memória com um defeito de cada tipo; a tabela `delta_scan` faz o papel da
versão publicada. O segundo é o motor por partes.

```python
"""Etapa 4: as verificações da auditoria derivadas do Table, o texto por dialeto e a execução no DuckDB com defeitos plantados."""
import dataclasses
import datetime as dt

import duckdb
import duckdb_engine
import sqlalchemy as sa
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import quoted_name
from sqlalchemy.sql.functions import GenericFunction
from sqlalchemy.sql.visitors import replacement_traverse
from sqlalchemy_redshift.dialect import RedshiftDialect_redshift_connector

DIALECTS = {"duckdb": duckdb_engine.Dialect(paramstyle="named"), "redshift": RedshiftDialect_redshift_connector(paramstyle="named")}


class partition_text(GenericFunction):
    """strftime(data, '%Y-%m-%d') no DuckDB, to_char(data, 'YYYY-MM-DD') no Redshift: o valor da coluna de partição."""
    type = sa.String()
    inherit_cache = True


@compiles(partition_text, "duckdb")
def _duckdb_partition_text(element, compiler, **kw):
    return f"strftime({compiler.process(element.clauses.clauses[0], **kw)}, '%Y-%m-%d')"


@compiles(partition_text, "redshift")
def _redshift_partition_text(element, compiler, **kw):
    return f"to_char({compiler.process(element.clauses.clauses[0], **kw)}, 'YYYY-MM-DD')"


class json_valid(GenericFunction):
    type = sa.Boolean()
    inherit_cache = True


@compiles(json_valid, "redshift")
def _redshift_json_valid(element, compiler, **kw):
    return f"is_valid_json({compiler.process(element.clauses.clauses[0], **kw)})"


@dataclasses.dataclass(frozen=True)
class Check:
    name: str
    statement: sa.Select
    fails_when: str       # "count > 0" para defeitos; "any" para chaves (uma linha por chave repetida)


def sandbox_copy(table: sa.Table, prefix: str) -> sa.Table:
    return table.to_metadata(sa.MetaData(), name=quoted_name(f"{prefix}{table.name}", quote=False))


def checks(table: sa.Table, partitions: list[str] | None, options: dict, prefix: str = "{prefix}", published=None, key_scope: str | None = None) -> list[Check]:
    """Uma consulta de defeitos de linha, uma por chave, uma por chave estrangeira; 'published' é a fonte das demais partições (delta_scan)."""
    sandbox = sandbox_copy(table, prefix)
    partition_by, source = options.get("partition_by", [None])[0], options.get("partition_source")
    scope = sandbox.c[partition_by].in_(partitions) if partition_by and partitions else sa.true()
    filters = []
    for column in sandbox.columns:
        if not column.nullable:
            filters.append(sa.func.count().filter(column.is_(None)).label(f"nulo_{column.name}"))
        if isinstance(column.type, sa.String) and column.type.length and column.name != partition_by:
            filters.append(sa.func.count().filter(sa.func.length(column) > column.type.length).label(f"texto_{column.name}"))
        if isinstance(column.type, sa.JSON):
            filters.append(sa.func.count().filter(sa.and_(column.isnot(None), sa.not_(json_valid(column)))).label(f"json_{column.name}"))
        if isinstance(column.type, (sa.Double, sa.Numeric)) and not isinstance(column.type, sa.Float) or isinstance(column.type, sa.Double):
            filters.append(sa.func.sum(sa.cast(column, sa.Numeric(38, 6))).label(f"total_{column.name}"))
    if partition_by and source:
        filters.append(sa.func.count().filter(sandbox.c[partition_by] != partition_text(sandbox.c[source])).label(f"particao_{partition_by}"))
    result = [Check("linhas", sa.select(*filters).where(scope), "algum contador de defeito acima de zero")]

    keys = [tuple(c.name for c in table.primary_key.columns)] + [tuple(c.name for c in k.columns) for k in table.constraints if isinstance(k, sa.UniqueConstraint)]
    for key in keys:
        columns = [sandbox.c[name] for name in key]
        within = sa.select(*columns, sa.func.count().label("n")).where(scope).group_by(*columns).having(sa.func.count() > 1)
        result.append(Check(f"chave_{'_'.join(key)}", within, "alguma linha"))
        if partition_by and partition_by not in key and key_scope != "partition" and published is not None:
            other = published.alias("publicado")
            outside = sa.select(*columns).select_from(sandbox.join(other, sa.and_(*[sandbox.c[n] == other.c[n] for n in key]))).where(scope, other.c[partition_by].notin_(partitions or []))
            result.append(Check(f"chave_{'_'.join(key)}_publicada", outside, "alguma linha"))
    return result


def audit_sql(table: sa.Table, dialect: str, **options) -> dict[str, str]:
    return {check.name: str(check.statement.compile(dialect=DIALECTS[dialect], compile_kwargs={"literal_binds": True})) for check in checks(table, **options)}


metadata = sa.MetaData()
entries = sa.Table("cad_lancamentos", metadata,
                   sa.Column("id_lancamento", sa.BigInteger, primary_key=True, autoincrement=False), sa.Column("id_conta", sa.BigInteger, nullable=False),
                   sa.Column("data_base", sa.Date, nullable=False), sa.Column("valor", sa.Double, nullable=False), sa.Column("area", sa.String(10)),
                   sa.Column("meta", sa.JSON), sa.Column("data_base_str", sa.String(10), nullable=False),
                   sa.UniqueConstraint("id_conta", "data_base", "area"))
options = {"partition_by": ["data_base_str"], "partition_source": "data_base"}

# A versão publicada, que as chaves fora da partição comparam: no motor, delta_scan(uri, version := v).
published = sa.table("delta_scan", *[sa.column(c.name) for c in entries.columns])
texts = audit_sql(entries, "duckdb", partitions=["2026-08-31"], options=options, prefix="", published=published)
for name, text in texts.items():
    print(f"-- {name}\n{' '.join(text.split())}")
redshift = audit_sql(entries, "redshift", partitions=["2026-08-31"], options=options, prefix="exec_42_", published=published)
print("-- redshift, linhas:", " ".join(redshift["linhas"].split())[:230], "...")

con = duckdb.connect()
con.execute("CREATE TABLE cad_lancamentos (id_lancamento BIGINT, id_conta BIGINT, data_base DATE, valor DOUBLE, area VARCHAR, meta JSON, data_base_str VARCHAR)")
con.execute("""INSERT INTO cad_lancamentos VALUES
    (1, 7, '2026-08-31', 10.5, 'TI', '{"ok": true}', '2026-08-31'),
    (1, 7, '2026-08-31', 20.25, 'RH', NULL, '2026-08-31'),
    (2, NULL, '2026-08-31', 0.1, 'x', NULL, '2026-08-31'),
    (3, 8, '2026-07-31', 0.2, 'area longa demais', NULL, '2026-08-31'),
    (4, 9, '2026-08-31', 0.3, 'TI', NULL, '2026-07-31')""")   # a partição de julho fica fora do escopo de agosto
con.execute("""CREATE TABLE delta_scan AS SELECT * FROM (VALUES
    (3, 8, DATE '2026-07-31', 0.2, 'x', NULL, '2026-06-30'),
    (4, 9, DATE '2026-08-31', 0.3, 'TI', NULL, '2026-08-31'),
    (9, 7, DATE '2026-08-31', 1.0, 'TI', NULL, '2026-05-31')) t(id_lancamento, id_conta, data_base, valor, area, meta, data_base_str)""")   # 3 repete a chave primária fora da partição; 9 repete a chave única (7, 2026-08-31, TI)
for name, text in texts.items():
    print(f"{name}: {con.execute(text).fetchall()}")
```

Saída:

```
-- linhas
SELECT count(*) FILTER (WHERE cad_lancamentos.id_lancamento IS NULL) AS nulo_id_lancamento, count(*) FILTER (WHERE cad_lancamentos.id_conta IS NULL) AS nulo_id_conta, count(*) FILTER (WHERE cad_lancamentos.data_base IS NULL) AS nulo_data_base, count(*) FILTER (WHERE cad_lancamentos.valor IS NULL) AS nulo_valor, sum(CAST(cad_lancamentos.valor AS NUMERIC(38, 6))) AS total_valor, count(*) FILTER (WHERE length(cad_lancamentos.area) > 10) AS texto_area, count(*) FILTER (WHERE cad_lancamentos.meta IS NOT NULL AND NOT json_valid(cad_lancamentos.meta)) AS json_meta, count(*) FILTER (WHERE cad_lancamentos.data_base_str IS NULL) AS nulo_data_base_str, count(*) FILTER (WHERE cad_lancamentos.data_base_str != strftime(cad_lancamentos.data_base, '%Y-%m-%d')) AS particao_data_base_str FROM cad_lancamentos WHERE cad_lancamentos.data_base_str IN ('2026-08-31')
-- chave_id_lancamento
SELECT cad_lancamentos.id_lancamento, count(*) AS n FROM cad_lancamentos WHERE cad_lancamentos.data_base_str IN ('2026-08-31') GROUP BY cad_lancamentos.id_lancamento HAVING count(*) > 1
-- chave_id_lancamento_publicada
SELECT cad_lancamentos.id_lancamento FROM cad_lancamentos JOIN delta_scan AS publicado ON cad_lancamentos.id_lancamento = publicado.id_lancamento WHERE cad_lancamentos.data_base_str IN ('2026-08-31') AND (publicado.data_base_str NOT IN ('2026-08-31'))
-- chave_id_conta_data_base_area
SELECT cad_lancamentos.id_conta, cad_lancamentos.data_base, cad_lancamentos.area, count(*) AS n FROM cad_lancamentos WHERE cad_lancamentos.data_base_str IN ('2026-08-31') GROUP BY cad_lancamentos.id_conta, cad_lancamentos.data_base, cad_lancamentos.area HAVING count(*) > 1
-- chave_id_conta_data_base_area_publicada
SELECT cad_lancamentos.id_conta, cad_lancamentos.data_base, cad_lancamentos.area FROM cad_lancamentos JOIN delta_scan AS publicado ON cad_lancamentos.id_conta = publicado.id_conta AND cad_lancamentos.data_base = publicado.data_base AND cad_lancamentos.area = publicado.area WHERE cad_lancamentos.data_base_str IN ('2026-08-31') AND (publicado.data_base_str NOT IN ('2026-08-31'))
-- redshift, linhas: SELECT count(*) FILTER (WHERE exec_42_cad_lancamentos.id_lancamento IS NULL) AS nulo_id_lancamento, count(*) FILTER (WHERE exec_42_cad_lancamentos.id_conta IS NULL) AS nulo_id_conta, count(*) FILTER (WHERE exec_42_cad_lancamentos. ...
linhas: [(0, 1, 0, 0, Decimal('31.050000'), 1, 0, 0, 1)]
chave_id_lancamento: [(1, 2)]
chave_id_lancamento_publicada: [(3,)]
chave_id_conta_data_base_area: []
chave_id_conta_data_base_area_publicada: [(7, datetime.date(2026, 8, 31), 'TI')]
```

```python
"""Etapa 4: o motor DuckDB por partes: conexão por thread, ingest presa à versão, query, loader por lotes e export_partition em modo register."""
import dataclasses
import datetime as dt
import decimal
import json
import os
import tempfile
import threading
import time
import uuid
from pathlib import Path

import duckdb
import pyarrow as pa
from deltalake import CommitProperties, DeltaTable, Schema as DeltaSchema, write_deltalake
from deltalake.transaction import AddAction

PARTITION = "data_str"
CONTRACT = pa.schema([pa.field("id_operacao", pa.int64(), nullable=False), pa.field("data", pa.date32(), nullable=False),
                      pa.field("valor", pa.decimal128(18, 2), nullable=False), pa.field("descricao", pa.string()), pa.field(PARTITION, pa.string(), nullable=False)])
DDL = "CREATE TABLE IF NOT EXISTS {name} (id_operacao BIGINT NOT NULL, data DATE NOT NULL, valor DECIMAL(18, 2) NOT NULL, descricao VARCHAR, data_str VARCHAR(10) NOT NULL)"


@dataclasses.dataclass(frozen=True, kw_only=True)
class DuckDBConfig:
    database: str | None = None            # None: em memória
    threads: int | None = None
    memory_limit: str | None = None
    temp_directory: str | None = None
    extension_directory: str | None = None


class DuckDBEngine:
    def __init__(self, config: DuckDBConfig, execution_id: str) -> None:
        settings = {k: v for k, v in {"threads": config.threads, "memory_limit": config.memory_limit, "temp_directory": config.temp_directory}.items() if v}
        settings |= {"extension_directory": config.extension_directory or os.environ.get("SERIALIZE_DB_DUCKDB_EXTENSIONS", ".duckdb"),
                     "autoinstall_known_extensions": False, "autoload_known_extensions": False, "preserve_insertion_order": False}
        self._root = duckdb.connect(config.database or ":memory:", config=settings)
        self._root.execute("LOAD delta")
        self._local = threading.local()
        self.execution_id = execution_id

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        """O cursor da thread, criado no primeiro uso; o cliente nunca vê a conexão raiz."""
        if not hasattr(self._local, "cursor"):
            self._local.cursor = self._root.cursor()
        return self._local.cursor

    def ingest(self, name: str, uri: str, version: int, partitions: list[str] | None = None, materialize: bool = False) -> None:
        where = f" WHERE {PARTITION} IN ({', '.join(repr(p) for p in partitions)})" if partitions else ""
        kind = "TABLE" if materialize else "VIEW"
        self.connection.execute(f"CREATE {kind} {name} AS SELECT * FROM delta_scan('{uri}', version := {version}){where}")

    def query(self, sql: str, params: dict | None = None) -> pa.Table:
        with self._root.cursor() as cursor:                                 # um cursor próprio: o da thread fica livre
            return cursor.execute(sql, params or {}).to_arrow_table()

    def load(self, name: str, data: pa.Table | pa.RecordBatch, schema: pa.Schema) -> int:
        """A forma curta do loader: cada lote passa pelo cast e por um INSERT ... BY NAME numa transação; nada visível antes do commit."""
        batches = data.to_batches() if isinstance(data, pa.Table) else [data]
        cursor = self._root.cursor()
        cursor.execute(DDL.format(name=name))
        cursor.begin()
        try:
            for batch in batches:
                cursor.register("lote", batch.select([f.name for f in schema if f.name in batch.schema.names]).cast(pa.schema([schema.field(n) for n in batch.schema.names if n in schema.names])))
                cursor.execute(f"INSERT INTO {name} BY NAME SELECT * FROM lote")
                cursor.unregister("lote")
            cursor.commit()
        except Exception:
            cursor.rollback()
            raise
        finally:
            cursor.close()
        return sum(b.num_rows for b in batches)

    def export_partition(self, name: str, uri: str, value: str, metadata: dict[str, str]) -> int:
        """Modo register: COPY ... RETURN_STATS grava o arquivo dentro da pasta da tabela, e o commit registra a ação com as estatísticas."""
        relative = f"{PARTITION}={value}/{self.execution_id}_{uuid.uuid4().hex}.parquet"
        Path(uri, relative).parent.mkdir(parents=True, exist_ok=True)
        columns = [f.name for f in CONTRACT if f.name != PARTITION]
        (row,) = self._root.cursor().execute(
            f"COPY (SELECT {', '.join(columns)} FROM {name} WHERE {PARTITION} = '{value}' ORDER BY id_operacao) TO '{Path(uri, relative)}' (FORMAT parquet, RETURN_STATS)"
        ).fetchall()
        count, size, stats = row[1], row[2], {k.strip('"'): v for k, v in row[4].items()}
        expected = self.query(f"SELECT count(*) FROM {name} WHERE {PARTITION} = '{value}'").column(0)[0].as_py()
        assert count == expected, (count, expected)
        typed = {"numRecords": count, "minValues": {}, "maxValues": {}, "nullCount": {c: int(stats[c]["null_count"]) for c in columns if c in stats}}
        for column in ("id_operacao",):                                       # só as colunas cuja transcrição tem teste adversarial
            typed["minValues"][column], typed["maxValues"][column] = int(stats[column]["min"]), int(stats[column]["max"])
        table = DeltaTable(uri)
        table.create_write_transaction(
            [AddAction(path=relative, size=size, partition_values={PARTITION: value}, modification_time=int(time.time() * 1000), data_change=True, stats=json.dumps(typed))],
            mode="overwrite", schema=table.schema(), partition_by=[PARTITION], partition_filters=[(PARTITION, "=", value)],
            commit_properties=CommitProperties(custom_metadata=metadata))
        return DeltaTable(uri).version()

    def cleanup(self) -> None:
        self._root.close()


def sample(value: str, start: int, n: int) -> pa.Table:
    return pa.table({"id_operacao": pa.array(range(start, start + n), pa.int64()), "data": pa.array([dt.date.fromisoformat(value)] * n, pa.date32()),
                     "valor": pa.array([decimal.Decimal(k) / 100 for k in range(start, start + n)], pa.decimal128(18, 2)),
                     "descricao": pa.array([f"op {k}" for k in range(start, start + n)]), PARTITION: pa.array([value] * n)}, schema=CONTRACT)


with tempfile.TemporaryDirectory() as folder:
    uri = os.path.join(folder, "prod", "cad_operacoes")
    DeltaTable.create(uri, DeltaSchema.from_arrow(CONTRACT), partition_by=[PARTITION])
    write_deltalake(uri, pa.concat_tables([sample("2026-07-31", 1, 1000), sample("2026-08-31", 1001, 1000)]), mode="append")
    fixed = DeltaTable(uri).version()
    write_deltalake(uri, sample("2026-09-30", 5001, 10), mode="append")                # outra execução avança a tabela depois da abertura

    engine = DuckDBEngine(DuckDBConfig(threads=2, memory_limit="1GB", temp_directory=os.path.join(folder, "tmp")), "exec-2026-09-05")
    engine.ingest("cad_operacoes", uri, fixed, partitions=["2026-08-31"], materialize=True)
    engine.ingest("dim", uri, fixed)                                                  # a view enxerga a versão fixada, não a atual
    print("ingest:", engine.query("SELECT count(*) FROM cad_operacoes").column(0)[0].as_py(), engine.query("SELECT count(*) FROM dim").column(0)[0].as_py(), "| atual:", DeltaTable(uri).to_pyarrow_dataset().count_rows())

    cursors = set()
    def work(k: int) -> None:
        cursors.add(id(engine.connection))
        engine.connection.execute(f"CREATE TABLE t{k} AS SELECT {k} AS k")
    threads = [threading.Thread(target=work, args=(k,)) for k in range(3)]
    [t.start() for t in threads]; [t.join() for t in threads]
    print("cursores distintos por thread:", len(cursors), "| tabelas das threads visíveis:", engine.query("SELECT count(*) FROM duckdb_tables() WHERE table_name LIKE 't%'").column(0)[0].as_py())

    projected = engine.query("SELECT id_operacao + 100000 AS id_operacao, data, valor * 2 AS valor, descricao, data_str FROM cad_operacoes")
    print("load:", engine.load("cad_operacoes_projetadas", projected, CONTRACT), "linhas |", engine.query("SELECT count(*), sum(valor) FROM cad_operacoes_projetadas").to_pylist())
    try:
        engine.load("cad_operacoes_projetadas", pa.table({"id_operacao": pa.array([None], pa.int64()), "data": [dt.date(2026, 8, 31)], "valor": pa.array([1], pa.decimal128(18, 2)), "descricao": ["x"], PARTITION: ["2026-08-31"]}), CONTRACT)
    except Exception as error:
        print("load recusado:", type(error).__name__, "| linhas depois do rollback:", engine.query("SELECT count(*) FROM cad_operacoes_projetadas").column(0)[0].as_py())

    version = engine.export_partition("cad_operacoes_projetadas", uri, "2026-08-31", {"serialize_db_execution_id": "exec-2026-09-05"})
    print("export_partition register: versão", version, "| partição relida:", engine.query(f"SELECT count(*), min(id_operacao), sum(valor) FROM delta_scan('{uri}') WHERE {PARTITION} = '2026-08-31'").to_pylist(),
          "| poda:", "Scanning Files: 0/" in engine.query(f"EXPLAIN ANALYZE SELECT count(*) FROM delta_scan('{uri}') WHERE id_operacao > 999999").column(1)[0].as_py())
    engine.cleanup()
```

Saída:

```
ingest: 1000 2000 | atual: 2010
cursores distintos por thread: 3 | tabelas das threads visíveis: 3
load: 1000 linhas | [{'count_star()': 1000, 'sum(valor)': Decimal('30010.00')}]
load recusado: ValueError | linhas depois do rollback: 1000
export_partition register: versão 3 | partição relida: [{'count_star()': 1000, 'min(id_operacao)': 101001, 'sum(valor)': Decimal('30010.00')}] | poda: True
```

## Decisões pendentes

- **[decisão] `loader` numa tabela que já existe no sandbox.** O rascunho cria com `IF NOT EXISTS` e
  acrescenta; a alternativa é recusar, para um nome de saída igual ao de uma tabela ingerida não
  misturar as linhas.
- **[decisão] O padrão de `memory_limit`.** O DuckDB toma 80% da memória da máquina (6,1 GiB dos
  7,6 GiB do ambiente alvo); o rascunho da interface propõe 60% para o pandas do cliente ter o
  resto. A primeira execução real mede.
- **[decisão] O banco em arquivo como padrão** e o banco em memória só por `DuckDBConfig(database=":memory:")`;
  o plano dizia os dois sem escolher.
- **[decisão] A amostra do `AuditReport`.** Vinte linhas por verificação reprovada, no log; o
  relatório inteiro não vai para `_serialize_db/`.
