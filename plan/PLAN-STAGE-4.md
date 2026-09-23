# Etapa 4: `audit` e motor DuckDB

A entrega e o critério de aceite desta etapa estão na tabela de etapas de [`PLAN.md`](PLAN.md), que
também fixa as decisões, as regras que toda etapa obedece e a ordem do trabalho.

`serialize_db.audit` monta as verificações a partir do contrato e não sabe qual motor as roda;
`serialize_db.engine` declara o protocolo `Engine`, que a execução também não conhece, e
`serialize_db.engine.duckdb` o implementa. O protocolo fixa os tipos da fronteira: `stream` devolve
um `BatchStream` de `pa.RecordBatch` e `loader` recebe lotes por `write`; `query` e `execute` devolvem
a `pa.Table` que `stream` montou, e `load` entrega ao `loader` os lotes de uma `pa.Table`, de um
`RecordBatch`, de um `RecordBatchReader` ou de um iterável ([`PLAN.md`](PLAN.md), seção "A troca de
dados com o código cliente"). Os esboços `SandboxEngine`, `BatchStream` e `Loader` de
`test_parallel.py`, na sessão única, são a referência da implementação.

Chave primária, unicidade e chave estrangeira ficam fora do DDL dos dois sandboxes (`schema.md`), e
a auditoria é onde elas são aplicadas. Cada verificação sai do `Table`, sem declaração adicional no
modelo:

| Verificação | De onde sai | Escopo |
| --- | --- | --- |
| Nulo em coluna `NOT NULL` | `column.nullable` | As partições da execução. |
| Chave repetida | `table.primary_key` e os `UniqueConstraint`, mais o que `keys` acrescenta | As partições da execução quando as colunas da chave incluem a coluna de partição; a tabela inteira quando não incluem. |
| Órfão de chave estrangeira | `table.foreign_keys` | Só com `foreign_keys=True`; a tabela referenciada entra na versão fixada pela execução. |
| Partição fora da origem, e valor que não serve de nome de pasta | `partition_by` e `partition_source` de `table_options`: com `partition_source` declarado, a coluna de partição diferente de `strftime(<coluna de data>, '%Y-%m-%d')`; em toda tabela particionada, o valor vazio ou com `/`, `=` ou espaço (a partição é texto desde a decisão de 2026-09-22, e a data é o caso da base atual) | As partições da execução. |
| Texto acima de `String(n)` em bytes e valor fora do `Numeric(18, 2)` | os tipos do contrato (`docs/index.md`); no Redshift, o `COPY` de uma string maior que o `VARCHAR` aborta (`Spectrum Scan Error` 15007, 2026-09-21), e esta verificação é a barreira | As partições da execução. |
| Documento JSON inválido, ou acima de 65.535 bytes se a decisão da [etapa 8](PLAN-STAGE-8.md) fixar o teto | as colunas JSON, que nem o Arrow nem o Delta validam; o teto é o do `VARCHAR` da staging do Redshift e da string que o `COPY` de Parquet aceita numa coluna `SUPER` (2026-09-21) | As partições da execução. |
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
| `AuditReport` | Por verificação: nome, o SQL rodado, a contagem de defeitos, uma amostra das linhas reprovadas e o veredito; `passed` é a conjunção, e `report.sql()` devolve o texto de todas. |

| Primitiva | DuckDB |
| --- | --- |
| `connect(config)` | Banco em arquivo `<temp_directory>/<execution_id>.duckdb`, e em memória só com `DuckDBConfig(database=":memory:")` (decisão do usuário de 2026-09-22); `temp_directory` omitido é uma pasta nova de `tempfile.mkdtemp`, apagada com o banco em `cleanup`, porque o padrão `.tmp` do DuckDB é relativo à pasta corrente; `extension_directory` de `SERIALIZE_DB_DUCKDB_EXTENSIONS` (ou `.duckdb/` da pasta preparada), `autoinstall_known_extensions` e `autoload_known_extensions` desligados; `storage.duckdb_setup`; `threads`, `temp_directory` e `preserve_insertion_order = false`; `memory_limit` só quando a configuração o informa, e o log registra na abertura o `current_setting('memory_limit')` que o DuckDB escolheu e o espaço livre de `temp_directory` (decisão do usuário de 2026-09-22); uma conexão só, a sessão da execução, e um `threading.RLock` que todo comando toma pelo tempo do comando (decisão do usuário de 2026-09-22, a mesma do motor Redshift); `session()` dá a conexão crua ao cliente com o lock tomado pelo bloco. |
| `new_session()` | Uma sessão a mais sobre o mesmo banco: um motor sobre `cursor()` da conexão, com o seu `RLock`, as mesmas primitivas e a mesma pasta de transbordo, gerenciador de contexto. Ele vê o que a sessão principal confirmou e não as tabelas temporárias dela; o fim do `with` e o `cleanup` dele fecham só essa conexão. `run.ingest` abre uma por tabela, e o cliente a usa para o que roda em paralelo ([`PLAN.md`](PLAN.md), seção "Regras que as etapas obedecem"). |
| `ingest(table, uri, version, partitions=None, materialize=False)` | View com o nome do modelo sobre `delta_scan(uri, version := v)`, ou `CREATE TABLE ... AS SELECT ... FROM delta_scan(...)` com `materialize=True`; com `partitions`, as duas filtram por `<coluna> BETWEEN '<menor>' AND '<maior>' AND <coluna> IN (...)`, porque o `delta_scan` poda por `=` e por intervalo e abre todos os arquivos com um `IN` de mais de um valor (leitura de 2026-09-23). |
| `published(table, uri, version)` | A versão fixada como origem de consulta, sem ocupar nome no sandbox: o `FromClause` com as colunas do contrato que compila para `delta_scan('<uri>', version := <v>)`. É por ele que o pipeline lê as partições publicadas da tabela que ele mesmo grava, cujo nome no sandbox pertence ao `loader`, e é ele que a auditoria usa como `published` nas chaves que não incluem a coluna de partição (decisão do usuário de 2026-09-22). Sem versão fixada, numa tabela que ainda não existe, levanta `SandboxError` nomeando a tabela. |
| `stream(statement_or_sql, params=None, batch_size=100_000)` | Um statement Core com as tabelas do contrato trocadas pelas do sandbox por `sql.prefixed(prefix="")`, com os valores do cliente dados por `statement.params(**params)` e compilado pelo dialeto `duckdb` com `paramstyle="named"`, sem `literal_binds` e com `render_postcompile=True`, que expande o `IN` de lista e o `bindparam(..., expanding=True)` (sem ele o texto sai com `__[POSTCOMPILE_...]`, que o DuckDB recusa, leitura de 2026-09-23); os nomes de `params` conferidos contra os `bindparam` sem valor do statement, porque `params` ignora um nome a mais; as constantes e os valores do cliente juntados por `construct_params()`, o marcador `:nome` em `$nome` pelo reescritor de `bind` (leituras de 2026-09-22 e 2026-09-23, [`POC.md`](POC.md); `param` não é exigido); ou um texto pronto, gerado por `render` ou lido por `sql.read_sql(..., prefix="")`, com `:nome` em `$nome` por `sql.bind`; roda numa thread auxiliar, na sessão, sob o lock e sem `Session` do SQLAlchemy, que grava cada lote de `to_arrow_reader(batch_size)` num arquivo Arrow IPC com LZ4 na pasta de transbordo assim que o DuckDB o entrega e solta o lock quando o resultado acaba, sem esperar pelo cliente; dentro de `session()`, na mesma thread, a consulta roda na thread de quem chama. O motor devolve o `BatchStream`: iterável de `pa.RecordBatch` com os tipos do motor (`decimal128(18, 2)`, `date32`, JSON como `string`), `schema`, `read_next_batch`, `read_all`, `close`, gerenciador de contexto e `__arrow_c_stream__` (para `write_deltalake` e `RecordBatchReader.from_stream`, nunca para o `register` do DuckDB). O cliente lê cada lote gravado, na sua thread, enquanto a consulta continua, com esperas com prazo; a thread não referencia o stream; `close` para a consulta no lote seguinte e apaga o arquivo; o erro anterior ao primeiro lote chega na construção, e o posterior na leitura seguinte ao último lote gravado. |
| `query(statement, **params)` | A consulta sob o lock, por `to_arrow_table()`: a `pa.Table` com os tipos do motor, igual a `stream(statement, params).read_all()`, sem arquivo. |
| `execute(sql, params)` | O texto sob o lock, por `to_arrow_table()`; um comando sem resultado devolve a tabela `Count` ou `Success` do DuckDB. |
| `loader(table, queue_depth=2)` | O gerenciador de contexto que grava lotes numa tabela nova do sandbox, criada por `ddl(table, "duckdb")`: um nome já ocupado, pela view do `ingest` ou pela tabela de um `loader` anterior, é recusado com `SandboxError` na abertura, antes do primeiro lote (decisão do usuário de 2026-09-22), e um laço por partição mantém um `loader` só aberto; `write(data)` aceita `pa.RecordBatch` ou `pa.Table`, faz `cast(batch, table)` na thread do cliente e põe o lote numa fila limitada; uma thread auxiliar grava os lotes num arquivo Arrow IPC com LZ4 na pasta de transbordo, sem a sessão, e o `close` roda, sob o lock, um único `INSERT ... BY NAME SELECT * FROM <leitor do arquivo>`; uma exceção dentro do `with`, um lote recusado pelo `cast` ou um loader abandonado apagam o arquivo sem `INSERT`; `close` relança o erro da thread e o do `INSERT`; `rows` conta as linhas gravadas. Nada é visível antes do `INSERT`, um comando atômico, e o leitor que ele consome é o do arquivo, nativo: nenhum gerador Python é entregue ao DuckDB. |
| `load(table, data)` | `data` é uma `pa.Table`, um `pa.RecordBatch`, um `RecordBatchReader` ou um iterável de lotes: `with loader(table) as l: for batch in ...: l.write(batch)`; outro tipo é recusado com a mensagem que aponta `pa.Table.from_pandas` e `pa.RecordBatch.from_pandas`. |
| `audit(table, partitions, uri=None, version=None, **opcoes)` | `uri` e `version` são os da tabela fixada, e montam `published` e as tabelas referenciadas sem depender de `Execution`. Roda o texto de `audit.audit_sql(table, "duckdb")` — `json_valid` e `strftime(data, '%Y-%m-%d')` são as funções do dialeto — e monta o `AuditReport`, com até 20 linhas inteiras de amostra por verificação reprovada (decisão do usuário de 2026-09-22): a verificação `linhas` é um `count(*) FILTER` por coluna e não tem linha para amostrar, então cada contador acima de zero ganha uma segunda consulta, `SELECT * ... WHERE <condição da coluna> LIMIT 20`, rodada só na reprovação; a comparação com as demais partições sai de `published` na versão fixada, e `passed` falso interrompe a execução. |
| `export_partition(table, uri, value, metadata, mode=None, expected_rows=None)` | `uri`, `metadata` e `expected_rows` vêm do chamador, porque o motor não conhece o `Database` e o commit precisa dos metadados. `mode` é a flag `export_mode` da [etapa 6](PLAN-STAGE-6.md) (`register` ou `rewrite`, `SERIALIZE_DB_EXPORT_MODE` por omissão, `register` sem ela), a mesma do motor Redshift ([etapa 5](PLAN-STAGE-5.md)) e da carga inicial ([etapa 7](PLAN-STAGE-7.md)). **`register`**: `COPY (SELECT <colunas do contrato, sem a de partição> FROM <sandbox> WHERE <coluna de partição> = '<valor>') TO '<uri>/<coluna de partição>=<valor>/<execution_id>_<uuid>.parquet' (FORMAT parquet, RETURN_STATS)` mais `register_files` com as estatísticas de `RETURN_STATS`, as conferências da [etapa 3](PLAN-STAGE-3.md) antes do commit e a releitura depois; a memória é constante (600 MB na reescrita medida em [`delta.md`](delta.md)). O `uuid` no nome impede uma reexecução com o mesmo `execution_id` de sobrescrever o arquivo que a versão anterior referencia. **`rewrite`**: o `stream` da partição, transbordado e passado por `cast`, para `publish_partition`; o `write_deltalake` calcula estatística e nulidade, e a memória cresce com a partição (1.140 MB para 135 MB de Parquet na mesma medição). A mesma partição sai igual pelos dois, e o teste os compara. |
| `cleanup()` | Fecha a conexão e apaga o arquivo do banco e a pasta de transbordo. |

Testes: `tests/test_audit.py`, sem gravar: o texto de cada verificação nos dois dialetos, a chave
lida do `primary_key` do modelo e o escopo escolhido pelas colunas da chave. `tests/test_engine_duckdb.py` sob a raiz local: o pipeline de exemplo (doze partições
materializadas e dimensões em view, um `select` com `join`, auditoria, exportação da partição nos dois modos com as mesmas linhas) sobre um
Delta local criado no teste; o ciclo `query`, `to_pandas(types_mapper=pd.ArrowDtype)`, `from_pandas` e
`load` com `decimal128(18, 2)` e `date32` mantidos, `load` recusando um DataFrame e o `loader`
recusando o nome que a view do `ingest` ocupa, com a leitura da versão publicada por `published` no
lugar; o ciclo por
lotes: `stream` que entrega o primeiro lote enquanto a consulta roda e deixa outros comandos
rodarem, `stream` sobre uma tabela temporária e dentro de `session()`, `stream` fechado no meio, `loader` que não insere nada na exceção do cliente e no lote
recusado pelo `cast`, `load` de um iterável e de uma `pa.Table` com o mesmo resultado, e `query`
igual a `stream(...).read_all()`;
auditoria que reprova a chave repetida dentro da partição e a repetida contra uma partição já
publicada, e o órfão de chave estrangeira com a tabela referenciada fora do sandbox; `execute` com
`%` em literal. Provas de conceito: `test_duckdb.py` (configuração, Arrow na entrada e na saída, o
leitor esvaziado pelo comando seguinte e preservado num cursor próprio, a consulta em streaming com
a memória medida em subprocesso, o `INSERT` sobre um leitor de gerador com a leitura antecipada e
os lotes que o leitor não confere, `DECIMAL`, JSON, `executemany`, `COPY` com `RETURN_STATS` e
particionado, banco em arquivo, `test_audit_queries` com o texto medido em bytes por `strlen`),
`poc_delta.py`
(`delta_scan`, poda, tempos, `ATTACH ... PIN_SNAPSHOT`),
`test_deltalake.py::test_duckdb_view_pins_version_and_reader_feeds_write`
(a view presa a `version := v` e o `to_arrow_reader()` no `write_deltalake` com predicado),
`test_sqlalchemy.py::test_arrow_path_on_raw_connection`,
`test_stdlib.py::test_engine_protocol_and_config_dataclass` e `test_concurrency.py` (o GIL liberado
pelo DuckDB, pelo delta-rs e pelo PyArrow, um `cursor()` por thread, a conexão compartilhada sem lock
que troca os resultados das threads, o arquivo do DuckDB recusado com outra configuração, dois
comandos em dois cursores, o intervalo de troca do GIL pago por cada retomada ao lado de uma thread
Python ocupada) e `test_parallel.py` (o motor com a sessão única e a sessão a mais de
`new_session`, os esboços `BatchStream`, que grava cada lote enquanto a consulta roda, e `Loader`,
com o transbordo em Arrow IPC, o pipeline de três estágios, a ingestão de quatro tabelas por
`delta_scan` numa sessão a mais por tabela, e cargas Arrow e `COPY ... TO` pedidos por quatro
threads à sessão) e
`test_pyarrow.py::test_record_batch_cast_and_conversions_share_buffers`; `test_duckdb.py` mede a
memória do stream transbordado (`test_spooled_stream_bounds_memory`).

## Interface

```python
"""Assinaturas de serialize_db.audit, serialize_db.engine e serialize_db.engine.duckdb; os corpos estão nos rascunhos abaixo."""
import contextlib
import dataclasses
from collections.abc import Iterable, Iterator, Mapping
from typing import Literal, Protocol, runtime_checkable

import duckdb
import pyarrow as pa
import sqlalchemy as sa

Dialect = Literal["duckdb", "redshift"]
ExportMode = Literal["register", "rewrite"]


class SandboxError(ValueError):      # o módulo importa o de serialize_db.errors
    """Um nome já ocupado no sandbox, ou um objeto do sandbox que não serve ao que foi pedido."""


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
    sample: pa.Table                 # até 20 linhas inteiras reprovadas; a verificação de linhas as busca numa segunda consulta
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
    """A interface dos dois motores; Execution só depende dela. Cada motor guarda uma sessão por
    execução sob um RLock que as primitivas tomam e soltam; session() dá a conexão crua com o lock
    tomado pelo bloco, reentrante na mesma thread; new_session() abre uma sessão a mais sobre o
    mesmo banco, e o fim do with fecha só a conexão dela."""
    def session(self) -> contextlib.AbstractContextManager[object]: ...
    def new_session(self) -> "Engine": ...
    def __enter__(self) -> "Engine": ...
    def __exit__(self, *exc: object) -> None: ...
    def ingest(self, table: sa.Table, uri: str, version: int, partitions: list[str] | None = None, materialize: bool = False) -> None: ...
    def published(self, table: sa.Table, uri: str, version: int) -> sa.FromClause: ...
    def stream(self, statement_or_sql: sa.sql.ClauseElement | str, params: Mapping[str, object] | None = None, batch_size: int = 100_000) -> BatchStream: ...
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
    memory_limit: str | None = None  # None: o padrão do DuckDB, 80% da memória, registrado no log
    temp_directory: str | None = None  # None: uma pasta nova de tempfile.mkdtemp, apagada em cleanup
    extension_directory: str | None = None


class DuckDBEngine:
    def __init__(self, config: DuckDBConfig, execution_id: str, storage: object) -> None: ...
```

## Estratégia de implementação

- **`checks`** monta os statements Core sobre a cópia prefixada da tabela (`sql.prefixed`). Uma
  consulta de linhas reúne num `count(*) FILTER` por coluna: nulo em `NOT NULL`, texto acima de
  `String(n)` em bytes, JSON inválido, a coluna de partição diferente de `partition_text(partition_source)`
  quando o modelo declara `partition_source`, o valor de partição vazio ou com `/`, `=` ou espaço
  (a partição é texto desde 2026-09-22, e a data é o caso da base atual), e a soma de controle de
  cada coluna `Double` e `Numeric` como `DECIMAL(38, 6)`. A soma de uma coluna `Double` corre só
  sobre os valores finitos, `sum(CASE WHEN isfinite(x) THEN CAST(x AS DECIMAL(38, 6)) END)` no
  DuckDB, e a consulta conta à parte os não finitos: o `CAST` de um `NaN` ou de um infinito para
  `DECIMAL` falha com `ConversionException` e derrubaria a verificação inteira, e o `FILTER` do
  agregado não o evita (leitura de 2026-09-23). Se a contagem reprova depende da decisão da
  [etapa 1](PLAN-STAGE-1.md) sobre o `Double` não finito. As funções com `@compiles` por dialeto
  fazem a portabilidade, e cada uma é subclasse de `FunctionElement` com `name`, como o `month_of`
  de [`sqlalchemy.md`](sqlalchemy.md), e não de `GenericFunction`, a classe do rascunho abaixo, que
  se registra em `sa.func` para o processo inteiro: depois dela, o `sa.func.json_valid` do próprio
  cliente sai `is_valid_json` no Redshift, e a subclasse de `FunctionElement` deixa o `sa.func`
  intacto (leitura de 2026-09-23). `partition_text` é
  `strftime(x, '%Y-%m-%d')` no DuckDB e `to_char(x, 'YYYY-MM-DD')` no Redshift; `json_valid` fica
  no DuckDB e vira `is_valid_json` no Redshift; `text_bytes`, o comprimento em bytes que o
  `String(n)` mede como o `VARCHAR(n)` do Redshift, é `strlen` no DuckDB e `octet_length` no
  Redshift, porque o `length` dos dois conta caracteres e o `octet_length` do DuckDB só aceita
  `BLOB`. Uma consulta por chave (`GROUP BY ... HAVING count(*)
  > 1`) dentro das partições da execução, e, quando a chave não inclui a coluna de partição e
  `key_scope != "partition"`, uma segunda consulta que junta o sandbox com `published` (o
  `delta_scan` da versão fixada, ou a staging no Redshift) fora das partições da execução. Uma
  consulta por chave estrangeira (anti-join contra `referenced[tabela]`) só com
  `foreign_keys=True`; sem ele, o nome entra em `not_run`.
- **`audit_sql`** renderiza cada `Check` por `sql.render` com o prefixo pedido; é o texto que
  `serialize-db audit --sql` imprime, para depuração, e nenhum arquivo o guarda (decisão do usuário
  de 2026-09-23).
- **`AuditReport`** guarda por verificação o SQL rodado, a contagem e uma amostra; `passed` é a
  conjunção; `sql()` concatena os textos para o log da execução.
- **`DuckDBEngine.__init__`** abre a conexão raiz com `extension_directory`,
  `autoinstall_known_extensions` e `autoload_known_extensions` desligados, `threads`,
  `temp_directory`, `preserve_insertion_order = false`, e chama
  `storage.duckdb_setup`. O banco é um arquivo em `<temp_directory>/<execution_id>.duckdb` por
  padrão: no ambiente alvo a máquina tem 7,6 GiB de memória, 2 vCPUs e 29,8 GiB livres num só
  disco, e uma tabela materializada de doze partições de `cad_lancamentos` não cabe em memória, cabe
  em disco ([`POC.md`](POC.md), leitura de 2026-09-21). `temp_directory` omitido é uma pasta de
  `tempfile.mkdtemp`, e `cleanup` apaga a pasta com o banco dentro. O `memory_limit` fica no padrão
  do DuckDB, 80% da memória, e só é ajustado quando a configuração o informa, em bytes ou com
  unidade, porque o DuckDB recusa porcentagem (`Unknown unit for memory: '%'`, leitura de
  2026-09-22). O que o DuckDB escolheu (`current_setting('memory_limit')`) e o espaço livre de
  `temp_directory`, lido por `shutil.disk_usage`, vão para o log na abertura: é por ele que a
  primeira execução real mede quanto sobra para o pandas do cliente, que o limite do DuckDB não
  cobre. `threads` omitido fica no padrão do DuckDB, um por núcleo; ele é da instância, vale para a
  sessão principal e para as de `new_session()`, e muda em execução por `SET threads`. A leitura do
  S3 pede mais threads que núcleos, porque cada thread faz uma requisição HTTP por vez, e quantas é
  o que a primeira execução no ambiente alvo mede ([`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md)). A
  conexão é a sessão da execução, e `session()` toma o `threading.RLock` e a dá ao
  bloco; toda primitiva toma o mesmo lock pelo tempo do seu comando, e nenhuma espera pelo código do
  cliente com ele tomado. O motor guarda a thread que está dentro de `session()`, e é por ela que
  `stream` sabe quando roda a consulta na thread de quem chama. `new_session()` devolve um motor
  sobre `cursor()` da conexão, com o seu lock e a mesma pasta de transbordo; o `cleanup` dele fecha
  só o cursor. O cursor nasce sem o lock da sessão principal: `cursor()` voltou em 0,04 ms com uma
  ordenação em curso na conexão (leitura de 2026-09-23), e a sessão a mais pedida durante um
  comando longo não espera por ele, como esperaria o esboço de `test_parallel.py`, que toma o lock.
  Os arquivos intermediários de `stream` e de `loader` ficam na pasta de transbordo e saem no
  `close`.
- **`ingest`** cria `VIEW <nome do modelo> AS SELECT * FROM delta_scan('<uri>', version := <v>)`,
  ou a `TABLE` com `materialize=True`, na sessão. Com `partitions`, o filtro é `<coluna> BETWEEN
  '<menor>' AND '<maior>' AND <coluna> IN (...)`: numa tabela de 12 partições, o `IN` de dois
  valores e o `OR` abriram os 12 arquivos, e o intervalo abriu só os seus (log `FileSystem` do
  DuckDB, 2026-09-23, [`POC.md`](POC.md)). As `n` partições de `previous_partitions` são contíguas,
  e o intervalo é exato; numa lista salteada, ele limita a leitura e o `IN` filtra as linhas. O
  `EXPLAIN ANALYZE` dessa forma falha com `InternalException` na extensão `delta`, e o teste lê a
  poda pelo log.
- **`published`** devolve o `FromClause` que compila para `delta_scan('<uri>', version := <v>)`, com
  as colunas do contrato, e não cria objeto no sandbox: é a origem que a auditoria já precisa nas
  chaves fora da partição, e a que o pipeline usa para ler a tabela cujo nome no sandbox é a saída
  do `loader`. No Redshift ele é a staging da [etapa 5](PLAN-STAGE-5.md).
- **`stream`** é o `BatchStream` de `test_parallel.py`: um statement Core vira a cópia prefixada de
  `sql.prefixed(prefix="")`, recebe os valores do cliente por `statement.params(**params)` e é
  compilado pelo dialeto com `paramstyle="named"`, sem `literal_binds` e com
  `render_postcompile=True`; `construct_params()` junta as constantes e os valores, e o marcador
  `:nome` é reescrito em `$nome` (a sonda de 2026-09-22 rodou esse caminho no DuckDB, e a de
  2026-09-23 achou o `IN` de lista, que sem `render_postcompile` sai `__[POSTCOMPILE_...]`). Antes de
  compilar, os nomes de `params` são conferidos contra os `bindparam` sem valor do statement, e um
  nome a mais ou a menos é `SqlError`, como no `bind`: `params` ignora o nome a mais, e o valor que
  falta seria `InvalidRequestError` do SQLAlchemy. Um texto pronto, já sem o sentinela, passa por
  `bind`. Uma thread auxiliar roda a consulta na sessão, sob o lock, com
  `to_arrow_reader(batch_size)`, grava cada lote num arquivo Arrow IPC com LZ4 na pasta de
  transbordo assim que o DuckDB o entrega e conta os lotes gravados numa `threading.Condition`; o
  cliente lê cada lote gravado, na sua thread, com esperas com prazo. A thread recebe só o que usa,
  nunca o stream, e confere o `stop` a cada lote; `close` o liga, espera a thread e apaga o arquivo.
  Dentro de `session()`, na mesma thread, a consulta roda na thread de quem chama e o cliente lê o
  arquivo depois dela, porque a auxiliar esperaria o bloco. Para 10.000.000 de linhas, o primeiro
  lote chegou em 0,006 s e a consulta terminou em 0,37 s, com 94 MB de memória máxima, contra 83 MB
  do leitor direto e 322 MB da tabela inteira (2026-09-23, [`POC.md`](POC.md)).
- **`query` e `execute`** rodam sob o lock e devolvem `to_arrow_table()`, sem arquivo.
- **`loader`** é o `Loader` de `test_parallel.py`, com a recusa do nome ocupado na abertura:
  `duckdb_tables()` e `duckdb_views()` dizem o que existe, e um nome tomado levanta `SandboxError`
  com a mensagem que aponta `run.published(table)` para ler a versão publicada. O `CREATE TABLE IF
  NOT EXISTS` não serve de guarda: sobre uma view ele passa em silêncio e o `INSERT ... BY NAME`
  seguinte morre com `Catalog Error: <nome> is not an table`, e sobre uma tabela de outro formato
  ele também passa, deixando o `INSERT ... BY NAME` preencher com nulo a coluna que sobra (leituras
  de 2026-09-22). Depois da guarda, `write` faz `cast(batch, table)` na thread do
  cliente e enfileira. A abertura roda, sob o lock, `CREATE TABLE <nome> (ddl)`, e por isso um
  `loader` aberto depois de um `stream`, como no exemplo mensal de [`PLAN.md`](PLAN.md), espera a
  consulta inteira do stream antes do primeiro lote: 0,811 s contra 0,006 s em 20.000.000 de linhas
  (leitura de 2026-09-23, [`POC.md`](POC.md)); criar a tabela no `close` é a proposta da seção
  "Decisões pendentes". A thread auxiliar grava os lotes num arquivo Arrow IPC com LZ4, sem a
  sessão; o `close` registra o leitor do arquivo
  com um nome único, que não é o de uma tabela do modelo, porque um leitor registrado ocupa um nome
  de view (leitura de 2026-09-22), e roda, sob o lock, um único `INSERT INTO <nome> BY NAME SELECT *
  FROM <leitor>`; qualquer erro apaga o arquivo sem `INSERT` e sobe em `close` ou no `write`
  seguinte. `load` embrulha
  `loader` para `pa.Table`, `RecordBatch`, `RecordBatchReader` e iteráveis, e recusa o resto com a
  mensagem que aponta `pa.Table.from_pandas`.
- **`audit`** roda `audit_sql(table, "duckdb", published=published(table, uri, version), ...)` e
  monta o `AuditReport`; `passed` falso não levanta aqui, levanta em `Execution.audit`. Cada
  verificação reprovada leva até 20 linhas inteiras de amostra: as verificações de chave e de
  órfão já devolvem as linhas, e a de `linhas`, que conta por coluna, ganha uma segunda consulta por
  contador acima de zero, `SELECT * ... WHERE <condição da coluna> LIMIT 20`, rodada só quando esse
  contador reprova. A amostra vai para o log, com o relatório; `_serialize_db/` não a recebe.
- **`export_partition`** com `mode="register"`: `COPY (SELECT <colunas sem a de partição> FROM
  <sandbox> WHERE <coluna> = '<valor>' ORDER BY <sort_key>) TO '<uri>/<coluna>=<valor>/<execution_id>_<uuid>.parquet'
  (FORMAT parquet, RETURN_STATS)`, a linha do `RETURN_STATS` vira um `RegisteredFile` (contagem,
  tamanho, `null_count` de todas as colunas, `min` e `max` das colunas com transcrição testada), e
  `delta.register_files` faz as conferências e o commit, com `expected_rows` do `count(*)` do
  sandbox. Com `mode="rewrite"`: `stream` da partição, `cast`, `delta.publish_partition`. O modo
  vem do argumento, de `SERIALIZE_DB_EXPORT_MODE` ou de `"register"`.
- **`cleanup`** fecha a conexão e apaga o arquivo do banco e a pasta de transbordo.

## Pré-requisitos e pós-condições

| Primitiva | Pré-requisitos | Pós-condições |
| --- | --- | --- |
| `checks`, `audit_sql` | Modelo aprovado por `check_models`; `published` informado quando alguma chave não inclui a coluna de partição. | Um `Check` por consulta, com o texto renderizável nos dois dialetos; sem conexão. |
| `DuckDBEngine` | Extensões na pasta configurada. | Uma conexão sobre o arquivo do banco, a sessão, sob um `RLock`; nenhum download; o `memory_limit` do DuckDB e o espaço livre de `temp_directory` no log. |
| `ingest` | Tabela Delta legível na versão pedida. | Uma view ou tabela com o nome do modelo, presa à versão; a tabela atual pode avançar sem afetar a leitura. |
| `published` | Tabela Delta legível na versão fixada. | Um `FromClause` sobre essa versão; nenhum objeto no sandbox, e o nome do modelo livre para o `loader`. |
| `new_session` | O motor aberto. | Outra conexão ao mesmo banco, com o seu lock; o que a sessão principal confirmou visível, as tabelas temporárias dela não; o fim do `with` fecha só essa conexão. |
| `stream` | Texto ou statement válido; parâmetros que fecham com o dicionário. | Lotes na ordem da consulta, o primeiro antes do fim de uma consulta sem operador bloqueante; a sessão livre quando a consulta acaba, sem esperar o cliente; a consulta parada e o arquivo apagado em `close`; o erro anterior ao primeiro lote no construtor, o posterior na leitura seguinte ao último lote gravado. |
| `loader`, `load` | O nome da tabela livre no sandbox; lotes que passam por `cast`. | Nada visível antes do `INSERT` do `close`; nada inserido na exceção; `rows` igual às linhas escritas; `SandboxError` com o nome ocupado, sem criar nem alterar objeto algum. |
| `audit` | Sandbox com a tabela; `uri` e `version` para as chaves fora da partição. | Um `AuditReport` com o SQL de cada verificação e até 20 linhas de amostra por verificação reprovada; nenhuma escrita. |
| `export_partition` | Auditoria aprovada (conferida por `Execution.publish`); a pasta da partição gravável. | Uma versão nova no Delta com a partição substituída e as estatísticas registradas; a mesma partição sai igual pelos dois modos. |
| `cleanup` | Nenhum. | Arquivo do banco e pasta de transbordo apagados; chamadas seguintes falham. |

## Testes por caso

`tests/test_audit.py` sem gravar; `tests/test_engine_duckdb.py` sob a raiz local, sobre um Delta
criado no teste a partir do modelo cliente.

| Caso | Teste | O que confere |
| --- | --- | --- |
| Texto das verificações | `test_audit_sql_per_dialect` | Cada `Check` do modelo cliente renderiza nos dois dialetos; `strftime` no DuckDB e `to_char` no Redshift; `json_valid` e `is_valid_json`; `strlen` e `octet_length`. |
| Escopo da chave | `test_key_scope_follows_the_partition_column` | Chave com a coluna de partição gera uma consulta; sem ela, gera a segunda contra `published`; `key_scope="partition"` a suprime e o relatório registra. |
| Chave estrangeira | `test_foreign_key_check_only_on_request` | Sem `foreign_keys=True` o nome está em `not_run`; com ele, o anti-join contra `referenced`. |
| Defeitos plantados | `test_audit_finds_each_defect` | O sandbox do rascunho: nulo, texto acima do `String(n)` em bytes e dentro dele em caracteres, partição errada, chave repetida dentro da partição e contra a publicada, chave única repetida contra a publicada. |
| Sessão | `test_engine_config_and_single_session` | `duckdb_settings()` com os valores pedidos, o `memory_limit` no padrão do DuckDB quando a configuração o omite, e o banco em arquivo dentro da pasta de `tempfile.mkdtemp`; três threads usam a mesma sessão, uma de cada vez; a tabela temporária criada por uma é visível às outras; uma primitiva chamada dentro de `session()` não trava. |
| Sessão a mais | `test_new_session_runs_beside_the_main_one` | A sessão de `new_session` vê a tabela confirmada pela principal e não a temporária dela, roda enquanto a principal está num bloco `session()`, e a principal vê o que ela confirma; o fim do `with` fecha só o cursor, e o arquivo do banco continua. |
| Ingestão presa | `test_ingest_pins_the_version` | Um `append` na tabela depois da abertura não aparece na view nem na tabela materializada. |
| Poda da ingestão | `test_ingest_opens_only_the_range_of_partitions` | A ingestão de partições contíguas e de uma lista salteada abre só os arquivos do intervalo, lidos pelo log `FileSystem` do DuckDB, e traz só as linhas pedidas. |
| Parâmetros | `test_statement_parameters_expand_in_lists` | Um statement com `IN` de lista, `NOT IN` e `bindparam(..., expanding=True)` roda por `query` e por `stream`; um nome de parâmetro a mais ou a menos é `SqlError` antes de rodar. |
| Versão publicada | `test_published_reads_the_pinned_version` | `published` lê a versão fixada sem criar objeto no sandbox, e o `loader` da mesma tabela fica com o nome do modelo. |
| Stream | `test_stream_writes_each_batch_while_the_query_runs` | Os casos de `test_parallel.py`: o primeiro lote com a consulta ainda rodando, a ordem, um comando no meio da leitura, a tabela temporária lida pelo stream, o stream dentro de `session()`, o `close` antecipado e o abandono que param a consulta e apagam o arquivo, e o erro da consulta na construção ou na leitura seguinte ao último lote. |
| Loader | `test_loader_inserts_on_close_in_one_statement` | Nada visível antes do `close`; exceção do cliente, lote recusado pelo `cast` e erro do `INSERT` deixam a tabela como estava; `rows`. |
| Nome ocupado | `test_loader_refuses_a_name_in_use` | O `loader` sobre a view do `ingest`, sobre a tabela do `ingest` materializado e sobre a tabela de um `loader` anterior levanta `SandboxError` antes do primeiro lote, e o objeto que estava lá não muda. |
| Formas por tabela | `test_query_and_load_match_stream_and_loader` | `query` igual a `stream(...).read_all()`; `load` de `pa.Table`, `RecordBatch`, leitor e iterável com o mesmo resultado; DataFrame recusado com a mensagem. |
| Ciclo pandas | `test_pandas_round_trip_keeps_contract_types` | `to_pandas(types_mapper=pd.ArrowDtype)` e `from_pandas` mantêm `decimal128(18, 2)` e `date32`. |
| Exportação | `test_export_partition_modes_produce_the_same_partition` | `register` e `rewrite` sobre a mesma partição dão as mesmas linhas e somas; `register` poda pela estatística (`Scanning Files: 0/n`); `expected_rows` diferente recusa. |
| Pipeline de exemplo | `test_example_pipeline_in_a_file_backed_database` | Doze partições materializadas, dimensões em view, um `select` com `join`, auditoria, exportação; o arquivo `.duckdb` apagado por `cleanup`. |
| Amostra | `test_audit_report_samples_failing_rows` | Até 20 linhas inteiras por verificação reprovada; a de `linhas` busca as suas numa segunda consulta por contador acima de zero, e uma verificação aprovada não roda consulta alguma. |
| Texto com `%` | `test_execute_keeps_percent_literals` | `LIKE 'A%'` chega ao DuckDB como está. |

## Rascunhos executados

Os dois rascunhos rodaram em 2026-09-21 com as versões fixadas, e o primeiro de novo em 2026-09-22,
com o texto medido em bytes. O primeiro monta as verificações a partir de um `Table` com chave
primária e chave única, imprime o texto de cada uma nos dois dialetos e as executa num DuckDB em
memória com um defeito de cada tipo (o texto `'ação ação'` tem 9 caracteres e 13 bytes: só a
medida em bytes o acusa numa coluna `String(10)`); a tabela `delta_scan` faz o papel da versão
publicada. O segundo é o motor por partes, com o modelo de conexão anterior à sessão única (um
cursor por thread num `threading.local`); a sessão única e os arquivos intermediários estão nos
esboços de `test_parallel.py`.

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


class text_bytes(GenericFunction):
    """O comprimento do texto em bytes, a medida do VARCHAR(n): strlen no DuckDB, octet_length no Redshift; o length dos dois conta caracteres."""
    type = sa.Integer()
    inherit_cache = True


@compiles(text_bytes, "duckdb")
def _duckdb_text_bytes(element, compiler, **kw):
    return f"strlen({compiler.process(element.clauses.clauses[0], **kw)})"


@compiles(text_bytes, "redshift")
def _redshift_text_bytes(element, compiler, **kw):
    return f"octet_length({compiler.process(element.clauses.clauses[0], **kw)})"


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
            filters.append(sa.func.count().filter(text_bytes(column) > column.type.length).label(f"texto_{column.name}"))
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
for dialect in DIALECTS:
    print(f"-- {dialect}, texto:", text_bytes(entries.c.area).compile(dialect=DIALECTS[dialect]))

con = duckdb.connect()
con.execute("CREATE TABLE cad_lancamentos (id_lancamento BIGINT, id_conta BIGINT, data_base DATE, valor DOUBLE, area VARCHAR, meta JSON, data_base_str VARCHAR)")
con.execute("""INSERT INTO cad_lancamentos VALUES
    (1, 7, '2026-08-31', 10.5, 'TI', '{"ok": true}', '2026-08-31'),
    (1, 7, '2026-08-31', 20.25, 'RH', NULL, '2026-08-31'),
    (2, NULL, '2026-08-31', 0.1, 'x', NULL, '2026-08-31'),
    (3, 8, '2026-07-31', 0.2, 'ação ação', NULL, '2026-08-31'),
    (4, 9, '2026-08-31', 0.3, 'TI', NULL, '2026-07-31')""")   # 'ação ação': 9 caracteres e 13 bytes; a partição de julho fica fora do escopo de agosto
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
SELECT count(*) FILTER (WHERE cad_lancamentos.id_lancamento IS NULL) AS nulo_id_lancamento, count(*) FILTER (WHERE cad_lancamentos.id_conta IS NULL) AS nulo_id_conta, count(*) FILTER (WHERE cad_lancamentos.data_base IS NULL) AS nulo_data_base, count(*) FILTER (WHERE cad_lancamentos.valor IS NULL) AS nulo_valor, sum(CAST(cad_lancamentos.valor AS NUMERIC(38, 6))) AS total_valor, count(*) FILTER (WHERE strlen(cad_lancamentos.area) > 10) AS texto_area, count(*) FILTER (WHERE cad_lancamentos.meta IS NOT NULL AND NOT json_valid(cad_lancamentos.meta)) AS json_meta, count(*) FILTER (WHERE cad_lancamentos.data_base_str IS NULL) AS nulo_data_base_str, count(*) FILTER (WHERE cad_lancamentos.data_base_str != strftime(cad_lancamentos.data_base, '%Y-%m-%d')) AS particao_data_base_str FROM cad_lancamentos WHERE cad_lancamentos.data_base_str IN ('2026-08-31')
-- chave_id_lancamento
SELECT cad_lancamentos.id_lancamento, count(*) AS n FROM cad_lancamentos WHERE cad_lancamentos.data_base_str IN ('2026-08-31') GROUP BY cad_lancamentos.id_lancamento HAVING count(*) > 1
-- chave_id_lancamento_publicada
SELECT cad_lancamentos.id_lancamento FROM cad_lancamentos JOIN delta_scan AS publicado ON cad_lancamentos.id_lancamento = publicado.id_lancamento WHERE cad_lancamentos.data_base_str IN ('2026-08-31') AND (publicado.data_base_str NOT IN ('2026-08-31'))
-- chave_id_conta_data_base_area
SELECT cad_lancamentos.id_conta, cad_lancamentos.data_base, cad_lancamentos.area, count(*) AS n FROM cad_lancamentos WHERE cad_lancamentos.data_base_str IN ('2026-08-31') GROUP BY cad_lancamentos.id_conta, cad_lancamentos.data_base, cad_lancamentos.area HAVING count(*) > 1
-- chave_id_conta_data_base_area_publicada
SELECT cad_lancamentos.id_conta, cad_lancamentos.data_base, cad_lancamentos.area FROM cad_lancamentos JOIN delta_scan AS publicado ON cad_lancamentos.id_conta = publicado.id_conta AND cad_lancamentos.data_base = publicado.data_base AND cad_lancamentos.area = publicado.area WHERE cad_lancamentos.data_base_str IN ('2026-08-31') AND (publicado.data_base_str NOT IN ('2026-08-31'))
-- redshift, linhas: SELECT count(*) FILTER (WHERE exec_42_cad_lancamentos.id_lancamento IS NULL) AS nulo_id_lancamento, count(*) FILTER (WHERE exec_42_cad_lancamentos.id_conta IS NULL) AS nulo_id_conta, count(*) FILTER (WHERE exec_42_cad_lancamentos. ...
-- duckdb, texto: strlen(cad_lancamentos.area)
-- redshift, texto: octet_length(cad_lancamentos.area)
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

As medições de cada proposta estão em [`POC.md`](POC.md), nas seções de 2026-09-23 sobre as sondas
da revisão das etapas 3 e 4 e sobre as estratégias de stream, loader e ingestão.

- **A criação da tabela do `loader` no `close`.** Proposto: a abertura confere o nome sem o lock da
  sessão, lendo `duckdb_tables()` e `duckdb_views()` num cursor próprio, e o `close` roda o
  `CREATE TABLE` e o `INSERT` no mesmo bloco, sob o lock. No pipeline de 20.000.000 de linhas, o
  primeiro lote passou de 0,811 s a 0,006 s e o total de 3,120 s a 2,761 s. O cursor não vê as
  tabelas temporárias da sessão principal, e o nome ocupado entre a abertura e o `close` falha no
  `CREATE` do `close`, sem inserir nada. A alternativa sem código é o `loader` aberto antes do
  `stream`, a ordem que o exemplo mensal inverte.
- **O `stream` híbrido.** Proposto: a thread guarda os lotes em memória até um orçamento fixo de
  256 MiB e grava no arquivo com LZ4 o que passa dele; o cliente esvazia a memória antes de ler o
  arquivo. Em 13.333.333 linhas, sem trabalho, com Python puro e com pandas, o total passou de
  0,598, 0,996 e 0,629 s a 0,407, 0,709 e 0,418 s, perto do cursor próprio (0,399, 0,673 e
  0,408 s), e o lock da sessão sai mais cedo pelo mesmo tanto. O custo é um segundo caminho na
  leitura. As alternativas são o arquivo com LZ4 de hoje e o arquivo sem compressão (0,436 s, com
  486 MB de arquivo contra 178 MB).
- **O `interrupt()` no `close` do stream e no `cleanup`.** Proposto: os dois chamam
  `interrupt()` na conexão quando a consulta do stream ainda roda, sob a `Condition` do stream, e a
  thread marca o fim da consulta sob a mesma `Condition` antes de soltar o lock, para o
  `interrupt()` nunca alcançar o comando seguinte, de outra thread. Uma ordenação parou 2 ms depois
  do pedido; sem ele, o `close` espera o fim do operador bloqueante, 1,63 s numa ordenação de
  60.000.000 de linhas.
- **O estilo `qmark` no caminho do statement Core.** Proposto: compilar com
  `duckdb_engine.Dialect(paramstyle="qmark")` e passar a lista de `positiontup`, o estilo do driver
  do DuckDB, sem reescrever `:nome` em `$nome` por uma expressão que só lê `[a-z_][a-z0-9_]*`; o
  `bind` fica para o texto pronto.
- **O escopo das chaves da auditoria.** Proposto: a chave que inclui a coluna de `partition_source`
  é conferida só nas partições da execução, porque a verificação da derivação, na mesma auditoria,
  garante que duas partições não têm a mesma data (`(data, sistema, contrato)` de `cad_contratos` e
  `(data, operacao)` de `cad_operacoes`); e a chave primária inteira de uma coluna dispensa a junção
  com as demais partições quando o menor valor da execução está acima do `max_key` da versão
  fixada, que `next_ids` já lê. Hoje as quatro tabelas particionadas do modelo cliente juntam a
  chave primária com a tabela publicada inteira, e `cad_lancamentos` tem 141.901.795 linhas na base
  de produção.
- **A interface do motor.** Propostos, sem medição: `query(statement, params=None)`, como `stream`
  e `execute`, no lugar de `**params`, que colide com um parâmetro chamado `statement`; `execute`
  como a mesma primitiva de `query` sobre texto; `export_partition` recebendo o `mode` já resolvido,
  com a leitura de `SERIALIZE_DB_EXPORT_MODE` só em `Execution`; `audit` e `audit_sql` com os
  argumentos nomeados de `checks` no lugar de `**options`; `checks` sem `prefix`, que `render` já
  aplica; e o `rewrite` de `export_partition` entregando o leitor do DuckDB ao `write_deltalake` sob
  o lock, como o `COPY` do `register` roda, sem `stream` nem arquivo, porque ali não há trabalho do
  cliente para sobrepor.

As quatro decisões que o usuário tomou em 2026-09-22 — o `loader` recusando com `SandboxError` um
nome já ocupado no sandbox, o `memory_limit` no padrão do DuckDB, o banco em arquivo com
`temp_directory` em pasta nova, e a amostra de até 20 linhas inteiras por verificação reprovada —
estão escritas, cada uma, na seção que a descreve. A recusa do `loader` trouxe duas
decisões do mesmo dia: `published(table, uri, version)` no protocolo dos dois motores, por onde o
pipeline lê as partições publicadas da tabela que ele grava, e `SandboxError` como a exceção da
etapa em `serialize_db.errors`.
