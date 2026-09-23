# Etapa 5: motor Redshift

A entrega e o critério de aceite desta etapa estão na tabela de etapas de [`PLAN.md`](PLAN.md), que
também fixa as decisões, as regras que toda etapa obedece e a ordem do trabalho.

`serialize_db.engine.redshift` implementa o mesmo protocolo. A conexão vem de `RedshiftConfig`:
`workgroup` (serverless), `database` (o banco da conexão), `share_database` (o banco do datashare que
guarda o esquema, quando não é o da conexão), `schema`, o `iam_role` opcional do `COPY` e do `UNLOAD`,
e `host`, `port`, `user` e `password` para o par informado, com os padrões nas variáveis
`SERIALIZE_DB_REDSHIFT_*`.

O caminho é o de [`../examples/redshift_native.py`](../examples/redshift_native.py), executado no
ambiente alvo em 2026-09-20: `redshift-serverless:GetWorkgroup` para o endereço e a porta,
`GetCredentials` para o par usuário e senha derivado da identidade IAM, e
`redshift_connector.connect` com esse par. A credencial dura no máximo uma hora, e `connect` a pede a
cada conexão em vez de guardá-la. Um par informado em `user` e `password` entra na mesma chamada. O
IAM interno do `redshift_connector` (`iam=True`) e o cluster provisionado
(`redshift:GetClusterCredentials`) não são caminhos da biblioteca: ninguém os executou no ambiente
alvo, que não tem cluster, e a regra do projeto é repetir o que rodou lá. A Data API não é caminho de
conexão da biblioteca (decisão do usuário de 2026-09-20): ela devolve `DECIMAL`, data e hora como
texto e limita o resultado a 500 MB, o que não serve à troca de lotes Arrow; ela fica nos exemplos e
em `RS-10`.

`connect` não passa `timeout` ao `redshift_connector`: lá ele é o tempo limite do socket, para
conectar e para ler, e um `COPY` ou um `UNLOAD` dura mais que qualquer espera razoável (10 s abortaram
uma visão de sistema no ambiente alvo em 2026-09-20, e a conexão não voltou a servir). Uma rede morta
aparece como o tempo limite do sistema.

`connect` roda `USE <share_database>` logo depois de abrir a sessão, e a partir daí
`qualified(name)` é `esquema.tabela`: é o caminho executado no ambiente alvo em 2026-09-20
([`../examples/redshift_copy_unload.py`](../examples/redshift_copy_unload.py)), onde o `CREATE`, o
`COPY`, o `SELECT` e o `UNLOAD` passaram assim. O nome em três partes fica para quem está conectado
a outro banco, como a Data API, e no SQLAlchemy ele só atravessa com `quoted_name(..., quote=False)`
([`sqlalchemy.md`](sqlalchemy.md)).

`credentials_clause()` monta como o `COPY` e o `UNLOAD` alcançam o S3, e quem decide é o `iam_role`
da configuração: com ele, `IAM_ROLE` com o ARN ou com `default`; sem ele, `ACCESS_KEY_ID`,
`SECRET_ACCESS_KEY` e `SESSION_TOKEN` da sessão `boto3`, que é o caminho do ambiente alvo, onde o
namespace não tem papel associado e por isso nem um ARN explícito funcionaria (`RS-6`). As
credenciais expiram, então a cláusula é montada por comando, nunca guardada; e **nenhum texto que a
carregue vai para log, para o relatório ou para arquivo**.

As restrições da escrita num datashare estão em [`redshift.md`](redshift.md). O `COPY` roda sem
cláusula `COMPUPDATE` alguma, e o de Parquet nem a aceita: a codificação das colunas vem do DDL ou
de `ENCODE AUTO`, e `ANALYZE COMPRESSION` numa amostra real é o que a fixa. O motivo de um `COPY`
recusado está em `sys_load_error_detail`, que a sessão lê no ambiente alvo (2026-09-20);
`stl_load_errors` cobre só clusters provisionados e é negada a um usuário comum.

| Primitiva | Redshift |
| --- | --- |
| `connect(config)` | `GetWorkgroup`, `GetCredentials` e `redshift_connector.connect` sem `timeout` e com `max_prepared_statements=0`, porque o cache de prepared statements do driver reaproveita um statement preparado antes de um `TRUNCATE` e o datashare o recusa com `34510` (leitura de 2026-09-21, [`redshift.md`](redshift.md)); `USE <share_database>` quando o esquema vem de um datashare, conferido pela resolução de um nome em duas partes, porque `current_database()` continua `dev` depois dele (leitura de 2026-09-21); `search_path` no esquema; `cursor.paramstyle = "named"`; uma conexão por execução, aberta na construção do motor e fechada em `cleanup`, e um `threading.RLock` que todo comando toma pelo tempo do comando (decisão do usuário de 2026-09-22, a mesma do motor DuckDB): `session()` dá essa conexão ao cliente com o lock tomado pelo bloco, reentrante na mesma thread. A senha dura no máximo uma hora, e o Redshift Serverless encerra a sessão ociosa há 3.600 s e a transação aberta e inativa há 21.600 s ([`redshift.md`](redshift.md)), então uma conexão derrubada pelo servidor é reaberta com credencial nova, uma vez por comando, e o comando é repetido; a reconexão perde a tabela temporária que o pipeline tenha criado na sessão, e o log a nomeia; o que o servidor faz com uma conexão cuja senha expirou, e se ela cai no meio de um `COPY`, é questão em aberto ([`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md)). |
| `new_session()` | Uma sessão a mais para o que roda em paralelo: outra conexão pelo caminho de `connect` (credencial temporária própria, `USE` e `max_prepared_statements=0`), com o seu `RLock` e as mesmas primitivas, gerenciador de contexto. Ela vê as tabelas `exec_<id>_*` que a sessão principal confirmou e não as temporárias dela; o fim do `with` e o `cleanup` dela fecham só essa conexão, sem apagar tabela. `run.ingest` de mais de uma tabela abre uma por tabela; dois `COPY` em conexões abertas dentro da tarefa levaram 4,3 s e 3,8 s no ambiente alvo (2026-09-21, [`POC.md`](POC.md)). |
| `ingest(table, uri, version, partitions=None, materialize=True)` | `copy_manifest` dos arquivos dessas partições, `COPY ... FORMAT AS PARQUET MANIFEST FILLRECORD` com a cláusula de credenciais numa staging sem a coluna de partição criada por `ddl` (`FILLRECORD` carrega um arquivo anterior a uma coluna nova com ela nula, leitura de 2026-09-21; a cláusula em todo `COPY` é a proposta da [etapa 8](PLAN-STAGE-8.md)), e `INSERT INTO exec_<id>_<tabela> SELECT *, '<valor>'`; `JSON_PARSE` nas colunas `SUPER`. O `COPY` também lê um prefixo de pasta direto, sem manifesto, e converte `int32` da origem para a coluna `BIGINT` do contrato. |
| `published(table, uri, version)` | A versão fixada como origem de consulta, sem ocupar o nome do modelo no sandbox: a staging `exec_<id>_<tabela>_publicado`, criada por `ddl` e carregada uma vez por execução com as colunas do contrato por `copy_manifest` da versão fixada e `COPY ... FORMAT AS PARQUET MANIFEST`, e o `FromClause` devolvido é ela. É por ele que o pipeline lê as partições publicadas da tabela que o `loader` grava ([etapa 4](PLAN-STAGE-4.md), decisão do usuário de 2026-09-22); a auditoria continua com as suas stagings só das colunas da chave, e `cleanup` apaga as duas. |
| `stream(statement_or_sql, params=None, batch_size=100_000)` | O statement compilado para o Redshift pela cópia prefixada, com os `bindparam` do cliente e as constantes como parâmetros, ou o texto com `{prefix}` em `exec_<id>_`, executado na sessão do motor, sob o lock e na thread de quem chama, que o solta antes de fatiar, porque o driver lê o resultado inteiro no `execute` e `fetchmany` só fatia a fila (leitura do código, 2026-09-21); a thread auxiliar fatia sem a sessão; cada lote é `RecordBatch.from_arrays` das colunas de `cursor.fetchmany(batch_size)` (`zip(*linhas)`), com o esquema do statement, ou os lotes de `ParquetFile.iter_batches` de um `UNLOAD` acima de um limite de linhas, lidos fora do lock; a interface `BatchStream` do DuckDB, com uma fila de dois lotes entre a thread auxiliar e o cliente. O `redshift_connector` é Python puro, e a thread auxiliar compete pelo GIL com o cliente ([`PLAN.md`](PLAN.md)). |
| `query(statement, **params)` | A consulta sob o lock e o resultado inteiro do cursor numa `pa.Table`, igual a `stream(statement, params).read_all()`. |
| `execute(sql, params)` | O texto sob o lock; a `pa.Table` do resultado, vazia para um comando sem resultado. |
| `loader(table, queue_depth=2)` | `write` faz `cast(batch, table)` na thread do cliente; a thread auxiliar grava um row group por lote com `ParquetWriter.write_batch` num arquivo de `staging/<execution_id>/`, e `close` fecha o arquivo e roda o `COPY` sob o lock: nada entra antes dele, e uma exceção dentro do `with` apaga o arquivo sem `COPY`. |
| `load(table, data)` | Os lotes de `data` pelo `loader`; uma `pa.Table` abaixo de um limite de linhas entra por um único `INSERT` multilinha montado de `to_pylist()`, numa ida ao servidor. |
| `audit(table, partitions, **opcoes)` | O mesmo texto compilado para o Redshift; as demais partições e a tabela referenciada entram em stagings só com as colunas da chave, por `COPY ... MANIFEST`. |
| `export_partition(table, uri, value, metadata, mode=None, expected_rows=None)` | `mode` é `"register"` ou `"rewrite"` (flag do usuário, 2026-09-21): do argumento, de `run.publish(export_mode=...)` ou de `SERIALIZE_DB_EXPORT_MODE`, `"register"` por omissão, a mesma flag do motor DuckDB ([etapa 4](PLAN-STAGE-4.md)) e da carga inicial ([etapa 7](PLAN-STAGE-7.md)); a mesma partição sai igual pelos dois, e o teste de integração os compara. Os dois começam por `UNLOAD ('<select do contrato>') TO '<destino>/' PARTITION BY (<coluna de partição>) FORMAT PARQUET MANIFEST VERBOSE` com a cláusula de credenciais, que passou no ambiente alvo em 2026-09-21 a partir de uma tabela do datashare ([`../examples/redshift_manifest.py`](../examples/redshift_manifest.py), [`POC.md`](POC.md)): grava na convenção Hive com a coluna de partição fora dos arquivos, e `PARALLEL OFF` quando a partição cabe num arquivo, porque o `UNLOAD` fragmenta por slice (500.000 linhas em 32 arquivos) e `MAXFILESIZE` é teto, não piso. **`register`**: o destino é `<uri>/<execution_id>/<valor>/`, dentro da pasta da tabela e vazio por construção, porque o `UNLOAD` confere o destino como prefixo e recusa um prefixo com objetos abaixo (leitura de 2026-09-21: o mesmo prefixo e o prefixo pai reprovam, um subprefixo novo passa), a pasta da partição já tem os arquivos das versões anteriores e `<uri>/<execution_id>/` deixa de estar vazio depois da primeira partição da execução; `register_files` recebe `<execution_id>/<valor>/<coluna>=<valor>/<arquivo>` por entrada do manifesto (`<slice>_part_<nn>.parquet`, com o número da slice variando entre execuções), o `count(*)` da fonte em `expected_rows`, faz as conferências da [etapa 3](PLAN-STAGE-3.md) antes do commit e a releitura depois. Os dados não passam pela máquina local, e os arquivos ficam como o Redshift os gravou até a compactação: `TIMESTAMP` em `INT96`, sem estatística de mínimo e máximo, lido de volta como `timestamp[us]` pelo delta-rs e pelo `delta_scan`, e `DECIMAL` em `FIXED_LEN_BYTE_ARRAY`. **`rewrite`**: o destino é `staging/<execution_id>/<tabela>/`, fora da tabela; a partição volta pelo leitor da [etapa 7](PLAN-STAGE-7.md) (`read_parquet` da pasta Hive, `INT96` a microssegundos, a coluna de partição com o valor do caminho, `cast`) e entra por `publish_partition`, onde o `write_deltalake` calcula estatística e valores de partição e recusa o que o contrato recusa. Os dados passam pela máquina local, e a memória do `write_deltalake` de uma partição de `cad_lancamentos` é a medição em aberto ([`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md)), que a flag faz com o mesmo `UNLOAD`; `cleanup` apaga o staging. |
| `cleanup()` | `DROP TABLE` de `exec_<id>_*` e da staging; os objetos de `staging/<execution_id>/` apagados; a sessão fechada. |

O identificador de execução entra no nome do sandbox normalizado para `[a-z0-9_]`, dentro dos
127 bytes de um identificador do Redshift. As tabelas `exec_<id>_*` nascem no esquema do datashare,
onde o `CREATE TABLE` passou (decisão do usuário de 2026-09-22, que fechou a alternativa das tabelas
temporárias do banco da conexão: elas morrem com a sessão, que o serverless encerra depois de
3.600 s ociosa, ninguém as inspeciona de fora nem depois de uma auditoria reprovada, e nascem com
codificação `RAW`, [`redshift.md`](redshift.md)); o banco local saiu das alternativas em 2026-09-20,
porque `has_database_privilege(dev, CREATE)` é falso e `TEMP` é verdadeiro (`RS-9`). A sessão única
do motor deixa ao pipeline a tabela temporária de que ele precise, criada por `execute` e visível
aos comandos seguintes, para um intermediário que não deva ficar no esquema.

Testes: `tests/test_engine_redshift.py` compara o SQL gerado (`COPY`, `INSERT ... SELECT`,
`UNLOAD`, DDL da staging) com texto esperado, sem conexão; os testes marcados `redshift` rodam a
mesma sequência com uma amostra no esquema autorizado, depois do
`test_redshift.py` da [etapa 0](PLAN-STAGE-0.md). Provas de conceito: `test_redshift.py` (sessão e
`paramstyle` nomeado, o `fetchmany` por lotes, banco do esquema e o `USE`, DDL, `COPY ... MANIFEST`, lista de
colunas e `FILLRECORD`, `VARCHAR`, `SUPER`, `UNLOAD`, ciclo da Data API, o comando repetido depois de um `TRUNCATE` com e sem o cache do driver), `test_sqlalchemy.py`
(`test_redshift_dialect_compiles_dml`, `test_three_part_name_needs_quoted_name_without_quotes`,
`test_sandbox_copy_of_table_and_schema_files_diff`) e `test_stdlib.py::test_execution_identifiers`
(o prefixo do sandbox).

Paralelismo: `test_redshift.py::test_parallel_copy_and_unload_on_two_connections`, dois `COPY` e dois
`UNLOAD` em tabelas distintas em duas conexões, limitados pelas slots do WLM, é o caminho de
`publish_redshift` da [etapa 8](PLAN-STAGE-8.md), uma conexão por tabela, e o de `new_session`; a
sessão principal roda os comandos do pipeline em série. O que duas transações simultâneas fazem no
esquema do datashare está em `test_redshift_transactions.py` ([etapa 8](PLAN-STAGE-8.md)).

## Interface

```python
"""Assinaturas de serialize_db.engine.redshift; os corpos estão no rascunho abaixo."""
import contextlib
import dataclasses
from collections.abc import Iterable, Mapping
from typing import Literal

import pyarrow as pa
import sqlalchemy as sa

ExportMode = Literal["register", "rewrite"]


@dataclasses.dataclass(frozen=True, kw_only=True)
class RedshiftConfig:
    workgroup: str | None = None          # serverless: GetWorkgroup e GetCredentials
    database: str = "dev"                 # o banco da conexão
    share_database: str | None = None     # o banco do datashare que guarda o esquema; a sessão roda USE nele
    schema: str = "public"
    iam_role: str | None = None           # ARN ou "default"; sem ele, as credenciais de quem chama
    host: str | None = None               # o par informado, na mesma chamada connect
    port: int = 5439
    user: str | None = None
    password: str | None = None
    region: str | None = None

    @staticmethod
    def from_environment() -> "RedshiftConfig": ...   # SERIALIZE_DB_REDSHIFT_*


def sandbox_prefix(execution_id: str) -> str: ...
def schema_from_description(description: list[tuple], numeric_types: Mapping[str, tuple[int, int]] | None = None) -> pa.Schema: ...


class RedshiftEngine:
    def __init__(self, config: RedshiftConfig, execution_id: str, storage: object, staging_prefix: str) -> None: ...
    def session(self) -> contextlib.AbstractContextManager[object]: ...   # a conexão da execução, com o RLock tomado pelo bloco
    def new_session(self) -> "RedshiftEngine": ...          # outra conexão, com o seu lock; o fim do with a fecha
    def __enter__(self) -> "RedshiftEngine": ...
    def __exit__(self, *exc: object) -> None: ...
    def qualified(self, name: str) -> str: ...                # esquema.tabela
    def credentials_clause(self) -> str: ...                  # montada por comando, nunca guardada nem logada
    def ingest(self, table: sa.Table, uri: str, version: int, partitions: list[str] | None = None, materialize: bool = True) -> None: ...
    def published(self, table: sa.Table, uri: str, version: int) -> sa.FromClause: ...   # a staging exec_<id>_<tabela>_publicado
    def stream(self, statement_or_sql: sa.sql.ClauseElement | str, params: Mapping[str, object] | None = None, batch_size: int = 100_000) -> object: ...
    def query(self, statement: sa.sql.ClauseElement | str, **params: object) -> pa.Table: ...
    def execute(self, sql: str, params: Mapping[str, object] | None = None) -> pa.Table: ...
    def loader(self, table: sa.Table, queue_depth: int = 2) -> object: ...
    def load(self, table: sa.Table, data: pa.Table | pa.RecordBatch | pa.RecordBatchReader | Iterable[pa.RecordBatch]) -> int: ...
    def audit(self, table: sa.Table, partitions: list[str] | None, uri: str | None = None, version: int | None = None, **options: object) -> object: ...
    def export_partition(self, table: sa.Table, uri: str, value: str | None, metadata: Mapping[str, str], mode: ExportMode | None = None, expected_rows: int | None = None) -> int: ...
    def cleanup(self) -> None: ...
```

## Estratégia de implementação

- **`connect`** (interno, uma vez por execução) repete `examples/redshift_native.py`: `GetWorkgroup`,
  `GetCredentials(durationSeconds=3600)`, `redshift_connector.connect` sem `timeout`, ou o par
  informado. Com `share_database`, roda `USE <banco>` e confirma a troca resolvendo um nome em duas
  partes, não por `current_database()`: a leitura de 2026-09-21 no ambiente alvo devolveu `dev`
  depois do `USE`, e o usuário confirmou no mesmo dia que o `USE` vale mesmo assim, como os exemplos
  de 2026-09-20 e 2026-09-21 já mostravam ([`POC.md`](POC.md)). A conferência é `CREATE TABLE IF NOT
  EXISTS <esquema>.serialize_db_publications (...)`, que só resolve no banco do datashare e deixa a
  tabela de controle da [etapa 8](PLAN-STAGE-8.md) criada; se ela falhar, a conexão é recusada com a
  mensagem do servidor. Depois vêm `SET search_path TO <esquema>` e `cursor.paramstyle = "named"`.
  Uma conexão derrubada pelo servidor é reaberta uma vez por comando, com credencial nova, e a
  reconexão perde as tabelas temporárias da sessão, que o log nomeia. O motor guarda essa conexão e
  um `threading.RLock` que todo comando toma pelo tempo do comando, e o cliente usa a conexão direto
  só dentro de `session()`, que toma o mesmo lock (decisão do usuário de 2026-09-22, a mesma do motor
  DuckDB): não há conexão por thread, e dois comandos da sessão não rodam ao mesmo tempo; o que roda
  em paralelo abre uma sessão a mais por `new_session()`, com credencial e `USE` próprios. A
  conexão vai com `max_prepared_statements=0`: o driver prepara cada comando sem nome logo antes de o
  executar, e nenhum statement guardado sobrevive a um `TRUNCATE` ou a um DDL de outra sessão
  ([`redshift.md`](redshift.md)).
- **`qualified`** é `f"{schema}.{name}"`; o nome em três partes não entra no motor.
- **`credentials_clause`** devolve `IAM_ROLE '<arn>'` ou `IAM_ROLE default` quando `iam_role` está
  configurado, e `ACCESS_KEY_ID`, `SECRET_ACCESS_KEY` e `SESSION_TOKEN` das credenciais congeladas
  do `boto3` quando não está; ela é chamada dentro de cada comando, e o texto do comando passa por
  `mask` antes de qualquer log, relatório ou exceção.
- **`ingest`** grava `copy_manifest` da versão fixada em `staging/<execution_id>/<tabela>.manifest`,
  cria a staging `exec_<id>_<tabela>_staging` por `staging_ddl`, o `ddl` da [etapa 1](PLAN-STAGE-1.md)
  sem a coluna de partição, escrito nesta etapa sobre `column_ddl` e `quoted`,
  roda `COPY ... FORMAT AS PARQUET MANIFEST FILLRECORD` (a proposta da [etapa 8](PLAN-STAGE-8.md):
  um manifesto pode listar arquivos anteriores a uma coluna nova) e um
  `INSERT INTO exec_<id>_<tabela> SELECT *, '<valor>'` por partição (ou `SELECT *` numa tabela sem partição), com `JSON_PARSE` nas colunas `SUPER`.
  `materialize=False` não existe aqui: o Redshift não lê o Delta no lugar.
- **`stream`** compila a cópia prefixada do statement (`sql.prefixed` com `prefix=exec_<id>_`), com
  os valores do cliente dados por `statement.params(**params)` e os nomes conferidos como no motor
  DuckDB, pelo dialeto Redshift com `paramstyle="named"`, sem `literal_binds` e com
  `render_postcompile=True`, que expande o `IN` de lista (sem ele o texto sai com
  `__[POSTCOMPILE_...]`, leitura de 2026-09-23 no DuckDB, [etapa 4](PLAN-STAGE-4.md));
  `construct_params()` dá os valores e o marcador `:nome` fica como o `redshift_connector` o lê com
  `cursor.paramstyle = "named"`; ou recebe o texto já com o prefixo trocado
  (`sql.read_sql(..., prefix="exec_<id>_")`) e o passa por `bind(style="redshift")`, e roda na sessão
  do motor, sob o lock, na thread de quem chama: o `execute` materializa o resultado, o lock sai, e a
  thread auxiliar fatia sem a sessão. O `execute` não vai para a thread auxiliar: lá ele esperaria
  pelo lock que o cliente segura num bloco `session()`, enquanto o cliente espera o primeiro lote. Cada
  fatia de `fetchmany(batch_size)` vira um `RecordBatch` **por colunas**: `zip(*rows)` e
  `pa.array(coluna, type=campo.type)`, com o esquema do statement ou o de
  `schema_from_description`. O rascunho mediu 0,03 s por colunas contra 0,10 s por dicionários em
  200.000 linhas, e o plano anterior dizia dicionários. O driver lê o resultado inteiro no
  `execute` e `fetchmany` fatia a fila (`redshift_connector` 2.1.16, leitura do código em 2026-09-21):
  a memória de um `stream` por `fetchmany` é a do resultado, e acima de um limite de linhas `stream`
  passa a `UNLOAD` para `staging/` e `ParquetFile.iter_batches`; o limite espera a medição no
  ambiente alvo (seção "Decisões pendentes").
- **`schema_from_description`** traduz o `type_code` de `cursor.description`, um OID do PostgreSQL,
  para o tipo Arrow; `NUMERIC` (1700) não carrega precisão e escala no OID, então o chamador informa
  `numeric_types` por coluna, ou o padrão `decimal128(18, 2)` do contrato. A tabela de OIDs é
  hipótese até a suíte a ler no ambiente alvo.
- **`loader`** grava um row group por lote com `ParquetWriter.write_batch` em
  `staging/<execution_id>/<tabela>/<uuid>.parquet` no S3 (ou na pasta local nos testes), e `close`
  roda o `COPY` desse prefixo na tabela do sandbox, sob o lock; uma exceção dentro do `with` apaga o
  arquivo sem `COPY`. `load` de uma `pa.Table` abaixo de um limite de linhas monta um `INSERT` multilinha com os
  valores embutidos, numa ida ao servidor.
- **`audit`** roda `audit_sql(table, "redshift", prefix=exec_<id>_, published=<staging>)`; as
  demais partições e a tabela referenciada entram em stagings só com as colunas da chave, por
  `COPY ... MANIFEST` da versão fixada.
- **`export_partition`** começa por `UNLOAD ('<select do contrato>') TO '<destino>/'
  <credenciais> FORMAT AS PARQUET PARTITION BY (<coluna>) MANIFEST VERBOSE`, com `PARALLEL OFF` quando
  a partição cabe num arquivo (o `UNLOAD` fragmenta por slice, 32 arquivos para 500.000 linhas). Com
  `mode="register"`, o destino é `<uri>/<execution_id>/<valor>/`, um subprefixo novo por partição e
  execução, e cada entrada do manifesto vira um
  `RegisteredFile` com `path` relativo à pasta da tabela, `content_length`, `record_count` e as
  estatísticas do rodapé, lido por `pyarrow.fs`; `register_files` confere e commita, com
  `expected_rows` do `count(*)` do sandbox. Com `mode="rewrite"`, o destino é
  `staging/<execution_id>/<tabela>/`, e a partição volta pelo leitor da [etapa 7](PLAN-STAGE-7.md)
  para `publish_partition`.
- **`cleanup`** roda `DROP TABLE IF EXISTS` de cada `exec_<id>_*`, um comando por chamada, apaga
  `staging/<execution_id>/` pelo `Storage` e fecha a sessão.

## Pré-requisitos e pós-condições

| Primitiva | Pré-requisitos | Pós-condições |
| --- | --- | --- |
| conexão | `redshift-serverless:GetWorkgroup` e `GetCredentials`, endpoint VPC para as APIs (o ambiente alvo tem), `USAGE` e `CREATE` no esquema concedidos pelo produtor. | Sessão no banco da conexão com os nomes em duas partes resolvendo no datashare; a tabela de controle existe; `paramstyle` nomeado; uma sessão por execução, sob o lock do motor. |
| `ingest` | Tabela Delta legível pela identidade da sessão, que o `COPY` leva; staging inexistente. | `exec_<id>_<tabela>` com as partições pedidas e a coluna de partição preenchida; a staging apagada. |
| `new_session` | O motor aberto; a identidade pode pedir outra credencial temporária. | Outra conexão no banco do datashare, com o seu lock; as tabelas confirmadas pela sessão principal visíveis, as temporárias dela não; o fim do `with` fecha só essa conexão. |
| `stream`, `query`, `execute` | Texto ou statement válido. | Lotes com os tipos do contrato; o lock solto depois do `execute`, e a thread do cliente livre para o lote atual. |
| `loader`, `load` | Lotes que passam por `cast`; `s3:PutObject` sob `staging/`. | Nada na tabela antes do `close`; o arquivo do staging apagado no erro. |
| `export_partition` | Auditoria aprovada; `<uri>/<execution_id>/<valor>/` vazio. | Uma versão nova no Delta; os arquivos como o Redshift os gravou (`INT96`, `FIXED_LEN_BYTE_ARRAY`, `optional`) em `register`, normalizados em `rewrite`. |
| `cleanup` | Nenhum. | Nenhuma tabela `exec_<id>_*` no esquema; `staging/<execution_id>/` vazio; a sessão fechada. |

## Testes por caso

`tests/test_engine_redshift.py`: o texto de cada comando comparado com o esperado, sem conexão; os
testes marcados `redshift` repetem a sequência com uma amostra no esquema autorizado.

| Caso | Teste | O que confere |
| --- | --- | --- |
| Configuração | `test_config_from_environment` | `SERIALIZE_DB_REDSHIFT_*` para `RedshiftConfig`; o par informado e o workgroup são caminhos alternativos; nenhum outro. |
| Prefixo | `test_sandbox_prefix_normalizes_and_limits` | `[a-z0-9_]`, 127 bytes. |
| Cláusula de credenciais | `test_credentials_clause_and_mask` | `IAM_ROLE` com ARN e `default`; as três chaves da sessão sem `iam_role`; `mask` tira os valores; nenhuma exceção carrega o texto sem máscara. |
| Comandos | `test_copy_insert_unload_text` | `COPY ... FORMAT AS PARQUET MANIFEST` sem `COMPUPDATE`, com `FILLRECORD` quando a decisão da [etapa 8](PLAN-STAGE-8.md) o fixar; `INSERT ... SELECT *, '<valor>'`; `UNLOAD ... PARTITION BY (...) MANIFEST VERBOSE` com `PARALLEL OFF` opcional e as aspas do `select` dobradas; nomes em duas partes. |
| DDL da staging | `test_staging_ddl_without_partition_column` | A staging sem a coluna de partição; a tabela do sandbox com ela. |
| Lotes de `fetchmany` | `test_batches_from_cursor_by_columns` | Um cursor de mentira: lotes do tamanho pedido, tipos do esquema, o último menor; igual ao caminho por dicionários. |
| Sessão única | `test_statements_serialize_on_the_single_session` | Uma conexão de mentira que registra o início e o fim de cada comando: dois `execute` de duas threads não se sobrepõem; um `execute` roda enquanto um `stream` ainda fatia lotes, porque o lock solta depois do `execute`; um `stream` aberto dentro de `session()`, na mesma thread, não trava; no alvo (`redshift`), a tabela temporária criada por `execute` é vista pelo `stream` seguinte. |
| Sessão a mais | `test_new_session_sees_committed_tables` (`redshift`) | A sessão de `new_session` vê a tabela `exec_<id>_*` confirmada pela principal e recusa a temporária dela; duas ingestões em duas sessões terminam, e a principal lê as duas tabelas. |
| Esquema de um texto | `test_schema_from_description` | Cada OID da tabela para o tipo Arrow; `NUMERIC` com `numeric_types`. |
| Conexão real | `test_connect_uses_share_database` (`redshift`) | Depois do `USE`, `CREATE TABLE IF NOT EXISTS <esquema>.serialize_db_publications` passa e `current_database()` é registrado como leitura. |
| Ciclo com amostra | `test_ingest_stream_loader_export` (`redshift`) | `ingest` de uma partição de um Delta no bucket, `stream` em lotes, `loader` por `COPY`, `export_partition` nos dois modos com as mesmas linhas, `cleanup` sem tabela restante. |
| Memória do `fetchmany` | `test_fetchmany_memory` (`redshift`) | Uma consulta grande em subprocesso, RSS medido com `fetchmany` contra `fetchall`, para dimensionar o limite do `UNLOAD`; o driver materializa o resultado no `execute`. |

## Rascunhos executados

O rascunho monta a configuração e o texto de cada comando, com a cláusula de credenciais mascarada,
e mede as duas formas de montar um lote a partir das tuplas de `fetchmany`. Ele rodou em 2026-09-21
com as versões fixadas; os comandos foram compilados, não executados num cluster.

```python
"""Etapa 5: a configuração, o prefixo do sandbox, o texto de cada comando com a cláusula de credenciais mascarada e os lotes de fetchmany."""
import dataclasses
import datetime as dt
import decimal
import re
import time

import pyarrow as pa


@dataclasses.dataclass(frozen=True, kw_only=True)
class RedshiftConfig:
    workgroup: str | None = None
    database: str = "dev"
    share_database: str | None = None
    schema: str = "public"
    iam_role: str | None = None
    host: str | None = None
    port: int = 5439
    user: str | None = None
    password: str | None = None
    region: str | None = None


def sandbox_prefix(execution_id: str) -> str:
    normalized = re.sub(r"[^a-z0-9_]", "_", execution_id.lower())
    prefix = f"exec_{normalized}_"
    if len(prefix.encode()) + 63 > 127:
        raise ValueError(f"identificador longo demais para o Redshift: {execution_id}")
    return prefix


def qualified(config: RedshiftConfig, name: str) -> str:
    """esquema.tabela depois do USE; o nome em três partes só serve a uma sessão aberta em outro banco."""
    return f"{config.schema}.{name}"


def credentials_clause(config: RedshiftConfig, session_credentials) -> str:
    if config.iam_role:
        return f"IAM_ROLE {'default' if config.iam_role == 'default' else repr(config.iam_role)}"
    clause = f"ACCESS_KEY_ID '{session_credentials.access_key}'\nSECRET_ACCESS_KEY '{session_credentials.secret_key}'"
    return clause + (f"\nSESSION_TOKEN '{session_credentials.token}'" if session_credentials.token else "")


def mask(sql: str) -> str:
    return re.sub(r"(ACCESS_KEY_ID|SECRET_ACCESS_KEY|SESSION_TOKEN)\s+'[^']*'", r"\1 '***'", sql)


def copy_sql(config, target: str, manifest_uri: str, credentials: str) -> str:
    return f"COPY {qualified(config, target)}\nFROM '{manifest_uri}'\n{credentials}\nFORMAT AS PARQUET MANIFEST;"


def insert_with_partition_sql(config, target: str, staging: str, partition_by: str, value: str) -> str:
    return f"INSERT INTO {qualified(config, target)}\nSELECT *, '{value}' AS {partition_by} FROM {qualified(config, staging)};"


def unload_sql(config, select: str, destination: str, partition_by: str, credentials: str, parallel: bool) -> str:
    return (f"UNLOAD ('{select.replace(chr(39), chr(39) * 2)}')\nTO '{destination}/'\n{credentials}\nFORMAT AS PARQUET PARTITION BY ({partition_by}) MANIFEST VERBOSE"
            + ("" if parallel else " PARALLEL OFF") + ";")


config = RedshiftConfig(workgroup="controladoria-wg", database="dev", share_database="datalake_rw_shared", schema="sbx_aco_decon", region="sa-east-1")
prefix = sandbox_prefix("exec-2026-09-05")
Credentials = dataclasses.make_dataclass("Credentials", ["access_key", "secret_key", "token"])
clause = credentials_clause(config, Credentials("AKIA...", "segredo", "token"))
print("prefixo:", prefix, "| tabela:", qualified(config, f"{prefix}cad_lancamentos"))
print(mask(copy_sql(config, f"{prefix}cad_lancamentos_staging", "s3://bucket/prod/staging/exec-2026-09-05/cad_lancamentos.manifest", clause)))
print(insert_with_partition_sql(config, f"{prefix}cad_lancamentos", f"{prefix}cad_lancamentos_staging", "data_base_str", "2026-08-31"))
print(mask(unload_sql(config, f"select id_lancamento, data_base, valor, data_base_str from {qualified(config, prefix + 'cad_lancamentos')} where data_base_str = '2026-08-31'",
                      "s3://bucket/prod/cad_lancamentos/exec-2026-09-05", "data_base_str", clause, parallel=False)))
print("IAM_ROLE:", credentials_clause(dataclasses.replace(config, iam_role="default"), None), "| a cláusula com segredo nunca é impressa:", "segredo" not in mask(clause))

# stream no Redshift: cada fatia de fetchmany vira um lote; por colunas (zip) é mais barato que por dicionários.
SCHEMA = pa.schema([("id_lancamento", pa.int64()), ("valor", pa.decimal128(18, 2)), ("data_base", pa.date32()), ("area", pa.string())])
rows = [(k, decimal.Decimal(k) / 100, dt.date(2026, 8, 31), f"area {k % 7}") for k in range(200_000)]


class FakeCursor:
    """O que o redshift_connector entrega: tuplas por fetchmany e description com nome e type_code."""
    description = [("id_lancamento", 20), ("valor", 1700), ("data_base", 1082), ("area", 1043)]

    def __init__(self, rows):
        self._rows, self._at = rows, 0

    def fetchmany(self, n):
        chunk = self._rows[self._at:self._at + n]
        self._at += n
        return chunk


def batches_by_columns(cursor, schema: pa.Schema, batch_size: int):
    while chunk := cursor.fetchmany(batch_size):
        yield pa.RecordBatch.from_arrays([pa.array(column, type=field.type) for column, field in zip(zip(*chunk), schema)], schema=schema)


def batches_by_dicts(cursor, schema: pa.Schema, batch_size: int):
    names = [c[0] for c in cursor.description]
    while chunk := cursor.fetchmany(batch_size):
        yield pa.RecordBatch.from_pylist([dict(zip(names, row)) for row in chunk], schema=schema)


for label, builder in (("por dicionários", batches_by_dicts), ("por colunas", batches_by_columns), ("por dicionários de novo", batches_by_dicts), ("por colunas de novo", batches_by_columns)):
    started = time.perf_counter()
    table = pa.Table.from_batches(list(builder(FakeCursor(rows), SCHEMA, 50_000)))
    print(f"{label}: {time.perf_counter() - started:.3f} s, {table.num_rows} linhas, {table.schema.field('valor').type}, {table.column('valor')[3].as_py()}")

# O esquema de um texto SQL vem de cursor.description: os type_code do Redshift são OIDs do PostgreSQL. [uncertain: conferir na suíte]
OID_TO_ARROW = {16: pa.bool_(), 20: pa.int64(), 21: pa.int16(), 23: pa.int32(), 701: pa.float64(), 1043: pa.string(), 1082: pa.date32(), 1114: pa.timestamp("us"), 1184: pa.timestamp("us", "UTC")}


def schema_from_description(description, numeric_scale: dict[str, tuple[int, int]] | None = None) -> pa.Schema:
    fields = []
    for column in description:
        name, code = column[0], column[1]
        if code == 1700:
            precision, scale = (numeric_scale or {}).get(name, (18, 2))
            fields.append(pa.field(name, pa.decimal128(precision, scale)))
        else:
            fields.append(pa.field(name, OID_TO_ARROW[code]))
    return pa.schema(fields)


print(schema_from_description(FakeCursor.description))
```

Saída:

```
prefixo: exec_exec_2026_09_05_ | tabela: sbx_aco_decon.exec_exec_2026_09_05_cad_lancamentos
COPY sbx_aco_decon.exec_exec_2026_09_05_cad_lancamentos_staging
FROM 's3://bucket/prod/staging/exec-2026-09-05/cad_lancamentos.manifest'
ACCESS_KEY_ID '***'
SECRET_ACCESS_KEY '***'
SESSION_TOKEN '***'
FORMAT AS PARQUET MANIFEST;
INSERT INTO sbx_aco_decon.exec_exec_2026_09_05_cad_lancamentos
SELECT *, '2026-08-31' AS data_base_str FROM sbx_aco_decon.exec_exec_2026_09_05_cad_lancamentos_staging;
UNLOAD ('select id_lancamento, data_base, valor, data_base_str from sbx_aco_decon.exec_exec_2026_09_05_cad_lancamentos where data_base_str = ''2026-08-31''')
TO 's3://bucket/prod/cad_lancamentos/exec-2026-09-05/'
ACCESS_KEY_ID '***'
SECRET_ACCESS_KEY '***'
SESSION_TOKEN '***'
FORMAT AS PARQUET PARTITION BY (data_base_str) MANIFEST VERBOSE PARALLEL OFF;
IAM_ROLE: IAM_ROLE default | a cláusula com segredo nunca é impressa: True
por dicionários: 0.252 s, 200000 linhas, decimal128(18, 2), 0.03
por colunas: 0.034 s, 200000 linhas, decimal128(18, 2), 0.03
por dicionários de novo: 0.113 s, 200000 linhas, decimal128(18, 2), 0.03
por colunas de novo: 0.034 s, 200000 linhas, decimal128(18, 2), 0.03
id_lancamento: int64
valor: decimal128(18, 2)
data_base: date32[day]
area: string
```

A primeira medição de cada forma paga a importação preguiçosa do PyArrow; as repetições são a
medida: por colunas, um terço do tempo por dicionários.

## Decisões pendentes

- **[decisão] A confirmação do `USE` pela criação da tabela de controle.** Ela cria
  `serialize_db_publications` em toda conexão, inclusive nas execuções que não publicam; a
  alternativa é `select 1 from <esquema>.<tabela existente> limit 0`, que depende de haver uma tabela.
- **[decisão] O limite de linhas entre `fetchmany` e `UNLOAD` em `stream`**, e entre o `INSERT`
  multilinha e o `COPY` em `load`; os dois esperam a medição no ambiente alvo.
- **[decisão] A tabela de OIDs de `schema_from_description`** é hipótese até a suíte a ler.
- **[decisão] O destino de `export_partition` por partição**: `<uri>/<execution_id>/<valor>/` com
  `PARTITION BY`, como está, ou `<uri>/<coluna>=<valor>/<execution_id>/` sem `PARTITION BY` e com a
  coluna de partição fora do `select`, que mantém a convenção Hive no primeiro nível da pasta; o
  `UNLOAD` confere o destino como prefixo (2026-09-21), e os dois são vazios por construção.
