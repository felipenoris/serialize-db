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
| `connect(config)` | `GetWorkgroup`, `GetCredentials` e `redshift_connector.connect` sem `timeout` e com `max_prepared_statements=0`, porque o cache de prepared statements do driver reaproveita um statement preparado antes de um `TRUNCATE` e o datashare o recusa com `34510` (leitura de 2026-09-21, [`redshift.md`](redshift.md)); `USE <share_database>` quando o esquema vem de um datashare, sem comando de conferência e sem criar a tabela de controle da [etapa 8](PLAN-STAGE-8.md) (decisão do usuário de 2026-09-23): um banco inexistente faz o `USE` falhar, e o primeiro comando em duas partes confirma a troca, porque `current_database()` continua `dev` depois dele (leitura de 2026-09-21); `search_path` no esquema; `cursor.paramstyle = "named"`; uma conexão por execução, aberta na construção do motor e fechada em `cleanup`, e um `threading.RLock` que todo comando toma pelo tempo do comando (decisão do usuário de 2026-09-22, a mesma do motor DuckDB): `session()` dá essa conexão ao cliente com o lock tomado pelo bloco, reentrante na mesma thread. A senha dura no máximo uma hora, e o Redshift Serverless encerra a sessão ociosa há 3.600 s e a transação aberta e inativa há 21.600 s ([`redshift.md`](redshift.md)), então uma conexão derrubada pelo servidor é reaberta com credencial nova, uma vez por comando, e o comando é repetido; a reconexão perde a tabela temporária que o pipeline tenha criado na sessão, e o log a nomeia; o que o servidor faz com uma conexão cuja senha expirou, e se ela cai no meio de um `COPY`, é questão em aberto ([`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md)). |
| `new_session()` | Uma sessão a mais para o que roda em paralelo: outra conexão pelo caminho de `connect` (credencial temporária própria, `USE` e `max_prepared_statements=0`), com o seu `RLock` e as mesmas primitivas, gerenciador de contexto. Ela vê as tabelas `exec_<id>_*` que a sessão principal confirmou e não as temporárias dela; o fim do `with` e o `cleanup` dela fecham só essa conexão, sem apagar tabela. `run.ingest` de mais de uma tabela abre uma por tabela; dois `COPY` em conexões abertas dentro da tarefa levaram 4,3 s e 3,8 s no ambiente alvo (2026-09-21, [`POC.md`](POC.md)). |
| `ingest(table, uri, version, partitions=None, materialize=True)` | `copy_manifest` dos arquivos dessas partições, `COPY ... FORMAT AS PARQUET MANIFEST FILLRECORD` com a cláusula de credenciais numa staging sem a coluna de partição criada por `ddl` (`FILLRECORD` carrega um arquivo anterior a uma coluna nova com ela nula, leitura de 2026-09-21; a cláusula em todo `COPY` é a proposta da [etapa 8](PLAN-STAGE-8.md)), e `INSERT INTO exec_<id>_<tabela> SELECT *, '<valor>'`; `JSON_PARSE` nas colunas `SUPER`. O `COPY` também lê um prefixo de pasta direto, sem manifesto, e converte `int32` da origem para a coluna `BIGINT` do contrato. |
| `published(table, uri, version)` | A versão fixada como origem de consulta, sem ocupar o nome do modelo no sandbox: a staging `exec_<id>_<tabela>_publicado`, criada por `ddl` e carregada uma vez por execução com as colunas do contrato por `copy_manifest` da versão fixada e `COPY ... FORMAT AS PARQUET MANIFEST`, e o `FromClause` devolvido é ela. É por ele que o pipeline lê as partições publicadas da tabela que o `loader` grava ([etapa 4](PLAN-STAGE-4.md), decisão do usuário de 2026-09-22); a auditoria continua com as suas stagings só das colunas da chave, e `cleanup` apaga as duas. |
| `stream(statement_or_sql, params=None, batch_size=100_000)` | Sempre por `UNLOAD` (decisão do usuário de 2026-09-23): o motor não sabe o tamanho do resultado antes do `execute`, e o `redshift_connector` o materializa inteiro ali (leitura do código, 2026-09-21). O statement compilado para o Redshift pela cópia prefixada, ou o texto com `{prefix}` em `exec_<id>_`, com os valores do cliente como literais, entra em `UNLOAD ('<select>') TO 'staging/<execution_id>/stream/<uuid>/' <credenciais> FORMAT AS PARQUET MANIFEST VERBOSE PARALLEL OFF`, na sessão do motor, sob o lock e na thread de quem chama; `PARALLEL OFF` mantém a ordem do `ORDER BY`, como no DuckDB. O lock sai no fim do `UNLOAD`, e a thread auxiliar lê os lotes dos arquivos do manifesto por `ParquetFile.iter_batches(batch_size)`, com o esquema do statement, enquanto o cliente trabalha no lote anterior; a interface é o `BatchStream` do DuckDB, com uma fila de dois lotes, e `close` apaga o prefixo do `stream`. O `UNLOAD` recusa `LIMIT` no `select` externo, e um resultado limitado cabe em `query`; o esquema de um resultado vazio espera a leitura do `UNLOAD` sem linhas ([`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md)). |
| `query(statement_or_sql, params=None)` | O statement compilado para o Redshift pela cópia prefixada, com os `bindparam` do cliente e as constantes como parâmetros do driver, ou o texto com `{prefix}` em `exec_<id>_` por `bind`, sob o lock, e o resultado inteiro do cursor numa `pa.Table` montada por colunas, com o esquema do statement ou o de `schema_from_row_description`; vazia para um comando sem resultado. Os valores são os de `stream(statement_or_sql, params).read_all()`, e o teste de integração compara os dois. `execute` saiu da interface (decisão do usuário de 2026-09-23). |
| `loader(table, queue_depth=2)` | Um nome já ocupado no sandbox é recusado com `SandboxError` na abertura, antes do primeiro lote, como na [etapa 4](PLAN-STAGE-4.md) (decisão do usuário de 2026-09-23); `write` faz `cast(batch, table)` na thread do cliente; a thread auxiliar grava um row group por lote com `ParquetWriter.write_batch` num arquivo de `staging/<execution_id>/`, e `close` fecha o arquivo e roda, sob o lock e numa transação, o `CREATE TABLE` e o `COPY`: nada existe antes dele, um erro desfaz os dois, e uma exceção dentro do `with` apaga o arquivo sem criar a tabela. |
| `load(table, data)` | Os lotes de `data` pelo `loader`, como no motor DuckDB. O `INSERT` multilinha saiu (decisão do usuário de 2026-09-23): ele embutia os valores no texto, com o risco de escape e o teto de 16 MB por comando, para poupar um custo fixo do `COPY` que ninguém mediu, e a suíte no ambiente alvo o registra como leitura. |
| `audit(table, partitions, uri=None, version=None, foreign_keys=False, key_scope=None)` | O mesmo texto compilado para o Redshift; as demais partições e a tabela referenciada entram em stagings só com as colunas da chave, por `COPY ... MANIFEST`, carregadas só quando a junção roda: o `skip_when` da chave primária inteira de uma coluna, verdadeiro quando o menor valor da execução passa do `max_key` da versão fixada, dispensa a junção e a staging (decisão do usuário de 2026-09-23). |
| `export_partition(table, uri, value, metadata, mode, expected_rows=None, columns_without_min_max=())` | `mode` é `"register"` ou `"rewrite"` (flag do usuário, 2026-09-21), resolvido por `Execution` a partir de `run.publish(export_mode=...)`, do `export_mode` da execução, de `SERIALIZE_DB_EXPORT_MODE` ou de `"register"`, sem leitura da variável no motor (decisão do usuário de 2026-09-23), a mesma flag do motor DuckDB ([etapa 4](PLAN-STAGE-4.md)) e da carga inicial ([etapa 7](PLAN-STAGE-7.md)); a mesma partição sai igual pelos dois, e o teste de integração os compara. Os dois começam por `UNLOAD ('<select das colunas do contrato, sem a de partição, da partição>') TO '<destino>/' FORMAT PARQUET MANIFEST VERBOSE` com a cláusula de credenciais, sem `PARTITION BY` (decisão do usuário de 2026-09-23): o caminho `<coluna>=<valor>/` é montado pela biblioteca, como no `COPY` do DuckDB, e `PARALLEL OFF` entra quando a partição cabe num arquivo, porque o `UNLOAD` fragmenta por slice (500.000 linhas em 32 arquivos) e `MAXFILESIZE` é teto, não piso. O `UNLOAD ... MANIFEST VERBOSE` passou no ambiente alvo em 2026-09-21 a partir de uma tabela do datashare, com `PARTITION BY` ([`../examples/redshift_manifest.py`](../examples/redshift_manifest.py), [`POC.md`](POC.md)); a forma sem ele, com `=` no prefixo, espera a próxima execução da suíte ([`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md)). O destino é um prefixo novo por partição e por tentativa: o `UNLOAD` confere o destino como prefixo e recusa um prefixo com objetos abaixo (leitura de 2026-09-21: o mesmo prefixo e o prefixo pai reprovam, um subprefixo novo passa), `run.publish` exporta uma partição por chamada, e a reexecução com o mesmo `execution_id` ([etapa 6](PLAN-STAGE-6.md)) acharia ocupado um destino sem o `uuid`, que o arquivo do DuckDB também leva no nome. **`register`**: o destino é `<uri>/<coluna>=<valor>/<execution_id>_<uuid>/`, dentro da pasta da partição; `register_files` recebe `<coluna>=<valor>/<execution_id>_<uuid>/<arquivo>` por entrada do manifesto (`<slice>_part_<nn>.parquet`, com o número da slice variando entre execuções), o `count(*)` da fonte em `expected_rows`, faz as conferências da [etapa 3](PLAN-STAGE-3.md) antes do commit e a releitura depois. Os dados não passam pela máquina local, e os arquivos ficam como o Redshift os gravou até a compactação: `TIMESTAMP` em `INT96`, sem estatística de mínimo e máximo, lido de volta como `timestamp[us]` pelo delta-rs e pelo `delta_scan`, e `DECIMAL` em `FIXED_LEN_BYTE_ARRAY`. **`rewrite`**: o destino é `staging/<execution_id>/<tabela>/<coluna>=<valor>/<uuid>/`, fora da tabela; a partição volta pelo leitor da [etapa 7](PLAN-STAGE-7.md) sobre os arquivos do manifesto (`read_parquet`, `INT96` a microssegundos, a coluna de partição acrescentada com o valor, `cast`) e entra por `publish_partition`, onde o `write_deltalake` calcula estatística e valores de partição e recusa o que o contrato recusa. Os dados passam pela máquina local, e a memória do `write_deltalake` de uma partição de `cad_lancamentos` é a medição em aberto ([`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md)), que a flag faz com o mesmo `UNLOAD`; `cleanup` apaga o staging. Nos dois modos, `columns_without_min_max`, as colunas `Double` com valor não finito na partição, vem de `run.publish` e vai a `register_files` ou a `publish_partition` (decisão do usuário de 2026-09-23, [issue #59](https://github.com/felipenoris/serialize-db/issues/59)). O mínimo e o máximo que o `UNLOAD` grava no rodapé de um grupo de linhas com `NaN` não foram medidos: um máximo sem o `NaN` faria o leitor Parquet do DuckDB perder a linha mesmo com o log sem estatística ([`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md)). |
| `cleanup()` | `DROP TABLE` de `exec_<id>_*` e da staging; os objetos de `staging/<execution_id>/` apagados; a sessão fechada. |

O identificador de execução entra no nome do sandbox normalizado para `[a-z0-9_]`, dentro dos
127 bytes de um identificador do Redshift. As tabelas `exec_<id>_*` nascem no esquema do datashare,
onde o `CREATE TABLE` passou (decisão do usuário de 2026-09-22, que fechou a alternativa das tabelas
temporárias do banco da conexão: elas morrem com a sessão, que o serverless encerra depois de
3.600 s ociosa, ninguém as inspeciona de fora nem depois de uma auditoria reprovada, e nascem com
codificação `RAW`, [`redshift.md`](redshift.md)); o banco local saiu das alternativas em 2026-09-20,
porque `has_database_privilege(dev, CREATE)` é falso e `TEMP` é verdadeiro (`RS-9`). A sessão única
do motor deixa ao pipeline a tabela temporária de que ele precise, criada por `query` e visível
aos comandos seguintes, para um intermediário que não deva ficar no esquema.

Testes: `tests/test_engine_redshift.py` compara o SQL gerado (`COPY`, `INSERT ... SELECT`,
`UNLOAD`, DDL da staging) com texto esperado, sem conexão; os testes marcados `redshift` rodam a
mesma sequência com uma amostra no esquema autorizado, depois do
`test_redshift.py` da [etapa 0](PLAN-STAGE-0.md). Provas de conceito: `test_redshift.py` (sessão e
`paramstyle` nomeado, o `fetchmany` por lotes, banco do esquema e o `USE`, DDL, `COPY ... MANIFEST`, lista de
colunas e `FILLRECORD`, `VARCHAR`, `SUPER`, `UNLOAD`, ciclo da Data API, o comando repetido depois de um `TRUNCATE` com e sem o cache do driver; e as leituras das decisões de 2026-09-23, que ainda não rodaram no ambiente alvo: `test_unload_to_a_hive_prefix_and_register`, `test_stream_by_unload_with_literal_values`, `test_unload_limit_empty_result_temp_table_and_super`, `test_row_description_oids_and_type_modifier`, `test_small_load_copy_cost` e `test_unload_footer_statistics_with_nan`), `test_sqlalchemy.py`
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
from collections.abc import Collection, Iterable, Mapping
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
def schema_from_row_description(row_description: list[Mapping[str, object]]) -> pa.Schema: ...   # cursor.ps["row_desc"]: label, type_oid, type_modifier


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
    def query(self, statement_or_sql: sa.sql.ClauseElement | str, params: Mapping[str, object] | None = None) -> pa.Table: ...
    def loader(self, table: sa.Table, queue_depth: int = 2) -> object: ...
    def load(self, table: sa.Table, data: pa.Table | pa.RecordBatch | pa.RecordBatchReader | Iterable[pa.RecordBatch]) -> int: ...
    def audit(self, table: sa.Table, partitions: list[str] | None, uri: str | None = None, version: int | None = None,
              foreign_keys: bool = False, key_scope: str | None = None) -> object: ...
    def export_partition(self, table: sa.Table, uri: str, value: str | None, metadata: Mapping[str, str], mode: ExportMode, expected_rows: int | None = None,
                         columns_without_min_max: Collection[str] = ()) -> int: ...
    def cleanup(self) -> None: ...
```

## Estratégia de implementação

- **`connect`** (interno, uma vez por execução) repete `examples/redshift_native.py`: `GetWorkgroup`,
  `GetCredentials(durationSeconds=3600)`, `redshift_connector.connect` sem `timeout`, ou o par
  informado. Com `share_database`, roda `USE <banco>` e nenhum comando de conferência (decisão do
  usuário de 2026-09-23): o `USE` num banco inexistente falha sozinho, e o primeiro comando que cita
  `<esquema>.<tabela>` confirma a troca pelo efeito de que o motor depende. `current_database()` não
  serve de conferência: a leitura de 2026-09-21 no ambiente alvo devolveu `dev` depois do `USE`, e o
  usuário confirmou no mesmo dia que o `USE` vale mesmo assim, como os exemplos de 2026-09-20 e
  2026-09-21 já mostravam ([`POC.md`](POC.md)). Um nome em duas partes só resolveria em silêncio no
  banco da conexão se ele tivesse um esquema com o mesmo nome, e o do ambiente alvo não tem
  (`sbx_aco_decon` só existe no datashare) nem deixa criar (`has_database_privilege(dev, CREATE)`
  falso, `RS-9`). A tabela de controle da [etapa 8](PLAN-STAGE-8.md) é criada só pelo usuário, por
  `create_publications_table`. Depois vêm `SET search_path TO <esquema>` e
  `cursor.paramstyle = "named"`.
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
- **`stream`** vai sempre por `UNLOAD` (decisão do usuário de 2026-09-23). O driver lê o resultado
  inteiro no `execute` e `fetchmany` só fatia a fila (`redshift_connector` 2.1.16, leitura do código
  em 2026-09-21), e o motor não sabe o tamanho do resultado antes desse `execute`: um limite de
  linhas entre os dois caminhos não teria como ser aplicado. O `UNLOAD` limita a memória e troca o
  parser Python do driver pela leitura nativa do Parquet, ao custo de um `UNLOAD` por `stream`
  (0,8 s a 1,9 s em 2026-09-21). O texto do `UNLOAD` é um literal que não recebe parâmetro, então os
  valores do cliente entram como literais: o statement Core é a cópia prefixada (`sql.prefixed` com
  `prefix=exec_<id>_`), com os valores dados por `statement.params(**params)` e os nomes conferidos
  como no motor DuckDB, compilada pelo dialeto Redshift com `paramstyle="named"`,
  `literal_binds=True` e `render_postcompile=True`, que expande o `IN` de lista (sem ele o texto
  sai com `__[POSTCOMPILE_...]`, leitura de 2026-09-23 no DuckDB, [etapa 4](PLAN-STAGE-4.md)). O
  `paramstyle="named"` é o de `render` ([etapa 2](PLAN-STAGE-2.md)): com o padrão `format`, o
  dialeto dobra o `%` dos literais (`'50%% certo'`), e o texto vai ao driver sem parâmetros, sem
  conversão que o desfaça (sonda local e leitura do código de 2026-09-23, [`POC.md`](POC.md)). O
  texto pronto (`sql.read_sql(..., prefix="exec_<id>_")`) passa por `sa.text(texto).bindparams(...)`
  com cada `bindparam` tipado pelo valor, `sa.bindparam(nome, value=valor, expanding=...)`, e pelo
  mesmo compilador: o `bindparam` de `text()` sem tipo não renderiza literal (`CompileError: No
  literal value renderer is available ... with datatype NULL`). Antes do comando, o motor percorre
  o statement e recusa todo `BindParameter` com `required`, como `render` faz: sob `literal_binds`,
  `compiled.binds` sai vazio, e o `bindparam` sem valor vira `NULL` calado, até num `IN` de lista,
  que sai `IN (NULL)` (leituras de 2026-09-22, [etapa 2](PLAN-STAGE-2.md), e de 2026-09-23). O
  dialeto dobra a aspa simples e a contrabarra (`'d''agua'`, `'barra \\ invertida'`), o escape do
  PostgreSQL, e as aspas simples do `select` são dobradas de novo no literal do `UNLOAD`; se o
  Redshift lê a contrabarra dobrada como uma só, dentro e fora do `UNLOAD`, é leitura da próxima
  execução da suíte (`test_stream_by_unload_with_literal_values`,
  [`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md)). O `UNLOAD` roda na sessão do motor, sob o lock, na
  thread de quem chama: numa thread auxiliar ele esperaria pelo lock que o cliente segura num bloco
  `session()`, enquanto o cliente espera o primeiro lote. A thread auxiliar só lê os arquivos do
  manifesto por `ParquetFile.iter_batches(batch_size)` pelo `Storage`, com
  `coerce_int96_timestamp_unit="us"`, e entrega cada lote com o esquema do statement, ou o do
  arquivo no texto, numa fila de dois lotes.
- **`query`** compila a cópia prefixada pelo dialeto Redshift com `paramstyle="named"`, sem
  `literal_binds` e com `render_postcompile=True`; `construct_params()` dá os valores e o marcador
  `:nome` fica como o `redshift_connector` o lê com `cursor.paramstyle = "named"`; o texto pronto
  passa por `bind(style="redshift")`. O resultado do `fetchall` vira a `pa.Table` **por colunas**:
  `zip(*rows)` e `pa.array(coluna, type=campo.type)`, com o esquema do statement ou o de
  `schema_from_row_description`. O rascunho mediu 0,03 s por colunas contra 0,10 s por dicionários
  em 200.000 linhas.
- **`schema_from_row_description`** traduz a descrição de cada coluna do resultado para o tipo Arrow
  (decisão do usuário de 2026-09-23). O `cursor.description` do `redshift_connector` 2.1.16 devolve
  só `(nome, oid, None, None, None, None, None)`; a precisão e a escala do `NUMERIC` estão no
  `type_modifier` de cada entrada de `cursor.ps["row_desc"]`, que o próprio driver usa para
  decodificar o `NUMERIC` binário: escala `(type_modifier - 4) & 0xFFFF` e precisão
  `((type_modifier - 4) >> 16) & 0xFFFF` (leitura do código, 2026-09-23, [`POC.md`](POC.md)). O
  atributo é privado, e o extra `redshift` fixa `redshift-connector==2.1.16`. Os OIDs vêm de
  `redshift_connector.utils.oids.RedshiftOID`: `BOOLEAN` em `bool`; `SMALLINT`, `INTEGER` e
  `BIGINT` em `int16`, `int32` e `int64`; `REAL` e `FLOAT` em `float32` e `float64`; `NUMERIC` em
  `decimal128(p, s)`; `CHAR`, `BPCHAR`, `VARCHAR`, `TEXT` e `UNKNOWN` em `string`; `DATE` em
  `date32`; `TIMESTAMP` em `timestamp[us]`; `TIMESTAMPTZ` em `timestamp[us, UTC]`; e `SUPER` em
  `string`, como o JSON do motor DuckDB. Outro OID, e um `NUMERIC` sem `type_modifier`, são
  recusados com `SandboxError`, que nomeia a coluna e o tipo (`get_datatype_name`). Uma precisão e
  uma escala fixas quebrariam: `pa.array` com `decimal128(18, 2)` recusou um valor de escala 6 e um
  de 17 dígitos inteiros (2026-09-23, [`POC.md`](POC.md)). A tabela é hipótese até a suíte ler o
  `row_desc` no ambiente alvo ([`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md)).
- **`loader`** confere o nome na abertura, antes do primeiro lote, por `select 1 from
  <esquema>.<nome> limit 0` sob o lock: a resposta é o nome ocupado, recusado com `SandboxError`, e
  o erro de relação inexistente é o nome livre, porque `information_schema.columns` leu vazio o
  esquema do datashare em 2026-09-21. Ele grava um row group por lote com
  `ParquetWriter.write_batch` em `staging/<execution_id>/<tabela>/<uuid>.parquet` no S3 (ou na
  pasta local nos testes), e `close` roda, sob o lock e numa transação aberta por `BEGIN`, o
  `CREATE TABLE` por `ddl` e o `COPY` desse prefixo: nada existe antes dele, e um erro desfaz os
  dois; uma exceção dentro do `with` apaga o arquivo sem criar a tabela. É a regra do `loader` da
  [etapa 4](PLAN-STAGE-4.md) (decisão do usuário de 2026-09-23): um `load` esquecido numa thread faz
  a leitura concorrente falhar, em vez de ver a tabela vazia. `load` passa sempre pelo `loader`
  (decisão do usuário de 2026-09-23).
- **`audit`** roda `audit_sql(table, "redshift", prefix=exec_<id>_, published=<staging>)`; as
  demais partições e a tabela referenciada entram em stagings só com as colunas da chave, por
  `COPY ... MANIFEST` da versão fixada.
- **`export_partition`** começa por `UNLOAD ('<select do contrato sem a coluna de partição>') TO
  '<destino>/' <credenciais> FORMAT AS PARQUET MANIFEST VERBOSE`, sem `PARTITION BY` (decisão do
  usuário de 2026-09-23), com `PARALLEL OFF` quando a partição cabe num arquivo (o `UNLOAD` fragmenta
  por slice, 32 arquivos para 500.000 linhas). O último segmento do destino é novo a cada chamada,
  com um `uuid`. Com `mode="register"`, o destino é `<uri>/<coluna>=<valor>/<execution_id>_<uuid>/`,
  e cada entrada do manifesto vira um `RegisteredFile` com `path` relativo à pasta da tabela,
  `content_length`, `record_count` e as estatísticas do rodapé, lido por `pyarrow.fs`;
  `register_files` confere e commita, com `expected_rows` do `count(*)` do sandbox. Com
  `mode="rewrite"`, o destino é `staging/<execution_id>/<tabela>/<coluna>=<valor>/<uuid>/`, e a
  partição volta pelo leitor da [etapa 7](PLAN-STAGE-7.md), sobre os arquivos do manifesto, para
  `publish_partition`.
- **`cleanup`** roda `DROP TABLE IF EXISTS` de cada `exec_<id>_*`, um comando por chamada, apaga
  `staging/<execution_id>/` pelo `Storage` e fecha a sessão.

## Pré-requisitos e pós-condições

| Primitiva | Pré-requisitos | Pós-condições |
| --- | --- | --- |
| conexão | `redshift-serverless:GetWorkgroup` e `GetCredentials`, endpoint VPC para as APIs (o ambiente alvo tem), `USAGE` e `CREATE` no esquema concedidos pelo produtor. | Sessão no banco da conexão com os nomes em duas partes resolvendo no datashare; nenhuma tabela criada; `paramstyle` nomeado; uma sessão por execução, sob o lock do motor. |
| `ingest` | Tabela Delta legível pela identidade da sessão, que o `COPY` leva; staging inexistente. | `exec_<id>_<tabela>` com as partições pedidas e a coluna de partição preenchida; a staging apagada. |
| `new_session` | O motor aberto; a identidade pode pedir outra credencial temporária. | Outra conexão no banco do datashare, com o seu lock; as tabelas confirmadas pela sessão principal visíveis, as temporárias dela não; o fim do `with` fecha só essa conexão. |
| `stream` | Texto ou statement válido, sem `LIMIT` no `select` externo e com valor em todo `bindparam`; `s3:PutObject` e `s3:GetObject` sob `staging/`. | Lotes com os tipos do contrato; o lock solto no fim do `UNLOAD`, e a thread do cliente livre para o lote atual; o prefixo do `stream` apagado no `close`. |
| `query` | Texto ou statement válido. | A `pa.Table` com os tipos do contrato; o lock solto no fim do `fetchall`. |
| `loader`, `load` | O nome da tabela livre no sandbox; lotes que passam por `cast`; `s3:PutObject` sob `staging/`. | Nada existe antes do `close`, que cria a tabela e carrega numa transação; nenhuma tabela no erro; `SandboxError` com o nome ocupado; o arquivo do staging apagado no erro. |
| `export_partition` | Auditoria aprovada; o destino novo, vazio por construção. | Uma versão nova no Delta; os arquivos como o Redshift os gravou (`INT96`, `FIXED_LEN_BYTE_ARRAY`, `optional`) em `register`, normalizados em `rewrite`. |
| `cleanup` | Nenhum. | Nenhuma tabela `exec_<id>_*` no esquema; `staging/<execution_id>/` vazio; a sessão fechada. |

## Testes por caso

`tests/test_engine_redshift.py`: o texto de cada comando comparado com o esperado, sem conexão; os
testes marcados `redshift` repetem a sequência com uma amostra no esquema autorizado.

| Caso | Teste | O que confere |
| --- | --- | --- |
| Configuração | `test_config_from_environment` | `SERIALIZE_DB_REDSHIFT_*` para `RedshiftConfig`; o par informado e o workgroup são caminhos alternativos; nenhum outro. |
| Prefixo | `test_sandbox_prefix_normalizes_and_limits` | `[a-z0-9_]`, 127 bytes. |
| Cláusula de credenciais | `test_credentials_clause_and_mask` | `IAM_ROLE` com ARN e `default`; as três chaves da sessão sem `iam_role`; `mask` tira os valores; nenhuma exceção carrega o texto sem máscara. |
| Comandos | `test_copy_insert_unload_text` | `COPY ... FORMAT AS PARQUET MANIFEST` sem `COMPUPDATE`, com `FILLRECORD` quando a decisão da [etapa 8](PLAN-STAGE-8.md) o fixar; `INSERT ... SELECT *, '<valor>'`; `UNLOAD ... MANIFEST VERBOSE` sem `PARTITION BY`, o `select` da exportação sem a coluna de partição, `PARALLEL OFF` opcional na exportação e fixo no `stream`, as aspas do `select` dobradas; nomes em duas partes. |
| Destino por tentativa | `test_unload_destination_is_new_per_call` | Duas exportações da mesma partição e duas partições da mesma tabela, em `register` e em `rewrite`, recebem destinos distintos; em `register`, `<coluna>=<valor>/` é o primeiro segmento do caminho relativo à pasta da tabela. |
| DDL da staging | `test_staging_ddl_without_partition_column` | A staging sem a coluna de partição; a tabela do sandbox com ela. |
| Tabela do cursor | `test_table_from_cursor_by_columns` | Um cursor de mentira: a `pa.Table` com os tipos do esquema, igual ao caminho por dicionários. |
| Valores literais | `test_stream_literal_values` | O texto do `UNLOAD` de um statement com texto, data, número e `IN` de lista, com o `%` sem dobrar; o texto pronto com os `bindparam` tipados pelo valor; um `bindparam` sem valor, também num `IN` de lista, recusado antes de qualquer comando; no alvo (`redshift`), valores com `'` e `\` voltam iguais, e o `stream` devolve as linhas de `query`. |
| Sessão única | `test_statements_serialize_on_the_single_session` | Uma conexão de mentira que registra o início e o fim de cada comando: dois comandos de duas threads não se sobrepõem; um comando roda enquanto um `stream` ainda lê os arquivos, porque o lock solta no fim do `UNLOAD`; um `stream` aberto dentro de `session()`, na mesma thread, não trava; no alvo (`redshift`), a tabela temporária criada por `query` é lida pelo `UNLOAD` do `stream` seguinte. |
| Sessão a mais | `test_new_session_sees_committed_tables` (`redshift`) | A sessão de `new_session` vê a tabela `exec_<id>_*` confirmada pela principal e recusa a temporária dela; duas ingestões em duas sessões terminam, e a principal lê as duas tabelas. |
| Esquema de um texto | `test_schema_from_row_description` | Um `row_desc` de mentira: cada OID da tabela para o tipo Arrow; `NUMERIC` com a precisão e a escala do `type_modifier`; outro OID recusado com o nome da coluna; no alvo (`redshift`), o `row_desc` de um `select` com uma coluna de cada tipo do contrato, `SUPER`, `count(*)`, `sum` de `NUMERIC(18, 2)`, `sum` de `DOUBLE PRECISION` e um literal de texto. |
| Conexão real | `test_connect_uses_share_database` (`redshift`) | Depois do `USE`, o `CREATE TABLE` de uma tabela `exec_<id>_*` por nome em duas partes passa, nenhum comando da conexão cria `serialize_db_publications`, e `current_database()` é registrado como leitura. |
| Ciclo com amostra | `test_ingest_stream_loader_export` (`redshift`) | `ingest` de uma partição de um Delta no bucket, `stream` em lotes, `loader` por `COPY`, `export_partition` nos dois modos com as mesmas linhas, `cleanup` sem tabela restante. |
| Tabela no `close` | `test_loader_creates_the_table_at_close` (`redshift`) | O nome ocupado é recusado com `SandboxError` na abertura; antes do `close` a leitura da tabela falha com relação inexistente; um erro do `COPY` desfaz o `CREATE TABLE` no esquema do datashare, e o nome fica livre. |
| Custo fixo do `COPY` | `test_small_load_copy_cost` (`redshift`) | O tempo de um `load` de 10 linhas pelo `loader`, como leitura, nunca como reprovação: é a leitura que traria de volta o `INSERT` multilinha. |

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
medida: por colunas, um terço do tempo por dicionários. O `UNLOAD` com `PARTITION BY` e o
`schema_from_description` com `numeric_scale` são os do rascunho de 2026-09-21; a seção "Estratégia
de implementação" descreve os que as decisões de 2026-09-23 fixaram.

## Decisões pendentes

Nenhuma. As decisões que o usuário tomou em 2026-09-23 estão escritas nas seções que as descrevem:
`connect` sem comando de conferência do `USE`, e a tabela de controle criada só pelo usuário
([etapa 8](PLAN-STAGE-8.md)); `stream` sempre por `UNLOAD` e `query` pelo cursor; `load` sempre
pelo `loader`; a precisão e a escala do `NUMERIC` pelo `type_modifier` do driver; e a exportação
sem `PARTITION BY`, num prefixo novo por partição e por tentativa. O que a próxima execução da
suíte no ambiente alvo lê para elas está em [`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md).
