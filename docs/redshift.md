# Redshift

O Amazon Redshift é o banco de publicação do projeto e uma das duas engines de execução do pipeline.
Este documento descreve a conectividade do ambiente alvo, como ele organiza os dados, os tipos que
interessam ao contrato, o DDL, a manipulação de dados com pandas e Arrow, a ingestão em volume, a
exportação para Parquet, as recomendações de performance e o suporte a SQLAlchemy. As afirmações vêm
da documentação oficial, consultada em 2026-09-18 e, na seção de conectividade, em 2026-09-20, e do
código dos pacotes `redshift_connector` 2.1.16, `sqlalchemy-redshift` 1.0.0 e `awswrangler` 3.17.1.
Os dois caminhos de conexão rodaram no ambiente alvo em 2026-09-20 e estão em
[`../examples/`](../examples/); os exemplos de SQLAlchemy foram compilados sem conexão a um cluster,
e os itens marcados como pendentes dependem da prova de conceito.

## Comandos utilitários para diagnóstico

| Comando | O que mostra |
| --- | --- |
| `SELECT current_user;` | Usuário do banco da sessão. |
| `SELECT user_name, superuser, createdb FROM svv_user_info WHERE user_name = current_user;` | Se o usuário da sessão é superusuário e se pode criar bancos. |
| `SELECT database_name, database_type, database_isolation_level FROM svv_redshift_databases;` | Bancos acessíveis, locais ou compartilhados, e o nível de isolamento de cada um. |
| `SELECT database_name, schema_name, schema_type FROM svv_all_schemas;` | Esquemas locais, externos e compartilhados; usuários comuns só veem os próprios dados. |
| `SHOW GRANTS FOR <usuario> FROM DATABASE <banco>;` | Permissões do usuário no banco compartilhado. |
| `SELECT default_iam_role();` | Papel IAM padrão, usado por `IAM_ROLE default` no `COPY` e no `UNLOAD`. |
| `SHOW data_catalog_auto_mount;` | Se o `awsdatacatalog` está montado no workgroup. |
| `SELECT "table", diststyle, sortkey1, unsorted, stats_off, tbl_rows, skew_rows, vacuum_sort_benefit FROM svv_table_info WHERE schema = '<esquema>';` | Estilo de distribuição, chave de ordenação, fração não ordenada, estatísticas desatualizadas, linhas, assimetria e ganho estimado de um `VACUUM SORT`. |
| `SELECT * FROM svv_alter_table_recommendations;` | Recomendações do Advisor para chaves de distribuição e ordenação; visível só a superusuários. |
| `SELECT pg_last_copy_count();` | Linhas carregadas pelo último `COPY` da sessão; `0` quando a carga falhou. |
| `SELECT * FROM sys_load_error_detail ORDER BY start_time DESC LIMIT 20;` | Erros de carga, inclusive em workgroups serverless; `stl_load_errors` cobre só clusters provisionados e é negada a um usuário comum no ambiente alvo (SQLSTATE 42501, leitura de 2026-09-20), assim como `stv_slices`. |
| `EXPLAIN <consulta>;` | Plano de execução, com os rótulos de redistribuição `DS_DIST_*`. |

A biblioteca consulta `pg_last_copy_count()` e `sys_load_error_detail` depois de cada `COPY`, na
mesma conexão. A função não rodou nesta sessão:

```python
import sqlalchemy as sa

# Depois de um COPY, na mesma conexão: linhas carregadas e os erros de carga mais recentes.
def load_diagnostics(conn: sa.Connection) -> tuple[int, list[dict]]:
    loaded = conn.execute(sa.text("SELECT pg_last_copy_count()")).scalar_one()
    errors = conn.execute(sa.text(
        "SELECT * FROM sys_load_error_detail ORDER BY start_time DESC LIMIT 20"
    )).mappings().all()
    return loaded, [dict(error) for error in errors]
```

## Conectividade no ambiente alvo

O ambiente alvo expõe um workgroup serverless, e o esquema do projeto vem de um datashare. Os dois
caminhos de conexão estão em [`../examples/`](../examples/), como foram executados lá em 2026-09-20;
o que eles mostraram está em [`POC.md`](POC.md), e `probes/redshift.py` repete as mesmas chamadas.

### Credencial temporária do workgroup

`redshift-serverless:GetWorkgroup` devolve o endereço e a porta do endpoint, e
`redshift-serverless:GetCredentials` devolve o par usuário e senha derivado da identidade IAM de
quem chama ([`../examples/redshift_native.py`](../examples/redshift_native.py)). Não há senha
guardada em lugar nenhum.

- O usuário sai como `IAMR:<papel>` para uma role e `IAM:<usuário>` para um usuário IAM, é criado no
  banco quando ainda não existe e entra em `PUBLIC`: os `GRANT` do esquema precisam alcançá-lo.
- A senha dura 900 segundos por padrão e 3600 no máximo (`durationSeconds`). Uma execução mais longa
  que a emissão precisa de uma conexão nova, e é por isso que a biblioteca pede a credencial a cada
  conexão em vez de guardá-la.
- `dbName` é opcional; informando-o, a política IAM precisa permitir o recurso `dbname` daquele banco.
- O `redshift_connector` faz o mesmo por dentro com `iam=True, is_serverless=True,
  serverless_work_group=...`; o projeto não o usa: o caminho explícito é o que foi executado, e o
  erro dele diz qual das duas chamadas falhou. O cluster provisionado (`redshift:GetClusterCredentials`)
  também fica fora, porque o ambiente alvo não tem cluster.
- `redshift_connector.connect(timeout=...)` é o tempo limite do socket, aplicado uma vez e válido
  para conectar e para ler: 10 s abortaram `sys_load_error_detail` no ambiente alvo (2026-09-20), e
  o socket não voltou a servir (`cannot read from timed out object`). O probe usa 30 s sobre visões
  de sistema; a suíte e a biblioteca conectam sem `timeout`, porque um `COPY` dura mais que qualquer
  espera de leitura. `ssl=True` (`verify-ca`) é o padrão da biblioteca.

### Data API

A Data API executa SQL por HTTPS, sem a porta 5439, e é assíncrona: `ExecuteStatement` devolve o
identificador na hora, `DescribeStatement` é consultado até o estado ser `FINISHED`, `FAILED` ou
`ABORTED`, e `GetStatementResult` devolve o resultado paginado
([`../examples/redshift_data_api.py`](../examples/redshift_data_api.py)). Ela serve a comandos e a
diagnóstico; a troca de dados da biblioteca não passa por ela:

- Cada célula é um dicionário de um item (`stringValue`, `longValue`, `doubleValue`, `booleanValue`,
  `blobValue`) ou `{"isNull": true}`. `DECIMAL` chega como texto, e data e hora também: o tipo do
  contrato se perde no caminho.
- O resultado morre em 24 horas e para em 500 MB depois da compressão; o statement vai até 200 KB e
  a consulta até 24 horas. O máximo é 500 consultas ativas e 500 sessões por warehouse.
- `WaitTimeSeconds` faz a chamada esperar até 30 segundos pelo fim, no lugar de um laço de consultas.
- A sessão morre com o statement, a não ser que `SessionKeepAliveSeconds` a mantenha (24 horas no
  máximo) e as chamadas seguintes levem o `SessionId`: sem isso não há tabela temporária entre um
  comando e o outro, e uma sessão roda um statement por vez.
- `BatchExecuteStatement` roda os comandos em série, numa transação por padrão (`ExecutionMode`
  `TRANSACTION`) ou um a um com `AUTO_COMMIT`.

### O esquema do projeto num banco de datashare

O esquema do projeto está num banco de datashare. Uma sessão conectada ao banco local cita a tabela
por nome em três partes, `banco.esquema.tabela`; `USE <banco>` troca o banco da sessão, e a partir
dele `esquema.tabela` basta, que é como o `CREATE TABLE`, o `COPY` e o `UNLOAD` passaram no ambiente
alvo ([`../examples/redshift_copy_unload.py`](../examples/redshift_copy_unload.py)). A restrição
documentada, de que só o nome em três partes vale, se aplica a quem não está conectado ao banco
compartilhado. `current_database()` continua a responder o banco da conexão depois do `USE` (leitura de
2026-09-21 no ambiente alvo por `probes/redshift.py`, confirmada pelo usuário no mesmo dia): a
troca é confirmada pela resolução de um nome em duas partes, como o `CREATE TABLE` dos exemplos,
e não por essa função. `svv_redshift_databases` diz o tipo de
cada banco (`local` ou `shared`) e o nível de isolamento; `svv_all_schemas` diz em que banco está
cada esquema. `has_schema_privilege` e `svv_table_info` enxergam o banco da sessão: antes do `USE`,
o local, e num esquema compartilhado quem concede `USAGE` e `CREATE` é o produtor e a lista de
tabelas vem de `svv_all_tables`, que cruza bancos; o que as duas respondem depois do `USE` ainda não
foi lido, e `probes/redshift.py` (`RS-5`, `RS-8`) o lê como leitura, sem reprovar.

Os objetos de um datashare só aceitam escrita quando o produtor concede `INSERT`, `CREATE` e os
demais privilégios ao datashare, e o consumidor precisa atender três requisitos:

| Requisito | Onde ler |
| --- | --- |
| Patch 186: `1.0.78890` ou maior no serverless, `1.0.78881` no provisionado | `select version()` |
| Isolamento de snapshot no banco que recebe a escrita | `svv_redshift_databases.database_isolation_level` |
| 64 slices ou mais no consumidor | `select count(*) from stv_slices` |

O que o Redshift aceita escrever num datashare, e o que ele não lista:

- DDL: `CREATE`/`DROP SCHEMA`, `CREATE`/`DROP`/`SHOW TABLE`, `CREATE TABLE ... AS`, `ALTER TABLE
  ADD`/`DROP COLUMN`, `ALTER TABLE RENAME`, `ALTER SCHEMA RENAME`, `TRUNCATE`, `BEGIN` e `COMMIT`.
- DML: `SELECT`, `INSERT`, `INSERT INTO SELECT`, `UPDATE`, `DELETE`, `MERGE` e **`COPY` sem
  `COMPUPDATE`**. `COMPUPDATE` é a compressão automática do `COPY`, que numa tabela vazia troca a
  codificação das colunas a partir de uma amostra; o `COPY` de Parquet não aceita o parâmetro e não
  aplica compressão automática (seção "Regras do COPY para Parquet"), então o da biblioteca satisfaz
  a regra por construção, e foi assim, sem cláusula alguma, que ele passou no ambiente alvo em
  2026-09-20. A codificação das colunas vem do DDL ou de `ENCODE AUTO`.
- `UNLOAD` não está na lista dos comandos suportados nem na dos recusados, e passou no ambiente alvo
  a partir de uma tabela do datashare: `FORMAT AS PARQUET` sem `PARTITION BY` em 2026-09-20, e
  `PARTITION BY (<coluna>) MANIFEST VERBOSE` em 2026-09-21, este em 0,8 s sobre 500.000 linhas
  ([`../examples/redshift_manifest.py`](../examples/redshift_manifest.py)).
- `COPY ... FORMAT AS PARQUET MANIFEST` passou numa tabela do datashare em 2026-09-21, 500.000
  linhas em 4,6 s a partir de um manifesto de uma entrada apontando para um arquivo gravado pelo
  delta-rs: o manifesto se comporta como numa tabela local.
- A escrita de uma transação vai para um banco só, e um comando múltiplo fora de um bloco de
  transação não é aceito: a transação da publicação abre com `BEGIN` explícito, e a tabela de
  controle mora no mesmo banco das tabelas publicadas.
- `VIEW` e `MATERIALIZED VIEW` não podem ser criadas, alteradas nem apagadas num banco de datashare.
- `TRUNCATE` numa tabela remota é transacional, ao contrário do `TRUNCATE` local, que confirma
  sozinho.
- O consumidor não altera nem apaga o datashare, e não põe um objeto dele em outro datashare.

### O S3 alcançado pelas credenciais de quem chama

O `COPY` e o `UNLOAD` aceitam `ACCESS_KEY_ID`, `SECRET_ACCESS_KEY` e `SESSION_TOKEN` no texto do
comando, em vez de `IAM_ROLE`. É o que destrava o ambiente alvo, onde o namespace não tem papel IAM
associado e por isso nem um ARN explícito funcionaria: quem alcança o S3 passa a ser a identidade da
sessão, a mesma que o `boto3` usa. As credenciais do espaço expiram em cerca de uma hora, então a
cláusula é montada a cada comando, nunca guardada, e **nenhum texto que a carregue vai para log,
para o relatório da suíte ou para arquivo** — `tests/conftest.py` mascara toda cláusula de
credencial antes de gravar o relatório. O `COPY` desse caminho leu um prefixo de pasta Parquet
direto, sem manifesto, e converteu `int32` da origem para a coluna `BIGINT` do contrato; em
2026-09-21 leu também um manifesto, e o `UNLOAD` gravou com `PARTITION BY ... MANIFEST VERBOSE`
pelas mesmas credenciais.

O que o ambiente alvo respondeu a esses requisitos, lido em 2026-09-20 por `probes/redshift.py`
([`POC.md`](POC.md), [`readings/`](readings/)): o patch `1.0.436211` atende; o isolamento do banco
que recebe a escrita fica no produtor e chega ao consumidor como `UNKNOWN`; `stv_slices` é negada a
um usuário comum, então os 64 slices não são verificáveis pela sessão. Nenhum papel IAM está
associado ao namespace, e é por isso que o `COPY` e o `UNLOAD` levam as credenciais de quem chama. `has_database_privilege(dev, CREATE)` é falso e `TEMP` é verdadeiro. `pg_settings` do
serverless não lista `timezone` nem `enable_case_sensitive_identifier`, que `SHOW` responde.

A lista dos comandos recusados acrescenta três que o projeto precisa conhecer: uma referência a
objeto que não seja o nome em três partes, quando a sessão não está conectada ao banco
compartilhado; a escrita numa tabela com chave de ordenação intercalada, que o projeto não usa (o
`sort_key` de `Table.info` é composto); e `UPDATE`, `INSERT` ou `COPY` em coluna de identidade
quando o consumidor tem mais slices que o produtor, que o projeto também não usa, porque as chaves
são geradas no cliente. O `UNLOAD` não aparece em nenhuma das duas listas.

## Organização dos dados

### Armazenamento colunar e processamento paralelo

O Redshift guarda cada coluna em blocos de 1 MB. Uma consulta lê só os blocos das colunas que usa, e
cada bloco recebe a codificação de compressão adequada ao tipo da coluna: por padrão `AZ64` para
inteiros, decimais, datas e timestamps, `LZO` para `CHAR` e `VARCHAR`, `RAW` para `BOOLEAN`, ponto
flutuante e colunas de chave de ordenação, e `ZSTD` para `SUPER`. Com `ENCODE AUTO`, o padrão, o
banco ajusta a codificação ao longo do tempo.

O cluster tem um nó líder, que compila cada consulta em código executável, e nós de computação
divididos em slices. As linhas de uma tabela são distribuídas entre as slices pelo estilo de
distribuição (`AUTO`, `EVEN`, `KEY`, `ALL`), e dentro de cada slice ficam ordenadas pela chave de
ordenação. Cada bloco guarda os valores mínimo e máximo; um predicado por intervalo sobre a chave de
ordenação pula os blocos fora do intervalo. Numa tabela com cinco anos ordenados por data, um filtro
de um mês evita até 98 % dos blocos. Nas consultas, o otimizador redistribui linhas entre nós para
joins e agregações, e essa redistribuição pode responder por parte substancial do custo do plano.

A primeira execução de uma consulta inclui a compilação; as seguintes usam o cache de código, local e
remoto. Medições comparam sempre a segunda execução.

### Diferenças para o PostgreSQL

O Redshift deriva do PostgreSQL, mas o armazenamento e o executor são outros. A documentação avisa
que elementos com o mesmo nome podem ter semântica diferente. O que muda para o projeto:

| Aspecto | PostgreSQL | Redshift |
| --- | --- | --- |
| Índices | B-tree, hash, GIN. | Não existem. Chaves de ordenação e de distribuição fazem o papel. |
| `PRIMARY KEY`, `UNIQUE`, `FOREIGN KEY` | Aplicadas. | Informativas: o planejador as usa e supõe que valem; `NOT NULL` é aplicado. |
| Autoincremento | `SERIAL`, sequências. | Sem sequências; `IDENTITY(seed, step)` ou `GENERATED BY DEFAULT AS IDENTITY`. Os valores são únicos, com saltos e sem ordem garantida em cargas paralelas. |
| Tipos ausentes | | Arrays, `JSON`, `UUID`, `BYTEA`, tipos compostos e enumerados. `SUPER` cobre dados semiestruturados, `VARBYTE` cobre binários. |
| `CHECK` | Aplicado. | Não suportado. |
| `ALTER TABLE` | Geral. | Uma coluna por `ADD COLUMN`; `ALTER COLUMN TYPE` só muda o tamanho de `VARCHAR`; alterações de chaves e codificação por cláusulas próprias. |
| `VACUUM` | Recupera espaço. | O padrão do comando é `VACUUM FULL`, que recupera espaço e reordena; a ordenação automática e o `VACUUM DELETE` rodam sozinhos em segundo plano. |
| `TRUNCATE` | Transacional. | Faz commit da transação corrente. |
| Particionamento, tablespaces, herança, triggers, `VALUES` como tabela | Existem. | Não existem. |
| Isolamento | Read committed por padrão. | Snapshot isolation por padrão em clusters e workgroups novos, com o nível serializável como opção; sob o serializável, o segundo de dois escritores conflitantes é abortado. |
| Comprimento de `VARCHAR(n)` | Caracteres. | Bytes; um caractere UTF-8 pode ocupar até 4 bytes. `TEXT` vira `VARCHAR(256)`. |
| Espaços finais | Significativos. | Ignorados na comparação de `VARCHAR`. |
| Tamanho de comando | Sem limite prático. | 16 MB por comando SQL; 4 MB por linha de entrada no `COPY`. |
| Identificadores | 63 bytes. | 127 bytes; 1.600 colunas por tabela. |
| Subconsultas em `INSERT ... VALUES` de várias linhas | Aceitas. | Rejeitadas. |
| `UPDATE ... FROM` com outer join | Aceito. | Rejeitado; a alternativa é subconsulta no `WHERE`. |

### Efeitos nas formas de manipular os dados

Na entrada, o `COPY` a partir do S3 é o caminho recomendado: ele carrega em paralelo pelas slices,
divide arquivos de 128 MB ou mais em pedaços e, nos formatos de texto, aplica compressão automática.
`INSERT` de uma linha por comando é "proibitivamente lento" nas palavras da documentação; quando o
`COPY` não é possível, o `INSERT` de várias linhas num único comando é a alternativa. Alterações em
lote passam por uma tabela de staging e por `MERGE`, ou por `DELETE` mais `INSERT`, nunca por
`UPDATE` linha a linha.

Na saída, o `UNLOAD` grava Parquet no S3 em paralelo, um ou mais arquivos por slice. Consultas que
voltam pela conexão do cliente chegam linha a linha pelo protocolo do PostgreSQL, sem formato
colunar; o driver ADBC é a exceção, com resultado em Arrow.

Depois de um `COPY` grande, de `DELETE` ou de `UPDATE` extensos, a tabela precisa de `VACUUM` e
`ANALYZE`, que o banco agenda sozinho em períodos de baixa carga; cargas em ordem da chave de
ordenação (meses novos ao fim de uma tabela ordenada por data) dispensam a reordenação. Tabelas por
período, unidas por uma view `UNION ALL`, permitem descartar meses antigos com `DROP TABLE` em vez
de `DELETE`.

## Tipos suportados

| Tipo | Apelidos | Descrição |
| --- | --- | --- |
| `SMALLINT`, `INTEGER`, `BIGINT` | `INT2`, `INT`/`INT4`, `INT8` | Inteiros de 2, 4 e 8 bytes. |
| `DECIMAL(p, s)` | `NUMERIC` | Decimal exato; precisão até 38, padrão `DECIMAL(18, 0)`. |
| `REAL`, `DOUBLE PRECISION` | `FLOAT4`, `FLOAT8`/`FLOAT` | Ponto flutuante de 4 e 8 bytes. |
| `CHAR(n)` | `CHARACTER`, `NCHAR`, `BPCHAR` | Fixo, até 4.096 bytes, sem multibyte. |
| `VARCHAR(n)` | `CHARACTER VARYING`, `NVARCHAR`, `TEXT` | Variável, até 65.535 bytes (`VARCHAR(MAX)`); `n` conta bytes. |
| `DATE` | | Data. |
| `TIME`, `TIMETZ` | | Hora sem e com fuso. |
| `TIMESTAMP`, `TIMESTAMPTZ` | `TIMESTAMP WITHOUT/WITH TIME ZONE` | Data e hora; conversões entre os dois usam o fuso da sessão, UTC por padrão. |
| `INTERVAL YEAR TO MONTH`, `INTERVAL DAY TO SECOND` | | Durações. |
| `BOOLEAN` | `BOOL` | Lógico. |
| `SUPER` | | Semiestruturado: escalares, arrays e estruturas, até 16 MB por valor. |
| `VARBYTE` | `VARBINARY`, `BINARY VARYING` | Binário variável. |
| `HLLSKETCH`, `GEOMETRY`, `GEOGRAPHY` | | Sketches HyperLogLog e dados espaciais. |

Conversões implícitas seguem a categoria do tipo: um decimal inserido em coluna inteira é
arredondado; uma string que representa um número ou uma data converte para o tipo da coluna; um
`VARCHAR` com multibyte não é comparável a `CHAR`. A correspondência com os tipos do contrato está
na [tabela de tipos](schema.md).

O tipo que o dialeto `sqlalchemy-redshift` emite no DDL para cada tipo do contrato, compilado nesta
sessão. `dialect` e `sql()` servem aos exemplos seguintes deste documento:

```python
import sqlalchemy as sa
from sqlalchemy_redshift.dialect import RedshiftDialect_redshift_connector

# Tipo emitido no DDL para cada tipo do contrato.
dialect = RedshiftDialect_redshift_connector()

def sql(command) -> str:
    return str(command.compile(dialect=dialect, compile_kwargs={"literal_binds": True}))

for sa_type in [sa.BigInteger(), sa.Numeric(18, 2), sa.Double(), sa.String(200), sa.String(65535), sa.Text(),
             sa.Date(), sa.DateTime(), sa.DateTime(timezone=True), sa.Boolean(), sa.Uuid(), sa.JSON(),
             sa.LargeBinary()]:
    print(f"{sa_type!r:32} {sa_type.compile(dialect=dialect)}")
```

```text
BigInteger()                     BIGINT
Numeric(precision=18, scale=2)   NUMERIC(18, 2)
Double()                         DOUBLE PRECISION
String(length=200)               VARCHAR(200)
String(length=65535)             VARCHAR(65535)
Text()                           TEXT
Date()                           DATE
DateTime()                       TIMESTAMP WITHOUT TIME ZONE
DateTime(timezone=True)          TIMESTAMP WITH TIME ZONE
Boolean()                        BOOLEAN
Uuid()                           UUID
JSON()                           JSON
LargeBinary()                    BYTEA
```

`Text` sai como `TEXT`, que a tabela de diferenças registra como `VARCHAR(256)`; o `VARCHAR(65535)`
do contrato exige `String(65535)`. `Uuid`, `JSON` e `LargeBinary` compilam para `UUID`, `JSON` e
`BYTEA`, tipos que a mesma tabela lista como ausentes: o dialeto não os rejeita na compilação, e o
`VARCHAR(36)` do contrato para `Uuid` exige `String(36)` ou uma regra `@compiles`.

### DECIMAL com escala fixa

`DECIMAL(precisao, escala)` guarda até 38 dígitos; a precisão padrão é 18 e a escala padrão é 0. A
representação depende da precisão: até 19 dígitos, inteiro de 8 bytes; de 20 a 38, inteiro de 16
bytes, que ocupa o dobro em disco e torna as consultas mais lentas. A documentação pede que a precisão
máxima não seja atribuída sem necessidade. `DECIMAL(18, 2)`, o tipo do contrato para valores
contábeis, fica em 8 bytes nos dois bancos; o `decimal128(18, 2)` do Arrow, que o representa nos
arquivos e na memória, ocupa 16 bytes por valor.

Regras de carga documentadas:

- Um valor com escala maior que a da coluna é arredondado: `4323.8951` numa coluna `DECIMAL(8, 2)`
  vira `4323.90`, e `20.259` em `DECIMAL(8, 2)` vira `20.26`.
- Um valor cuja parte inteira não cabe em `precisao - escala` é rejeitado: o intervalo de
  `DECIMAL(5, 2)` é `-999.99` a `999.99`.
- O maior valor de qualquer `DECIMAL` de 19 dígitos é `9223372036854775807`; `DECIMAL(19, 18)` para
  em `9.223372036854775807`.
- Resultados de casts explícitos em `SELECT` não são arredondados.
- `REAL` e `DOUBLE PRECISION` são aproximados; a documentação manda usar `DECIMAL` para valores
  monetários.

A carga por `COPY` de Parquet exige um `DECIMAL` do arquivo compatível com a coluna. O
[documento sobre Parquet](parquet.md) registra que a tabela de correspondência de tipos físicos do
Parquet para o `COPY` fica pendente da prova de conceito.

A conferência de escala e precisão acontece no Arrow, antes do `COPY`, com os valores das regras
acima:

```python
from decimal import Decimal
import pyarrow as pa, pyarrow.compute as pc

# Escala e precisão conferidas no Arrow, antes do COPY, com os valores das regras de carga.
values = pa.array([Decimal("4323.8951"), Decimal("20.259")])         # inferido como decimal128(8, 4)
try:
    values.cast(pa.decimal128(18, 2))
except pa.ArrowInvalid as error:
    print(error)                                          # Rescaling Decimal value would cause data loss
print(values.cast(pa.decimal128(18, 2), safe=False).to_pylist())   # [4323.89, 20.25]: trunca
print(pc.round(values, 2).cast(pa.decimal128(18, 2)).to_pylist())  # [4323.90, 20.26]: os valores da carga
try:
    pa.array([Decimal("1000.00")], pa.decimal128(6, 2)).cast(pa.decimal128(5, 2))
except pa.ArrowInvalid as error:
    print(error)                                          # Decimal value does not fit in precision 5
```

O cast seguro rejeita a perda de escala e o estouro de precisão; `pa.Table.from_pandas(df,
schema=esquema)` falha com a mesma mensagem numa coluna `object` de `Decimal`. `safe=False` trunca e
diverge do arredondamento da carga; `pc.round` antes do cast reproduz os valores documentados. A
biblioteca rejeita o lote ou arredonda de forma explícita; nos dois casos, o valor que chega ao
`COPY` já tem a escala da coluna.

### JSON e o tipo SUPER

O Redshift não tem tipo `JSON`. As opções são:

- Guardar o texto num `VARCHAR` e usar as funções textuais (`JSON_EXTRACT_PATH_TEXT` e afins). A
  documentação desaconselha: cada consulta reanalisa o texto e o formato não usa o
  armazenamento colunar.
- Guardar num `SUPER`, o tipo recomendado. `JSON_PARSE(texto)` converte na inserção
  (`INSERT INTO t VALUES (JSON_PARSE('{"a": 1}'))`), e `JSON_SERIALIZE(valor)` devolve o texto. O
  `COPY` carrega `SUPER` a partir de JSON, Avro, texto, CSV, Parquet e ORC; valores acima de 1 MB só
  entram por Parquet, JSON, texto ou CSV. A navegação usa PartiQL: `SELECT doc.a, doc.b.c[0] FROM t`
  e `FROM t, t.doc.itens AS item` para desaninhar arrays, com tipagem dinâmica e semântica lax (erros
  de tipo viram `NULL`).

Limites do `SUPER`: 16 MB por valor, profundidade de 1.000 níveis, strings de até 16.000.000 bytes,
sem uso como chave de distribuição ou de ordenação, sem atualização parcial, sem right join ou full
outer join sobre a coluna, sem cast de datas para `SUPER` (o inverso funciona). Um `SUPER` com
objeto ou array vira `NULL` ao ser convertido para `VARCHAR`; `JSON_SERIALIZE` é a conversão
correta. A documentação recomenda `enable_case_sensitive_super_attribute = true` e, para consultas
frequentes, materializar os atributos em views materializadas com colunas convencionais.

No `awswrangler`, colunas Arrow de tipo `list`, `struct` e `map` viram `SUPER` na criação da tabela,
e a opção `serialize_to_json` acrescenta `SERIALIZETOJSON` ao `COPY` de Parquet. No
`sqlalchemy-redshift`, o tipo `SUPER` existe para modelos e reflexão.

No contrato ([campos JSON](schema.md)), a coluna é `sa.JSON().with_variant(SUPER(), "redshift")`: o
dialeto compila `SUPER` no `CREATE TABLE`, e o `duckdb_engine` compila `JSON`. Os arquivos do Delta
trazem o documento como texto; a carga passa pela staging `VARCHAR(65535)` e o `INSERT ... SELECT`
aplica `JSON_PARSE`; a exportação devolve texto com `JSON_SERIALIZE`. Se o `COPY` de Parquet carrega
o texto diretamente numa coluna `SUPER`, e o que acontece com documentos acima de 65.535 bytes, fica
pendente da prova de conceito.

```python
import sqlalchemy as sa
from sqlalchemy.schema import CreateTable
from sqlalchemy_redshift.dialect import RedshiftDialect_redshift_connector, SUPER

# JSON no contrato: SUPER no Redshift, texto nos arquivos, JSON_PARSE na carga e JSON_SERIALIZE na saída.
events = sa.Table("eventos", sa.MetaData(),
                   sa.Column("id_evento", sa.BigInteger, primary_key=True, autoincrement=False),
                   sa.Column("meta", sa.JSON().with_variant(SUPER(), "redshift")))
print(CreateTable(events).compile(dialect=RedshiftDialect_redshift_connector()))
# CREATE TABLE eventos (id_evento BIGINT NOT NULL, meta SUPER, PRIMARY KEY (id_evento))

load_sql = """INSERT INTO prod_eventos (id_evento, meta, mes)
SELECT id_evento, JSON_PARSE(meta), '2026-08' FROM staging_eventos"""
unload_sql = """UNLOAD ('SELECT id_evento, JSON_SERIALIZE(meta) AS meta, mes FROM exec_42_eventos')
TO 's3://bucket/prod/eventos/' IAM_ROLE 'arn:aws:iam::123456789012:role/papel'
FORMAT AS PARQUET PARTITION BY (mes) MANIFEST VERBOSE"""
```

## DDL

### CREATE TABLE

```sql
CREATE TABLE IF NOT EXISTS operacoes (
    id_operacao BIGINT NOT NULL,
    data_ref DATE NOT NULL,
    id_cliente BIGINT NOT NULL,
    valor DECIMAL(18, 2) NOT NULL,
    descricao VARCHAR(200) ENCODE zstd,
    PRIMARY KEY (id_operacao, data_ref)
)
DISTSTYLE KEY DISTKEY (id_cliente)
COMPOUND SORTKEY (data_ref, id_operacao);

COMMENT ON TABLE operacoes IS 'Operações do mês';
```

Sintaxe, segundo a referência:

```text
CREATE [ [LOCAL] { TEMPORARY | TEMP } ] TABLE [ IF NOT EXISTS ] nome
( { coluna tipo [DEFAULT expr] [IDENTITY(seed, step) | GENERATED BY DEFAULT AS IDENTITY(seed, step)]
      [ENCODE codificacao] [DISTKEY] [SORTKEY] [COLLATE {CASE_SENSITIVE | CASE_INSENSITIVE}]
      [NOT NULL | NULL] [UNIQUE | PRIMARY KEY] [REFERENCES tabela [(coluna)]]
  | UNIQUE (colunas) | PRIMARY KEY (colunas) | FOREIGN KEY (colunas) REFERENCES tabela [(coluna)]
  | LIKE tabela_pai [{INCLUDING | EXCLUDING} DEFAULTS] } [, ...] )
[ BACKUP { YES | NO } ]
[ DISTSTYLE { AUTO | EVEN | KEY | ALL } ] [ DISTKEY (coluna) ]
[ [COMPOUND | INTERLEAVED] SORTKEY (colunas) | SORTKEY AUTO ] [ ENCODE AUTO ]
```

Pontos da referência:

- `DISTSTYLE`, `SORTKEY` e `ENCODE` têm padrão `AUTO`. Uma codificação explícita em qualquer coluna
  desliga `ENCODE AUTO` para a tabela inteira. Uma tabela com `DISTSTYLE` ou `SORTKEY` explícitos sai
  da otimização automática.
- Compound aceita até 400 colunas; interleaved, até 8, e não deve usar colunas monotônicas como
  datas e identidades.
- Tipos aceitos em `DISTKEY` e `SORTKEY`: booleanos, numéricos, datas, horas, timestamps, `CHAR` e
  `VARCHAR`; `SUPER` não.
- `IDENTITY` exige `INT` ou `BIGINT` e é `NOT NULL`; `COPY` e `INSERT ... SELECT` geram valores com
  saltos. `GENERATED BY DEFAULT AS IDENTITY` aceita valores fornecidos sem verificar unicidade.
- `LIKE` copia nomes, tipos, `NOT NULL`, estilo de distribuição, chaves de ordenação e `BACKUP`;
  não copia chaves primárias nem estrangeiras, e só copia `DEFAULT` com `INCLUDING DEFAULTS`.
- `BACKUP NO` não tem efeito em clusters RA3 e RG nem em workgroups serverless: a tabela entra nos
  snapshots de qualquer forma.
- `CREATE TABLE ... AS SELECT` aceita `DISTSTYLE`, `DISTKEY` e `SORTKEY`, herda os tipos da consulta
  e recebe `ANALYZE` automático.
- Tabelas temporárias vivem num esquema da sessão, aceitam nome igual ao de uma permanente e recebem
  codificação `RAW`. Um nome iniciado por `#` cria uma tabela temporária.
- Limites: 127 bytes por nome, 1.600 colunas, cota de tabelas por tipo de nó.

As tabelas do sandbox recebem o prefixo da execução no nome, dentro do único esquema. O `Table` do
modelo `Operacao` da seção sobre o suporte a SQLAlchemy, renomeado, gera o DDL:

```python
import sqlalchemy as sa
from sqlalchemy.schema import CreateTable

# DDL de uma tabela do sandbox: o Table do modelo renomeado com o prefixo da execução.
def ddl_sandbox(model, prefix: str) -> str:
    table = model.__table__.to_metadata(sa.MetaData(), name=f"{prefix}_{model.__tablename__}")
    return str(CreateTable(table, if_not_exists=True).compile(dialect=dialect))

print(ddl_sandbox(Operacao, "exec_abc123"))
```

`to_metadata` copia colunas, restrições, comentários e os argumentos `redshift_*`. A saída é o
`CREATE TABLE IF NOT EXISTS exec_abc123_operacoes (...) DISTSTYLE KEY DISTKEY (id_cliente) SORTKEY
(data_ref, id_operacao)` compilado na seção sobre o modelo, com o nome novo e `IF NOT EXISTS`.

### ALTER TABLE

```sql
ALTER TABLE operacoes ADD COLUMN moeda VARCHAR(3) DEFAULT 'BRL' ENCODE bytedict;
ALTER TABLE operacoes DROP COLUMN moeda;
ALTER TABLE operacoes RENAME COLUMN descricao TO historico;
ALTER TABLE operacoes ALTER COLUMN historico TYPE VARCHAR(400);
ALTER TABLE operacoes ALTER COLUMN historico ENCODE lzo;
ALTER TABLE operacoes ALTER DISTKEY id_cliente;
ALTER TABLE operacoes ALTER DISTSTYLE AUTO;
ALTER TABLE operacoes ALTER COMPOUND SORTKEY (data_ref, id_operacao);
ALTER TABLE operacoes ALTER SORTKEY AUTO;
ALTER TABLE operacoes ADD CONSTRAINT operacoes_pk PRIMARY KEY (id_operacao, data_ref);
ALTER TABLE operacoes DROP CONSTRAINT operacoes_pk;
ALTER TABLE operacoes RENAME TO operacoes_2026;
ALTER TABLE operacoes OWNER TO pipeline;
```

Regras da referência:

- `ALTER TABLE` bloqueia a tabela para leitura e escrita até o fim da transação, salvo onde a
  documentação diz o contrário (troca de codificação mantém a tabela consultável).
- `ADD COLUMN` aceita uma coluna por comando, e a coluna nova não pode ser chave de distribuição, de
  ordenação, `UNIQUE`, `PRIMARY KEY`, `REFERENCES` nem identidade.
- `ALTER COLUMN TYPE` só muda o tamanho de um `VARCHAR`, sem descer abaixo do maior valor
  existente, fora de transação, e não aceita colunas
  com `DEFAULT`, com chaves, nem com codificações `BYTEDICT`, `RUNLENGTH`, `TEXT255` e `TEXT32K`.
- `ALTER DISTKEY`, `ALTER DISTSTYLE` e `ALTER SORTKEY` não rodam junto com `VACUUM`, não valem para
  tabelas temporárias nem com chave interleaved, e retiram a tabela da otimização automática quando
  ela estava em `AUTO`. Uma chave compound pode virar interleaved apenas recriando a tabela.
- `ADD CONSTRAINT PRIMARY KEY` exige colunas `NOT NULL`. Os nomes das restrições estão em
  `information_schema.table_constraints`.
- Combinações num só comando reduzem o tempo: `ALTER SORTKEY (...), ALTER DISTKEY coluna`.

O SQLAlchemy não tem construto para `ADD COLUMN`. Sem Alembic, a biblioteca monta esse comando com a
especificação de coluna do compilador do dialeto, que rende `DEFAULT` e `ENCODE`, usa os construtos
do Core para restrições e `sa.DDL` para as demais cláusulas:

```python
import sqlalchemy as sa
from sqlalchemy.schema import AddConstraint, DropConstraint

# ADD COLUMN com a especificação de coluna do compilador do dialeto; ADD e DROP CONSTRAINT do Core.
compiler = dialect.ddl_compiler(dialect, None)
currency = sa.Column("moeda", sa.String(3), server_default="BRL", redshift_encode="bytedict")
print(f"ALTER TABLE operacoes ADD COLUMN {compiler.get_column_specification(currency)}")
pk = Operacao.__table__.primary_key
print(AddConstraint(pk).compile(dialect=dialect))
print(DropConstraint(pk).compile(dialect=dialect))
print(sa.DDL("ALTER TABLE %(table)s ALTER COLUMN descricao TYPE VARCHAR(400)")
      .against(Operacao.__table__).compile(dialect=dialect))
```

```sql
ALTER TABLE operacoes ADD COLUMN moeda VARCHAR(3) DEFAULT 'BRL' ENCODE bytedict
ALTER TABLE operacoes ADD CONSTRAINT operacoes_pk PRIMARY KEY (id_operacao, data_ref)
ALTER TABLE operacoes DROP CONSTRAINT operacoes_pk
ALTER TABLE operacoes ALTER COLUMN descricao TYPE VARCHAR(400)
```

A saída supõe `metadata = sa.MetaData(naming_convention={"pk": "%(table_name)s_pk"})` na classe
`Base`, que também leva o nome ao `CREATE TABLE` (`CONSTRAINT operacoes_pk PRIMARY KEY (...)`). Sem
nome na chave, `AddConstraint` emite `ADD PRIMARY KEY (id_operacao, data_ref)` e `DropConstraint`
falha na compilação: `Can't emit DROP CONSTRAINT for constraint PrimaryKeyConstraint(...); it has no
name`.

### DROP TABLE

```sql
DROP TABLE IF EXISTS operacoes_2026 CASCADE;
DROP TABLE staging_a, staging_b;
```

`RESTRICT`, o padrão, falha com `cannot drop table ... because other objects depend on it` quando
há views dependentes; `CASCADE` remove as views, exceto as criadas com `WITH NO SCHEMA BINDING`. A
referência traz a consulta em `pg_depend` que lista os dependentes. `DROP TABLE` de uma tabela
externa não roda dentro de transação.

`DropTable(table, if_exists=True)` do SQLAlchemy compila para `DROP TABLE IF EXISTS <nome>`; o
construto não tem parâmetro para `CASCADE`, que entra por `sa.DDL`.

### Chaves, restrições e índices

Não há índices. `PRIMARY KEY`, `UNIQUE` e `FOREIGN KEY` são declaradas para o planejador, que as usa
para decorrelacionar subconsultas, ordenar e eliminar joins, e supõe que valem: uma chave primária
duplicada faz `SELECT DISTINCT` devolver duplicatas. A regra da documentação é declará-las apenas
quando o processo de carga garante a integridade, o que a [política de restrições](schema.md) do
projeto traduz em "declarada quando auditada". `NOT NULL` é aplicado e vale como validação de carga:
um `COPY` que tenta gravar `NULL` numa coluna `NOT NULL` falha.

### Documentação do esquema

```sql
COMMENT ON TABLE operacoes IS 'Operações do mês';
COMMENT ON COLUMN operacoes.id_cliente IS 'Chave do cliente';
COMMENT ON CONSTRAINT operacoes_pk ON operacoes IS 'Auditada na carga';
COMMENT ON SCHEMA execucao_abc123 IS 'Sandbox da execução abc123';
SELECT obj_description('public.operacoes'::regclass);
SELECT col_description('public.operacoes'::regclass, 3);
```

`COMMENT ON` cobre tabelas, colunas, restrições, bancos, views e esquemas; o texto fica em
`pg_description`. Só o superusuário ou o dono do objeto comenta. Tabelas externas, colunas externas
e colunas de views de ligação tardia não aceitam comentários.

Os atributos `comment` do modelo geram os mesmos comandos. O dialeto declara `supports_comments =
True` e `inline_comments = False`, e `create_all` num `create_mock_engine` emitiu o `COMMENT ON
TABLE` e o `COMMENT ON COLUMN` logo depois do `CREATE TABLE`:

```python
from sqlalchemy.schema import SetTableComment, SetColumnComment

# COMMENT ON gerado dos atributos comment do modelo.
print(SetTableComment(Operacao.__table__).compile(dialect=dialect))
print(SetColumnComment(Operacao.__table__.c.id_cliente).compile(dialect=dialect))
```

```sql
COMMENT ON TABLE operacoes IS 'Operações do mês'
COMMENT ON COLUMN operacoes.id_cliente IS 'Chave do cliente'
```

O modelo leva `__table_args__ = {"comment": "Operações do mês", ...}` e
`id_cliente: Mapped[int] = mapped_column(BigInteger, comment="Chave do cliente")`.

## SELECT, INSERT, UPDATE e DELETE

Todo comando tem 16 MB no máximo. Os dados de entrada e saída do pipeline são DataFrames pandas com
backend pyarrow ou tabelas Arrow; a conexão de referência é o `redshift_connector`.

### SELECT

```text
[ WITH ... ] SELECT [ TOP n | [ALL | DISTINCT] ] lista [ EXCLUDE colunas ]
[ FROM ... ] [ WHERE ... ] [ GROUP BY ALL | ... ] [ HAVING ... ] [ QUALIFY ... ]
[ UNION | INTERSECT | EXCEPT ... ] [ ORDER BY ... ] [ LIMIT n | ALL ] [ OFFSET n ]
```

`QUALIFY` filtra funções de janela, `EXCLUDE` retira colunas do `*`, `GROUP BY ALL` agrupa por todas
as colunas não agregadas. A saída pelo `redshift_connector` chega em tuplas Python:
`cursor.fetch_dataframe()` monta um DataFrame a partir das tuplas, com nomes em minúsculas e tipos
inferidos pelo pandas (`Decimal` e `date` ficam em colunas `object`), e `cursor.fetch_numpy_array()`
devolve um array. Um DataFrame com os tipos do contrato sai de
tuplas convertidas em dicionários por nome de coluna,
`pa.Table.from_pylist([dict(zip(names, row)) for row in cursor.fetchall()], schema=schema)`, seguido de
`to_pandas(types_mapper=pd.ArrowDtype)`; `Decimal` e `date` das tuplas entram em `decimal128` e
`date32` sem conversão para `float`. Volumes grandes saem por `UNLOAD` e voltam pelo leitor Parquet.

O caminho pelas tuplas, com a consulta compilada e o esquema Arrow do modelo (`arrow_schema` em
[sqlalchemy.md](sqlalchemy.md)):

```python
import datetime as dt
from decimal import Decimal
import pandas as pd, pyarrow as pa, sqlalchemy as sa
from sqlalchemy import select

# Do select do contrato ao DataFrame com os tipos do contrato, a partir das tuplas do redshift_connector.
query = (select(Operacao).where(Operacao.mes == "2026-08")
            .order_by(Operacao.data_ref, Operacao.id_operacao).limit(10))
print(sql(query))    # SELECT operacoes.id_operacao, ... WHERE operacoes.mes = '2026-08' ORDER BY ... LIMIT 10

def to_dataframe(rows: list[tuple], query: sa.Select, schema: pa.Schema) -> pd.DataFrame:
    names = list(query.selected_columns.keys())
    data = pa.Table.from_pylist([dict(zip(names, row)) for row in rows], schema=schema)
    return data.to_pandas(types_mapper=pd.ArrowDtype)

rows = [(1, dt.date(2026, 8, 1), 100, Decimal("10.50"), "op-1", "2026-08")]   # forma de cursor.fetchall()
print(to_dataframe(rows, query, arrow_schema(Operacao)).dtypes)
```

```text
id_operacao                int64[pyarrow]
data_ref             date32[day][pyarrow]
id_cliente                 int64[pyarrow]
valor          decimal128(18, 2)[pyarrow]
descricao                 string[pyarrow]
mes                       string[pyarrow]
```

`from_pylist` espera dicionários: com as tuplas diretamente, `pa.Table.from_pylist(linhas,
schema=esquema)` devolveu nesta sessão uma tabela só de nulos, sem erro, e a conversão por nome é
obrigatória.

### INSERT

```sql
INSERT INTO operacoes (id_operacao, data_ref, id_cliente, valor, descricao) VALUES
    (1, '2026-08-01', 100, 10.50, 'op-1'),
    (2, '2026-08-01', 101, 20.00, DEFAULT);
INSERT INTO operacoes SELECT * FROM staging_operacoes;
```

O `VALUES` de várias linhas é o "multi-row insert" que a documentação recomenda quando o `COPY` não
serve; cada lista precisa do mesmo número de valores, subconsultas não são aceitas em várias linhas,
e um `DECIMAL` com escala maior é arredondado. `INSERT INTO ... SELECT` e `CREATE TABLE AS` são as
formas rápidas quando os dados já estão no banco. Um `INSERT` sem lista de colunas segue a ordem do
`CREATE TABLE`; com menos valores que colunas, as primeiras `n` colunas recebem os valores. Colunas
`IDENTITY` recebem `DEFAULT` ou um valor explícito quando são `GENERATED BY DEFAULT`.

O mesmo comando a partir do modelo, um `INSERT` por lote:

```python
from sqlalchemy import insert

# Um único INSERT de várias linhas a partir do modelo.
batch = [
    {"id_operacao": 1, "data_ref": dt.date(2026, 8, 1), "id_cliente": 100, "valor": Decimal("10.50"),
     "descricao": "op-1", "mes": "2026-08"},
    {"id_operacao": 2, "data_ref": dt.date(2026, 8, 1), "id_cliente": 101, "valor": Decimal("20.00"),
     "descricao": None, "mes": "2026-08"},
]
command = insert(Operacao).values(batch)
print(command.compile(dialect=dialect))   # com parâmetros
print(sql(command))                        # com os valores embutidos
```

```sql
INSERT INTO operacoes (id_operacao, data_ref, id_cliente, valor, descricao, mes) VALUES (%s, %s, %s, %s, %s, %s), (%s, %s, %s, %s, %s, %s)
INSERT INTO operacoes (id_operacao, data_ref, id_cliente, valor, descricao, mes) VALUES (1, '2026-08-01', 100, 10.50, 'op-1', '2026-08'), (2, '2026-08-01', 101, 20.00, NULL, '2026-08')
```

`None` vira `NULL`, e o `Decimal` embutido sai com a escala do valor. O tamanho do lote respeita os
16 MB por comando; a seção sobre a ingestão de um DataFrame, adiante, mostra o laço por lotes.

### UPDATE, DELETE e MERGE

```sql
UPDATE operacoes SET descricao = s.descricao
    FROM staging_correcoes s
    WHERE operacoes.id_operacao = s.id_operacao AND operacoes.data_ref = s.data_ref;

DELETE FROM operacoes USING staging_cancelamentos s
    WHERE operacoes.id_operacao = s.id_operacao;

MERGE INTO operacoes USING staging_operacoes s
    ON operacoes.id_operacao = s.id_operacao AND operacoes.data_ref = s.data_ref
    WHEN MATCHED THEN UPDATE SET valor = s.valor, descricao = s.descricao
    WHEN NOT MATCHED THEN INSERT VALUES (s.id_operacao, s.data_ref, s.id_cliente, s.valor, s.descricao);

MERGE INTO operacoes USING staging_operacoes s
    ON operacoes.id_operacao = s.id_operacao AND operacoes.data_ref = s.data_ref
    REMOVE DUPLICATES;
```

Regras:

- `UPDATE ... FROM` aceita só equijoins; outer joins voltam `Target table must be part of an equijoin
  predicate`. Com `error_on_nondeterministic_update = true`, várias correspondências por linha são
  erro.
- `DELETE` sem `WHERE` apaga tudo; `TRUNCATE` é mais rápido, dispensa `VACUUM` e faz commit.
- `MERGE` exige que cada linha alvo case com no máximo uma linha da fonte (`Found multiple matches to
  update the same tuple`), não aceita `WITH`, não aceita a mesma tabela nos dois lados e não navega
  dentro de `SUPER`. `REMOVE DUPLICATES` é o modo simplificado, mais rápido, para fonte e alvo com as
  mesmas colunas na mesma ordem. A fonte pode ser view, subconsulta ou tabela Spectrum. Definir as
  colunas do join como chave de distribuição nas duas tabelas acelera o comando.
- O padrão de merge por tabela de staging da documentação: `CREATE TEMP TABLE staging (LIKE alvo)`,
  `COPY` na staging, `DELETE alvo USING staging` e `INSERT INTO alvo SELECT * FROM staging`, numa
  transação. Esse é o método que o `awswrangler` usa no modo `upsert`.
- Depois de `INSERT`, `UPDATE` ou `DELETE` de muitas linhas: `VACUUM` e `ANALYZE`, ou esperar as
  rotinas automáticas.

Os três comandos a partir do `Table` do modelo. `UPDATE ... FROM` e `DELETE ... USING` saem do Core,
que move a segunda tabela do `WHERE` para essas cláusulas; o `MERGE` não tem construto e sai de um
texto montado com as colunas do `Table`:

```python
import sqlalchemy as sa

# UPDATE ... FROM e DELETE ... USING pelo Core; MERGE por texto montado com as colunas do Table.
target = Operacao.__table__
staging = sa.Table("staging_operacoes", sa.MetaData(), *[sa.Column(c.name, c.type) for c in target.columns])
keys = ["id_operacao", "data_ref"]
join_condition = sa.and_(*[target.c[key] == staging.c[key] for key in keys])
print(sql(sa.update(target).values(descricao=staging.c.descricao).where(join_condition)))
print(sql(sa.delete(target).where(join_condition)))

def merge_sql(target: sa.Table, source: sa.Table, keys: list[str]) -> str:
    columns = list(target.columns.keys())
    condition = " AND ".join(f"{target.name}.{c} = s.{c}" for c in keys)
    set_clause = ", ".join(f"{c} = s.{c}" for c in columns if c not in keys)
    return (f"MERGE INTO {target.name} USING {source.name} s ON {condition}\n"
            f"    WHEN MATCHED THEN UPDATE SET {set_clause}\n"
            f"    WHEN NOT MATCHED THEN INSERT VALUES ({', '.join('s.' + c for c in columns)})")

print(merge_sql(target, staging, keys))
```

```sql
UPDATE operacoes SET descricao=staging_operacoes.descricao FROM staging_operacoes WHERE operacoes.id_operacao = staging_operacoes.id_operacao AND operacoes.data_ref = staging_operacoes.data_ref
DELETE FROM operacoes USING staging_operacoes WHERE operacoes.id_operacao = staging_operacoes.id_operacao AND operacoes.data_ref = staging_operacoes.data_ref
MERGE INTO operacoes USING staging_operacoes s ON operacoes.id_operacao = s.id_operacao AND operacoes.data_ref = s.data_ref
    WHEN MATCHED THEN UPDATE SET id_cliente = s.id_cliente, valor = s.valor, descricao = s.descricao, mes = s.mes
    WHEN NOT MATCHED THEN INSERT VALUES (s.id_operacao, s.data_ref, s.id_cliente, s.valor, s.descricao, s.mes)
```

O texto do `MERGE` roda por `conn.execute(sa.text(...))`. O `INSERT VALUES` sem lista de colunas
segue a ordem do `Table`, que é a ordem da staging criada a partir dele.

O `redshift_connector` segue o DB-API: `autocommit` desligado por padrão, `conn.commit()` fecha a
transação, `cursor.paramstyle` aceita `qmark`, `numeric`, `named`, `format` (padrão) e `pyformat`.
`cursor.executemany` executa o comando uma vez por conjunto de parâmetros, com uma ida ao servidor
por linha.

## Ingestão de dados

Ordem de preferência da documentação:

1. `COPY` de arquivos no S3, com um único comando por tabela: `COPY` paralelos sobre a mesma tabela
   são serializados e, em tabelas com chave de ordenação, exigem `VACUUM` depois.
2. `INSERT INTO ... SELECT` a partir de tabelas externas do Spectrum ou de tabelas locais.
3. `INSERT` de várias linhas por comando.
4. `INSERT` de uma linha por comando.

Regras do `COPY` que valem para o pipeline:

- Arquivos CSV sem compressão e arquivos Parquet e ORC são divididos automaticamente a partir de
  128 MB; arquivos menores não são divididos. CSV e JSON comprimidos com gzip, lzop ou bzip2 não
  são divididos: a recomendação é de 1 MB a 1 GB por arquivo, em quantidade múltipla do número de
  slices.
- `MANIFEST` carrega exatamente os arquivos listados; um manifesto de Parquet exige
  `meta.content_length` por entrada.
- `STATUPDATE ON` força o `ANALYZE` depois da carga. `NOLOAD`, que valida sem carregar, e
  `COMPUPDATE ON`, que escolhe a codificação numa tabela vazia a partir de uma amostra de 100.000
  linhas por slice, valem para os formatos de texto e ficam fora do `COPY` de Parquet, que também
  não aplica compressão automática.
- `pg_last_copy_count()` devolve as linhas carregadas; `sys_load_error_detail` guarda os erros.
- Cargas em ordem da chave de ordenação, ao fim da tabela, ficam ordenadas sem `VACUUM`, quando o
  `COPY` não é grande o bastante para disparar certas otimizações de carga.

O fluxo do projeto substitui o `INSERT` grande da biblioteca atual:

1. Converter o DataFrame numa tabela Arrow com o esquema do modelo (cast seguro).
2. Gravar Parquet em `<caminho S3 do projeto>/staging/<execution_id>/<tabela>/`, fora das pastas das
   tabelas Delta, com o pyarrow.
3. `COPY execucao_<id>.<tabela> FROM '<manifesto>' IAM_ROLE '<arn>' FORMAT AS PARQUET MANIFEST`, ou
   com `IAM_ROLE 'SESSION'` numa conexão federada por IAM enquanto o namespace não tiver papel
   associado; `SESSION` usa as permissões da identidade da sessão no S3 e não combina com outro
   método.
4. Comparar `pg_last_copy_count()` com o número de linhas enviadas e então `commit`.

```python
import json
import boto3, pyarrow as pa, pyarrow.parquet as pq, redshift_connector

def load(conn: redshift_connector.Connection, df, schema: pa.Schema, table: str, s3_prefix: str, role: str) -> int:
    data = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    s3 = boto3.client("s3")
    bucket, prefix = s3_prefix.removeprefix("s3://").split("/", 1)
    buf = pa.BufferOutputStream()
    pq.write_table(data, buf, compression="snappy")
    body = buf.getvalue().to_pybytes()
    file_key = f"{prefix}/{table}/parte-0.parquet"
    s3.put_object(Bucket=bucket, Key=file_key, Body=body)
    manifest = {"entries": [{"url": f"s3://{bucket}/{file_key}", "mandatory": True,
                              "meta": {"content_length": len(body)}}]}
    s3.put_object(Bucket=bucket, Key=f"{prefix}/{table}/manifest", Body=json.dumps(manifest).encode())
    with conn.cursor() as cur:
        cur.execute(f"COPY {table} FROM 's3://{bucket}/{prefix}/{table}/manifest' "
                    f"IAM_ROLE '{role}' FORMAT AS PARQUET MANIFEST")
        cur.execute("SELECT pg_last_copy_count()")
        loaded = cur.fetchone()[0]
    if loaded != data.num_rows:
        conn.rollback()
        raise RuntimeError(f"COPY carregou {loaded} de {data.num_rows} linhas")
    conn.commit()
    return loaded
```

Um arquivo por lote basta abaixo de 128 MB; acima disso, vários arquivos de tamanho parecido
aproveitam as slices. A biblioteca apaga o staging da execução ao terminar, e a rotina de limpeza
remove o staging de execuções que falharam. Uma regra de ciclo de vida no bucket do domínio
dependeria do administrador.

Os métodos do `redshift_connector` para DataFrames não substituem esse fluxo:

| Método | O que faz | Custo |
| --- | --- | --- |
| `cursor.write_dataframe(df, table)` | `INSERT INTO tabela VALUES (%s, ...)` via `executemany`, sem lista de colunas. | Uma ida ao servidor por linha. |
| `cursor.insert_data_bulk(filename, table_name, parameter_indices, column_names, delimiter, batch_size)` | Lê um CSV local e emite `INSERT ... VALUES` com `batch_size` linhas por comando. | Uma ida por lote; o padrão de `batch_size` é 1. |
| `cursor.executemany(sql, parametros)` | Laço de `execute`. | Uma ida por linha. |

O `awswrangler.redshift.copy` implementa o fluxo Parquet mais `COPY` (`max_rows_by_file` de
10.000.000, `mode` `append`, `overwrite` ou `upsert` com tabela temporária, `use_column_names` para
gerar a lista de colunas, criação da tabela a partir do esquema Arrow com `diststyle`, `distkey`,
`sortstyle`, `sortkey` e `primary_keys`); `awswrangler.redshift.to_sql` gera `INSERT` de várias
linhas com `chunksize` de 200 e é indicado pela própria documentação só abaixo de 1.000 linhas. O
`awswrangler` monta `COPY tabela (colunas) FROM ... FORMAT AS PARQUET` quando `use_column_names` é
verdadeiro, o que sugere que o `COPY` de Parquet aceita lista de colunas; a documentação do `COPY`
descreve a lista de colunas apenas para arquivos planos, e o item continua pendente da prova de
conceito. O driver ADBC para Redshift (versão 1.7.0, 2026-09-09) faz ingestão em bloco e leitura em
Arrow e merece um benchmark contra o fluxo acima.

### Regras do COPY para Parquet

| Regra da documentação | Consequência para a biblioteca |
| --- | --- |
| Colunas são associadas por posição, e a quantidade precisa coincidir com a tabela. | A ordem das colunas no Parquet é a ordem do modelo. Os dois derivam do mesmo `Table`. |
| Só existem as colunas gravadas no arquivo. | A coluna de partição `mes` não está nos arquivos do Delta: a carga passa por uma staging sem `mes` e por `INSERT ... SELECT ..., '<mes>'` ([delta.md](delta.md)). |
| Parâmetros aceitos: `ACCEPTINVCHARS`, `FILLRECORD`, `FROM`, `IAM_ROLE`, `STATUPDATE`, `MANIFEST`, `EXPLICIT_IDS`. `MAXERROR`, `NOLOAD` e `COMPUPDATE` não são aceitos, e não há compressão automática. | O primeiro erro aborta o `COPY`. A validação acontece antes, no Arrow. |
| `MANIFEST` é aceito. | O `COPY` carrega exatamente os arquivos gravados pela biblioteca, e não o que mais estiver na pasta: o Delta guarda as versões anteriores até o `vacuum`. Exercitado no datashare em 2026-09-21. |
| O bucket precisa estar na mesma região do Redshift. | Configuração da infraestrutura. |
| O `COPY` de Parquet usa URLs pré-assinadas válidas por 1 hora. | Políticas IAM do bucket não podem bloquear URLs pré-assinadas. |
| O `COPY` grava `NULL` em coluna `NOT NULL` só se o arquivo trouxer `NULL`; a falha aborta a carga. | `NOT NULL` do modelo é a última barreira; a auditoria no Arrow vem antes. |

Com o Delta Lake como fonte da verdade ([delta.md](delta.md)), os arquivos de dados não trazem a
coluna de partição `mes`, e o `COPY` lê só o conteúdo dos arquivos, por posição. A carga de um mês
passa por uma staging temporária sem `mes`, criada a partir do mesmo `Table`, e o
`INSERT ... SELECT` acrescenta o literal do mês:

```python
import pyarrow as pa, pyarrow.compute as pc
import sqlalchemy as sa
from deltalake import DeltaTable
from sqlalchemy.schema import CreateTable

# Manifesto do mês a partir das ações add do Delta e a transação que substitui o mês na publicação.
def month_manifest(dt: DeltaTable, month: str) -> tuple[dict, int]:
    actions = pa.table(dt.get_add_actions(flatten=True)).filter(pc.field("partition.mes") == month)
    root = dt.table_uri.rstrip("/")
    entries = [{"url": f"{root}/{path}", "mandatory": True, "meta": {"content_length": size}}
                for path, size in zip(actions["path"].to_pylist(), actions["size_bytes"].to_pylist())]
    return {"entries": entries}, pc.sum(actions["num_records"]).as_py()

destination = Operacao.__table__.to_metadata(sa.MetaData(), name="prod_operacoes")
staging = sa.Table("prod_operacoes_staging", destination.metadata,
                   *[sa.Column(c.name, c.type, nullable=c.nullable) for c in destination.columns if c.name != "mes"],
                   prefixes=["TEMPORARY"])
transaction = [
    sa.delete(destination).where(destination.c.mes == "2026-08"),
    CreateTable(staging),
    sa.text("COPY prod_operacoes_staging FROM 's3://bucket/publicacao/exec-42/operacoes/2026-08.manifest' "
            "IAM_ROLE 'arn:aws:iam::123456789012:role/papel' FORMAT AS PARQUET MANIFEST"),
    sa.insert(destination).from_select(list(destination.columns.keys()),
                                   sa.select(*staging.c, sa.literal("2026-08", sa.String(7)).label("mes"))),
]
```

Numa tabela Delta local com dois meses, `month_manifest` devolveu a única entrada de `2026-08`,
com `content_length` igual ao `size_bytes` da ação `add`, e o total de `num_records` do mês, que é o
valor a comparar com `pg_last_copy_count()` antes do `commit`. Os quatro comandos vão numa transação
(`engine.begin()`) e compilam para:

```sql
DELETE FROM prod_operacoes WHERE prod_operacoes.mes = '2026-08'
CREATE TEMPORARY TABLE prod_operacoes_staging (id_operacao BIGINT NOT NULL, data_ref DATE NOT NULL,
    id_cliente BIGINT NOT NULL, valor NUMERIC(18, 2) NOT NULL, descricao VARCHAR(200))
COPY prod_operacoes_staging FROM 's3://bucket/publicacao/exec-42/operacoes/2026-08.manifest'
    IAM_ROLE 'arn:aws:iam::123456789012:role/papel' FORMAT AS PARQUET MANIFEST
INSERT INTO prod_operacoes (id_operacao, data_ref, id_cliente, valor, descricao, mes)
    SELECT prod_operacoes_staging.id_operacao, prod_operacoes_staging.data_ref, prod_operacoes_staging.id_cliente,
        prod_operacoes_staging.valor, prod_operacoes_staging.descricao, '2026-08' AS mes FROM prod_operacoes_staging
```

A staging temporária dispensa a codificação e as restrições do modelo: tabelas temporárias recebem
`RAW`, e a auditoria acontece no destino. Se a lista de colunas no `COPY` de Parquet funcionar
(pendente), a staging some.

## Exportação para Parquet

```sql
UNLOAD ('SELECT id_operacao, data_ref, id_cliente, valor, descricao
         FROM operacoes
         WHERE data_ref >= ''2026-08-01'' AND data_ref < ''2026-09-01''
         ORDER BY data_ref, id_operacao')
TO 's3://bucket/operacoes/data/2026-08/abc123_'
IAM_ROLE 'arn:aws:iam::123456789012:role/papel'
FORMAT AS PARQUET
MAXFILESIZE 256 MB
MANIFEST VERBOSE;
```

Comportamento do `UNLOAD ... FORMAT AS PARQUET` segundo a documentação:

- Grava Parquet 1.0 e comprime cada row group com SNAPPY, sem compressão no nível do arquivo. O row
  group tem 32 MB por padrão; `ROWGROUPSIZE` aceita de 32 MB a 128 MB apenas nos nós ra3.4xlarge,
  ra3.16xlarge, rg.4xlarge, rg.12xlarge e dc2.8xlarge.
- `MAXFILESIZE` aceita de 5 MB a 6,2 GB (padrão 6,2 GB), arredondado para baixo até um múltiplo de
  32 MB: `MAXFILESIZE 200 MB` produz arquivos de cerca de 192 MB.
- Com `PARALLEL` (padrão), cada slice grava um ou mais arquivos; `PARALLEL OFF` grava em série,
  respeitando `ORDER BY`, em arquivos de até 6,2 GB.
- Com `PARTITION BY`, as colunas de partição saem dos arquivos, exceto com `INCLUDE`, e as pastas
  seguem a convenção Hive.
- `CLEANPATH` apaga de forma permanente os arquivos das pastas que recebem dados novos;
  `ALLOWOVERWRITE` sobrescreve arquivos existentes; sem os dois, o comando falha se o destino tiver
  arquivos.
- `MANIFEST VERBOSE` lista os arquivos, os nomes e tipos das colunas, as linhas por arquivo e o
  total; o manifesto simples lista só as URLs e serve ao `COPY ... MANIFEST`.
- `PARQUET` não combina com `DELIMITER`, `FIXEDWIDTH`, `ADDQUOTES`, `ESCAPE`, `NULL AS`, `HEADER`,
  `GZIP`, `BZIP2` nem `ZSTD`; `ENCRYPTED` só com SSE-KMS.
- Colunas `TIMESTAMPTZ` perdem a informação de fuso; `VARBYTE`, `GEOMETRY` e `HLLSKETCH` só saem em
  texto ou CSV.
- O `SELECT` externo não aceita `LIMIT`; aspas dentro da consulta são escapadas como `''`.
- O Parquet é até 2 vezes mais rápido de descarregar e ocupa até 6 vezes menos espaço no S3 que
  texto.

O `UNLOAD` do projeto grava um mês por comando, com `PARTITION BY (mes)` na pasta da tabela Delta,
`MANIFEST VERBOSE` e `MAXFILESIZE` igual ao tamanho alvo da tabela. O `SELECT` lista as colunas na
ordem do modelo, com casts para os tipos do contrato e `ORDER BY` pela chave de ordenação. A
biblioteca confere o manifesto do `UNLOAD` e o rodapé de cada arquivo antes de registrá-los no log do
Delta, e relê a versão depois (seção "O manifesto entre o log do Delta e o Redshift"). `CLEANPATH`
não é usado: arquivos de execuções abortadas ficam fora do log e saem pelo `vacuum`.

A documentação do `UNLOAD` não informa os tipos físicos Parquet, a obrigatoriedade das colunas nem a
presença de estatísticas, e os três afetam o registro dos arquivos no log do Delta.
[`../examples/redshift_manifest.py`](../examples/redshift_manifest.py) leu o rodapé de um arquivo no
ambiente alvo em 2026-09-21 e respondeu os três:

| Coluna do `UNLOAD` | Tipo físico Parquet | Tipo lógico |
| --- | --- | --- |
| `BIGINT` | `INT64` | `Int(bitWidth=64, isSigned=true)` |
| `DATE` | `INT32` | `Date` |
| `VARCHAR` | `BYTE_ARRAY` | `String` |
| `DOUBLE PRECISION` | `DOUBLE` | nenhum |
| `DECIMAL(18, 2)` | `FIXED_LEN_BYTE_ARRAY(8)` | `Decimal(precision=18, scale=2)` |
| `TIMESTAMP` | `INT96` | nenhum |

Toda coluna sai `optional`, inclusive as declaradas `NOT NULL` na tabela de origem: a nulidade do
Delta vem do esquema da tabela, não dos arquivos. As estatísticas de mínimo e máximo estão
presentes, o que faz valer preencher `minValues` e `maxValues` na `AddAction`.

Os dois tipos físicos que divergem do resto do projeto:

- `DECIMAL(18, 2)` sai em `FIXED_LEN_BYTE_ARRAY(8)`, como o PyArrow grava, e não em `INT64`, como
  gravam o delta-rs, o DuckLake e o DuckDB ([estrategia.md](estrategia.md)). Uma tabela Delta que
  recebe arquivos dos dois escritores fica com duas codificações físicas da mesma coluna lógica;
  os leitores leem as duas, porque o tipo lógico é o mesmo.
- `TIMESTAMP` sai em `INT96`, que o formato Parquet marca como obsoleto e que o contrato do projeto
  tira na carga inicial ([schema.md](schema.md)). O `UNLOAD` o traz de volta, e o `INT96` não carrega
  estatística de mínimo e máximo: a coluna de timestamp de um arquivo do `UNLOAD` não poda. Um
  arquivo `INT96` registrado numa tabela Delta declarada `timestamp_ntz` foi lido de volta pelo
  delta-rs e pelo `delta_scan` do DuckDB como `timestamp[us]`, com os valores intactos
  (2026-09-21, macOS, [POC.md](POC.md)).

O `UNLOAD` fragmenta por slice: 500.000 linhas em seis colunas saíram em 32 arquivos, sem
`MAXFILESIZE`, que é um teto e não um piso. Quem controla a quantidade é `PARALLEL OFF`, que grava
em série num arquivo só e respeita o `ORDER BY`, ou uma compactação posterior na camada Delta.

O `UNLOAD` lê tabelas do sandbox no banco local, fora das regras de escrita por datashare. Sem
acesso do Redshift ao S3, a exportação lê o mês em Arrow pelo driver ADBC e grava o Parquet com o
pyarrow e o papel do projeto. O `awswrangler.redshift.unload` executa o `UNLOAD` e lê os arquivos de
volta num DataFrame; `unload_to_files` só descarrega.

Com o Delta como fonte da verdade ([delta.md](delta.md)), o `UNLOAD` grava com `PARTITION BY (mes)`
na pasta da tabela, e a biblioteca registra os arquivos no log do Delta depois de conferir o
manifesto verboso. Uma amostra do manifesto, reduzida aos campos que a biblioteca lê, no leiaute que
a documentação descreve (URL, `content_length` e `record_count` por entrada, `schema.elements` com
nome e tipo, total em `meta`):

```json
{
  "entries": [
    {"url": "s3://bucket/prod/operacoes/mes=2026-08/0000_part_00.parquet",
     "meta": {"content_length": 33554432, "record_count": 180000}},
    {"url": "s3://bucket/prod/operacoes/mes=2026-08/0001_part_00.parquet",
     "meta": {"content_length": 22369621, "record_count": 120000}}
  ],
  "schema": {"elements": [
    {"name": "id_operacao", "type": {"base": "bigint"}},
    {"name": "data_ref", "type": {"base": "date"}},
    {"name": "id_cliente", "type": {"base": "bigint"}},
    {"name": "valor", "type": {"base": "numeric", "precision": 18, "scale": 2}},
    {"name": "descricao", "type": {"base": "character varying", "byte_length": 200}}
  ]},
  "meta": {"content_length": 55924053, "record_count": 300000}
}
```

O comando sai do mesmo `select` do contrato, com as aspas internas duplicadas, e a conferência lê a
amostra acima como `amostra`:

```python
import json
import sqlalchemy as sa

# UNLOAD do mês montado do select do contrato; conferência do manifesto verboso antes de registrar os arquivos.
query = (sa.select(Operacao.__table__).where(Operacao.mes == "2026-08")
            .order_by(Operacao.data_ref, Operacao.id_operacao))
inner_sql = sql(query).replace("'", "''")
unload = (f"UNLOAD ('{inner_sql}')\n"
          "TO 's3://bucket/prod/operacoes/' IAM_ROLE 'arn:aws:iam::123456789012:role/papel'\n"
          "FORMAT AS PARQUET PARTITION BY (mes) MANIFEST VERBOSE")

def check_manifest(manifest: dict, expected_columns: list[str]) -> int:
    columns = [element["name"] for element in manifest["schema"]["elements"]]
    if columns != expected_columns:
        raise ValueError(f"colunas do UNLOAD {columns} diferem do modelo {expected_columns}")
    per_file = sum(entry["meta"]["record_count"] for entry in manifest["entries"])
    if per_file != manifest["meta"]["record_count"]:
        raise ValueError("soma das linhas por arquivo difere do total do manifesto")
    return per_file

without_partition = [c.name for c in Operacao.__table__.columns if c.name != "mes"]
print(check_manifest(json.loads(sample), without_partition))     # 300000
```

`unload` vale:

```sql
UNLOAD ('SELECT operacoes.id_operacao, operacoes.data_ref, operacoes.id_cliente, operacoes.valor, operacoes.descricao, operacoes.mes
FROM operacoes
WHERE operacoes.mes = ''2026-08'' ORDER BY operacoes.data_ref, operacoes.id_operacao')
TO 's3://bucket/prod/operacoes/' IAM_ROLE 'arn:aws:iam::123456789012:role/papel'
FORMAT AS PARQUET PARTITION BY (mes) MANIFEST VERBOSE
```

Os nomes de tipo da amostra e a presença da coluna de partição em `schema` sob `PARTITION BY` não
foram verificados; por isso a lista esperada é um parâmetro. `UnloadFromSelect` do dialeto não tem
`PARTITION BY` nem `VERBOSE`, e o comando fica em texto.

## O manifesto entre o log do Delta e o Redshift

O formato do manifesto é da AWS, descrito na referência do `COPY` e na do `UNLOAD`. O Delta não
participa dele nos dois sentidos: o que o log guarda são ações `add`, e o manifesto que o Delta tem,
o `GENERATE symlink_format_manifest`, é outro formato, de texto, que serve ao Spectrum e não ao
`COPY`, e que o delta-rs não implementa ([delta.md](delta.md)). As duas conversões abaixo montam e
leem o formato da AWS a partir do log e para o log;
[`../examples/redshift_manifest.py`](../examples/redshift_manifest.py) executou as duas no ambiente
alvo em 2026-09-21, numa tabela do banco de datashare.

O manifesto é um objeto com `entries`, uma entrada por arquivo:

| Campo | Quando é exigido |
| --- | --- |
| `url` | Sempre; a URL `s3://` completa do arquivo. |
| `mandatory` | Opcional, falso por omissão, e um arquivo ausente é pulado em silêncio. |
| `meta.content_length` | Nos formatos colunares, Parquet e ORC. |
| `meta.record_count` | Só no manifesto que o `UNLOAD ... MANIFEST VERBOSE` grava. |

### Do log do Delta para o manifesto do COPY

A biblioteca lê as ações `add` da versão (`get_add_actions()`, uma linha por arquivo vivo) e grava o
manifesto. É o `copy_manifest` da [etapa 3](PLAN-STAGE-3.md), usado pela ingestão da
[etapa 5](PLAN-STAGE-5.md) e pela publicação da [etapa 8](PLAN-STAGE-8.md).

| Ação `add` | Entrada do manifesto |
| --- | --- |
| `path`, relativo à pasta da tabela | `url`, a URI da tabela mais o caminho |
| `size_bytes` | `meta.content_length` |
| nenhuma | `mandatory: true`, porque o log afirma que o arquivo existe: sumiu um, o `COPY` falha em vez de carregar de menos |
| `num_records` | fica fora; a soma é o valor a comparar com `pg_last_copy_count()` antes do `commit` |
| `partition.<coluna>` | fica fora, e é a razão da staging: o valor da partição não está no arquivo, o `COPY` é posicional, e o `INSERT ... SELECT *, '<valor>'` acrescenta a coluna |
| `min`, `max`, `null_count` | ficam fora; o `COPY` não lê estatística |

Escolher os arquivos é filtrar as ações `add` por `partition.<coluna>` antes de montar as entradas,
o que é a diferença entre publicar uma partição e publicar a tabela.

### Do manifesto do UNLOAD para o log do Delta

O caminho de volta não produz manifesto nenhum: ele grava um commit. `create_write_transaction`
escreve uma versão nova do log com uma ação `add` por entrada, e os arquivos que o `UNLOAD` gravou
ficam onde estão, sem cópia nem reescrita. É o `register_files` da [etapa 3](PLAN-STAGE-3.md),
usado pelo `export_partition` da [etapa 5](PLAN-STAGE-5.md).

| Entrada do manifesto | `AddAction` |
| --- | --- |
| `url`, absoluta | `path`, **relativo à pasta da tabela** |
| `meta.content_length` | `size` |
| `meta.record_count` | `stats.numRecords` |
| nenhuma | `partition_values`, lido do caminho: o `PARTITION BY` grava `<coluna>=<valor>/` na convenção Hive |
| nenhuma | `modification_time`, do `LastModified` do objeto no S3 |
| nenhuma | `data_change`, conceito do Delta sem contraparte |
| nenhuma | `stats.minValues`, `maxValues` e `nullCount`, lidos do rodapé Parquet |

Tirar o prefixo da URL é a conversão que sustenta o resto: o log do Delta guarda caminhos relativos
à pasta da tabela, e nenhum caminho absoluto ([delta.md](delta.md)), que é o que permite mover a
pasta. Um caminho absoluto registrado ali quebra a tabela no primeiro `mv`.

O manifesto não tem estatística, e os rodapés do `UNLOAD` têm: preencher `minValues` e `maxValues`
custa uma leitura de rodapé por arquivo e é o que faz o `delta_scan` podar. Sem elas a poda é só por
partição. A coluna de timestamp é a exceção, porque o `INT96` do `UNLOAD` não carrega estatística.

O `schema.elements` do manifesto verboso traz o nome e o tipo de cada coluna, e é a primeira
conferência antes do commit: um `cast` errado no `select` do `UNLOAD` aparece ali, não na primeira
leitura da tabela meses depois. A presença da coluna de partição nesse bloco sob `PARTITION BY` não
foi verificada. A conferência a que o leitor obedece é a do rodapé, abaixo.

### As conferências antes do commit e a releitura depois

O `create_write_transaction` grava a ação como a recebe, e os leitores obedecem à ação, não ao
arquivo (sondagem de 2026-09-21, [POC.md](POC.md)): o caminho inexistente commita e derruba a leitura
da partição; a estatística falsa poda o arquivo certo no delta-rs, no `delta_scan` e no DataFusion,
sem erro; a coluna `NOT NULL` ausente do arquivo lê nulo; o tipo que não converte falha só quando a
coluna é lida. O `write_deltalake` recusa cada um desses casos, e é o que o registro perde. A
biblioteca repõe a conferência antes do commit, só com o rodapé de cada arquivo
([PLAN-STAGE-3.md](PLAN-STAGE-3.md), seção "As conferências do registro de arquivos"): o arquivo
existe no caminho que o leitor resolve, com o tamanho da entrada; o esquema do rodapé bate com o da
tabela nome a nome, com o `INT96` e o `FIXED_LEN_BYTE_ARRAY` entre os tipos físicos admitidos; o
valor de partição do caminho é o pedido; a soma de linhas dos rodapés é a do manifesto e a do
`count(*)` da fonte; mínimo e máximo só das colunas cuja transcrição tem teste, omitidos nas demais.
Depois do commit a versão é relida pelo delta-rs e pelo `delta_scan`, e uma diferença volta por
`restore`; o snapshot e a publicação no Redshift esperam a releitura.

A alternativa sem registro é reler os arquivos do `UNLOAD` e gravar por `write_deltalake`, que faz
essas conferências sozinho e normaliza os tipos físicos, ao custo de passar os dados pela máquina
local: é o `export_mode="rewrite"` da [etapa 5](PLAN-STAGE-5.md), ao lado do registro
(`"register"`), e a mesma partição sai igual pelos dois.

## Recomendações de performance

### Ingestão

- Um `COPY` por tabela, com arquivos divisíveis (Parquet a partir de 128 MB) ou vários arquivos de 1
  MB a 1 GB em quantidade múltipla das slices.
- Compressão nos arquivos de entrada para reduzir o tempo de envio ao S3; o Parquet já vem
  comprimido por row group.
- Carga em ordem da chave de ordenação e ao fim da tabela, para dispensar o `VACUUM` nas cargas que
  não disparam as otimizações de carga; tabelas por período com view `UNION ALL` para descartar
  meses antigos por `DROP TABLE`.
- Merge por tabela de staging: `DELETE ... USING` seguido de `INSERT ... SELECT` quando todas as
  colunas mudam, `UPDATE` e `INSERT` separados quando poucas linhas da staging participam.
- `VACUUM` e `ANALYZE` manuais depois de cargas grandes, ou confiar nas rotinas automáticas
  (`vacuum_sort_benefit` e `stats_off` em `svv_table_info` dizem quando vale a pena). `VACUUM` pula
  a ordenação quando mais de 95 % da tabela já está ordenada.
- `ANALYZE COMPRESSION` numa amostra real antes de fixar codificações (o `COMPUPDATE` do `COPY` não
  vale para Parquet); `RAW` nas colunas de chave de ordenação, para que a poda por blocos não fique
  mais lenta que a leitura das demais colunas.
- Evitar `DECIMAL` acima de 19 dígitos e `VARCHAR` maiores que o necessário: os de 128 bits ocupam o
  dobro em disco e tornam as consultas mais lentas, e a referência do `CREATE TABLE` alerta para o
  limite de largura de linha nos resultados intermediários das cargas e das consultas.
- Manutenção fora do horário de carga: `VACUUM` e `ALTER TABLE` de chaves não rodam juntos.

### Organização das tabelas para filtros por chave e joins

**Estilos de distribuição.** `AUTO` (padrão) começa com `ALL` para tabelas pequenas, passa a `KEY`
pela chave primária quando a tabela cresce e a `EVEN` quando nenhuma coluna serve; a documentação
recomenda `AUTO`. `EVEN` distribui em rodízio e serve a tabelas que não participam de joins. `KEY`
coloca as linhas com o mesmo valor da coluna `DISTKEY` na mesma slice, o que colocaliza joins entre
tabelas distribuídas pela mesma coluna. `ALL` copia a tabela inteira para todos os nós, multiplica o
armazenamento, encarece cargas e serve a tabelas de dimensão pouco alteradas que não podem ser
colocalizadas; para tabelas pequenas o ganho é insignificante, porque redistribuí-las numa consulta
custa pouco.

**Escolha da chave de distribuição.** Distribuir a tabela fato e a maior dimensão pela coluna do
join entre elas (`DISTKEY` na chave primária da dimensão e na chave estrangeira do fato); só um join
por tabela fato fica colocalizado. A coluna precisa de cardinalidade alta no conjunto filtrado: uma
tabela de vendas distribuída por data concentra um filtro de um mês em poucas slices. Para a tabela
`operacoes`, `id_cliente` colocaliza o join com a tabela de clientes e os agrupamentos por cliente;
`data_ref` seria uma chave ruim.

**Chaves de ordenação.** A chave compound ordena pelas colunas na ordem declarada e serve a filtros
por prefixo, a `GROUP BY` e a merge joins; o benefício cai quando as consultas usam só as colunas
secundárias. A chave interleaved dá peso igual a cada coluna (até 8), serve a filtros por qualquer
subconjunto, custa mais na carga e no `VACUUM REINDEX`, e não deve incluir colunas monotônicas. A
documentação recomenda criar as tabelas com `SORTKEY AUTO` e, ao escolher a chave, compound para
tabelas atualizadas regularmente com `INSERT`, `UPDATE` ou `DELETE`. Com dados recentes consultados
com frequência, a coluna de tempo lidera a chave; com joins frequentes, a coluna do join como chave
de ordenação e de distribuição habilita o sort merge join sem fase de ordenação. Para `operacoes`,
`COMPOUND SORTKEY (data_ref, id_operacao)` atende aos filtros por mês e à publicação por período.

**Leitura do plano.** `EXPLAIN` mostra, em cada join, como as linhas se moveram:

| Rótulo | Significado | Avaliação |
| --- | --- | --- |
| `DS_DIST_NONE` | Slices já colocalizadas. | Bom. |
| `DS_DIST_ALL_NONE` | Tabela interna em `ALL`. | Bom. |
| `DS_DIST_INNER` | Tabela interna redistribuída. | Custo alto; distribuir a interna pela coluna do join. |
| `DS_DIST_OUTER` | Tabela externa redistribuída. | Sem avaliação na documentação. |
| `DS_BCAST_INNER` | Tabela interna transmitida a todos os nós. | Ruim; as tabelas não estão unidas pela chave de distribuição. |
| `DS_DIST_ALL_INNER` | Tabela interna inteira numa única slice, porque a externa é `ALL`. | Ruim; execução serial. |
| `DS_DIST_BOTH` | As duas redistribuídas. | Ruim. |

**Escrita das consultas.** Sem `SELECT *`; predicados sobre a chave de ordenação; o mesmo filtro
repetido nas duas tabelas de um join, mesmo que redundante, para que ambas sejam podadas; sem funções
sobre colunas nos predicados; `GROUP BY` pelas colunas da chave de ordenação, na ordem da chave, para
habilitar a agregação em uma fase; `GROUP BY` e `ORDER BY` com as colunas na mesma ordem; cross
joins evitados; subconsulta em vez de join quando a segunda tabela só filtra e devolve menos de
cerca de 200 linhas; comparação em vez de `LIKE`, e `LIKE` em vez de `SIMILAR TO`.

### Diferenças de abordagem em relação a bancos relacionais tradicionais

O ajuste de um banco relacional de linha passa por índices, transações curtas e atualizações
pontuais. No Redshift, ele passa pela distribuição e pela ordenação física, pela codificação e pelo
tamanho dos lotes. As chaves declaradas não protegem os dados, mas mudam os planos; a integridade é
responsabilidade da carga. As escritas concorrentes seguem snapshot isolation por padrão ou o nível
serializável, que aborta a segunda transação conflitante; os dois favorecem um escritor por tabela.
Os comandos têm limite de 16 MB, o que exclui `INSERT` gigantes. E o custo de `UPDATE` e `DELETE`
inclui a reordenação e a recuperação de espaço posteriores, motivo para substituir partições por
período em vez de alterá-las.

## Suporte a SQLAlchemy

O dialeto é o pacote `sqlalchemy-redshift` (versão 1.0.0, de 2026-04-27, para SQLAlchemy 2.0 e
Python 3.10 ou superior). Ele exige `redshift_connector` ou `psycopg2`:

```python
import sqlalchemy as sa

engine = sa.create_engine(
    "redshift+redshift_connector://usuario:senha@workgroup.123456789012.sa-east-1.redshift-serverless.amazonaws.com:5439/dev",
    connect_args={"ssl": True, "sslmode": "verify-full"},
)
```

O dialeto com `redshift_connector` define `sslmode = verify-full`, `ssl = True` e
`application_name = sqlalchemy-redshift` por padrão, e aceita `client_encoding` na URL. Fatos do
código do dialeto:

| Aspecto | Comportamento |
| --- | --- |
| Cache de comandos | `supports_statement_cache = False` nos dois drivers. |
| `LIMIT`/`OFFSET` | Compilados como literais no dialeto `redshift_connector`. |
| Argumentos de tabela | `redshift_diststyle`, `redshift_distkey`, `redshift_sortkey`, `redshift_interleaved_sortkey`. |
| Argumentos de coluna | `redshift_encode`, `redshift_distkey`, `redshift_sortkey`, `redshift_identity=(seed, step)`. |
| `Identity()` genérico | Ignorado no DDL: a coluna sai como `BIGINT NOT NULL`, sem `IDENTITY`. Só `redshift_identity` gera `IDENTITY(1,1)`. |
| `Sequence()` | `create_all` emitiria `CREATE SEQUENCE`, que o Redshift não suporta. |
| Tipos próprios | `TIMESTAMPTZ`, `TIMETZ`, `SUPER`, `GEOMETRY`, `HLLSKETCH`, importados de `sqlalchemy_redshift.dialect`. |
| Reflexão | Colunas, chaves, comentários e as opções de distribuição e ordenação por `inspect(engine).get_table_options(table_name)`. |
| Comandos | `CopyCommand`, `UnloadFromSelect`, `AlterTableAppendCommand` e `RefreshMaterializedView` em `sqlalchemy_redshift.commands`; `CreateMaterializedView` e `DropMaterializedView` em `sqlalchemy_redshift.ddl`. |
| `executemany` sem `RETURNING` | Com `redshift_connector`, `use_insertmanyvalues_wo_returning = False`: o SQLAlchemy chama `cursor.executemany`, que executa uma ida por linha. Com `psycopg2`, o SQLAlchemy reescreve em `INSERT ... VALUES (...), (...)` em lotes de até 1.000 linhas. |
| `postgresql.insert(...).on_conflict_do_update` | Compila, mas o Redshift não tem `ON CONFLICT`; o upsert é `MERGE` por texto. |
| `RETURNING` | O dialeto herda `insert_returning = True` do PostgreSQL: um modelo com `server_default` ou chave gerada no servidor faz o ORM emitir `INSERT ... RETURNING`, que o Redshift não tem. `__table_args__ = {"implicit_returning": False}` desliga o recurso na tabela. |

### Parâmetros específicos do Redshift no modelo

```python
import datetime as dt, decimal
from sqlalchemy import BigInteger, Numeric, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.schema import CreateTable
from sqlalchemy_redshift.dialect import RedshiftDialect_redshift_connector

class Base(DeclarativeBase):
    pass

class Operacao(Base):
    __tablename__ = "operacoes"
    __table_args__ = {
        "redshift_diststyle": "KEY",
        "redshift_distkey": "id_cliente",
        "redshift_sortkey": ["data_ref", "id_operacao"],   # ou redshift_interleaved_sortkey
    }
    id_operacao: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=False)
    data_ref: Mapped[dt.date] = mapped_column(primary_key=True)
    id_cliente: Mapped[int] = mapped_column(BigInteger)
    valor: Mapped[decimal.Decimal] = mapped_column(Numeric(18, 2))
    descricao: Mapped[str | None] = mapped_column(String(200), redshift_encode="zstd")

print(CreateTable(Operacao.__table__).compile(dialect=RedshiftDialect_redshift_connector()))
```

Saída compilada:

```sql
CREATE TABLE operacoes (
	id_operacao BIGINT NOT NULL,
	data_ref DATE NOT NULL,
	id_cliente BIGINT NOT NULL,
	valor NUMERIC(18, 2) NOT NULL,
	descricao VARCHAR(200) ENCODE zstd,
	PRIMARY KEY (id_operacao, data_ref)
) DISTSTYLE KEY DISTKEY (id_cliente) SORTKEY (data_ref, id_operacao)
```

`redshift_interleaved_sortkey=["data_ref", "id_cliente"]` gera `INTERLEAVED SORTKEY (data_ref,
id_cliente)`; `sortkey` e `interleaved_sortkey` juntos são erro, assim como `DISTSTYLE EVEN` com
`DISTKEY` ou `DISTSTYLE KEY` sem `DISTKEY`. `redshift_distkey=True` e `redshift_sortkey=True` numa
coluna geram a forma `coluna BIGINT DISTKEY SORTKEY`.

Com o dialeto instalado, o SQLAlchemy valida os argumentos `redshift_*` e rejeita os que o dialeto
não aceita (`ArgumentError`); sem o dialeto, cada argumento entra com o aviso `Can't validate
argument` e não gera DDL. Para manter os modelos neutros, o projeto guarda as opções em `Table.info`
([esquema a partir dos modelos](schema.md)) e as aplica na compilação do DDL com um gancho do
compilador, verificado com o dialeto 1.0.0:

```python
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.schema import CreateTable
from sqlalchemy_redshift.ddl import get_table_attributes

@compiles(CreateTable, "redshift")
def create_table_redshift(element, compiler, **kw):
    ddl = compiler.visit_create_table(element, **kw)
    options = element.element.info.get("serialize_db", {})
    redshift = options.get("redshift", {})
    attributes = get_table_attributes(
        compiler.preparer,
        diststyle=redshift.get("diststyle"),
        distkey=redshift.get("distkey"),
        sortkey=options.get("sort_key"),
    )
    return ddl.rstrip() + attributes + "\n"
```

Com `__table_args__ = {"info": {"serialize_db": {"sort_key": ["data_ref", "id_operacao"],
"redshift": {"diststyle": "KEY", "distkey": "id_cliente"}}}}`, a compilação para o dialeto Redshift
produziu o mesmo `DISTSTYLE KEY DISTKEY (id_cliente) SORTKEY (data_ref, id_operacao)`, e a compilação
para o DuckDB não foi afetada.

### Consulta que devolve um DataFrame

```python
import pandas as pd
from sqlalchemy import select

query = (
    select(Operacao)
    .where(Operacao.data_ref >= dt.date(2026, 8, 1), Operacao.data_ref < dt.date(2026, 9, 1))
    .order_by(Operacao.data_ref, Operacao.id_operacao)
)
df = pd.read_sql(query, engine, coerce_float=False)   # Decimal e date como objetos
```

`pd.read_sql` com `coerce_float=True` (padrão) converte `Decimal` em `float`. Um DataFrame com os
tipos do contrato sai de `pa.Table.from_pandas(df, schema=schema)` ou, sem pandas no meio, de
`session.execute(query).all()` seguido de `pa.Table.from_pylist([dict(r._mapping) for r in linhas],
schema=esquema)`. Para meses inteiros, `UnloadFromSelect` gera o `UNLOAD` a partir da mesma consulta:

```python
from sqlalchemy_redshift.commands import UnloadFromSelect, Format

unload = UnloadFromSelect(
    query, unload_location="s3://bucket/operacoes/data/2026-08/abc123_",
    iam_role_arns="arn:aws:iam::123456789012:role/papel", format=Format.parquet,
    manifest=True, max_file_size=256 * 1024 * 1024,
)
with engine.begin() as conn:
    conn.execute(unload)
```

O comando compilado, com os valores embutidos:

```sql
UNLOAD ('SELECT operacoes.id_operacao, operacoes.data_ref, operacoes.id_cliente, operacoes.valor,
operacoes.descricao FROM operacoes WHERE operacoes.data_ref >= ''2026-08-01''')
TO 's3://bucket/operacoes/data/2026-08/abc123_'
CREDENTIALS 'aws_iam_role=arn:aws:iam::123456789012:role/papel' MANIFEST FORMAT AS PARQUET MAXFILESIZE 256.0 MB
```

### Ingestão de um DataFrame numa tabela definida pelo ORM

```python
import sqlalchemy as sa
from sqlalchemy import insert
from sqlalchemy.orm import Session
from sqlalchemy_redshift.commands import CopyCommand, Format

# Volume: Parquet no S3 e COPY, gerado a partir do Table do modelo.
copy_cmd = CopyCommand(
    Operacao.__table__, data_location="s3://bucket/staging/abc123/operacoes/manifest",
    iam_role_arns="arn:aws:iam::123456789012:role/papel", format=Format.parquet, manifest=True,
)
with engine.begin() as conn:
    conn.execute(copy_cmd)
    loaded = conn.execute(sa.text("SELECT pg_last_copy_count()")).scalar()

# Volumes pequenos: um INSERT de várias linhas por lote, sem passar por executemany.
records = df.to_dict("records")
with Session(engine) as session:
    for start in range(0, len(records), 500):
        session.execute(insert(Operacao).values(records[start:start + 500]))
    session.commit()
```

`CopyCommand` compila para `COPY operacoes FROM 's3://.../manifest' WITH CREDENTIALS AS
'aws_iam_role=arn:...' FORMAT AS PARQUET MANIFEST` e exige um ARN com conta de 12 dígitos. A forma
`CREDENTIALS 'aws_iam_role=...'`, que `CopyCommand` e `UnloadFromSelect` emitem, não consta da
referência atual do `COPY` nem da do `UNLOAD`, que documentam só `IAM_ROLE`; se o servidor ainda a
aceita fica pendente da prova de conceito. O `insert(...).values(lista)` gera um único `INSERT ...
VALUES (...), (...)`, o multi-row insert da documentação, dentro do limite de 16 MB por comando;
`session.execute(insert(Operacao), lista)`, o bulk insert do ORM, cairia no `executemany` linha a
linha do `redshift_connector`.

## Referências

- Guia do desenvolvedor do Amazon Redshift: <https://docs.aws.amazon.com/redshift/latest/dg/>.
  Páginas usadas: armazenamento colunar, Redshift e PostgreSQL (recursos implementados de outra
  forma, recursos e tipos não suportados), tipos de dados (numéricos, caracteres, `SUPER`, dados
  semiestruturados e limites), `CREATE TABLE`, `CREATE TABLE AS`, `ALTER TABLE`, `DROP TABLE`,
  `COMMENT`, restrições, chaves de ordenação e distribuição, `SELECT`, `INSERT`, `UPDATE`, `DELETE`,
  `MERGE`, `TRUNCATE`, tabelas de staging, carga de dados e boas práticas, `COPY`, `UNLOAD`,
  `VACUUM`, `ANALYZE`, codificações de compressão, otimização automática de tabelas, planejamento e
  desempenho de consultas, `EXPLAIN`, isolamento e escritas concorrentes, `PG_LAST_COPY_COUNT`,
  `STL_LOAD_ERRORS` e `SYS_LOAD_ERROR_DETAIL`.
- Guia de gerenciamento do Amazon Redshift, conector Python e Data API:
  <https://docs.aws.amazon.com/redshift/latest/mgmt/>.
- Repositório do `redshift_connector`: <https://github.com/aws/amazon-redshift-python-driver>.
- Repositório do `sqlalchemy-redshift`: <https://github.com/sqlalchemy-redshift/sqlalchemy-redshift>
  e documentação em <https://sqlalchemy-redshift.readthedocs.io/en/latest/>.
- Repositório do `awswrangler` (AWS SDK for pandas): <https://github.com/aws/aws-sdk-pandas>.
- Driver ADBC para Redshift: <https://adbc-drivers.org/drivers/redshift/>.
- Documentação do SQLAlchemy 2.0: <https://docs.sqlalchemy.org/en/20/>.
- Lista completa das páginas consultadas: [REFERENCES.md](../REFERENCES.md).
