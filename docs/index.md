Os dois motores executam o pipeline sobre uma cópia dos dados, e o Redshift é também o destino de
publicação. Os modelos SQLAlchemy do cliente são o contrato de esquema: a
biblioteca deriva deles o esquema Arrow e Delta, o DDL de cada motor, a conversão dos lotes de
dados e a conferência dos próprios modelos.

Esta página explica o funcionamento geral do pacote, traz o tutorial de uso, o uso das threads, a
retenção dos arquivos removidos e a tabela de mapeamento de tipos. A referência de cada módulo, com
os argumentos, o retorno e as exceções de cada função, está no menu: `serialize_db.schema`,
`serialize_db.sql`, `serialize_db.storage`, `serialize_db.delta`, `serialize_db.audit`,
`serialize_db.engine` (com os motores `serialize_db.engine.duckdb` e
`serialize_db.engine.redshift`), `serialize_db.resources`, `serialize_db.execution`,
`serialize_db.parquet_import`, `serialize_db.publication`, `serialize_db.reader`,
`serialize_db.errors` e `serialize_db.cli`, com o runbook da operação e as opções de cada subcomando
da linha de comando. A página `serialize_db.technology_review`, a revisão das tecnologias, descreve
o funcionamento do Parquet, do Delta Lake, do DuckDB, do Redshift e do SQLAlchemy e o que a
biblioteca faz com cada um.

## Como o pacote funciona

- **O contrato é o modelo.** O cliente declara as tabelas em SQLAlchemy (`DeclarativeBase`,
  `mapped_column`), com comentários de documentação opcionais nas tabelas e colunas e as opções
  físicas em `Table.info["serialize_db"]`. O pacote não contém modelo algum; ele recebe
  `Base.metadata`.
- **A fonte da verdade são as tabelas Delta Lake**, em disco local ou no S3. O esquema Delta de cada
  tabela sai do modelo.
- **O DuckDB e o Redshift são sandboxes**: cada execução leva ao sandbox as tabelas de entrada,
  como view ou cópia do `delta_scan` no DuckDB e por `COPY` numa tabela do DDL do modelo no
  Redshift, roda o pipeline, que grava as de saída em tabelas do DDL do modelo, audita e publica
  no Delta.
  O DDL de cada motor sai do modelo pela tabela de tipos abaixo, sem os dialetos do SQLAlchemy.
- **Os dados atravessam a fronteira em lotes Arrow** (`pyarrow.RecordBatch`, `pyarrow.Table` ou
  `pyarrow.RecordBatchReader`), e `serialize_db.schema.cast` leva cada lote ao esquema do contrato
  ou o recusa com a instrução ao cliente.
- **O statement Core do pipeline roda no motor como está.** O cliente o submete às primitivas do
  motor, que o compilam pelo dialeto com os parâmetros dele; o mesmo statement roda num
  `sqlalchemy.Connection` criado fora do pacote. Como opção para quem quer sair do SQLAlchemy,
  `serialize_db.sql.render` gera o texto do DuckDB e do Redshift, com as constantes embutidas, a
  partição como parâmetro `:nome` e cada tabela do contrato com o sentinela `{prefix}` no nome,
  que a execução troca pelo prefixo do sandbox, e o pipeline versiona o texto.
- **Todo identificador que a biblioteca emite vai entre aspas duplas**: nomes de coluna como `to` e
  `timestamp` são palavras reservadas do DuckDB e do Redshift.

O pacote tem o módulo de esquema, `serialize_db.schema`, o de texto SQL,
`serialize_db.sql`, com a linha de comando `serialize-db schema` e `serialize-db sql`, a camada
de tabela, `serialize_db.storage` e `serialize_db.delta`, na pasta local e no S3, a auditoria,
`serialize_db.audit`, os dois motores, `serialize_db.engine.duckdb` e
`serialize_db.engine.redshift`, a execução, `serialize_db.execution`, com `serialize-db run` e
`serialize-db audit`, a publicação para os clientes no Redshift, `serialize_db.publication`,
com `serialize-db publish_redshift`, a carga inicial da base Parquet atual,
`serialize_db.parquet_import`, com `serialize-db import`, a operação, `serialize-db snapshot`,
`vacuum`, `compact`, `archive`, `export`, `history` e `channel`, com o runbook na página de
`serialize_db.cli`, e o acesso de leitura à base com o modelo, `serialize_db.reader`, por
`db.open_delta()` e `db.open_redshift()`.

### Origem e destino dos dados

Cada tabela Delta fica em `<raiz>/<ambiente>/<tabela>`, e o ambiente (`prd`, `dsv`) também prefixa
a tabela publicada no Redshift, `<ambiente>_<tabela>`. Cada operação lê de uma origem e grava num
destino:

| Operação | Origem | Destino |
| --- | --- | --- |
| `serialize-db import`, `serialize_db.parquet_import.import_table` | A base Parquet de `--source`, uma pasta por tabela, `<origem>/<tabela>/`, com as partições em `<coluna>=<valor>/`; a carga só a lê. | A tabela Delta, um commit por partição. |
| `run.ingest` | A versão fixada da tabela Delta. | O sandbox, com o nome do modelo: a view ou a tabela no banco DuckDB da execução, ou a tabela `exec_<id>_<tabela>` no esquema do Redshift. |
| `run.publish_delta` | As partições auditadas da tabela do sandbox. | A tabela Delta: os arquivos que o motor grava na pasta da partição, registrados num commit por partição. |
| `run.snapshot`, `serialize-db snapshot` | A versão de cada tabela do ambiente. | A entrada do snapshot em `<raiz>/<ambiente>/_serialize_db/snapshots.json`. |
| `serialize-db publish_redshift` | As versões Delta de um snapshot, pelo nome ou pelo canal; com `--channel current`, a versão atual de cada tabela. | A tabela `<ambiente>_<tabela>` no esquema do Redshift e a linha dela em `serialize_db_publications`. |
| `db.open_delta()` | As versões Delta de um snapshot, pelo canal ou pelo nome; o arquivado, pela cópia em `<raiz>/<ambiente>/arquivo/<nome>/`. | Uma view por tabela num DuckDB no processo do cliente, lida em Arrow. |
| `db.open_redshift()`, `serialize_db.reader.open_redshift` | As tabelas `<ambiente>_<tabela>` do Redshift. | O resultado em Arrow no processo do cliente. |
| `serialize-db archive` | As versões Delta de um snapshot. | Uma tabela Delta nova por tabela, em `<raiz>/<ambiente>/arquivo/<nome>/<tabela>`. |
| `serialize-db export`, `serialize_db.delta.export_parquet` | Uma versão da tabela Delta. | Arquivos Parquet sem o log, na pasta de `--destination`, sob a raiz. |

Os arquivos intermediários também ficam sob a raiz. O motor Redshift grava os manifestos do `COPY`
do `ingest`, os arquivos do `UNLOAD` e o Parquet do `appender` em
`<raiz>/<ambiente>/staging/<execution_id>/`, que o fim da execução esvazia; o leitor de
`db.open_redshift()` grava os arquivos do `UNLOAD` de `stream` em
`<raiz>/<ambiente>/staging/<id do leitor>/`, que o `close` esvazia. Os manifestos da publicação
ficam em `<raiz>/<ambiente>/publicacao/<execution_id>/<tabela>/<valor>/`, e nenhum subcomando os
apaga.

### O log dos módulos

Cada módulo registra as suas mensagens no `logging` do Python, pelo logger com o nome dele:
`serialize_db.execution` (a abertura e o resumo de cada execução, o relatório de cada auditoria com
o SQL e as amostras), `serialize_db.publication` (a linha de cada tabela publicada; em `DEBUG`, pelo
logger `serialize_db.publication.commands`, o tempo da abertura de cada conexão e o de cada comando,
com o texto mascarado), `serialize_db.parquet_import`, `serialize_db.delta`, `serialize_db.reader`,
`serialize_db.engine.duckdb` e `serialize_db.engine.redshift`. A linha de comando imprime o log no
stderr a partir do nível `INFO`; um programa que chama o pacote vê os avisos (`WARNING`) sem
configurar nada, e o resto depois de `logging.basicConfig(level=logging.INFO)`. O log Delta, ou
log da tabela, é outra coisa: a pasta `_delta_log` da tabela, onde cada commit registra os arquivos
da versão.

## Instalação

Um projeto cliente declara o pacote como dependência uma vez, pela pasta do repositório ou pelo
endereço git, e daí em diante o `uv sync` do projeto instala tudo o que a interface pública usa:

```shell
uv add ../serialize-db                                   # a pasta do repositório
uv add git+https://github.com/felipenoris/serialize-db   # ou o endereço git
uv sync
```

O projeto cliente pede o Python 3.13 ou mais novo, como o pacote (`uv init --python 3.13`); com um
`requires-python` que aceita versões anteriores, o `uv add` recusa o pacote. As dependências que o
`uv sync` instala são as de execução, nas versões fixadas em `pyproject.toml`: `sqlalchemy`,
`pyarrow`, `deltalake`, `duckdb`, `boto3`, que no S3 faz a escrita condicional do arquivo de
controle e a cópia dos arquivos do `archive` e do `export`, dá a chave da sessão AWS ao segredo do
DuckDB e à cláusula de credenciais do `COPY` e do `UNLOAD` sem `iam_role` e pede a credencial
temporária do grupo de trabalho do Redshift, os dialetos `duckdb-engine` e `sqlalchemy-redshift`,
que compilam o texto SQL de cada motor, e `redshift-connector`, o driver do motor Redshift, da
publicação e do leitor Redshift. Os grupos de
`pyproject.toml`, `dev` entre eles, servem ao desenvolvimento do pacote e nunca vão para o projeto
cliente. O pacote não importa o pandas: o cliente que converte o resultado com `to_pandas`, como no
tutorial, declara o pandas no próprio projeto.

No repositório, o `uv sync` instala o Python 3.13, o pacote e o grupo `dev`, o dos testes, que o
`uv` inclui por padrão; `uv sync --no-dev` instala só o pacote e as dependências de execução.

O S3 precisa da região em `AWS_REGION` ou `AWS_DEFAULT_REGION`, e as credenciais vêm da cadeia
padrão do ambiente; as extensões `delta` e, no S3, `httpfs` do DuckDB vêm da pasta de
`SERIALIZE_DB_DUCKDB_EXTENSIONS`, ou de `.duckdb/` ao lado do ambiente virtual, sem download. O
DuckDB lê o S3 com a chave que a cadeia do `boto3` resolve, e a execução e o leitor Delta a trocam
antes de cada comando quando o `boto3` a renova; um comando só mais longo que a validade da chave
ainda falha.

## Tutorial

### Declarar os modelos

Uma tabela particionada por data, com a chave primária inteira sem `autoincrement`, comentários em
toda coluna e as opções físicas em `Table.info`:

```python
from datetime import date

import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Operacao(Base):
    __tablename__ = "cad_operacoes"
    __table_args__ = (
        sa.UniqueConstraint("data", "operacao"),
        {
            "comment": "Operações de crédito por data-base",
            "info": {
                "serialize_db": {
                    "partition_by": ["data_str"],
                    "partition_source": "data",
                    "sort_key": ["data", "operacao"],
                }
            },
        },
    )

    id_operacao: Mapped[int] = mapped_column(
        sa.BigInteger, primary_key=True, autoincrement=False, comment="Identificador da operação"
    )
    data: Mapped[date] = mapped_column(sa.Date, comment="Data-base da operação")
    operacao: Mapped[str] = mapped_column(sa.String(50), comment="Código da operação")
    valor: Mapped[float] = mapped_column(sa.Double, comment="Valor da operação")
    data_str: Mapped[str] = mapped_column(sa.String(10), comment="Partição: data em AAAA-MM-DD")
```

As chaves de `Table.info["serialize_db"]`:

| Chave | O que declara |
| --- | --- |
| `partition_by` | A coluna de partição, uma no máximo, de texto `String(n)`; o valor é o nome da pasta da partição e começa por letra ou dígito, seguido de letras, dígitos, `_`, `.` e `-` (`[0-9A-Za-z][0-9A-Za-z_.-]*`). Na base atual é a data em `AAAA-MM-DD`, a última coluna de cada tabela particionada. |
| `partition_source` | Opcional: a coluna de data de que a coluna de partição deriva (`strftime('%Y-%m-%d')`); com ela, a auditoria e a carga inicial conferem a derivação. |
| `sort_key` | As colunas da `SORTKEY` do Redshift e da ordenação dos arquivos. |
| `redshift` | `diststyle` e `distkey` do Redshift; ausente, a distribuição é `AUTO`. |
| `keys` | `{"add": [[...]], "drop": [[...]]}`: uma chave de negócio a mais para a auditoria, ou uma chave do modelo a menos, sempre por lista de colunas. |

### Conferir os modelos

`serialize_db.schema.check_models` devolve uma lista de textos, um por violação do contrato, vazia
quando os modelos obedecem a ele:

```python
from serialize_db import schema

problems = schema.check_models(Base.metadata)
assert problems == [], "\n".join(problems)
```

As regras: tipo fora da tabela de tipos; `autoincrement` numa chave inteira (o padrão `"auto"`
inclusive); `Identity`; `String` sem comprimento, e as subclasses dela fora `Text`, como
`Unicode`, `VARCHAR` e `CHAR` (declare `String(n)` ou `Text`); chave
estrangeira `DEFERRABLE`, ou cujas colunas apontadas não são a chave primária nem uma
`UniqueConstraint` da tabela apontada, na mesma ordem (um índice único não serve no DuckDB nem no
Redshift, e `create_all` num `sqlalchemy.Connection` do DuckDB falha); `partition_by` sem a coluna
ou com a coluna fora de `String(n)`, como `Text`, `partition_source` que a tabela não tem ou sem
`partition_by`; tabela sem chave primária e sem `keys`.

### Derivar o esquema e o DDL

```python
table = Operacao.__table__

schema.arrow_schema(table)            # pyarrow.Schema: id_operacao int64 not null, data date32, ...
schema.delta_schema(table)            # deltalake.Schema, com timestamp_ntz, decimal(p,s), string
schema.table_options(table)           # TableOptions(partition_by="data_str", ..., keys=(...))
print(schema.ddl(table, "duckdb"))
print(schema.ddl(table, "redshift", prefix="exec_42_"))
```

O DDL do DuckDB:

```sql
CREATE TABLE "cad_operacoes" (
    "id_operacao" BIGINT NOT NULL,
    "data" DATE NOT NULL,
    "operacao" VARCHAR(50) NOT NULL,
    "valor" DOUBLE NOT NULL,
    "data_str" VARCHAR(10) NOT NULL
)
```

O DDL do Redshift, com o prefixo no nome da tabela:

```sql
CREATE TABLE "exec_42_cad_operacoes" (
    "id_operacao" BIGINT NOT NULL,
    "data" DATE NOT NULL,
    "operacao" VARCHAR(50) NOT NULL,
    "valor" DOUBLE PRECISION NOT NULL,
    "data_str" VARCHAR(10) NOT NULL
) SORTKEY ("data", "operacao")
```

As colunas são as mesmas nos dois motores; o `Double` é `DOUBLE` no DuckDB e `DOUBLE PRECISION` no
Redshift, e a `sort_key` vira `SORTKEY` só no Redshift. O `CREATE TABLE` leva colunas, tipos e
`NOT NULL`; as chaves ficam para a auditoria, e o comentário de cada coluna vai no esquema Delta.
`serialize_db.schema.sql_type` dá o nome de um tipo num motor, para um `CAST` ou um `ALTER TABLE`,
e `serialize_db.schema.quoted` cita um identificador. `temporary=True` faz `ddl` emitir
`CREATE TEMP TABLE`, a tabela que dura a sessão: no DuckDB só a conexão que a criou a vê, e um
`cursor()` é outra conexão.

### Converter um lote de dados

`serialize_db.schema.cast` recebe um `pyarrow.RecordBatch`, uma `pyarrow.Table` ou um
`pyarrow.RecordBatchReader` e devolve o mesmo tipo no esquema do contrato: só as colunas do
contrato, na ordem do contrato, cada uma no tipo do contrato, com a nulidade conferida.

```python
import pyarrow as pa

batch = pa.RecordBatch.from_pydict({
    "operacao": ["A1", "B2"],
    "id_operacao": pa.array([1, 2], pa.int32()),      # int32 vira int64
    "data": [date(2026, 8, 31), date(2026, 8, 31)],
    "valor": [10.5, 20.0],
    "data_str": ["2026-08-31", "2026-08-31"],
    "extra": [0, 0],                                   # fora do contrato: ignorada
})
done = schema.cast(batch, table)
done.schema.names   # ["id_operacao", "data", "operacao", "valor", "data_str"]
```

`cast` recusa com `serialize_db.errors.ContractError`, com a tabela e a coluna na mensagem. As
perdas que o cast seguro do PyArrow não acusa vêm com a instrução ao cliente: `double` fora da
escala de um `Numeric`, `timestamp` com hora numa coluna `Date`, `timestamp` com fuso numa coluna
`DateTime` sem fuso e o inverso, documento JSON como `struct`, `list` ou `map`, texto acima de
`String(n)` (medido em bytes, como o `VARCHAR(n)` do Redshift) e texto numa coluna `Text` ou
documento JSON acima de 65.535 bytes, o teto do Redshift. As que o cast seguro acusa vêm com o
texto do PyArrow: nulo em coluna `NOT NULL`, escala perdida num decimal, inteiro que não cabe na
precisão de um `Numeric`, nanossegundo não nulo num timestamp, estouro de inteiro e um tipo sem
conversão para o do contrato (`struct` numa coluna `Integer`). O lote sem coluna alguma do
contrato é recusado com a lista das colunas que chegaram. O texto é medido depois da conversão para
`string`, então o `large_string` do `str` do pandas 3, o `string_view` e o dicionário da `category`
passam pela mesma medida. Um `double` entra numa coluna `Numeric` só quando `round` o devolve igual;
numa coluna `Double` ele entra como chega. O fuso é recusado porque tirá-lo ou pô-lo muda a hora
gravada, e cada camada o faz de um jeito: o mesmo 12:00 UTC vira 12:00 no `cast` do PyArrow e 09:00
no `CAST` do DuckDB com a sessão em `America/Sao_Paulo`. O cliente converte antes de chamar, no
pandas com `serie.dt.tz_convert("America/Sao_Paulo").dt.tz_localize(None)`, ou no SQL do sandbox. Um
`timestamp` de outro fuso numa coluna com fuso entra no mesmo instante, em UTC.

### Versionar os arquivos de esquema

`serialize_db.schema.schema_files` gera, por tabela, o esquema Delta em JSON canônico e o
`CREATE TABLE` de cada motor. O pipeline versiona esses arquivos no seu repositório, e o diff contra
a geração nova mostra o que uma mudança de modelo altera em cada motor:

```shell
serialize-db schema write --metadata pipeline.models:Base.metadata schema/
serialize-db schema check --metadata pipeline.models:Base.metadata schema/
```

`--metadata` recebe `modulo:atributo`, o caminho importável do `MetaData`. O `check` sai com 0
quando os arquivos estão atualizados, 1 com o diff impresso quando há diferença, e 2 no erro de uso.
O `check` compara o texto exato, a quebra de linha final inclusive, e acusa como removido o arquivo
de sufixo gerado que a geração não produz mais; o `write` não apaga arquivo, e por isso `schema/` e
`sql/` são pastas separadas. Em Python, `serialize_db.schema.write_schema_files` e
`serialize_db.schema.check_schema_files` fazem o mesmo.

### Gerar o texto SQL de cada motor

A opção de migração para fora do SQLAlchemy. Um statement Core do pipeline vira texto do DuckDB e
do Redshift por `serialize_db.sql.render`. A partição de referência entra por um `sa.bindparam`
sem valor, que chega ao texto como `:nome`, e o statement é o mesmo que roda num
`sqlalchemy.Connection` do cliente e nos motores; as constantes ficam embutidas, e cada tabela do
contrato sai com o sentinela `{prefix}` no nome, dentro das aspas, que a execução troca pelo
prefixo do sandbox:

```python
from serialize_db import sql

operations = Operacao.__table__
statement = (
    sa.select(operations.c.operacao, sa.func.sum(operations.c.valor).label("total"))
    .where(operations.c.data_str == sa.bindparam("data_str", type_=sa.String(10)),
           operations.c.operacao.like("A%"))
    .group_by(operations.c.operacao)
    .order_by(operations.c.operacao)
)
print(sql.render(statement, "duckdb", Base.metadata))
```

O texto, igual nos dois motores para um statement portável:

```sql
SELECT "{prefix}cad_operacoes"."operacao", sum("{prefix}cad_operacoes"."valor") AS total
FROM "{prefix}cad_operacoes"
WHERE "{prefix}cad_operacoes"."data_str" = :data_str AND "{prefix}cad_operacoes"."operacao" LIKE 'A%' GROUP BY "{prefix}cad_operacoes"."operacao" ORDER BY "{prefix}cad_operacoes"."operacao"
```

Um `bindparam` com valor sai como constante, e um nome de parâmetro fora de `[a-z_][a-z0-9_]*` é
recusado com `serialize_db.errors.SqlError`. Na execução, `serialize_db.sql.bind` reescreve o
marcador para o estilo do motor (`$nome` no DuckDB, `:nome` no `redshift_connector` com
`paramstyle = "named"`) e confere o dicionário de parâmetros; toda região citada passa intacta, e
um texto que ainda traz o sentinela é recusado:

```python
import duckdb

text, values = sql.bind(sql.render(statement, "duckdb", Base.metadata, prefix=""),
                        {"data_str": "2026-08-31"}, "duckdb")
duckdb.connect().execute(text, values)   # ... WHERE "cad_operacoes"."data_str" = $data_str ...
```

O pipeline versiona o texto gerado, um arquivo por statement e por motor, e o diff contra a
geração nova mostra o que uma mudança de modelo ou de statement altera em cada motor;
`--statements` recebe o caminho importável do dicionário `{nome: statement}`:

```shell
serialize-db sql write --metadata pipeline.models:Base.metadata --statements pipeline.queries:STATEMENTS sql/
serialize-db sql check --metadata pipeline.models:Base.metadata --statements pipeline.queries:STATEMENTS sql/
```

`serialize_db.sql.read_sql` lê o arquivo versionado com o sentinela trocado pelo prefixo informado,
a string vazia para as tabelas do contrato ou `exec_<id>_` para o sandbox de uma execução, e
`serialize_db.sql.referenced_tables` lista as tabelas do contrato que um statement ou um texto cita.

### Gravar as tabelas Delta

`serialize_db.storage.Storage` é a raiz do banco, uma pasta local ou um prefixo `s3://`, e cada
primitiva de `serialize_db.delta` recebe a URI da pasta da tabela e o `Storage`. A tabela nasce do
modelo, e cada partição entra num commit que a substitui, com os metadados da execução:

```python
from serialize_db import delta, schema
from serialize_db.storage import Storage

storage = Storage.for_uri("s3://bucket/projeto/delta")
uri = storage.uri_of("prd/cad_operacoes")
delta.create_table(uri, table, storage)                         # versão 0, repetível
metadata = delta.commit_metadata("exec-2026-09-05", {"cad_contratos": 88})
version = delta.publish_partition(uri, table, "2026-08-31", schema.cast(data, table), metadata, storage)
delta.version_diff(uri, version - 1, version, table, storage)   # {"2026-08-31"}
```

O valor de partição segue `serialize_db.schema.PARTITION_VALUE`, letra ou dígito no início e
depois letras, dígitos, `_`, `.` e `-`, porque vira nome de pasta e literal SQL. Duas escritas da
mesma partição a partir da mesma versão levantam `serialize_db.errors.ExecutionConflict`, e a
segunda não commita. `serialize_db.delta.register_files` registra no log os arquivos que outro
escritor gravou dentro da pasta da tabela, como o `COPY` do DuckDB, depois de conferir o rodapé de
cada um, e relê a versão pelos dois leitores, desfazendo o commit numa diferença.

`serialize_db.delta.reconcile` aplica ao log o que o modelo acrescentou (coluna anulável,
`NOT NULL` relaxado, comentários) e recusa com `serialize_db.errors.SchemaDiffRefused` o que só
`serialize_db.delta.rewrite` resolve, num commit: renomeação, remoção e mudança de tipo. A coluna
nova entra em qualquer posição do modelo: os arquivos gravados antes dela não a têm, e todo leitor,
o `COPY` do Redshift inclusive, liga cada coluna do arquivo à de mesmo nome e lê a nova nula neles.
`serialize_db.delta.snapshot` marca as versões de um snapshot do banco no arquivo de controle do
ambiente, e `serialize_db.delta.vacuum_keeping_snapshots` as preserva. O nome do snapshot é
imutável: a entrada não muda depois de gravada, e o nome não volta a ser usado, nem depois do
arquivamento.

### Importar a base Parquet atual

`serialize_db.parquet_import` leva a base Parquet de hoje, uma pasta por tabela sob a raiz de
origem, `<origem>/<tabela>/`, com as partições Hive `<coluna>=<valor>/`, para as tabelas Delta do
ambiente, `<raiz>/<ambiente>/<tabela>`, uma partição por commit, sem tocar a origem.
`serialize_db.parquet_import.import_table` cria a tabela do contrato, pula as partições já no log
da tabela Delta, confere cada uma das outras (o valor do caminho na coluna de origem, os nulos das
colunas `NOT NULL`, os textos acima do limite que `cast` e a auditoria medem) e a grava pelo `COPY`
do DuckDB num arquivo novo da pasta da partição, na ordem da `sort_key`, registrando o arquivo no
log da tabela;
`serialize_db.parquet_import.import_report` confere contagem e somas por partição entre a origem e o
Delta:

```python
from serialize_db import parquet_import
from serialize_db.execution import Database

db = Database("s3://bucket/projeto/delta", "prd", Base.metadata)
for table in parquet_import.import_order(db.tables()):
    parquet_import.import_table(db, table, "s3://bucket/projeto/db_projetado")   # as partições gravadas
    report = parquet_import.import_report(db, table, "s3://bucket/projeto/db_projetado")
    assert report.matches, report
```

Uma partição fora do contrato é `serialize_db.errors.ContractError` antes de qualquer gravação, com
a tabela, a partição e a coluna, e o arquivo quando é o primeiro da pasta que não tem uma coluna do
contrato; a chamada seguinte recomeça dela. Uma coluna do contrato ausente de um arquivo depois do
primeiro, ou um valor que não converte para o tipo do contrato, falha na conferência ou no `COPY`
com o erro do DuckDB, também sem commit.

A carga converte cada coluna para o tipo do contrato pelo `CAST` do DuckDB, que aceita quatro
perdas que `cast` recusa: um `double` com mais casas que a escala de um `Numeric` entra
arredondado, um `timestamp` com hora numa coluna `Date` perde a hora, um `timestamp` com fuso numa
coluna `DateTime` sem fuso entra na hora do `TimeZone` da conexão, o fuso da máquina, e um
`timestamp` `INT96` com nanossegundos numa coluna `DateTime` entra truncado a microssegundos.
`import_report` mostra o arredondamento quando ele muda a soma da coluna, e não vê a hora, o fuso
nem os nanossegundos. A linha `conversões` do relatório lista cada coluna cujo tipo no arquivo
difere do contrato, como `carimbo: INT96 -> timestamp[us]`, lida no rodapé do primeiro arquivo de
cada partição conferida: a linha traz as conversões da maior parte das partições, e cada partição
com outras sai numa linha `conversões em <valor>`, como a de colunas `Double` gravadas em `float`,
cujo valor chega ao Delta com a precisão do `float` (1.2 vira 1.2000000476837158). As linhas não
dizem quais conversões perdem dado nem se algum valor perdeu, e não veem o tipo dos outros
arquivos da partição. Na linha de comando:

```shell
serialize-db import --root s3://bucket/projeto/delta --environment prd \
    --metadata pipeline.models:Base.metadata --source s3://bucket/projeto/db_projetado
```

### Rodar uma execução

`serialize_db.Database` junta a raiz, o ambiente e os modelos, e `serialize_db.Execution` é o ciclo
de uma execução: a entrada do `with` abre as tabelas do ambiente, fixa a versão de cada uma e cria
o sandbox, e o fim do bloco o descarta. Entre os dois, o pipeline traz as tabelas, roda a lógica,
audita e publica no Delta:

```python
import sqlalchemy as sa

from serialize_db import Database, Execution

db = Database("s3://bucket/projeto/delta", "prd", Base.metadata)
with Execution(db, "duckdb", "2026-08-31", execution_id="exec-2026-09-05") as run:
    run.ingest(Lancamento.__table__, partitions=run.previous_partitions(Lancamento.__table__, 12),
               materialize=True)
    run.sandbox.create_table(Projetado.__table__)
    with run.sandbox.stream(sa.select(Lancamento)) as stream, \
            run.sandbox.appender(Projetado.__table__) as appender:
        for batch in stream:
            ids = run.next_ids(Projetado.__table__, batch.num_rows)      # faixa contígua, sob lock
            appender.write(project(batch, ids))
    run.audit(Projetado.__table__, ["2026-08-31"])                      # AuditFailed na reprovação
    run.publish_delta(Projetado.__table__, partitions=["2026-08-31"])   # overwrite por partição
```

`run.sandbox` é o motor da execução, que o `Execution` constrói pelo segundo argumento: `"duckdb"`
dá um `serialize_db.engine.duckdb.DuckDBEngine`, e `"redshift"` um
`serialize_db.engine.redshift.RedshiftEngine`. O pipeline chama nele `stream`, `query`,
`create_table`, `append`, `appender`, `session` e `new_session`, e pelo `run` as primitivas que
precisam da pasta e da versão fixada de cada tabela: `run.ingest`, `run.pinned_delta`, `run.audit`
e `run.publish_delta`. Os dois motores seguem a interface `serialize_db.engine.Engine`, e o
pipeline escrito em statements Core roda em qualquer um deles; as seções seguintes descrevem cada
motor. Fora de uma execução, como nos testes, o motor se constrói à mão, com a pasta e a versão de
cada tabela em cada chamada, e a página de cada motor traz o exemplo.

O terceiro argumento do `Execution`, opcional, é a partição da execução, `run.partition`:
`run.previous_partitions` devolve as partições até ela, e o log `serialize_db.execution` a registra
na abertura e no fim da execução. `run.audit` e `run.publish_delta` recebem as partições por
argumento.

`run.publish_delta` exige a auditoria aprovada das partições na própria execução e recusa com
`serialize_db.errors.ExecutionConflict` a tabela em que outra execução gravou dados depois da
abertura. Cada partição sai do sandbox num arquivo que o motor grava na pasta da partição na tabela
Delta e entra no log da tabela por `serialize_db.delta.register_files`, depois das conferências.
`run.snapshot("2026T3")` marca a execução: os commits levam o nome, e o encerramento sem erro grava
as versões de todas as tabelas no arquivo de controle do ambiente; um nome já usado, mesmo
arquivado, é `serialize_db.errors.ContractError` na chamada, antes de qualquer commit;
`serialize-db channel --name default --snapshot 2026T3` aponta depois o canal `default`, o snapshot
que o leitor Delta lê sem argumento e que `serialize-db publish_redshift --channel default` publica.
Refazer os dados de um snapshot é uma execução nova, marcada com outro nome
(`run.snapshot("2026T3.r2")`), seguida do canal apontado para o nome novo; o `2026T3` continua
legível pelo nome, e o runbook na página de `serialize_db.cli`, seção "Refazer um snapshot", traz os
comandos.

A linha de comando abre a mesma execução para uma função `modulo:funcao` que recebe `run`, e
`serialize-db audit` imprime o texto das verificações de uma tabela ou roda a auditoria sobre a
versão atual do Delta:

```shell
serialize-db run --root s3://bucket/projeto/delta --environment prd --partition 2026-08-31 \
    --metadata pipeline.models:Base.metadata pipeline.mensal:main
serialize-db audit --metadata pipeline.models:Base.metadata --table cad_lancamentos --engine redshift --sql
serialize-db audit --metadata pipeline.models:Base.metadata --table cad_lancamentos \
    --partitions 2026-08-31 --root s3://bucket/projeto/delta --environment prd
```

O `run` sai com 0 quando o pipeline termina, 1 na auditoria reprovada e 2 no conflito e no erro de
uso; `--root`, `--environment` e `--engine` têm por padrão `SERIALIZE_DB_ROOT`,
`SERIALIZE_DB_ENVIRONMENT` (`dsv`) e `SERIALIZE_DB_ENGINE` (`duckdb`), e um motor fora de `duckdb` e
`redshift`, na opção ou na variável, é erro de uso.

### Rodar o pipeline no sandbox DuckDB

Com `"duckdb"`, `run.sandbox` é um `serialize_db.engine.duckdb.DuckDBEngine`: um banco em arquivo
numa pasta nova de `tempfile.gettempdir()`, com uma sessão que várias threads usam uma de cada vez.
O fim da execução fecha a conexão, que só então devolve a memória, e apaga a pasta com o banco. Os
limites do DuckDB saem da máquina na abertura: `threads` são as CPUs que o processo pode usar e
`memory_limit` é metade da memória que ele ainda pode usar, lidas por `serialize_db.resources` com
o limite do cgroup de um contêiner; a memória abaixo de 2 MiB, ou negativa, é recusada com
`SandboxError` antes da abertura. A tabela Delta entra presa à versão fixada, e os dados saem e
voltam em lotes Arrow:

```python
import sqlalchemy as sa

with Execution(db, "duckdb", "2026-08-31", execution_id="exec-2026-09-05") as run:
    run.ingest(Lancamento.__table__, partitions=["2026-07-31", "2026-08-31"], materialize=True)
    query = sa.select(Lancamento).where(Lancamento.data_base_str == sa.bindparam("particao"))
    run.sandbox.create_table(Projetado.__table__)
    with run.sandbox.stream(query, {"particao": "2026-08-31"}) as stream, \
            run.sandbox.appender(Projetado.__table__) as appender:
        for batch in stream:                 # a consulta continua enquanto o cliente trabalha
            appender.write(project(batch))   # cast aqui; as linhas entram no close do appender
```

`run.sandbox.query` devolve a `pa.Table` inteira, e `run.sandbox.append` acrescenta a uma tabela do
sandbox uma `pa.Table`, um lote, um leitor ou um iterável de lotes; um DataFrame é recusado com a
conversão sem cópia na mensagem (`pa.Table.from_pandas(frame, preserve_index=False)`). A tabela
vem do `ingest` com `materialize=True` ou de `run.sandbox.create_table`, que cria a tabela vazia
do modelo e recusa com `serialize_db.errors.SandboxError` o nome já ocupado; o `appender` recusa
com o mesmo erro a tabela que não existe e a view do `ingest`, e `run.pinned_delta(table)` lê a
versão fixada sem ocupar nome.
`with run.sandbox.session() as connection:` dá a conexão crua ao que as primitivas não cobrem, e
`with run.sandbox.new_session() as other:` abre uma sessão a mais para o que roda em paralelo. No
Redshift, o `append`, o `appender`, o `ingest` e o `pinned_delta` recusam com `SandboxError` a
sessão numa transação que o cliente abriu por essa conexão, cujo trabalho o `COMMIT` deles
confirmaria junto. A execução usa sempre os limites da máquina;
`DuckDBConfig(threads=..., memory_limit=...)` os troca só no motor construído à mão.

### Auditar antes de publicar no Delta

`run.audit(table, partitions)` roda no motor as verificações que `serialize_db.audit.checks`
deriva do modelo: nulo em coluna `NOT NULL`, texto acima de `String(n)` em bytes, texto numa coluna
`Text` ou documento JSON acima de 65.535 bytes, JSON inválido, a partição fora da coluna de origem
e do padrão de nome de pasta, a chave repetida na partição e, quando a chave não inclui a
partição, contra as demais partições da versão fixada, e o órfão de chave estrangeira com
`foreign_keys=True`. Ele devolve o `AuditReport` aprovado e levanta
`serialize_db.errors.AuditFailed` na reprovação. O relatório traz o SQL de cada verificação, até 20
linhas de amostra das reprovadas, as somas de controle e as colunas `Double` com `NaN` ou infinito,
que `run.publish_delta` grava sem mínimo e máximo. `serialize_db.audit.audit_sql(table, "redshift")`
devolve o texto de cada verificação pelo nome dela, para depuração, e `serialize-db audit --sql`
o imprime.

`run.publish_delta` leva cada partição auditada ao Delta pelo `export_partition` do motor: o motor
DuckDB registra o arquivo que o seu `COPY` gravou, e o motor Redshift os arquivos do seu `UNLOAD`,
depois das conferências do rodapé; a partição com uma coluna `Double` de valor não finito sai do
Redshift por `serialize_db.delta.publish_partition`, com um aviso no log
`serialize_db.engine.redshift`, porque o rodapé do `UNLOAD` deixa o `NaN` fora do máximo.

### Rodar o pipeline no sandbox Redshift

Com `"redshift"`, `run.sandbox` é um `serialize_db.engine.redshift.RedshiftEngine`, o mesmo sandbox
nas tabelas `exec_<id>_*` do esquema do Redshift. A conexão vem de
`serialize_db.engine.redshift.RedshiftConfig`, a credencial temporária do workgroup serverless ou o
par informado, com o `USE` no banco do datashare e o `search_path` no esquema; sem `redshift=`, o
`Execution` a lê das variáveis `SERIALIZE_DB_REDSHIFT_*` por `RedshiftConfig.from_environment()`.
`run.ingest` carrega as partições por `COPY ... MANIFEST`, `stream` lê os arquivos de um `UNLOAD` em
`<raiz>/<ambiente>/staging/<execution_id>/`, `create_table` cria a tabela `exec_<id>_<tabela>` pela
DDL, `appender` grava ali um Parquet e o acrescenta no `close` por um `COPY ... MANIFEST` com a
lista das colunas do lote, que falha se o arquivo faltar, e `run.publish_delta` registra os
arquivos do `UNLOAD` na pasta da partição. O fim da
execução apaga as tabelas `exec_<id>_*` que ela criou e os arquivos do `staging/` e fecha a conexão:

```python
import sqlalchemy as sa

from serialize_db.engine.redshift import RedshiftConfig

config = RedshiftConfig(workgroup="controladoria-wg", database="dev",
                        share_database="datalake_rw_shared", schema="sbx_aco_decon",
                        region="sa-east-1")
with Execution(db, "redshift", "2026-08-31", execution_id="exec-2026-09-05",
               redshift=config) as run:
    run.ingest(Lancamento.__table__, partitions=["2026-08-31"])
    run.sandbox.create_table(Projetado.__table__)
    with run.sandbox.stream(sa.select(Lancamento)) as stream, \
            run.sandbox.appender(Projetado.__table__) as appender:
        for batch in stream:                 # os lotes vêm dos arquivos do UNLOAD
            appender.write(project(batch))
```

Um texto SQL pronto cita as tabelas do sandbox pelo sentinela `{prefix}`
(`"{prefix}cad_lancamentos"`), que o motor troca pelo prefixo da execução. O `COPY` e o `UNLOAD`
levam as credenciais da sessão `boto3`, ou o `IAM_ROLE` da configuração; o texto que as carrega
nunca vai a log, e `serialize_db.engine.redshift.mask` o mascara.

### Atualizar uma tabela de domínio sem partição

Uma tabela de domínio, como a de moedas, não declara `partition_by`, e cada publicação no Delta a
substitui inteira:

```python
class Moeda(Base):
    __tablename__ = "dom_moedas"
    __table_args__ = {"comment": "Moedas"}

    id_moeda: Mapped[int] = mapped_column(
        sa.BigInteger, primary_key=True, autoincrement=False, comment="Identificador da moeda"
    )
    sigla: Mapped[str] = mapped_column(sa.String(3), comment="Sigla ISO 4217")
    nome: Mapped[str] = mapped_column(sa.String(40), comment="Nome da moeda")
```

O pipeline que só atualiza tabelas assim abre a execução sem partição, e `run.partition` é `None`.
`run.ingest` sem `partitions` traz a tabela inteira na versão fixada, e `materialize=True` a torna
uma tabela do sandbox, que `UPDATE`, `INSERT` e `DELETE` alteram; sem ele, a view do DuckDB recusa
o `UPDATE` com `Can only update base table`:

```python
import sqlalchemy as sa

with Execution(db, "duckdb") as run:
    run.ingest(Moeda.__table__, materialize=True)
    run.sandbox.query(sa.update(Moeda.__table__)
                      .where(Moeda.__table__.c.sigla == "USD")
                      .values(nome="Dólar dos EUA"))
    run.sandbox.query(sa.insert(Moeda.__table__).values(
        id_moeda=run.next_ids(Moeda.__table__, 1)[0], sigla="EUR", nome="Euro"))
    run.audit(Moeda.__table__, None)
    run.publish_delta(Moeda.__table__)
```

`run.audit(table, None)` confere a tabela inteira do sandbox, e `run.publish_delta(table)`, sem
`partitions`, grava a tabela num commit que substitui a versão anterior. `run.next_ids` dá os ids
acima do maior da versão fixada, que avança a cada `run.publish_delta` da tabela, a partir de 1 na
tabela nova.

Na primeira carga a tabela ainda não existe: `run.ingest` e `run.pinned_delta` a recusam com
`serialize_db.errors.SandboxError`, os dados entram na tabela de `run.sandbox.create_table` por
`run.sandbox.append` ou pelo `appender`, e `run.publish_delta` cria a tabela antes do commit. Para
gravar a tabela a partir da versão fixada sem ocupar o nome dela no sandbox, `run.pinned_delta` a
lê, `run.sandbox.create_table` cria a tabela vazia e `run.sandbox.append` grava o resultado:

```python
with Execution(db, "duckdb") as run:
    current = run.pinned_delta(Moeda.__table__)
    kept = run.sandbox.query(sa.select(current).where(current.c.sigla != "EUR"))
    run.sandbox.create_table(Moeda.__table__)
    run.sandbox.append(Moeda.__table__, kept)
    run.audit(Moeda.__table__, None)
    run.publish_delta(Moeda.__table__)
```

A auditoria da tabela de domínio não olha as tabelas que a referenciam: a moeda removida que outra
tabela ainda aponta só reprova a auditoria dessa tabela, com `foreign_keys=True`. Na execução sem
partição, `run.previous_partitions` é `serialize_db.errors.ContractError`, e uma tabela
particionada continua publicável com `partitions` explícito. Na linha de comando, `serialize-db run`
sem `--partition` abre a mesma execução:

```shell
serialize-db run --root s3://bucket/projeto/delta --environment prd \
    --metadata pipeline.models:Base.metadata pipeline.dominios:main
```

`serialize-db publish_redshift` também substitui inteira, no Redshift, a tabela sem partição.

### Publicar para os clientes no Redshift

`serialize_db.publication` publica as tabelas `<ambiente>_<tabela>` no esquema do Redshift a partir
do Delta, uma transação por tabela: a linha de `serialize_db_publications` lida no início diz a
versão publicada, `serialize_db.delta.version_diff` diz as partições alteradas desde ela, cada uma
entra por `COPY ... MANIFEST`, um por lista de colunas dos arquivos, numa staging temporária e
`INSERT ... SELECT`, e a linha de controle é gravada por último; cada tabela publicada vai ao log
`serialize_db.publication` com as partições, o tempo e o pico de memória residente do processo. Os
manifestos do `COPY` ficam em `<raiz>/<ambiente>/publicacao/<execution_id>/<tabela>/<valor>/`, e a
publicação não os apaga. A tabela de controle é criada uma vez, pelo usuário:

```shell
serialize-db publish_redshift --init
```

A publicação vem depois da execução, pela linha de comando, que escolhe as versões por
`--snapshot <nome>` ou `--channel <nome>`: `default` é o snapshot que `serialize-db channel`
apontou, e `current` a versão atual de cada tabela, sem snapshot; `--status` mostra o estado, e
`--unpublish` despublica:

```shell
serialize-db channel --root s3://bucket/projeto/delta --environment prd \
    --metadata pipeline.models:Base.metadata --name default --snapshot 2026T3
serialize-db publish_redshift --root s3://bucket/projeto/delta --environment prd \
    --metadata pipeline.models:Base.metadata --channel default
serialize-db publish_redshift --root s3://bucket/projeto/delta --environment prd \
    --metadata pipeline.models:Base.metadata --snapshot 2026T2 --tables cad_lancamentos_projetados
serialize-db publish_redshift --root s3://bucket/projeto/delta --environment prd \
    --metadata pipeline.models:Base.metadata --channel current
serialize-db publish_redshift --root s3://bucket/projeto/delta --environment prd \
    --metadata pipeline.models:Base.metadata --status
serialize-db publish_redshift --root s3://bucket/projeto/delta --environment prd \
    --metadata pipeline.models:Base.metadata --unpublish --tables cad_lancamentos_projetados
```

A publicação de um snapshot anterior ao publicado volta a tabela: as partições alteradas entre as
duas versões recebem os arquivos da versão pedida, e a partição que só a versão publicada tinha
sai. O snapshot arquivado e a tabela do modelo fora do snapshot são erro de uso, sem escrita no
Redshift, e `--tables` deixa a tabela de fora.

Sem a tabela de controle, a publicação para com `serialize_db.errors.PublicationError` antes de
qualquer escrita; duas publicações da mesma tabela ao mesmo tempo terminam com a segunda em
`serialize_db.errors.ExecutionConflict`, sem repetição; uma coluna anulável nova no modelo entra
na tabela publicada por `ALTER TABLE ... ADD COLUMN`, e um diff destrutivo (coluna removida,
coluna `NOT NULL` nova, tipo ou largura de `VARCHAR(n)` que mudou) despublica a tabela e a recria
inteira na mesma publicação.

### Ler a base com o modelo

`serialize_db.reader` lê a base para quem tem o modelo, com o mesmo statement Core nas duas origens
e o resultado em Arrow. `db.open_delta()` abre um DuckDB no processo com uma view por tabela do
modelo sobre o snapshot do canal `default`; `snapshot=` lê um snapshot pelo nome, o arquivado pela
cópia em `<raiz>/<ambiente>/arquivo/<nome>/`, e `channel="current"` a versão atual de cada tabela.
`db.open_redshift()` e `serialize_db.reader.open_redshift` leem as tabelas publicadas
`<ambiente>_<tabela>`, a segunda para o cliente sem a raiz Delta:

```python
import pandas as pd
import sqlalchemy as sa

from serialize_db.reader import open_redshift

statement = sa.select(Lancamento).where(Lancamento.data_base_str == "2026-08-31")

with db.open_delta() as reader:                        # o snapshot do canal default
    reader.materialize(Conta.__table__)
    reader.materialize(Lancamento.__table__, partitions=["2026-07-31", "2026-08-31"])
    frame = reader.query(statement).to_pandas(types_mapper=pd.ArrowDtype)
    reader.versions                                    # {"cad_contas": 1, "cad_lancamentos": 143}

reader = db.open_delta(channel="current")             # a versão atual, sem with, num caderno
reader.close()                                         # sem ele, a coleta apaga a pasta temporária

with open_redshift(Base.metadata, "prd") as reader:   # SERIALIZE_DB_REDSHIFT_*, só query
    frame = reader.query(statement).to_pandas(types_mapper=pd.ArrowDtype)

with open_redshift(Base.metadata, "prd", unload_to="s3://bucket-do-cliente/tmp") as reader:
    with reader.stream(statement) as batches:          # o resultado grande, pelo UNLOAD
        for batch in batches:
            work(batch)
```

Cada view fica presa à versão lida na abertura, e a leitura entre tabelas é consistente num
snapshot; no Redshift, cada tabela é publicada na sua transação, e uma consulta que junta duas
tabelas durante uma publicação pode ler versões diferentes. O leitor roda só `Select` e
`CompoundSelect`; um comando, como uma tabela temporária, vai pela conexão de `session()`. O
`delta_scan` poda as partições por `=`, por `BETWEEN` e pelo `IN` ao lado de um intervalo, e abre
todos os arquivos com um `IN` de mais de um valor sozinho. O DuckDB só devolve a memória no
`close`; o leitor sem `close` apaga a pasta temporária quando é coletado, e o `stream` e a `session`
o seguram até o `close` e o fim do bloco, também o leitor fora de uma variável, como em
`db.open_delta().stream(...)`.

Fora do pacote, o `delta_scan` do DuckDB, o `DeltaTable.scan(predicate=...)` e o `QueryBuilder` do
deltalake e o `scan_delta` do Polars leem as tabelas com filtro certo. O dataset Arrow do delta-rs
erra o filtro numa coluna sem mínimo e máximo no log do arquivo: `DeltaTable.to_pyarrow_dataset()`
com filtro, `to_pyarrow_table` e `to_pandas` com `filters` e o `scan_delta(..., use_pyarrow=True)`
do Polars pulam esse arquivo num filtro de valor. Quando o log também não tem o `nullCount` da
coluna, o dataset e o Polars com `use_pyarrow=True` pulam o arquivo num `IS NULL`, e o `IS NOT NULL`
do dataset traz os nulos dele; o DuckDB sobre esse dataset erra parte dos filtros (deltalake 1.6.4 e
1.6.6, em 2026-09-25). `serialize_db.delta.register_files`, que o `export_partition` dos motores e o
`import_table` usam, deixa sem mínimo e máximo as colunas `Numeric`, `DateTime` e `Boolean` e o
texto dos arquivos do `UNLOAD`; ele e `serialize_db.delta.publish_partition` deixam sem os dois as
`Double` com valor não finito na partição. O filtro nas outras colunas e a leitura sem filtro saem
certos.

## Multithreading

O pacote usa threads em três lugares: no trabalho nativo de cada comando, porque o DuckDB, o
delta-rs e o PyArrow soltam o GIL e usam as CPUs da máquina; nas threads auxiliares que o `stream` e
o `appender` abrem ao lado do laço do cliente; e nos pools que levam várias tabelas ao mesmo tempo.
Toda primitiva pode ser chamada de qualquer thread e é síncrona: quando ela volta, o efeito vale
para o comando seguinte, de qualquer thread. Cada motor guarda uma sessão por execução, sob um lock:
os comandos que várias threads mandam à sessão principal rodam um de cada vez, e
`run.sandbox.new_session()` abre uma sessão a mais, que roda ao lado da principal.

### As APIs com threads

| API | O que roda ao mesmo tempo |
| --- | --- |
| `run.sandbox.stream(statement)` e o `stream` dos leitores | No DuckDB, uma thread roda a consulta e entrega os lotes, até 64 MiB em memória e o resto num arquivo intermediário, enquanto o cliente trabalha nos que já chegaram. No Redshift, o `UNLOAD` roda inteiro antes do primeiro lote, e uma thread lê os arquivos dele dois lotes à frente do cliente. |
| `run.sandbox.appender(table)` | O `write` converte o lote na thread do cliente, e uma thread grava os lotes num arquivo, Arrow IPC na pasta de transbordo do DuckDB ou Parquet no `staging/` do Redshift, enquanto o cliente produz o seguinte; o `close` insere tudo num comando. `run.sandbox.append(table, data)` é a forma de uma chamada, com os lotes prontos. |
| `run.sandbox.new_session()` | Uma sessão a mais, com o seu lock: um cursor da mesma conexão no DuckDB e outra conexão no Redshift, sem as tabelas temporárias da sessão principal. |
| `run.ingest(*tables)` | Uma sessão a mais por tabela, todas ao mesmo tempo. |
| `reader.materialize(*tables)` e `db.open_delta()` | Uma sessão a mais por tabela, todas ao mesmo tempo: a cópia de cada tabela e a view de cada uma na abertura. |
| `run.publish_delta(*tables, max_workers=n)` | Até `n` tabelas ao mesmo tempo: o arquivo de cada partição sai da sessão principal, uma tabela por vez, e o registro no log, as conferências, o commit e a releitura correm em paralelo. |
| `serialize_db.publication.publish_redshift(..., max_workers=n)` e `serialize-db publish_redshift --max-workers n` | Até `n` tabelas ao mesmo tempo, uma conexão cada. |
| `run.next_ids(table, n)` | Faixas de ids que não se sobrepõem entre threads. |

### O ganho sobre a execução em série

Em 2026-10-04, num contêiner Linux de 4 vCPUs (Python 3.13.14, DuckDB 1.5.5, PyArrow 25.0.1,
deltalake 1.6.6, pandas 3.0.6), o motor DuckDB num banco em arquivo leu e gravou 10.000.000 de
linhas de seis colunas em lotes de 100.000, e as tabelas Delta ficaram numa pasta local. O trabalho
do cliente em cada lote foi a conversão para o pandas, duas colunas calculadas nele e a volta ao
Arrow. Cada tempo é o menor de três medidas, cada uma num processo novo, e cada memória é o maior
pico de memória residente do processo acima da base nas três:

| O que o pipeline faz | Em série | Com threads | Ganho |
| --- | --- | --- | --- |
| Ler e trabalhar em cada lote | `query` e o laço: 1,378 s, 699 MB | `stream`: 0,835 s, 133 MB | 1,65 vez |
| Gravar os lotes que o cliente produz | o trabalho de todos e o `append`: 4,733 s, 1.173 MB | `appender.write` em cada lote: 3,109 s, 857 MB | 1,52 vez |
| Ler, trabalhar e gravar | `query`, o trabalho e o `append`: 5,162 s, 1.870 MB | `stream` e `appender` no mesmo `with`: 3,509 s, 1.020 MB | 1,47 vez |
| 200 consultas pequenas em texto SQL, em quatro threads | 0,375 s em série e 0,377 s nas threads, na sessão principal | uma sessão a mais por thread: 0,170 s | 2,20 vezes |
| Quatro tabelas de 5.000.000 de linhas para o sandbox | `run.ingest` de cada uma, `materialize=True`: 4,859 s, 439 MB | `run.ingest` das quatro: 3,735 s, 612 MB | 1,30 vez |
| As mesmas quatro no leitor Delta | `materialize` de cada uma: 5,888 s, 435 MB | `materialize` das quatro: 3,765 s, 635 MB | 1,56 vez |
| As mesmas quatro para o Delta | `publish_delta` com `max_workers=1`: 1,941 s | `max_workers=4`: 1,905 s | 1,02 vez |

Sem trabalho do cliente, o `stream` ganhou de 1,12 a 1,14 vez, o `appender` 1,09 e os dois juntos
1,07; com 5 ms de espera por lote, como uma chamada de rede, o `stream` ganhou de 1,90 a 1,91 vez, e
com 5 ms de laço Python puro, de 1,70 a 1,73. O primeiro lote do `stream` chegou em 0,02 s, contra
0,8 s do `query`. Num processo que já tinha usado a memória, como o da sonda do ambiente alvo, a
série do `appender` custou menos, e o ganho dele com o pandas ficou entre 1,07 e 1,23 vez. As
consultas pequenas da tabela são texto SQL; as mesmas por statement Core, que o motor compila em
Python a cada chamada, levaram 0,760 s em série, 0,826 s nas threads da sessão principal e 0,608 s
nas sessões a mais (1,25 vez), num processo só. Quatro agregações sobre as 10.000.000 de linhas
levaram 0,270 s em série e 0,242 s em quatro threads com uma sessão a mais cada.

No ambiente alvo, com as tabelas no S3, o `run.ingest` das quatro tabelas de uma partição da base
ganhou 1,25 vez com 4 vCPUs (2026-09-23, com o cache de arquivos externos do DuckDB ligado), 1,89
vez com 16 vCPUs (2026-09-24) e, numa partição em que a maior tabela tinha 85% das linhas, de 1,16 a
1,22 vez com 8 vCPUs (2026-09-27, 2026-09-29, 2026-10-05 e às 19:45 de 2026-10-09) e 1,05 vez com
4 vCPUs (às 02:27 de 2026-10-09).

Em 2026-10-05, a comparação rodou no ambiente alvo, numa máquina de 8 vCPUs com 13,0 GiB
disponíveis (Python 3.13.15 e as versões acima), sobre quatro tabelas Delta iguais de 5.000.000 de
linhas no S3, com o Redshift serverless do projeto e o mesmo trabalho do cliente. Cada tempo é o
menor de três medidas num processo só, e cada memória é o pico do processo acima da base nessa
medida:

| O que o pipeline faz | Em série | Com threads | Ganho |
| --- | --- | --- | --- |
| Ler e trabalhar em cada lote, no DuckDB | `query` e o laço: 0,712 s, 313 MB | `stream`: 0,389 s, 8 MB | 1,83 vez |
| Gravar os lotes que o cliente produz, no DuckDB | o trabalho de todos e o `append`: 2,437 s, 266 MB | `appender.write` em cada lote: 2,279 s, 223 MB | 1,07 vez |
| Ler, trabalhar e gravar, no DuckDB | `query`, o trabalho e o `append`: 3,009 s, 628 MB | `stream` e `appender` no mesmo `with`: 2,143 s, 207 MB | 1,40 vez |
| 200 consultas pequenas por statement Core, em quatro threads, no DuckDB | 0,526 s em série e 0,541 s nas threads, na sessão principal | uma sessão a mais por thread: 0,348 s | 1,51 vez |
| Quatro tabelas para o sandbox DuckDB | `run.ingest` de cada uma, `materialize=True`: 9,564 s, 916 MB | `run.ingest` das quatro: 4,005 s, 1.035 MB | 2,39 vezes |
| As mesmas quatro no leitor Delta | `materialize` de cada uma: 10,097 s, 896 MB | `materialize` das quatro: 4,139 s, 991 MB | 2,44 vezes |
| As mesmas quatro do DuckDB para o Delta | `publish_delta` com `max_workers=1`: 18,221 s | `max_workers=4`: 8,951 s | 2,04 vezes |
| Quatro tabelas para o sandbox Redshift | `run.ingest` de cada uma: 32,509 s | `run.ingest` das quatro: 12,536 s | 2,59 vezes |
| Ler, trabalhar e gravar, no Redshift | o `stream` lido inteiro, o trabalho e o `append`: 12,454 s, 457 MB | `stream` e `appender` no mesmo `with`: 12,424 s, 122 MB | 1,00 vez |
| 80 consultas pequenas por statement Core, em quatro threads, no Redshift | 6,858 s em série e 6,746 s nas threads, na sessão principal | uma sessão a mais por thread, aberta na medida: 2,181 s | 3,14 vezes |
| As mesmas quatro do Redshift para o Delta | `publish_delta` com `max_workers=1`: 26,681 s | `max_workers=4`: 19,131 s | 1,39 vez |
| As quatro do Delta para o Redshift | `publish_redshift` com `max_workers=1`: 29,987 s | `max_workers=4`: 31,521 s | 0,95 vez |

Sem trabalho do cliente, o `stream` do DuckDB ganhou 1,26 vez, o `appender` 1,00 e os dois juntos
1,03. No Redshift, o `stream`, o `appender` e os dois juntos ficaram entre 0,96 e 1,07 vez, com e
sem o pandas: o `UNLOAD` do `stream` roda inteiro antes do primeiro lote, o `COPY` do `close` roda
depois do laço, e o trabalho do cliente só se sobrepõe à leitura e à gravação dos arquivos. O
`stream` e os dois juntos ficaram de 79 MB a 129 MB acima da base, contra 243 MB a 457 MB da série.
O `run.ingest`, o `materialize` e o `publish_delta` das quatro tabelas ganharam mais que na pasta
local, porque o S3 e o Redshift fazem esperar a rede, e o pico do `run.ingest` e do `materialize`
ficou de 11% a 13% acima da série. Cada consulta pequena no Redshift levou cerca de 86 ms.

Em 2026-10-07 a mesma sonda rodou numa máquina de 2 vCPUs com 5,8 GiB disponíveis, sobre as mesmas
quatro tabelas no S3 e o mesmo Redshift. No DuckDB, o trabalho em série levou de 1,5 a 2 vezes o
tempo da máquina de 8 vCPUs e os pools ganharam a metade: `run.ingest` das quatro 1,15 vez
(14,727 s contra 16,987 s), `materialize` das quatro 1,15 vez, `publish_delta` com `max_workers=4`
1,71 vez (15,695 s contra 26,902 s), o `stream` 1,29 vez sem trabalho e 1,44 vez com o pandas, e
as 200 consultas pequenas 1,28 vez na sessão a mais. No Redshift, que espera a rede e o servidor,
o ganho não dependeu da máquina: `run.ingest` das quatro 2,75 vezes (11,500 s contra 31,608 s), as
80 consultas pequenas 2,82 vezes na sessão a mais (1,967 s contra 5,541 s), `publish_delta` com
`max_workers=4` 1,48 vez (19,951 s contra 29,608 s) e `publish_redshift` com `max_workers=4` 2,35
vezes (11,819 s contra 27,757 s). A sonda rodou de novo às 18:55 UTC do mesmo dia, com os ganhos
de 1,02 a 1,61 vez no DuckDB e de 1,01 a 2,68 vezes no Redshift, e `publish_redshift` com
`max_workers=4` 2,40 vezes (11,133 s contra 26,750 s), com uma das três repetições em quatro
conexões em 33,135 s, acima de toda repetição em série.

Em 2026-10-09 a sonda rodou numa máquina de 4 vCPUs com 13,3 GiB disponíveis. No DuckDB, os pools
ganharam entre o que ganharam com 2 e com 8 vCPUs: `run.ingest` das quatro 1,57 vez (7,058 s contra
11,077 s), `materialize` das quatro 1,39 vez e `publish_delta` com `max_workers=4` 1,81 vez
(11,059 s contra 19,963 s); o `stream` ganhou 1,29 vez sem trabalho e 1,68 vez com o pandas, e as
200 consultas pequenas 1,54 vez na sessão a mais. No Redshift: `run.ingest` das quatro 2,84 vezes
(11,973 s contra 33,998 s), as 80 consultas pequenas 1,96 vez na sessão a mais (1,944 s contra
3,808 s), `publish_delta` com `max_workers=4` 1,37 vez (19,057 s contra 26,058 s) e
`publish_redshift` com `max_workers=4` 2,03 vezes (13,813 s contra 28,083 s), com as outras duas
repetições em quatro conexões em 32,850 s e 32,314 s.

A sonda rodou de novo às 22:09 UTC do mesmo dia, numa máquina de 8 vCPUs, e os ganhos repetiram os
de 2026-10-05. No DuckDB: `run.ingest` das quatro 2,45 vezes (4,192 s contra 10,290 s),
`materialize` das quatro 2,42 vezes e `publish_delta` com `max_workers=4` 2,08 vezes (8,209 s contra
17,044 s). No Redshift: `run.ingest` das quatro 2,68 vezes (12,443 s contra 33,405 s), as 80
consultas pequenas 1,82 vez na sessão a mais e `publish_delta` com `max_workers=4` 1,34 vez.
`publish_redshift` com `max_workers=4` levou 13,782 s numa repetição e 36,110 s e 34,213 s nas
outras duas, contra 27,657 s a 30,377 s uma tabela por vez. O tempo de cada comando da publicação,
lido pela primeira vez, põe a diferença no `INSERT` da linha de controle, o último comando antes do
`COMMIT` de cada tabela: os quatro somaram 5,42 s na repetição rápida e 70,03 s e 65,94 s nas
lentas, até 24,87 s num só, enquanto o `COPY`, o `INSERT` dos dados e o `COMMIT` levaram o mesmo nas
três.

### Como usar as threads

- **Leia o resultado grande por `stream`, com o trabalho dentro do laço.** A consulta segue enquanto
  o cliente trabalha, o primeiro lote chega antes e a memória fica nos 64 MiB de lotes mais o
  arquivo intermediário. O `query` serve ao resultado pequeno e ao trabalho que precisa da tabela
  inteira. No Redshift, o `query` passa pelo cursor do driver, que lê o resultado inteiro antes de
  devolver a primeira linha.
- **Abra `stream` e `appender` no mesmo `with`**, com a tabela de saída criada antes por
  `create_table` ou pelo `ingest` com `materialize=True`: a leitura, o trabalho e a gravação se
  sobrepõem, como no exemplo de "Rodar o pipeline no sandbox DuckDB".
- **O ganho é o trabalho que se sobrepõe**, no máximo o menor de dois tempos: o do cliente e o da
  thread da biblioteca, que roda a consulta no `stream` e grava o arquivo no `appender`. Ele cresce
  com o trabalho do cliente até igualar o da thread. O comando do `close` do `appender` roda depois
  do laço e não se sobrepõe a nada. No Redshift, onde o `UNLOAD` e o `COPY` são quase todo o tempo,
  o `stream` e o `appender` não encurtam o laço, e o `stream` poupa memória.
- **Não abra o `stream` dentro de `session()` na mesma thread.** No bloco, a consulta roda inteira
  na thread do cliente antes do primeiro lote, e passa pelo arquivo intermediário: 1,995 s contra
  1,378 s do `query`, com o primeiro lote em 1,2 s. O leitor da conexão crua no bloco,
  `connection.execute(...).to_arrow_reader(...)`, tem a memória do `stream` e quase o tempo da série
  (1,327 s), porque a consulta espera o cliente, e segura o lock da sessão até o fim do laço.
- **Passe todas as tabelas numa chamada**: `run.ingest(*tables)`, `reader.materialize(*tables)` e
  `run.publish_delta(*tables, max_workers=n)`. Um laço com uma chamada por tabela as leva uma por
  vez. O ganho para na maior tabela, e o pico de memória soma o das tabelas em curso: 39% a 46%
  acima da série na pasta local, e 11% a 13% com as tabelas no S3.
- **Dê a cada thread do cliente a sua `new_session()`** para as consultas independentes. Na sessão
  principal as threads esperam o lock, e o tempo é o da série. A sessão a mais ganha mais nas
  consultas pequenas; a consulta grande já usa todas as `threads` do DuckDB, as CPUs do processo,
  num pool que as sessões dividem. Ela não vê as tabelas temporárias da sessão principal. No
  Redshift, cada sessão a mais é outra conexão, e as consultas pequenas, que esperam a rede, ganham
  com ela mais que no DuckDB.
- **Junte as consultas pequenas numa só**, com `GROUP BY` ou uma junção, antes de levá-las a
  threads: cada chamada com um statement Core o compila em Python, cerca de 2 ms sob o GIL, e esse
  tempo não se divide entre as threads.
- **Espere o `Future` do passo de que outro depende.** Cada comando está confirmado quando volta, e
  a ordem entre os passos é do código do cliente. As faixas de `run.next_ids` não se sobrepõem entre
  threads.
- **Um pool do cliente ganha com trabalho nativo e com espera de rede.** O trabalho em Python puro
  de várias threads roda uma de cada vez, sob o GIL; ao lado das threads da biblioteca, que rodam no
  código nativo, ele ainda se sobrepõe.
- **`max_workers` da publicação no Delta ganha com as tabelas no S3**, onde o registro no log, as
  conferências, o commit e a releitura de cada tabela esperam a rede. Na pasta local ele não ganhou,
  porque o arquivo de cada partição sai da sessão principal uma tabela por vez. O padrão é 1, e cada
  tabela em curso soma a memória da sua escrita.
- **`max_workers` de `publish_redshift` ora ganha, ora perde no ambiente alvo**: das 15 repetições
  da publicação de quatro tabelas iguais em quatro conexões, de 2026-10-05 a 2026-10-09, sete
  levaram de 11,133 s a 13,813 s e oito de 31,521 s a 36,110 s, nenhuma entre os dois grupos,
  contra 26,750 s a 32,179 s das 15 repetições uma tabela por vez. Nas lentas, as transações
  esperam no `INSERT` da linha de controle, que cada uma grava no fim; a causa da espera não foi
  lida, e o runbook publica a base com `--max-workers 4`.

```python
from concurrent.futures import ThreadPoolExecutor

import pyarrow as pa
import sqlalchemy as sa

by_account = sa.select(Lancamento.id_conta, sa.func.sum(Lancamento.valor).label("total")).group_by(
    Lancamento.id_conta
)
by_operation = sa.select(Operacao.operacao, sa.func.sum(Operacao.valor).label("total")).group_by(
    Operacao.operacao
)

with Execution(db, "duckdb", "2026-08-31") as run:
    run.ingest(Lancamento.__table__, Operacao.__table__, partitions=["2026-08-31"],
               materialize=True)                                   # uma sessão a mais cada

    def summarize(statement: sa.Select) -> pa.Table:
        """Uma consulta independente, na sua sessão a mais."""
        with run.sandbox.new_session() as session:
            return session.query(statement)

    with ThreadPoolExecutor(max_workers=2) as pool:
        accounts = pool.submit(summarize, by_account)
        operations = pool.submit(summarize, by_operation)
        totals = (accounts.result(), operations.result())   # o passo seguinte usa os dois

    run.sandbox.create_table(Projetado.__table__)
    with run.sandbox.stream(sa.select(Lancamento)) as stream, \
            run.sandbox.appender(Projetado.__table__) as appender:
        for batch in stream:                 # a consulta segue enquanto o cliente trabalha
            ids = run.next_ids(Projetado.__table__, batch.num_rows)
            appender.write(project(batch, ids))   # a thread do appender grava o lote anterior
    run.audit(Projetado.__table__, ["2026-08-31"])
    run.publish_delta(Projetado.__table__, partitions=["2026-08-31"])
```

## Retenção dos arquivos removidos

Um commit que substitui uma partição tira do log os arquivos da versão anterior sem apagá-los: eles
ficam no armazenamento até o `vacuum`, e enquanto ficam a versão que os usa continua legível.
`serialize_db.delta.create_table` grava duas propriedades em toda tabela:

| Propriedade | Valor | O que controla |
| --- | --- | --- |
| `delta.deletedFileRetentionDuration` | `interval 400 days` | A idade mínima de um arquivo removido antes que o `vacuum` o apague: a janela em que toda versão continua legível, para um `restore` ou para reler a entrada de uma execução pela versão em `serialize_db_input_versions`. |
| `delta.logRetentionDuration` | `interval 3650 days` | A idade mínima dos arquivos de log que a limpeza do log preserva: o histórico que `serialize_db.delta.version_diff` lê. Com o log limpo, `version_diff` levanta `serialize_db.errors.LogUnavailable`, com a instrução de publicar a tabela inteira. |

`serialize_db.delta.vacuum_keeping_snapshots` roda o `vacuum` de uma tabela com
`retention_hours=9600`, os 400 dias, por padrão: lista os arquivos sem apagar e apaga com
`apply=True`. As versões que `serialize_db.delta.snapshot` registra em
`<ambiente>/_serialize_db/snapshots.json` continuam legíveis qualquer que seja a retenção. A chamada
vale pelo `retention_hours` informado, e a propriedade da tabela vale para quem roda o `vacuum` do
delta-rs sem esse argumento. Em regime, cada tabela guarda cerca de 13 meses (400/30) de partições
substituídas além da versão atual. A rotina mensal que chama o `vacuum` de cada tabela é
`serialize-db vacuum`, no runbook da página de `serialize_db.cli`.

**O bucket versionado.** Num bucket com versionamento, o `vacuum` não libera espaço: cada objeto
apagado vira uma versão não corrente, invisível à listagem e cobrada até que uma regra de ciclo de
vida `NoncurrentVersionExpiration` a expire. O papel do projeto no ambiente alvo não lê a
configuração de ciclo de vida do bucket, e `probes/bucket.py` (`BK-14`) conta as versões não
correntes acumuladas sob a raiz. A regra é pedida a quem administra o bucket, com o prefixo da
raiz, junto com `AbortIncompleteMultipartUpload`, que apaga as partes de um envio interrompido; os
dias abaixo são exemplos:

```json
{
  "Rules": [
    {
      "ID": "serialize-db-versoes-nao-correntes",
      "Filter": {"Prefix": "prefixo/da/raiz/"},
      "Status": "Enabled",
      "NoncurrentVersionExpiration": {"NoncurrentDays": 30},
      "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 7}
    }
  ]
}
```

`aws s3api put-bucket-lifecycle-configuration` substitui a configuração inteira do bucket, então a
regra entra ao lado das que já existem. Os `NoncurrentDays` somam-se à retenção: durante eles, quem
administra o bucket ainda recupera um arquivo que o `vacuum` apagou.

**Como alterar a retenção.**

- A janela de um `vacuum`: o argumento `retention_hours` de
  `serialize_db.delta.vacuum_keeping_snapshots`, por exemplo `24 * 90` para 90 dias.
- A propriedade de uma tabela existente, que os outros leitores e escritores Delta respeitam: o
  comando abaixo grava um commit sem dados, que `version_diff` não conta como partição alterada.
- As tabelas novas nascem com os valores da tabela acima, fixos em `serialize_db.delta`: outro
  padrão é uma mudança da biblioteca, e as tabelas já criadas mudam pelo comando abaixo.

```python
delta.open_table(uri, storage).alter.set_table_properties(
    {"delta.deletedFileRetentionDuration": "interval 90 days"})
```

Uma retenção menor libera espaço antes, desde que o bucket tenha a regra, e encurta a janela em que
uma versão intermediária continua legível.

## Tabela de mapeamento de tipos

O tipo SQLAlchemy de cada coluna determina o tipo Arrow do contrato, o tipo Delta e o nome do tipo
no DDL que `serialize_db.schema.ddl` gera para cada motor. A busca é por `isinstance`, e uma
subclasse segue a linha do tipo de que deriva: `Unicode(n)` e `CHAR(n)` a de `String(n)`,
`UnicodeText` a de `Text`, `TIMESTAMP` a de `DateTime`, `UUID` a de `Uuid` e `DOUBLE_PRECISION` a
de `Double`. O `Enum` deriva de `String` e fica fora do contrato, porque nada confere a lista de
valores. Um tipo fora do contrato é listado por `check_models` e recusado por `arrow_schema`.

| SQLAlchemy | Arrow | Delta | DuckDB | Redshift | Observação |
| --- | --- | --- | --- | --- | --- |
| `SmallInteger` | `int16` | `short` | `SMALLINT` | `SMALLINT` | O Parquet grava `int16` no tipo físico `INT32`. |
| `Integer` | `int32` | `integer` | `INTEGER` | `INTEGER` | |
| `BigInteger` | `int64` | `long` | `BIGINT` | `BIGINT` | O tipo das chaves. Tipos sem sinal do Arrow e do DuckDB ficam fora do contrato. |
| `Boolean` | `bool` | `boolean` | `BOOLEAN` | `BOOLEAN` | |
| `Double` | `float64` | `double` | `DOUBLE` | `DOUBLE PRECISION` | Entra como chega, sem arredondamento, com `NaN` e infinito; numa partição com valor não finito, a coluna fica sem mínimo e máximo no log Delta. A documentação do `UNLOAD` avisa que descarregar e recarregar pode perder precisão; o pacote faz os dois em Parquet, que guarda o valor binário, e a perda não foi medida. O `COPY` do DuckDB, que grava os arquivos do `export_partition` do motor DuckDB, de `import_table`, de `rewrite` e de `export_parquet(mode="rewrite")`, perde o sinal do zero: num grupo de linhas com `-0.0` e `0.0`, todos saem com o sinal do primeiro gravado (DuckDB 1.5.5, 2026-09-25). O escritor do delta-rs, de `publish_partition` e de `compact`, e o Redshift guardam o sinal, que só aparece no texto do valor, em `math.copysign` e em `1 / x` no DuckDB e no PyArrow (`-inf` no lugar de `inf`), nunca numa comparação ou numa soma. |
| `Numeric(p, s)` | `decimal128(p, s)` | `decimal(p,s)` | `DECIMAL(p, s)` | `DECIMAL(p, s)` | `p` de 1 a 38 e `s` de 0 a `p`, até 37, o maior `s` do Redshift; fora disso `check_models` lista a violação. Sem `p` a precisão é 18, e sem `s` a escala é 0, os padrões do `DECIMAL` do Redshift. O tipo físico no Parquet varia com o escritor, e os leitores leem todos: o DuckDB e o delta-rs gravam `INT32` até 9 dígitos, `INT64` até 18 e `FIXED_LEN_BYTE_ARRAY` acima, e o `UNLOAD` do Redshift e o PyArrow gravaram `DECIMAL(18, 2)` em `FIXED_LEN_BYTE_ARRAY`. |
| `String(n)` | `string` | `string` | `VARCHAR(n)` | `VARCHAR(n)` | `n` em bytes no Redshift, e é assim que `cast`, a auditoria e a carga inicial medem o texto; o DuckDB aceita o comprimento e o ignora. `String` sem `n` é violação, e também `Unicode`, `VARCHAR` e `CHAR` sem `n`. |
| `Text` | `string` | `string` | `VARCHAR` | `VARCHAR(65535)` | O texto sem `n`, e o `n` de `Text(n)` é ignorado; o teto é o do `VARCHAR` do Redshift, que `cast`, a auditoria e a carga inicial medem; `TEXT` no Redshift seria `VARCHAR(256)`. |
| `Date` | `date32` | `date` | `DATE` | `DATE` | |
| `DateTime` | `timestamp[us]` | `timestamp_ntz` | `TIMESTAMP` | `TIMESTAMP` | Microssegundos; um nanossegundo não nulo e um `timestamp` com fuso são recusados por `cast`. O `timestamp_ntz` põe a tabela no protocolo com o recurso `timestampNtz` (leitor 3, escritor 7), que o leitor Delta precisa ter. O `UNLOAD` do Redshift grava `INT96`, que o delta-rs e o `delta_scan` leem em microssegundos. |
| `DateTime(timezone=True)` | `timestamp[us, tz=UTC]` | `timestamp` | `TIMESTAMPTZ` | `TIMESTAMPTZ` | Sempre em UTC; outro fuso entra no mesmo instante, e um `timestamp` sem fuso é recusado por `cast`. |
| `Uuid` | `string` | `string` | `VARCHAR(36)` | `VARCHAR(36)` | O contrato guarda o UUID como texto; o Redshift não tem o tipo. O cliente passa o `uuid.UUID`, que o PyArrow e o pandas inferem como `arrow.uuid` e `cast` converte no texto de `str(valor)`, ou o próprio texto; `cast` e a carga inicial recusam e a auditoria reprova o texto acima de 36 bytes. |
| `JSON` | `string` | `string` | `JSON` | `SUPER` | O documento entra serializado (`json.dumps`); `cast` recusa `struct`, `list` e `map`, e o documento acima de 65.535 bytes, o teto do `VARCHAR(65535)` em que o Redshift o carrega antes do `JSON_PARSE` para `SUPER`. O `ingest` do DuckDB com `materialize=True` põe a coluna em `JSON` pela DDL, e um documento malformado no Delta faz esse `ingest` falhar; a view do `ingest` e o leitor Delta trazem a coluna do `delta_scan` em `VARCHAR`, e as funções JSON do DuckDB leem os dois tipos. O `with_variant(SUPER(), "redshift")` do `sqlalchemy-redshift` no modelo é opcional. |
| `Float`, `Time`, `Interval`, `LargeBinary`, `ARRAY`, `Enum` | | | | | Fora do contrato. |

Cada campo Arrow leva a nulidade da coluna, o comentário em `metadata` e `PARQUET:field_id` pela
posição; o esquema leva o nome da tabela em `serialize_db_table`. O esquema Delta é derivado do
Arrow pelo delta-rs, com os comentários preservados e sem o `PARQUET:field_id`: com ele no esquema
Delta, o `delta_scan` do DuckDB lê toda coluna como nula.

Uma coluna do contrato volta de `query` e de `stream` no tipo Arrow da tabela. No motor Redshift,
`serialize_db.engine.redshift.schema_from_row_description` dá o tipo de cada coluna do resultado
pelo driver, com o `SUPER` em `string` e o `NUMERIC` com a precisão e a escala do `type_modifier`,
e recusa com `serialize_db.errors.SandboxError` o tipo que não lista, como `TIME`; o motor DuckDB
devolve o `TIMESTAMPTZ` com o fuso da sessão (`TimeZone`), que `cast` leva a UTC no mesmo instante.
