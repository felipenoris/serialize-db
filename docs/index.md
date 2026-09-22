Os dois motores executam o pipeline sobre uma cópia dos dados, e o Redshift é também o destino de
publicação. Os modelos SQLAlchemy do cliente são o contrato de esquema: a
biblioteca deriva deles o esquema Arrow e Delta, o DDL de cada motor, a conversão dos lotes de
dados e a conferência dos próprios modelos.

Esta página explica o funcionamento geral do pacote, traz o tutorial de uso e a tabela de
mapeamento de tipos. A referência de cada módulo está no menu: `serialize_db.schema`,
`serialize_db.sql`, `serialize_db.errors` e `serialize_db.cli`.

## Como o pacote funciona

- **O contrato é o modelo.** O cliente declara as tabelas em SQLAlchemy (`DeclarativeBase`,
  `mapped_column`), com comentários de documentação opcionais nas tabelas e colunas e as opções físicas em
  `Table.info["serialize_db"]`. O pacote não contém modelo algum; ele recebe `Base.metadata`.
- **A fonte da verdade são as tabelas Delta Lake**, em disco local ou no S3. O esquema Delta de cada tabela sai do modelo.
- **O DuckDB e o Redshift são sandboxes**: cada execução cria as tabelas de que precisa a partir do
  DDL do modelo, carrega os dados, roda o pipeline, audita e publica. O DDL de cada motor sai do
  modelo pela tabela de tipos abaixo, sem os dialetos do SQLAlchemy.
- **Os dados atravessam a fronteira em lotes Arrow** (`pyarrow.RecordBatch`, `pyarrow.Table` ou
  `pyarrow.RecordBatchReader`), e `serialize_db.schema.cast` leva cada lote ao esquema do contrato
  ou o recusa com a instrução ao cliente.
- **O SQL do pipeline vira texto gerado por motor.** Cada statement Core do pipeline sai como
  texto do DuckDB e do Redshift por `serialize_db.sql.render`, com as constantes embutidas, a
  partição como parâmetro `:nome` e cada tabela do contrato com o sentinela `{prefix}` no nome,
  que a execução troca pelo prefixo do sandbox. O pipeline versiona o texto e o executa no lugar
  de compilar o statement a cada execução.
- **Todo identificador que a biblioteca emite vai entre aspas duplas**: nomes de coluna como `to` e
  `timestamp` são palavras reservadas do DuckDB e do Redshift.

O que já existe são o módulo de esquema, `serialize_db.schema`, e o de texto SQL,
`serialize_db.sql`, com a linha de comando `serialize-db schema` e `serialize-db sql`. O
armazenamento Delta, os motores, a auditoria, a execução e a publicação
são as etapas seguintes do plano, na pasta `plan/` do repositório.

## Instalação

No repositório, o `uv` instala o Python 3.13, o pacote e as dependências:

```shell
uv sync --group dev
```

O pacote depende de `sqlalchemy`, `pyarrow`, `deltalake`, `duckdb` e dos dialetos `duckdb-engine`
e `sqlalchemy-redshift`, que compilam o texto SQL de cada motor, nas versões fixadas em
`pyproject.toml`.

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
        sa.Index("ix_operacoes_data_operacao", "data", "operacao", unique=True),
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
| `partition_by` | A coluna de partição, uma no máximo, de texto `String(n)`, no fim da tabela; o valor é o nome da pasta da partição, sem `/`, `=` nem espaço. Na base atual é a data em `AAAA-MM-DD`. |
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
inclusive); `Identity`; `String` sem comprimento (declare `String(n)` ou `Text`); chave estrangeira `DEFERRABLE`; `partition_by` sem a coluna ou com a coluna fora de
`String(n)`, `partition_source` que a tabela não tem ou sem `partition_by`; tabela sem chave
primária e sem `keys`.

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

O que `cast` recusa, com `serialize_db.errors.ContractError` e a instrução ao cliente na mensagem:
nulo em coluna `NOT NULL`, `double` fora da escala de um `Numeric`, `timestamp` com hora numa
coluna `Date`, documento JSON como `struct`, texto acima de `String(n)` (medido em bytes, como o
`VARCHAR(n)` do Redshift), texto acima de 65.535 bytes numa coluna `Text`, escala perdida num decimal, nanossegundo não nulo num timestamp, estouro
de inteiro e um lote sem coluna alguma do contrato. Um `double` entra numa coluna `Numeric` só
quando `round` o devolve igual; numa coluna `Double` ele entra como chega.

### Versionar os arquivos de esquema

`serialize_db.schema.schema_files` gera, por tabela, o esquema Delta em JSON canônico e o
`CREATE TABLE` de cada motor. O pipeline versiona esses arquivos no seu repositório, e o diff contra
a geração nova mostra o que uma mudança de modelo altera em cada motor:

```shell
serialize-db schema write --metadata pipeline.models:Base.metadata schema/
serialize-db schema check --metadata pipeline.models:Base.metadata schema/
```

`--metadata` recebe `modulo:atributo`, o caminho importável do `MetaData`. O `check` sai com 0 quando
os arquivos estão atualizados, 1 com o diff impresso quando há diferença, e 2 no erro de uso. Em
Python, `serialize_db.schema.write_schema_files` e `serialize_db.schema.check_schema_files` fazem o
mesmo.

### Gerar o texto SQL de cada motor

Um statement Core do pipeline vira texto do DuckDB e do Redshift por `serialize_db.sql.render`. A
partição de referência entra por `serialize_db.sql.param`, que chega ao texto como `:nome`; as
constantes ficam embutidas, e cada tabela do contrato sai com o sentinela `{prefix}` no nome,
dentro das aspas, que a execução troca pelo prefixo do sandbox:

```python
from serialize_db import sql

operations = Operacao.__table__
statement = (
    sa.select(operations.c.operacao, sa.func.sum(operations.c.valor).label("total"))
    .where(operations.c.data_str == sql.param("data_str", sa.String(10)),
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

Um `bindparam` sem valor é recusado com `serialize_db.errors.SqlError`, porque o compilador o
renderizaria como `NULL`. Na execução, `serialize_db.sql.bind` reescreve o marcador para o estilo
do motor (`$nome` no DuckDB, `:nome` no `redshift_connector` com `paramstyle = "named"`) e confere
o dicionário de parâmetros; toda região citada passa intacta, e um texto que ainda traz o sentinela
é recusado:

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

## Tabela de mapeamento de tipos

O tipo SQLAlchemy de cada coluna determina o tipo Arrow do contrato, o tipo Delta e o nome do tipo
em cada motor. Um tipo fora desta tabela é recusado por `check_models` e por `arrow_schema`.

| SQLAlchemy | Arrow | Delta | DuckDB | Redshift | Observação |
| --- | --- | --- | --- | --- | --- |
| `SmallInteger` | `int16` | `short` | `SMALLINT` | `SMALLINT` | O Parquet grava `int16` no tipo físico `INT32`. |
| `Integer` | `int32` | `integer` | `INTEGER` | `INTEGER` | |
| `BigInteger` | `int64` | `long` | `BIGINT` | `BIGINT` | O tipo das chaves. Tipos sem sinal do Arrow e do DuckDB ficam fora do contrato. |
| `Boolean` | `bool` | `boolean` | `BOOLEAN` | `BOOLEAN` | |
| `Double` | `float64` | `double` | `DOUBLE` | `DOUBLE PRECISION` | Entra como chega, sem arredondamento. Descarregar e recarregar pelo Redshift pode perder precisão. |
| `Numeric(p, s)` | `decimal128(p, s)` | `decimal(p, s)` | `DECIMAL(p, s)` | `DECIMAL(p, s)` | `p` até 38. O delta-rs e o DuckDB gravam `DECIMAL(18, 2)` no tipo físico `INT64`; o PyArrow, em `FIXED_LEN_BYTE_ARRAY`. |
| `String(n)` | `string` | `string` | `VARCHAR(n)` | `VARCHAR(n)` | `n` em bytes no Redshift, e é assim que `cast` mede o texto; o DuckDB aceita o comprimento e o ignora. `String` sem `n` é violação. |
| `Text` | `string` | `string` | `VARCHAR` | `VARCHAR(65535)` | O texto sem `n`; o teto é o do `VARCHAR` do Redshift, que o `cast` mede; `TEXT` no Redshift seria `VARCHAR(256)`. |
| `Date` | `date32` | `date` | `DATE` | `DATE` | |
| `DateTime` | `timestamp[us]` | `timestamp_ntz` | `TIMESTAMP` | `TIMESTAMP` | Microssegundos; um nanossegundo não nulo é recusado por `cast`. |
| `DateTime(timezone=True)` | `timestamp[us, tz=UTC]` | `timestamp` | `TIMESTAMPTZ` | `TIMESTAMPTZ` | Sempre em UTC. |
| `Uuid` | `string` | `string` | `VARCHAR(36)` | `VARCHAR(36)` | O contrato guarda o UUID como texto; o Redshift não tem o tipo. |
| `JSON` | `string` | `string` | `JSON` | `SUPER` | O documento entra serializado (`json.dumps`); `cast` recusa `struct`, `list` e `map`. O `with_variant(SUPER(), "redshift")` do `sqlalchemy-redshift` no modelo é opcional. |
| `Float`, `LargeBinary`, `ARRAY`, `Interval` | | | | | Fora do contrato. |

Cada campo Arrow leva a nulidade da coluna, o comentário em `metadata` e `PARQUET:field_id` pela
posição; o esquema leva o nome da tabela em `serialize_db_table`. O esquema Delta é derivado do Arrow
pelo delta-rs, com os comentários preservados e sem o `PARQUET:field_id`: com ele no esquema Delta, o
`delta_scan` do DuckDB lê toda coluna como nula.
