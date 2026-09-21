Os dois motores executam o pipeline sobre uma cópia dos dados, e o Redshift é também o destino de
publicação. Os modelos SQLAlchemy do cliente são o contrato de esquema: a
biblioteca deriva deles o esquema Arrow e Delta, o DDL de cada motor, a conversão dos lotes de
dados e a conferência dos próprios modelos.

Esta página explica o funcionamento geral do pacote, traz o tutorial de uso e a tabela de
mapeamento de tipos. A referência de cada módulo está no menu: `serialize_db.schema`,
`serialize_db.errors` e `serialize_db.cli`.

## Como o pacote funciona

- **O contrato é o modelo.** O cliente declara as tabelas em SQLAlchemy (`DeclarativeBase`,
  `mapped_column`), com comentários em toda tabela e coluna e as opções físicas em
  `Table.info["serialize_db"]`. O pacote não contém modelo algum; ele recebe `Base.metadata`.
- **A fonte da verdade são as tabelas Delta Lake**, em disco local ou no S3, uma partição por data
  (`AAAA-MM-DD`). O esquema Delta de cada tabela sai do modelo.
- **O DuckDB e o Redshift são sandboxes**: cada execução cria as tabelas de que precisa a partir do
  DDL do modelo, carrega os dados, roda o pipeline, audita e publica. O DDL de cada motor sai do
  modelo pela tabela de tipos abaixo, sem os dialetos do SQLAlchemy.
- **Os dados atravessam a fronteira em lotes Arrow** (`pyarrow.RecordBatch`, `pyarrow.Table` ou
  `pyarrow.RecordBatchReader`), e `serialize_db.schema.cast` leva cada lote ao esquema do contrato
  ou o recusa com a instrução ao cliente.
- **Todo identificador que a biblioteca emite vai entre aspas duplas**: nomes de coluna como `to` e
  `timestamp` são palavras reservadas do DuckDB e do Redshift.

O que já existe é o módulo de esquema, `serialize_db.schema`, com a linha de comando
`serialize-db schema`. O armazenamento Delta, os motores, a auditoria, a execução e a publicação
são as etapas seguintes do plano, na pasta `plan/` do repositório.

## Instalação

No repositório, o `uv` instala o Python 3.13, o pacote e as dependências:

```
uv sync --group dev
```

O pacote depende de `sqlalchemy`, `pyarrow` e `deltalake`, nas versões fixadas em
`pyproject.toml`; o DuckDB entra com o motor.

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
| `partition_by` | A coluna de partição, uma no máximo, `String(10)` em `AAAA-MM-DD`, no fim da tabela. |
| `partition_source` | A coluna de data de que a coluna de partição deriva (`strftime('%Y-%m-%d')`). |
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
inclusive); `Identity`; `String` sem comprimento (declare `String(n)` ou `Text`); tabela ou coluna
sem comentário; chave estrangeira `DEFERRABLE`; `partition_by` sem a coluna, com a coluna fora de
`String(10)` ou sem `partition_source`; tabela sem chave primária e sem `keys`.

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
e `serialize_db.schema.quoted` cita um identificador.

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
`VARCHAR(n)` do Redshift), escala perdida num decimal, nanossegundo não nulo num timestamp, estouro
de inteiro e um lote sem coluna alguma do contrato. Um `double` entra numa coluna `Numeric` só
quando `round` o devolve igual; numa coluna `Double` ele entra como chega.

### Versionar os arquivos de esquema

`serialize_db.schema.schema_files` gera, por tabela, o esquema Delta em JSON canônico e o
`CREATE TABLE` de cada motor. O pipeline versiona esses arquivos no seu repositório, e o diff contra
a geração nova mostra o que uma mudança de modelo altera em cada motor:

```
serialize-db schema write --metadata pipeline.models:Base.metadata schema/
serialize-db schema check --metadata pipeline.models:Base.metadata schema/
```

`--metadata` recebe `modulo:atributo`, o caminho importável do `MetaData`. O `check` sai com 0 quando
os arquivos estão atualizados, 1 com o diff impresso quando há diferença, e 2 no erro de uso. Em
Python, `serialize_db.schema.write_schema_files` e `serialize_db.schema.check_schema_files` fazem o
mesmo.

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
| `Text` | `string` | `string` | `VARCHAR` | `VARCHAR(65535)` | O texto sem limite; `TEXT` no Redshift seria `VARCHAR(256)`. |
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
