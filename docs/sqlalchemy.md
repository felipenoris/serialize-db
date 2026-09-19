# SQLAlchemy

O SQLAlchemy é a camada que descreve o esquema do projeto e gera o SQL para os dois bancos. Ele tem
duas partes: o Core, com `MetaData`, `Table`, `Column`, os tipos e os construtores de comandos, e o
ORM, com as classes mapeadas e a `Session`. Os modelos ORM são o [contrato de esquema](guia.md) do
projeto: deles derivam o DDL do DuckDB e do Redshift, o esquema Arrow dos arquivos Parquet e as
auditorias. Este documento resume os conceitos usados pela biblioteca, as opções de customização e o
comportamento com Redshift, DuckDB e Parquet.

As afirmações vêm da documentação oficial da versão 2.0, consultada em 2026-09-18. Os exemplos foram
executados com SQLAlchemy 2.0.54, duckdb_engine 0.17.0 sobre DuckDB 1.5.5 e sqlalchemy-redshift 1.0.0;
os comandos do Redshift foram apenas compilados, sem conexão a um cluster. Os exemplos com Arrow e
Delta Lake rodaram em 2026-09-19 com PyArrow 25.0.1 e deltalake 1.6.4.

## Esquema, metadata e reflexão

### MetaData, Table e Column

`MetaData` é a coleção de tabelas. Cada `Table` recebe nome, a `MetaData`, as colunas e as restrições;
cada `Column` recebe nome, tipo e opções (`primary_key`, `nullable`, `default`, `server_default`,
`comment`, `info`). A coleção conhece a ordem de dependência por chaves estrangeiras
(`MetaData.sorted_tables`) e emite o DDL nessa ordem.

```python
from sqlalchemy import MetaData, Table, Column, BigInteger, Date, Numeric, String, PrimaryKeyConstraint

metadata = MetaData()
operations = Table(
    "operacoes", metadata,
    Column("id_operacao", BigInteger, nullable=False),
    Column("data_ref", Date, nullable=False),
    Column("id_cliente", BigInteger, nullable=False, comment="Chave do cliente"),
    Column("valor", Numeric(18, 2), nullable=False),
    Column("descricao", String(200)),
    PrimaryKeyConstraint("id_operacao", "data_ref", name="operacoes_pk"),
    comment="Operações do mês",
    info={"serialize_db": {"sort_key": ["data_ref", "id_operacao"]}},
)
```

Os tipos genéricos (`Integer`, `BigInteger`, `Numeric(precisao, escala)`, `String(n)`, `Text`,
`Boolean`, `Date`, `DateTime(timezone=...)`, `Uuid`, `JSON`, `LargeBinary`) são traduzidos por cada
dialeto na compilação: `Numeric(18, 2)` vira `NUMERIC(18, 2)` nos dois (`DECIMAL(18,2)` no catálogo
do DuckDB), `String(200)` vira `VARCHAR(200)` nos dois, e o catálogo do DuckDB descarta o
comprimento. Os tipos específicos ficam em `sqlalchemy.dialects.<dialeto>` e nos dialetos externos
(`sqlalchemy_redshift.dialect.SUPER`, `TIMESTAMPTZ`). `type_.with_variant(other_type, "<dialeto>")` troca o
tipo num dialeto só. A [tabela de tipos do contrato](schema.md) fixa a correspondência com Arrow,
Delta, DuckDB e Redshift.

Restrições e índices são objetos: `PrimaryKeyConstraint`, `ForeignKey` na coluna ou
`ForeignKeyConstraint` na tabela, `UniqueConstraint`, `CheckConstraint`, `Index`. Desde a versão
2.0, `restricao.ddl_if(dialect="redshift")` limita a emissão a um dialeto, o que implementa a
[política de restrições](schema.md) (chaves no Redshift, nenhuma no DuckDB) sem dois modelos:

```python
PrimaryKeyConstraint("id_operacao", "data_ref").ddl_if(dialect="redshift")
```

### Restrições adiáveis

Uma restrição comum é verificada ao fim de cada comando. `DEFERRABLE` permite adiar a verificação
para o fim da transação; `NOT DEFERRABLE`, o padrão, proíbe o adiamento. Entre as adiáveis,
`INITIALLY IMMEDIATE`, o padrão, verifica após cada comando, e `INITIALLY DEFERRED` verifica só no
commit; `SET CONSTRAINTS {ALL | nome} {DEFERRED | IMMEDIATE}` muda o modo dentro da transação, e a
mudança para `IMMEDIATE` verifica na hora o que estava pendente. No PostgreSQL, só `UNIQUE`,
`PRIMARY KEY`, `EXCLUDE` e `FOREIGN KEY` aceitam a cláusula; `NOT NULL` e `CHECK` são sempre
imediatas, e uma restrição adiável não serve de árbitro em `INSERT ... ON CONFLICT`.

O adiamento serve a linhas que se referem a linhas ainda não gravadas na mesma transação: uma tabela
de ligação carregada antes das tabelas que ela referencia, duas linhas que apontam uma para a outra,
uma hierarquia em que pai e filho entram no mesmo lote, ou a troca de dois valores únicos entre
linhas. A restrição vale no commit, não a cada comando.

No SQLAlchemy, `deferrable` (booleano) e `initially` (texto) existem em `ForeignKey` e em todas as
classes de restrição (`ForeignKeyConstraint`, `PrimaryKeyConstraint`, `UniqueConstraint`,
`CheckConstraint`) e só produzem DDL; o banco decide quais restrições aceitam a cláusula. A
declaração

```python
ForeignKey("cad_contas.id_conta", deferrable=True, initially="DEFERRED")
```

compila, nos dois dialetos do projeto, para
`FOREIGN KEY(id_conta) REFERENCES cad_contas (id_conta) DEFERRABLE INITIALLY DEFERRED`. A unidade de
trabalho do ORM não muda com a cláusula: ela continua ordenando os `INSERT` pelas dependências entre
tabelas, e o `SET CONSTRAINTS` fica a cargo da aplicação, em `text()`. Para os ciclos que o
adiamento costuma resolver, o SQLAlchemy tem mecanismos próprios: `use_alter=True` na
`ForeignKeyConstraint` emite a restrição por `ALTER TABLE ... ADD CONSTRAINT` depois das duas tabelas
(e exige `name` para o `DROP`), e `relationship(..., post_update=True)` grava as duas linhas com
`INSERT` e fecha a ligação com um `UPDATE`, na tabela com chave para si mesma ou em duas tabelas que
se referenciam.

Nos bancos do projeto a cláusula não tem efeito útil:

| Banco | Comportamento verificado |
| --- | --- |
| DuckDB 1.5.5 | Na forma de restrição de tabela que o SQLAlchemy emite (`FOREIGN KEY (...) REFERENCES ... DEFERRABLE INITIALLY DEFERRED`, `UNIQUE (...) DEFERRABLE ...`), a cláusula é aceita e descartada: `duckdb_constraints()` mostra a chave sem ela, e a verificação é imediata. Na forma de coluna (`a_id BIGINT REFERENCES a (id) DEFERRABLE ...`) e em `PRIMARY KEY ... DEFERRABLE`, falha com `Constraint not implemented!`; `SET CONSTRAINTS` é erro de sintaxe; `ALTER TABLE ... ADD CONSTRAINT` falha com `No support for that ALTER TABLE option yet!`, então `use_alter=True` também derruba o `create_all`. |
| Redshift | A sintaxe do `CREATE TABLE` não tem `DEFERRABLE` nem `INITIALLY`, e chaves primárias, únicas e estrangeiras são informativas, nunca verificadas. Se o parser aceita e ignora a cláusula fica pendente da prova de conceito. |

Os modelos em `src/serialize_db/model/` declaram `deferrable=True, initially='DEFERRED'` em todas as
chaves estrangeiras de `model_base_contabil.py` e `model_base_gerencial.py`, inclusive nas compostas,
e em nenhuma de `model_db_projetado.py`. No DuckDB o `create_all` passa, porque a cláusula é
descartada; no Redshift ela não existe. A [política de restrições](schema.md) dispensa a cláusula: no sandbox as chaves
estrangeiras ficam de fora e a auditoria verifica a integridade referencial com o mês inteiro
carregado, antes da publicação, que é a verificação adiada feita pelo próprio pipeline; no Redshift a
chave é declarada só quando auditada, sem `deferrable`.

O nome do esquema vai em `Table.schema` ou em `MetaData(schema=...)`; `BLANK_SCHEMA` exclui uma tabela
do padrão. A opção de execução `schema_translate_map` troca nomes de esquema por conexão, útil quando
cada execução do pipeline tem o próprio esquema (`execucao_abc123`) e os modelos declaram um nome
lógico. Prefixos no nome da tabela, o caminho quando só há um esquema no Redshift, exigem gerar o
nome em `__tablename__`.

### DDL

`metadata.create_all(engine)` verifica cada tabela e cria as ausentes, com sequências e restrições;
`drop_all` remove na ordem inversa; `Table.create(engine, checkfirst=True)` e `Table.drop` agem numa
tabela. Alterações de esquema ficam fora do SQLAlchemy: `ALTER TABLE` passa por `text()` ou pelo
`DDL()`, e a ferramenta de migração é o Alembic.

O DDL também pode ser gerado como texto, sem conexão, o que permite versionar um arquivo por backend
e comparar no teste:

```python
from sqlalchemy.schema import CreateTable
from sqlalchemy_redshift.dialect import RedshiftDialect_redshift_connector
import duckdb_engine

sql_redshift = str(CreateTable(operations).compile(dialect=RedshiftDialect_redshift_connector()))
sql_duckdb = str(CreateTable(operations).compile(dialect=duckdb_engine.Dialect()))
```

Os construtores de DDL (`CreateTable`, `DropTable`, `CreateSequence`, `CreateIndex`,
`SetTableComment`) são `ExecutableDDLElement` e aceitam `execute_if(dialect=..., callable_=...)`. Os
eventos `before_create`, `after_create`, `before_drop` e `after_drop` de `MetaData` e `Table`
recebem a conexão e permitem emitir DDL adicional:

```python
from sqlalchemy import event, DDL

event.listen(
    operations, "after_create",
    DDL("COMMENT ON TABLE operacoes IS 'Operações do mês'").execute_if(dialect="redshift"),
)
```

A tabela do Delta Lake, a fonte da verdade do projeto ([`delta.md`](delta.md)), nasce sem DDL em SQL.
`DeltaTable.create` recebe o esquema Arrow derivado do modelo (`arrow_schema`, definida na seção
sobre arquivos Parquet, aplicada ao modelo `Operacao` da seção sobre o mapeamento declarativo), e os
metadados do modelo chegam à tabela: `Table.name` vira o nome, `Table.comment` vira a descrição e os
metadados de campo do Arrow, como o `comment` da coluna, ficam no esquema Delta. A coluna de partição
`mes` é uma coluna comum do modelo. No S3, o URI `s3://...` acompanha `storage_options`.

```python
# Cria a tabela Delta a partir do modelo: esquema Arrow do contrato, comentários, partição e descrição.
import pyarrow as pa
from deltalake import DeltaTable

table = Operacao.__table__
schema = pa.schema([
    field.with_metadata({**field.metadata, "comment": column.comment}) if column.comment else field
    for field, column in zip(arrow_schema(Operacao), table.columns)
])
delta_table = DeltaTable.create(
    "lago/operacoes", schema, mode="ignore", partition_by=["mes"],
    name=table.name, description=table.comment,
    configuration={"delta.checkpointInterval": "10"},
)
print(delta_table.version(), delta_table.metadata().name, delta_table.metadata().partition_columns)
print(pa.schema(delta_table.schema().to_arrow()).field("id_cliente").metadata)
```

Saída:

```
0 operacoes ['mes']
{b'PARQUET:field_id': b'3', b'comment': b'Chave do cliente'}
```

`mode="ignore"` torna a criação idempotente: a segunda chamada devolveu a mesma versão 0.
`delta_table.schema().to_arrow()` devolve um esquema arro3, que `pa.schema` converte; comparado ao
esquema enviado com `check_metadata=True`, ele é igual. Uma `CheckConstraint` do modelo entra com
`delta_table.alter.add_constraint({nome: sqltext})` e é gravada como `delta.constraints.<nome>`
(`valor >= 0` foi gravada como `valor >= '0'::decimal(18, 2)`). Na reconciliação de esquema descrita
em `delta.md`, `delta_table.alter.add_columns` exige `deltalake.schema.Field`; um `pyarrow.Field` é
recusado com `'Field' object is not an instance of 'Field'`.

### Reflexão

`Table("operacoes", metadata, autoload_with=engine)` lê colunas, tipos, nulidade, chaves e
comentários do banco; colunas passadas explicitamente sobrescrevem as refletidas, o que serve para
impor tipos ou chaves que o banco não declara (views, por exemplo). `metadata.reflect(bind=engine,
schema=...)` reflete todas as tabelas de um esquema, e `inspect(engine)` expõe os métodos
individuais: `get_table_names`, `get_columns`, `get_pk_constraint`, `get_foreign_keys`,
`get_unique_constraints`, `get_indexes`, `get_table_comment` e, no dialeto do Redshift,
`get_table_options`, que devolve `redshift_diststyle`, `redshift_distkey`, `redshift_sortkey` e
`redshift_interleaved_sortkey`.

A reflexão devolve o que o dialeto sabe extrair. No duckdb_engine 0.17.0, `get_pk_constraint`
devolve vazio e índices não são refletidos, embora a função `duckdb_constraints()` do DuckDB liste a
chave; colunas, tipos e comentários voltam corretos. A comparação entre o modelo e o banco, uma das
auditorias do projeto, precisa de uma consulta ao catálogo para as chaves no DuckDB.

A auditoria compara os tipos pela correspondência Arrow (`arrow_type`, definida na seção sobre
arquivos Parquet), o que ignora o comprimento de `String(n)` que o catálogo do DuckDB descarta, e
busca a chave no catálogo:

```python
# Compara a tabela refletida do DuckDB com o contrato; a chave vem do catálogo, não da reflexão.
from sqlalchemy import MetaData, Table, create_engine, inspect, text

engine = create_engine("duckdb:///:memory:")
metadata.create_all(engine)
reflected = Table("operacoes", MetaData(), autoload_with=engine)
mismatches = []
for c in operations.columns:
    r = reflected.columns.get(c.name)
    if r is None:
        mismatches.append(f"{c.name}: ausente no banco")
    elif arrow_type(r.type) != arrow_type(c.type) or r.nullable != c.nullable:
        mismatches.append(f"{c.name}: banco {r.type}, contrato {c.type}")
mismatches += [f"{c.name}: fora do contrato" for c in reflected.columns if c.name not in operations.columns]
print(mismatches)
print(inspect(engine).get_pk_constraint("operacoes"))
with engine.connect() as conn:
    print(conn.execute(text(
        "SELECT constraint_column_names FROM duckdb_constraints() "
        "WHERE table_name = 'operacoes' AND constraint_type = 'PRIMARY KEY'"
    )).scalar())
```

Saída:

```
[]
{'name': None, 'constrained_columns': []}
['id_operacao', 'data_ref']
```

Depois de `ALTER TABLE operacoes ALTER COLUMN valor TYPE DOUBLE` e de
`ALTER TABLE operacoes ADD COLUMN canal VARCHAR`, a lista passou a
`['valor: banco FLOAT, contrato NUMERIC(18, 2)', 'canal: fora do contrato']`.

### Customização do comportamento

| Mecanismo | Uso |
| --- | --- |
| `Table.info`, `Column.info` | Dicionários livres, guardados com o objeto e ignorados pelo DDL. É o canal para metadados da aplicação, lidos por eventos e ganchos de compilação. |
| `Table.comment`, `Column.comment` | Emitidos como `COMMENT ON` pelos dialetos que suportam comentários; refletidos de volta. |
| `Column.key`, `Column.doc` | Nome alternativo no Python e documentação interna, sem efeito no banco. |
| `MetaData(naming_convention=...)` | Nomes determinísticos de restrições e índices (`"pk": "%(table_name)s_pk"`). |
| Argumentos `<dialeto>_<opcao>` | Opções de DDL por dialeto (`redshift_sortkey`, `postgresql_partition_by`). Com o dialeto instalado, um argumento que ele não aceita é `ArgumentError`; sem o dialeto, o argumento é aceito com o aviso `Can't validate argument` e não produz DDL. |
| `Table.implicit_returning=False` | Desliga `RETURNING` para a tabela, para backends com gatilhos ou sem suporte. |
| `TypeDecorator` | Tipo derivado com `process_bind_param` e `process_result_value`; `cache_ok = True` para participar do cache de compilação. Serve, por exemplo, para forçar UTC em `DateTime` ou serializar JSON. |
| `@compiles(Construct, "<dialeto>")` | Troca a compilação de um tipo, de um comando ou de um DDL num dialeto. Exemplo: [gancho que aplica `Table.info` ao `CREATE TABLE` do Redshift](redshift.md). |
| Eventos | `DDLEvents` (`before_create`), `ConnectionEvents` (`before_cursor_execute`), `PoolEvents.connect` para configurar cada conexão nova (`SET search_path`, `SET memory_limit`). |
| `Sequence`, `Identity`, `server_default`, `FetchedValue`, `Computed` | Geração de valores no servidor, descrita na seção do ORM. |
| `create_engine(..., use_insertmanyvalues=False, insertmanyvalues_page_size=...)` | Controle do modo de inserção em lote. |

Uma função com nome diferente nos dois bancos, como a que deriva `mes` de `data_ref`, é um
`FunctionElement` com uma regra `@compiles` por dialeto. O mesmo `select` compila para cada banco; o
DuckDB não tem `to_char` (`Catalog Error: Scalar Function with name to_char does not exist!`):

```python
# Uma função por dialeto: strftime no DuckDB, to_char no Redshift, escolhida na compilação.
from sqlalchemy import String, select
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.functions import FunctionElement

class month_of(FunctionElement):
    """Mês 'AAAA-MM' de uma data; cada banco tem a própria função de formatação."""
    type = String(7)
    name = "month_of"
    inherit_cache = True

@compiles(month_of, "duckdb")
def _month_of_duckdb(element, compiler, **kw):
    return f"strftime({compiler.process(element.clauses, **kw)}, '%Y-%m')"

@compiles(month_of, "redshift")
def _month_of_redshift(element, compiler, **kw):
    return f"to_char({compiler.process(element.clauses, **kw)}, 'YYYY-MM')"

stmt = select(operations.c.id_operacao, month_of(operations.c.data_ref).label("mes"))
print(stmt.compile(dialect=duckdb_engine.Dialect()))
print(stmt.compile(dialect=RedshiftDialect_redshift_connector()))
```

Saída:

```
SELECT operacoes.id_operacao, strftime(operacoes.data_ref, '%Y-%m') AS mes
FROM operacoes
SELECT operacoes.id_operacao, to_char(operacoes.data_ref, 'YYYY-MM') AS mes
FROM operacoes
```

Executado pelo engine do DuckDB, o `select` devolveu `'2026-08'` para `data_ref = 2026-08-01`. Sem
regra para o dialeto em uso, a compilação falha com `UnsupportedCompilationError` (`construct has no
default compilation handler`), o que denuncia um backend não previsto.

## Statements de insert, update, delete e select

```python
from datetime import date
from decimal import Decimal
from sqlalchemy import insert, select, update, delete, func, text

stmt = insert(operations).values(id_operacao=1, data_ref=date(2026, 8, 1), id_cliente=100, valor=Decimal("10.50"))
batch = insert(operations)                          # executemany com lista de dicionários
multi_row = insert(operations).values([row1, row2])   # um comando com várias linhas
copy_stmt = insert(operations).from_select(["id_operacao", "data_ref", "id_cliente", "valor", "descricao"],
                                      select(staging))
query = (
    select(operations.c.id_cliente, func.sum(operations.c.valor).label("total"))
    .where(operations.c.data_ref >= date(2026, 8, 1))
    .group_by(operations.c.id_cliente)
    .order_by(operations.c.id_cliente)
)
adjustment = update(operations).where(operations.c.id_operacao == 1).values(descricao="ajustada")
removal = delete(operations).where(operations.c.data_ref < date(2020, 1, 1))
raw_stmt = text("SELECT count(*) FROM operacoes WHERE data_ref >= :start").bindparams(start=date(2026, 8, 1))
```

Execução:

```python
with engine.begin() as conn:                      # transação com commit no fim do bloco
    conn.execute(batch, [row1, row2, row3])
    result = conn.execute(query)
    rows = result.mappings().all()           # dicionários por linha
    total = conn.execute(raw_stmt).scalar()

with engine.connect() as conn:                    # commit explícito, "commit as you go"
    conn.execute(adjustment)
    conn.commit()
```

Regras que importam:

- O `executemany` com lista de dicionários usa apenas as chaves do primeiro dicionário para montar o
  `VALUES`; dicionários heterogêneos são recurso do ORM.
- O recurso `insertmanyvalues` reescreve o `executemany` em comandos `INSERT ... VALUES (...),
  (...)` com até 1.000 linhas por comando (`insertmanyvalues_page_size`) e até 32.700 parâmetros.
  Ele é usado sempre que há `RETURNING` e, sem `RETURNING`, apenas nos dialetos com
  `use_insertmanyvalues_wo_returning` verdadeiro: psycopg2, duckdb_engine e o dialeto do Redshift
  com psycopg2 sim; o dialeto do Redshift com `redshift_connector` não, e nele o `executemany` vira
  uma ida ao servidor por linha.
- `insert(...).values(lista)` gera um único comando com todas as linhas, sem paginação; o limite é o
  do banco (16 MB por comando no Redshift).
- `insert(...).returning(...)` existe em todos os dialetos incluídos, exceto MySQL, e no DuckDB;
  `update(...).returning(...)` também, exceto no MariaDB. O Redshift não tem `RETURNING`, mas o
  dialeto herda `insert_returning = True` do PostgreSQL e o emite quando há valores gerados no
  servidor; `Table.implicit_returning = False` desliga.
- `stmt.compile(dialect=..., compile_kwargs={"literal_binds": True})` embute os valores no SQL; a
  compilação normal produz o estilo de parâmetro do dialeto (`$1` no duckdb_engine ligado a um
  engine, `%s` no Redshift).
- `Result` oferece `all()`, `first()`, `scalar()`, `scalars()`, `mappings()` e `partitions(n)` para
  consumir em pedaços; `rowcount` depende do dialeto e é `-1` no duckdb_engine.
- `pd.read_sql(query, engine)` aceita o `select` do SQLAlchemy; `coerce_float=True`, o padrão,
  converte `Decimal` em `float`. `DataFrame.to_sql` gera `INSERT` por `executemany`, com
  `method="multi"` para um `VALUES` de várias linhas.

O mesmo `select` serve aos dois bancos. Compilado com `literal_binds`, o texto é idêntico nos dois
dialetos; no DuckDB, a conexão bruta por trás do engine executa o texto e devolve Arrow, que preserva
o decimal:

```python
# O mesmo select compilado para os dois dialetos e executado no DuckDB com resultado em Arrow.
import duckdb_engine
from sqlalchemy import create_engine
from sqlalchemy_redshift.dialect import RedshiftDialect_redshift_connector

sql_duckdb = str(query.compile(dialect=duckdb_engine.Dialect(), compile_kwargs={"literal_binds": True}))
sql_redshift = str(query.compile(dialect=RedshiftDialect_redshift_connector(), compile_kwargs={"literal_binds": True}))
assert sql_duckdb == sql_redshift
print(sql_duckdb)

rows = [
    {"id_operacao": 1, "data_ref": date(2026, 8, 1), "id_cliente": 100, "valor": Decimal("10.50"), "descricao": None},
    {"id_operacao": 2, "data_ref": date(2026, 8, 2), "id_cliente": 100, "valor": Decimal("4.25"), "descricao": "estorno"},
    {"id_operacao": 3, "data_ref": date(2026, 8, 9), "id_cliente": 200, "valor": Decimal("7.00"), "descricao": None},
]
engine = create_engine("duckdb:///:memory:")
metadata.create_all(engine)
with engine.begin() as conn:
    conn.execute(insert(operations), rows)
    raw = conn.connection.dbapi_connection                    # conexão DuckDB por trás do engine
    table = raw.sql(sql_duckdb).to_arrow_table()              # pyarrow.Table
    batches = list(raw.sql(sql_duckdb).to_arrow_reader(1))    # RecordBatchReader, consumido antes de outro comando
print(table.schema)
print(table.to_pylist(), [b.num_rows for b in batches])
```

Saída:

```
SELECT operacoes.id_cliente, sum(operacoes.valor) AS total
FROM operacoes
WHERE operacoes.data_ref >= '2026-08-01' GROUP BY operacoes.id_cliente ORDER BY operacoes.id_cliente
id_cliente: int64
total: decimal128(38, 2)
[{'id_cliente': 100, 'total': Decimal('14.75')}, {'id_cliente': 200, 'total': Decimal('7.00')}] [1, 1]
```

A soma de `DECIMAL(18, 2)` sai como `decimal128(38, 2)`; o cast para o esquema do contrato acontece
antes de gravar. No DuckDB 1.5.5, `arrow()` da relação devolve um `RecordBatchReader`, não uma
tabela, e `fetch_arrow_table()` e `fetch_record_batch()` estão obsoletos em favor de
`to_arrow_table()` e `to_arrow_reader()`. O leitor é consumido antes de qualquer outro comando na
mesma conexão, inclusive o commit do fim do bloco; depois disso ele devolve zero lotes, sem erro.

## ORM: modelos e DDL

### Mapeamento declarativo

```python
import datetime as dt, decimal
from typing import Annotated
import sqlalchemy as sa
from sqlalchemy import BigInteger, MetaData, Numeric, PrimaryKeyConstraint, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, declared_attr

Amount = Annotated[decimal.Decimal, mapped_column(Numeric(18, 2))]

class Base(DeclarativeBase):
    metadata = MetaData(naming_convention={"pk": "%(table_name)s_pk"})
    type_annotation_map = {
        int: BigInteger,
        str: String(),
        dt.datetime: sa.TIMESTAMP(timezone=True),
    }

class Rastreio:
    """Mixin com colunas comuns; declared_attr gera uma coluna por classe."""
    @declared_attr
    def id_execucao(cls) -> Mapped[str]:
        return mapped_column(String(32))

class Operacao(Rastreio, Base):
    __tablename__ = "operacoes"
    __table_args__ = (
        PrimaryKeyConstraint("id_operacao", "data_ref").ddl_if(dialect="redshift"),
        {
            "comment": "Operações do mês",
            "info": {"serialize_db": {"partition_by": ["mes"],
                                      "sort_key": ["data_ref", "id_operacao"],
                                      "redshift": {"diststyle": "KEY", "distkey": "id_cliente"}}},
        },
    )
    id_operacao: Mapped[int] = mapped_column(autoincrement=False)
    data_ref: Mapped[dt.date]
    id_cliente: Mapped[int] = mapped_column(comment="Chave do cliente", info={"serialize_db": {"pii": False}})
    valor: Mapped[Amount]
    descricao: Mapped[str | None] = mapped_column(String(200))
    mes: Mapped[str] = mapped_column(String(7), comment="Mês de data_ref no formato AAAA-MM")
```

- `DeclarativeBase` cria a `MetaData` e o `registry`; `__tablename__` e as anotações `Mapped[...]`
  geram a `Table`, acessível em `Operacao.__table__`. O tipo e a nulidade vêm da anotação:
  `Mapped[str | None]` é `NULL`, `Mapped[str]` é `NOT NULL`, `primary_key=True` implica `NOT NULL`;
  `mapped_column(nullable=...)` prevalece.
- `type_annotation_map` na classe base troca a tabela padrão de tipos (`int` para `Integer`, `str`
  para `String()`, `Decimal` para `Numeric()` sem precisão, `datetime` para `DateTime()` sem fuso).
  O contrato exige `BigInteger`, `Numeric(18, 2)` e `TIMESTAMP` com fuso, então o mapa é parte do
  modelo.
- `Annotated` com `mapped_column` define tipos reutilizáveis, como `Amount` acima.
- `__table_args__` aceita um dicionário ou uma tupla de restrições com o dicionário no fim; nele
  entram `schema`, `comment`, `info`, `implicit_returning` e os argumentos de dialeto.
- `__mapper_args__` configura o `Mapper`: `primary_key` para mapear uma view sem chave,
  `version_id_col`, `eager_defaults`, `polymorphic_on`.
- `MappedAsDataclass` transforma as classes em dataclasses com `__init__`, `__repr__` e `__eq__`
  gerados. O mapeamento imperativo (`registry.map_imperatively(Model, table)`) mapeia uma `Table`
  existente, e o híbrido usa `__table__` no lugar de `__tablename__`.

### Metadados da aplicação no modelo

O canal documentado é `info`: `mapped_column(info={...})` preenche `Column.info`, e a chave `"info"`
de `__table_args__` preenche `Table.info`. Os dois viajam com os objetos até os compiladores, os
eventos de DDL e a inspeção:

```python
from sqlalchemy import inspect

options = Operacao.__table__.info["serialize_db"]
for column in Operacao.__table__.columns:
    print(column.name, column.type, column.nullable, column.comment, column.info)
mapper = inspect(Operacao)             # Mapper: mapper.columns, mapper.attrs, mapper.primary_key
```

Ganchos `@compiles(CreateTable, "redshift")` e eventos `before_create` leem `Table.info` e produzem
o DDL específico do banco, o que dispensa os argumentos `redshift_*` no modelo e o aviso que eles
geram sem o dialeto instalado. `comment` documenta o esquema no próprio banco; `doc` fica só no
Python.

## ORM: insert, update, delete e select

### Session e unidade de trabalho

```python
from sqlalchemy import select, insert, update, delete
from sqlalchemy.orm import Session

with Session(engine) as session:
    session.add(Operacao(id_operacao=1, data_ref=dt.date(2026, 8, 1), id_cliente=100,
                         valor=decimal.Decimal("10.50"), mes="2026-08", id_execucao="abc123"))
    session.commit()

    op = session.get(Operacao, (1, dt.date(2026, 8, 1)))
    objects = session.scalars(select(Operacao).where(Operacao.id_cliente == 100)).all()
    pairs = session.execute(select(Operacao.id_operacao, Operacao.valor)).all()

    op.descricao = "ajustada"          # UPDATE no flush
    session.commit()
    session.delete(op)                 # DELETE no flush
    session.commit()
```

A `Session` acumula objetos novos, alterados e removidos e emite os comandos no `flush`, que o
`commit` dispara. Por padrão, `expire_on_commit=True` invalida os atributos após o commit, e o
próximo acesso reconsulta o banco. O mapa de identidade garante um objeto por chave primária na
sessão.

### Operações em lote

```python
with Session(engine) as session:
    session.execute(insert(Operacao), records)                         # bulk insert
    inserted = session.scalars(insert(Operacao).returning(Operacao), records).all()
    session.execute(insert(Operacao).execution_options(render_nulls=True), records)
    session.execute(update(Operacao), [{"id_operacao": 1, "data_ref": d, "descricao": "x"}])   # por chave
    session.execute(update(Operacao).where(Operacao.data_ref < d).values(descricao=None),
                    execution_options={"synchronize_session": "fetch"})
    session.execute(delete(Operacao).where(Operacao.data_ref < d))
    session.commit()
```

- O bulk insert do ORM (`session.execute(insert(Modelo), lista)`) aceita dicionários com chaves
  diferentes, agrupa-os por conjunto de chaves e emite um `INSERT` por grupo; linhas com `None`
  também viram grupos separados, para que `DEFAULT` do servidor se aplique, e `render_nulls=True`
  mantém tudo num lote. As chaves são os nomes dos atributos, não das colunas.
- `insert(Modelo).returning(Modelo)` devolve objetos; `sort_by_parameter_order=True` garante a ordem
  dos retornos em relação à entrada, ao custo de inserções uma a uma quando a chave é gerada no
  servidor e o backend não tem forma ordenada; com chaves geradas no cliente o lote se mantém.
- `insert(Modelo).values(lista)` desliga o modo bulk e gera um único comando; é a forma para
  expressões SQL por linha e para upserts.
- `update(Modelo)` com lista de dicionários atualiza por chave primária; `update(...).where(...)` e
  `delete(...).where(...)` são comandos em lote, com `synchronize_session` (`auto`, `evaluate`,
  `fetch`, `False`) decidindo como os objetos na sessão são atualizados.
- Upserts usam o construtor `insert` do dialeto: `sqlalchemy.dialects.postgresql.insert(...).
  on_conflict_do_update(index_elements=[...], set_={...})` (aceito pelo DuckDB) e o equivalente do
  SQLite. O Redshift não tem `ON CONFLICT`; o caminho é `MERGE` em `text()`.
- Os métodos `bulk_insert_mappings` e `bulk_update_mappings` são a forma legada dos mesmos recursos.

O lote da biblioteca não passa pelo `executemany` em nenhum dos dois bancos. No DuckDB, o lote é uma
tabela Arrow com o esquema do contrato, registrada na conexão bruta; no Redshift, um `INSERT` de
várias linhas compilado do mesmo modelo:

```python
# Lote sem executemany: tabela Arrow registrada no DuckDB; um INSERT de várias linhas no Redshift.
import pyarrow as pa
from sqlalchemy import create_engine, text

records = [
    {"id_operacao": 1, "data_ref": dt.date(2026, 8, 1), "id_cliente": 100, "valor": decimal.Decimal("10.50"), "id_execucao": "abc123"},
    {"id_operacao": 2, "data_ref": dt.date(2026, 8, 2), "id_cliente": 100, "valor": decimal.Decimal("4.25"), "id_execucao": "abc123"},
]
for r in records:
    r["mes"] = r["data_ref"].strftime("%Y-%m")               # coluna de partição, derivada antes de gravar
batch = pa.Table.from_pylist(records, schema=arrow_schema(Operacao))   # chaves ausentes viram nulo

engine = create_engine("duckdb:///:memory:")
Base.metadata.create_all(engine)
with engine.begin() as conn:
    raw = conn.connection.dbapi_connection
    raw.register("lote", batch)                              # visível só pela conexão bruta
    conn.execute(text("INSERT INTO operacoes BY NAME SELECT * FROM lote"))
    raw.unregister("lote")
    print(conn.execute(select(func.count()).select_from(Operacao)).scalar())

stmt = insert(Operacao).values(records)                     # um comando, sem paginação
print(stmt.compile(dialect=RedshiftDialect_redshift_connector(), compile_kwargs={"literal_binds": True}))
```

Saída:

```
2
INSERT INTO operacoes (id_operacao, data_ref, id_cliente, valor, mes, id_execucao) VALUES (1, '2026-08-01', 100, 10.50, '2026-08', 'abc123'), (2, '2026-08-02', 100, 4.25, '2026-08', 'abc123')
```

`pa.Table.from_pylist` com o esquema do contrato recusa um valor fora do tipo: um `Decimal` de 19
dígitos em `decimal128(18, 2)` falha com
`Decimal type with precision 19 does not fit into precision inferred from first array element: 18`, e
um texto em `int64` falha com `Could not convert 'x' with type str`. Um nulo em campo não anulável
passa pela construção da tabela Arrow e é recusado pelo `NOT NULL` da tabela na carga
(`Constraint Error: NOT NULL constraint failed: operacoes.id_cliente`). O registro vale até o
`unregister`: depois dele, o mesmo `INSERT` falha com `Catalog Error: Table with name batch does not exist!`.

### Chaves geradas no servidor

Uma coluna inteira única na chave primária tem `autoincrement="auto"`: cada dialeto emite o seu
mecanismo no DDL (`SERIAL` no PostgreSQL, `IDENTITY` no SQL Server, `AUTO_INCREMENT` no MySQL; no
SQLite a coluna `INTEGER PRIMARY KEY` já é o rowid) e o ORM recupera o valor após o `INSERT` por
`RETURNING`, quando o backend suporta, ou por `cursor.lastrowid`. Com `RETURNING` e
`insertmanyvalues`, muitos objetos entram num comando só, e o SQLAlchemy usa uma coluna sentinela (a
própria chave ou uma coluna marcada com `insert_sentinel=True`) para casar os valores devolvidos com
os objetos.

Construtores explícitos:

| Construtor | DDL | Comportamento |
| --- | --- | --- |
| `Identity(start=1, increment=1)` | `GENERATED BY DEFAULT AS IDENTITY` | PostgreSQL 10+, Oracle, SQL Server. Ignorado por dialetos sem suporte; o DuckDB rejeita o DDL; o dialeto do Redshift o omite em silêncio. |
| `Sequence("nome", start=1)` | `CREATE SEQUENCE` e `nextval('nome')` no `INSERT` | PostgreSQL, Oracle, SQL Server, MariaDB e DuckDB. O Redshift não tem sequências, mas o dialeto declara suporte e o `create_all` emite o `CREATE SEQUENCE`, que o servidor não aceita. |
| `server_default=func.now()` | `DEFAULT now()` (`DEFAULT SYSDATE` no dialeto do Redshift) | Valor do servidor; com `eager_defaults="auto"` o ORM o busca por `RETURNING` no `INSERT` quando o dialeto declara suporte. |
| `server_default=FetchedValue()` | nenhum | Marca um valor gerado por gatilho ou regra externa, para o ORM buscar. |
| `Computed("expr")` | `GENERATED ALWAYS AS` | Coluna calculada. |

Sem `RETURNING`, com `eager_defaults="auto"`, os valores não chave gerados no servidor ficam
expirados e são buscados num `SELECT` no primeiro acesso; com `eager_defaults=True`, o ORM emite um
`SELECT` por linha logo após o `INSERT`, o que a documentação classifica como pouco performante.
Chaves primárias geradas no servidor precisam de `RETURNING` ou de `lastrowid`. Os modelos do
projeto usam chaves de negócio geradas no cliente (`autoincrement=False`), o que evita o problema
nos dois bancos: no DuckDB o `autoincrement` padrão vira `SERIAL`, tipo que o banco não tem, e o
único mecanismo é a sequência; no Redshift o `IDENTITY` gera valores com saltos, sem ordem garantida
e sem `RETURNING` para recuperá-los.

O DDL compilado mostra a diferença entre os dois modos:

```python
# Chave gerada no cliente: sem SERIAL no DuckDB nem IDENTITY no Redshift, e o INSERT não precisa de RETURNING.
from sqlalchemy import BigInteger, Column, MetaData, Table
from sqlalchemy.schema import CreateTable

md = MetaData()
server_key = Table("t_servidor", md, Column("id", BigInteger, primary_key=True))
client_key = Table("t_cliente", md, Column("id", BigInteger, primary_key=True, autoincrement=False))
for table in (server_key, client_key):
    print(str(CreateTable(table).compile(dialect=duckdb_engine.Dialect())).strip())
    print(str(CreateTable(table).compile(dialect=RedshiftDialect_redshift_connector())).strip())

stmt = insert(Operacao).values(id_operacao=1, data_ref=dt.date(2026, 8, 1), id_cliente=100,
                               valor=decimal.Decimal("10.50"), mes="2026-08", id_execucao="abc123")
print(stmt.compile(dialect=RedshiftDialect_redshift_connector()))
```

Com o `autoincrement` padrão, `t_servidor` sai com `id BIGSERIAL NOT NULL` no duckdb_engine, tipo
que o DuckDB não tem, e com `id BIGINT NOT NULL` no dialeto do Redshift, sem `IDENTITY`. Com
`autoincrement=False`, `t_cliente` sai com `id BIGINT NOT NULL` nos dois. O `INSERT` de `Operacao`
compilado para o Redshift lista as colunas informadas, com `id_operacao` vindo do cliente, e não tem
`RETURNING`:

```
INSERT INTO operacoes (id_operacao, data_ref, id_cliente, valor, mes, id_execucao) VALUES (%s, %s, %s, %s, %s, %s)
```

## Suporte a Redshift, DuckDB e arquivos Parquet

### Redshift

O dialeto externo `sqlalchemy-redshift` (versão 1.0.0) aparece na lista de dialetos externos da
documentação do SQLAlchemy. O [documento do Redshift](redshift.md) detalha o dialeto; o resumo para o
contrato:

| Tema | Comportamento |
| --- | --- |
| DDL | `redshift_diststyle`, `redshift_distkey`, `redshift_sortkey`, `redshift_interleaved_sortkey` na tabela; `redshift_encode`, `redshift_distkey`, `redshift_sortkey`, `redshift_identity` na coluna. `PRIMARY KEY`, `UNIQUE` e `FOREIGN KEY` saem no DDL e são informativas no banco. |
| Tipos | `Numeric(18, 2)` vira `NUMERIC(18, 2)`; `String(n)` vira `VARCHAR(n)`, com `n` em bytes; `String()` sem comprimento vira `VARCHAR`, que o Redshift trata como `VARCHAR(256)`. O dialeto compila `Text` como `TEXT`, também `VARCHAR(256)` no Redshift, então o `VARCHAR(65535)` da tabela do contrato exige `String(65535)` ou uma regra `@compiles(Text, "redshift")`. `JSON` compila como `JSON`, que o Redshift não tem; o contrato usa `JSON().with_variant(SUPER(), "redshift")`, que compila `SUPER` ([campos JSON](schema.md)). |
| Importação | `CopyCommand(Table, ...)` gera o `COPY ... FORMAT AS PARQUET MANIFEST` a partir do `Table` do modelo; a validação do esquema acontece no Arrow, antes do Parquet. |
| Exportação | `UnloadFromSelect(select(Modelo)...)` gera o `UNLOAD` da consulta do modelo. |
| Inserção pelo ORM | Volumes pequenos com `insert(Modelo).values(lista)` em lotes; o bulk insert do ORM cai no `executemany` linha a linha do `redshift_connector`. |
| Chaves | Nenhuma geração no servidor recuperável; chaves de negócio no cliente. Um `server_default` exige `__table_args__ = {"implicit_returning": False}`, senão o ORM emite `RETURNING`. |

### DuckDB

O dialeto `duckdb_engine` (versão 0.17.0) não consta da lista de dialetos externos da documentação do
SQLAlchemy e deriva do dialeto PostgreSQL com psycopg2. O [documento do DuckDB](duckdb.md) traz os
detalhes; o resumo para o contrato:

| Tema | Comportamento |
| --- | --- |
| DDL | `create_all` funciona com chaves, `NOT NULL`, comentários e sequências; `Identity` e `use_alter` falham, e `DEFERRABLE` é descartado nas restrições de tabela; `String(n)` perde o comprimento. Uma chave primária `Integer` de uma coluna com o `autoincrement` padrão sai como `SERIAL` e falha com `Type with name SERIAL does not exist!`, então as chaves do cliente levam `autoincrement=False`. Restrições ficam fora pela política do projeto (`ddl_if(dialect="redshift")`). |
| Reflexão | Colunas e comentários sim; chave primária e índices não. |
| Importação | `INSERT INTO tabela BY NAME SELECT * FROM entrada` com a tabela Arrow registrada na conexão bruta (`conn.connection.dbapi_connection.register`), porque variáveis Python não são visíveis pelo engine. Volumes pequenos pelo bulk insert do ORM (0,98 s para 50.000 linhas, 0,77 s com `render_nulls=True`). |
| Exportação | `COPY (...) TO 'arquivo.parquet'` em `text()`, com a consulta do modelo compilada com `literal_binds`. |
| Leitura | `pd.read_sql` com `coerce_float=False` para `Decimal`; ou o SQL compilado executado pela conexão DuckDB com `to_arrow_table()` para manter `decimal128` e `date32`. |
| Transações | Banco em memória com `SingletonThreadPool`: conexões abertas sem fechar deixam transações pendentes. |

### Arquivos Parquet

Não há dialeto SQLAlchemy para Parquet. O esquema dos arquivos deriva do modelo por uma
correspondência de tipos, e a aplicação do esquema acontece no Arrow, na escrita e na leitura:

```python
import pyarrow as pa, pyarrow.parquet as pq
from sqlalchemy import types as t

def arrow_type(sa_type: t.TypeEngine) -> pa.DataType:
    match sa_type:
        case t.SmallInteger():
            return pa.int16()
        case t.BigInteger():
            return pa.int64()
        case t.Integer():
            return pa.int32()
        case t.Boolean():
            return pa.bool_()
        case t.Float():
            return pa.float64()
        case t.Numeric(precision=int() as p, scale=int() as s):
            return pa.decimal128(p, s)
        case t.String():
            return pa.string()
        case t.Date():
            return pa.date32()
        case t.DateTime(timezone=True):
            return pa.timestamp("us", tz="UTC")
        case t.DateTime():
            return pa.timestamp("us")
        case t.Uuid():
            return pa.string()
        case t.JSON():
            return pa.json_(pa.string())
    raise TypeError(f"tipo sem correspondência Arrow: {sa_type!r}")

def arrow_schema(model) -> pa.Schema:
    return pa.schema([
        pa.field(c.name, arrow_type(c.type), nullable=c.nullable, metadata={"PARQUET:field_id": str(i)})
        for i, c in enumerate(model.__table__.columns, start=1)
    ])

def write_parquet(model, df, path: str) -> None:
    schema = arrow_schema(model)
    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False, safe=True)
    pq.write_table(table, path, compression="zstd", row_group_size=100_000)

def check(model, path: str) -> None:
    expected, actual = arrow_schema(model), pq.read_schema(path)
    for field in expected:
        actual_field = actual.field(field.name)
        if actual_field.type != field.type or (not field.nullable and actual_field.nullable):
            raise ValueError(f"{field.name}: arquivo {actual_field}, modelo {field}")
```

- A ordem dos casos importa: `BigInteger` e `SmallInteger` são subclasses de `Integer`, e `Float` é
  subclasse de `Numeric`. `Text` cai em `String`. `JSON` vira a extensão `arrow.json`, texto com anotação; o
  `with_variant(SUPER(), "redshift")` do contrato não muda o tipo genérico, e `isinstance(sa_type, t.JSON)`
  continua verdadeiro. O metadado `PARQUET:field_id` é o que o PyArrow grava como `field_id` no Parquet;
  a chave `field_id` sem prefixo não gera nada. Um `Numeric()` sem precisão e escala, que é o que o
  mapa de tipos padrão dá a `Mapped[decimal.Decimal]`, cai no erro final; o mapa do modelo ou o
  `Annotated` precisa fixar `Numeric(18, 2)`.
- `pa.Table.from_pandas(..., schema=..., safe=True)` falha quando um valor não cabe no tipo (inteiro
  fora do intervalo, decimal com mais dígitos que a precisão, nulo em campo não anulável), o que é a
  verificação de contrato antes de gravar.
- O escritor Parquet do DuckDB grava todas as colunas como `optional`, e a conferência acima
  rejeita esses arquivos onde o modelo exige valor; para arquivos do DuckDB, a verificação de
  `NOT NULL` fica na auditoria, que conta os nulos. O Parquet gravado pelo PyArrow mantém `required`.
- Para consultar arquivos com os construtores do SQLAlchemy, o DuckDB serve de engine:
  `CREATE VIEW operacoes AS SELECT * FROM read_parquet('operacoes/**/*.parquet')` e o modelo mapeado
  sobre a view (`__mapper_args__ = {"primary_key": [...]}` quando a view não declara chave). O
  [documento sobre Parquet](parquet.md) descreve os metadados e as opções de leitura.

## Referências

- Documentação do SQLAlchemy 2.0: <https://docs.sqlalchemy.org/en/20/>. Páginas usadas: tutorial
  unificado (engine, transações, metadata, insert, select, update, manipulação de dados no ORM),
  `MetaData` e `Table`, reflexão, DDL, restrições e índices, defaults e `Identity`, tipos e tipos
  customizados, extensão de compilação, conexões e `insertmanyvalues`, eventos, tabelas
  declarativas, configuração e estilos de mapeamento, dataclasses, guia de consultas do ORM
  (`INSERT`, `UPDATE`, `DELETE`), técnicas de persistência, `Session`, FAQ de performance e a lista
  de dialetos.
- Dialeto do Redshift: <https://github.com/sqlalchemy-redshift/sqlalchemy-redshift> e
  <https://sqlalchemy-redshift.readthedocs.io/en/latest/>.
- Dialeto do DuckDB: <https://github.com/Mause/duckdb_engine>.
- Alembic, ferramenta de migração do projeto SQLAlchemy: <https://alembic.sqlalchemy.org/>.
- Documentação do pandas sobre `read_sql` e `to_sql`: <https://pandas.pydata.org/docs/>.
- Documentação do PyArrow: <https://arrow.apache.org/docs/python/>.
- Tutorial de referência do projeto: <https://github.com/felipenoris/etl-cookbook-tutorial>.
- Lista completa das páginas consultadas: [REFERENCES.md](../REFERENCES.md).
