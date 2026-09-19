# SQLAlchemy

O SQLAlchemy é a camada que descreve o esquema do projeto e gera o SQL para os dois bancos. Ele tem
duas partes: o Core, com `MetaData`, `Table`, `Column`, os tipos e os construtores de comandos, e o
ORM, com as classes mapeadas e a `Session`. Os modelos ORM são o [contrato de esquema](guia.md) do
projeto: deles derivam o DDL do DuckDB e do Redshift, o esquema Arrow dos arquivos Parquet e as
auditorias. Este documento resume os conceitos usados pela biblioteca, as opções de customização e o
comportamento com Redshift, DuckDB e Parquet.

As afirmações vêm da documentação oficial da versão 2.0, consultada em 2026-09-18. Os exemplos foram
executados com SQLAlchemy 2.0.54, duckdb_engine 0.17.0 sobre DuckDB 1.5.5 e sqlalchemy-redshift 1.0.0;
os comandos do Redshift foram apenas compilados, sem conexão a um cluster.

## Esquema, metadata e reflexão

### MetaData, Table e Column

`MetaData` é a coleção de tabelas. Cada `Table` recebe nome, a `MetaData`, as colunas e as restrições;
cada `Column` recebe nome, tipo e opções (`primary_key`, `nullable`, `default`, `server_default`,
`comment`, `info`). A coleção conhece a ordem de dependência por chaves estrangeiras
(`MetaData.sorted_tables`) e emite o DDL nessa ordem.

```python
from sqlalchemy import MetaData, Table, Column, BigInteger, Date, Numeric, String, PrimaryKeyConstraint

metadata = MetaData()
operacoes = Table(
    "operacoes", metadata,
    Column("id_operacao", BigInteger, nullable=False),
    Column("data_ref", Date, nullable=False),
    Column("id_cliente", BigInteger, nullable=False, comment="Chave do cliente"),
    Column("valor", Numeric(18, 2), nullable=False),
    Column("descricao", String(200)),
    PrimaryKeyConstraint("id_operacao", "data_ref", name="operacoes_pk"),
    comment="Operações do mês",
    info={"serialize_db": {"chave_ordenacao": ["data_ref", "id_operacao"]}},
)
```

Os tipos genéricos (`Integer`, `BigInteger`, `Numeric(precisao, escala)`, `String(n)`, `Text`,
`Boolean`, `Date`, `DateTime(timezone=...)`, `Uuid`, `JSON`, `LargeBinary`) são traduzidos por cada
dialeto na compilação: `Numeric(18, 2)` vira `NUMERIC(18, 2)` no Redshift e no DuckDB (`DECIMAL(18,2)`
no catálogo do DuckDB), `String(200)` vira `VARCHAR(200)` no Redshift e `VARCHAR` no DuckDB. Os tipos
específicos ficam em `sqlalchemy.dialects.<dialeto>` e nos dialetos externos
(`sqlalchemy_redshift.dialect.SUPER`, `TIMESTAMPTZ`). `tipo.with_variant(outro, "dialeto")` troca o
tipo num dialeto só. A [tabela de tipos do contrato](schema.md) fixa a correspondência com Arrow,
Iceberg, DuckDB e Redshift.

Restrições e índices são objetos: `PrimaryKeyConstraint`, `ForeignKey` na coluna ou
`ForeignKeyConstraint` na tabela, `UniqueConstraint`, `CheckConstraint`, `Index`. Desde a versão
2.0, `restricao.ddl_if(dialect="redshift")` limita a emissão a um dialeto, o que implementa a
[política de restrições](schema.md) (chaves no Redshift, nenhuma no DuckDB) sem dois modelos:

```python
PrimaryKeyConstraint("id_operacao", "data_ref").ddl_if(dialect="redshift")
```

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

sql_redshift = str(CreateTable(operacoes).compile(dialect=RedshiftDialect_redshift_connector()))
sql_duckdb = str(CreateTable(operacoes).compile(dialect=duckdb_engine.Dialect()))
```

Os construtores de DDL (`CreateTable`, `DropTable`, `CreateSequence`, `CreateIndex`, `SetTableComment`)
são `ExecutableDDLElement` e aceitam `execute_if(dialect=..., callable_=...)`. Os eventos
`before_create`, `after_create`, `before_drop` e `after_drop` de `MetaData` e `Table` recebem a
conexão e permitem emitir DDL adicional:

```python
from sqlalchemy import event, DDL

event.listen(
    operacoes, "after_create",
    DDL("COMMENT ON TABLE operacoes IS 'Operações do mês'").execute_if(dialect="redshift"),
)
```

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

### Customização do comportamento

| Mecanismo | Uso |
| --- | --- |
| `Table.info`, `Column.info` | Dicionários livres, guardados com o objeto e ignorados pelo DDL. É o canal para metadados da aplicação, lidos por eventos e ganchos de compilação. |
| `Table.comment`, `Column.comment` | Emitidos como `COMMENT ON` pelos dialetos que suportam comentários; refletidos de volta. |
| `Column.key`, `Column.doc` | Nome alternativo no Python e documentação interna, sem efeito no banco. |
| `MetaData(naming_convention=...)` | Nomes determinísticos de restrições e índices (`"pk": "%(table_name)s_pk"`). |
| Argumentos `<dialeto>_<opcao>` | Opções de DDL por dialeto (`redshift_sortkey`, `postgresql_partition_by`). O SQLAlchemy valida cada argumento contra o dialeto registrado e recusa os desconhecidos, então o dialeto precisa estar instalado. |
| `Table.implicit_returning=False` | Desliga `RETURNING` para a tabela, para backends com gatilhos ou sem suporte. |
| `TypeDecorator` | Tipo derivado com `process_bind_param` e `process_result_value`; `cache_ok = True` para participar do cache de compilação. Serve, por exemplo, para forçar UTC em `DateTime` ou serializar JSON. |
| `@compiles(Construto, "dialeto")` | Troca a compilação de um tipo, de um comando ou de um DDL num dialeto. Exemplo: [gancho que aplica `Table.info` ao `CREATE TABLE` do Redshift](redshift.md). |
| Eventos | `DDLEvents` (`before_create`), `ConnectionEvents` (`before_cursor_execute`), `PoolEvents.connect` para configurar cada conexão nova (`SET search_path`, `SET memory_limit`). |
| `Sequence`, `Identity`, `server_default`, `FetchedValue`, `Computed` | Geração de valores no servidor, descrita na seção do ORM. |
| `create_engine(..., use_insertmanyvalues=False, insertmanyvalues_page_size=...)` | Controle do modo de inserção em lote. |

## Statements de insert, update, delete e select

```python
from sqlalchemy import insert, select, update, delete, func, text

stmt = insert(operacoes).values(id_operacao=1, data_ref=date(2026, 8, 1), id_cliente=100, valor=Decimal("10.50"))
lote = insert(operacoes)                          # executemany com lista de dicionários
varias = insert(operacoes).values([linha1, linha2])   # um comando com várias linhas
copia = insert(operacoes).from_select(["id_operacao", "data_ref", "id_cliente", "valor", "descricao"],
                                      select(staging))
consulta = (
    select(operacoes.c.id_cliente, func.sum(operacoes.c.valor).label("total"))
    .where(operacoes.c.data_ref >= date(2026, 8, 1))
    .group_by(operacoes.c.id_cliente)
    .order_by(operacoes.c.id_cliente)
)
ajuste = update(operacoes).where(operacoes.c.id_operacao == 1).values(descricao="ajustada")
remocao = delete(operacoes).where(operacoes.c.data_ref < date(2020, 1, 1))
bruto = text("SELECT count(*) FROM operacoes WHERE data_ref >= :inicio").bindparams(inicio=date(2026, 8, 1))
```

Execução:

```python
with engine.begin() as conn:                      # transação com commit no fim do bloco
    conn.execute(lote, [linha1, linha2, linha3])
    resultado = conn.execute(consulta)
    linhas = resultado.mappings().all()           # dicionários por linha
    total = conn.execute(bruto).scalar()

with engine.connect() as conn:                    # commit explícito, "commit as you go"
    conn.execute(ajuste)
    conn.commit()
```

Regras que importam:

- O `executemany` com lista de dicionários usa apenas as chaves do primeiro dicionário para montar o
  `VALUES`; dicionários heterogêneos são recurso do ORM.
- O recurso `insertmanyvalues` reescreve o `executemany` em comandos `INSERT ... VALUES (...), (...)`
  com até 1.000 linhas por comando (`insertmanyvalues_page_size`) e até 32.700 parâmetros. Ele é
  usado sempre que há `RETURNING` e, sem `RETURNING`, apenas nos dialetos com
  `use_insertmanyvalues_wo_returning` verdadeiro: psycopg2, duckdb_engine e o dialeto do Redshift com
  psycopg2 sim; o dialeto do Redshift com `redshift_connector` não, e nele o `executemany` vira uma ida
  ao servidor por linha.
- `insert(...).values(lista)` gera um único comando com todas as linhas, sem paginação; o limite é o
  do banco (16 MB por comando no Redshift).
- `insert(...).returning(...)` e `update(...).returning(...)` existem em PostgreSQL, SQLite, MariaDB,
  Oracle, SQL Server e DuckDB; o Redshift não tem `RETURNING`.
- `stmt.compile(dialect=..., compile_kwargs={"literal_binds": True})` embute os valores no SQL; a
  compilação normal produz o estilo de parâmetro do dialeto (`$1` no duckdb_engine, `%s` no Redshift).
- `Result` oferece `all()`, `first()`, `scalar()`, `scalars()`, `mappings()` e `partitions(n)` para
  consumir em pedaços; `rowcount` depende do dialeto e é `-1` no duckdb_engine.
- `pd.read_sql(consulta, engine)` aceita o `select` do SQLAlchemy; `coerce_float=True`, o padrão,
  converte `Decimal` em `float`. `DataFrame.to_sql` gera `INSERT` por `executemany`, com
  `method="multi"` para um `VALUES` de várias linhas.

## ORM: modelos e DDL

### Mapeamento declarativo

```python
import datetime as dt, decimal
from typing import Annotated
from sqlalchemy import BigInteger, Numeric, String, MetaData
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, declared_attr

Valor = Annotated[decimal.Decimal, mapped_column(Numeric(18, 2))]

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
            "info": {"serialize_db": {"particionamento": {"coluna": "data_ref", "transformacao": "month"},
                                      "chave_ordenacao": ["data_ref", "id_operacao"],
                                      "redshift": {"diststyle": "KEY", "distkey": "id_cliente"}}},
        },
    )
    id_operacao: Mapped[int] = mapped_column(autoincrement=False)
    data_ref: Mapped[dt.date]
    id_cliente: Mapped[int] = mapped_column(comment="Chave do cliente", info={"serialize_db": {"pii": False}})
    valor: Mapped[Valor]
    descricao: Mapped[str | None] = mapped_column(String(200))
```

- `DeclarativeBase` cria a `MetaData` e o `registry`; `__tablename__` e as anotações `Mapped[...]`
  geram a `Table`, acessível em `Operacao.__table__`. O tipo e a nulidade vêm da anotação:
  `Mapped[str | None]` é `NULL`, `Mapped[str]` é `NOT NULL`, `primary_key=True` implica `NOT NULL`;
  `mapped_column(nullable=...)` prevalece.
- `type_annotation_map` na classe base troca a tabela padrão de tipos (`int` para `Integer`, `str`
  para `String()`, `Decimal` para `Numeric()` sem precisão, `datetime` para `DateTime()` sem fuso). O
  contrato exige `BigInteger`, `Numeric(18, 2)` e `TIMESTAMP` com fuso, então o mapa é parte do modelo.
- `Annotated` com `mapped_column` define tipos reutilizáveis, como `Valor` acima.
- `__table_args__` aceita um dicionário ou uma tupla de restrições com o dicionário no fim; nele
  entram `schema`, `comment`, `info`, `implicit_returning` e os argumentos de dialeto.
- `__mapper_args__` configura o `Mapper`: `primary_key` para mapear uma view sem chave,
  `version_id_col`, `eager_defaults`, `polymorphic_on`.
- `MappedAsDataclass` transforma as classes em dataclasses com `__init__`, `__repr__` e `__eq__`
  gerados. O mapeamento imperativo (`registry.map_imperatively(Classe, tabela)`) mapeia uma `Table`
  existente, e o híbrido usa `__table__` no lugar de `__tablename__`.

### Metadados da aplicação no modelo

O canal documentado é `info`: `mapped_column(info={...})` preenche `Column.info`, e a chave `"info"`
de `__table_args__` preenche `Table.info`. Os dois viajam com os objetos até os compiladores, os
eventos de DDL e a inspeção:

```python
from sqlalchemy import inspect

opcoes = Operacao.__table__.info["serialize_db"]
for coluna in Operacao.__table__.columns:
    print(coluna.name, coluna.type, coluna.nullable, coluna.comment, coluna.info)
mapper = inspect(Operacao)             # Mapper: mapper.columns, mapper.attrs, mapper.primary_key
```

Ganchos `@compiles(CreateTable, "redshift")` e eventos `before_create` leem `Table.info` e produzem
o DDL específico do banco, o que dispensa os argumentos `redshift_*` no modelo e mantém os modelos
importáveis sem o dialeto instalado. `comment` documenta o esquema no próprio banco; `doc` fica só
no Python.

## ORM: insert, update, delete e select

### Session e unidade de trabalho

```python
from sqlalchemy import select, insert, update, delete
from sqlalchemy.orm import Session

with Session(engine) as session:
    session.add(Operacao(id_operacao=1, data_ref=dt.date(2026, 8, 1), id_cliente=100, valor=Decimal("10.50")))
    session.commit()

    op = session.get(Operacao, (1, dt.date(2026, 8, 1)))
    lista = session.scalars(select(Operacao).where(Operacao.id_cliente == 100)).all()
    pares = session.execute(select(Operacao.id_operacao, Operacao.valor)).all()

    op.descricao = "ajustada"          # UPDATE no flush
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
    session.execute(insert(Operacao), registros)                         # bulk insert
    novos = session.scalars(insert(Operacao).returning(Operacao), registros).all()
    session.execute(insert(Operacao).execution_options(render_nulls=True), registros)
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
- `insert(Modelo).returning(Modelo)` devolve objetos; `sort_by_parameter_order=True` garante a
  ordem dos retornos em relação à entrada, ao custo de inserções uma a uma em backends sem forma
  ordenada.
- `insert(Modelo).values(lista)` desliga o modo bulk e gera um único comando; é a forma para
  expressões SQL por linha e para upserts.
- `update(Modelo)` com lista de dicionários atualiza por chave primária; `update(...).where(...)`
  e `delete(...).where(...)` são comandos em lote, com `synchronize_session` (`auto`, `evaluate`,
  `fetch`, `False`) decidindo como os objetos na sessão são atualizados.
- Upserts usam o construtor `insert` do dialeto: `sqlalchemy.dialects.postgresql.insert(...).
  on_conflict_do_update(index_elements=[...], set_={...})` (aceito pelo DuckDB) e o equivalente do
  SQLite. O Redshift não tem `ON CONFLICT`; o caminho é `MERGE` em `text()`.
- Os métodos `bulk_insert_mappings` e `bulk_update_mappings` são a forma legada dos mesmos recursos.

### Chaves geradas no servidor

Uma coluna inteira única na chave primária tem `autoincrement="auto"`: cada dialeto emite o seu
mecanismo no DDL (`SERIAL` no PostgreSQL, `IDENTITY` no SQL Server, `AUTOINCREMENT` no SQLite) e o
ORM recupera o valor após o `INSERT` por `RETURNING`, quando o backend suporta, ou por
`cursor.lastrowid`. Com `RETURNING` e `insertmanyvalues`, muitos objetos entram num comando só, e o
SQLAlchemy usa uma coluna sentinela (a própria chave ou uma coluna marcada com
`insert_sentinel=True`) para casar os valores devolvidos com os objetos.

Construtores explícitos:

| Construtor | DDL | Comportamento |
| --- | --- | --- |
| `Identity(start=1, increment=1)` | `GENERATED BY DEFAULT AS IDENTITY` | PostgreSQL 10+, Oracle, SQL Server. Ignorado por dialetos sem suporte; o DuckDB rejeita o DDL; o dialeto do Redshift o omite em silêncio. |
| `Sequence("nome", start=1)` | `CREATE SEQUENCE` e `nextval('nome')` no `INSERT` | PostgreSQL, Oracle, SQL Server, MariaDB e DuckDB. O Redshift não tem sequências. |
| `server_default=func.now()` | `DEFAULT now()` | Valor do servidor; com `eager_defaults="auto"` o ORM o busca por `RETURNING` no `INSERT`. |
| `server_default=FetchedValue()` | nenhum | Marca um valor gerado por gatilho ou regra externa, para o ORM buscar. |
| `Computed("expr")` | `GENERATED ALWAYS AS` | Coluna calculada. |

Sem `RETURNING`, o ORM busca valores não chave num `SELECT` por linha após o `flush`, o que a
documentação classifica como pouco performante; chaves primárias geradas no servidor precisam de
`RETURNING` ou de `lastrowid`. Os modelos do projeto usam chaves de negócio geradas no cliente
(`autoincrement=False`), o que evita o problema nos dois bancos: no DuckDB o único mecanismo é a
sequência, e no Redshift o `IDENTITY` gera valores com saltos, sem ordem garantida e sem `RETURNING`
para recuperá-los.

## Suporte a Redshift, DuckDB e arquivos Parquet

### Redshift

O dialeto externo `sqlalchemy-redshift` (versão 1.0.0) aparece na lista de dialetos externos da
documentação do SQLAlchemy. O [documento do Redshift](redshift.md) detalha o dialeto; o resumo para o
contrato:

| Tema | Comportamento |
| --- | --- |
| DDL | `redshift_diststyle`, `redshift_distkey`, `redshift_sortkey`, `redshift_interleaved_sortkey` na tabela; `redshift_encode`, `redshift_distkey`, `redshift_sortkey`, `redshift_identity` na coluna. `PRIMARY KEY`, `UNIQUE` e `FOREIGN KEY` saem no DDL e são informativas no banco. |
| Tipos | `Numeric(18, 2)` vira `NUMERIC(18, 2)`; `String(n)` vira `VARCHAR(n)`, com `n` em bytes; `String()` sem comprimento vira `VARCHAR`, que o Redshift trata como `VARCHAR(256)`. O dialeto compila `Text` como `TEXT`, também `VARCHAR(256)` no Redshift, então o `VARCHAR(65535)` da tabela do contrato exige `String(65535)` ou uma regra `@compiles(Text, "redshift")`. `JSON` não existe; `SUPER` vem do dialeto. |
| Importação | `CopyCommand(Table, ...)` gera o `COPY ... FORMAT AS PARQUET MANIFEST` a partir do `Table` do modelo; a validação do esquema acontece no Arrow, antes do Parquet. |
| Exportação | `UnloadFromSelect(select(Modelo)...)` gera o `UNLOAD` da consulta do modelo. |
| Inserção pelo ORM | Volumes pequenos com `insert(Modelo).values(lista)` em lotes; o bulk insert do ORM cai no `executemany` linha a linha do `redshift_connector`. |
| Chaves | Nenhuma geração no servidor recuperável; chaves de negócio no cliente. |

### DuckDB

O dialeto `duckdb_engine` (versão 0.17.0) não consta da lista de dialetos externos da documentação do
SQLAlchemy e deriva do dialeto PostgreSQL com psycopg2. O [documento do DuckDB](duckdb.md) traz os
detalhes; o resumo para o contrato:

| Tema | Comportamento |
| --- | --- |
| DDL | `create_all` funciona com chaves, `NOT NULL`, comentários e sequências; `Identity` falha; `String(n)` perde o comprimento. Restrições ficam fora pela política do projeto (`ddl_if(dialect="redshift")`). |
| Reflexão | Colunas e comentários sim; chave primária e índices não. |
| Importação | `INSERT INTO tabela BY NAME SELECT * FROM entrada` com a tabela Arrow registrada na conexão bruta (`conn.connection.dbapi_connection.register`), porque variáveis Python não são visíveis pelo engine. Volumes pequenos pelo bulk insert do ORM (0,67 s para 50.000 linhas). |
| Exportação | `COPY (...) TO 'arquivo.parquet'` em `text()`, com a consulta do modelo compilada com `literal_binds`. |
| Leitura | `pd.read_sql` com `coerce_float=False` para `Decimal`; ou o SQL compilado executado pela conexão DuckDB com `to_arrow_table()` para manter `decimal128` e `date32`. |
| Transações | Banco em memória com `SingletonThreadPool`: conexões abertas sem fechar deixam transações pendentes. |

### Arquivos Parquet

Não há dialeto SQLAlchemy para Parquet. O esquema dos arquivos deriva do modelo por uma
correspondência de tipos, e a aplicação do esquema acontece no Arrow, na escrita e na leitura:

```python
import pyarrow as pa, pyarrow.parquet as pq
from sqlalchemy import types as t

def tipo_arrow(tipo: t.TypeEngine) -> pa.DataType:
    match tipo:
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
        case t.Numeric(precision=p, scale=s):
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
    raise TypeError(f"tipo sem correspondência Arrow: {tipo!r}")

def esquema_arrow(modelo) -> pa.Schema:
    return pa.schema([
        pa.field(c.name, tipo_arrow(c.type), nullable=c.nullable, metadata={"field_id": str(i)})
        for i, c in enumerate(modelo.__table__.columns, start=1)
    ])

def gravar(modelo, df, caminho: str) -> None:
    esquema = esquema_arrow(modelo)
    tabela = pa.Table.from_pandas(df, schema=esquema, preserve_index=False, safe=True)
    pq.write_table(tabela, caminho, compression="zstd", row_group_size=100_000)

def conferir(modelo, caminho: str) -> None:
    esperado, lido = esquema_arrow(modelo), pq.read_schema(caminho)
    for campo in esperado:
        campo_lido = lido.field(campo.name)
        if campo_lido.type != campo.type or (not campo.nullable and campo_lido.nullable):
            raise ValueError(f"{campo.name}: arquivo {campo_lido}, modelo {campo}")
```

- A ordem dos casos importa: `BigInteger` e `SmallInteger` são subclasses de `Integer`, e `Float` é
  subclasse de `Numeric`. `Text` cai em `String`.
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
