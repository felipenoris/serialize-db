# Etapa 1: `schema`

A entrega e o critério de aceite desta etapa estão na tabela de etapas de [`PLAN.md`](PLAN.md), que
também fixa as decisões, as regras que toda etapa obedece e a ordem do trabalho.

O módulo `serialize_db.schema` deriva dos modelos tudo o que os outros módulos precisam saber sobre
uma tabela. O modelo de referência de `tests/reference_model/`, o modelo SQLAlchemy da base original
em Parquet particionado, fica como está (decisão do usuário de 2026-09-21); a etapa começa pelo
modelo cliente, a cópia dele em `tests/client_model/` (escrita em 2026-09-21: o modelo de dados
que o código cliente apresenta para usar a biblioteca) corrigida como a biblioteca cliente o
escreveria: `Base` importável de um módulo só, cada tabela declarada uma vez, `BigInteger` nas chaves primárias inteiras
e nas colunas que as referenciam, `autoincrement=False` nessas chaves, chaves estrangeiras sem
`DEFERRABLE`, a coluna de partição `data_str` (`String(10)`, `AAAA-MM-DD` de `data`;
`data_base_str` de `data_base` em `cad_lancamentos`) nas quatro tabelas particionadas, comentários
de tabela e de coluna, e `Table.info["serialize_db"]` com `partition_by`, `partition_source`,
`sort_key` e `redshift`, como em `schema.md`; as colunas numéricas continuam `Double` (as decisões
de 2026-09-20 estão nas premissas de [`PLAN.md`](PLAN.md)). `String(n)` leva o comprimento tirado
das leituras, com folga; os índices não únicos e o `sqlite_strict` do original ficam de fora,
porque motor algum da biblioteca os usa; `redshift` fica ausente de `Table.info` (distribuição
`AUTO`) até a decisão. `tests/test_client_model.py` confere a cópia contra o original: as tabelas e
as colunas na mesma ordem, os tipos e as chaves mudados só onde previsto, sem `DEFERRABLE`,
`autoincrement` nem índice não único, todo comentário presente, e a partição de cada tabela
particionada igual à da base.

O modelo de referência bate com a base de origem lida em 2026-09-20 (desenvolvimento) e em
2026-09-21 (produção) ([`POC.md`](POC.md); `tests/test_reference_model.py` fixa a conferência): as 12
tabelas existem nos arquivos com as mesmas colunas, na mesma ordem e com os tipos da tabela de
`schema.md` (`Integer` em `int32`, `String` em `string`, `Date` em `date32`, `Double` em `double`,
`Boolean` em `bool`, `DateTime` em `timestamp`), e a nulidade declarada é a mesma, exceto em sete
colunas de `cad_contratos` (`sistema`, `um`, `to`, `fonte`, `taxa_juros_fixos`,
`data_primeira_amortizacao`, `data_ultima_amortizacao`), anuláveis nos arquivos e `NOT NULL` no
modelo, sem nulo algum nos dados; o modelo prevalece. A origem tem duas tabelas fora do modelo,
`alembic_version` e `meta_update_status`, que a carga ignora. A coluna de partição da origem é a do
contrato: `data_str` deriva de `data` em `cad_contratos`, `cad_operacoes` e `rel_contrato_operacao`,
e `data_base_str` de `data_base` em `cad_lancamentos`.

| Primitiva | O que faz |
| --- | --- |
| `arrow_schema(table)` | O `pa.Schema` do `Table`: os tipos da tabela de `schema.md`, a nulidade, o comentário de cada coluna em `metadata` do campo, `PARQUET:field_id`; um campo JSON é `string`, sem a extensão `arrow.json`. |
| `delta_schema(table)` | O `deltalake.Schema` derivado do Arrow: `decimal(18,2)`, `timestamp_ntz` para `DateTime` sem fuso e `timestamp` para o com fuso, `string` para JSON e UUID, comentários preservados. |
| `ddl(table, dialect, prefix="", keys=False)` | `CREATE TABLE` para `duckdb` ou `redshift`: sem `DEFERRABLE` nem `Identity`, `CHECK` só no DuckDB, chaves só com `keys=True` (as tabelas publicadas da [etapa 8](PLAN-STAGE-8.md)), `SORTKEY`, `DISTSTYLE` e `DISTKEY` no Redshift, `Text` como `VARCHAR(65535)`, `Uuid` como `VARCHAR(36)`, `JSON` como `SUPER`; `prefix` renomeia a tabela para o sandbox. |
| `table_options(table)` | O `TableOptions` (`partition_by`, `partition_source`, `sort_key`, `redshift`, `keys`) lido de `Table.info["serialize_db"]` com os padrões da biblioteca: tabela sem partição quando `partition_by` está ausente, uma coluna de partição no máximo, `String(10)`, derivada por `strftime(partition_source, '%Y-%m-%d')`, e as chaves do próprio modelo, `table.primary_key` e os `UniqueConstraint`; `keys` só acrescenta uma chave de negócio ou exclui uma delas, sempre de forma explícita. |
| `cast(data, table)` | A `pa.Table`, o `pa.RecordBatch` ou o `RecordBatchReader` lote a lote, convertido para `arrow_schema(table)` com `safe=True` e devolvido no mesmo tipo (`RecordBatch.cast` recusa o mesmo que `Table.cast`): as colunas do contrato presentes, na ordem do contrato; `large_string` para `string`, timestamps a microssegundos e UTC, inteiro em `Numeric` por `decimal128(21, 2)`; recusa perda de precisão, `double` fora da escala e `timestamp` com hora numa coluna `Date` (as duas perdas que `safe=True` não acusa), `struct` numa coluna JSON, texto acima de `String(n)` e nulo em coluna `NOT NULL`, com a mensagem que diz o que o cliente faz antes de chamar. |
| `check_models(metadata)` | A lista de violações do contrato nos modelos: tipo fora da tabela de tipos, `autoincrement` em chave inteira, `DEFERRABLE`, `Identity`, `partition_by` sem a coluna, sem `partition_source` ou com a coluna fora de `String(10)`, tabela sem chave primária e sem `keys`, coluna sem comentário. Vazia nos modelos corrigidos. |
| `schema_files(metadata)` | `{"<tabela>.delta.json": ..., "<tabela>.duckdb.sql": ..., "<tabela>.redshift.sql": ...}` em memória. |
| `write_schema_files(metadata, directory)` | Grava `schema_files` em `directory`; `serialize-db schema write` grava e `serialize-db schema check` compara sem gravar. |

Testes: `tests/test_schema.py`, sem gravar, sobre o modelo cliente e sobre um modelo de teste com
todos os tipos; `create_all` no DuckDB
em memória com o DDL de cada modelo; o diff dos arquivos `tests/client_model/schema/` versionados; `write_schema_files` sob a raiz local. Dependências: `sqlalchemy`, `pyarrow`,
`deltalake`, `duckdb`, `duckdb-engine` e `sqlalchemy-redshift`. Provas de conceito:
`test_sqlalchemy.py` (`test_declarative_model_exposes_table`, `test_ddl_per_dialect`,
`test_create_all_and_reflection`, `test_arrow_and_delta_schema_from_table`, com o mapa de tipos e
`Schema.from_arrow().to_json()`, `test_sandbox_copy_of_table_and_schema_files_diff`),
`test_pyarrow.py` (`test_schema_metadata_and_from_pylist`, `test_safe_cast_refuses_data_loss`, com as
perdas que o cast seguro não acusa, `test_arrow_table_round_trips_through_pandas_without_copy` e
`test_record_batch_cast_and_conversions_share_buffers`, o mesmo por lote) e
`test_stdlib.py` (`test_generated_files_diff`, `test_decimal_totals`).

## Interface

As exceções da biblioteca vivem em `serialize_db.errors`, um módulo sem dependências, porque
`delta` levanta `ExecutionConflict` e `execution` a captura, e um módulo por exceção criaria
importações cíclicas. `ContractError` é a primeira: dados ou modelo fora do contrato, com a
instrução ao cliente na mensagem.

```python
"""Assinaturas de serialize_db.schema; os corpos estão nos rascunhos abaixo."""
import dataclasses
from typing import Literal

import pyarrow as pa
import sqlalchemy as sa
from deltalake import Schema as DeltaSchema

Dialect = Literal["duckdb", "redshift"]


class ContractError(ValueError):
    """Dados ou modelo fora do contrato; a mensagem diz o que o cliente faz antes de chamar de novo."""


@dataclasses.dataclass(frozen=True)
class TableOptions:
    partition_by: str | None          # a coluna de partição, String(10), AAAA-MM-DD
    partition_source: str | None      # a coluna de data de que ela deriva
    sort_key: tuple[str, ...]
    redshift: dict[str, str]          # diststyle, distkey
    keys: tuple[tuple[str, ...], ...]  # a chave primária e as únicas do modelo, mais keys["add"], menos keys["drop"]


def arrow_type(column: sa.Column) -> pa.DataType: ...
def arrow_schema(table: sa.Table) -> pa.Schema: ...
def delta_schema(table: sa.Table) -> DeltaSchema: ...
def table_options(table: sa.Table) -> TableOptions: ...
def ddl(table: sa.Table, dialect: Dialect, prefix: str = "", keys: bool = False) -> str: ...
def cast(data: pa.Table | pa.RecordBatch | pa.RecordBatchReader, table: sa.Table) -> pa.Table | pa.RecordBatch | pa.RecordBatchReader: ...
def check_models(metadata: sa.MetaData) -> list[str]: ...
def schema_files(metadata: sa.MetaData) -> dict[str, str]: ...
def write_schema_files(metadata: sa.MetaData, directory: str) -> list[str]: ...
def check_schema_files(metadata: sa.MetaData, directory: str) -> list[str]: ...
```

`check_schema_files` é a forma sem gravar de `serialize-db schema check`: o diff unificado de cada
arquivo versionado contra a geração nova, vazio quando nada mudou. `ddl` ganhou `keys`: a etapa 8
declara a chave primária informativa nas tabelas publicadas, e o sandbox não.

## Estratégia de implementação

- **`arrow_type`** resolve `Numeric` e `DateTime` pelos parâmetros e os demais por `isinstance` numa
  ordem que põe `BigInteger` e `SmallInteger` antes de `Integer`, e `Text` antes de `String`;
  `Float` e `Double` derivam de `Numeric` no SQLAlchemy, então `Double` é testado antes. Um tipo fora
  da tabela de [`schema.md`](schema.md) é `ContractError`, não `TypeError`.
- **`arrow_schema`** numera `PARQUET:field_id` pela posição, guarda o comentário em `metadata` do
  campo e o nome da tabela em `metadata` do esquema (`serialize_db_table`).
- **`delta_schema`** é `Schema.from_arrow(arrow_schema(table))`; o delta-rs deriva `timestamp_ntz`
  do `timestamp[us]` sem fuso e preserva o comentário.
- **`table_options`** lê `Table.info["serialize_db"]` com os padrões da biblioteca e deriva `keys`
  de `table.primary_key` e dos `UniqueConstraint`; um `Index(unique=True)` do modelo de referência
  (`ix_contratos_data_sistema_contrato`) também é chave, então `table.indexes` com `unique` entra
  na lista. `keys["add"]` acrescenta e `keys["drop"]` exclui, sempre por lista de colunas.
- **`ddl`** não compila o `Table` do modelo: monta um `Table` novo com `Column(name, type, nullable,
  comment)` por coluna, sem chave, `ForeignKey` nem `Identity`, com os `CheckConstraint` só no
  DuckDB, e o nome `quoted_name(prefix + name, quote=False)` para o sentinela `{prefix}` e o
  `exec_<id>_` saírem sem aspas. Três regras `@compiles` vivem no módulo: `CreateTable` no Redshift
  acrescenta `DISTSTYLE`, `DISTKEY` e `SORTKEY` de `table_options`; `Text` no Redshift vira
  `VARCHAR(65535)`; `Uuid` vira `VARCHAR(36)` nos dois dialetos, porque o contrato guarda UUID como
  texto e o `duckdb_engine` emitiria `UUID`. O `duckdb_engine` escreve `NUMERIC(18, 2)`, `DOUBLE
  PRECISION` e `TEXT`, que o DuckDB lê como `DECIMAL(18,2)`, `DOUBLE` e `VARCHAR` (rascunho abaixo).
- **`cast`** seleciona as colunas do contrato presentes, na ordem do contrato, e trata coluna a
  coluna o que `safe=True` não acusa: `double` numa coluna `Numeric` só quando
  `pc.round(x, escala)` devolve o valor igual; `timestamp` numa coluna `Date` só quando a ida e
  volta devolve o valor igual; inteiro numa coluna `Numeric` pelo desvio por `decimal128(p + 3, s)`;
  `struct`, `list` e `map` numa coluna JSON recusados; texto acima de `String(n)` medido em bytes por
  `pc.binary_length`, a medida do `VARCHAR(n)` do Redshift. Depois disso, o `cast` do PyArrow com
  `safe=True` faz o resto: nulo em `NOT NULL`, escala perdida, nanossegundo não nulo, estouro. Toda
  recusa sai como `ContractError` com a tabela, a coluna e a instrução. Um `RecordBatchReader` volta
  como leitor que converte lote a lote, com o esquema do primeiro lote convertido.
- **`check_models`** percorre `metadata.sorted_tables` e devolve uma lista de textos, um por
  violação, para o teste do modelo cliente ser `assert check_models(Base.metadata) == []`. O
  `autoincrement` padrão é a string `"auto"`, não `True`: a regra reprova os dois numa chave
  inteira.
- **`schema_files`** gera `<tabela>.delta.json` por `delta_schema(...).to_json()` e os dois `.sql`
  por `ddl`; `write_schema_files` grava com `\n` final; `check_schema_files` compara por
  `difflib.unified_diff`.

## Pré-requisitos e pós-condições

| Primitiva | Pré-requisitos | Pós-condições |
| --- | --- | --- |
| `arrow_schema`, `delta_schema` | Toda coluna com tipo da tabela de [`schema.md`](schema.md). | Um campo por coluna, na ordem do modelo, com nulidade, comentário e `field_id`; `ContractError` na primeira coluna fora da tabela. |
| `table_options` | `partition_by` com uma coluna no máximo. | `partition_by` e `partition_source` preenchidos juntos ou ambos `None`; `keys` não vazia quando o modelo tem chave primária. |
| `ddl` | Dialeto `duckdb` ou `redshift`. | Texto que o motor aceita como está: o DuckDB em memória o executa no teste, o Redshift o compila; nenhuma chave, `DEFERRABLE` ou `Identity` no texto; o prefixo sem aspas. |
| `cast` | `data` com pelo menos uma coluna do contrato. | O mesmo tipo de entrada, só com colunas do contrato, na ordem do contrato, cada uma no tipo do contrato e com a nulidade conferida; ou `ContractError` sem nada convertido. Colunas ausentes ficam para o `INSERT ... BY NAME` do `loader` ou para a recusa de `publish_partition`. |
| `check_models` | Modelos importáveis. | Lista vazia no modelo cliente; cada violação nomeia tabela e coluna. |
| `write_schema_files`, `check_schema_files` | Pasta gravável, ou existente para o `check`. | Um arquivo por tabela e formato; o `check` devolve o diff sem gravar. |

## Testes por caso

`tests/test_schema.py`, sem gravar, sobre o modelo cliente e sobre um modelo de teste com todos os
tipos da tabela de [`schema.md`](schema.md).

| Caso | Teste | O que confere |
| --- | --- | --- |
| Tipos do contrato | `test_arrow_schema_maps_every_contract_type` | Cada tipo do modelo de teste no Arrow esperado; `DateTime` sem fuso em `timestamp[us]`, com fuso em `tz=UTC`; JSON e UUID em `string`. |
| Tipo fora do contrato | `test_arrow_schema_refuses_foreign_types` | `LargeBinary`, `ARRAY` e `Interval` levantam `ContractError` com tabela e coluna. |
| Esquema Delta | `test_delta_schema_json_matches_versioned_file` | `to_json()` igual ao `tests/client_model/schema/<tabela>.delta.json`. |
| Opções físicas | `test_table_options_defaults_and_keys` | Tabela sem `info` dá `partition_by=None` e `keys` só da chave primária; o índice único do modelo entra em `keys`; `keys["drop"]` remove; duas colunas de partição são `ContractError`. |
| DDL por dialeto | `test_ddl_per_dialect_without_keys` | Sem `PRIMARY KEY`, `UNIQUE`, `REFERENCES`, `DEFERRABLE`, `SERIAL` ou `IDENTITY`; `SORTKEY`, `DISTSTYLE` e `DISTKEY` só no Redshift; `CHECK` só no DuckDB; `Text` em `VARCHAR(65535)` e JSON em `SUPER` no Redshift; `Uuid` em `VARCHAR(36)` nos dois. |
| DDL executável | `test_ddl_runs_in_duckdb_memory` | O DDL de cada modelo executa num DuckDB em memória e `information_schema.columns` devolve os tipos do contrato. |
| Prefixo | `test_ddl_prefix_without_quotes` | `prefix="exec_42_"` e `prefix="{prefix}"` saem sem aspas nos dois dialetos. |
| Cast que aceita | `test_cast_reorders_and_normalizes` | Colunas fora de ordem, `large_string`, `timestamp[ns]` com nanossegundo zero, `int64` em `Numeric`, coluna a mais ignorada; o mesmo para `pa.Table`, `pa.RecordBatch` e `RecordBatchReader`. |
| Cast que recusa | `test_cast_refuses_each_loss`, parametrizado | Nulo em `NOT NULL`, `double` fora da escala, `timestamp` com hora em `Date`, `struct` em JSON, texto acima de `String(n)` em bytes, escala perdida, nanossegundo não nulo, estouro de inteiro; cada mensagem cita tabela e coluna. |
| Cast que preserva | `test_cast_keeps_doubles_of_the_reference_model` | `Double` do modelo de referência entra sem arredondamento. |
| Modelos | `test_check_models_finds_each_violation`, `test_client_model_is_clean` | Um modelo com cada defeito produz uma violação por defeito; o modelo cliente produz lista vazia. |
| Cópia fiel | `tests/test_client_model.py`, escrito em 2026-09-21 | O modelo cliente tem as tabelas e as colunas do modelo de referência, na mesma ordem, com a coluna de partição no fim; só os tipos e as chaves previstos mudam; sem `DEFERRABLE`, `autoincrement` nem índice não único; todo comentário presente; a partição declarada é a da base. |
| Arquivos gerados | `test_schema_files_match_versioned`, `test_check_schema_files_reports_a_changed_model` | Diff vazio contra `tests/client_model/schema/`; uma coluna acrescentada aparece no diff dos três formatos. |
| Gravação | `test_write_schema_files` (`local`) | Os arquivos sob a raiz local, com os nomes previstos. |

## Rascunhos executados

O rascunho define as primitivas sobre um modelo de exemplo com `partition_by`, `partition_source`,
`sort_key` e `redshift` em `Table.info`, executa o DDL no DuckDB em memória e exercita `cast` e
`check_models`. Ele rodou em 2026-09-21 com as versões fixadas.

```python
"""Etapa 1: o esquema Arrow e Delta, o DDL por dialeto, as opções físicas, o cast por lote e a conferência dos modelos."""
import dataclasses
import datetime as dt
import decimal
import json

import duckdb
import duckdb_engine
import pyarrow as pa
import pyarrow.compute as pc
import sqlalchemy as sa
from deltalake import Schema as DeltaSchema
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.schema import CreateTable
from sqlalchemy.sql import quoted_name
from sqlalchemy_redshift.dialect import SUPER, RedshiftDialect_redshift_connector


class Base(DeclarativeBase):
    pass


class Operacao(Base):
    __tablename__ = "cad_operacoes"
    __table_args__ = (
        sa.UniqueConstraint("data", "operacao"),
        {"comment": "Operações", "info": {"serialize_db": {
            "partition_by": ["data_str"], "partition_source": "data",
            "sort_key": ["data", "id_operacao"], "redshift": {"diststyle": "KEY", "distkey": "id_operacao"}}}},
    )
    id_operacao: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=False, comment="Identificador")
    data: Mapped[dt.date] = mapped_column(sa.Date, nullable=False, comment="Data da operação")
    operacao: Mapped[str] = mapped_column(sa.String(100), nullable=False, comment="Código")
    valor: Mapped[decimal.Decimal] = mapped_column(sa.Numeric(18, 2), nullable=False, comment="Valor")
    spread: Mapped[float | None] = mapped_column(sa.Double, comment="Spread")
    observacao: Mapped[str | None] = mapped_column(sa.Text, comment="Texto longo")
    meta: Mapped[dict | None] = mapped_column(sa.JSON().with_variant(SUPER(), "redshift"), comment="Documento")
    carimbo: Mapped[dt.datetime | None] = mapped_column(sa.DateTime, comment="Gravação")
    data_str: Mapped[str] = mapped_column(sa.String(10), nullable=False, comment="Partição AAAA-MM-DD de data")


class ContractError(ValueError):
    """Dados ou modelo fora do contrato; a mensagem diz o que o cliente faz antes de chamar de novo."""


ARROW_TYPES = {sa.SmallInteger: pa.int16(), sa.Integer: pa.int32(), sa.BigInteger: pa.int64(), sa.Boolean: pa.bool_(),
               sa.Double: pa.float64(), sa.Date: pa.date32(), sa.String: pa.string(), sa.Text: pa.string(),
               sa.Uuid: pa.string(), sa.JSON: pa.string()}


def arrow_type(column: sa.Column) -> pa.DataType:
    kind = column.type
    if isinstance(kind, sa.Numeric) and not isinstance(kind, sa.Float):
        return pa.decimal128(kind.precision or 18, kind.scale or 0)
    if isinstance(kind, sa.DateTime):
        return pa.timestamp("us", tz="UTC" if kind.timezone else None)
    for sa_type in (sa.BigInteger, sa.SmallInteger, sa.Integer, sa.Boolean, sa.Double, sa.Date, sa.Text, sa.Uuid, sa.JSON, sa.String):
        if isinstance(kind, sa_type):
            return ARROW_TYPES[sa_type]
    raise ContractError(f"{column.table.name}.{column.name}: tipo fora do contrato: {kind!r}")


def arrow_schema(table: sa.Table) -> pa.Schema:
    fields = []
    for index, column in enumerate(table.columns, start=1):
        metadata = {"PARQUET:field_id": str(index)} | ({"comment": column.comment} if column.comment else {})
        fields.append(pa.field(column.name, arrow_type(column), nullable=column.nullable, metadata=metadata))
    return pa.schema(fields, metadata={"serialize_db_table": table.name})


def delta_schema(table: sa.Table) -> DeltaSchema:
    return DeltaSchema.from_arrow(arrow_schema(table))


@dataclasses.dataclass(frozen=True)
class TableOptions:
    partition_by: str | None
    partition_source: str | None
    sort_key: tuple[str, ...]
    redshift: dict[str, str]
    keys: tuple[tuple[str, ...], ...]   # a chave primária e as únicas do modelo, mais o que `keys` acrescenta


def table_options(table: sa.Table) -> TableOptions:
    info = table.info.get("serialize_db", {})
    partition = info.get("partition_by") or []
    if len(partition) > 1:
        raise ContractError(f"{table.name}: uma coluna de partição no máximo, recebidas {partition}")
    keys = [tuple(c.name for c in table.primary_key.columns)] if table.primary_key.columns else []
    keys += [tuple(c.name for c in constraint.columns) for constraint in table.constraints if isinstance(constraint, sa.UniqueConstraint)]
    keys += [tuple(key) for key in info.get("keys", {}).get("add", [])]
    keys = [key for key in keys if list(key) not in info.get("keys", {}).get("drop", [])]
    return TableOptions(partition[0] if partition else None, info.get("partition_source"), tuple(info.get("sort_key", [])), dict(info.get("redshift", {})), tuple(keys))


DIALECTS = {"duckdb": duckdb_engine.Dialect(paramstyle="named"), "redshift": RedshiftDialect_redshift_connector(paramstyle="named")}


@compiles(sa.Text, "redshift")
def text_as_widest_varchar(element, compiler, **kw):
    return "VARCHAR(65535)"           # TEXT no Redshift seria VARCHAR(256)


@compiles(sa.Uuid, "redshift")
@compiles(sa.Uuid, "duckdb")
def uuid_as_varchar(element, compiler, **kw):
    return "VARCHAR(36)"              # o contrato guarda UUID como texto nos dois motores


@compiles(CreateTable, "redshift")
def create_table_with_physical_options(element, compiler, **kw):
    text = compiler.visit_create_table(element, **kw).rstrip()
    options = table_options(element.element)
    clauses = ([f"DISTSTYLE {options.redshift['diststyle']}"] if options.redshift.get("diststyle") else []) \
        + ([f"DISTKEY ({options.redshift['distkey']})"] if options.redshift.get("distkey") else []) \
        + ([f"SORTKEY ({', '.join(options.sort_key)})"] if options.sort_key else [])
    return f"{text} {' '.join(clauses)}\n\n" if clauses else f"{text}\n\n"


def ddl(table: sa.Table, dialect: str, prefix: str = "", keys: bool = False) -> str:
    """CREATE TABLE do sandbox: colunas, NOT NULL e comentários; chaves só com keys=True; CHECK só no DuckDB."""
    columns = [sa.Column(c.name, c.type, nullable=c.nullable, comment=c.comment, primary_key=keys and c.primary_key) for c in table.columns]
    checks = [sa.CheckConstraint(c.sqltext) for c in table.constraints if isinstance(c, sa.CheckConstraint)] if dialect == "duckdb" else []
    copy = sa.Table(quoted_name(f"{prefix}{table.name}", quote=False), sa.MetaData(), *columns, *checks, comment=table.comment, info=table.info)
    return str(CreateTable(copy).compile(dialect=DIALECTS[dialect])).strip()


def cast(data, table: sa.Table):
    """O lote ou a tabela no esquema do contrato; um RecordBatchReader sai como leitor que converte lote a lote."""
    if isinstance(data, pa.RecordBatchReader):
        schema = cast(data.schema.empty_table(), table).schema
        return pa.RecordBatchReader.from_batches(schema, (cast(batch, table) for batch in data))
    contract = arrow_schema(table)
    present = [field for field in contract if field.name in data.schema.names]
    if not present:
        raise ContractError(f"{table.name}: nenhuma coluna do contrato em {data.schema.names}")
    columns, fields = [], []
    for field in present:
        column, kind = data.column(field.name), data.schema.field(field.name).type
        if pa.types.is_floating(kind) and pa.types.is_decimal(field.type):
            if not pc.all(pc.equal(pc.round(column, field.type.scale), column)).as_py():
                raise ContractError(f"{table.name}.{field.name}: double fora da escala {field.type.scale}; arredonde no cliente antes de chamar")
        if pa.types.is_integer(kind) and pa.types.is_decimal(field.type):
            column = column.cast(pa.decimal128(field.type.precision + 3, field.type.scale))
        if pa.types.is_timestamp(kind) and pa.types.is_date(field.type):
            if not pc.all(pc.equal(column.cast(field.type).cast(kind), column)).as_py():
                raise ContractError(f"{table.name}.{field.name}: timestamp com hora numa coluna Date; trunque no cliente")
        if pa.types.is_nested(kind) and isinstance(table.c[field.name].type, sa.JSON):
            raise ContractError(f"{table.name}.{field.name}: documento JSON como {kind}; serialize com json.dumps antes de chamar")
        limit = getattr(table.c[field.name].type, "length", None)
        if limit and pa.types.is_string(field.type) and pa.types.is_string(kind) and (pc.max(pc.binary_length(column)).as_py() or 0) > limit:
            raise ContractError(f"{table.name}.{field.name}: texto acima de String({limit}) em bytes")
        columns.append(column)
        fields.append(field)
    try:                                                   # nomes já na ordem; nulo em NOT NULL e perda de precisão recusados aqui
        return type(data).from_arrays([c.cast(f.type, safe=True) for c, f in zip(columns, fields)], schema=pa.schema(fields, metadata=contract.metadata)).cast(pa.schema(fields, metadata=contract.metadata), safe=True)
    except (pa.ArrowInvalid, ValueError) as error:
        raise ContractError(f"{table.name}: {error}") from None


def check_models(metadata: sa.MetaData) -> list[str]:
    problems = []
    for table in metadata.sorted_tables:
        options = table_options(table)
        for column in table.columns:
            try:
                arrow_type(column)
            except ContractError as error:
                problems.append(str(error))
            if column.primary_key and isinstance(column.type, sa.Integer) and column.autoincrement in ("auto", True):   # o padrão "auto" emite SERIAL no duckdb_engine
                problems.append(f"{table.name}.{column.name}: chave inteira com autoincrement; declare autoincrement=False")
            if column.identity is not None:
                problems.append(f"{table.name}.{column.name}: Identity fora do contrato")
            if not column.comment:
                problems.append(f"{table.name}.{column.name}: coluna sem comentário")
        for constraint in table.foreign_key_constraints:
            if constraint.deferrable or constraint.initially:
                problems.append(f"{table.name}: chave estrangeira DEFERRABLE em {[c.name for c in constraint.columns]}")
        if options.partition_by:
            column = table.c.get(options.partition_by)
            if column is None:
                problems.append(f"{table.name}: partition_by aponta {options.partition_by}, que a tabela não tem")
            elif not (isinstance(column.type, sa.String) and column.type.length == 10):
                problems.append(f"{table.name}.{options.partition_by}: coluna de partição fora de String(10)")
            if not options.partition_source or options.partition_source not in table.c:
                problems.append(f"{table.name}: partition_by sem partition_source válido")
        if not options.keys:
            problems.append(f"{table.name}: sem chave primária e sem keys")
    return problems


schema = arrow_schema(Operacao.__table__)
print("arrow:", [f"{f.name}:{f.type}{'' if f.nullable else '!'}" for f in schema])
print("delta:", {f["name"]: f["type"] for f in json.loads(delta_schema(Operacao.__table__).to_json())["fields"]})
print("options:", table_options(Operacao.__table__))
print("-- duckdb\n" + ddl(Operacao.__table__, "duckdb"))
print("-- redshift\n" + ddl(Operacao.__table__, "redshift", prefix="exec_42_"))
con = duckdb.connect()
con.execute(ddl(Operacao.__table__, "duckdb"))
print("duckdb:", con.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'cad_operacoes'").fetchall())

# Um lote com as colunas fora de ordem, large_string, nanossegundos zerados e inteiro em Numeric entra no contrato.
batch = pa.RecordBatch.from_pydict({
    "operacao": pa.array(["a", "b"], pa.large_string()), "id_operacao": pa.array([1, 2], pa.int32()),
    "valor": pa.array([10, 20], pa.int64()), "data": pa.array([dt.datetime(2026, 8, 31), dt.datetime(2026, 8, 31)], pa.timestamp("ns")),
    "data_str": ["2026-08-31", "2026-08-31"], "extra": [1, 2]})
done = cast(batch, Operacao.__table__)
print("cast:", done.schema.names, [str(t) for t in done.schema.types], done.column("valor").to_pylist())

# As recusas, cada uma com a instrução ao cliente.
def refused(description, batch):
    try:
        cast(batch, Operacao.__table__)
        print(f"{description}: aceito")
    except ContractError as error:
        print(f"{description}: {str(error)[:110]}")

refused("nulo em NOT NULL", pa.RecordBatch.from_pydict({"id_operacao": pa.array([1, None], pa.int64())}))
refused("double fora da escala", pa.RecordBatch.from_pydict({"valor": pa.array([1.236])}))
refused("double na escala", pa.RecordBatch.from_pydict({"valor": pa.array([1.25, 2.5])}))
refused("hora numa coluna Date", pa.RecordBatch.from_pydict({"data": pa.array([dt.datetime(2026, 8, 31, 12)], pa.timestamp("us"))}))
refused("struct em JSON", pa.RecordBatch.from_pydict({"meta": pa.array([{"k": 1}])}))
refused("texto acima de String(100)", pa.RecordBatch.from_pydict({"operacao": pa.array(["x" * 101])}))
refused("precisão perdida", pa.RecordBatch.from_pydict({"valor": pa.array([decimal.Decimal("1.234")], pa.decimal128(20, 3))}))
refused("nanossegundo não nulo", pa.RecordBatch.from_pydict({"carimbo": pa.array([1], pa.timestamp("ns"))}))
print("nanossegundo nulo:", cast(pa.RecordBatch.from_pydict({"carimbo": pa.array([1000], pa.timestamp("ns"))}), Operacao.__table__).schema.field("carimbo").type)


class Ruim(Base):
    __tablename__ = "ruim"
    __table_args__ = {"info": {"serialize_db": {"partition_by": ["mes"]}}}
    id: Mapped[int] = mapped_column(sa.Integer, primary_key=True)
    id_operacao: Mapped[int] = mapped_column(sa.ForeignKey("cad_operacoes.id_operacao", deferrable=True, initially="DEFERRED"))
    mes: Mapped[str] = mapped_column(sa.String(7))
    peso: Mapped[bytes] = mapped_column(sa.LargeBinary)


print("check_models:")
for problem in check_models(Base.metadata):
    print("  -", problem)
```

Saída:

```
arrow: ['id_operacao:int64!', 'data:date32[day]!', 'operacao:string!', 'valor:decimal128(18, 2)!', 'spread:double', 'observacao:string', 'meta:string', 'carimbo:timestamp[us]', 'data_str:string!']
delta: {'id_operacao': 'long', 'data': 'date', 'operacao': 'string', 'valor': 'decimal(18,2)', 'spread': 'double', 'observacao': 'string', 'meta': 'string', 'carimbo': 'timestamp_ntz', 'data_str': 'string'}
options: TableOptions(partition_by='data_str', partition_source='data', sort_key=('data', 'id_operacao'), redshift={'diststyle': 'KEY', 'distkey': 'id_operacao'}, keys=(('id_operacao',), ('data', 'operacao')))
-- duckdb
CREATE TABLE cad_operacoes (
	id_operacao BIGINT NOT NULL,
	data DATE NOT NULL,
	operacao VARCHAR(100) NOT NULL,
	valor NUMERIC(18, 2) NOT NULL,
	spread DOUBLE PRECISION,
	observacao TEXT,
	meta JSON,
	carimbo TIMESTAMP,
	data_str VARCHAR(10) NOT NULL
)
-- redshift
CREATE TABLE exec_42_cad_operacoes (
	...
	observacao VARCHAR(65535),
	meta SUPER,
	carimbo TIMESTAMP,
	data_str VARCHAR(10) NOT NULL
) DISTSTYLE KEY DISTKEY (id_operacao) SORTKEY (data, id_operacao)
duckdb: [('id_operacao', 'BIGINT'), ('data', 'DATE'), ('operacao', 'VARCHAR'), ('valor', 'DECIMAL(18,2)'), ('spread', 'DOUBLE'), ('observacao', 'VARCHAR'), ('meta', 'JSON'), ('carimbo', 'TIMESTAMP'), ('data_str', 'VARCHAR')]
cast: ['id_operacao', 'data', 'operacao', 'valor', 'data_str'] ['int64', 'date32[day]', 'string', 'decimal128(18, 2)', 'string'] [Decimal('10.00'), Decimal('20.00')]
nulo em NOT NULL: cad_operacoes: Casting field 'id_operacao' with null values to non-nullable
double fora da escala: cad_operacoes.valor: double fora da escala 2; arredonde no cliente antes de chamar
double na escala: aceito
hora numa coluna Date: cad_operacoes.data: timestamp com hora numa coluna Date; trunque no cliente
struct em JSON: cad_operacoes.meta: documento JSON como struct<k: int64>; serialize com json.dumps antes de chamar
texto acima de String(100): cad_operacoes.operacao: texto acima de String(100) em bytes
precisão perdida: cad_operacoes: Rescaling Decimal value would cause data loss
nanossegundo não nulo: cad_operacoes: Casting from timestamp[ns] to timestamp[us] would lose data: 1
nanossegundo nulo: timestamp[us]
check_models:
  - ruim.id: chave inteira com autoincrement; declare autoincrement=False
  - ruim.id: coluna sem comentário
  - ruim.id_operacao: coluna sem comentário
  - ruim.mes: coluna sem comentário
  - ruim.peso: tipo fora do contrato: LargeBinary()
  - ruim.peso: coluna sem comentário
  - ruim: chave estrangeira DEFERRABLE em ['id_operacao']
  - ruim.mes: coluna de partição fora de String(10)
  - ruim: partition_by sem partition_source válido
```

## Decisões pendentes

- **[decisão] `Text` como `VARCHAR(65535)` no Redshift** por regra `@compiles`, em vez de exigir
  `String(65535)` nos modelos ([`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md)).
- **[decisão] O comprimento de `String(n)` em bytes ou em caracteres.** O rascunho mede bytes, a
  medida do `VARCHAR(n)` do Redshift; um texto de `n` caracteres acentuados passaria na medida por
  caracteres e seria recusado pelo `COPY`. A auditoria da [etapa 4](PLAN-STAGE-4.md) usa a mesma
  medida (`octet_length` no Redshift, `strlen` no DuckDB).
- **[decisão] A coluna sem comentário como violação em `check_models`.** O modelo de referência não
  tem comentário algum; a regra obriga a etapa 1 a escrever um por coluna na cópia, o que é trabalho
  do dono do modelo.
- **[decisão] `duckdb-engine` e `sqlalchemy-redshift` como dependências de execução** enquanto `ddl`
  compilar pelo dialeto. A alternativa é `ddl` gerar o texto sem dialeto, com a tabela de tipos de
  [`schema.md`](schema.md), o que tira as duas dependências já na etapa 1 e deixa o SQLAlchemy só
  nos modelos e no `sql.render`.
- **[decisão] Os comprimentos de `String(n)` do modelo cliente.** Escolhidos das leituras com
  folga (`contrato` e `operacao` 50, os nomes 50 e 100, `numero` 20, `descricao` e `meta` 255,
  `area` e `departamento` 20, `to` 2, `fonte_familia` 3); sem `n`, o Redshift daria `VARCHAR(256)`
  e o `cast` não mediria nada.
- **[decisão] A `sort_key` de cada tabela particionada**, proposta no modelo cliente: `data,
  sistema, contrato` em `cad_contratos`; `data, operacao` em `cad_operacoes` e em
  `rel_contrato_operacao`; `data_base, data, id_conta` em `cad_lancamentos`. Ela fica fechada antes
  da migração adiantada ([`PLAN-STAGE-7.md`](PLAN-STAGE-7.md)).
- **[decisão] A distribuição no Redshift** (`diststyle`, `distkey`): o modelo cliente não declara
  `redshift`, o padrão `AUTO`, até a decisão.
- **[decisão] A chave estrangeira de `cad_contratos` para `rel_contrato_operacao`**, do original,
  referencia colunas não únicas, o que motor algum aceitaria; fica no modelo cliente como a regra
  que a auditoria verifica por anti-join (todo contrato está em alguma operação), ou sai.
- **[decisão] Os comentários do modelo cliente** são uma primeira redação para a revisão do dono do
  modelo; `um`, `to`, `meta`, `estagio` e `fonte` são os que o nome e os valores não explicam.
