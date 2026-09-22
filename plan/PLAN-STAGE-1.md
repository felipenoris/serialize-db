# Etapa 1: `schema`

A entrega e o critério de aceite desta etapa estão na tabela de etapas de [`PLAN.md`](PLAN.md), que
também fixa as decisões, as regras que toda etapa obedece e a ordem do trabalho.

O módulo `serialize_db.schema` deriva dos modelos tudo o que os outros módulos precisam saber sobre
uma tabela. O modelo de referência de `tests/reference_model/`, o modelo SQLAlchemy da base original
em Parquet particionado, fica como está (decisão do usuário de 2026-09-21); o modelo cliente, a
cópia dele em `tests/client_model/` (decisão do usuário de 2026-09-21: o modelo de dados que o
código cliente apresenta para usar a biblioteca), leva as correções que a biblioteca cliente faria:
`Base` importável de um módulo só, cada tabela declarada uma vez, `BigInteger` nas chaves primárias
inteiras e nas colunas que as referenciam, `autoincrement=False` nessas chaves, chaves estrangeiras
sem `DEFERRABLE` e sem a de `cad_contratos` para `rel_contrato_operacao` (o destino não é único,
porque o contrato está em N operações; decisão do usuário de 2026-09-21), a coluna de partição
`data_str` (`String(10)`, `AAAA-MM-DD` de `data`;
`data_base_str` de `data_base` em `cad_lancamentos`) no fim das quatro tabelas particionadas,
comentários de tabela e de coluna, uma primeira redação que o dono do modelo revisa no código
(decisão do usuário de 2026-09-21), e `Table.info["serialize_db"]` com `partition_by`,
`partition_source` e `sort_key`, como em `schema.md`. A `sort_key` de cada tabela particionada é
decisão do usuário de 2026-09-21: `data, sistema, contrato` em `cad_contratos`, `data, operacao`
em `cad_operacoes`, `data, sistema, contrato, operacao` em `rel_contrato_operacao` e `data_base,
id_mensuracao, id_veiculo, id_conta` em `cad_lancamentos`; a primeira coluna é a origem da
partição, constante dentro dela, que no Redshift poda a tabela publicada inteira. As colunas
numéricas continuam `Double` (as
decisões de 2026-09-20 estão nas premissas de [`PLAN.md`](PLAN.md)). `String(n)` leva o comprimento
tirado das leituras, com folga, e o dono do modelo o revisa no código (decisão do usuário de
2026-09-21); os índices não únicos e o `sqlite_strict` do original ficam de fora, porque motor
algum da biblioteca os usa; `redshift` fica ausente de `Table.info`, a distribuição `AUTO` (decisão
do usuário de 2026-09-21): a leitura de `svv_table_info` depois da primeira publicação
([etapa 8](PLAN-STAGE-8.md)) diz o que o Redshift atribuiu a cada tabela, e uma chave de
distribuição, se vier dessa leitura, entra por `ALTER TABLE`. `tests/test_client_model.py` confere a cópia contra o original: as tabelas e
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

O pacote da etapa está escrito: `serialize_db.errors` com `ContractError`, `serialize_db.schema`,
o subcomando `serialize-db schema` em `serialize_db.cli`, `tests/test_schema.py` e os arquivos
gerados em `tests/client_model/schema/`. A migração adiantada ([`PLAN-STAGE-7.md`](PLAN-STAGE-7.md), seção "A
migração adiantada") vem logo depois e usa cinco primitivas: `check_models` (o modelo aprovado
antes da carga), `arrow_schema` e `sql_type` (os `CAST` da consulta de cada partição),
`delta_schema` (o `DeltaTable.create`) e `table_options` (a coluna de partição, a origem dela e a
`sort_key`). Os tipos do modelo cliente ficam decididos antes da migração, porque mudá-los depois
é reescrever o Delta; a `sort_key`, que o log do Delta não guarda, só ordena os arquivos, e mudá-la
depois é reordenar as partições que interessam, mas a migração grava todas uma vez, e por isso ela
também ficou decidida antes (2026-09-21).

## Os identificadores entre aspas

Duas colunas do modelo cliente são palavras reservadas: `to`, de `cad_contratos`, no DuckDB
(`duckdb_keywords()` a classifica `reserved`, e `CREATE TABLE t (to VARCHAR(2))` falha com `Parser
Error: syntax error at or near "to"`) e no Redshift; e `timestamp`, de `cad_lancamentos`, no
Redshift (no DuckDB ela é `column_name`, aceita sem aspas). `examples/redshift_manifest.py` já cria
`cad_contratos` com `"to"` entre aspas. Todo identificador que a biblioteca emite, tabela ou coluna,
vai entre aspas duplas: no DDL desta etapa, na consulta da migração adiantada, no
`INSERT ... BY NAME` da [etapa 4](PLAN-STAGE-4.md), nas listas de colunas do `COPY` e nas consultas
do `UNLOAD` da [etapa 5](PLAN-STAGE-5.md). Os dois dialetos do SQLAlchemy citam `"to"` sozinhos, e o
do Redshift também `"timestamp"`, inclusive em `DISTKEY` e `SORTKEY` (leituras de 2026-09-21,
[`POC.md`](POC.md)); o texto gerado sem dialeto cita tudo por `quoted`. Os nomes do contrato são
minúsculos, e os dois motores leem o nome entre aspas como o mesmo nome sem aspas.

| Primitiva | O que faz |
| --- | --- |
| `arrow_type(column)` | O `pa.DataType` da coluna pela tabela de tipos de `schema.md`: `Numeric(p, s)` em `decimal128(p, s)`, `DateTime` em `timestamp[us]` (`tz=UTC` com fuso), os demais por `isinstance` na ordem que põe `BigInteger` e `SmallInteger` antes de `Integer` e `Text` antes de `String`; `Float`, `LargeBinary`, `ARRAY` e `Interval` são `ContractError`, com tabela e coluna. |
| `arrow_schema(table)` | O `pa.Schema` do `Table`: um campo por coluna, com a nulidade, o comentário em `metadata` do campo, `PARQUET:field_id` pela posição, e o nome da tabela em `serialize_db_table`; um campo JSON é `string`, sem a extensão `arrow.json`. |
| `delta_schema(table)` | O `deltalake.Schema` derivado do Arrow: `decimal(18,2)`, `timestamp_ntz` para `DateTime` sem fuso e `timestamp` para o com fuso, `string` para JSON e UUID, comentários preservados, sem o `PARQUET:field_id` do Arrow (com ele no esquema Delta, como `parquet.field.id`, o `delta_scan` do DuckDB lê toda coluna como nula, com qualquer escritor; leitura de 2026-09-21 em [`POC.md`](POC.md)). |
| `table_options(table)` | O `TableOptions` (`partition_by`, `partition_source`, `sort_key`, `redshift`, `keys`) lido de `Table.info["serialize_db"]` com os padrões da biblioteca: tabela sem partição quando `partition_by` está ausente, uma coluna de partição no máximo, `String(10)`, derivada por `strftime(partition_source, '%Y-%m-%d')`; `keys` são a chave primária, as `UniqueConstraint` e os índices únicos do modelo (`ix_contratos_data_sistema_contrato` e `ix_operacoes_data_operacao` no modelo cliente), mais `keys["add"]`, menos `keys["drop"]`, sempre por lista de colunas. |
| `sql_type(column, dialect)` | O nome do tipo no motor, pela tabela de tipos de `schema.md`: `DECIMAL(p, s)`, `VARCHAR(n)` nos dois (o DuckDB ignora o comprimento), `Text` em `VARCHAR` e `VARCHAR(65535)` (decisão do usuário de 2026-09-21), `Uuid` em `VARCHAR(36)`, JSON em `JSON` e `SUPER`, `Double` em `DOUBLE` e `DOUBLE PRECISION`, `DateTime` em `TIMESTAMP` e `TIMESTAMPTZ`; a migração adiantada o usa nos `CAST`. |
| `quoted(name)`, `column_ddl(column, dialect)` | O identificador entre aspas duplas, e a linha da coluna no `CREATE TABLE` (`"<coluna>" <tipo> [NOT NULL]`); públicas porque as etapas [4](PLAN-STAGE-4.md), [5](PLAN-STAGE-5.md) e [8](PLAN-STAGE-8.md) montam texto com identificadores e as variantes do DDL (a staging sem a coluna de partição, a tabela publicada com a chave primária informativa). |
| `ddl(table, dialect, prefix="")` | O `CREATE TABLE` do sandbox para `duckdb` ou `redshift`, gerado como texto sem o dialeto do SQLAlchemy: colunas, tipos por `sql_type` e `NOT NULL`, todo identificador entre aspas; sem chave, `DEFERRABLE`, `Identity`, `CHECK`, `DEFAULT` nem comentário (as chaves são da auditoria, e o comentário vai no esquema Delta); `DISTSTYLE`, `DISTKEY` e `SORTKEY` no Redshift, de `table_options`; `prefix` renomeia a tabela para o sandbox (`exec_<id>_`, ou o sentinela `{prefix}` da [etapa 2](PLAN-STAGE-2.md), que sai como `"{prefix}cad_operacoes"`). |
| `cast(data, table)` | A `pa.Table`, o `pa.RecordBatch` ou o `RecordBatchReader` lote a lote, convertido para `arrow_schema(table)` com `safe=True` e devolvido no mesmo tipo (`RecordBatch.cast` recusa o mesmo que `Table.cast`): as colunas do contrato presentes, na ordem do contrato; `large_string` para `string`, timestamps a microssegundos e UTC, inteiro em `Numeric` por `decimal128(p + 3, s)` (`decimal128(21, 2)` para `Numeric(18, 2)`); recusa perda de precisão, `double` fora da escala e `timestamp` com hora numa coluna `Date` (as duas perdas que `safe=True` não acusa), `struct` numa coluna JSON, texto acima de `String(n)` em bytes, texto acima de 65.535 bytes numa coluna `Text`, nulo em coluna `NOT NULL` e um lote sem coluna alguma do contrato, com a tabela, a coluna e a instrução ao cliente na mensagem. |
| `check_models(metadata)` | A lista de violações do contrato nos modelos, um texto por violação com tabela e coluna: tipo fora da tabela de tipos, `autoincrement` em chave inteira (o padrão `"auto"` inclusive), `Identity`, `String` sem comprimento, chave estrangeira `DEFERRABLE`, `partition_by` sem a coluna, com a coluna fora de `String(10)` ou sem `partition_source`, tabela sem chave primária e sem `keys`. Vazia no modelo cliente; no modelo de referência lista o `autoincrement` das 12 chaves, as 12 chaves estrangeiras `DEFERRABLE` (das 14), as 20 colunas `String` sem comprimento; o comentário de tabela e de coluna é opcional (decisão do usuário de 2026-09-21), e o da coluna, quando existe, vai para o esquema Arrow e para o Delta. |
| `schema_files(metadata)` | `{"<tabela>.delta.json": ..., "<tabela>.duckdb.sql": ..., "<tabela>.redshift.sql": ...}` em memória, cada texto com `\n` final; o `.delta.json` é o JSON canônico (chaves ordenadas, indentado), porque o `to_json()` do delta-rs serializa os metadados de cada campo em ordem arbitrária, que muda a cada geração, e grava `PARQUET:field_id` como `parquet.field.id` inteiro. |
| `write_schema_files(metadata, directory)` | Grava `schema_files` em `directory` e devolve os caminhos; `serialize-db schema write --metadata modulo:atributo <pasta>` grava. |
| `check_schema_files(metadata, directory)` | O diff unificado de cada arquivo versionado contra a geração nova, vazio quando nada mudou; `serialize-db schema check --metadata modulo:atributo <pasta>` compara sem gravar. |

Testes: `tests/test_schema.py`, sem gravar, sobre o modelo cliente, o modelo de referência e um
modelo de teste com todos os tipos; o DDL de cada tabela executado no DuckDB em memória; o diff dos
arquivos `tests/client_model/schema/` versionados; `write_schema_files` sob a raiz local.
Dependências de execução: `sqlalchemy`, `pyarrow`, `deltalake` e `duckdb`; `duckdb-engine` e
`sqlalchemy-redshift` ficam no grupo `dev`, das suítes de estudo, porque a etapa não compila pelo
dialeto (decisão do usuário de 2026-09-21). Provas de conceito: `test_sqlalchemy.py`
(`test_declarative_model_exposes_table`, `test_ddl_per_dialect`, `test_create_all_and_reflection`,
`test_arrow_and_delta_schema_from_table`, com o mapa de tipos e `Schema.from_arrow().to_json()`,
`test_sandbox_copy_of_table_and_schema_files_diff`), `test_pyarrow.py`
(`test_schema_metadata_and_from_pylist`, `test_safe_cast_refuses_data_loss`, com as perdas que o
cast seguro não acusa, `test_arrow_table_round_trips_through_pandas_without_copy` e
`test_record_batch_cast_and_conversions_share_buffers`, o mesmo por lote) e `test_stdlib.py`
(`test_generated_files_diff`, `test_decimal_totals`, `test_entry_point_by_import_string`).

## Interface

As exceções da biblioteca vivem em `serialize_db.errors`, um módulo sem dependências, porque
`delta` levanta `ExecutionConflict` e `execution` a captura, e um módulo por exceção criaria
importações cíclicas. `ContractError` é a primeira e a única desta etapa: dados ou modelo fora do
contrato, com a instrução ao cliente na mensagem; cada etapa acrescenta as suas.

```python
"""Assinaturas de serialize_db.schema; os corpos estão no rascunho abaixo."""
import dataclasses
from typing import Literal

import pyarrow as pa
import sqlalchemy as sa
from deltalake import Schema as DeltaSchema

from serialize_db.errors import ContractError   # ValueError: dados ou modelo fora do contrato

Dialect = Literal["duckdb", "redshift"]


@dataclasses.dataclass(frozen=True)
class TableOptions:
    partition_by: str | None           # a coluna de partição, String(10), AAAA-MM-DD
    partition_source: str | None       # a coluna de data de que ela deriva
    sort_key: tuple[str, ...]
    redshift: dict[str, str]           # diststyle, distkey
    keys: tuple[tuple[str, ...], ...]  # as chaves do modelo, mais keys["add"], menos keys["drop"]


def arrow_type(column: sa.Column) -> pa.DataType: ...
def arrow_schema(table: sa.Table) -> pa.Schema: ...
def delta_schema(table: sa.Table) -> DeltaSchema: ...
def table_options(table: sa.Table) -> TableOptions: ...
def sql_type(column: sa.Column, dialect: Dialect) -> str: ...
def quoted(name: str) -> str: ...
def column_ddl(column: sa.Column, dialect: Dialect) -> str: ...
def ddl(table: sa.Table, dialect: Dialect, prefix: str = "") -> str: ...
def cast(data: pa.Table | pa.RecordBatch | pa.RecordBatchReader, table: sa.Table) -> pa.Table | pa.RecordBatch | pa.RecordBatchReader: ...
def check_models(metadata: sa.MetaData) -> list[str]: ...
def schema_files(metadata: sa.MetaData) -> dict[str, str]: ...
def write_schema_files(metadata: sa.MetaData, directory: str) -> list[str]: ...
def check_schema_files(metadata: sa.MetaData, directory: str) -> list[str]: ...
```

O módulo segue a seção "Python Code Style" de `CLAUDE.md`: uma função por responsabilidade, com
anotação de tipo em toda assinatura e docstring com um exemplo em cada função pública (o exemplo do
módulo mostra `check_models(Base.metadata)`, `arrow_schema(table)`, `ddl(table, "duckdb")` e
`cast(batch, table)`); laços explícitos em vez de comprehensions com condição ou com dois `for`;
no máximo dois níveis de aninhamento, com retorno antecipado; sem regra `@compiles`, sem
`quoted_name` e sem despacho por `type(data)`. `cast` é a única função que recebe mais de um tipo,
e só despacha para `_cast_batch`, `_cast_table` e `_cast_reader`. O rascunho abaixo tem essa forma
e é a referência do módulo; as docstrings dele têm uma linha, e o módulo acrescenta o exemplo.
As assinaturas acima são o `__all__` do módulo, a interface pública que o `pdoc` documenta; o que
só o módulo usa leva o prefixo `_`, pela regra dos três níveis de [`PLAN.md`](PLAN.md). O
subcomando `serialize-db schema` recebe `--metadata modulo:atributo`, a convenção da
[etapa 6](PLAN-STAGE-6.md) para o `MetaData` dos modelos do pipeline.

## Estratégia de implementação

- **`arrow_type`** resolve `Numeric` e `DateTime` pelos parâmetros e os demais por `isinstance`
  sobre `_ARROW_TYPES`, uma tupla de pares na ordem que põe `BigInteger` e `SmallInteger` antes de
  `Integer`, e `Text` antes de `String`; `Float` e `Double` derivam de `Numeric` no SQLAlchemy, e
  só `Double` está na tupla. Um tipo fora da tabela de [`schema.md`](schema.md) é `ContractError`,
  não `TypeError`.
- **`arrow_schema`** monta um campo por coluna com `_arrow_field`, que numera `PARQUET:field_id`
  pela posição e guarda o comentário em `metadata` do campo, e põe o nome da tabela em `metadata`
  do esquema (`serialize_db_table`).
- **`delta_schema`** é `Schema.from_arrow` sobre o esquema Arrow com o `PARQUET:field_id` tirado
  de cada campo; o delta-rs deriva `timestamp_ntz` do `timestamp[us]` sem fuso e preserva o
  comentário. O `field_id` fica só no Arrow: no esquema Delta ele vira `parquet.field.id`, e com
  essa chave o `delta_scan` do DuckDB 1.5.5 lê toda coluna como nula, qualquer que seja o
  escritor do arquivo, com ou sem `field_id` nele (a migração adiantada o encontrou em 2026-09-21,
  [`POC.md`](POC.md)).
- **`table_options`** lê `Table.info["serialize_db"]` com os padrões da biblioteca; `_declared_keys`
  reúne `table.primary_key`, as `UniqueConstraint` e os `table.indexes` com `unique`, porque o
  modelo cliente declara `ix_contratos_data_sistema_contrato` e `ix_operacoes_data_operacao` por
  `Index(unique=True)`, e
  `_adjusted_keys` aplica `keys["add"]` e `keys["drop"]`, sempre por lista de colunas.
- **`sql_type` e `ddl`** geram texto, sem o dialeto do SQLAlchemy (decisão do usuário de
  2026-09-21: as três regras `@compiles` do desenho anterior, `Text` no Redshift, `Uuid` nos dois e
  `CreateTable` com as cláusulas físicas, eram registros globais no compilador, e a alternativa,
  compilar pelo dialeto, levaria `duckdb-engine` e `sqlalchemy-redshift` às dependências de
  execução; a escolha vale para `ddl`, e a [etapa 2](PLAN-STAGE-2.md) decide o `render` dos
  statements). `_SQL_TYPES` guarda por dialeto o
  nome dos tipos sem parâmetro, `sql_type` monta `DECIMAL(p, s)` da precisão e da escala do tipo
  Arrow, `VARCHAR(n)` do comprimento e `TIMESTAMP` ou `TIMESTAMPTZ` do fuso; `column_ddl` monta
  `"<coluna>" <tipo>` mais `NOT NULL`; `_redshift_options` monta `DISTSTYLE`, `DISTKEY` e `SORTKEY`
  de `table_options`, com os nomes entre aspas; `ddl` junta as linhas sob
  `CREATE TABLE "<prefixo><tabela>"`, o prefixo dentro das aspas. `Identity`, `server_default`,
  `CheckConstraint` e comentário não saem no texto; `check_models` reprova `Identity`. Sem o
  dialeto, o `with_variant(SUPER(), "redshift")` de [`schema.md`](schema.md) deixa de ser
  necessário no modelo: `sql_type` emite `SUPER` por `isinstance(kind, sa.JSON)`, e um modelo com
  a variante continua aceito. O DuckDB registra `DECIMAL(18, 2)`, `TIMESTAMPTZ`, `VARCHAR(n)` e
  `JSON` como `DECIMAL(18,2)`, `TIMESTAMP WITH TIME ZONE`, `VARCHAR` e `JSON` (rascunho abaixo).
- **`cast`** despacha por `isinstance` para `_cast_batch`, `_cast_table` e `_cast_reader`, e as três
  passam por `_contract_arrays`: `_contract_fields` seleciona as colunas do contrato presentes, na
  ordem do contrato, e `_contract_column` converte coluna a coluna, depois de
  `_refuse_silent_losses` recusar o que `safe=True` não acusa, numa função por recusa: `_refuse_double_out_of_scale` (`double` numa coluna `Numeric` só quando
  `pc.round(x, escala)` devolve o valor igual), `_refuse_timestamp_with_time` (`timestamp` numa coluna
  `Date` só quando a ida e volta devolve o valor igual), `_refuse_nested_json` (`struct`, `list` e
  `map` numa coluna JSON), `_refuse_text_above_length` (texto acima de `String(n)` medido em bytes
  por `pc.binary_length`, a medida do `VARCHAR(n)` do Redshift; decisão do usuário de
  2026-09-21, a mesma medida da auditoria da [etapa 4](PLAN-STAGE-4.md) e da migração
  adiantada, e `probes/parquet_source.py` relata o máximo em bytes ao lado do de caracteres) e `_refuse_text_above_varchar`
  (texto acima de 65.535 bytes numa coluna `Text`, que não declara `n`: o teto do `VARCHAR` do
  Redshift, que `sql_type` escreve no DDL); o inteiro numa coluna `Numeric`
  passa pelo desvio `decimal128(p + 3, s)`. Depois disso, `column.cast(field.type, safe=True)`
  recusa escala perdida, nanossegundo não nulo e estouro, e o `cast` do esquema sobre
  `from_arrays` recusa nulo em `NOT NULL`. Toda recusa sai como `ContractError` com a tabela, a
  coluna e a instrução. `_cast_reader` deriva o esquema de saída de `reader.schema.empty_table()` e
  embrulha `_cast_batches`, uma função geradora, em `RecordBatchReader.from_batches`; as duas
  conferências por `pc.all` levam `min_count=0`, porque `pc.all` de uma coluna vazia devolve nulo e
  a tabela vazia seria recusada (rascunho abaixo).
- **`check_models`** percorre `metadata.sorted_tables` e reúne, por tabela, `_column_problems`
  (tipo, `autoincrement`, `Identity`, `String` sem comprimento), `_key_problems`
  (`DEFERRABLE`, tabela sem chave) e `_partition_problems` (a coluna, `String(10)`,
  `partition_source`), uma lista de textos, um por violação, para o teste do modelo cliente ser
  `assert check_models(Base.metadata) == []`. O `autoincrement` padrão é a string `"auto"`, não
  `True`: a regra reprova os dois numa chave inteira. `String` sem comprimento é violação por
  decisão do usuário de 2026-09-21: sem `n`, o Redshift daria `VARCHAR(256)` e `cast` não mediria
  nada, e `Text` é a forma sem `n`, cujo teto é o do `VARCHAR` do Redshift, 65.535 bytes, medido por `cast` (decisão do usuário de 2026-09-21). O comentário de tabela e de coluna
  não é violação, por decisão do usuário de 2026-09-21: a biblioteca não obriga o dono do modelo a
  documentá-lo, e o comentário da coluna, quando existe, vai para o esquema Arrow e para o Delta.
  O modelo de referência é o modelo com defeitos do teste: o `autoincrement` das 12 chaves, as 12
  chaves estrangeiras `DEFERRABLE` (das 14) e os `String` sem comprimento.
- **`schema_files`** gera `<tabela>.delta.json` por `delta_schema(...).to_json()` e os dois `.sql`
  por `ddl`, cada texto com `\n` final; `write_schema_files` grava e devolve os caminhos;
  `check_schema_files` compara por `difflib.unified_diff`. `serialize-db schema write` e
  `serialize-db schema check` resolvem `--metadata modulo:atributo` por `importlib.import_module` e
  `getattr` (`test_stdlib.py::test_entry_point_by_import_string`); o `check` sai com 0 sem diff, 1
  com diff impresso, 2 no erro de uso do `argparse`.

## Pré-requisitos e pós-condições

| Primitiva | Pré-requisitos | Pós-condições |
| --- | --- | --- |
| `arrow_schema`, `delta_schema` | Toda coluna com tipo da tabela de [`schema.md`](schema.md). | Um campo por coluna, na ordem do modelo, com nulidade, comentário e `field_id`; `ContractError` na primeira coluna fora da tabela. |
| `table_options` | `partition_by` com uma coluna no máximo. | `partition_by` e `partition_source` preenchidos juntos ou ambos `None`; `keys` não vazia quando o modelo tem chave primária, única ou índice único. |
| `sql_type`, `ddl` | Dialeto `duckdb` ou `redshift`. | Texto que o motor aceita como está: o DuckDB em memória o executa no teste, as 12 tabelas do modelo cliente inclusive, e o Redshift o executa na primeira carga da [etapa 5](PLAN-STAGE-5.md); nenhuma chave, `DEFERRABLE` ou `Identity` no texto; todo identificador entre aspas, o prefixo dentro delas. |
| `cast` | `data` com pelo menos uma coluna do contrato. | O mesmo tipo de entrada, só com colunas do contrato, na ordem do contrato, cada uma no tipo do contrato e com a nulidade conferida; ou `ContractError` sem nada convertido. Colunas ausentes ficam para o `INSERT ... BY NAME` do `loader` ou para a recusa de `publish_partition`; uma tabela vazia passa. |
| `check_models` | Modelos importáveis. | Lista vazia no modelo cliente; cada violação nomeia tabela e coluna; o modelo de referência produz as suas e nada mais. |
| `write_schema_files`, `check_schema_files` | Pasta gravável, ou existente para o `check`. | Um arquivo por tabela e formato; o `check` devolve o diff sem gravar. |

## Testes por caso

`tests/test_schema.py`, sem gravar, sobre o modelo cliente, o modelo de referência e um modelo de
teste com todos os tipos da tabela de [`schema.md`](schema.md), `to` e `timestamp` entre as colunas.

| Caso | Teste | O que confere |
| --- | --- | --- |
| Tipos do contrato | `test_arrow_schema_maps_every_contract_type` | Cada tipo do modelo de teste no Arrow esperado; `DateTime` sem fuso em `timestamp[us]`, com fuso em `tz=UTC`; JSON e UUID em `string`. |
| Tipo fora do contrato | `test_arrow_schema_refuses_foreign_types` | `Float`, `LargeBinary`, `ARRAY` e `Interval` levantam `ContractError` com tabela e coluna. |
| Esquema Delta | `test_delta_schema_json_matches_versioned_file` | O documento de `to_json()` igual ao `tests/client_model/schema/<tabela>.delta.json`, e o texto de `schema_files` igual ao arquivo, porque a ordem dos metadados em `to_json()` muda a cada geração. |
| Esquema Delta sem `field_id` | `test_delta_schema_carries_no_field_id` | Nenhum campo do esquema Delta do modelo cliente traz `parquet.field.id`, todos trazem o comentário, e o Arrow continua com `PARQUET:field_id` (leitura de 2026-09-21: com a chave no esquema Delta, o `delta_scan` lê toda coluna como nula). |
| Opções físicas | `test_table_options_defaults_and_keys` | Tabela sem `info` dá `partition_by=None` e `keys` só da chave primária; o índice único de `cad_contratos` e a `UniqueConstraint` de `cad_aliquotas` entram em `keys`; `keys["add"]` acrescenta e `keys["drop"]` remove; duas colunas de partição são `ContractError`. |
| Tipos por dialeto | `test_sql_type_per_dialect` | Cada tipo do modelo de teste no texto da tabela de tipos: `DECIMAL(18, 2)`, `VARCHAR(100)`, `VARCHAR` e `VARCHAR(65535)` para `Text`, `VARCHAR(36)`, `JSON` e `SUPER`, `DOUBLE` e `DOUBLE PRECISION`, `TIMESTAMP` e `TIMESTAMPTZ`. |
| DDL por dialeto | `test_ddl_per_dialect` | Sem `PRIMARY KEY`, `UNIQUE`, `REFERENCES`, `DEFERRABLE`, `SERIAL`, `IDENTITY` ou `CHECK`; `SORTKEY`, `DISTSTYLE` e `DISTKEY` só no Redshift, com os nomes entre aspas; o texto igual ao esperado, linha a linha. |
| Identificadores | `test_ddl_quotes_every_identifier` | `"to"` em `cad_contratos` e `"timestamp"` em `cad_lancamentos` nos dois dialetos; nenhum nome de tabela ou coluna sem aspas no texto. |
| DDL executável | `test_ddl_runs_in_duckdb_memory` | O DDL das 12 tabelas do modelo cliente e o do modelo de teste executam num DuckDB em memória, e `information_schema.columns` devolve os tipos do contrato (`DECIMAL(18,2)`, `TIMESTAMP WITH TIME ZONE`, `JSON`). |
| Prefixo | `test_ddl_prefix_inside_the_quotes` | `prefix="exec_42_"` e `prefix="{prefix}"` saem como `"exec_42_cad_operacoes"` e `"{prefix}cad_operacoes"` nos dois dialetos, e o segundo executa no DuckDB. |
| Cast que aceita | `test_cast_reorders_and_normalizes` | Colunas fora de ordem, `large_string`, `timestamp[ns]` com nanossegundo zero, `int64` em `Numeric`, coluna a mais ignorada; o mesmo para `pa.Table` e `pa.RecordBatch`. |
| Cast por leitor | `test_cast_reader_converts_batch_by_batch` | Um leitor de dois lotes sai como `RecordBatchReader` com o esquema do contrato e as linhas dos dois; a tabela vazia de `reader.schema.empty_table()` passa nas conferências. |
| Cast que recusa | `test_cast_refuses_each_loss`, parametrizado | Nulo em `NOT NULL`, `double` fora da escala, `timestamp` com hora em `Date`, `struct` em JSON, texto acima de `String(n)` em bytes, escala perdida, nanossegundo não nulo, estouro de inteiro, lote sem coluna do contrato; cada mensagem cita a tabela e a coluna. |
| Teto de `Text` | `test_cast_measures_text_against_the_varchar_ceiling` | Numa coluna `Text`, 65.535 bytes passam e 65.536 são `ContractError` com a tabela, a coluna e o tamanho lido. |
| Cast que preserva | `test_cast_keeps_doubles_of_the_reference_model` | `Double` do modelo de referência entra sem arredondamento. |
| Modelos | `test_check_models_finds_each_violation`, `test_check_models_lists_the_reference_model_defects`, `test_client_model_is_clean` | Um modelo com cada defeito produz uma violação por defeito; o modelo de referência produz o `autoincrement` das 12 chaves, as 12 chaves estrangeiras `DEFERRABLE` (das 14) e os 20 `String` sem comprimento, e nada mais, nem por comentário ausente; o modelo cliente produz lista vazia. |
| Cópia fiel | `tests/test_client_model.py` | O modelo cliente tem as tabelas e as colunas do modelo de referência, na mesma ordem, com a coluna de partição no fim; só os tipos e as chaves previstos mudam; sem `DEFERRABLE`, `autoincrement` nem índice não único; todo comentário presente; a partição declarada é a da base. |
| Arquivos gerados | `test_schema_files_match_versioned`, `test_check_schema_files_reports_a_changed_model` | Diff vazio contra `tests/client_model/schema/`; uma coluna acrescentada aparece no diff dos três formatos. |
| Gravação | `test_write_schema_files` (`local`) | Os arquivos sob a raiz local, com os nomes previstos. |
| Linha de comando | `test_cli_schema_check_reads_the_versioned_files` | `serialize-db schema check --metadata client_model:Base.metadata tests/client_model/schema` sai com 0 sem gravar; um modelo mudado sai com 1 e imprime o diff; sem `--metadata` sai com 2. |

## Rascunhos executados

O rascunho define as primitivas sobre um modelo de exemplo com todos os tipos do contrato, a chave
única por índice, `partition_by`, `partition_source`, `sort_key` e `redshift` em `Table.info`, e as
colunas `to` e `timestamp`; executa o DDL no DuckDB em memória, com o prefixo e com o sentinela, e
exercita `cast` nos três tipos de entrada e `check_models`. Ele tem a forma do módulo e rodou em
2026-09-21 com as versões fixadas.

```python
"""Etapa 1: esquema Arrow e Delta, opções físicas, DDL, cast e a conferência dos modelos."""

import dataclasses
import datetime as dt
import decimal
import json
import uuid
from typing import Literal

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import sqlalchemy as sa
from deltalake import Schema as DeltaSchema
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

Dialect = Literal["duckdb", "redshift"]


class ContractError(ValueError):
    """Dados ou modelo fora do contrato; a mensagem diz o que o cliente faz antes de chamar."""


# O modelo de exemplo tem todos os tipos do contrato, a chave única por índice, a partição em
# Table.info, e as colunas `to` (reservada no DuckDB e no Redshift) e `timestamp` (no Redshift).
class Base(DeclarativeBase):
    pass


class Operacao(Base):
    __tablename__ = "cad_operacoes"
    __table_args__ = (
        sa.Index("ix_operacoes_data_operacao", "data", "operacao", unique=True),
        {"comment": "Operações", "info": {"serialize_db": {
            "partition_by": ["data_str"], "partition_source": "data",
            "sort_key": ["data", "operacao"],
            "redshift": {"diststyle": "KEY", "distkey": "id_operacao"}}}},
    )
    id_operacao: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=False,
                                             comment="Identificador")
    data: Mapped[dt.date] = mapped_column(sa.Date, comment="Data da operação")
    operacao: Mapped[str] = mapped_column(sa.String(100), comment="Código")
    to: Mapped[str] = mapped_column(sa.String(2), comment="Código TO")
    valor: Mapped[decimal.Decimal] = mapped_column(sa.Numeric(18, 2), comment="Valor")
    spread: Mapped[float | None] = mapped_column(sa.Double, comment="Spread")
    parcelas: Mapped[int | None] = mapped_column(sa.SmallInteger, comment="Parcelas")
    sistema: Mapped[int] = mapped_column(sa.Integer, comment="Sistema de origem")
    ativa: Mapped[bool] = mapped_column(sa.Boolean, comment="Se está ativa")
    observacao: Mapped[str | None] = mapped_column(sa.Text, comment="Texto longo")
    meta: Mapped[str | None] = mapped_column(sa.JSON, comment="Documento JSON serializado")
    timestamp: Mapped[dt.datetime] = mapped_column(sa.DateTime, comment="Gravação, sem fuso")
    carimbo_utc: Mapped[dt.datetime | None] = mapped_column(sa.DateTime(timezone=True),
                                                            comment="Gravação em UTC")
    chave: Mapped[uuid.UUID | None] = mapped_column(sa.Uuid, comment="UUID como texto")
    data_str: Mapped[str] = mapped_column(sa.String(10), comment="Partição AAAA-MM-DD de data")


# ---------------------------------------------------------------- o esquema Arrow e Delta

# A ordem importa: BigInteger e SmallInteger derivam de Integer, Text de String.
_ARROW_TYPES: tuple[tuple[type, pa.DataType], ...] = (
    (sa.BigInteger, pa.int64()),
    (sa.SmallInteger, pa.int16()),
    (sa.Integer, pa.int32()),
    (sa.Boolean, pa.bool_()),
    (sa.Double, pa.float64()),
    (sa.Date, pa.date32()),
    (sa.Text, pa.string()),
    (sa.Uuid, pa.string()),
    (sa.JSON, pa.string()),
    (sa.String, pa.string()),
)


def arrow_type(column: sa.Column) -> pa.DataType:
    """O tipo Arrow da coluna, pela tabela de tipos de plan/schema.md."""
    kind = column.type
    # Numeric leva precisão e escala; Float e Double derivam de Numeric e ficam fora deste ramo.
    if isinstance(kind, sa.Numeric) and not isinstance(kind, sa.Float):
        return pa.decimal128(kind.precision or 18, kind.scale or 0)
    if isinstance(kind, sa.DateTime):
        timezone = "UTC" if kind.timezone else None
        return pa.timestamp("us", tz=timezone)
    for sa_type, arrow in _ARROW_TYPES:
        if isinstance(kind, sa_type):
            return arrow
    raise ContractError(f"{column.table.name}.{column.name}: tipo fora do contrato: {kind!r}")


def _arrow_field(column: sa.Column, position: int) -> pa.Field:
    """O campo Arrow da coluna: tipo, nulidade, `PARQUET:field_id` pela posição, comentário."""
    metadata = {"PARQUET:field_id": str(position)}
    if column.comment:
        metadata["comment"] = column.comment
    return pa.field(column.name, arrow_type(column), nullable=column.nullable, metadata=metadata)


def arrow_schema(table: sa.Table) -> pa.Schema:
    """O esquema Arrow da tabela, com o nome dela em `serialize_db_table`."""
    fields = []
    for position, column in enumerate(table.columns, start=1):
        fields.append(_arrow_field(column, position))
    return pa.schema(fields, metadata={"serialize_db_table": table.name})


def delta_schema(table: sa.Table) -> DeltaSchema:
    """O esquema Delta derivado do Arrow: `timestamp_ntz` sem fuso, `timestamp` com fuso."""
    return DeltaSchema.from_arrow(arrow_schema(table))


# ---------------------------------------------------------------- as opções físicas

@dataclasses.dataclass(frozen=True)
class TableOptions:
    """O que `Table.info["serialize_db"]` declara, com os padrões da biblioteca."""

    partition_by: str | None           # a coluna de partição, String(10), AAAA-MM-DD
    partition_source: str | None       # a coluna de data de que ela deriva
    sort_key: tuple[str, ...]
    redshift: dict[str, str]           # diststyle, distkey
    keys: tuple[tuple[str, ...], ...]  # as chaves do modelo, mais keys["add"], menos keys["drop"]


def _column_names(columns) -> tuple[str, ...]:
    """Os nomes de uma coleção de colunas, na ordem dela."""
    return tuple(column.name for column in columns)


def _declared_keys(table: sa.Table) -> list[tuple[str, ...]]:
    """A chave primária, as `UniqueConstraint` e os índices únicos do modelo."""
    keys = []
    if table.primary_key.columns:
        keys.append(_column_names(table.primary_key.columns))
    for constraint in table.constraints:
        if isinstance(constraint, sa.UniqueConstraint):
            keys.append(_column_names(constraint.columns))
    for index in table.indexes:
        if index.unique:
            keys.append(_column_names(index.columns))
    return keys


def _adjusted_keys(keys: list[tuple[str, ...]], info: dict) -> tuple[tuple[str, ...], ...]:
    """As chaves do modelo mais `keys["add"]` e menos `keys["drop"]` de `Table.info`."""
    adjustments = info.get("keys", {})
    for key in adjustments.get("add", []):
        keys.append(tuple(key))
    dropped = [tuple(key) for key in adjustments.get("drop", [])]
    kept = []
    for key in keys:
        if key not in dropped:
            kept.append(key)
    return tuple(kept)


def table_options(table: sa.Table) -> TableOptions:
    """As opções físicas da tabela; mais de uma coluna de partição é `ContractError`."""
    info = table.info.get("serialize_db", {})
    partition = info.get("partition_by") or []
    if len(partition) > 1:
        raise ContractError(
            f"{table.name}: uma coluna de partição no máximo, recebidas {partition}")
    partition_by = partition[0] if partition else None
    return TableOptions(
        partition_by=partition_by,
        partition_source=info.get("partition_source"),
        sort_key=tuple(info.get("sort_key", [])),
        redshift=dict(info.get("redshift", {})),
        keys=_adjusted_keys(_declared_keys(table), info),
    )


# ---------------------------------------------------------------- o DDL por dialeto

# A tabela de tipos de plan/schema.md; Numeric, String e DateTime têm parâmetros: sql_type.
_SQL_TYPES: dict[str, dict[type, str]] = {
    "duckdb": {sa.BigInteger: "BIGINT", sa.SmallInteger: "SMALLINT", sa.Integer: "INTEGER",
               sa.Boolean: "BOOLEAN", sa.Double: "DOUBLE", sa.Date: "DATE", sa.Text: "VARCHAR",
               sa.Uuid: "VARCHAR(36)", sa.JSON: "JSON"},
    "redshift": {sa.BigInteger: "BIGINT", sa.SmallInteger: "SMALLINT", sa.Integer: "INTEGER",
                 sa.Boolean: "BOOLEAN", sa.Double: "DOUBLE PRECISION", sa.Date: "DATE",
                 sa.Text: "VARCHAR(65535)", sa.Uuid: "VARCHAR(36)", sa.JSON: "SUPER"},
}

# O teto do VARCHAR no Redshift, em bytes: o limite de uma coluna Text, que não declara n.
_TEXT_LIMIT = 65535


def sql_type(column: sa.Column, dialect: Dialect) -> str:
    """O nome do tipo da coluna no motor, pela tabela de tipos de plan/schema.md."""
    kind = column.type
    arrow = arrow_type(column)      # recusa o tipo fora do contrato antes de qualquer texto
    if pa.types.is_decimal(arrow):
        return f"DECIMAL({arrow.precision}, {arrow.scale})"
    if isinstance(kind, sa.DateTime):
        return "TIMESTAMPTZ" if kind.timezone else "TIMESTAMP"
    for sa_type, text in _SQL_TYPES[dialect].items():
        if isinstance(kind, sa_type):
            return text
    # String(n): o DuckDB aceita e ignora o comprimento, o Redshift o aplica em bytes.
    return f"VARCHAR({kind.length})" if kind.length else "VARCHAR"


def quoted(name: str) -> str:
    """O identificador entre aspas duplas: `to` e `timestamp` são palavras reservadas."""
    return f'"{name}"'


def column_ddl(column: sa.Column, dialect: Dialect) -> str:
    """A linha da coluna no CREATE TABLE: nome, tipo e NOT NULL."""
    text = f"{quoted(column.name)} {sql_type(column, dialect)}"
    if not column.nullable:
        text += " NOT NULL"
    return text


def _redshift_options(options: TableOptions) -> str:
    """As cláusulas físicas do Redshift depois do parêntese: DISTSTYLE, DISTKEY e SORTKEY."""
    clauses = []
    if options.redshift.get("diststyle"):
        clauses.append(f"DISTSTYLE {options.redshift['diststyle']}")
    if options.redshift.get("distkey"):
        clauses.append(f"DISTKEY ({quoted(options.redshift['distkey'])})")
    if options.sort_key:
        names = ", ".join(quoted(name) for name in options.sort_key)
        clauses.append(f"SORTKEY ({names})")
    return " " + " ".join(clauses) if clauses else ""


def ddl(table: sa.Table, dialect: Dialect, prefix: str = "") -> str:
    """O CREATE TABLE do sandbox: colunas, tipos e NOT NULL, todo identificador entre aspas.

    Sem chave, DEFERRABLE, Identity, CHECK nem comentário: as chaves são da auditoria, e o
    comentário vai no esquema Delta.
    """
    lines = []
    for column in table.columns:
        lines.append("    " + column_ddl(column, dialect))
    text = f"CREATE TABLE {quoted(prefix + table.name)} (\n" + ",\n".join(lines) + "\n)"
    if dialect == "redshift":
        text += _redshift_options(table_options(table))
    return text


# ---------------------------------------------------------------- o cast por lote

def _contract_fields(data: pa.Table | pa.RecordBatch, contract: pa.Schema, table: str) -> list:
    """Os campos do contrato presentes nos dados, na ordem do contrato; nenhum é erro."""
    present = []
    for field in contract:
        if field.name in data.schema.names:
            present.append(field)
    if not present:
        raise ContractError(f"{table}: nenhuma coluna do contrato em {data.schema.names}")
    return present


def _refuse_double_out_of_scale(column, field: pa.Field, table: str) -> None:
    """Um double numa coluna Numeric entra só quando pc.round o devolve igual."""
    rounded = pc.round(column, field.type.scale)
    # min_count=0: a tabela vazia de reader.schema.empty_table() passa; sem ele, pc.all dá nulo.
    if not pc.all(pc.equal(rounded, column), min_count=0).as_py():
        raise ContractError(f"{table}.{field.name}: double fora da escala {field.type.scale}; "
                            "arredonde no cliente antes de chamar")


def _refuse_timestamp_with_time(column, field: pa.Field, table: str) -> None:
    """Um timestamp numa coluna Date entra só quando a ida e volta o devolve igual."""
    round_trip = column.cast(field.type).cast(column.type)
    if not pc.all(pc.equal(round_trip, column), min_count=0).as_py():
        raise ContractError(f"{table}.{field.name}: timestamp com hora numa coluna Date; "
                            "trunque no cliente")


def _refuse_nested_json(column, field: pa.Field, table: str) -> None:
    """Um documento JSON chega serializado; struct, list e map são recusados."""
    if pa.types.is_nested(column.type):
        raise ContractError(f"{table}.{field.name}: documento JSON como {column.type}; "
                            "serialize com json.dumps antes de chamar")


def _longest_text(column) -> int:
    """O maior valor da coluna em bytes; 0 numa coluna vazia ou só de nulos."""
    return pc.max(pc.binary_length(column)).as_py() or 0


def _refuse_text_above_length(column, field: pa.Field, table: str, limit: int) -> None:
    """Texto acima de String(n), medido em bytes como o VARCHAR(n) do Redshift."""
    longest = _longest_text(column)
    if longest > limit:
        raise ContractError(f"{table}.{field.name}: texto de {longest} bytes acima de "
                            f"String({limit}) em bytes; corte o valor ou aumente o comprimento")


def _refuse_text_above_varchar(column, field: pa.Field, table: str) -> None:
    """Texto acima do teto do VARCHAR do Redshift numa coluna Text, que não declara n."""
    longest = _longest_text(column)
    if longest > _TEXT_LIMIT:
        raise ContractError(f"{table}.{field.name}: texto de {longest} bytes acima do teto de "
                            f"{_TEXT_LIMIT} bytes do VARCHAR do Redshift; corte o valor")


def _contract_column(data: pa.Table | pa.RecordBatch, field: pa.Field, table: sa.Table):
    """A coluna dos dados no tipo do contrato; as perdas que safe=True não acusa vêm antes."""
    column = data.column(field.name)
    kind = table.c[field.name].type
    if pa.types.is_floating(column.type) and pa.types.is_decimal(field.type):
        _refuse_double_out_of_scale(column, field, table.name)
    if pa.types.is_integer(column.type) and pa.types.is_decimal(field.type):
        # O cast direto de int64 para decimal128(p, s) pede precisão p + 3; o desvio é seguro.
        column = column.cast(pa.decimal128(field.type.precision + 3, field.type.scale))
    if pa.types.is_timestamp(column.type) and pa.types.is_date(field.type):
        _refuse_timestamp_with_time(column, field, table.name)
    if isinstance(kind, sa.JSON):
        _refuse_nested_json(column, field, table.name)
    limit = getattr(kind, "length", None)
    if isinstance(kind, sa.Text) and pa.types.is_string(column.type):
        _refuse_text_above_varchar(column, field, table.name)
    elif limit and pa.types.is_string(column.type):
        _refuse_text_above_length(column, field, table.name, limit)
    try:
        # safe=True recusa escala perdida, nanossegundo não nulo e estouro.
        return column.cast(field.type, safe=True)
    except (pa.ArrowInvalid, ValueError) as error:
        raise ContractError(f"{table.name}.{field.name}: {error}") from None


def _contract_arrays(data: pa.Table | pa.RecordBatch, table: sa.Table) -> tuple[list, pa.Schema]:
    """As colunas do contrato presentes, convertidas, e o esquema delas."""
    contract = arrow_schema(table)
    fields = _contract_fields(data, contract, table.name)
    arrays = []
    for field in fields:
        arrays.append(_contract_column(data, field, table))
    return arrays, pa.schema(fields, metadata=contract.metadata)


def _cast_batch(batch: pa.RecordBatch, table: sa.Table) -> pa.RecordBatch:
    """O lote no esquema do contrato; nulo em coluna NOT NULL é recusado pelo cast do esquema."""
    arrays, schema = _contract_arrays(batch, table)
    try:
        return pa.RecordBatch.from_arrays(arrays, schema=schema).cast(schema, safe=True)
    except (pa.ArrowInvalid, ValueError) as error:
        raise ContractError(f"{table.name}: {error}") from None


def _cast_table(data: pa.Table, table: sa.Table) -> pa.Table:
    """A tabela no esquema do contrato, pelo mesmo caminho do lote."""
    arrays, schema = _contract_arrays(data, table)
    try:
        return pa.Table.from_arrays(arrays, schema=schema).cast(schema, safe=True)
    except (pa.ArrowInvalid, ValueError) as error:
        raise ContractError(f"{table.name}: {error}") from None


def _cast_batches(reader: pa.RecordBatchReader, table: sa.Table):
    """Os lotes do leitor convertidos um a um, para o leitor de saída."""
    for batch in reader:
        yield _cast_batch(batch, table)


def _cast_reader(reader: pa.RecordBatchReader, table: sa.Table) -> pa.RecordBatchReader:
    """O leitor que converte lote a lote, com o esquema do primeiro lote convertido."""
    schema = _cast_table(reader.schema.empty_table(), table).schema
    return pa.RecordBatchReader.from_batches(schema, _cast_batches(reader, table))


def cast(data, table: sa.Table):
    """`pa.Table`, `pa.RecordBatch` ou `RecordBatchReader` no contrato, no mesmo tipo."""
    if isinstance(data, pa.RecordBatchReader):
        return _cast_reader(data, table)
    if isinstance(data, pa.RecordBatch):
        return _cast_batch(data, table)
    return _cast_table(data, table)


# ---------------------------------------------------------------- a conferência dos modelos

def _column_problems(column: sa.Column) -> list[str]:
    """As violações de uma coluna: tipo, autoincrement, Identity, String sem comprimento."""
    table = column.table.name
    problems = []
    try:
        arrow_type(column)
    except ContractError as error:
        problems.append(str(error))
    # O autoincrement padrão é a string "auto", e o duckdb_engine emitiria SERIAL por ele.
    integer_key = column.primary_key and isinstance(column.type, sa.Integer)
    if integer_key and column.autoincrement in ("auto", True):
        problems.append(f"{table}.{column.name}: chave inteira com autoincrement; "
                        "declare autoincrement=False")
    if column.identity is not None:
        problems.append(f"{table}.{column.name}: Identity fora do contrato")
    if type(column.type) is sa.String and not column.type.length:
        problems.append(f"{table}.{column.name}: String sem comprimento; "
                        "declare String(n) ou Text")
    return problems


def _key_problems(table: sa.Table, options: TableOptions) -> list[str]:
    """As violações das chaves: DEFERRABLE e a tabela sem chave alguma."""
    problems = []
    for constraint in table.foreign_key_constraints:
        if constraint.deferrable or constraint.initially:
            columns = list(_column_names(constraint.columns))
            problems.append(f"{table.name}: chave estrangeira DEFERRABLE em {columns}")
    if not options.keys:
        problems.append(f"{table.name}: sem chave primária e sem keys")
    return problems


def _partition_problems(table: sa.Table, options: TableOptions) -> list[str]:
    """As violações da partição: a coluna ausente ou fora de String(10), a origem ausente."""
    if not options.partition_by:
        return []
    problems = []
    column = table.c.get(options.partition_by)
    if column is None:
        problems.append(
            f"{table.name}: partition_by aponta {options.partition_by}, que a tabela não tem")
    elif not (isinstance(column.type, sa.String) and column.type.length == 10):
        problems.append(
            f"{table.name}.{options.partition_by}: coluna de partição fora de String(10)")
    if not options.partition_source or options.partition_source not in table.c:
        problems.append(f"{table.name}: partition_by sem partition_source válido")
    return problems


def check_models(metadata: sa.MetaData) -> list[str]:
    """As violações do contrato nos modelos, uma por texto; vazia nos modelos corrigidos."""
    problems = []
    for table in metadata.sorted_tables:
        for column in table.columns:
            problems.extend(_column_problems(column))
        options = table_options(table)
        problems.extend(_key_problems(table, options))
        problems.extend(_partition_problems(table, options))
    return problems


def schema_files(metadata: sa.MetaData) -> dict[str, str]:
    """`<tabela>.delta.json`, `<tabela>.duckdb.sql` e `<tabela>.redshift.sql` em memória."""
    files = {}
    for table in metadata.sorted_tables:
        files[f"{table.name}.delta.json"] = delta_schema(table).to_json() + "\n"
        files[f"{table.name}.duckdb.sql"] = ddl(table, "duckdb") + "\n"
        files[f"{table.name}.redshift.sql"] = ddl(table, "redshift") + "\n"
    return files


# ---------------------------------------------------------------- a execução

table = Operacao.__table__
schema = arrow_schema(table)
print("arrow:", [f"{f.name}:{f.type}{'' if f.nullable else '!'}" for f in schema])
delta_fields = json.loads(delta_schema(table).to_json())["fields"]
print("delta:", {f["name"]: f["type"] for f in delta_fields})
print("options:", table_options(table))
print("-- duckdb\n" + ddl(table, "duckdb"))
print("-- redshift\n" + ddl(table, "redshift", prefix="exec_42_"))
con = duckdb.connect()
con.execute(ddl(table, "duckdb"))
con.execute(ddl(table, "duckdb", prefix="{prefix}"))
print("duckdb:", con.execute(
    "SELECT column_name, data_type FROM information_schema.columns "
    "WHERE table_name = 'cad_operacoes' ORDER BY ordinal_position").fetchall())
print("tabelas:",
      con.execute("SELECT table_name FROM information_schema.tables ORDER BY 1").fetchall())
print("arquivos:", sorted(schema_files(Base.metadata)))

# Um lote com as colunas fora de ordem, large_string, nanossegundos zerados e inteiro em Numeric
# entra no contrato, como lote, como tabela e como leitor.
batch = pa.RecordBatch.from_pydict({
    "operacao": pa.array(["a", "b"], pa.large_string()),
    "id_operacao": pa.array([1, 2], pa.int32()),
    "valor": pa.array([10, 20], pa.int64()),
    "data": pa.array([dt.datetime(2026, 8, 31), dt.datetime(2026, 8, 31)], pa.timestamp("ns")),
    "data_str": ["2026-08-31", "2026-08-31"], "extra": [1, 2]})
done = cast(batch, table)
print("cast:", done.schema.names, [str(t) for t in done.schema.types],
      done.column("valor").to_pylist())
print("tabela:", type(cast(pa.Table.from_batches([batch]), table)).__name__,
      cast(pa.Table.from_batches([batch]), table).num_rows)
reader = cast(pa.RecordBatchReader.from_batches(batch.schema, [batch, batch]), table)
print("leitor:", type(reader).__name__, reader.schema.names[:3], reader.read_all().num_rows)


# As recusas, cada uma com a instrução ao cliente.
def refused(description: str, batch: pa.RecordBatch) -> None:
    try:
        cast(batch, table)
        print(f"{description}: aceito")
    except ContractError as error:
        print(f"{description}: {str(error)[:110]}")


refused("nulo em NOT NULL",
        pa.RecordBatch.from_pydict({"id_operacao": pa.array([1, None], pa.int64())}))
refused("double fora da escala", pa.RecordBatch.from_pydict({"valor": pa.array([1.236])}))
refused("double na escala", pa.RecordBatch.from_pydict({"valor": pa.array([1.25, 2.5])}))
refused("hora numa coluna Date",
        pa.RecordBatch.from_pydict(
            {"data": pa.array([dt.datetime(2026, 8, 31, 12)], pa.timestamp("us"))}))
refused("struct em JSON", pa.RecordBatch.from_pydict({"meta": pa.array([{"k": 1}])}))
refused("texto acima de String(100)",
        pa.RecordBatch.from_pydict({"operacao": pa.array(["x" * 101])}))
refused("texto acima do teto do VARCHAR",
        pa.RecordBatch.from_pydict({"observacao": pa.array(["x" * 65536])}))
refused("texto no teto do VARCHAR",
        pa.RecordBatch.from_pydict({"observacao": pa.array(["x" * 65535])}))
refused("precisão perdida",
        pa.RecordBatch.from_pydict(
            {"valor": pa.array([decimal.Decimal("1.234")], pa.decimal128(20, 3))}))
refused("nanossegundo não nulo",
        pa.RecordBatch.from_pydict({"timestamp": pa.array([1], pa.timestamp("ns"))}))
refused("coluna alguma do contrato", pa.RecordBatch.from_pydict({"extra": [1]}))
accepted = cast(pa.RecordBatch.from_pydict({"timestamp": pa.array([1000], pa.timestamp("ns"))}),
                table)
print("nanossegundo nulo:", accepted.schema.field("timestamp").type)


class Ruim(Base):
    __tablename__ = "ruim"
    __table_args__ = {"info": {"serialize_db": {"partition_by": ["mes"]}}}
    id: Mapped[int] = mapped_column(sa.Integer, primary_key=True)
    id_operacao: Mapped[int] = mapped_column(
        sa.ForeignKey("cad_operacoes.id_operacao", deferrable=True, initially="DEFERRED"))
    mes: Mapped[str] = mapped_column(sa.String(7))
    nome: Mapped[str] = mapped_column(sa.String)
    peso: Mapped[bytes] = mapped_column(sa.LargeBinary)


problems = check_models(Base.metadata)
print("cad_operacoes limpa:", [p for p in problems if "cad_operacoes" in p] == [])
print("check_models:")
for problem in check_models(Base.metadata):
    print("  -", problem)
```

Saída:

```
arrow: ['id_operacao:int64!', 'data:date32[day]!', 'operacao:string!', 'to:string!', 'valor:decimal128(18, 2)!', 'spread:double', 'parcelas:int16', 'sistema:int32!', 'ativa:bool!', 'observacao:string', 'meta:string', 'timestamp:timestamp[us]!', 'carimbo_utc:timestamp[us, tz=UTC]', 'chave:string', 'data_str:string!']
delta: {'id_operacao': 'long', 'data': 'date', 'operacao': 'string', 'to': 'string', 'valor': 'decimal(18,2)', 'spread': 'double', 'parcelas': 'short', 'sistema': 'integer', 'ativa': 'boolean', 'observacao': 'string', 'meta': 'string', 'timestamp': 'timestamp_ntz', 'carimbo_utc': 'timestamp', 'chave': 'string', 'data_str': 'string'}
options: TableOptions(partition_by='data_str', partition_source='data', sort_key=('data', 'operacao'), redshift={'diststyle': 'KEY', 'distkey': 'id_operacao'}, keys=(('id_operacao',), ('data', 'operacao')))
-- duckdb
CREATE TABLE "cad_operacoes" (
    "id_operacao" BIGINT NOT NULL,
    "data" DATE NOT NULL,
    "operacao" VARCHAR(100) NOT NULL,
    "to" VARCHAR(2) NOT NULL,
    "valor" DECIMAL(18, 2) NOT NULL,
    "spread" DOUBLE,
    "parcelas" SMALLINT,
    "sistema" INTEGER NOT NULL,
    "ativa" BOOLEAN NOT NULL,
    "observacao" VARCHAR,
    "meta" JSON,
    "timestamp" TIMESTAMP NOT NULL,
    "carimbo_utc" TIMESTAMPTZ,
    "chave" VARCHAR(36),
    "data_str" VARCHAR(10) NOT NULL
)
-- redshift
CREATE TABLE "exec_42_cad_operacoes" (
    "id_operacao" BIGINT NOT NULL,
    "data" DATE NOT NULL,
    "operacao" VARCHAR(100) NOT NULL,
    "to" VARCHAR(2) NOT NULL,
    "valor" DECIMAL(18, 2) NOT NULL,
    "spread" DOUBLE PRECISION,
    "parcelas" SMALLINT,
    "sistema" INTEGER NOT NULL,
    "ativa" BOOLEAN NOT NULL,
    "observacao" VARCHAR(65535),
    "meta" SUPER,
    "timestamp" TIMESTAMP NOT NULL,
    "carimbo_utc" TIMESTAMPTZ,
    "chave" VARCHAR(36),
    "data_str" VARCHAR(10) NOT NULL
) DISTSTYLE KEY DISTKEY ("id_operacao") SORTKEY ("data", "operacao")
duckdb: [('id_operacao', 'BIGINT'), ('data', 'DATE'), ('operacao', 'VARCHAR'), ('to', 'VARCHAR'), ('valor', 'DECIMAL(18,2)'), ('spread', 'DOUBLE'), ('parcelas', 'SMALLINT'), ('sistema', 'INTEGER'), ('ativa', 'BOOLEAN'), ('observacao', 'VARCHAR'), ('meta', 'JSON'), ('timestamp', 'TIMESTAMP'), ('carimbo_utc', 'TIMESTAMP WITH TIME ZONE'), ('chave', 'VARCHAR'), ('data_str', 'VARCHAR')]
tabelas: [('cad_operacoes',), ('{prefix}cad_operacoes',)]
arquivos: ['cad_operacoes.delta.json', 'cad_operacoes.duckdb.sql', 'cad_operacoes.redshift.sql']
cast: ['id_operacao', 'data', 'operacao', 'valor', 'data_str'] ['int64', 'date32[day]', 'string', 'decimal128(18, 2)', 'string'] [Decimal('10.00'), Decimal('20.00')]
tabela: Table 2
leitor: RecordBatchReader ['id_operacao', 'data', 'operacao'] 4
nulo em NOT NULL: cad_operacoes: Casting field 'id_operacao' with null values to non-nullable
double fora da escala: cad_operacoes.valor: double fora da escala 2; arredonde no cliente antes de chamar
double na escala: aceito
hora numa coluna Date: cad_operacoes.data: timestamp com hora numa coluna Date; trunque no cliente
struct em JSON: cad_operacoes.meta: documento JSON como struct<k: int64>; serialize com json.dumps antes de chamar
texto acima de String(100): cad_operacoes.operacao: texto de 101 bytes acima de String(100) em bytes; corte o valor ou aumente o comprimen
texto acima do teto do VARCHAR: cad_operacoes.observacao: texto de 65536 bytes acima do teto de 65535 bytes do VARCHAR do Redshift; corte o va
texto no teto do VARCHAR: aceito
precisão perdida: cad_operacoes.valor: Rescaling Decimal value would cause data loss
nanossegundo não nulo: cad_operacoes.timestamp: Casting from timestamp[ns] to timestamp[us] would lose data: 1
coluna alguma do contrato: cad_operacoes: nenhuma coluna do contrato em ['extra']
nanossegundo nulo: timestamp[us]
cad_operacoes limpa: True
check_models:
  - ruim.id: chave inteira com autoincrement; declare autoincrement=False
  - ruim.nome: String sem comprimento; declare String(n) ou Text
  - ruim.peso: tipo fora do contrato: LargeBinary()
  - ruim: chave estrangeira DEFERRABLE em ['id_operacao']
  - ruim.mes: coluna de partição fora de String(10)
  - ruim: partition_by sem partition_source válido
```

## Decisões pendentes

Nenhuma: as sete decisões da etapa foram tomadas pelo usuário em 2026-09-21, e cada uma está escrita
na seção que a descreve.
