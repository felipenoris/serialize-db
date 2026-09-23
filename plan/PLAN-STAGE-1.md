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
porque o contrato está em N operações; decisão do usuário de 2026-09-21), os dois índices únicos
compostos do original como `UniqueConstraint` (`cad_operacoes (data, operacao)` e
`cad_contratos (data, sistema, contrato)`), porque as chaves estrangeiras compostas os apontam e o
DuckDB e o Redshift exigem chave primária ou `UNIQUE` no alvo (decisão do usuário de 2026-09-22),
a coluna de partição
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
| `arrow_type(column)` | O `pa.DataType` da coluna pela tabela de tipos da documentação (`docs/index.md`): `Numeric(p, s)` em `decimal128(p, s)`, `DateTime` em `timestamp[us]` (`tz=UTC` com fuso), os demais por `isinstance` na ordem que põe `BigInteger` e `SmallInteger` antes de `Integer` e `Text` antes de `String`; `Float`, `LargeBinary`, `ARRAY` e `Interval` são `ContractError`, com tabela e coluna. |
| `arrow_schema(table)` | O `pa.Schema` do `Table`: um campo por coluna, com a nulidade, o comentário em `metadata` do campo, `PARQUET:field_id` pela posição, e o nome da tabela em `serialize_db_table`; um campo JSON é `string`, sem a extensão `arrow.json`. |
| `delta_schema(table)` | O `deltalake.Schema` derivado do Arrow: `decimal(18,2)`, `timestamp_ntz` para `DateTime` sem fuso e `timestamp` para o com fuso, `string` para JSON e UUID, comentários preservados, sem o `PARQUET:field_id` do Arrow (com ele no esquema Delta, como `parquet.field.id`, o `delta_scan` do DuckDB lê toda coluna como nula, com qualquer escritor; leitura de 2026-09-21 em [`POC.md`](POC.md)). |
| `table_options(table)` | O `TableOptions` (`partition_by`, `partition_source`, `sort_key`, `redshift`, `keys`) lido de `Table.info["serialize_db"]` com os padrões da biblioteca: tabela sem partição quando `partition_by` está ausente, uma coluna de partição no máximo, de texto `String(n)`, com `partition_source` opcional, a coluna de data de que ela deriva por `strftime(partition_source, '%Y-%m-%d')` (decisão do usuário de 2026-09-22; `String(10)` e a origem obrigatória até então); `keys` são a chave primária, as `UniqueConstraint` e os índices únicos do modelo (no modelo cliente as duas chaves únicas compostas são `UniqueConstraint` desde 2026-09-22; um índice único entra igual), mais `keys["add"]`, menos `keys["drop"]`, sempre por lista de colunas. |
| `sql_type(column, dialect)` | O nome do tipo no motor, pela tabela de tipos da documentação (`docs/index.md`): `DECIMAL(p, s)`, `VARCHAR(n)` nos dois (o DuckDB ignora o comprimento), `Text` em `VARCHAR` e `VARCHAR(65535)` (decisão do usuário de 2026-09-21), `Uuid` em `VARCHAR(36)`, JSON em `JSON` e `SUPER`, `Double` em `DOUBLE` e `DOUBLE PRECISION`, `DateTime` em `TIMESTAMP` e `TIMESTAMPTZ`; a migração adiantada o usa nos `CAST`. |
| `quoted(name)`, `column_ddl(column, dialect)` | O identificador entre aspas duplas, e a linha da coluna no `CREATE TABLE` (`"<coluna>" <tipo> [NOT NULL]`); públicas porque as etapas [4](PLAN-STAGE-4.md), [5](PLAN-STAGE-5.md) e [8](PLAN-STAGE-8.md) montam texto com identificadores e as variantes do DDL (a staging sem a coluna de partição, a tabela publicada com a chave primária informativa). |
| `ddl(table, dialect, prefix="", temporary=False)` | O `CREATE TABLE` do sandbox para `duckdb` ou `redshift`, gerado como texto sem o dialeto do SQLAlchemy: colunas, tipos por `sql_type` e `NOT NULL`, todo identificador entre aspas; sem chave, `DEFERRABLE`, `Identity`, `CHECK`, `DEFAULT` nem comentário (as chaves são da auditoria, e o comentário vai no esquema Delta); `DISTSTYLE`, `DISTKEY` e `SORTKEY` no Redshift, de `table_options`; `prefix` renomeia a tabela para o sandbox (`exec_<id>_`, ou o sentinela `{prefix}` da [etapa 2](PLAN-STAGE-2.md), que sai como `"{prefix}cad_operacoes"`); `temporary=True` emite `CREATE TEMP TABLE`, a tabela da sessão, pedida pelo usuário em 2026-09-22 sem uso no plano, porque o sandbox dos dois motores é de tabelas comuns e, no DuckDB, a temporária é da conexão que a criou e um `cursor()` não a vê (leitura de 2026-09-22, [`POC.md`](POC.md)). |
| `cast(data, table)` | A `pa.Table`, o `pa.RecordBatch` ou o `RecordBatchReader` lote a lote, convertido para `arrow_schema(table)` com `safe=True` e devolvido no mesmo tipo (`RecordBatch.cast` recusa o mesmo que `Table.cast`): as colunas do contrato presentes, na ordem do contrato; `large_string`, `string_view` e dicionário para `string`, timestamps a microssegundos e UTC, inteiro em `Numeric(p, s)` por `decimal128(38, s)`, que recebe qualquer `int64`, antes de conferir `p`; recusa perda de precisão, `double` fora da escala e `timestamp` com hora numa coluna `Date` (as duas perdas que `safe=True` não acusa), `struct` numa coluna JSON, texto acima de `String(n)` em bytes, texto acima de 65.535 bytes numa coluna `Text`, os dois medidos depois da conversão para `string`, um tipo sem conversão para o do contrato, nulo em coluna `NOT NULL` e um lote sem coluna alguma do contrato, com a tabela, a coluna e a instrução ao cliente na mensagem. Aceita `NaN` e infinito numa coluna `Double` (decisão do usuário de 2026-09-23): os dois escritores do Delta gravam o máximo sem o `NaN`, e o `delta_scan` responde a um filtro por intervalo conforme a poda, a questão aberta da [issue #59](https://github.com/felipenoris/serialize-db/issues/59). |
| `check_models(metadata)` | A lista de violações do contrato nos modelos, um texto por violação com tabela e coluna: tipo fora da tabela de tipos, `autoincrement` em chave inteira (o padrão `"auto"` inclusive), `Identity`, `String` sem comprimento, chave estrangeira `DEFERRABLE` ou cujas colunas apontadas não são a chave primária nem uma `UniqueConstraint` da tabela apontada, na mesma ordem (decisão do usuário de 2026-09-22: o DuckDB recusa o índice único como alvo e a ordem trocada, e o Redshift documenta a mesma exigência), `partition_by` sem a coluna ou com a coluna fora de `String(n)`, `partition_source` que a tabela não tem ou sem `partition_by`, tabela sem chave primária e sem `keys`. Vazia no modelo cliente; no modelo de referência lista o `autoincrement` das 12 chaves, as 12 chaves estrangeiras `DEFERRABLE` (das 14), as 3 chaves estrangeiras sem chave no alvo, as 20 colunas `String` sem comprimento; o comentário de tabela e de coluna é opcional (decisão do usuário de 2026-09-21), e o da coluna, quando existe, vai para o esquema Arrow e para o Delta. |
| `schema_files(metadata)` | `{"<tabela>.delta.json": ..., "<tabela>.duckdb.sql": ..., "<tabela>.redshift.sql": ...}` em memória, cada texto com `\n` final; o `.delta.json` é o JSON canônico (chaves ordenadas, indentado), porque o `to_json()` do delta-rs serializa os metadados de cada campo em ordem arbitrária, que muda a cada geração, e grava `PARQUET:field_id` como `parquet.field.id` inteiro. |
| `write_schema_files(metadata, directory)` | Grava `schema_files` em `directory` e devolve os caminhos; `serialize-db schema write --metadata modulo:atributo <pasta>` grava. |
| `check_schema_files(metadata, directory)` | O diff unificado de cada arquivo versionado contra a geração nova, vazio quando nada mudou; `serialize-db schema check --metadata modulo:atributo <pasta>` compara sem gravar. |

Testes: `tests/test_schema.py`, sem gravar, sobre o modelo cliente, o modelo de referência e um
modelo de teste com todos os tipos; o DDL de cada tabela executado no DuckDB em memória; o diff dos
arquivos `tests/client_model/schema/` versionados; `write_schema_files` sob a raiz local.
Dependências de execução: `sqlalchemy`, `pyarrow`, `deltalake` e `duckdb`; `duckdb-engine` e
`sqlalchemy-redshift` ficam no grupo `dev`, das suítes de estudo, porque a etapa não compila pelo
dialeto (decisão do usuário de 2026-09-21). Provas de conceito: `test_sqlalchemy.py`
(`test_declarative_model_exposes_table`, `test_ddl_per_dialect`, com as opções físicas de `info`
acrescentadas por uma função comum e não por uma regra `@compiles`, `test_create_all_and_reflection`,
`test_arrow_and_delta_schema_from_table`, com os tipos, a nulidade, o comentário e o
`parquet.field.id` que `Schema.from_arrow` leva do Arrow ao esquema Delta, num esquema montado à
mão, e `test_sandbox_copy_of_table_and_schema_files_diff`, com a cópia de `to_metadata` para o
sandbox; o mapa de tipos e os arquivos de esquema são de `tests/test_schema.py`), `test_pyarrow.py`
(`test_schema_metadata_and_from_pylist`, `test_safe_cast_refuses_data_loss`, com as perdas que o
cast seguro não acusa e o texto medido em bytes, `test_arrow_table_round_trips_through_pandas_without_copy` e
`test_record_batch_cast_and_conversions_share_buffers`, o mesmo por lote) e `test_stdlib.py`
(`test_generated_files_diff`, `test_decimal_totals`, `test_entry_point_by_import_string`).

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
  reúne `table.primary_key`, as `UniqueConstraint` e os `table.indexes` com `unique`, porque um
  modelo pode declarar a chave única por `Index(unique=True)`, como o modelo de referência faz, e
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
  `CREATE TABLE "<prefixo><tabela>"`, o prefixo dentro das aspas, ou sob `CREATE TEMP TABLE` com
  `temporary=True`. `Identity`, `server_default`,
  `CheckConstraint` e comentário não saem no texto; `check_models` reprova `Identity`. Sem o
  dialeto, o `with_variant(SUPER(), "redshift")` de [`schema.md`](schema.md) deixa de ser
  necessário no modelo: `sql_type` emite `SUPER` por `isinstance(kind, sa.JSON)`, e um modelo com
  a variante continua aceito. O DuckDB registra `DECIMAL(18, 2)`, `TIMESTAMPTZ`, `VARCHAR(n)` e
  `JSON` como `DECIMAL(18,2)`, `TIMESTAMP WITH TIME ZONE`, `VARCHAR` e `JSON` (rascunho abaixo).
- **`cast`** despacha por `isinstance` para `_cast_batch`, `_cast_table` e `_cast_reader`, e as três
  passam por `_contract_arrays`: `_contract_fields` seleciona as colunas do contrato presentes, na
  ordem do contrato, e `_contract_column` converte coluna a coluna em três passos. Antes da
  conversão, `_refuse_silent_losses` recusa o que `safe=True` não acusa, numa função por recusa:
  `_refuse_double_out_of_scale` (`double` numa coluna `Numeric` só quando `pc.round(x, escala)`
  devolve o valor igual), `_refuse_timestamp_with_time` (`timestamp` numa coluna `Date` só quando a
  ida e volta devolve o valor igual) e `_refuse_nested_json` (`struct`, `list` e `map` numa coluna
  JSON). A conversão, `_converted`, é `column.cast(field.type, safe=True)`, que recusa escala
  perdida, nanossegundo não nulo e estouro; o inteiro numa coluna `Numeric(p, s)` passa antes por
  `decimal128(38, s)`, porque o cast direto exige que `p` comporte qualquer `int64` (19 dígitos
  mais a escala), e o segundo cast confere se cada valor cabe em `p`. `ArrowInvalid` e
  `ArrowNotImplementedError`, o tipo sem conversão para o do contrato, viram `ContractError`.
  Depois da conversão, `_refuse_long_text` mede o texto já em `string`, porque ele chega também em
  `large_string` (o `str` do pandas 3), `string_view` e dicionário, que `pa.types.is_string` não
  reconhece (leitura de 2026-09-22, [`POC.md`](POC.md)): `_refuse_text_above_length` (texto acima
  de `String(n)` medido em bytes por `pc.binary_length`, a medida do `VARCHAR(n)` do Redshift;
  decisão do usuário de 2026-09-21, a mesma medida da auditoria da [etapa 4](PLAN-STAGE-4.md) e
  da migração adiantada, e `probes/parquet_source.py` relata o máximo em bytes ao lado do de
  caracteres) e `_refuse_text_above_varchar` (texto acima de 65.535 bytes numa coluna `Text`, que
  não declara `n`: o teto do `VARCHAR` do Redshift, que `sql_type` escreve no DDL). O `cast` do
  esquema sobre `from_arrays` recusa nulo em `NOT NULL`. Toda recusa sai como `ContractError` com
  a tabela, a coluna e a instrução. `_cast_reader` deriva o esquema de saída de `reader.schema.empty_table()` e
  embrulha `_cast_batches`, uma função geradora, em `RecordBatchReader.from_batches`; as duas
  conferências por `pc.all` levam `min_count=0`, porque `pc.all` de uma coluna vazia devolve nulo e
  a tabela vazia seria recusada (rascunho abaixo).
- **`check_models`** percorre `metadata.sorted_tables` e reúne, por tabela, `_column_problems`
  (tipo, `autoincrement`, `Identity`, `String` sem comprimento), `_key_problems`
  (`DEFERRABLE`, tabela sem chave) e `_partition_problems` (a coluna de texto `String(n)`, o
  `partition_source` que a tabela não tem ou que vem sem `partition_by`), uma lista de textos, um por violação, para o teste do modelo cliente ser
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
  `serialize-db schema check` resolvem `--metadata modulo:atributo` por `pkgutil.resolve_name`
  (`test_stdlib.py::test_entry_point_by_import_string`); o `check` sai com 0 sem diff, 1
  com diff impresso, 2 no erro de uso do `argparse`.

## Pré-requisitos e pós-condições

| Primitiva | Pré-requisitos | Pós-condições |
| --- | --- | --- |
| `arrow_schema`, `delta_schema` | Toda coluna com tipo da tabela de [`schema.md`](schema.md). | Um campo por coluna, na ordem do modelo, com nulidade, comentário e `field_id`; `ContractError` na primeira coluna fora da tabela. |
| `table_options` | `partition_by` com uma coluna no máximo. | `partition_source` só com `partition_by`, conferido por `check_models`; `keys` não vazia quando o modelo tem chave primária, única ou índice único. |
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
| Opções físicas | `test_table_options_defaults_and_keys` | Tabela sem `info` dá `partition_by=None` e `keys` só da chave primária; as `UniqueConstraint` de `cad_contratos` e de `cad_aliquotas` e o índice único da tabela de teste entram em `keys`; `keys["add"]` acrescenta e `keys["drop"]` remove; duas colunas de partição são `ContractError`. |
| Tipos por dialeto | `test_sql_type_per_dialect` | Cada tipo do modelo de teste no texto da tabela de tipos: `DECIMAL(18, 2)`, `VARCHAR(100)`, `VARCHAR` e `VARCHAR(65535)` para `Text`, `VARCHAR(36)`, `JSON` e `SUPER`, `DOUBLE` e `DOUBLE PRECISION`, `TIMESTAMP` e `TIMESTAMPTZ`. |
| DDL por dialeto | `test_ddl_per_dialect` | Sem `PRIMARY KEY`, `UNIQUE`, `REFERENCES`, `DEFERRABLE`, `SERIAL`, `IDENTITY` ou `CHECK`; `SORTKEY`, `DISTSTYLE` e `DISTKEY` só no Redshift, com os nomes entre aspas; o texto igual ao esperado, linha a linha. |
| Identificadores | `test_ddl_quotes_every_identifier` | `"to"` em `cad_contratos` e `"timestamp"` em `cad_lancamentos` nos dois dialetos; nenhum nome de tabela ou coluna sem aspas no texto. |
| DDL executável | `test_ddl_runs_in_duckdb_memory` | O DDL das 12 tabelas do modelo cliente e o do modelo de teste executam num DuckDB em memória, e `information_schema.columns` devolve os tipos do contrato (`DECIMAL(18,2)`, `TIMESTAMP WITH TIME ZONE`, `JSON`). |
| Prefixo | `test_ddl_prefix_inside_the_quotes` | `prefix="exec_42_"` e `prefix="{prefix}"` saem como `"exec_42_cad_operacoes"` e `"{prefix}cad_operacoes"` nos dois dialetos, e o segundo executa no DuckDB. |
| Partição de texto | `test_partition_column_is_any_text_and_the_source_optional` | Uma coluna `String(20)` sem `partition_source` passa em `check_models`, com `partition_source=None`; `Text` na coluna de partição, `partition_source` que a tabela não tem e `partition_source` sem `partition_by` são violações (decisão do usuário de 2026-09-22). |
| Tabela temporária | `test_ddl_temporary_table` | `temporary=True` dá `CREATE TEMP TABLE "exec_42_tudo" (` nos dois dialetos, com o resto do texto igual ao de `temporary=False`; no DuckDB a tabela nasce no catálogo `temp`, e um `cursor()` da mesma conexão não a vê. |
| Cast que aceita | `test_cast_reorders_and_normalizes` | Colunas fora de ordem, `large_string`, `timestamp[ns]` com nanossegundo zero, `int64` em `Numeric`, coluna a mais ignorada; o mesmo para `pa.Table` e `pa.RecordBatch`. |
| Cast por leitor | `test_cast_reader_converts_batch_by_batch` | Um leitor de dois lotes sai como `RecordBatchReader` com o esquema do contrato e as linhas dos dois; a tabela vazia de `reader.schema.empty_table()` passa nas conferências. |
| Cast que recusa | `test_cast_refuses_each_loss`, parametrizado | Nulo em `NOT NULL`, `double` fora da escala, `timestamp` com hora em `Date`, `struct` em JSON, texto acima de `String(n)` em bytes, escala perdida, nanossegundo não nulo, estouro de inteiro, lote sem coluna do contrato; cada mensagem cita a tabela e a coluna. |
| Teto de `Text` | `test_cast_measures_text_against_the_varchar_ceiling` | Numa coluna `Text`, 65.535 bytes passam e 65.536 são `ContractError` com a tabela, a coluna e o tamanho lido. |
| Cast que preserva | `test_cast_keeps_doubles_of_the_reference_model` | `Double` do modelo de referência entra sem arredondamento. |
| Modelos | `test_check_models_finds_each_violation`, `test_foreign_key_target_must_be_a_key_in_the_same_order`, `test_check_models_lists_the_reference_model_defects`, `test_client_model_is_clean` | Um modelo com cada defeito produz uma violação por defeito, a chave estrangeira composta que aponta um índice único inclusive; a chave estrangeira que aponta uma `UniqueConstraint` na ordem dela passa, na ordem trocada é violação e o DuckDB recusa o mesmo `create_all`; o modelo de referência produz o `autoincrement` das 12 chaves, as 12 chaves estrangeiras `DEFERRABLE` (das 14), as 3 chaves estrangeiras sem chave no alvo e os 20 `String` sem comprimento, e nada mais, nem por comentário ausente; o modelo cliente produz lista vazia. |
| Cópia fiel | `tests/test_client_model.py` | O modelo cliente tem as tabelas e as colunas do modelo de referência, na mesma ordem, com a coluna de partição no fim; só os tipos e as chaves previstos mudam; sem `DEFERRABLE`, `autoincrement` nem índice, os dois índices únicos compostos do original como `UniqueConstraint`; o modelo inteiro criado por `create_all` num `Connection` do DuckDB criado fora da biblioteca; todo comentário presente; a partição declarada é a da base. |
| Arquivos gerados | `test_schema_files_match_versioned`, `test_check_schema_files_reports_a_changed_model` | Diff vazio contra `tests/client_model/schema/`; uma coluna acrescentada aparece no diff dos três formatos. |
| Gravação | `test_write_schema_files` (`local`) | Os arquivos sob a raiz local, com os nomes previstos. |
| Linha de comando | `test_cli_schema_check_reads_the_versioned_files` | `serialize-db schema check --metadata client_model:Base.metadata tests/client_model/schema` sai com 0 sem gravar; um modelo mudado sai com 1 e imprime o diff; sem `--metadata` sai com 2. |

## A implementação

O módulo `serialize_db.schema` e os casos de `tests/test_schema.py` e `tests/test_client_model.py`
substituem a interface e o rascunho executado em 2026-09-21: as assinaturas e as docstrings estão
no código e na documentação do `pdoc`, e o que o rascunho mostrou além do que já estava medido está
em [`POC.md`](POC.md), seção "O que os rascunhos das etapas mostraram".

## Decisões pendentes

As decisões da etapa foram tomadas pelo usuário em 2026-09-21, 2026-09-22 e 2026-09-23, e cada uma
está escrita na seção que a descreve. A de 2026-09-23 manteve o `cast` aceitando o `NaN` e o
infinito numa coluna `Double`, e o que eles fazem nas estatísticas do Delta ficou na
[issue #59](https://github.com/felipenoris/serialize-db/issues/59). A de 2026-09-22 pôs `UniqueConstraint` no lugar dos dois índices
únicos compostos do modelo cliente, que `rel_contrato_operacao` e `cad_lancamentos` apontam por
chave estrangeira composta, e a regra da chave estrangeira sem chave no alvo em `check_models`:
`Base.metadata.create_all` num `sqlalchemy.Connection` do DuckDB falhava em
`rel_contrato_operacao` (`Binder Error: referenced table "cad_operacoes" does not have a primary
key or unique constraint on the columns data,operacao`, leitura de 2026-09-22,
[`POC.md`](POC.md)), e a página de `CREATE TABLE` do Redshift exige o mesmo.

A revisão de código de 2026-09-22 deixou uma proposta à espera do usuário:

- **O timestamp com fuso numa coluna sem fuso.** `cast` aceita `timestamp[us, tz=...]` numa coluna
  `DateTime` sem fuso, e guarda o instante UTC como hora local, e aceita o inverso assumindo UTC
  (leitura de 2026-09-22, [`POC.md`](POC.md)). Proposto: recusar os dois com `ContractError`, com a
  instrução de converter no cliente, porque a conversão muda o valor que o cliente vê e nenhuma
  coluna do modelo cliente tem fuso.

