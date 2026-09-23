# DuckDB

O DuckDB é um banco analítico embutido no processo Python, sem servidor. Este documento descreve como
ele organiza os dados, os tipos que interessam ao contrato, o DDL, a manipulação de dados com pandas e
Arrow, a ingestão em volume, a exportação para Parquet, as recomendações de performance e o suporte a
SQLAlchemy. O papel do DuckDB no projeto é o [sandbox da execução](guia.md), um banco em memória ou
num arquivo local que recebe os dados novos, roda as auditorias e exporta os meses aprovados. Com
`memory_limit`, as operações maiores que a memória vão para `temp_directory`, no EBS do espaço.

As afirmações sobre comportamento vêm da documentação oficial. Os exemplos e as medições foram
executados em 2026-09-18 com DuckDB 1.5.5, pandas 3.0.6, pyarrow 25.0.1, polars 1.44.2, SQLAlchemy
2.0.54 e duckdb_engine 0.17.0, sobre a mesma amostra do [documento sobre Parquet](parquet.md):
300.000 linhas da tabela `operacoes`, com as colunas `id_operacao`, `data_ref`, `id_cliente`, `valor`
e `descricao`.

## Organização dos dados

### Armazenamento colunar em row groups

Um banco DuckDB é um único arquivo. O cabeçalho traz os bytes mágicos `DUCK` e o número da versão de
armazenamento. As versões 1.0 a 1.5 gravam por padrão a versão de armazenamento 64, a do DuckDB
1.0.0; a opção `storage_compatibility_version` (ou `ATTACH ... (STORAGE_VERSION 'latest')`) habilita
formatos mais novos, que versões anteriores não leem. A leitura de arquivos antigos por versões
novas é garantida; a leitura de arquivos novos por versões antigas não.

Cada tabela é dividida em row groups de 122.880 linhas, o mesmo conceito dos row groups do Parquet.
Dentro do row group, cada coluna fica em segmentos próprios, comprimidos com algoritmos leves
(constante, RLE, bit packing, frame of reference, dicionário, FSST para strings, ALP para ponto
flutuante, Zstd). A compressão só se aplica a bancos persistentes; um banco em memória fica sem
compressão, exceto quando aberto com `ATTACH ':memory:' AS db (COMPRESS)`. A documentação estima que
100 GB de CSV ocupam cerca de 25 GB num arquivo DuckDB, e 100 GB de Parquet, cerca de 120 GB.

Dois índices existem sem que ninguém os peça:

- Um zonemap (mínimo e máximo por row group) para toda coluna de tipo simples. Um filtro
  `WHERE data_ref = DATE '2026-08-15'` pula os row groups cujo intervalo não contém a data.
- Uma árvore ART para cada `PRIMARY KEY`, `UNIQUE` e `FOREIGN KEY`. A ART garante a restrição e serve
  a filtros muito seletivos; ela não acelera joins nem agregações.

A execução é vetorizada: os operadores processam vetores de 2.048 valores, e o paralelismo começa no
row group. Uma consulta usa `k` threads apenas se varre ao menos `k × 122.880` linhas.

Três pontos do modelo de escrita afetam o pipeline:

- Um processo escreve por vez. Dentro do processo, várias conexões podem escrever ao mesmo tempo com
  MVCC e controle otimista: appends nunca conflitam, e duas transações que alteram a mesma linha fazem
  a segunda falhar com `TransactionContext Error: Conflict on update!`. Outros processos só abrem o arquivo em modo
  `READ_ONLY`; a escrita por vários processos passa pelo protocolo Quack, em beta na 1.5.2, ou pelo
  formato DuckLake.
- O isolamento é por snapshot (leituras repetíveis). Uma transação enxerga o estado do início dela.
- `DELETE` e `UPDATE` apenas marcam linhas. Um `CHECKPOINT` recupera parte do espaço, fundindo row
  groups adjacentes quando cerca de 25 % das linhas deles foram apagadas; o arquivo não encolhe, e
  `VACUUM` só recalcula estatísticas.

### Diferenças para o PostgreSQL

O dialeto SQL do DuckDB segue o PostgreSQL, com o parser derivado dele. As diferenças que importam ao
projeto:

| Aspecto | PostgreSQL | DuckDB |
| --- | --- | --- |
| Arquitetura | Servidor, armazenamento por linha, muitos escritores concorrentes. | Biblioteca no processo, armazenamento colunar, um processo escritor. |
| Índices | B-tree é a ferramenta central de performance. | Zonemaps automáticos; ART só para restrições e filtros com seletividade abaixo de 0,1 %. |
| Chaves e restrições | Aplicadas, custo baixo. | Aplicadas, mas a carga com chave primária de 554 milhões de linhas levou 461,6 s contra 121,0 s sem chave. |
| Autoincremento | `SERIAL`, `IDENTITY`. | Só `CREATE SEQUENCE` com `DEFAULT nextval(...)`. `GENERATED ... AS IDENTITY` falha com `Constraint not implemented!` (testado na 1.5.5). |
| `ALTER TABLE` | `ADD CONSTRAINT`, `DROP CONSTRAINT`. | Sem `ADD`/`DROP CONSTRAINT`; `ADD PRIMARY KEY` existe. Colunas com índice não podem ser removidas nem mudar de tipo. |
| `VACUUM` | Recupera espaço e atualiza estatísticas. | Só estatísticas. |
| Divisão de inteiros | `1 / 2` é `0`. | `1 / 2` é `0.5`; `1 // 2` é `0`. |
| Identificadores | Sem aspas viram minúsculas; com aspas preservam maiúsculas. `TO` e `TIMESTAMP` são reservadas. | Insensíveis a maiúsculas em qualquer caso, com a grafia preservada. `duckdb_keywords()` dá a categoria: `to` é `reserved` (`CREATE TABLE t (to VARCHAR(2))` falha com `syntax error at or near "to"`), `timestamp` é `column_name` e `data` é `unreserved`, aceitas sem aspas (2026-09-21). |
| Igualdade com cast implícito | `'1.1' = 1` é erro. | `'1.1' = 1` é verdadeiro. |

| Inserção linha a linha | Adequada. | Prejudicial; a documentação pede lotes acima de algumas linhas. |
| Tipos | Arrays, `JSON`, `UUID`. | `LIST`, `ARRAY`, `STRUCT`, `MAP`, `UNION`, `JSON` (extensão), `UUID`, inteiros sem sinal, `HUGEINT`. |
| Comprimento de `VARCHAR(n)` | Aplicado. | Ignorado; `VARCHAR(200)` vira `VARCHAR`. |
| Expressões regulares | `~` faz busca parcial, `~*` existe. | `~` exige casamento completo; `~*` não existe. |

A opção `SET preserve_identifier_case = false` reproduz a conversão para minúsculas do PostgreSQL. A
extensão `postgres` lê tabelas do PostgreSQL diretamente.

### Efeitos nas formas de manipular os dados

Na entrada, o caminho rápido é um comando por lote: `INSERT INTO ... SELECT` a partir de uma tabela
Arrow, de um DataFrame ou de arquivos Parquet e CSV. Chaves e índices, quando necessários, entram
depois da carga. Um laço de `INSERT` com uma linha por comando é a forma mais lenta, e a documentação
pede que ele rode ao menos dentro de `BEGIN TRANSACTION` e `COMMIT`, porque cada commit faz `fsync`.

Na saída, o resultado vai para Arrow com os tipos preservados. A conversão direta para
pandas com `df()` troca `DECIMAL` por `float64` e `DATE` por `datetime64[us]`; o caminho Arrow, com
`to_arrow_table()` e `to_pandas(types_mapper=pd.ArrowDtype)`, mantém `decimal128(18, 2)` e
`date32`. A exportação de arquivos usa `COPY ... TO`.

Nas alterações, a unidade de trabalho é o mês: `DELETE` do mês e `INSERT` do mês novo numa transação,
ou `CREATE OR REPLACE TABLE ... AS`. `MERGE INTO` cobre o caso de alterar linhas por chave. Muitos
`UPDATE` pequenos são o padrão a evitar.

## Tipos suportados

Tipos de uso geral, com os apelidos aceitos:

| Tipo | Apelidos | Descrição |
| --- | --- | --- |
| `BOOLEAN` | `BOOL`, `LOGICAL` | Lógico. |
| `TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT` | `INT1`, `INT2`/`SHORT`, `INT4`/`INT`/`SIGNED`, `INT8`/`LONG` | Inteiros com sinal de 1, 2, 4 e 8 bytes. |
| `HUGEINT` | | Inteiro com sinal de 16 bytes. |
| `UTINYINT`, `USMALLINT`, `UINTEGER`, `UBIGINT`, `UHUGEINT` | | Inteiros sem sinal. Fora do contrato: Arrow e Redshift não os têm da mesma forma. |
| `FLOAT` | `FLOAT4`, `REAL` | Ponto flutuante de 4 bytes. |
| `DOUBLE` | `FLOAT8` | Ponto flutuante de 8 bytes. |
| `DECIMAL(p, s)` | `NUMERIC(p, s)` | Decimal exato; padrão `DECIMAL(18, 3)`. |
| `VARCHAR` | `CHAR`, `BPCHAR`, `TEXT`, `STRING` | String; o comprimento declarado não tem efeito. |
| `BLOB` | `BYTEA`, `BINARY`, `VARBINARY` | Binário. |
| `DATE` | | Data. |
| `TIME` | | Hora sem fuso. |
| `TIMESTAMP` | `DATETIME` | Data e hora sem fuso, microssegundos. |
| `TIMESTAMP WITH TIME ZONE` | `TIMESTAMPTZ` | Data e hora com fuso, exibida no fuso da sessão. |
| `INTERVAL` | | Intervalo. |
| `UUID` | | Identificador universal. |
| `JSON` | | JSON pela extensão `json`. |
| `BIT`, `BIGNUM` | `BITSTRING` | Bits e inteiro de tamanho variável. |

Tipos aninhados: `LIST` (`INTEGER[]`), `ARRAY` de tamanho fixo (`INTEGER[3]`), `STRUCT`, `MAP`,
`UNION` e `VARIANT` (valor semiestruturado que carrega o próprio tipo), aninháveis em qualquer
profundidade. A atualização de um valor aninhado é reescrita como
remoção e inserção.

A conversão de objetos Python segue regras fixas: `int` vira o menor inteiro em que cabe (`typeof(?)`
devolveu `INTEGER` para `1` e `BIGINT` para `2**40`), com `UBIGINT` e `DOUBLE` para o que não cabe em
`BIGINT`; `float` vira `DOUBLE`; `decimal.Decimal` vira `DECIMAL`; `datetime.datetime`
vira `TIMESTAMP` ou `TIMESTAMPTZ` conforme tenha `tzinfo`; `dict` vira `STRUCT` ou `MAP`. Colunas
`object` do pandas passam por uma fase de análise que amostra 1.000 valores para escolher o tipo; a
opção `pandas_analyze_sample` muda o tamanho da amostra.

### DECIMAL com escala fixa

`DECIMAL(largura, escala)` guarda um decimal exato. A largura vai de 1 a 38, a escala de 0 à largura,
e um `DECIMAL` sem parâmetros é `DECIMAL(18, 3)`, não um decimal livre como no PostgreSQL. A
representação interna depende da largura:

| Largura | Inteiro interno | Bytes |
| --- | --- | --- |
| 1 a 4 | `INT16` | 2 |
| 5 a 9 | `INT32` | 4 |
| 10 a 18 | `INT64` | 8 |
| 19 a 38 | `INT128` | 16 |

A documentação recomenda largura até 18: a aritmética em `INT128` é muito mais cara. `DECIMAL(18, 2)`,
o tipo do contrato para valores contábeis, cabe em 8 bytes e corresponde ao `decimal128(18, 2)` do
Arrow.

Regras aritméticas, verificadas na 1.5.5:

| Expressão | Tipo do resultado | Valor |
| --- | --- | --- |
| `1.10 + 2.20` | `DECIMAL(4, 2)` | `3.30` |
| `x * y` com `x`, `y` `DECIMAL(18, 2)` | `DECIMAL(18, 4)` | exato |
| `sum(x)` com `x` `DECIMAL(18, 2)` | `DECIMAL(38, 2)` | exato |
| `avg(x)` com `x` `DECIMAL(18, 2)` | `DOUBLE` | aproximado |
| `10::DECIMAL(18, 2) / 3` | `DOUBLE` | `3.3333333333333335` |

Soma, subtração e multiplicação ampliam o tipo para manter o resultado exato, e falham quando a
largura passaria de 38. Divisão e média saem em `DOUBLE`; um valor contábil derivado delas precisa de
`round(..., 2)::DECIMAL(18, 2)` explícito.

Conversões para a escala da coluna arredondam, e estouros de largura são erro:

| Operação | Resultado |
| --- | --- |
| `1.005::DECIMAL(18, 2)` e `'1.005'::DECIMAL(18, 2)` | `1.01` |
| `1.005::DOUBLE::DECIMAL(18, 2)` | `1.00`, porque o binário de 1.005 fica abaixo de 1.005. |
| `2.675::DOUBLE::DECIMAL(18, 2)` | `2.68` |
| `INSERT` de `1.2345::DECIMAL(18, 4)` em `DECIMAL(18, 2)` | `1.23` |
| `INSERT` de `decimal128(18, 3)` do Arrow em `DECIMAL(18, 2)` | Arredondado. |
| `INSERT` de coluna `float64` do pandas em `DECIMAL(18, 2)` | `1.005` vira `1.00`. |
| `INSERT` de `99999999999999999.99` em `DECIMAL(18, 2)` | `Conversion Error: ... value is out of range!` |

Uma coluna `object` do pandas com `decimal.Decimal` recebe o tipo pela amostra: a coluna `valor` da
amostra, com valores até `99999.99`, foi inferida como `DECIMAL(7, 2)`, e um valor maior num lote
posterior falharia no cast. A conversão para Arrow com o esquema do contrato antes do `INSERT` fixa o
tipo e foi 25 vezes mais rápida na medição da seção de ingestão.

A inferência e a correção aparecem numa ida e volta pelo Arrow, com o `arrow_schema` de
[sqlalchemy.md](sqlalchemy.md) e o modelo `Operacao` da seção sobre SQLAlchemy:

```python
# Coluna object de Decimal: tipo inferido pela amostra contra tipo fixado pelo esquema do contrato.
from datetime import date
from decimal import Decimal
import duckdb, pandas as pd, pyarrow as pa

con = duckdb.connect()
con.execute("CREATE TABLE operacoes (id_operacao BIGINT NOT NULL, data_ref DATE NOT NULL, "
            "id_cliente BIGINT NOT NULL, valor DECIMAL(18, 2) NOT NULL, descricao VARCHAR)")
df = pd.DataFrame({"id_operacao": [1, 2], "data_ref": [date(2026, 8, 1), date(2026, 8, 2)],
                   "id_cliente": [10, 20], "valor": [Decimal("10.50"), Decimal("99999.99")],
                   "descricao": ["a", None]})
con.sql("DESCRIBE SELECT valor FROM df").fetchone()[1]         # 'DECIMAL(7,2)', inferido da amostra

incoming = pa.Table.from_pandas(df, schema=arrow_schema(Operacao.__table__), preserve_index=False)
con.sql("DESCRIBE SELECT valor FROM entrada").fetchone()[1]    # 'DECIMAL(18,2)', fixado pelo contrato
con.execute("INSERT INTO operacoes BY NAME SELECT * FROM entrada")

returned = con.execute("SELECT * FROM operacoes ORDER BY id_operacao").to_arrow_table()
returned.schema.field("valor").type, returned.schema.field("data_ref").type   # decimal128(18, 2), date32[day]
returned.to_pandas(types_mapper=pd.ArrowDtype)["valor"].tolist() == df["valor"].tolist()   # True
```

Um lote posterior com `123456.78` na tabela inferida como `DECIMAL(7,2)` falha com `Conversion Error:
Casting value "123456.78" to type DECIMAL(7,2) failed: value is out of range!`. O cast do Arrow barra
`1.005` antes de qualquer arredondamento pelo DuckDB, com `Rescaling Decimal value would cause data
loss`.

### JSON

A extensão `json` vem na distribuição e carrega sozinha no primeiro uso. O tipo lógico `JSON` é
fisicamente um `VARCHAR` validado: `'unquoted'::JSON` falha com `Malformed JSON`, espaços e ordem das
chaves contam na comparação de igualdade, e chaves duplicadas são aceitas. Qualquer tipo converte para
`JSON` e volta: `'{"duck": 42}'::JSON::STRUCT(duck INTEGER)`, `{duck: 42}::JSON`.

Extração e criação:

```sql
CREATE TABLE eventos (id INTEGER, doc JSON);
INSERT INTO eventos VALUES (1, '{"a": 1, "b": {"c": [1, 2, 3]}}');
SELECT doc->>'a', json_extract(doc, '$.b.c[1]'), json_type(doc), json_structure(doc) FROM eventos;
```

A consulta acima devolve `1`, `2`, `OBJECT` e `{"a":"UBIGINT","b":{"c":["UBIGINT"]}}`. Os caminhos
JSON usam índice a partir de zero; listas e arrays SQL começam em um. `read_json` lê arquivos e infere
o esquema; `COPY (...) TO 'arquivo.json'` grava.

Na ida e volta com pandas, uma coluna de strings com JSON serializado entra direto na coluna `JSON`;
uma coluna de `dict` vira `STRUCT` e também entra, com conversão implícita no `INSERT ... SELECT`
(`{"a":1,"b":{"c":[1,2]}}` foi gravado sem cast); a leitura devolve `string` no Arrow e `str` no
pandas. Chaves com esquema fixo viram colunas do modelo, mais rápidas de filtrar; a coluna `JSON` fica
para o documento sem esquema fixo.

No contrato ([campos JSON](schema.md)), a coluna é `sa.JSON().with_variant(SUPER(), "redshift")`, que
compila `JSON` no DuckDB, e o texto JSON é a forma de troca. O DuckDB é o único ponto da cadeia que
valida: `::JSON` recusa texto malformado (`Malformed JSON at byte 1`), enquanto o Arrow e o Delta
aceitam qualquer texto. Uma tabela Arrow com a extensão `arrow.json` é vista como `JSON` ao ser
registrada; o `delta_scan` mostra `VARCHAR`, e `->>`, `json_extract` e `json_valid` funcionam sobre
ele; a saída em Arrow volta como `string`, sem a extensão.

```python
import json
import duckdb
import pyarrow as pa

# Documentos serializados entram como arrow.json; o DuckDB valida ao materializar e extrai campos.
documents = [{"origem": "sistema A", "tags": ["x"]}, None]
texts = pa.array([json.dumps(d) if d is not None else None for d in documents], pa.string())
incoming = pa.table({"id_evento": pa.array([1, 2], pa.int64()), "meta": texts.cast(pa.json_(pa.string()))})
con = duckdb.connect()
con.register("entrada", incoming)
con.sql("DESCRIBE SELECT * FROM entrada").fetchall()[1][1]                        # 'JSON'
con.execute("CREATE TABLE eventos (id_evento BIGINT, meta JSON)")
con.execute("INSERT INTO eventos BY NAME SELECT * FROM entrada")
con.sql("SELECT id_evento, meta->>'origem' AS origem, json_valid(meta) AS valido FROM eventos").fetchall()
# [(1, 'sistema A', True), (2, None, None)]
con.sql("SELECT meta FROM eventos").to_arrow_table().schema.field("meta").type     # string
```

## DDL

### CREATE TABLE

```sql
CREATE TABLE operacoes (
    id_operacao BIGINT NOT NULL,
    data_ref DATE NOT NULL,
    id_cliente BIGINT NOT NULL,
    valor DECIMAL(18, 2) NOT NULL,
    descricao VARCHAR,
    PRIMARY KEY (id_operacao, data_ref)
);
COMMENT ON TABLE operacoes IS 'Operações do mês';
```

Variantes documentadas:

- `CREATE OR REPLACE TABLE` remove e recria; `CREATE TABLE IF NOT EXISTS` não altera a existente.
- `CREATE TEMP TABLE` cria no esquema `temp.main`, visível só à conexão, em memória, com
  transbordo para `temp_directory` quando configurado.
- `CREATE TABLE ... AS SELECT` (CTAS) cria a partir de qualquer consulta, inclusive de um DataFrame ou
  de um arquivo: `CREATE TABLE operacoes AS FROM 'operacoes.parquet'`. O CTAS copia nomes e tipos e
  não aceita restrições; `AS FROM outra WITH NO DATA` copia só o esquema.
- Colunas aceitam `DEFAULT`, `NOT NULL`, `CHECK (expr)`, `UNIQUE`, `PRIMARY KEY` e
  `REFERENCES tabela (coluna)`. Chaves estrangeiras não aceitam `ON DELETE CASCADE` e não podem ser
  autorreferentes na inserção.
- Colunas geradas: `two_x AS (2 * x)`, apenas `VIRTUAL`.
- Sequências como chave: `CREATE SEQUENCE id_seq START 1;` e
  `id INTEGER PRIMARY KEY DEFAULT nextval('id_seq')`.

O DDL do sandbox sai do contrato SQLAlchemy, sem chaves pela política de restrições, compilado pelo
dialeto `duckdb_engine` e executado na conexão DuckDB:

```python
# DDL do sandbox derivado do contrato: sem chaves, com comentários, compilado pelo duckdb_engine.
import duckdb, duckdb_engine, sqlalchemy as sa
from sqlalchemy.schema import CreateTable, SetColumnComment, SetTableComment

def sandbox_table(table: sa.Table) -> sa.Table:
    """Cópia sem PRIMARY KEY, UNIQUE e FOREIGN KEY, conforme a política de restrições do sandbox."""
    columns = (sa.Column(c.name, c.type, nullable=c.nullable, comment=c.comment) for c in table.columns)
    return sa.Table(table.name, sa.MetaData(), *columns, comment=table.comment)

def create_table(con: duckdb.DuckDBPyConnection, table: sa.Table) -> None:
    sandbox_copy = sandbox_table(table)
    commands = [CreateTable(sandbox_copy, if_not_exists=True),
                *([SetTableComment(sandbox_copy)] if sandbox_copy.comment else []),
                *(SetColumnComment(c) for c in sandbox_copy.columns if c.comment)]
    for command in commands:
        con.execute(str(command.compile(dialect=duckdb_engine.Dialect())))

con = duckdb.connect()
create_table(con, Operacao.__table__)
con.sql("SELECT sql FROM duckdb_tables() WHERE table_name = 'operacoes'").fetchone()[0]
# 'CREATE TABLE operacoes(id_operacao BIGINT NOT NULL, data_ref DATE NOT NULL, id_cliente BIGINT NOT NULL,
#  valor DECIMAL(18,2) NOT NULL, descricao VARCHAR);'
con.sql("SELECT column_name, comment FROM duckdb_columns() WHERE comment IS NOT NULL").fetchall()
# [('id_cliente', 'Chave do cliente')]
```

`CreateTable` compila `NUMERIC(18, 2)` e `VARCHAR(200)`, que o catálogo guarda como `DECIMAL(18,2)` e
`VARCHAR`, e não emite `COMMENT ON`; `SetTableComment` e `SetColumnComment` entram à parte. A tabela
do contrato com `PRIMARY KEY (id_operacao, data_ref)` compila e executa do mesmo modo; uma chave
`Integer` de uma coluna precisa de `autoincrement=False`, senão sai como `SERIAL`. O modelo
`Operacao` e o caso do `SERIAL` estão na seção sobre SQLAlchemy.

### ALTER TABLE

```sql
ALTER TABLE operacoes ADD COLUMN moeda VARCHAR DEFAULT 'BRL';
ALTER TABLE operacoes RENAME COLUMN descricao TO historico;
ALTER TABLE operacoes ALTER COLUMN historico SET DEFAULT '';
ALTER TABLE operacoes ALTER COLUMN moeda SET NOT NULL;
ALTER TABLE operacoes ALTER id_cliente TYPE INTEGER;
ALTER TABLE operacoes ALTER historico SET DATA TYPE VARCHAR USING upper(historico);
ALTER TABLE operacoes DROP COLUMN moeda;
ALTER TABLE operacoes RENAME TO operacoes_antigas;
CREATE TABLE operacoes_copia AS FROM operacoes_antigas WITH NO DATA;
ALTER TABLE operacoes_copia ADD PRIMARY KEY (id_operacao, data_ref);
```

O CTAS não copia restrições, e a chave primária da cópia entra depois, pela cláusula `ADD PRIMARY
KEY`; numa tabela que já tem chave, o comando falha com `can have only one primary key`.

Limites: `ADD CONSTRAINT` e `DROP CONSTRAINT` não existem; colunas com índice (inclusive o índice de
`PRIMARY KEY` e `UNIQUE`) não podem ser removidas nem mudar de tipo, e um `CHECK` sobre várias
colunas bloqueia a remoção delas; a mudança de tipo falha se a coluna já conteve um valor
inconvertível, mesmo apagado, e o contorno é `CREATE OR REPLACE TABLE tbl AS FROM tbl`; renomear não
atualiza views. As alterações são transacionais e podem ser revertidas com `ROLLBACK`. A versão 1.5
acrescentou `ALTER TABLE ... SET ('opcao' = 'valor')` e `RESET` para opções de tabela, cujo efeito
depende do catálogo.

### DROP TABLE

```sql
DROP TABLE IF EXISTS operacoes_antigas;
DROP SCHEMA execucao_abc123 CASCADE;
```

`RESTRICT`, o padrão, recusa remover um objeto com dependentes rastreados (esquema com tabelas,
sequências, tipos, views); `CASCADE` remove todos. Views não são rastreadas: uma view sobre tabela
removida fica inválida e falha na consulta. O espaço da tabela vira blocos livres do arquivo, visíveis
em `PRAGMA database_size`; o arquivo não diminui. Para encolher, o caminho é copiar o banco para um
arquivo novo com `COPY FROM DATABASE origem TO destino` ou exportar e importar.

### Índices, chaves e restrições

Os zonemaps existem para todas as colunas de tipos simples e tornam a ordenação por coluna de filtro
a otimização principal (seção de performance). A ART é o único índice explícito:

```sql
CREATE INDEX idx_cliente ON operacoes (id_cliente);
CREATE UNIQUE INDEX idx_operacao ON operacoes (id_operacao);
DROP INDEX idx_cliente;
```

Regras da documentação:

- Só índices de uma coluna sem expressão servem a index scans, usados em igualdade e `IN (...)` quando
  a seletividade estimada fica abaixo de `MAX(2048, 0,1 % das linhas)`; filtros dinâmicos de joins
  também entram nesses scans.
- A memória dos índices não é gerenciada pelo buffer manager; o índice precisa caber em memória na
  criação e permanece carregado até um `DETACH`/`ATTACH`.
- Um índice, explícito ou de chave, deve ser criado depois da carga.

A [política de restrições](schema.md) do projeto omite `PRIMARY KEY`, `UNIQUE` e `FOREIGN KEY` no
sandbox e verifica unicidade na auditoria. A medição da seção de ingestão confirma o custo: a carga de
300.000 linhas via Arrow levou 0,008 s sem chave e 0,073 s com `PRIMARY KEY (id_operacao, data_ref)`.

Restrições existentes se comportam como no PostgreSQL, com dois limites: um `UPDATE` que passa por
chave é verificado bloco a bloco, e `UPDATE my_table SET i = i + 1` numa tabela com 3.000 chaves
sequenciais falha com `Duplicate key "i: 2048"`; uma tabela com chave estrangeira pode acusar violação
ao atualizar um valor aninhado na tabela referenciada. O contorno documentado é `DELETE ... RETURNING`
seguido de `INSERT` na mesma transação.

### Documentação do esquema

`COMMENT ON` segue a sintaxe do PostgreSQL para tabelas, colunas, views, índices, sequências, tipos e
macros; `IS NULL` remove o comentário:

```sql
COMMENT ON TABLE operacoes IS 'Operações do mês';
COMMENT ON COLUMN operacoes.id_cliente IS 'Chave do cliente';
SELECT table_name, comment FROM duckdb_tables() WHERE comment IS NOT NULL;
SELECT column_name, comment FROM duckdb_columns() WHERE table_name = 'operacoes';
```

Limites documentados: não há comentários em esquemas e bancos, nem em objetos com dependências, como
uma tabela com índice. No teste, a tabela com chave primária composta aceitou o comentário de tabela
e de coluna. A definição completa da tabela sai de `duckdb_tables().sql`:

```text
CREATE TABLE operacoes(id_operacao BIGINT, data_ref DATE, id_cliente BIGINT NOT NULL,
    valor DECIMAL(18,2) NOT NULL, descricao VARCHAR, PRIMARY KEY(id_operacao, data_ref));
```

## SELECT, INSERT, UPDATE e DELETE

A conexão Python (`duckdb.connect()`) enxerga DataFrames pandas e polars, tabelas, datasets,
scanners e `RecordBatchReader` do Arrow pelo nome da variável Python, como se fossem tabelas
(replacement scan). Objetos guardados em dicionários ou atributos entram com
`con.register('nome', obj)`. A precedência é: objetos registrados, tabelas e views do banco,
variáveis Python. `SET python_enable_replacements = false` desliga a busca por variáveis. Esses
objetos são só de leitura: `INSERT` e `UPDATE` sobre um DataFrame não existem.

### SELECT com saída em Arrow e pandas

```python
from datetime import date
import duckdb, pandas as pd

con = duckdb.connect("sandbox.duckdb")
result = con.execute("SELECT * FROM operacoes WHERE data_ref >= ? ORDER BY data_ref", [date(2026, 8, 1)])
table = result.to_arrow_table()                      # decimal128(18, 2), date32, int64, string
df = table.to_pandas(types_mapper=pd.ArrowDtype)  # mantém decimal128 e date32
```

Formas de saída, com o tempo de uma execução para 300.000 linhas:

| Chamada | Tipos de `valor` e `data_ref` | Tempo |
| --- | --- | --- |
| `to_arrow_table()` | `decimal128(18, 2)`, `date32[day]` | 0,005 s |
| `to_arrow_reader(tamanho_lote)` | Os mesmos, em lotes. | 0,54 s para 20.000.000 de linhas em três colunas em lotes de 100.000, com o processo em 83 MB contra 567 MB de `to_arrow_table` (2026-09-20). |
| `df()` | `float64`, `datetime64[us]` | 0,020 s |
| `to_arrow_table().to_pandas(types_mapper=pd.ArrowDtype)` | `decimal128(18, 2)[pyarrow]`, `date32[day][pyarrow]` | 0,005 s mais menos de 0,001 s |
| `pl()` | `Decimal(18, 2)`, `Date` | 0,086 s |
| `fetchall()`, `fetchone()` | `decimal.Decimal`, `datetime.date` | |

`df()` é o caminho a evitar para valores contábeis. `fetchnumpy()` devolve arrays mascarados e
`fetch_df_chunk()` devolve o resultado em pedaços de 2.048 linhas vezes um multiplicador.

O leitor de `to_arrow_reader` pertence à consulta em curso: outro comando no mesmo cursor o esvazia
sem erro, e num `cursor()` próprio ele entrega o snapshot da sua consulta enquanto os outros cursores
inserem na mesma tabela, criam, alteram e apagam tabelas; fechar o cursor no meio não o interrompeu,
e cem `cursor()` mais `close()` levaram 0,4 ms. Sem `ORDER BY` o primeiro lote chega antes do fim da
consulta (3 ms em 20.000.000 de linhas); com `ORDER BY`, o `execute` só volta depois da ordenação
inteira (2,4 s) e os lotes vêm em seguida. Um erro que a consulta encontra no meio da leitura chega
ao Python como `OSError` com a mensagem do DuckDB, não como `duckdb.Error` (2026-09-20,
`test_duckdb.py`, `tests/test_engine_duckdb.py`). A sessão única da biblioteca consome o leitor inteiro num
arquivo antes do comando seguinte, lote a lote numa thread, e o cliente lê cada lote gravado
enquanto a consulta continua (2026-09-23, [`POC.md`](POC.md)).

Parâmetros: `?` posicional, `$1` numerado e reutilizável, `$nome` nomeado com um dicionário. A API
relacional (`con.sql(...)`, `con.table('operacoes').filter(...)`) monta consultas preguiçosas e
compõe relações por nome.

### INSERT

```python
import pyarrow as pa

incoming = pa.Table.from_pandas(df, schema=contract_schema, preserve_index=False)
con.execute("INSERT INTO operacoes BY NAME SELECT * FROM entrada")
con.append("operacoes", df, by_name=True)          # equivalente sem SQL
```

`INSERT INTO ... BY NAME` associa colunas pelo nome, aceita colunas faltantes (preenchidas com
`DEFAULT` ou `NULL`) e rejeita nomes desconhecidos; o padrão `BY POSITION` segue a ordem da tabela.
Valores de tipo diferente sofrem conversão automática, inclusive de `VARCHAR` para número, então a
conferência de tipos precisa acontecer no Arrow, antes do comando.

Um `RecordBatchReader` registrado entra por um único `INSERT ... SELECT`, atômico: se o gerador
Python que o alimenta falha no lote 20, o comando falha inteiro e a tabela fica como estava. O
`arrow_scan` lê o fluxo por uma thread de leitura antecipada do Arrow, que chama o gerador em outra
thread, tinha puxado de 5 a 15 lotes quando o comando falhou no primeiro, chegou a 10 ou 20 depois
da falha e parou ali; um gerador preso nessa thread numa espera sem prazo, ou ainda chamando Python
na saída do processo, segura o processo no destrutor do pool de threads do Arrow. O leitor não
confere os lotes contra o esquema declarado, e o `arrow_scan` lê os buffers por esse esquema: um
lote com as colunas em outra ordem entra sem erro com os bytes trocados, `(1, 1.0)` lido como
`(4607182418800017408, 5e-324)`; uma coluna a mais ou a menos falha com `ArrowArray struct has 3
children, expected 2`. A nulidade do esquema Arrow não é conferida; a coluna `NOT NULL` da tabela é.
Um `INSERT` por lote, um `RecordBatch` em memória por comando dentro de uma transação, custa cerca
de 3,5 ms por comando (2026-09-20). A biblioteca faz o `cast` de cada lote, grava os lotes num
arquivo Arrow IPC com LZ4 fora da sessão e os insere num único comando sobre o leitor do arquivo,
que é nativo e não traz a leitura antecipada de um gerador Python (2026-09-22, `test_duckdb.py`,
`tests/test_engine_duckdb.py`, [`POC.md`](POC.md)).

Conflitos em chave primária ou `UNIQUE`:

```sql
INSERT OR IGNORE INTO operacoes BY NAME SELECT * FROM entrada;
INSERT INTO operacoes BY NAME SELECT * FROM entrada
    ON CONFLICT (id_operacao, data_ref) DO UPDATE SET valor = EXCLUDED.valor, descricao = EXCLUDED.descricao;
INSERT OR REPLACE INTO operacoes BY NAME SELECT * FROM entrada;
```

O alvo `ON CONFLICT (colunas)` é opcional enquanto a tabela tem uma única restrição de unicidade;
com mais de uma (chave primária mais um índice `UNIQUE`, por exemplo), `DO UPDATE` e
`INSERT OR REPLACE` exigem o alvo. `RETURNING *` devolve as linhas inseridas, útil com sequências e
colunas geradas.

`MERGE INTO` faz o mesmo sem exigir chave, com condição livre e ações condicionais:

```sql
MERGE INTO operacoes
    USING entrada USING (id_operacao, data_ref)
    WHEN MATCHED AND operacoes.valor <> entrada.valor THEN UPDATE SET valor = entrada.valor
    WHEN NOT MATCHED THEN INSERT BY NAME
    WHEN NOT MATCHED BY SOURCE AND operacoes.data_ref >= DATE '2026-08-01' THEN DELETE
    RETURNING merge_action, *;
```

### UPDATE e DELETE

```sql
UPDATE operacoes SET descricao = correcoes.descricao
    FROM correcoes WHERE operacoes.id_operacao = correcoes.id_operacao;
DELETE FROM operacoes USING cancelamentos WHERE operacoes.id_operacao = cancelamentos.id_operacao;
DELETE FROM operacoes WHERE data_ref < DATE '2020-01-01' RETURNING id_operacao;
TRUNCATE operacoes;
```

`UPDATE ... FROM` e `DELETE ... USING` aceitam um DataFrame registrado como fonte, o que transforma
uma lista de correções num único comando em lote. A substituição de um mês inteiro, unidade de
escrita do projeto, é uma transação:

```sql
BEGIN TRANSACTION;
DELETE FROM operacoes WHERE data_ref >= DATE '2026-08-01' AND data_ref < DATE '2026-09-01';
INSERT INTO operacoes BY NAME SELECT * FROM entrada;
COMMIT;
```

Na API Python, `con.begin()`, `con.commit()` e `con.rollback()` fazem o mesmo; sem `begin()`, cada
comando é sua própria transação. Vários comandos numa única string executam numa transação implícita.

Com a coluna `mes` do contrato, a substituição vira uma função que confere o lote antes de
confirmar:

```python
# Substituição de um mês: DELETE e INSERT numa transação, desfeita se a conferência do lote falhar.
def replace_month(con: duckdb.DuckDBPyConnection, month: str, incoming: pa.Table) -> None:
    con.register("entrada", incoming)
    con.begin()
    try:
        con.execute("DELETE FROM operacoes WHERE mes = ?", [month])
        con.execute("INSERT INTO operacoes BY NAME SELECT * FROM entrada")
        (outside,) = con.execute("SELECT count(*) FROM operacoes "
                              "WHERE mes = ? AND strftime(data_ref, '%Y-%m') <> mes", [month]).fetchone()
        if outside:
            raise ValueError(f"{outside} linhas com data_ref fora do mês {month}")
        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.unregister("entrada")
```

O `rollback()` desfaz o `DELETE` e o `INSERT`: no teste local, um lote com `data_ref` de setembro
marcado como `2026-08` levantou `ValueError: 1 linhas com data_ref fora do mês 2026-08` e a tabela
ficou como estava. `register` dá nome SQL à tabela Arrow e `unregister` o remove ao fim, com ou sem
erro.

## Ingestão de dados

A documentação ordena as formas de importar: um scanner de extensão quando a fonte é MySQL,
PostgreSQL, SQLite ou ODBC; senão, exportar da fonte para Parquet ou CSV e carregar com o leitor
nativo; senão, o appender, disponível em C, C++, Go, Java e Rust, não em Python. Laços de `INSERT`
ficam para volumes abaixo de 100.000 linhas, e `executemany` não deve ser usado para volume.

Medições de carga de uma tabela sem chave, em memória, com os dados já em Python (menor de três
execuções; as linhas com 50.000 linhas usam os métodos lentos):

| Método | Linhas | Tempo |
| --- | --- | --- |
| `INSERT INTO t SELECT * FROM tabela_arrow` | 300.000 | 0,008 s |
| `INSERT INTO t BY NAME SELECT * FROM df` com dtypes pyarrow | 300.000 | 0,008 s |
| `con.append('t', df)` | 300.000 | 0,008 s |
| `CREATE TABLE t AS SELECT * FROM tabela_arrow` | 300.000 | 0,008 s |
| `INSERT INTO t SELECT * FROM tabela_arrow` com chave primária composta | 300.000 | 0,073 s |
| `INSERT INTO t SELECT * FROM tabela_arrow` em banco em arquivo, mais `CHECKPOINT` | 300.000 | 0,055 s |
| `pyarrow.parquet.write_table` (zstd) mais `INSERT ... SELECT FROM read_parquet(...)` | 300.000 | 0,056 s |
| `INSERT INTO t BY NAME SELECT * FROM df` com colunas `object` (`Decimal`, `date`) | 300.000 | 0,202 s |
| SQLAlchemy Core, `conn.execute(insert(t), lista_de_dicts)` via duckdb_engine | 50.000 | 0,670 s |
| ORM, `session.execute(insert(Modelo), lista_de_dicts)` via duckdb_engine | 50.000 | 0,978 s |
| ORM, o mesmo com `execution_options(render_nulls=True)` | 50.000 | 0,772 s |
| `pandas.DataFrame.to_sql` padrão via duckdb_engine | 50.000 | 0,736 s |
| `pandas.DataFrame.to_sql(method="multi", chunksize=5000)` | 50.000 | 1,999 s |
| `con.executemany` dentro de `BEGIN`/`COMMIT` | 50.000 | 4,356 s |
| `con.executemany` com autocommit | 50.000 | 5,241 s |

O arquivo com as 300.000 linhas ocupou 3.420.160 bytes após o `CHECKPOINT`.

A forma mais performática de ingerir um DataFrame pandas numa sessão Python é, portanto:

1. Converter o DataFrame numa tabela Arrow com o esquema do contrato (`pa.Table.from_pandas` com
   `schema=` ou `table.cast(schema, safe=True)`), o que fixa `decimal128(18, 2)` e `date32`, rejeita
   valores fora do tipo e evita a análise por amostra das colunas `object`. O cast também corrige as
   surpresas de dtype do pandas com backend numpy, como uma coluna de inteiros com nulos que vira
   `float64`.
2. Executar `INSERT INTO tabela BY NAME SELECT * FROM tabela_arrow`, ou `con.append`, num único
   comando. Com vários lotes, envolver em `BEGIN`/`COMMIT`.
3. Criar chaves ou índices, se forem necessários, depois da carga.

Para volumes maiores que a memória, os dados passam por Parquet: `SET preserve_insertion_order =
false` libera o DuckDB para reordenar e reduz a memória; `memory_limit` (padrão 80 % da RAM) e
`threads` limitam o uso; `temp_directory` recebe o transbordo. O DuckDB paraleliza a leitura por row
group e entre arquivos.

A fonte da verdade é a tabela Delta ([delta.md](delta.md)), e o sandbox recebe só os meses da
execução:

```python
# Meses da tabela Delta carregados no sandbox: snapshot fixado, filtro por partição e tabela local.
import duckdb

uri = "s3://bucket/prod/cad_operacoes"                 # ou o caminho de uma pasta local
con = duckdb.connect("sandbox.duckdb")
con.execute("INSTALL delta; LOAD delta;")
con.execute(f"ATTACH '{uri}' AS fonte (TYPE delta, PIN_SNAPSHOT true)")
con.execute("CREATE TABLE operacoes AS SELECT * FROM fonte WHERE mes IN ('2026-07', '2026-08')")
con.sql("SELECT mes, count(*), min(data_ref), max(data_ref) FROM operacoes GROUP BY 1 ORDER BY 1").fetchall()
# [('2026-07', 101091, datetime.date(2026, 7, 1), datetime.date(2026, 7, 31)),
#  ('2026-08', 101079, datetime.date(2026, 8, 1), datetime.date(2026, 8, 31))]

# Versão antiga lida sem anexar; a URI entra como parâmetro da função de tabela.
con.execute("SELECT count(*) FROM delta_scan(?, version := 1) WHERE mes = '2026-08'", [uri]).fetchone()
```

O exemplo rodou sobre uma cópia local da tabela, com a amostra de 300.000 linhas em três meses. O
`CREATE TABLE ... AS` traz a coluna de partição `mes` como coluna comum e todas as colunas como
anuláveis; a nulidade fica com o esquema Delta e com a auditoria. `PIN_SNAPSHOT true` fixa a versão
lida: um commit externo depois do `ATTACH` não mudou a contagem de `fonte`, e um `delta_scan` novo já
viu a linha nova. O filtro por `mes` poda as partições (`Scanning Files: 1/4` no `EXPLAIN ANALYZE` do
teste). Para URIs `s3://`, `CREATE SECRET (TYPE s3, PROVIDER credential_chain)` antes do `ATTACH` usa
as credenciais da sessão. O caminho de volta, do sandbox para o Delta, é
`write_deltalake(uri, con.execute(consulta).to_arrow_reader(50_000), mode="overwrite",
predicate="mes = '2026-08'")`; `fetch_record_batch` está depreciado na 1.5.5 em favor de
`to_arrow_reader`.

## Exportação para Parquet

O `COPY ... TO` grava Parquet com escritor paralelo. As opções relevantes ao projeto, detalhadas no
[documento sobre Parquet](parquet.md):

```sql
COPY (
    SELECT id_operacao, data_ref, id_cliente, valor, descricao
    FROM operacoes
    WHERE data_ref >= DATE '2026-08-01' AND data_ref < DATE '2026-09-01'
    ORDER BY data_ref, id_operacao
) TO 'operacoes/mes=2026-08/exec_abc123.parquet' (
    FORMAT parquet,
    COMPRESSION zstd,
    ROW_GROUP_SIZE 100_000,
    FIELD_IDS {id_operacao: 1, data_ref: 2, id_cliente: 3, valor: 4, descricao: 5},
    KV_METADATA {serialize_db_version: '0.1.0', serialize_db_execution_id: 'abc123'},
    RETURN_STATS
);
```

- `FILE_SIZE_BYTES` divide a saída em vários arquivos de tamanho alvo, e `PER_THREAD_OUTPUT` grava
  um arquivo por thread; `PARTITION_BY` produz pastas Hive, com `WRITE_PARTITION_COLUMNS` para
  manter as colunas de partição dentro dos arquivos.
- `OVERWRITE_OR_IGNORE`, `OVERWRITE` e `APPEND` controlam a escrita sobre pastas existentes;
  `FILENAME_PATTERN '{uuid}'` evita colisões de nome.
- `RETURN_STATS` devolve, por arquivo, contagem de linhas, tamanho e estatísticas por coluna, base
  para a conferência antes da publicação. Os valores saem como texto, com as chaves
  `column_size_bytes`, `min`, `max`, `null_count` e `num_values`, e `has_nan` numa coluna de ponto
  flutuante que tenha um: um `DOUBLE` faz o percurso de ida e volta por `float`, um `VARCHAR` sai
  inteiro, sem truncar, e uma coluna só de nulos não traz `min` nem `max` (2026-09-22,
  [`POC.md`](POC.md)).
- `WRITE_BLOOM_FILTER` (padrão verdadeiro) grava filtros Bloom para colunas codificadas por
  dicionário; `PARQUET_VERSION V2` habilita as codificações mais novas.
- O escritor da versão 1.5.5 declara todas as colunas como `optional`, mesmo `NOT NULL`, grava
  `DECIMAL(18, 2)` como `INT64` e não grava page index, como mostram os metadados lidos no
  [documento sobre Parquet](parquet.md). O leitor de outros sistemas recebe essas propriedades; a
  conferência de `NOT NULL` fica na auditoria.

A API relacional oferece o atalho `con.sql(query).write_parquet('arquivo.parquet')`. A
[gravação com ordenação](parquet.md) pela chave de ordenação melhora a compressão e a poda por
estatísticas na leitura.

Em Python, o `RETURN_STATS` devolve uma linha por arquivo gravado, e `column_statistics` chega como
dicionário de dicionários de texto:

```python
# Conferência do arquivo exportado pelo RETURN_STATS: nulos em colunas NOT NULL e data_ref dentro da partição.
import os

def export_partition(con: duckdb.DuckDBPyConnection, value: str, folder: str) -> dict[str, dict[str, str]]:
    destination = f"{folder}/mes={value}/exec_abc123.parquet"
    os.makedirs(os.path.dirname(destination), exist_ok=True)         # o COPY não cria a pasta
    (row,) = con.execute(f"""
        COPY (SELECT id_operacao, data_ref, id_cliente, valor, descricao FROM operacoes
              WHERE mes = ? ORDER BY data_ref, id_operacao)
        TO '{destination}' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 100_000, RETURN_STATS)""", [value]).fetchall()
    stats = row[4]      # filename, count, file_size_bytes, footer_size_bytes, column_statistics, partition_keys
    columns = {name.strip('"'): values for name, values in stats.items()}   # as chaves vêm entre aspas
    for name in ("id_operacao", "data_ref", "id_cliente", "valor"):
        if columns[name]["null_count"] != "0":
            raise ValueError(f"{name}: {columns[name]['null_count']} nulos em coluna NOT NULL")
    if not (columns["data_ref"]["min"][:7] == value == columns["data_ref"]["max"][:7]):
        raise ValueError(f"data_ref fora da partição {value}: {columns['data_ref']['min']} a {columns['data_ref']['max']}")
    return columns

export_partition(con, "2026-08", "operacoes")["valor"]
# {'column_size_bytes': '264685', 'max': '99994.51', 'min': '0.04', 'null_count': '0', 'num_values': '149991'}
```

Os valores `min`, `max`, `null_count`, `num_values` e `column_size_bytes` são strings, inclusive para
`DECIMAL` e `DATE` (`'2026-08-01'` a `'2026-08-31'` para `data_ref`). O `?` do valor da partição é
aceito dentro da consulta do `COPY`. É o `COPY` de `export_partition` no modo `register` da
[etapa 4](PLAN-STAGE-4.md), e os mesmos valores alimentam a `AddAction` do registro do arquivo na
tabela Delta ([delta.md](delta.md)).

## Recomendações de performance

### Ingestão

- Tipos corretos: colunas de data como `DATE` ou `TIMESTAMP`, chaves como `BIGINT`. Na medição da
  documentação, `DATETIME` em vez de `VARCHAR` reduziu o armazenamento de 5,2 GB para 3,3 GB e o tempo
  da agregação de 3,9 s para 0,9 s; um join em `BIGINT` foi 1,8 vezes mais rápido que o mesmo join em
  `VARCHAR`.
- Sem chaves nem índices na carga; criá-los depois, se a integridade exigir.
- Um comando por lote, a partir de Arrow ou Parquet; laços dentro de transação.
- Bancos persistentes usam compressão e podem ser mais rápidos que bancos em memória: a consulta 1 do
  TPC-H (fator 30) levou 4,22 s num banco em memória sem compressão, 0,55 s com `COMPRESS` e 0,56 s
  em arquivo.
- Uma conexão reutilizada; abrir e fechar a cada consulta descarta caches de dados e metadados.
- Memória: mínimo de 125 MB por thread; recomendação de 1 a 4 GB por thread (1 a 2 GB para
  agregações, 3 a 4 GB para joins); com falta de memória, reduzir `threads`, baixar `memory_limit`
  para 50 a 60 % da RAM e desligar `preserve_insertion_order`.
- Disco: SSD ou NVMe; EBS serve; a documentação desaconselha o formato nativo em modo leitura e
  escrita sobre NFS e SMB.

Os limites entram na abertura da conexão ou por `SET`:

```python
# Memória, threads e pasta de transbordo definidos na abertura da conexão e conferidos no catálogo.
import duckdb

con = duckdb.connect("sandbox.duckdb", config={
    "memory_limit": "8GB",                    # 50 a 60 % da RAM quando a memória falta
    "threads": 4,                             # 1 a 4 GB por thread
    "temp_directory": "sandbox_tmp",          # transbordo das operações maiores que a memória
    "preserve_insertion_order": False,        # libera o reordenamento e reduz a memória
})
con.sql("SELECT name, value FROM duckdb_settings() WHERE name IN "
        "('memory_limit', 'threads', 'temp_directory', 'preserve_insertion_order') ORDER BY name").fetchall()
# [('memory_limit', '7.4 GiB'), ('preserve_insertion_order', 'false'), ('temp_directory', 'sandbox_tmp'), ('threads', '4')]
con.execute("SET threads = 2; SET memory_limit = '4GB'")     # ajuste em tempo de execução
con.sql("SELECT current_setting('threads'), current_setting('memory_limit')").fetchone()   # (2, '3.7 GiB')
```

O `memory_limit` só aceita valor com unidade: `'60%'` e `'60'` são recusados com `Parser Error:
Unknown unit for memory` (DuckDB 1.5.5, 2026-09-22), e quem quer uma fração da máquina a calcula
antes. O padrão é 80% da memória que o DuckDB detecta: 14,3 GiB numa máquina que o `os.sysconf` do
Python lê como 18,0 GiB, e 6,1 GiB dos 7,6 GiB do ambiente alvo.

Sem `temp_directory`, um banco em arquivo transborda para `<arquivo>.tmp` ao lado dele e um banco em
memória para `.tmp` no diretório corrente; `max_temp_directory_size` limita o transbordo a 90 % do
espaço livre do disco por padrão. `8GB` aparece como `7.4 GiB` porque o limite é lido em bytes
decimais e exibido em unidades binárias.

O `threads` é da instância do banco, não da conexão: `duckdb_settings()` o dá com escopo `GLOBAL`,
`SET SESSION threads` é recusado (`option "threads" cannot be set locally`), e o valor que uma
conexão grava é o que as outras leem. O pool vale para todas as conexões e cursores da instância, e
a thread que chama cada conexão também executa a consulta dela, ao lado do pool (`external_threads`,
1 por padrão). Numa varredura de 100.000.000 de linhas em memória, com 11 núcleos (DuckDB 1.5.5,
2026-09-23, melhor de três), cada linha é o tempo de uma consulta sozinha e o de duas e de quatro
conexões em threads Python distintas, juntas:

| `threads` | Uma | Duas juntas | Quatro juntas |
| --- | --- | --- | --- |
| 1 | 0,608 s | 0,622 s | 0,639 s |
| 2 | 0,310 s | 0,473 s | 0,600 s |
| 4 | 0,159 s | 0,269 s | 0,449 s |
| 8 | 0,097 s | 0,175 s | 0,326 s |
| 11, o padrão | 0,079 s | 0,160 s | 0,313 s |
| 22 | 0,078 s | 0,156 s | 0,307 s |

Com `threads` igual aos núcleos, uma varredura grande já ocupa a máquina, e as conexões juntas levam
o tempo delas em série; mais threads que núcleos não mudam nada num trabalho de CPU. Uma conexão a
mais ganha quando a consulta não ocupa os núcleos: menos de `k × 122.880` linhas, um operador que
não se paraleliza (quatro `SELECT sum(hash(range)) FROM range(300_000_000)` juntos levaram 2,014 s
contra 1,777 s de um com `threads = 1`, e 2,042 s contra 1,904 s com 11, porque a função `range` gera
as linhas numa thread só) ou a espera do S3, onde cada thread faz uma requisição HTTP por vez.

### Organização das tabelas para filtros por chave e joins

- Ordenar os dados pelas colunas de filtro na carga. A ordenação alimenta os zonemaps: no exemplo da
  documentação, a coluna de timestamps ordenada ocupou 1,3 GB em vez de 3,3 GB e a consulta caiu de
  0,9 s para 0,6 s. Para a tabela `operacoes`, a ordem `data_ref, id_operacao` da chave de ordenação
  do contrato serve aos filtros por mês e por identificador.
- Chaves inteiras crescentes em vez de `UUID` para filtros seletivos: um `UUID` fora de ordem obriga
  a varrer muitos row groups.
- Filtros de igualdade e `IN` sobre uma coluna com ART e seletividade muito alta usam index scan;
  para os demais, os zonemaps bastam.
- Joins: o DuckDB usa hash join com filtros dinâmicos empurrados ao scan da tabela maior; índices e
  chaves não mudam o plano. O otimizador reordena joins por custo com estatísticas das tabelas e dos
  arquivos Parquet. Para forçar uma ordem, `SET disabled_optimizers =
  'join_order,build_side_probe_side'` ou tabelas temporárias intermediárias.
- Colunas de join com o mesmo tipo exato nas duas tabelas (`BIGINT` com `BIGINT`), sem cast.
- Row groups de 100.000 a 1.000.000 de linhas e arquivos de 100 MB a 10 GB ao ler Parquet; em
  arquivos remotos, selecionar colunas, filtrar e aumentar `threads` para 2 a 5 vezes o número de
  núcleos, porque a E/S é síncrona por thread.
- `EXPLAIN ANALYZE` mostra o plano e o tempo por operador; sinais de problema são nested loop joins,
  filtros aplicados depois do scan e cardinalidades explodindo nos joins.

### Diferenças de abordagem em relação a bancos relacionais tradicionais

Num banco relacional de linha, a performance vem de índices, de transações curtas e de atualizações
pontuais. No DuckDB, ela vem da ordenação física dos dados, dos tipos, do tamanho dos lotes e da
memória disponível. Não há tuning de índices para joins, não há `VACUUM` para recuperar espaço, e as
tabelas se comportam melhor quando reconstruídas ou substituídas por partição do que quando
atualizadas linha a linha. O único processo escritor e o controle otimista de concorrência
dispensam bloqueios, mas exigem reexecutar a transação em caso de conflito. As restrições existem,
porém custam na carga e não ajudam nas consultas, o que inverte a prática habitual de declarar todas
as chaves.

## Suporte a SQLAlchemy

O dialeto é o pacote `duckdb_engine` (versão 0.17.0), derivado do dialeto PostgreSQL com psycopg2 do
SQLAlchemy. Ele não consta da lista de dialetos externos da documentação do SQLAlchemy. A URL é
`duckdb:///:memory:` ou `duckdb:///caminho/sandbox.duckdb`; `connect_args={"config": {...}}` passa
as opções do DuckDB, e `preload_extensions` e `register_filesystems` carregam extensões e sistemas de
arquivos `fsspec`.

Comportamentos verificados com SQLAlchemy 2.0.54 e duckdb_engine 0.17.0:

| Aspecto | Comportamento |
| --- | --- |
| Cache de comandos | `supports_statement_cache = False`; cada execução recompila o SQL. |
| `rowcount` | Sempre `-1` em `INSERT`, `UPDATE` e `DELETE`. |
| Tipos | O DDL emite `NUMERIC(18, 2)` e `VARCHAR(200)`; o catálogo guarda `DECIMAL(18,2)` e `VARCHAR`, sem o comprimento. `JSON` cria coluna `JSON` e converte `dict` na ida e na volta. |
| Comentários | `Table.comment` e `Column.comment` geram `COMMENT ON` no `create_all`. |
| Reflexão | Colunas, tipos, nulidade e comentários voltam por `inspect(engine)`; `get_pk_constraint` devolve vazio e índices não são refletidos, embora `duckdb_constraints()` liste a chave. |
| Chave primária inteira | Com o `autoincrement="auto"` padrão, uma chave `Integer` de uma coluna sai como `SERIAL`, e o `create_all` falha com `Type with name SERIAL does not exist!`; `autoincrement=False` resolve para chaves geradas no cliente. `Identity()` gera `GENERATED BY DEFAULT AS IDENTITY`, que o DuckDB rejeita. `Sequence('nome')` na coluna funciona e devolve o valor gerado ao ORM. |
| `postgresql.insert(...).on_conflict_do_update` | Compila e executa. |
| Banco em memória | O pool é `SingletonThreadPool`: uma conexão aberta sem fechar mantém uma transação, e o próximo `engine.begin()` falha com `cannot start a transaction within a transaction`. |
| Replacement scans | Variáveis Python não são visíveis ao executar pelo engine; o DataFrame precisa de `register` na conexão bruta. |
| `pandas.read_sql` | `coerce_float=True` (padrão) converte `Decimal` em `float`; `dtype_backend="pyarrow"` devolve `double[pyarrow]` para `DECIMAL` e `string[pyarrow]` para `DATE`; `coerce_float=False` devolve `Decimal` e `date` em colunas `object`. |

### Modelo, DDL e consulta que devolve um DataFrame

```python
import datetime as dt, decimal
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import BigInteger, Numeric, String, select
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

class Base(DeclarativeBase):
    pass

class Operacao(Base):
    __tablename__ = "operacoes"
    __table_args__ = {"comment": "Operações do mês"}
    id_operacao: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=False)
    data_ref: Mapped[dt.date] = mapped_column(primary_key=True)
    id_cliente: Mapped[int] = mapped_column(BigInteger, comment="Chave do cliente")
    valor: Mapped[decimal.Decimal] = mapped_column(Numeric(18, 2))
    descricao: Mapped[str | None] = mapped_column(String(200))

engine = sa.create_engine("duckdb:///sandbox.duckdb", connect_args={"config": {"memory_limit": "4GB"}})
Base.metadata.create_all(engine)

query = select(Operacao).where(Operacao.data_ref >= dt.date(2026, 8, 1)).order_by(Operacao.id_operacao)

# Caminho pandas puro: Decimal e date preservados como objetos Python.
df = pd.read_sql(query, engine, coerce_float=False)

# Caminho Arrow: SQL compilado pelo SQLAlchemy, executado pela conexão DuckDB.
with engine.connect() as conn:
    compiled = query.compile(dialect=engine.dialect)
    parameters = [compiled.params[name] for name in compiled.positiontup]
    con = conn.connection.dbapi_connection
    table = con.execute(str(compiled), parameters).to_arrow_table()
df = table.to_pandas(types_mapper=pd.ArrowDtype)   # decimal128(18, 2)[pyarrow], date32[day][pyarrow]
```

O dialeto ligado ao engine usa `paramstyle = "numeric_dollar"` (um `duckdb_engine.Dialect()` avulso
usa `pyformat`), então o SQL compilado pelo engine traz `$1`, `$2` e a conexão DuckDB aceita a lista
de parâmetros na ordem de `positiontup`. `compile_kwargs={"literal_binds": True}`
gera o SQL com os valores embutidos, útil para `con.sql(...)` e para registrar a consulta numa view.

### Ingestão de um DataFrame numa tabela definida pelo ORM

```python
import pyarrow as pa
from sqlalchemy import insert
from sqlalchemy.orm import Session

def arrow_schema(model) -> pa.Schema:
    """Esquema Arrow derivado das colunas do modelo; implementação em plan/sqlalchemy.md."""
    ...

def ingest(engine, model, df: pd.DataFrame) -> None:
    table = pa.Table.from_pandas(df, schema=arrow_schema(model), preserve_index=False)
    with engine.begin() as conn:
        con = conn.connection.dbapi_connection
        con.register("entrada", table)
        conn.execute(sa.text(f"INSERT INTO {model.__tablename__} BY NAME SELECT * FROM entrada"))
        con.unregister("entrada")

# Volumes pequenos, sem passar por Arrow: bulk insert do ORM (0,98 s para 50.000 linhas; 0,77 s com render_nulls=True).
with Session(engine) as session:
    session.execute(insert(Operacao), df.to_dict("records"))
    session.commit()
```

`pandas.DataFrame.to_sql(nome, engine, if_exists="append", index=False)` também funciona (0,74 s
para 50.000 linhas), com `method=None`; `method="multi"` foi mais lento. O bulk insert do ORM aceita
dicionários com chaves diferentes, separa em lotes as linhas com `None` (por isso `render_nulls=True`
foi mais rápido na amostra, que tem nulos em `descricao`) e `insert(Operacao).returning(Operacao)`
devolve os objetos.

Migrações com Alembic exigem registrar a implementação do dialeto:

```python
from alembic.ddl.impl import DefaultImpl

class AlembicDuckDBImpl(DefaultImpl):
    __dialect__ = "duckdb"
```

## Referências

- Documentação do DuckDB, versão 1.5: <https://duckdb.org/docs/current/>. Páginas usadas: formato de
  armazenamento, vetores, tipos de dados (visão geral, numéricos, texto, JSON), instruções `CREATE
  TABLE`, `ALTER TABLE`, `DROP`, `COMMENT ON`, `CREATE INDEX`, `CREATE SEQUENCE`, `INSERT`,
  `UPDATE`, `DELETE`, `MERGE INTO`, transações, concorrência, restrições, índices, compatibilidade
  com PostgreSQL, guias de performance (esquema, indexação, joins, ajuste de cargas, ambiente,
  memória), guias do cliente Python (ingestão, conversão, pandas, Arrow, polars, DB-API, tipos) e
  `COPY`.
- Documentação do SQLAlchemy 2.0: <https://docs.sqlalchemy.org/en/20/>.
- Repositório do dialeto `duckdb_engine`: <https://github.com/Mause/duckdb_engine>.
- Documentação do pandas sobre `read_sql`, `to_sql` e o backend pyarrow:
  <https://pandas.pydata.org/docs/>.
- Documentação do PyArrow: <https://arrow.apache.org/docs/python/>.
- Tutorial de referência do projeto: <https://github.com/felipenoris/etl-cookbook-tutorial>.
- Lista completa das páginas consultadas: [REFERENCES.md](../REFERENCES.md).
