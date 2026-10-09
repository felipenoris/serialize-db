Esta página descreve o funcionamento das tecnologias sobre as quais o pacote opera e o que a
biblioteca faz com cada uma: os arquivos Parquet, o formato de cada arquivo de dados; o Delta Lake,
a fonte da verdade do banco; o DuckDB e o Redshift, os dois motores de execução, com o Redshift
também como destino de publicação; e o SQLAlchemy, o contrato de esquema e o gerador do SQL dos
dois motores. Cada seção abre com o ambiente e a data das suas medições, e cada leitura feita
depois no ambiente alvo traz a própria data. As páginas consultadas estão em
[REFERENCES.md][references].

## Parquet

Esta seção explica a estrutura de um arquivo Parquet, os metadados que ele carrega, como
inspecioná-los com DuckDB e PyArrow, como o particionamento funciona e como usar tudo isso para
acelerar consultas e para importar e exportar dados no DuckDB e no Redshift com um esquema definido.

As fontes são a especificação do formato (repositório `apache/parquet-format`, arquivo
`parquet.thrift`), a documentação do DuckDB, do PyArrow e do Redshift, e os artigos listados em
[REFERENCES.md][references]. Os exemplos e as medições foram executados em 2026-09-18 com DuckDB
1.5.5 e PyArrow 25.0.1 sobre um arquivo de exemplo com 300.000 linhas, 3 row groups de 100.000 linhas
e 5 colunas (`id_operacao`, `data_ref`, `id_cliente`, `valor`, `descricao`), ordenado por
`data_ref, id_operacao`, no modelo `Operacao` da seção [SQLAlchemy](#sqlalchemy).

### Estrutura do arquivo

Um arquivo Parquet guarda uma única tabela em layout colunar. O arquivo é dividido em row groups
(fatias horizontais de linhas); cada row group contém um column chunk por coluna; cada column chunk
é uma sequência de páginas (a unidade de codificação e compressão). Os metadados ficam num rodapé no
fim do arquivo, o que permite escrever em uma só passada.

```text
4 bytes: número mágico "PAR1"
<coluna 1, chunk 1> <coluna 2, chunk 1> ... <coluna N, chunk 1>     row group 1
<coluna 1, chunk 2> <coluna 2, chunk 2> ... <coluna N, chunk 2>     row group 2
...
[filtros Bloom]  [page index (ColumnIndex e OffsetIndex)]           estruturas opcionais
FileMetaData (Thrift, TCompactProtocol)                             rodapé
4 bytes: tamanho do FileMetaData (little endian)
4 bytes: número mágico "PAR1"
```

| Termo | Significado |
| --- | --- |
| Row group | Partição horizontal das linhas. Sem estrutura física própria: é um conjunto de column chunks. |
| Column chunk | Os dados de uma coluna dentro de um row group, contíguos no arquivo. |
| Página | Fatia de um column chunk. Indivisível para codificação e compressão. Tipos: data page (v1 ou v2), dictionary page, index page (reservada). |
| Rodapé (footer) | A estrutura `FileMetaData`, com o esquema, os row groups, os offsets de cada column chunk e as estatísticas. |
| Page index | `ColumnIndex` e `OffsetIndex` por column chunk, gravados perto do rodapé, com estatísticas por página. |
| Filtro Bloom | Estrutura por column chunk que responde "não está" ou "talvez esteja" para um valor. |

A especificação define as unidades de paralelismo: arquivo ou row group para distribuição de
trabalho, column chunk para I/O e página para codificação e compressão. Um leitor começa pelo rodapé
(lê os últimos 8 bytes, depois o `FileMetaData`), decide quais column chunks precisa e lê só esses
intervalos de bytes. Num object store como o S3 isso vira duas requisições de metadados e uma
requisição por intervalo de dados.

Os 8 bytes finais bastam para localizar o rodapé:

```python
# O leitor começa pelos 8 bytes finais: tamanho do rodapé e número mágico; depois lê o FileMetaData inteiro.
import struct
import pyarrow.parquet as pq

with open("operacoes.parquet", "rb") as f:
    f.seek(-8, 2)
    footer_size, magic = struct.unpack("<i4s", f.read(8))
    f.seek(-8 - footer_size, 2)
    footer = f.read(footer_size)  # FileMetaData em Thrift compacto, ainda sem decodificar
print(magic, footer_size == pq.read_metadata("operacoes.parquet").serialized_size)
# b'PAR1' True
```

Recuperação de erros, segundo a especificação: rodapé corrompido perde o arquivo; `ColumnMetaData`
corrompido perde o column chunk; cabeçalho de página corrompido perde as páginas seguintes do chunk.
Row groups menores tornam o arquivo mais resiliente.

### Metadados do arquivo Parquet

Todas as estruturas de metadados são definidas em Thrift no arquivo `parquet.thrift` e serializadas
com `TCompactProtocol`. Há dois grupos: os metadados do rodapé (`FileMetaData` e tudo que ele contém)
e os cabeçalhos de página, gravados junto dos dados. O page index, os filtros Bloom e os metadados de
criptografia ficam fora do rodapé, e o rodapé guarda apenas os offsets para eles.

#### FileMetaData

| Campo | Tipo | Conteúdo |
| --- | --- | --- |
| `version` | i32 obrigatório | Versão do arquivo. A especificação pede que gravadores escrevam `1` e que leitores aceitem `1` e `2` sem distinção; não há consenso sobre o que constitui a versão 2. |
| `schema` | lista de `SchemaElement` | O esquema como árvore achatada em profundidade; o primeiro elemento é a raiz. |
| `num_rows` | i64 | Linhas no arquivo. |
| `row_groups` | lista de `RowGroup` | Um por row group, na ordem do arquivo. |
| `key_value_metadata` | lista de `KeyValue` | Metadados livres do arquivo. |
| `created_by` | string | Aplicação gravadora, no formato `<aplicação> version <versão> (build <hash>)`. |
| `column_orders` | lista de `ColumnOrder` | Ordem usada em `min_value` e `max_value` de cada coluna folha. Sem este campo, esses valores não têm significado definido. |
| `encryption_algorithm` | `EncryptionAlgorithm` | Só em arquivos criptografados com rodapé em texto claro. |
| `footer_signing_key_metadata` | binary | Chave de assinatura do rodapé, no mesmo caso. |

Valores observados no arquivo de exemplo: `created_by` = `parquet-cpp-arrow version 25.0.1` quando
gravado pelo PyArrow e `DuckDB version v1.5.5 (build d8cdaa33fd)` quando gravado pelo DuckDB. O
PyArrow grava `version` 2 (exibido como `format_version 2.6`) e o DuckDB grava 1. O rodapé mediu
3.050 bytes no arquivo do PyArrow e 1.646 bytes no do DuckDB, para 3 row groups e 5 colunas. O rodapé
cresce com row groups × colunas, e um leitor o decodifica inteiro antes de ler qualquer dado, o que
pesa em arquivos com milhares de row groups ou colunas.

Nos exemplos em Python desta seção, `tabela` é a tabela Arrow do mês com as cinco colunas do
contrato, `esquema` é o esquema dela (`arrow_schema` de [SQLAlchemy](#sqlalchemy), sem a coluna
de partição `mes`, que fica no diretório), `tabela_mes` acrescenta `mes` derivada de `data_ref`
(`pc.strftime(tabela["data_ref"], "%Y-%m")`) e `con` é a conexão DuckDB do sandbox com a tabela
`operacoes`. Eles rodaram em 2026-09-19 com PyArrow 25.0.1, DuckDB 1.5.5 e deltalake 1.6.4, sobre
uma amostra com a mesma forma do arquivo de exemplo.

```python
# Grava o mês com o esquema do contrato e lê os campos do FileMetaData que o PyArrow expõe.
import pyarrow.parquet as pq

pq.write_table(table, "operacoes.parquet", version="2.6", compression="zstd", row_group_size=100_000)
md = pq.read_metadata("operacoes.parquet")
print(md.created_by, md.format_version, md.num_rows, md.num_row_groups)
# parquet-cpp-arrow version 25.0.1 2.6 300000 3
pq.write_table(table, "operacoes_v1.parquet", version="1.0")
print(pq.read_metadata("operacoes_v1.parquet").format_version)
# 1.0
```

#### Schema

O esquema é uma lista de `SchemaElement` em ordem de profundidade. Nós internos (grupos) têm
`num_children` e não têm `type`; folhas têm `type` e não têm `num_children`. Os column chunks se
ligam às folhas por `path_in_schema`.

| Campo | Conteúdo |
| --- | --- |
| `type` | Tipo físico: `BOOLEAN`, `INT32`, `INT64`, `INT96` (obsoleto), `FLOAT`, `DOUBLE`, `BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY`. |
| `type_length` | Comprimento em bytes de `FIXED_LEN_BYTE_ARRAY`. |
| `repetition_type` | `REQUIRED` (nunca nulo), `OPTIONAL` (0 ou 1 valor) ou `REPEATED` (0 ou mais valores). A raiz não tem. |
| `name` | Nome do campo. |
| `num_children` | Filhos de um grupo. |
| `converted_type`, `scale`, `precision` | Anotações antigas, mantidas para leitores anteriores ao `logicalType`. |
| `field_id` | Identificador estável do campo, independente do nome e da posição. O Iceberg, o Delta com column mapping e o parâmetro `schema` do DuckDB dependem dele; o PyArrow o grava a partir do metadado de campo `PARQUET:field_id`. |
| `logicalType` | Anotação que diz como interpretar o tipo físico: `STRING`, `MAP`, `LIST`, `ENUM`, `DECIMAL(scale, precision)`, `DATE`, `TIME(unit, isAdjustedToUTC)`, `TIMESTAMP(unit, isAdjustedToUTC)`, `INTEGER(bitWidth, isSigned)`, `UNKNOWN` (sempre nulo), `JSON`, `BSON`, `UUID`, `FLOAT16`, `VARIANT`, `GEOMETRY`, `GEOGRAPHY`, `FILE`. |

O conjunto de tipos físicos é mínimo de propósito: não existe `INT16` porque um `INT32` com uma boa
codificação cobre o caso, e o tipo lógico `INTEGER(16, true)` registra a intenção. Dados aninhados
usam a codificação do artigo Dremel: cada valor carrega um nível de definição (quantos campos
opcionais do caminho estão definidos) e um nível de repetição (em qual campo repetido a lista
recomeça). Nulos vivem só nos níveis de definição; um column chunk com 1.000 nulos grava a sequência
`(0, 1000 vezes)` em RLE e nenhum valor.

O mesmo tipo do modelo pode virar tipos físicos diferentes conforme o gravador. No arquivo de
exemplo, `valor DECIMAL(18, 2)` saiu como `FIXED_LEN_BYTE_ARRAY(8)` pelo PyArrow e como `INT64` pelo
DuckDB; a anotação `Decimal(precision=18, scale=2)` é a mesma. Os dois são válidos (a especificação
permite `DECIMAL` sobre `INT32`, `INT64`, `FIXED_LEN_BYTE_ARRAY` e `BYTE_ARRAY`), mas nem todo leitor
aceita os dois; a página de status de implementação marca o DuckDB como só leitura para `DECIMAL`
sobre `BYTE_ARRAY`. O PyArrow tem a opção `store_decimal_as_integer` para gravar como inteiro.

A obrigatoriedade também difere: o PyArrow grava `required` para campos `not null` do esquema Arrow,
e o DuckDB grava `optional` em toda coluna, inclusive as declaradas `NOT NULL` na tabela (verificado
na versão 1.5.5). Um contrato de esquema que dependa de `REQUIRED` no Parquet não pode ser produzido
pelo DuckDB.

Os três pontos se decidem no esquema Arrow e nas opções de gravação:

```python
# Obrigatoriedade, field_id e tipo físico do DECIMAL vêm do esquema Arrow e das opções de gravação.
import pyarrow as pa
import pyarrow.parquet as pq

schema = pa.schema([
    pa.field(c.name, arrow_type(c.type), nullable=c.nullable, metadata={"PARQUET:field_id": str(i)})
    for i, c in enumerate(Operacao.__table__.columns, start=1)
    if c.name != "mes"
])
pq.write_table(table.cast(schema), "operacoes.parquet", store_decimal_as_integer=True)
print(pq.ParquetFile("operacoes.parquet").schema)
# required int64 field_id=1 id_operacao;
# ...
# required int64 field_id=4 valor (Decimal(precision=18, scale=2));
# optional binary field_id=5 descricao (String);
print(pq.read_schema("operacoes.parquet").field("valor"))
# pyarrow.Field<valor: decimal128(18, 2) not null>
```

`nullable=False` vira `required`. O metadado de campo `PARQUET:field_id` vira `field_id`; um metadado
com outra chave (`field_id`, por exemplo) é ignorado, e o arquivo sai com `field_id=-1`.
`store_decimal_as_integer=True` grava o `DECIMAL(18, 2)` como `INT64`, como o DuckDB, e o esquema
Arrow lido de volta é `decimal128(18, 2)` nos dois tipos físicos.

Esquema do arquivo de exemplo gravado pelo PyArrow, na notação do `parquet-cpp`:

```text
required group field_id=-1 schema {
  required int64 field_id=1 id_operacao;
  required int32 field_id=2 data_ref (Date);
  required int64 field_id=3 id_cliente;
  required fixed_len_byte_array(8) field_id=4 valor (Decimal(precision=18, scale=2));
  optional binary field_id=5 descricao (String);
}
```

O tipo lógico `JSON` anota um `BYTE_ARRAY` com texto JSON. O PyArrow o grava para a extensão
`arrow.json` (`pa.json_(pa.string())`), o DuckDB o grava para colunas `JSON` e lê as duas formas como
`JSON`; um leitor que não conhece a anotação vê texto UTF-8. O delta-rs grava a mesma coluna como
`String`, e a tabela Delta aceita arquivos das duas formas ([tabela de mapeamento de
tipos][tipos]).

```python
import pyarrow as pa
import pyarrow.parquet as pq

# A extensão arrow.json vira o tipo lógico JSON no rodapé; o DuckDB lê a coluna como JSON.
docs = pa.array(['{"origem": "A"}', None], pa.string()).cast(pa.json_(pa.string()))
pq.write_table(pa.table({"meta": docs}), "eventos.parquet")
pq.ParquetFile("eventos.parquet").schema.column(0).logical_type            # JSON
con.sql("DESCRIBE SELECT * FROM 'eventos.parquet'").fetchall()[0][1]      # 'JSON'
```

#### Row group

| Campo | Conteúdo |
| --- | --- |
| `columns` | Lista de `ColumnChunk`, na mesma ordem das folhas do esquema. |
| `total_byte_size` | Bytes das colunas sem compressão. |
| `num_rows` | Linhas no row group. |
| `sorting_columns` | Ordenação das linhas dentro do row group: índice da coluna, `descending`, `nulls_first`. Opcional e informativa. |
| `file_offset` | Offset da primeira página do row group. |
| `total_compressed_size` | Bytes das colunas comprimidas. |
| `ordinal` | Posição do row group no arquivo. |

O row group é a unidade de poda por estatísticas e de paralelismo na leitura. Tamanhos padrão:

| Gravador | Padrão | Ajuste |
| --- | --- | --- |
| DuckDB | 122.880 linhas | `ROW_GROUP_SIZE` (mínimo 2.048) ou `ROW_GROUP_SIZE_BYTES` (só com `preserve_insertion_order = false`). |
| PyArrow | mínimo entre as linhas da tabela e 1.048.576; teto de 67.108.864 | `row_group_size` em `write_table`; um `write_table` por chamada em `ParquetWriter`. |
| Redshift `UNLOAD` | 32 MB | `ROWGROUPSIZE` de 32 MB a 128 MB, só em alguns tipos de nó. |
| Especificação | recomenda 512 MB a 1 GB | Escrita para blocos HDFS; não se aplica a leitura por intervalo de bytes no S3. |

`sorting_columns` é gravado pelo PyArrow quando se passa `sorting_columns` (o gravador não ordena
nem confere) e não é gravado pelo DuckDB (o arquivo do exemplo saiu com a lista vazia). A página de
status de implementação marca o DuckDB como leitor desse campo.

Em streaming, cada lote vira um row group:

```python
# Um row group por lote: o ParquetWriter fecha um row group a cada write_batch, sem materializar o mês.
import pyarrow.parquet as pq

reader = con.execute("""
    SELECT id_operacao, data_ref, id_cliente, valor, descricao
    FROM operacoes WHERE mes = '2026-08' ORDER BY data_ref, id_operacao
""").to_arrow_reader(100_000)
path = "operacoes/mes=2026-08/exec_abc123.parquet"
with pq.ParquetWriter(path, schema, compression="zstd") as writer:
    for batch in reader:
        writer.write_batch(batch.cast(schema))  # o DuckDB entrega todo campo anulável; o cast aplica o contrato
md = pq.read_metadata(path)
print([md.row_group(i).num_rows for i in range(md.num_row_groups)])
```

Sem o cast, o `ParquetWriter` recusa o lote com `Table schema does not match schema used to create
file`, porque o DuckDB entrega todo campo como anulável. `fetch_record_batch` está obsoleto no DuckDB
1.5.5; `to_arrow_reader` o substitui.

#### ColumnMetaData

Cada `ColumnChunk` da lista `columns` de um row group tem estes campos:

| Campo | Conteúdo |
| --- | --- |
| `file_path` | Arquivo onde o chunk está. Vazio no caso normal; usado só por arquivos de sumário `_metadata`. |
| `file_offset` | Obsoleto; gravadores devem escrever 0. |
| `meta_data` | O `ColumnMetaData` abaixo. Marcado opcional no Thrift, mas exigido por todas as implementações. |
| `offset_index_offset`, `offset_index_length` | Localização do `OffsetIndex` do chunk. |
| `column_index_offset`, `column_index_length` | Localização do `ColumnIndex` do chunk. |
| `crypto_metadata`, `encrypted_column_metadata` | Metadados de criptografia da coluna. |

E o `ColumnMetaData`:

| Campo | Conteúdo |
| --- | --- |
| `type` | Tipo físico. |
| `encodings` | Conjunto das codificações usadas no chunk (níveis e valores). |
| `path_in_schema` | Caminho até a folha do esquema. |
| `codec` | `UNCOMPRESSED`, `SNAPPY`, `GZIP`, `LZO`, `BROTLI`, `LZ4` (obsoleto), `ZSTD`, `LZ4_RAW`. |
| `num_values` | Valores no chunk, incluindo nulos. |
| `total_uncompressed_size`, `total_compressed_size` | Bytes das páginas com cabeçalhos, antes e depois da compressão. |
| `key_value_metadata` | Metadados livres da coluna. |
| `data_page_offset`, `index_page_offset`, `dictionary_page_offset` | Offsets da primeira data page, da index page (não usada) e da dictionary page. |
| `statistics` | Estatísticas do chunk, descritas abaixo. |
| `encoding_stats` | Contagem de páginas por tipo de página e codificação. Diz, por exemplo, se todas as data pages usam dicionário. |
| `bloom_filter_offset`, `bloom_filter_length` | Localização do filtro Bloom. O comprimento entrou na versão 2.10 do formato; gravadores antigos só escrevem o offset. |
| `size_statistics` | Estatísticas de tamanho, descritas em [Outros metadados](#outros-metadados). |
| `geospatial_statistics` | Caixa envolvente e tipos de geometria, para `GEOMETRY` e `GEOGRAPHY`. |

Estatísticas (`Statistics`), presentes no column chunk e, opcionalmente, em cada data page:

| Campo | Conteúdo |
| --- | --- |
| `min`, `max` | Obsoletos: comparação com sinal, sem respeitar o tipo lógico. |
| `min_value`, `max_value` | Limites inferior e superior segundo o `ColumnOrder` da coluna. Podem ser valores mais curtos que não existem na coluna (um gravador pode escrever `"B"` e `"C"` no lugar de `"Blart Versenwald III"`). |
| `is_min_value_exact`, `is_max_value_exact` | Se os limites são valores reais da coluna. |
| `null_count` | Nulos no chunk. Gravadores devem escrever mesmo quando é zero; um leitor não pode supor zero quando o campo falta. |
| `distinct_count` | Valores distintos. Raramente gravado: nem o PyArrow nem o DuckDB gravaram no exemplo. |
| `nan_count` | NaN em `FLOAT`, `DOUBLE` e `FLOAT16`. Sem o campo, o leitor deve supor que há NaN. |

`ColumnOrder` define a ordem dos limites: `TYPE_ORDER` (a ordem do tipo lógico ou físico: inteiros
com sinal, strings por bytes sem sinal, `DECIMAL` pelo valor), `IEEE_754_TOTAL_ORDER` para ponto
flutuante (recomendada, porque `TYPE_ORDER` é ambígua para NaN e para -0 e +0) e
`INT96_TIMESTAMP_ORDER`. Um leitor que não reconhece a ordem deve ignorar as estatísticas da coluna.
As estatísticas do chunk são a base da poda de row groups: se `max_value < literal` ou
`min_value > literal`, o row group inteiro é pulado.

As mesmas estatísticas alimentam a ação `add` do Delta quando um arquivo gravado fora do delta-rs
entra no log ([Ingestão de dados](#ingestao-de-dados) do Delta Lake):

```python
# Estatísticas da ação add do Delta a partir do rodapé: mínimo, máximo e nulos agregados por coluna.
import decimal
from datetime import date
import pyarrow.parquet as pq

def json_value(v):
    """O delta-rs grava DECIMAL como número JSON e DATE como texto ISO nas estatísticas."""
    if isinstance(v, decimal.Decimal):
        return float(v)
    return v.isoformat() if isinstance(v, date) else v

def delta_stats(path: str) -> dict:
    md = pq.read_metadata(path)
    min_values, max_values, null_counts = {}, {}, {}
    for i in range(md.num_row_groups):
        rg = md.row_group(i)
        for j in range(rg.num_columns):
            column = rg.column(j)
            s, name = column.statistics, column.path_in_schema
            null_counts[name] = null_counts.get(name, 0) + s.null_count
            if s.has_min_max:
                min_values[name] = min(min_values.get(name, s.min), s.min)
                max_values[name] = max(max_values.get(name, s.max), s.max)
    return {"numRecords": md.num_rows,
            "minValues": {k: json_value(v) for k, v in min_values.items()},
            "maxValues": {k: json_value(v) for k, v in max_values.items()},
            "nullCount": null_counts}
```

O dicionário é o campo `stats` de `serialize_db.delta.RegisteredFile`, a entrada de
`serialize_db.delta.register_files`. Com um
arquivo de agosto registrado assim, `dt.file_uris(file_pruning_predicate="data_ref >= '2026-09-15'")`
devolveu lista vazia, e `mes = '2026-08'` devolveu só esse arquivo.

#### Page index

O page index resolve um problema da versão original do formato: as estatísticas por página ficavam no
cabeçalho de cada página, então um leitor precisava percorrer todas as páginas para decidir quais
pular, o que já lia a maior parte da coluna. O page index foi adicionado na versão 2.5.0 do formato
(`PARQUET-1201`) e tem duas estruturas por column chunk, gravadas perto do rodapé, fora dos row
groups, com offset e comprimento em `ColumnChunk`:

| Estrutura | Campos | Uso |
| --- | --- | --- |
| `OffsetIndex` | `page_locations` (offset, `compressed_page_size` com cabeçalho, `first_row_index`), `unencoded_byte_array_data_bytes` | Navegar por índice de linha: achar a página que contém uma linha, e pular nas outras colunas as linhas eliminadas em uma. |
| `ColumnIndex` | `null_pages`, `min_values`, `max_values`, `boundary_order` (`UNORDERED`, `ASCENDING`, `DESCENDING`), `null_counts`, histogramas de níveis, `nan_counts` | Achar as páginas cujos limites cruzam o predicado. Com `boundary_order` ordenado o leitor faz busca binária. |

Quando há `OffsetIndex`, toda página começa em fronteira de linha (`repetition_level = 0`). Um leitor
que faz varredura completa não paga nada pelo índice, porque não precisa lê-lo. Os objetivos da
especificação: uma busca pontual pela coluna de ordenação lê uma página por coluna; uma varredura de
intervalo lê só as páginas relevantes; um predicado seletivo em coluna não ordenada restringe as
páginas lidas nas demais colunas.

Suporte (página de status de implementação do Parquet e verificação local):

| Implementação | Grava | Usa na leitura |
| --- | --- | --- |
| PyArrow (Arrow C++) | Sim, com `write_page_index=True` (padrão `False`). | Não. A documentação diz que o leitor ainda não usa o page index. |
| DuckDB 1.5.5 | Não (o arquivo gravado saiu sem `ColumnIndex` nem `OffsetIndex`). | Lê a estrutura, mas não poda páginas. |
| parquet-java (Spark), arrow-rs (DataFusion), arrow-go | Sim. | Sim, com poda de páginas. |
| Redshift | Não documentado. | Não documentado. |

```python
# O page index entra por opção do gravador; o rodapé diz se cada column chunk tem as duas estruturas.
import pyarrow.parquet as pq

pq.write_table(table, "operacoes.parquet", row_group_size=100_000, write_page_index=True)
column = pq.read_metadata("operacoes.parquet").row_group(0).column(1)
print(column.path_in_schema, column.has_column_index, column.has_offset_index)
# data_ref True True
```

O `pyarrow.parquet` 25.0.1 não tem função que devolva o conteúdo do `ColumnIndex` ou do
`OffsetIndex`; em Python, o rodapé só informa que eles existem.

#### Bloom filters

Um filtro Bloom responde a uma consulta de pertinência com "certamente não" ou "provavelmente sim";
não há falsos negativos. Entrou na versão 2.7.0 do formato (`PARQUET-41`) para cobrir o caso que
estatísticas e dicionários não cobrem: colunas de alta cardinalidade, com `min` e `max` distantes,
onde o gravador não usa dicionário por falta de espaço.

O único algoritmo definido é o split block Bloom filter: o filtro é um vetor de blocos de 256 bits
(8 palavras de 32 bits); o hash xxHash de 64 bits do valor em codificação `PLAIN` escolhe um bloco
pelos 32 bits altos e, pelos 32 bits baixos multiplicados por 8 constantes ímpares (`salt`), marca um
bit em cada palavra do bloco. Testar um valor lê um único bloco, que cabe numa linha de cache.

No arquivo, cada filtro é um `BloomFilterHeader` (`numBytes`, algoritmo `BLOCK`, hash `XXHASH`,
compressão `UNCOMPRESSED`) seguido do bitset. Os filtros de um row group ficam juntos, na ordem do
esquema, ou depois de todos os row groups (antes do page index) ou entre row groups. Como o
cabeçalho precisa ser decodificado para achar o bitset e `bloom_filter_length` é opcional, ler todos
os filtros de um arquivo pode custar várias requisições; a especificação pede que gravadores escrevam
o comprimento para permitir uma leitura só.

O tamanho é escolhido pelo gravador a partir do número esperado de valores distintos (`ndv`) e da
probabilidade de falso positivo (`fpp`):

| Bits por valor inserido | Falsos positivos |
| --- | --- |
| 6,0 | 10 % |
| 10,5 | 1 % |
| 16,9 | 0,1 % |
| 26,4 | 0,01 % |
| 41 | 0,001 % |

Um filtro dimensionado para menos valores do que recebe satura e deixa de excluir; um filtro
dimensionado para mais valores desperdiça espaço. O artigo da Logfire descreve o "folding": alocar um
filtro grande, inserir os valores, medir a taxa de bits ligados e dobrar o filtro (OR de blocos
adjacentes) até o limite de `fpp`, o que dispensa estimar `ndv`. O artigo da InfluxData mediu, com
`fpp` 0,01 e `ndv` 1.000, 2 KB por row group e consultas pontuais 30 vezes mais rápidas; com `ndv`
igual à cardinalidade real (1 milhão) o filtro subiu para 2 MB por row group sem podar mais.

Filtros Bloom só servem a predicados de igualdade e `IN`. Não ajudam em intervalos, nem em colunas
de baixa cardinalidade (o dicionário já resolve), nem em colunas ordenadas (as estatísticas já
resolvem).

Suporte:

| Implementação | Grava | Usa na leitura |
| --- | --- | --- |
| DuckDB | Sim, desde a 1.2.0, para inteiros, `FLOAT`, `DOUBLE`, `VARCHAR` e `BLOB`, quando a coluna do row group usa dicionário. `WRITE_BLOOM_FILTER` (padrão `true`), `BLOOM_FILTER_FALSE_POSITIVE_RATIO` (padrão 0,01) e `DICTIONARY_SIZE_LIMIT` (padrão `ROW_GROUP_SIZE / 5`) controlam. | Sim, automaticamente, para comparações com constante. `parquet_bloom_probe` mostra o efeito. |
| PyArrow | Sim, por coluna, com `bloom_filter_options`; padrões `ndv` 1.048.576 e `fpp` 0,05. | Não. |
| Redshift | Não documentado. | Não documentado. |

No exemplo, o DuckDB gravou filtros para `data_ref` (144 bytes por row group) e `id_cliente`
(8.209 bytes), as duas colunas em que usou dicionário, e nenhum para `id_operacao`, `valor` e
`descricao`, gravadas em `PLAIN`. O PyArrow, com `{'id_cliente': {'ndv': 5000, 'fpp': 0.01}}`,
gravou 8.209 bytes por row group.

```python
# O filtro é pedido por coluna na gravação; o rodapé guarda offset e comprimento, e o DuckDB o consulta.
import duckdb
import pyarrow.parquet as pq

pq.write_table(table, "operacoes.parquet", row_group_size=100_000,
               bloom_filter_options={"id_cliente": {"ndv": 5_000, "fpp": 0.01}})
md = pq.read_metadata("operacoes.parquet")
for i in range(md.num_row_groups):
    column = md.row_group(i).column(2)
    print(i, column.path_in_schema, column.bloom_filter_offset, column.bloom_filter_length)
# 0 id_cliente 6813051 8209
print(duckdb.sql("""
    SELECT row_group_id, bloom_filter_excludes
    FROM parquet_bloom_probe('operacoes.parquet', 'id_cliente', 99999)
""").fetchall())
# [(0, True), (1, True), (2, True)]
```

#### Page headers

Toda página começa com um `PageHeader`, e o resto do conteúdo depende do tipo:

| Campo | Conteúdo |
| --- | --- |
| `type` | `DATA_PAGE`, `INDEX_PAGE` (reservado, sem definição), `DICTIONARY_PAGE`, `DATA_PAGE_V2`. |
| `uncompressed_page_size`, `compressed_page_size` | Tamanhos da página sem o cabeçalho. |
| `crc` | CRC32 (polinômio do GZIP) do conteúdo serializado da página, calculado depois de compressão e criptografia, sem o cabeçalho. |
| `data_page_header` | `num_values` (com nulos), `encoding`, `definition_level_encoding`, `repetition_level_encoding`, `statistics`. |
| `dictionary_page_header` | `num_values`, `encoding`, `is_sorted`. |
| `data_page_header_v2` | `num_values`, `num_nulls`, `num_rows`, `encoding`, `definition_levels_byte_length`, `repetition_levels_byte_length`, `is_compressed`, `statistics`. |

Numa data page v1 o corpo é, nesta ordem e sem preenchimento: níveis de repetição, níveis de
definição e valores codificados, tudo comprimido junto. Colunas não aninhadas não gravam níveis de
repetição, e colunas `REQUIRED` não gravam níveis de definição; uma coluna plana e obrigatória grava
só os valores. Na v2 os níveis ficam fora da compressão (sempre em RLE) e cada página começa em
fronteira de linha. A dictionary page, quando existe, é a primeira página do column chunk, e há no
máximo uma; as data pages seguintes gravam índices do dicionário em `RLE_DICTIONARY`. Um chunk pode
trocar de dicionário para `PLAIN` no meio, quando o dicionário estoura o limite de tamanho.

As estatísticas no cabeçalho de página são o mecanismo antigo. Com page index, a especificação diz que
leitores não devem usá-las, e o PyArrow deixa de gravá-las quando `write_page_index=True`.

Tamanhos de página: a especificação recomendou 8 KB na época do HDFS. O PyArrow usa `data_page_size`
de 1 MB e `max_rows_per_page` de 20.000; `dictionary_pagesize_limit` é 1 MB. O DuckDB expõe
`STRING_DICTIONARY_PAGE_SIZE_LIMIT`, padrão 1 MB.

Codificações (enum `Encoding`): `PLAIN`, `RLE` (híbrido RLE e bit packing, usado nos níveis e em
booleanos), `RLE_DICTIONARY`, `DELTA_BINARY_PACKED` (inteiros; melhor em dados ordenados),
`DELTA_LENGTH_BYTE_ARRAY`, `DELTA_BYTE_ARRAY` (prefixos), `BYTE_STREAM_SPLIT` (separa os bytes de cada
valor em fluxos para comprimir melhor ponto flutuante e, desde a 2.11, inteiros e
`FIXED_LEN_BYTE_ARRAY`) e `ALP` (ponto flutuante, nova, ainda pouco suportada). `PLAIN_DICTIONARY` e
`BIT_PACKED` são obsoletas. Com `PARQUET_VERSION 'V2'` o DuckDB gravou `DELTA_BINARY_PACKED`,
`RLE_DICTIONARY` e `DELTA_LENGTH_BYTE_ARRAY` e o arquivo do exemplo caiu de 3.288.592 para
2.926.309 bytes (11 % menor, com zstd nos dois).

```python
# Dicionário e codificação são escolhidos por coluna na gravação e aparecem em `encodings` do column chunk.
import pyarrow.parquet as pq

pq.write_table(table, "operacoes.parquet", data_page_version="2.0", data_page_size=256 * 1024,
               max_rows_per_page=10_000, use_dictionary=["data_ref", "id_cliente"],
               column_encoding={"id_operacao": "DELTA_BINARY_PACKED", "descricao": "DELTA_LENGTH_BYTE_ARRAY"})
rg = pq.read_metadata("operacoes.parquet").row_group(0)
for j in range(rg.num_columns):
    column = rg.column(j)
    print(column.path_in_schema, column.encodings, column.has_dictionary_page)
# id_operacao ('RLE', 'DELTA_BINARY_PACKED') False
# data_ref ('PLAIN', 'RLE', 'RLE_DICTIONARY') True
```

`column_encoding` exige a coluna fora de `use_dictionary` (`To use 'column_encoding' set
'use_dictionary' to False`). O DuckDB 1.5.5 leu esse arquivo inteiro. Com `valor` em
`BYTE_STREAM_SPLIT`, que o PyArrow grava e lê sobre `FIXED_LEN_BYTE_ARRAY`, o DuckDB recusou a leitura
com `BYTE_STREAM_SPLIT encoding is only supported for FLOAT or DOUBLE data`.

#### Outros metadados

Metadados chave-valor (`KeyValue`, `key` obrigatória e `value` opcional). Existem no arquivo e em
cada column chunk. Convenções em uso:

| Chave | Quem grava | Conteúdo |
| --- | --- | --- |
| `ARROW:schema` | PyArrow (`store_schema=True`, padrão) | O esquema Arrow serializado, para recriar fusos horários, tipos de dicionário e extensões que o Parquet não representa. |
| `pandas` | pandas via PyArrow | Índice e dtypes do DataFrame. |
| `geo` | GeoParquet (DuckDB com `GEOPARQUET_VERSION`) | Colunas geométricas, CRS e caixas envolventes. |
| `iceberg.schema` | Gravadores Iceberg | O esquema Iceberg em JSON. |
| Chaves próprias | DuckDB `KV_METADATA {chave: valor}`; PyArrow `schema.with_metadata` | Identificador da execução, versão da biblioteca. |

Estatísticas de tamanho (`SizeStatistics`, versão 2.10.0): `unencoded_byte_array_data_bytes` (bytes
dos valores `BYTE_ARRAY` sem codificação e sem os prefixos de comprimento), `repetition_level_histogram`
e `definition_level_histogram`. Servem para estimar a memória necessária ao materializar a coluna e
para poda por nulidade e por comprimento de lista em dados aninhados. Aparecem no column chunk e, por
página, no `OffsetIndex` e no `ColumnIndex`.

Estatísticas geoespaciais (`GeospatialStatistics`): `bbox` com `xmin`, `xmax`, `ymin`, `ymax` e,
opcionais, `z` e `m`, mais `geospatial_types`. Só para `GEOMETRY` e `GEOGRAPHY`.

Estatísticas de codificação (`PageEncodingStats`): para cada par (tipo de página, codificação), o
número de páginas. Um leitor descobre por aqui se o chunk é inteiramente dicionarizado.

Metadados de criptografia (criptografia modular, versão 2.7.0, `PARQUET-1178`). Cada módulo do
arquivo (páginas, cabeçalhos de página, page index, filtros Bloom, rodapé) é cifrado separadamente com
AES em modo GCM (`AES_GCM_V1`) ou GCM para metadados e CTR para páginas (`AES_GCM_CTR_V1`), com chaves
de 128, 192 ou 256 bits. Com rodapé cifrado, o arquivo começa e termina com o número mágico `PARE` e
um `FileCryptoMetaData` (algoritmo e `key_metadata`) precede o rodapé cifrado. Com rodapé em texto
claro, `FileMetaData` recebe `encryption_algorithm` e `footer_signing_key_metadata`, e o rodapé é
assinado. Colunas cifradas com chave própria têm o `ColumnMetaData` serializado à parte em
`encrypted_column_metadata`, para proteger estatísticas. O DuckDB cifra rodapé e todas as colunas com
uma única chave (`PRAGMA add_parquet_key` e `ENCRYPTION_CONFIG {footer_key: ...}`); chaves por
coluna dão erro `Not implemented`. O PyArrow implementa o esquema completo com um cliente de KMS. O
DuckDB lê arquivos do PyArrow cifrados uniformemente. Custo medido pelo DuckDB no `lineitem` SF1:
leitura de 0,26 s para 0,64 s e escrita de 0,99 s para 2,21 s.

Arquivos de sumário `_metadata` e `_common_metadata`: convenção do Spark e do Dask, não da
especificação. São arquivos Parquet só com rodapé; `_common_metadata` traz o esquema do conjunto e
`_metadata` traz os row groups de todos os arquivos, com `ColumnChunk.file_path` apontando para cada
um. O PyArrow monta os dois com `metadata_collector` e `write_metadata`. O DuckDB não os lê.

```python
# Chaves próprias no esquema Arrow voltam no rodapé ao lado de ARROW:schema; os sumários saem de metadata_collector.
import pyarrow.parquet as pq

table_with_metadata = table.replace_schema_metadata({"serialize_db_version": "0.1.0", "serialize_db_execution_id": "abc123"})
pq.write_table(table_with_metadata, "operacoes.parquet")
print(pq.read_metadata("operacoes.parquet").metadata.keys())
# dict_keys([b'ARROW:schema', b'serialize_db_execution_id', b'serialize_db_version'])
with pq.ParquetWriter("operacoes_2.parquet", table.schema) as writer:
    writer.write_table(table)
    writer.add_key_value_metadata({"linhas": str(table.num_rows)})  # valor conhecido só no fim da gravação

collector = []
pq.write_to_dataset(month_table, "operacoes", partition_cols=["mes"], metadata_collector=collector)
data_schema = collector[0].schema.to_arrow_schema()  # o esquema dos arquivos, sem a coluna de partição
pq.write_metadata(data_schema, "operacoes/_common_metadata")
pq.write_metadata(data_schema, "operacoes/_metadata", metadata_collector=collector)
print(pq.read_metadata("operacoes/_metadata").row_group(0).column(0).file_path)
# mes=2026-01/597c7a1e1007448aa9114740c3c1a836-0.parquet
```

Com `store_schema=False` o PyArrow deixa de gravar também as chaves próprias, e `metadata` volta
`None`. `write_metadata` exige o esquema dos arquivos: com o esquema de `tabela_mes`, que inclui
`mes`, ele falha com `AppendRowGroups requires equal schemas`.

Ordem das colunas (`column_orders`) e `sorting_columns` estão descritos nas seções
[FileMetaData](#filemetadata) e [Row group](#row-group). A especificação reserva o campo Thrift 32767
de toda estrutura para extensões binárias.

### Inspeção dos metadados com DuckDB

Funções da extensão `parquet`, todas aceitando um caminho, uma lista ou um glob:

| Função | Devolve |
| --- | --- |
| `parquet_file_metadata(f)` | Uma linha por arquivo: `created_by`, `num_rows`, `num_row_groups`, `format_version`, `encryption_algorithm`, `footer_signing_key_metadata`, `file_size_bytes`, `footer_size`, `column_orders`. |
| `parquet_schema(f)` | Um `SchemaElement` por linha: `name`, `type`, `type_length`, `repetition_type`, `num_children`, `converted_type`, `scale`, `precision`, `field_id`, `logical_type`. |
| `parquet_metadata(f)` | Um column chunk por linha: `row_group_id`, `row_group_num_rows`, `row_group_num_columns`, `row_group_bytes`, `row_group_compressed_bytes`, `column_id`, `file_offset`, `num_values`, `path_in_schema`, `type`, `stats_min`, `stats_max`, `stats_min_value`, `stats_max_value`, `stats_null_count`, `stats_distinct_count`, `min_is_exact`, `max_is_exact`, `compression`, `encodings`, `index_page_offset`, `dictionary_page_offset`, `data_page_offset`, `total_compressed_size`, `total_uncompressed_size`, `key_value_metadata`, `bloom_filter_offset`, `bloom_filter_length`. |
| `parquet_kv_metadata(f)` | `key` e `value` como `BLOB`. |
| `parquet_bloom_probe(f, coluna, valor)` | Por row group, se o filtro Bloom exclui o valor. |
| `parquet_full_metadata(f)` | As quatro primeiras numa linha, como listas de structs. |
| `DESCRIBE SELECT * FROM f` | Nomes e tipos DuckDB das colunas, que é o que importa para casar com uma tabela. |

Saídas no arquivo de exemplo gravado pelo PyArrow:

```sql
SELECT created_by, num_rows, num_row_groups, format_version, file_size_bytes, footer_size
FROM parquet_file_metadata('operacoes.parquet');
```

```text
┌──────────────────────────────────┬──────────┬────────────────┬────────────────┬─────────────────┬─────────────┐
│            created_by            │ num_rows │ num_row_groups │ format_version │ file_size_bytes │ footer_size │
├──────────────────────────────────┼──────────┼────────────────┼────────────────┼─────────────────┼─────────────┤
│ parquet-cpp-arrow version 25.0.1 │   300000 │              3 │              2 │         4850488 │        3050 │
└──────────────────────────────────┴──────────┴────────────────┴────────────────┴─────────────────┴─────────────┘
```

```sql
SELECT name, type, repetition_type, field_id, logical_type
FROM parquet_schema('operacoes.parquet');
```

```text
┌─────────────┬──────────────────────┬─────────────────┬──────────┬────────────────────────────────────┐
│    name     │         type         │ repetition_type │ field_id │            logical_type            │
├─────────────┼──────────────────────┼─────────────────┼──────────┼────────────────────────────────────┤
│ schema      │ NULL                 │ REQUIRED        │     NULL │ NULL                               │
│ id_operacao │ INT64                │ REQUIRED        │        1 │ NULL                               │
│ data_ref    │ INT32                │ REQUIRED        │        2 │ DateType()                         │
│ id_cliente  │ INT64                │ REQUIRED        │        3 │ NULL                               │
│ valor       │ FIXED_LEN_BYTE_ARRAY │ REQUIRED        │        4 │ DecimalType(scale=2, precision=18) │
│ descricao   │ BYTE_ARRAY           │ OPTIONAL        │        5 │ StringType()                       │
└─────────────┴──────────────────────┴─────────────────┴──────────┴────────────────────────────────────┘
```

```sql
SELECT row_group_id, path_in_schema, stats_min_value, stats_max_value, stats_null_count,
       compression, encodings, bloom_filter_length, total_compressed_size
FROM parquet_metadata('operacoes.parquet')
WHERE path_in_schema IN ('data_ref', 'id_cliente')
ORDER BY row_group_id, column_id;
```

```text
┌──────────────┬────────────────┬─────────────────┬─────────────────┬──────────────────┬─────────────┬────────────────────────────┬─────────────────────┬───────────────────────┐
│ row_group_id │ path_in_schema │ stats_min_value │ stats_max_value │ stats_null_count │ compression │         encodings          │ bloom_filter_length │ total_compressed_size │
├──────────────┼────────────────┼─────────────────┼─────────────────┼──────────────────┼─────────────┼────────────────────────────┼─────────────────────┼───────────────────────┤
│            0 │ data_ref       │ 2026-01-01      │ 2026-03-22      │                0 │ ZSTD        │ PLAIN, RLE, RLE_DICTIONARY │                NULL │                   614 │
│            0 │ id_cliente     │ 1               │ 5000            │                0 │ ZSTD        │ PLAIN, RLE, RLE_DICTIONARY │                8209 │                171908 │
│            1 │ data_ref       │ 2026-03-22      │ 2026-06-10      │                0 │ ZSTD        │ PLAIN, RLE, RLE_DICTIONARY │                NULL │                   599 │
│            1 │ id_cliente     │ 1               │ 5000            │                0 │ ZSTD        │ PLAIN, RLE, RLE_DICTIONARY │                8209 │                171907 │
│            2 │ data_ref       │ 2026-06-10      │ 2026-08-29      │                0 │ ZSTD        │ PLAIN, RLE, RLE_DICTIONARY │                NULL │                   610 │
│            2 │ id_cliente     │ 1               │ 5000            │                0 │ ZSTD        │ PLAIN, RLE, RLE_DICTIONARY │                8209 │                171915 │
└──────────────┴────────────────┴─────────────────┴─────────────────┴──────────────────┴─────────────┴────────────────────────────┴─────────────────────┴───────────────────────┘
```

A coluna de ordenação `data_ref` tem faixas disjuntas entre row groups, e `id_cliente`, aleatória,
cobre `1` a `5000` em todos. Os valores de `BLOB` de `parquet_kv_metadata` precisam de cast:

```sql
SELECT key::VARCHAR AS key, value::VARCHAR AS value
FROM parquet_kv_metadata('operacoes_duckdb.parquet');
-- serialize_db_version      │ 0.1.0
-- serialize_db_execution_id │ abc123
```

```sql
FROM parquet_bloom_probe('operacoes.parquet', 'id_cliente', 99999);
-- bloom_filter_excludes = true nos 3 row groups
FROM parquet_bloom_probe('operacoes.parquet', 'id_cliente', 4242);
-- bloom_filter_excludes = false nos 3 row groups
```

Consultas úteis sobre `parquet_metadata`:

```sql
-- Row groups que um filtro por intervalo em data_ref não consegue pular.
SELECT row_group_id, stats_min_value, stats_max_value,
       NOT (stats_max_value::DATE < DATE '2026-08-01'
            OR stats_min_value::DATE > DATE '2026-08-31') AS precisa_ler
FROM parquet_metadata('operacoes.parquet')
WHERE path_in_schema = 'data_ref'
ORDER BY row_group_id;
-- 0 │ 2026-01-01 │ 2026-03-22 │ false
-- 1 │ 2026-03-22 │ 2026-06-10 │ false
-- 2 │ 2026-06-10 │ 2026-08-29 │ true

-- Distribuição de row groups por arquivo num diretório.
SELECT file_name, count(DISTINCT row_group_id) AS row_groups,
       min(row_group_num_rows) AS menor, max(row_group_num_rows) AS maior
FROM parquet_metadata('dados/**/*.parquet')
GROUP BY file_name;

-- Compressão e codificações por coluna, para achar gzip ou PLAIN onde não se espera.
SELECT path_in_schema, compression, encodings, count(*) AS chunks,
       sum(total_compressed_size) AS bytes
FROM parquet_metadata('dados/**/*.parquet')
GROUP BY ALL ORDER BY bytes DESC;

-- Colunas sem filtro Bloom em algum row group.
SELECT path_in_schema, count(*) FILTER (WHERE bloom_filter_offset IS NULL) AS sem_bloom
FROM parquet_metadata('dados/**/*.parquet')
GROUP BY path_in_schema;
```

`EXPLAIN ANALYZE` mostra o que o leitor aproveitou: `File Filters` e `Scanning Files: 1/8` para
poda por partição, `Filters` para predicados empurrados ao scan, `Dynamic Filters` para filtros
vindos de um join, e `Total Files Read`. Em arquivos remotos ele também imprime o número de
requisições e os bytes transferidos.

### Inspeção dos metadados com PyArrow

O módulo `pyarrow.parquet` expõe o rodapé como objetos:

| Objeto | Como obter | Atributos |
| --- | --- | --- |
| `FileMetaData` | `pq.read_metadata(f)` ou `pq.ParquetFile(f).metadata` | `created_by`, `format_version`, `num_rows`, `num_columns`, `num_row_groups`, `serialized_size` (bytes do rodapé), `metadata` (chave-valor, `bytes` para `bytes`), `schema`, `row_group(i)`, `to_dict()`, `append_row_groups`, `set_file_path`, `write_metadata_file`. |
| `RowGroupMetaData` | `metadata.row_group(i)` | `num_rows`, `num_columns`, `total_byte_size`, `sorting_columns`, `column(j)`. |
| `ColumnChunkMetaData` | `row_group.column(j)` | `path_in_schema`, `physical_type`, `num_values`, `compression`, `encodings`, `is_stats_set`, `statistics`, `has_dictionary_page`, `dictionary_page_offset`, `data_page_offset`, `has_column_index`, `has_offset_index`, `bloom_filter_offset`, `bloom_filter_length`, `total_compressed_size`, `total_uncompressed_size`, `geo_statistics`, `file_path`. |
| `Statistics` | `column.statistics` | `has_min_max`, `min`, `max` (decodificados), `min_raw`, `max_raw`, `null_count`, `distinct_count`, `num_values`, `physical_type`, `logical_type`. |
| `ParquetSchema` | `pq.ParquetFile(f).schema` | O esquema físico; `column(i)` dá um `ColumnSchema` com `physical_type`, `logical_type`, `converted_type`, `length`, `precision`, `scale`, `max_definition_level`, `max_repetition_level`. |
| `pyarrow.Schema` | `pq.read_schema(f)` ou `ParquetFile(f).schema_arrow` | O esquema Arrow, com `not null`, metadados de campo (`PARQUET:field_id`) e o `ARROW:schema` aplicado. |

Percorrer todos os column chunks:

```python
import pyarrow.parquet as pq

md = pq.read_metadata("operacoes.parquet")
print(md.created_by, md.format_version, md.num_rows, md.num_row_groups, md.serialized_size)
for i in range(md.num_row_groups):
    rg = md.row_group(i)
    print("row group", i, rg.num_rows, "linhas", rg.sorting_columns)
    for j in range(rg.num_columns):
        c = rg.column(j)
        s = c.statistics
        print(
            f"  {c.path_in_schema:12} {c.physical_type:22} {c.compression:6} {c.encodings}",
            f"min={s.min} max={s.max} nulos={s.null_count}" if c.is_stats_set else "sem estatísticas",
            f"bloom={c.bloom_filter_length}",
            f"page_index={c.has_column_index and c.has_offset_index}",
        )
```

Saída para o primeiro row group do arquivo de exemplo:

```text
parquet-cpp-arrow version 25.0.1 2.6 300000 3 3050
row group 0 100000 linhas (SortingColumn(column_index=1, descending=False, nulls_first=False), SortingColumn(column_index=0, descending=False, nulls_first=False))
  id_operacao  INT64                  ZSTD   ('PLAIN', 'RLE', 'RLE_DICTIONARY') min=4 max=299998 nulos=0 bloom=None page_index=True
  data_ref     INT32                  ZSTD   ('PLAIN', 'RLE', 'RLE_DICTIONARY') min=2026-01-01 max=2026-03-22 nulos=0 bloom=None page_index=True
  id_cliente   INT64                  ZSTD   ('PLAIN', 'RLE', 'RLE_DICTIONARY') min=1 max=5000 nulos=0 bloom=8209 page_index=True
  valor        FIXED_LEN_BYTE_ARRAY   ZSTD   ('PLAIN', 'RLE', 'RLE_DICTIONARY') min=0.08 max=9999.99 nulos=0 bloom=None page_index=True
  descricao    BYTE_ARRAY             ZSTD   ('PLAIN', 'RLE', 'RLE_DICTIONARY') min=op-100001 max=op-99998 nulos=2053 bloom=None page_index=True
```

`to_dict()` serializa tudo, o que serve para gravar o rodapé num log ou comparar dois arquivos:

```python
d = md.to_dict()
d["row_groups"][0]["columns"][2]["statistics"]
# {'has_min_max': True, 'min': 1, 'max': 5000, 'null_count': 0, 'distinct_count': None,
#  'num_values': 100000, 'physical_type': 'INT64'}
```

Esquema físico e esquema Arrow:

```python
pf = pq.ParquetFile("operacoes.parquet")
print(pf.schema)            # required int64 field_id=1 id_operacao; ...
print(pf.schema.column(3))  # physical_type FIXED_LEN_BYTE_ARRAY, logical_type Decimal(precision=18, scale=2)
print(pf.schema_arrow)      # id_operacao: int64 not null, PARQUET:field_id '1', ...
print(pf.metadata.metadata.keys())  # dict_keys([b'ARROW:schema'])
```

Para um diretório, `pyarrow.dataset` dá o rodapé de cada fragmento (`fragment.metadata`) e os row
groups com estatísticas (`fragment.row_groups`, cada um com `statistics` por coluna), sem ler
dados. `ParquetFile.iter_batches(batch_size, columns, row_groups)` e `read_row_group(i)` leem partes
do arquivo; no exemplo, `iter_batches(batch_size=65_536, columns=["id_operacao"])` devolveu 5 lotes.

### Particionamento de arquivos Parquet

O formato não tem partições. Particionamento é uma convenção de diretórios (e, opcionalmente, um
catálogo) sobre um conjunto de arquivos. A convenção Hive nomeia cada nível como `chave=valor`, e o
valor da coluna de partição é derivado do nome do diretório, não do arquivo:

```text
operacoes/
├── mes=2026-01/
│   └── exec_abc123_0.parquet
├── mes=2026-02/
│   └── exec_abc123_0.parquet
└── mes=2026-03/
    └── exec_abc123_0.parquet
```

Regras que os três ambientes compartilham:

- A coluna de partição normalmente não está dentro dos arquivos. O DuckDB só a grava com
  `WRITE_PARTITION_COLUMNS true`; o `UNLOAD` do Redshift só com `PARTITION BY (...) INCLUDE`; o
  PyArrow a remove em `write_to_dataset`. O Redshift Spectrum exige que a chave de partição não
  seja coluna da tabela. O `COPY` do Redshift, ao contrário, só vê o que está dentro dos arquivos,
  então dados a serem carregados por `COPY` precisam da coluna de partição gravada.
- O tipo da coluna de partição vem do nome do diretório. O DuckDB reconhece `DATE`, `TIMESTAMP` e
  `BIGINT` e deixa o resto como `VARCHAR`; `hive_types = {'mes': VARCHAR}` fixa o tipo e
  `hive_types_autocast = 0` desliga a detecção. O PyArrow infere com `partitioning="hive"` ou
  recebe um esquema em `ds.partitioning(pa.schema([...]), flavor="hive")`. O Spectrum recebe o tipo
  em `PARTITIONED BY (mes CHAR(7))`.
- Filtros na coluna de partição eliminam diretórios inteiros antes de abrir qualquer arquivo. No
  DuckDB, `EXPLAIN` mostra `File Filters: (mes = '2026-03')` e `Scanning Files: 1/8`. No PyArrow,
  `dataset.get_fragments(filter=ds.field("mes") == "2026-03")` devolve só o arquivo daquela
  partição. No Spectrum, `SVL_S3PARTITION` mostra partições totais e qualificadas.
- Partições pequenas custam caro: cada arquivo tem rodapé, listagem e requisição próprios. O DuckDB
  recomenda pelo menos 100 MB por partição; o Spectrum recomenda arquivos de 64 MB a 1 GB, de
  tamanhos parecidos. A partição como unidade de escrita da biblioteca segue essa regra.

Gravação particionada:

| Ferramenta | Comando | Comportamento |
| --- | --- | --- |
| DuckDB | `COPY tbl TO 'dir' (FORMAT parquet, PARTITION_BY (mes))` | Um arquivo por thread por diretório; `FILENAME_PATTERN` com `{i}`, `{uuid}` ou `{uuidv7}`; `OVERWRITE_OR_IGNORE` permite gravar sobre diretório existente, `OVERWRITE` apaga o conteúdo (só em sistema de arquivos local), `APPEND` gera nomes únicos e confere colisões; `partitioned_write_max_open_files` (padrão 100) limita arquivos abertos. `PARTITION_BY` não aceita expressões: a coluna se cria no `SELECT`. |
| PyArrow | `pq.write_to_dataset(t, "dir", partition_cols=["mes"], basename_template="exec_{i}.parquet", existing_data_behavior="delete_matching")` | `delete_matching` apaga o diretório de cada partição tocada antes de gravar (substituição idempotente por partição); `overwrite_or_ignore` só sobrescreve arquivos de mesmo nome; `error` recusa destino com dados. `ds.write_dataset` expõe `max_partitions`, `max_open_files`, `max_rows_per_file`, `min_rows_per_group` e `max_rows_per_group`. |
| Redshift | `UNLOAD ('...') TO 's3://.../' ... PARQUET PARTITION BY (mes)` | Diretórios `mes=valor/`, um arquivo por slice; `CLEANPATH` apaga os arquivos existentes só nas partições que recebem dados. `CREATE EXTERNAL TABLE ... PARTITIONED BY ... AS SELECT` e `INSERT` em tabela externa gravam Parquet e registram as partições no catálogo. |

Leitura particionada:

| Ferramenta | Como |
| --- | --- |
| DuckDB | `read_parquet('dir/*/*.parquet', hive_partitioning = true)`; a detecção é automática quando os diretórios seguem `chave=valor`. Um glob `dir/**/*.parquet` cobre níveis variáveis. |
| PyArrow | `pq.read_table("dir", filters=[("mes", "=", "2026-03")])` ou `ds.dataset("dir", format="parquet", partitioning="hive")`. Um caminho de arquivo único não infere partições, só o diretório. O PyArrow também aceita "directory partitioning" sem `chave=` (`ds.partitioning(field_names=["ano", "mes"])`). |
| Redshift Spectrum | `CREATE EXTERNAL TABLE ... PARTITIONED BY (mes CHAR(7)) STORED AS PARQUET LOCATION 's3://.../'` e `ALTER TABLE ... ADD PARTITION (mes='2026-03') LOCATION 's3://.../mes=2026-03/'` (até 100 partições por comando com o Glue), ou um catálogo Glue que já conheça as partições. |
| Redshift `COPY` | Não interpreta diretórios: um prefixo carrega todos os arquivos abaixo dele. Para carregar um mês, o `FROM` aponta para o diretório do mês ou para um manifesto. |

As duas tabelas, em Python:

```python
# Gravação Hive por mês, com substituição idempotente da partição, e leitura que poda pelo diretório.
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from datetime import date

pq.write_to_dataset(month_table, "operacoes", partition_cols=["mes"],
                    basename_template="exec_abc123_{i}.parquet", existing_data_behavior="delete_matching")
print(pq.read_schema("operacoes/mes=2026-08/exec_abc123_0.parquet").names)
# ['id_operacao', 'data_ref', 'id_cliente', 'valor', 'descricao']: a coluna mes ficou no diretório
partitioning = ds.partitioning(pa.schema([("mes", pa.string())]), flavor="hive")
dataset = ds.dataset("operacoes", format="parquet", partitioning=partitioning)
print([f.path for f in dataset.get_fragments(filter=ds.field("mes") == "2026-03")])
# ['operacoes/mes=2026-03/exec_abc123_0.parquet']
august = dataset.to_table(columns=["id_operacao", "data_ref", "valor"],
                           filter=(ds.field("mes") == "2026-08") & (ds.field("data_ref") >= date(2026, 8, 15)))
```

Além do Hive há o particionamento oculto do Iceberg (a transformação, como `month(data_ref)`, fica
nos metadados da tabela, e os arquivos não precisam de diretórios `chave=valor`), o particionamento do
Delta, que usa diretórios Hive com os valores registrados no log e sem a coluna dentro do arquivo
([Delta Lake](#delta-lake)), e o particionamento dentro do arquivo, que são os row groups ordenados. As duas camadas
se combinam: o diretório elimina meses, e as estatísticas dos row groups eliminam faixas dentro do
mês.

### Otimização do Parquet para performance de consultas

Uma consulta sobre Parquet custa bytes lidos, bytes decodificados e paralelismo disponível. As
otimizações agem em cada camada, e o leitor decide o que usar a partir dos metadados:

| Camada | Estrutura usada | Efeito | Quem aplica |
| --- | --- | --- | --- |
| Diretório | Nomes `chave=valor` | Elimina arquivos sem abrir. | DuckDB, PyArrow, Spectrum. |
| Coluna | `ColumnChunk` offsets | Lê só as colunas da consulta (projection pushdown). | Todos. |
| Row group | `Statistics` min/max e nulos | Elimina row groups por predicado. | DuckDB, PyArrow (`filters`), parquet-java, arrow-rs. |
| Row group | Filtro Bloom | Elimina row groups por igualdade em coluna não ordenada. | DuckDB, parquet-java, arrow-rs. |
| Página | `ColumnIndex` e `OffsetIndex` | Elimina páginas e, nas outras colunas, as linhas correspondentes. | parquet-java, arrow-rs, cuDF. |
| Valor | Materialização tardia | Avalia o predicado numa coluna decodificada antes de decodificar as outras. | arrow-rs (DataFusion). |

A camada de página e a materialização tardia não valem para o DuckDB nem para o PyArrow hoje. As
recomendações abaixo focam nas camadas de diretório, coluna e row group, que valem para os dois, e
mantêm o arquivo útil para leitores que usam as demais.

#### Filtros por chaves

Chave aqui é a chave primária do modelo ou um identificador usado em busca pontual (`WHERE
id_operacao = 42`, `WHERE id_cliente IN (...)`).

Ordenar pela chave na gravação. Estatísticas min/max só podam quando as faixas dos row groups são
estreitas e disjuntas, e isso depende da ordem física. A documentação do DuckDB compara um inteiro
sequencial, que permite pular todos os row groups menos um, com um UUID aleatório, que obriga a ler
todos. No arquivo de exemplo, ordenado por `data_ref, id_operacao`, o filtro por agosto leu 1 dos 3
row groups (a consulta sobre `parquet_metadata` na seção de inspeção mostra os dois pulados), enquanto
`id_cliente`, aleatória, tem `1` a `5000` em todos. Com chave composta, a segunda coluna só poda
dentro de faixas da primeira; a `sort_key` do contrato (`data_ref`,
`id_operacao`) é esse caso.

```sql
COPY (SELECT ... FROM operacoes WHERE mes = '2026-08' ORDER BY data_ref, id_operacao)
TO 'operacoes/mes=2026-08/exec_abc123.parquet' (FORMAT parquet, ROW_GROUP_SIZE 100_000);
```

```python
t = t.sort_by([("data_ref", "ascending"), ("id_operacao", "ascending")])
pq.write_table(t, path, row_group_size=100_000,
               sorting_columns=pq.SortingColumn.from_ordering(t.schema, [("data_ref", "ascending"), ("id_operacao", "ascending")]))
```

Filtros Bloom para chaves não ordenadas. Um identificador consultado por igualdade que não é a
chave de ordenação (`id_cliente` numa tabela ordenada por data) só é podado por filtro Bloom. O DuckDB
grava o filtro quando a coluna do row group cabe no dicionário (`DICTIONARY_SIZE_LIMIT`, padrão
`ROW_GROUP_SIZE / 5` valores distintos); com mais valores distintos do que isso a coluna sai em
`PLAIN` e sem filtro, e um `ROW_GROUP_SIZE` maior ou um `DICTIONARY_SIZE_LIMIT` explícito resolvem. No
PyArrow o filtro é pedido por coluna, e `ndv` deve ser a cardinalidade esperada por row group (não
do arquivo) com `fpp` 0,01, o que custa cerca de 10,5 bits por valor distinto. No exemplo, `WHERE
id_cliente = 99999` virou `EMPTY_RESULT` no plano do DuckDB sem ler dados, e `parquet_bloom_probe`
confirmou a exclusão dos 3 row groups.

```python
pq.write_table(t, path, bloom_filter_options={"id_cliente": {"ndv": 5_000, "fpp": 0.01}})
```

Tamanho do row group. Row groups menores podam com mais precisão e paralelizam mais, mas cada um
custa metadados no rodapé e uma requisição a mais em leitura remota. A medição da documentação do
DuckDB com uma agregação simples:

| Linhas por row group | Tempo |
| --- | --- |
| 960 | 8,77 s |
| 7.680 | 2,35 s |
| 30.720 | 1,17 s |
| 122.880 | 0,87 s |
| 491.520 | 0,95 s |
| 1.966.080 | 0,88 s |

Abaixo de 5.000 linhas o custo cresce de 5 a 10 vezes; acima de 100.000 as diferenças ficam em torno
de 10 %. A regra da documentação: pelo menos tantos row groups por arquivo quanto threads, e entre
100.000 e 1 milhão de linhas por row group. Consultas muito seletivas ganham com mais row groups;
agregações sobre o arquivo inteiro perdem.

Predicado e coluna do mesmo tipo. Um literal de tipo diferente ou uma função sobre a coluna
(`CAST(id_operacao AS VARCHAR) = '42'`, `date_trunc('month', data_ref) = ...`) impede o leitor de
comparar com as estatísticas. Escrever o predicado sobre a coluna crua, com literal do tipo dela, e
deixar a transformação para o outro lado da comparação.

#### Filtros por valores de colunas

Intervalos em colunas temporais. Partição por mês e ordenação por data dentro do arquivo cobrem
`BETWEEN` em duas camadas. A documentação do DuckDB mediu uma coluna `DATETIME` ordenada contra a
mesma coluna desordenada: 1,3 GB contra 3,3 GB de armazenamento e 0,6 s contra 0,9 s de consulta. A
ordem melhora tanto a poda quanto a compressão.

```python
# Poda de row groups pelas estatísticas de data_ref, visível em split_by_row_group, e leitura só do que passa.
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from datetime import date

august = (ds.field("data_ref") >= date(2026, 8, 1)) & (ds.field("data_ref") < date(2026, 9, 1))
fragment = next(ds.dataset("operacoes.parquet", format="parquet").get_fragments())
print([rg.id for part in fragment.split_by_row_group(filter=august) for rg in part.row_groups])
# [2]: só o terceiro row group tem data_ref em agosto; os outros dois não são lidos
print(len(fragment.split_by_row_group(filter=ds.field("id_cliente") == 4242)))
# 3: coluna aleatória, min e max cobrem o valor em todos os row groups
t = pq.read_table("operacoes.parquet", columns=["id_operacao", "data_ref", "valor"], filters=august)
```

Colunas de baixa cardinalidade (status, tipo, moeda). Dicionário e RLE comprimem bem, e o DuckDB grava
filtro Bloom para elas: no exemplo do blog do DuckDB, 10 valores distintos em 100 milhões de linhas
embaralhadas produziram filtros de 47 bytes por row group e uma busca por valor inexistente caiu de
0,1 s para 0,002 s. Particionar por uma coluna dessas só compensa quando quase toda consulta filtra
por ela e o número de partições resultante ainda dá partições de 100 MB ou mais.

Colunas de valores aleatórios (hashes, UUIDs, `valor`). Não há poda por min/max. Resta o filtro Bloom
para igualdade e a projeção: não ler a coluna quando ela não entra na consulta. Uma segunda ordenação
dentro da partição (por exemplo, `ORDER BY id_cliente` dentro de cada mês, quando as consultas por
cliente dominam) troca a poda por data pela poda por cliente; não há como ter as duas no mesmo
arquivo.

Ponto flutuante. NaN quebra a comparação com min/max; a especificação pede `nan_count` e ordem
`IEEE_754_TOTAL_ORDER`, e o DuckDB tem `can_have_nan` para levar isso em conta na poda. Valores
monetários ficam em `DECIMAL`, como na [tabela de mapeamento de tipos][tipos].

Strings. Estatísticas de strings comparam bytes sem sinal e podem ser truncadas pelo gravador (o
DuckDB expõe `min_is_exact` e `max_is_exact`). O DuckDB reescreve `LIKE 'op-1%'` como
`descricao >= 'op-1' AND descricao < 'op-2'` e empurra o intervalo ao scan (visível em `EXPLAIN`);
um `LIKE '%sufixo'` não vira intervalo e lê tudo. Colunas de texto longo devem ficar fora das
consultas que não as usam, porque o custo de decodificar e descomprimir strings domina.

Projeção. Selecionar só as colunas necessárias é a otimização com mais efeito em tabelas largas:
cada coluna é um column chunk separado, e colunas aninhadas são folhas separadas. `SELECT *` sobre
arquivos remotos baixa o arquivo inteiro.

#### Joins com outras tabelas

O Parquet não tem índices nem chaves estrangeiras; o custo de um join vem da leitura do lado maior e
da qualidade do plano. Três coisas ajudam.

Filtros dinâmicos empurrados ao scan. O DuckDB constrói a tabela hash com o lado menor e, antes de
varrer o lado maior, empurra para o scan do Parquet os limites (`min`, `max`) e, quando o lado menor é
pequeno, a lista de valores da chave. No exemplo, o join de `operacoes.parquet` com uma tabela de 3
clientes produziu este scan:

```text
TABLE_SCAN  PARQUET_SCAN
  Filters: id_cliente>=10 AND id_cliente<=30
  Dynamic Filters: optional: id_cliente IN (10, 20, 30) AND optional: id_cliente>=10 AND optional: id_cliente<=30
  1,295 rows
```

O scan devolveu 1.295 linhas das 300.000 sem que a consulta tivesse `WHERE`. Os filtros dinâmicos
aproveitam as mesmas estruturas de um filtro comum, então um lado maior ordenado ou particionado
pela chave do join, ou com filtro Bloom nela, é lido em parte. Sem essas estruturas o filtro só
reduz as linhas depois de decodificadas.

Estatísticas para a ordem dos joins. O otimizador do DuckDB estima cardinalidades pelas estatísticas
da fonte. O Parquet não tem contagem de distintos (o campo `distinct_count` quase nunca é gravado), e
o formato nativo do DuckDB tem HyperLogLog. A documentação recomenda carregar os arquivos em tabelas
DuckDB para cargas com muitos joins ou muitas consultas repetidas; o TPC-H rodou de 1,1 a 5,0 vezes
mais lento direto sobre Parquet. Para uma execução de pipeline que faz muitos joins sobre os mesmos
meses, ler os Parquet para o sandbox uma vez e consultar as tabelas é o caminho, e é o que
[DuckDB](#duckdb) já descreve. `SET disabled_optimizers = 'join_order,build_side_probe_side'`
força a ordem escrita quando o plano sai ruim.

Mesma chave, mesmo tipo, mesma partição. Chaves de join com tipos diferentes (`INT32` de um lado,
`INT64` do outro, ou `VARCHAR` contra inteiro) exigem cast e impedem o filtro dinâmico de chegar ao
scan. Os dois lados devem sair do mesmo contrato de tipos. Particionar as duas tabelas pelo mesmo
critério (mês) e filtrar pelo mês na consulta reduz os dois lados antes do join; nenhum dos leitores
faz join por partição sozinho, então o predicado precisa estar na consulta.

No Redshift, as decisões de join ficam no banco, não no arquivo: `DISTKEY` na chave de join e `SORTKEY`
nas colunas de filtro, conforme `serialize_db.schema.TableOptions`. Para tabelas externas, a
documentação do
Spectrum recomenda manter as tabelas de dimensão locais e as tabelas de fato no S3, definir `numRows`
em `TABLE PROPERTIES` (ou usar as estatísticas de coluna do Glue) para o otimizador, e escrever
predicados e agregações que o Spectrum consiga executar na camada de leitura; `DISTINCT` e `ORDER
BY` não descem.

#### Outros tópicos de otimização

Tamanho e número de arquivos. O DuckDB recomenda arquivos de 100 MB a 10 GB e, no total, pelo menos
tantos row groups quanto threads (10 arquivos de 1 row group e 1 arquivo de 10 row groups dão o
mesmo paralelismo). O Spectrum recomenda arquivos maiores que 64 MB, até 1 GB, de tamanhos
parecidos, porque distribui o trabalho por arquivo e por row group. `FILE_SIZE_BYTES`,
`ROW_GROUPS_PER_FILE` e `PER_THREAD_OUTPUT` no DuckDB e `MAXFILESIZE` no `UNLOAD` controlam o corte.

Compressão. Snappy é o padrão do DuckDB, do PyArrow e do `UNLOAD`; zstd comprime mais com
descompressão parecida (`COMPRESSION_LEVEL` de 1 a 22 no DuckDB, padrão 3); gzip comprime bem e
descomprime devagar, e a documentação do DuckDB o cita como motivo para carregar o arquivo em vez de
consultá-lo direto. O blog do DuckDB mediu o `lineitem` SF1: snappy 244 MB, zstd 152 MB. O Spectrum
lê snappy, gzip e zstd em Parquet. `LZ4` é obsoleto; `LZ4_RAW` o substitui.

Codificações. Dicionário e RLE são o padrão e valem para quase tudo. `DELTA_BINARY_PACKED`,
`DELTA_LENGTH_BYTE_ARRAY` e `BYTE_STREAM_SPLIT` reduzem bem inteiros ordenados, strings e ponto
flutuante (o blog do DuckDB mediu `range(1e9)` de 3,7 GB para 1,3 MB), mas o DuckDB só as grava com
`PARQUET_VERSION 'V2'` porque parte dos leitores do mercado não as lê. Antes de mudar a versão,
confirmar que o Redshift lê o arquivo: o Spectrum documenta suporte ao Parquet v1, e o `UNLOAD`
grava v1.

Tipos. O tipo mais estreito que cabe nos dados, `TIMESTAMP` em microssegundos (o Delta grava
microssegundos e converte os nanossegundos do pandas em silêncio; o `INT96` é obsoleto), `DECIMAL` como inteiro quando o leitor aceita
(`store_decimal_as_integer` no PyArrow; o DuckDB já grava assim) e strings com dicionário. A
[tabela de mapeamento de tipos][tipos] já fixa isso. O formato marca o `INT96` como obsoleto
(`parquet.thrift`: "deprecated, new Parquet writers should not write data in INT96"). Um `INT96` de
outro escritor (a base de origem lida em 2026-09-20, gravada pelo pandas com
`use_deprecated_int96_timestamps`) chega ao PyArrow como
`timestamp[ns]`, sem estatística de mínimo e máximo no rodapé; `coerce_int96_timestamp_unit="us"`
no PyArrow e o `TIMESTAMP` do DuckDB truncam a parte sub-microssegundo em silêncio, e o cast seguro
de `[ns]` para `[us]` recusa quando ela não é zero (sondagem de 2026-09-20).

Page index e checksums. Gravar o page index no PyArrow (`write_page_index=True`) custa alguns bytes
por página e nada para leitores que o ignoram; Spark (parquet-java), DataFusion (arrow-rs) e Arrow
Go o usam para podar páginas. CRC por
página (`write_page_checksum`, `page_checksum_verification` na leitura) detecta corrupção ao custo de
calcular o hash.

Leitura remota. Cada arquivo custa duas requisições de metadados mais uma por intervalo lido, então
os ganhos vêm de menos arquivos, colunas certas e filtros que podem. O DuckDB tem
`parquet_metadata_cache` (padrão `false`) para reler os mesmos arquivos, `enable_external_file_cache`
(padrão `true`) e prefetch; o PyArrow tem `pre_buffer` (padrão `True`) para juntar intervalos.

Evolução de esquema. `field_id` estável por coluna, colunas novas só em arquivos novos, sem renomear.
O DuckDB lê conjuntos heterogêneos com `union_by_name = true` (colunas ausentes viram `NULL`, com
mais memória) ou com o parâmetro `schema` por `field_id`; o PyArrow com `schema=` no dataset.

As opções desta seção numa gravação em streaming a partir do sandbox:

```python
# Corte de arquivos e de row groups por linhas, com zstd, page index e checksum, a partir de um leitor em streaming.
import pyarrow as pa
import pyarrow.dataset as ds

reader = con.execute("""
    SELECT id_operacao, data_ref, id_cliente, valor, descricao
    FROM operacoes WHERE mes = '2026-08' ORDER BY data_ref, id_operacao
""").to_arrow_reader(100_000)
batches = pa.RecordBatchReader.from_batches(schema, (batch.cast(schema) for batch in reader))
options = ds.ParquetFileFormat().make_write_options(compression="zstd", write_page_index=True,
                                                   write_page_checksum=True)
ds.write_dataset(batches, "operacoes/mes=2026-08", format="parquet", file_options=options,
                 basename_template="exec_abc123_{i}.parquet", existing_data_behavior="delete_matching",
                 max_rows_per_file=1_000_000, min_rows_per_group=100_000, max_rows_per_group=100_000)
```

`write_dataset` não aceita `schema` junto com um `RecordBatchReader` (`Cannot specify a schema when
providing a RecordBatchReader`); o contrato entra pelo leitor montado com `from_batches`, e o
`required` das colunas `NOT NULL` chega ao arquivo.

### Importação e exportação de Parquet no DuckDB

#### Leitura direta

`read_parquet` (ou o caminho terminado em `.parquet`) lê um arquivo, uma lista ou um glob, com
projeção e filtros empurrados ao leitor. Parâmetros: `binary_as_string`, `can_have_nan`,
`encryption_config`, `file_row_number`, `hive_partitioning`, `union_by_name` e `schema`. A coluna
virtual `filename` existe desde a 1.3.0. Uma `VIEW` sobre `read_parquet` consulta os arquivos no
lugar, e `CREATE TABLE ... AS FROM read_parquet(...)` os carrega com os tipos inferidos.

#### Importação com esquema definido

O esquema definido é a tabela criada antes da carga, com tipos, `NOT NULL` e, quando vale o custo
medido na seção de ingestão do DuckDB, chaves. O que o DuckDB verifica ao carregar, medido na 1.5.5:

| Situação | `COPY tbl FROM arquivo` | `INSERT INTO tbl BY NAME SELECT * FROM arquivo` |
| --- | --- | --- |
| Colunas na mesma ordem e tipos iguais | Carrega. | Carrega. |
| Colunas em outra ordem | Casa por posição e falha na conversão de tipo, ou carrega dados trocados quando os tipos coincidem. | Casa por nome e carrega. |
| Coluna a mais no arquivo | `column count mismatch: expected 5 columns but found 6`. | `Table "operacoes" does not have a column with name "extra"`; `SELECT * EXCLUDE (extra)` resolve. |
| Coluna a menos no arquivo | `column count mismatch: expected 5 columns but found 4`; `COPY tbl (col1, ...) FROM arquivo` com lista de colunas carrega e preenche o resto com o padrão. | Carrega e preenche com o padrão; `NOT NULL` sem padrão falha. |
| Tipo diferente e conversível (`VARCHAR '44'` para `BIGINT`) | Converte em silêncio e carrega. | Converte em silêncio e carrega. |
| Tipo diferente e não conversível (`'x44'`, `DOUBLE` fora de `DECIMAL(18,2)`) | `Conversion Error: ... failed to cast column "id_operacao" from type VARCHAR to BIGINT`, com a sugestão de `BY NAME`. | O mesmo erro. |
| `NULL` em coluna `NOT NULL` | `Constraint Error: NOT NULL constraint failed: operacoes.id_cliente`. | O mesmo erro. |

Duas consequências. A conversão implícita significa que a carga não garante que o arquivo tenha os
tipos do contrato, só que os valores cabem neles; a verificação de tipos precisa ser feita sobre os
metadados, antes da carga. E `COPY ... FROM` é posicional, então a ordem das colunas no arquivo é
parte do contrato, ou a carga usa `INSERT ... BY NAME`.

Verificação do esquema do arquivo contra a tabela, sem ler dados:

```sql
WITH esperado AS (
    SELECT column_name, data_type, ordinal_position
    FROM information_schema.columns
    WHERE table_name = 'operacoes'
), arquivo AS (
    SELECT column_name, column_type, row_number() OVER () AS ordinal_position
    FROM (DESCRIBE SELECT * FROM 'entrada.parquet')
)
SELECT column_name, e.data_type AS esperado, a.column_type AS arquivo,
       e.ordinal_position AS posicao_esperada, a.ordinal_position AS posicao_arquivo
FROM esperado e FULL OUTER JOIN arquivo a USING (column_name)
WHERE e.data_type IS DISTINCT FROM a.column_type
   OR e.ordinal_position IS DISTINCT FROM a.ordinal_position;
-- vazio quando o arquivo casa com a tabela
```

Em Python, a conferência lê só o rodapé e compara nomes, ordem e tipos com o contrato. `NOT NULL`
fica para a tabela, porque o arquivo do DuckDB marca toda coluna como `optional`:

```python
# Conferência do arquivo contra o contrato antes da carga (nomes, ordem e tipos, sem ler dados) e carga por nome.
import pyarrow.parquet as pq
import sqlalchemy as sa

def check_file(contract: sa.Table, path: str, partition_column: str = "mes") -> None:
    expected = [c for c in arrow_schema(contract) if c.name != partition_column]  # a partição fica no diretório
    actual = pq.read_schema(path)
    if actual.names != [c.name for c in expected]:
        raise ValueError(f"colunas do arquivo {actual.names}, contrato {[c.name for c in expected]}")
    for field in expected:
        if actual.field(field.name).type != field.type:
            raise ValueError(f"{field.name}: arquivo {actual.field(field.name).type}, contrato {field.type}")

check_file(Operacao.__table__, "operacoes/mes=2026-08/exec_abc123_0.parquet")
con.execute("""
    INSERT INTO operacoes BY NAME
    SELECT * FROM read_parquet('operacoes/mes=2026-08/*.parquet',
                               hive_partitioning = true, hive_types = {'mes': VARCHAR})
""")
```

A comparação é pelo tipo lógico: `pq.read_schema` devolve `decimal128(18, 2)` tanto para o
`FIXED_LEN_BYTE_ARRAY` do PyArrow quanto para o `INT64` do DuckDB. Um arquivo com `valor` em `DOUBLE`
falha com `valor: arquivo double, contrato decimal128(18, 2)`.

Leitura pelo `field_id`, que torna a carga independente de nome e posição e aplica o tipo pedido:

```sql
INSERT INTO operacoes BY NAME
SELECT * FROM read_parquet('entrada.parquet', schema = MAP {
    1: {name: 'id_operacao', type: 'BIGINT',        default_value: NULL},
    2: {name: 'data_ref',    type: 'DATE',          default_value: NULL},
    3: {name: 'id_cliente',  type: 'BIGINT',        default_value: NULL},
    4: {name: 'valor',       type: 'DECIMAL(18,2)', default_value: NULL},
    5: {name: 'descricao',   type: 'VARCHAR',       default_value: NULL},
    6: {name: 'origem',      type: 'VARCHAR',       default_value: 'legado'}
});
```

O parâmetro exige `field_id` nos arquivos (o DuckDB grava com `FIELD_IDS`, o PyArrow com o metadado
de campo `PARQUET:field_id`) e não combina com `union_by_name`. Um `field_id` ausente no arquivo
recebe `default_value`.

Chaves primárias e únicas são verificadas por índice ART na inserção, ao custo medido na
seção de ingestão do DuckDB; a auditoria por consulta depois da carga é a alternativa. `CHECK` está
disponível. Para cargas maiores que a memória, `SET preserve_insertion_order = false` reduz o uso de
memória em `COPY` e em `CREATE TABLE AS`.

`EXPORT DATABASE 'dir' (FORMAT parquet)` e `IMPORT DATABASE 'dir'` movem um banco inteiro: o
diretório recebe `schema.sql` com o DDL (`CREATE TABLE t(id BIGINT NOT NULL, ...)`), `load.sql` com
um `COPY ... FROM` por tabela e um Parquet por tabela. A importação recria as tabelas com as
restrições e carrega os arquivos por posição.

#### Exportação com esquema definido

`COPY (consulta) TO 'arquivo.parquet' (opções)` grava o resultado da consulta. As opções de Parquet:

| Opção | Padrão | Efeito |
| --- | --- | --- |
| `COMPRESSION` | `snappy` | `uncompressed`, `snappy`, `gzip`, `zstd`, `brotli`, `lz4`, `lz4_raw`. |
| `COMPRESSION_LEVEL` | 3 | Nível do zstd, de 1 a 22. |
| `ROW_GROUP_SIZE` | 122.880 | Linhas por row group. `CHUNK_SIZE` é sinônimo. |
| `ROW_GROUP_SIZE_BYTES` | `ROW_GROUP_SIZE × 1024` | Bytes por row group; só com `preserve_insertion_order = false`. |
| `ROW_GROUPS_PER_FILE` | vazio | Novo arquivo a cada N row groups. |
| `FIELD_IDS` | vazio | `{coluna: id}` ou `'auto'`; aninhados usam `__duckdb_field_id`. |
| `KV_METADATA` | vazio | `{chave: valor}` no rodapé; `BLOB` é gravado cru, o resto vira string. |
| `PARQUET_VERSION` | `V1` | `V2` liga as codificações delta e `BYTE_STREAM_SPLIT`. |
| `DICTIONARY_SIZE_LIMIT` | `ROW_GROUP_SIZE / 5` | Valores distintos máximos no dicionário; 0 desliga. |
| `WRITE_BLOOM_FILTER` | `true` | Grava filtros Bloom nas colunas dicionarizadas. |
| `BLOOM_FILTER_FALSE_POSITIVE_RATIO` | 0,01 | Alvo de falsos positivos. |
| `STRING_DICTIONARY_PAGE_SIZE_LIMIT` | 1 MB | Tamanho da dictionary page de strings. |
| `ENCRYPTION_CONFIG` | vazio | `{footer_key: 'nome'}` com chave registrada por `PRAGMA add_parquet_key`. |
| `SHREDDING`, `GEOPARQUET_VERSION` | | Colunas `VARIANT` e geometria. |

E as opções gerais de `COPY ... TO` que valem para vários arquivos: `PARTITION_BY`, `FILE_SIZE_BYTES`,
`PER_THREAD_OUTPUT`, `FILENAME_PATTERN`, `FILE_EXTENSION`, `OVERWRITE_OR_IGNORE`, `OVERWRITE`,
`APPEND`, `WRITE_PARTITION_COLUMNS`, `USE_TMP_FILE` (grava num `.tmp` e renomeia, para não deixar um
arquivo quebrado no lugar de um bom), `PRESERVE_ORDER`, `RETURN_FILES` e `RETURN_STATS`.

O esquema do arquivo é o esquema do resultado da consulta, então o contrato entra pela consulta: cada
coluna com cast explícito para o tipo do modelo, na ordem do modelo, com `ORDER BY` pela chave de
ordenação. O que o DuckDB não consegue expressar: `REQUIRED` (toda coluna sai `optional`) e
`sorting_columns`. `DECIMAL(18, 2)` sai como `INT64` anotado.

```sql
COPY (
    SELECT id_operacao::BIGINT        AS id_operacao,
           data_ref::DATE             AS data_ref,
           id_cliente::BIGINT         AS id_cliente,
           valor::DECIMAL(18, 2)      AS valor,
           descricao::VARCHAR         AS descricao
    FROM operacoes
    WHERE data_ref >= DATE '2026-08-01' AND data_ref < DATE '2026-09-01'
    ORDER BY data_ref, id_operacao
) TO 'operacoes/mes=2026-08/exec_abc123.parquet' (
    FORMAT parquet,
    COMPRESSION zstd,
    ROW_GROUP_SIZE 100_000,
    RETURN_STATS
);
```

`RETURN_STATS` devolve, por arquivo gravado, `count`, `file_size_bytes`, `footer_size_bytes`,
`column_statistics` (`min`, `max`, `null_count`, `num_values`, `column_size_bytes` por coluna) e
`partition_keys`. No exemplo: 300.000 linhas, 3.288.592 bytes, rodapé de 1.646 bytes,
`descricao` com `null_count = 6000`. Esses números servem à auditoria da exportação (contagem igual à
da consulta, nulos onde o modelo permite, `min` e `max` dentro do mês) sem reabrir o arquivo. Uma
segunda verificação lê `parquet_schema` do arquivo gravado e compara `type`, `logical_type`,
`field_id` e a ordem com o esperado, do mesmo modo que a consulta de verificação da importação.

```python
# Auditoria da exportação sobre a linha que RETURN_STATS devolve, sem reabrir o arquivo.
from datetime import date

name, rows, file_bytes, footer_bytes, columns, _ = con.execute(copy_command).fetchone()
columns = {column.strip('"'): stats for column, stats in columns.items()}  # as chaves vêm entre aspas; os valores são texto
expected = con.execute("SELECT count(*) FROM operacoes WHERE mes = '2026-08'").fetchone()[0]
assert rows == expected, f"gravadas {rows}, consultadas {expected}"
for required in ("id_operacao", "data_ref", "id_cliente", "valor"):
    assert columns[required]["null_count"] == "0", required
assert date(2026, 8, 1) <= date.fromisoformat(columns["data_ref"]["min"])
assert date.fromisoformat(columns["data_ref"]["max"]) < date(2026, 9, 1)
```

`comando_copy` é o `COPY` acima como string. `column_statistics` chega como
`MAP(VARCHAR, MAP(VARCHAR, VARCHAR))`: as chaves são os nomes das colunas entre aspas e todos os
valores são texto, inclusive `null_count`.

### Importação e exportação de Parquet no Redshift

O Redshift oferece três caminhos: `COPY` carrega arquivos do S3 numa tabela; `UNLOAD` grava o
resultado de uma consulta no S3; o Redshift Spectrum consulta arquivos no lugar por tabelas externas e
também grava, por `CREATE EXTERNAL TABLE ... AS` e `INSERT` em tabela externa. As regras abaixo vêm
da documentação oficial; o que ela não diz está marcado como leitura do ambiente alvo, com a data,
ou como não verificado.

#### COPY a partir de Parquet

```sql
COPY execucao_abc123.operacoes
FROM 's3://bucket/staging/abc123/operacoes/manifest'
IAM_ROLE 'arn:aws:iam::123456789012:role/papel'
FORMAT AS PARQUET
MANIFEST;

SELECT pg_last_copy_count();
```

Regras da documentação para `COPY` de formatos colunares:

- Mapeamento por posição. `COPY` insere os valores nas colunas da tabela na ordem em que as colunas
  aparecem no arquivo, e o número de colunas do arquivo e da tabela precisa ser igual. A
  documentação de mapeamento de colunas aceita uma lista de colunas no comando, mas, para arquivos
  planos, exige que ela siga a ordem do arquivo; a documentação de formatos colunares não menciona
  a lista. A prova de conceito de 2026-09-21 a aceitou com Parquet: um arquivo de cinco colunas
  entrou numa tabela de seis, com a sexta nula, e sem a lista o `COPY` reprova com
  `Unmatched number of columns` ([Regras do COPY para Parquet](#regras-do-copy-para-parquet)).
- Só estas opções: `ACCEPTINVCHARS`, `FILLRECORD`, `FROM`, `IAM_ROLE`, `STATUPDATE`, `MANIFEST`,
  `EXPLICIT_IDS`. `MAXERROR`, `IGNOREALLERRORS`, `ACCEPTANYDATE` e `REGION` não são aceitos.
  `FILLRECORD` carregou um arquivo de cinco colunas numa tabela de seis com a sexta nula
  (2026-09-21).
- O primeiro erro aborta o comando. Os erros aparecem no cliente e em `STL_LOAD_ERRORS` e
  `SYS_LOAD_ERROR_DETAIL` (`file_name`, `column_name`, `column_type`, `error_message`). `NOLOAD`
  valida os arquivos sem carregar.
- Só o conteúdo dos arquivos é carregado; colunas de partição em nomes de diretório não existem para
  o `COPY`.
- O bucket precisa estar na mesma região; a carga usa o Spectrum e URLs pré-assinadas válidas por 1
  hora, que políticas de bucket não podem bloquear (`s3:signatureAge` de pelo menos 3.600.000 ms).
- Extensões `.gz`, `.snappy` e `.bz2` são descomprimidas sem parâmetro. `COPY` não aplica compressão
  de coluna automaticamente (`COMPUPDATE` controla).
- O manifesto de Parquet exige `meta.content_length` em cada entrada, e `mandatory: true` faz o
  comando falhar quando o arquivo falta. O `UNLOAD ... MANIFEST` gera um manifesto compatível.
- `STATUPDATE ON` atualiza as estatísticas do otimizador depois da carga; por padrão isso só ocorre
  em tabela vazia.

O manifesto sai da lista de arquivos do snapshot Delta ([Exportação para
Parquet](#exportacao-para-parquet) do Delta Lake):

```python
# O manifesto do COPY sai da lista de arquivos do snapshot Delta; content_length é obrigatório em cada entrada.
import json
import pyarrow as pa
from deltalake import DeltaTable

# As credenciais de storage_options seguem "Requisitos do S3", na seção Delta Lake.
dt = DeltaTable("s3://bucket/operacoes/", storage_options=s3_options)
actions = pa.table(dt.get_add_actions(flatten=True)).to_pylist()  # get_add_actions devolve uma tabela arro3
root = dt.table_uri.rstrip("/")
manifest = {"entries": [
    {"url": f"{root}/{action['path']}", "mandatory": True, "meta": {"content_length": action["size_bytes"]}}
    for action in actions if action["partition.mes"] == "2026-08"
]}
json.dumps(manifest)  # vai para s3://bucket/staging/abc123/operacoes/manifest
```

Sobre uma tabela local com um arquivo de agosto registrado, o manifesto saiu com uma entrada,
`mandatory: true` e `content_length` igual ao `size` da ação `add`; nada rodou contra o S3 nem contra
o Redshift.

O esquema definido é o DDL da tabela de destino (`CREATE TABLE` com tipos, `NOT NULL` e as chaves
informativas). O que o Redshift garante e o que não garante:

| Verificação | Comportamento |
| --- | --- |
| Tipo do Parquet compatível com a coluna | Exigido. A documentação não publica a tabela de correspondência entre tipos Parquet e tipos Redshift; os tipos físicos de `TIMESTAMP` em `INT64` de microssegundos e de `DECIMAL(18, 2)` em `INT64` carregaram no ambiente alvo em 2026-09-21 ([Redshift](#redshift)); `FIXED_LEN_BYTE_ARRAY` fica sem leitura. |
| Número e ordem das colunas | Exigidos, por posição. |
| `NOT NULL` | Aplicado; um `NULL` em coluna `NOT NULL` falha o comando. |
| Comprimento de `VARCHAR` | Em bytes. Um valor maior que a coluna aborta o `COPY` (`Spectrum Scan Error` 15007, `The length of the data column ... is longer than the length defined in the table`, 2026-09-21); `TRUNCATECOLUMNS` não é aceito com Parquet (`0A000`, `TRUNCATECOLUMNS argument is not supported for PARQUET based COPY`, 2026-09-21). |
| `PRIMARY KEY`, `UNIQUE`, `FOREIGN KEY` | Informativas; não são verificadas. Duplicatas entram e depois produzem resultados errados no planejador ([Chaves, restrições e índices](#chaves-restricoes-e-indices)). |

A carga com verificação segue a [Ingestão de dados](#ingestao-de-dados-3) do Redshift: Arrow com o
esquema do modelo, Parquet
numa área de staging, `COPY` numa tabela do sandbox da execução, `pg_last_copy_count()` contra a
contagem enviada e auditoria de unicidade por consulta antes de publicar.

#### UNLOAD para Parquet

```sql
UNLOAD ('SELECT id_operacao, data_ref, id_cliente, valor::DECIMAL(18, 2) AS valor, descricao
         FROM execucao_abc123.operacoes
         WHERE data_ref >= ''2026-08-01'' AND data_ref < ''2026-09-01''
         ORDER BY data_ref, id_operacao')
TO 's3://bucket/operacoes/data/2026-08/abc123_'
IAM_ROLE 'arn:aws:iam::123456789012:role/papel'
FORMAT AS PARQUET
MAXFILESIZE 256 MB
ROWGROUPSIZE 128 MB
MANIFEST VERBOSE;
```

Regras da documentação:

- O arquivo sai em Parquet versão 1.0, com cada row group comprimido em Snappy. Não há opção de
  codec para Parquet (`GZIP`, `BZIP2` e `ZSTD` são para texto), nem `HEADER`, `DELIMITER`, `NULL AS`.
- `ROWGROUPSIZE` de 32 MB (padrão) a 128 MB, só em `ra3.4xlarge`, `ra3.16xlarge`, `rg.4xlarge`,
  `rg.12xlarge` e `dc2.8xlarge`. `MAXFILESIZE` de 5 MB a 6,2 GB (padrão 6,2 GB).
- `PARALLEL ON` (padrão) grava um ou mais arquivos por slice, com sufixo `<slice>_part_<parte>`;
  `PARALLEL OFF` grava em série, ordenado pelo `ORDER BY`, em arquivos de até 6,2 GB.
- `PARTITION BY (coluna)` cria diretórios Hive e retira a coluna dos arquivos; `INCLUDE` a mantém.
  Literais não são aceitos em `PARTITION BY`.
- `MANIFEST VERBOSE` grava, além das URLs, `content_length` e `record_count` por arquivo, a seção
  `schema` com nome, tipo base e dimensões de cada coluna (comprimento de `VARCHAR`, precisão e
  escala de `DECIMAL`) e o total em `meta`. Essa seção é o esquema que o Redshift exportou, e a
  biblioteca pode compará-la com o modelo antes de publicar.
- `ALLOWOVERWRITE` sobrescreve; `CLEANPATH` apaga de forma permanente os arquivos do destino (só das
  partições que recebem dados, com `PARTITION BY`) e exige `s3:DeleteObject`. Os dois são
  mutuamente exclusivos.
- O `SELECT` externo não aceita `LIMIT`; aspas dentro da consulta são duplicadas.
- `ENCRYPTED` com Parquet só com SSE-KMS. `EXTENSION` acrescenta um sufixo sem validação.
- Ponto flutuante pode perder precisão em descarga e recarga sucessivas. `GEOMETRY`, `HLLSKETCH` e
  `VARBYTE` só saem em texto ou CSV. A perda do fuso em `TIMESTAMPTZ` está registrada na seção
  [Redshift](#redshift).

O esquema definido entra pela consulta: colunas na ordem do modelo, casts para os tipos do contrato
e `ORDER BY` pela chave de ordenação. A documentação do `UNLOAD` não menciona `field_id`, metadados
chave-valor nem `sorting_columns`, não informa os tipos físicos gerados nem a presença de
estatísticas min/max; o rodapé de um arquivo lido no ambiente alvo em 2026-09-21 responde os três
([Exportação para Parquet](#exportacao-para-parquet-3) do Redshift). A verificação depois da
descarga lê o manifesto e o rodapé de cada arquivo com PyArrow ou DuckDB (`parquet_schema` e
`parquet_metadata`) e compara com o modelo.

#### Tabelas externas do Redshift Spectrum

```sql
CREATE EXTERNAL SCHEMA lake
FROM DATA CATALOG DATABASE 'projeto'
IAM_ROLE 'arn:aws:iam::123456789012:role/papel';

CREATE EXTERNAL TABLE lake.operacoes (
    id_operacao BIGINT,
    data_ref    DATE,
    id_cliente  BIGINT,
    valor       DECIMAL(18, 2),
    descricao   VARCHAR(200)
)
PARTITIONED BY (mes CHAR(7))
STORED AS PARQUET
LOCATION 's3://bucket/operacoes/'
TABLE PROPERTIES ('numRows' = '300000');

ALTER TABLE lake.operacoes ADD IF NOT EXISTS
    PARTITION (mes = '2026-08') LOCATION 's3://bucket/operacoes/mes=2026-08/';
```

Regras da documentação:

- O tipo de cada coluna do DDL precisa casar com o tipo embutido no Parquet. Uma diferença dá
  `Spectrum Scan Error ... has an incompatible Parquet schema for column ...`, com a mensagem
  completa em `SVL_S3LOG`; a correção é alterar a tabela externa. Colunas mapeadas por nome no
  Delta Lake e no Hudi, e por nome por padrão no ORC (`orc.schema.resolution`); a documentação não
  declara a regra para Parquet puro, e a mensagem de erro cita a coluna pelo nome. Não verificado no
  ambiente alvo; a biblioteca não cria tabelas externas.
- Tipos aceitos: `SMALLINT`, `INTEGER`, `BIGINT`, `DECIMAL`, `REAL`, `DOUBLE PRECISION`, `BOOLEAN`,
  `CHAR`, `VARCHAR`, `VARBYTE` (Parquet e ORC, sem partição), `DATE` (texto, Parquet, ORC ou
  partição), `TIMESTAMP`. `VARCHAR` é em bytes, e o resultado é truncado ao tamanho da coluna sem
  erro.
- A chave de partição não pode ser coluna da tabela, e cada partição é registrada com `ALTER TABLE
  ... ADD PARTITION` (até 100 por comando com o Glue) ou já existe no catálogo Glue. `CREATE
  EXTERNAL TABLE ... AS` e `INSERT` em tabela externa registram as partições que gravam.
- `LOCATION` aceita um prefixo (arquivos ocultos e nomes iniciados por `.`, `_` ou `#` são
  ignorados) ou um manifesto com `mandatory` por arquivo.
- O Spectrum documenta suporte ao Parquet v1 e a Snappy, gzip e zstd. Split por row group; arquivos
  de 64 MB a 1 GB de tamanhos parecidos; um diretório por tabela.
- O otimizador não analisa tabelas externas: `numRows` em `TABLE PROPERTIES` ou estatísticas de
  coluna do Glue evitam o plano padrão, que supõe a tabela externa como a maior. As pseudocolunas
  `$path` e `$size` mostram de onde cada linha veio.
- Não se cria tabela externa dentro de transação, e as permissões são por `USAGE` no esquema externo.

Os dados do Spectrum não passam pelas verificações de `NOT NULL` nem de tipos na leitura além da
compatibilidade acima; a tabela externa é um contrato de leitura. Para publicar os dados com o
esquema verificado, o caminho é o `COPY` da seção anterior, com o manifesto montado a partir dos arquivos
da tabela Delta ([Delta Lake](#delta-lake)).

## Delta Lake

O Delta Lake é a fonte da verdade do banco: cada tabela é uma pasta de arquivos Parquet mais um log
de transações, no S3 do projeto ou em disco local. A biblioteca grava e lê pelo pacote `deltalake`
(delta-rs, Rust com bindings Python), o DuckDB lê e anexa pela extensão `delta`, e o Redshift entra
por `COPY` e `UNLOAD` sobre os arquivos que o log lista. As verificações iniciais desta seção
rodaram em 2026-09-19 com Python 3.13, deltalake 1.6.4, DuckDB 1.5.5, PyArrow 25.0.1, num macOS
arm64 com 11 threads e disco local, sem S3 nem Redshift; as leituras feitas depois no ambiente
alvo trazem a própria data.

### Comandos utilitários para diagnóstico

| Comando | O que mostra |
| --- | --- |
| `DeltaTable(uri).version()` | Versão atual da tabela. |
| `DeltaTable(uri).history(n)` | Os `n` últimos commits: operação, parâmetros (`mode`, `predicate`, `partitionBy`), métricas e os metadados gravados pela biblioteca. |
| `DeltaTable(uri).schema().to_json()` | Esquema em JSON, com nulidade e metadados de coluna. |
| `DeltaTable(uri).metadata()` | `id`, nome, descrição, colunas de partição e propriedades (`configuration`). |
| `DeltaTable(uri).protocol()` | Versões mínimas de leitor e escritor e os recursos habilitados. |
| `DeltaTable(uri).file_uris(file_pruning_predicate="mes = '2026-08'")` | Arquivos do snapshot atual que sobrevivem ao predicado. |
| `pa.table(DeltaTable(uri).get_add_actions(flatten=True))` | Uma linha por arquivo: `path`, `size_bytes`, `num_records`, `partition.<coluna>`, `min.<coluna>`, `max.<coluna>`, `null_count.<coluna>`. |
| `DeltaTable(uri).transaction_version("pipeline")` | Última versão de aplicação registrada por esse `app_id`. |
| `DESCRIBE SELECT * FROM delta_scan('uri')` (DuckDB) | Colunas e tipos como o DuckDB os vê. |
| `EXPLAIN ANALYZE SELECT ... FROM delta_scan('uri') WHERE ...` (DuckDB) | `Scanning Files: k/n` e `Total Files Read`, que medem a poda. |
| `_delta_log/_last_checkpoint` | Versão do último checkpoint, `{"version":4,"size":6,"sizeInBytes":16115,"numOfAddFiles":3}` no teste. |

### Organização dos dados

#### Estrutura da pasta de uma tabela

A pasta da tabela contém os arquivos de dados, em subpastas Hive quando há partição, e a pasta
`_delta_log/`. Uma tabela particionada por `mes`, criada, carregada com dois meses e com fevereiro
substituído, ficou assim:

```text
cad_operacoes/
├── _delta_log/
│   ├── 00000000000000000000.json                     2.509 bytes   CREATE TABLE: protocol, metaData
│   ├── 00000000000000000001.json                     1.004 bytes   WRITE append: add
│   ├── 00000000000000000002.json                     1.010 bytes   WRITE append: add
│   ├── 00000000000000000003.json                     1.277 bytes   WRITE overwrite com predicado: remove, add
│   ├── 00000000000000000004.checkpoint.parquet                     estado consolidado até a versão 4
│   └── _last_checkpoint                                             {"version":4,...}
├── mes=2026-01/part-00000-ff55d0c4-...-c000.snappy.parquet    1.829.809 bytes
├── mes=2026-02/part-00000-e99418ef-...-c000.snappy.parquet    1.829.796 bytes   removido na versão 3
└── mes=2026-02/part-00000-41617a94-...-c000.snappy.parquet    2.173.827 bytes
```

Cada commit é um arquivo JSON nomeado pela versão com 20 dígitos, com uma ação por linha:

| Ação | Conteúdo | Quando aparece |
| --- | --- | --- |
| `commitInfo` | `timestamp`, `operation` (`CREATE TABLE`, `WRITE`, `UPDATE`, `DELETE`, `MERGE`, `ADD COLUMN`, `CHANGE COLUMN`, `ADD CONSTRAINT`, `RESTORE`), `operationParameters` (`mode`, `predicate`, `partitionBy`), `operationMetrics`, `engineInfo` (`delta-rs:py-1.6.4`) e os metadados personalizados do commit. | Em todo commit; informativo. |
| `protocol` | `minReaderVersion`, `minWriterVersion` e, a partir de 3 e 7, as listas `readerFeatures` e `writerFeatures`. | Na criação e quando um recurso é habilitado. |
| `metaData` | `id` da tabela, `name`, `description`, `format` (`parquet`), `schemaString`, `partitionColumns`, `createdTime`, `configuration`. | Na criação e em cada mudança de esquema ou propriedade. |
| `add` | `path` relativo à pasta da tabela, `partitionValues`, `size`, `modificationTime`, `dataChange` e `stats` em JSON (`numRecords`, `minValues`, `maxValues`, `nullCount`). | Em cada arquivo que entra. |
| `remove` | `path`, `deletionTimestamp`, `partitionValues`, `size`. | Em cada arquivo que sai; o arquivo físico permanece até o `vacuum`. |
| `txn` | `appId`, `version`, `lastUpdated`. | Quando a biblioteca registra uma transação de aplicação. |

```python
import json, pathlib
from deltalake import DeltaTable

# Ações de um commit lidas do log; no S3, history() e get_add_actions() dão o mesmo sem listar arquivos.
def commit_actions(root: str, version: int) -> list[str]:
    path = pathlib.Path(root, "_delta_log", f"{version:020d}.json")
    return [next(iter(json.loads(row))) for row in path.read_text().splitlines()]

commit_actions("cad_operacoes", 3)                 # ['commitInfo', 'remove', 'add']
DeltaTable("cad_operacoes").history(1)[0]["operationParameters"]   # {'mode': 'Overwrite', 'predicate': "mes = '2026-02'", ...}
```

O arquivo que sai numa substituição não é apagado: a ação `remove` o retira do snapshot, e a viagem
no tempo continua a enxergá-lo até o `vacuum`. Os valores de partição ficam na ação `add`, não dentro
do arquivo de dados: o Parquet gravado pelo delta-rs para `mes=2026-02` tem cinco colunas, sem `mes`.

O valor de partição entra no nome da pasta codificado por porcentagem (`p=a%3Ab` para `a:b`,
`p=d%27agua` para `d'agua`, `p=a%C3%A7%C3%A3o` para `ação`), e o `path` da ação `add` codifica a
pasta de novo (`p=a%253Ab`). O valor só de letras, dígitos, `_`, `.` e `-` sai igual nos dois. A
regra da partição de `serialize_db.schema.check_partition_value` aceita só esses caracteres,
porque o `COPY` do modo `register` monta o nome da pasta com o valor sem codificar (deltalake
1.6.4, 2026-09-23,
`test_deltalake.py::test_partition_value_is_percent_encoded_in_the_folder_and_the_log`).

#### Log, snapshot e checkpoint

O snapshot de uma versão é o resultado de reproduzir as ações do log em ordem: o conjunto de `add`
sem `remove` posterior, o último `metaData` e o último `protocol`. Para não reler o log inteiro, um
checkpoint em Parquet consolida o estado numa versão; `_last_checkpoint` aponta para ele, e o leitor
lê só os JSON posteriores. O delta-rs grava o checkpoint a cada `delta.checkpointInterval` commits
(com o valor 5, o checkpoint apareceu na versão 4, a quinta) e sob demanda por
`DeltaTable.create_checkpoint()`. `cleanup_metadata()` apaga arquivos de log anteriores ao último
checkpoint e mais velhos que `delta.logRetentionDuration`.

```python
import json, pathlib
from deltalake import DeltaTable

dt = DeltaTable("cad_operacoes")
dt.create_checkpoint()                       # checkpoint da versão atual, fora do intervalo automático
dt.cleanup_metadata()                        # só apaga o que delta.logRetentionDuration permite
json.loads(pathlib.Path("cad_operacoes/_delta_log/_last_checkpoint").read_text())
# {'version': 6, 'size': 8, 'sizeInBytes': ..., 'numOfAddFiles': 5}
```

Nesta página, "snapshot da tabela" é esse estado de uma versão, e "snapshot do banco" é o conjunto
`{tabela: versão}` que a biblioteca registra fora do log. O que a biblioteca guarda por conta
própria são os metadados de cada commit (`serialize_db.delta.commit_metadata`),
`_serialize_db/snapshots.json` (`serialize_db.delta.snapshot`) e a tabela
`serialize_db_publications` (`serialize_db.publication`).

#### Diferenças para o PostgreSQL

- Não há atualização no lugar. Toda escrita cria arquivos novos e um commit; `UPDATE`, `DELETE` e
  `MERGE` reescrevem os arquivos que contêm as linhas afetadas (no teste, um `UPDATE` de uma linha
  reescreveu um arquivo de dez linhas: `num_added_files: 1, num_removed_files: 1, num_copied_rows: 9`).
- Não há índices, chaves primárias, únicas nem estrangeiras. O que existe é `NOT NULL` e `CHECK`,
  aplicados pelo escritor, e as estatísticas por arquivo, que fazem o papel do índice na leitura.
- Não há banco nem schema como conjunto de tabelas. O esquema do Delta é a lista de colunas de uma
  tabela, na ação `metaData` do log dela, e o formato não sabe quais tabelas formam o banco. A pasta
  com uma subpasta por tabela agrupa os arquivos, e o `MetaData` do SQLAlchemy é o único lugar que
  declara o conjunto e as relações entre as tabelas, por isso é o contrato. Nesta página, "esquema
  da tabela" é o das colunas e "contrato de esquema" é o do conjunto; no DuckDB e no Redshift, schema
  é o namespace que agrupa tabelas.
- Não há transação entre tabelas. Cada tabela tem o próprio log, e um commit é atômico numa tabela.
  Uma execução que publica várias tabelas faz um commit por tabela. A biblioteca fixa a versão de
  cada tabela lida no início, grava `serialize_db_execution_id` e essas versões
  (`serialize_db_input_versions`) em cada commit, e quem precisa de um estado coerente entre
  tabelas lê esse conjunto de versões, não a última de cada uma.
- Não há sessão nem bloqueio. O controle de concorrência é otimista: o commit falha se outro escritor
  mudou o que a transação leu, e cabe ao escritor refazer a operação.
- O esquema está no log, versionado junto com os dados. Uma leitura de versão antiga usa o esquema
  daquela versão.
- Uma leitura vê um snapshot fixo enquanto a `DeltaTable` carregada existir; commits posteriores só
  aparecem depois de recarregar. No DuckDB, `ATTACH ... (PIN_SNAPSHOT true)` fixa a versão.

#### Efeitos nas formas de manipular os dados

O desenho da biblioteca, partições imutáveis substituídas por inteiro, é o uso natural do
formato: `write_deltalake(mode="overwrite", predicate="mes = '2026-08'")` troca os arquivos do mês
num commit. Operações linha a linha existem (`update`, `delete`, `merge`) e servem para correções
pontuais, mas cada uma reescreve arquivos inteiros e cria uma versão; um pipeline que as usasse por
linha produziria milhares de commits e de arquivos pequenos. Os dados entram e saem em Arrow, em
lotes; o DuckDB lê a tabela no lugar e serve de motor de consulta e de cálculo.

### Delta Lake e Iceberg

Os dois são formatos de tabela sobre Parquet, com os mesmos objetivos: lista de arquivos por versão,
commit atômico, esquema versionado, partição, estatísticas e histórico. A diferença que decide este
projeto é onde mora o ponteiro para a versão atual.

No Delta, o ponteiro é implícito: a versão atual é o maior número de commit em `_delta_log/`, e um
commit é criar o próximo arquivo com put-if-absent. O protocolo exige do armazenamento só isso, e o
S3 oferece desde 2024-08 (`If-None-Match: *`). Nada além da pasta é necessário para ler ou escrever;
um catálogo (Glue, Hive, Unity) serve só para dar nome à tabela.

No Iceberg, cada commit gera um novo `metadata.json`, e o ponteiro para o atual precisa ser trocado
com compare-and-swap fora dos arquivos. A especificação delega essa troca ao catálogo (REST, Glue,
Hive, um banco SQL), e por isso o Iceberg exige um serviço, ou um arquivo de catálogo que a biblioteca
mova. O catálogo Hadoop, baseado só em arquivos, depende
de renomear de forma atômica, o que o S3 não tem.

| Aspecto | Delta Lake | Iceberg |
| --- | --- | --- |
| Metadados | Log JSON por commit e checkpoints Parquet, na pasta da tabela. | `metadata.json` por commit, manifest lists e manifests em Avro, na pasta da tabela, mais o ponteiro no catálogo. |
| Exigência do armazenamento | Put-if-absent na criação do arquivo de versão. | Compare-and-swap do ponteiro, no catálogo. |
| Caminhos dos arquivos nos metadados | Relativos à pasta da tabela. | Absolutos (`file:/...` e `s3://...` nos manifests). |
| Partição | Coluna explícita da tabela, diretórios Hive, valor na ação `add`; a coluna não fica no arquivo. | Oculta, por transformação (`month(data_ref)`); a coluna de origem fica no arquivo; a especificação de partição evolui. |
| Evolução de esquema | Adicionar coluna; renomear e remover exigem o recurso column mapping, cuja escrita no delta-rs está incompleta; tipo só por reescrita. | Completa por `field_id`: adicionar, renomear, remover, reordenar, promover tipo. |
| Estatísticas por arquivo | JSON na ação `add`. | Colunas dos manifests Avro. |
| Exclusão por linha | Vetores de exclusão (recurso opcional). | Delete files por posição ou igualdade. |
| Concorrência | Otimista, por conflito no log. | Otimista, por conflito no catálogo. |
| Conjunto de tabelas e transação entre tabelas | Nenhum no formato: o esquema é de uma tabela, na ação `metaData`, e o commit é de uma tabela; agrupar é papel de um catálogo externo, ou da pasta e do `MetaData` do SQLAlchemy. | Esquema por tabela no `metadata.json`; o catálogo, que a especificação exige, tem namespaces de vários níveis, e o catálogo REST commita várias tabelas de uma vez em `POST /v1/{prefix}/transactions/commit`. |
| Escritor Python | `deltalake` (delta-rs). | PyIceberg, com o extra `pyiceberg-core` para transformações de partição. |
| DuckDB | Leitura, poda, viagem no tempo e `INSERT INTO`. | Leitura pelo caminho do `metadata.json`; escrita só com catálogo REST. |
| Redshift | Só por `COPY` dos arquivos, ou Spectrum via manifesto simbólico e Glue. | Só por `COPY` dos arquivos, ou Spectrum via Glue e Lake Formation. |
| Serviços da AWS | Athena lê; Glue cataloga por crawler; S3 Tables não. | Glue, Athena, Redshift Spectrum, S3 Tables e Lake Formation são nativos. |
| Conversão entre os dois | Apache XTable converte metadados nos dois sentidos; o Databricks tem o UniForm. | O mesmo. |

### Implementações do protocolo

O Delta Lake é um protocolo, o `PROTOCOL.md` do repositório `delta-io/delta`, e três implementações
interessam ao projeto. Uma tabela gravada por uma delas é lida pelas outras enquanto as table features
habilitadas estiverem no suporte de cada leitor.

| Implementação | O que é | Papel no projeto |
| --- | --- | --- |
| `delta-spark`, importado como `delta` | Implementação de referência, em Scala sobre a JVM, no repositório `delta-io/delta`. A versão 4.4.0 é de 2026-08-20 e exige `pyspark` e uma `SparkSession`. O tutorial "Getting started" do delta.io usa `configure_spark_with_delta_pip` e `delta.tables`: os exemplos não rodam sem Spark; as tabelas que eles produzem são lidas por qualquer implementação. | Nenhum: o Spark está excluído do projeto. |
| `deltalake`, o delta-rs | Reimplementação nativa do protocolo em Rust com bindings Python, sem JVM, no repositório `delta-io/delta-rs`, da mesma organização. O site delta.io a apresenta no artigo "Delta Lake without Spark" como o caminho para pandas, Polars, DuckDB, Dask, Daft e DataFusion, e a lista em Integrations como "Delta Rust API". | Escritor da biblioteca: log, commits, registro de arquivos e manutenção. |
| Delta Kernel, o `delta-kernel-rs` | Biblioteca em Rust e C do projeto Delta para conectores lerem e gravarem sem reimplementar o protocolo. A extensão `delta` do DuckDB é construída sobre ela, com leitura e append cego. O delta-rs também depende de um kernel, mas do fork `buoyant_kernel`, fixado por revisão em `buoyant-data/delta-kernel-rs` no `main` de 2026-09-19. | Leitor do DuckDB, por `delta_scan` e `ATTACH`. |

O delta-spark recebe os recursos novos do protocolo primeiro. A tabela compara o que o delta-rs 1.6.4
faz, conferido em 2026-09-19 na tabela de recursos da documentação e nos issues do repositório, com o
efeito no pipeline:

| Recurso do protocolo | delta-spark | delta-rs | Efeito no pipeline |
| --- | --- | --- | --- |
| Vetores de exclusão (`deletionVectors`) | Grava e lê. | Lê; a gravação é o issue 4512, aberto. `delete`, `update` e `merge` reescrevem os arquivos atingidos. | O `COPY` do Redshift lê os arquivos sem o log e ignoraria as exclusões; o pipeline não habilita o recurso com nenhuma biblioteca. A substituição do mês inteiro já é copy-on-write. |
| Column mapping (`columnMapping`) | Renomeia e remove colunas sem reescrever. | A tabela de recursos marca o escritor v5, mas não há `rename_column`, `drop_columns` está no PR 4732, aberto, e habilitar o recurso pela escrita é o issue 3936, aberto. | Os nomes físicos das colunas viram `col-<uuid>` nos arquivos Parquet e quebrariam o `COPY`; renomear e remover é reescrever a tabela, aceito porque são raros. |
| Identity columns (`identityColumns`) | Sim. | Não. | As chaves de negócio são geradas no cliente. |
| Generated columns (`generatedColumns`) | Sim. | Sim. | `mes` é calculado antes da gravação. |
| Change data feed, `CHECK` constraints, invariantes, append-only, `timestampNtz` | Sim. | Sim. | `timestampNtz` e as constraints estão em uso; o change data feed não. |
| Clustering (`clustering`), row tracking, in-commit timestamps, checkpoint V2, UniForm (`icebergCompatV1` e `icebergCompatV2`), `catalogManaged`, `allowColumnDefaults` | Sim. | Ausentes da tabela de recursos. | Não usados: a partição mensal e `optimize.z_order` cobrem a organização física. |
| `GENERATE symlink_format_manifest`, o manifesto que o Spectrum lê | Sim. | Não consta das operações. | O manifesto do `COPY` é construído de `get_add_actions()`, na seção de Redshift. |
| `merge`, `update` e `delete` | Distribuídos no cluster. | Num único processo, por DataFusion. | O DML roda no DuckDB ou no Redshift; o delta-rs registra os arquivos resultantes. |
| Escrita concorrente no S3 | Um único driver Spark, ou o `S3DynamoDBLogStore` para mais de um cluster, segundo a documentação de armazenamento. | Put condicional do S3 desde a 1.6.0, sem DynamoDB. | O delta-rs está à frente; é a primitiva da seção de transações. |

Sem Spark o pipeline não perde recurso nem velocidade. No desenho da biblioteca o delta-rs só lê e
grava o log e registra arquivos; o cálculo é do DuckDB ou do Redshift, e a seção de performance mede
a leitura por `delta_scan` a milissegundos da leitura direta do Parquet. O Spark ganharia só ao
distribuir o cálculo num cluster, e o caso dos dados maiores que a máquina é atendido ingerindo só as
partições necessárias ou executando no Redshift. O risco fica no caminho de escrita: um escritor Spark
ou Databricks que habilite vetores de exclusão nas mesmas tabelas invalida o `COPY` do Redshift e
expõe o delta-rs aos bugs abertos de leitura desse recurso, os issues 4613 e 4657. Por isso a
biblioteca é o único escritor.

### Requisitos do S3

O Delta exige do S3 o que o protocolo exige de qualquer armazenamento: listar e ler a pasta da
tabela, criar objetos com put-if-absent e, na manutenção, apagar. O resto é configuração do bucket e
dos papéis.

Layout: um prefixo por ambiente e por tabela, `s3://<bucket>/<caminho do projeto>/delta/<ambiente>/<tabela>/`.
Só a biblioteca escreve sob o prefixo de uma tabela e sob `_serialize_db/`, o controle da biblioteca
na raiz do ambiente; o staging do `COPY`, os manifestos de publicação e as cópias de snapshots do
banco ficam em prefixos próprios (`staging/`, `publicacao/`, `arquivo/`).

Permissões do papel que executa o pipeline (delta-rs, DuckDB e `boto3` usam o mesmo):

| Ação IAM | Uso |
| --- | --- |
| `s3:ListBucket`, com `s3:prefix` restrito ao caminho do projeto | Listar `_delta_log/` para achar a versão atual e os checkpoints; listar os dados no `vacuum` com `full=True`. |
| `s3:GetObject` | Ler log, checkpoints e arquivos de dados. |
| `s3:PutObject` | O commit é um `PutObject` com `If-None-Match: *`; arquivos de dados, checkpoints e `_last_checkpoint` são `PutObject` comuns. Arquivos grandes sobem por multipart, que usa a mesma ação mais `s3:AbortMultipartUpload` para a limpeza. |
| `s3:DeleteObject` | `vacuum`, `cleanup_metadata` e a remoção do staging. O `object_store` apaga em lote (`DeleteObjects`); `aws_disable_bulk_delete` desliga. |
| `kms:Decrypt`, `kms:Encrypt`, `kms:GenerateDataKey` | Só quando o bucket usa SSE-KMS. |

O papel do Redshift precisa de `s3:GetObject` e `s3:ListBucket` para o `COPY` (manifesto e arquivos) e
de `s3:PutObject` para o `UNLOAD` no prefixo da tabela. A política do bucket não pode bloquear as
URLs pré-assinadas do `COPY` (`s3:signatureAge` de pelo menos 3.600.000 ms, em
[Parquet](#parquet)), e o bucket fica na região do namespace do Redshift.

Escrita condicional: nenhuma ação IAM adicional. O `object_store` usa `aws_conditional_put` igual a
`etag` por padrão, e o S3 aceita `If-None-Match` e `If-Match` em `PutObject` e
`CompleteMultipartUpload`. Uma política de bucket pode exigir o cabeçalho com a chave de condição
`s3:if-none-match` (`"Null": {"s3:if-none-match": "false"}`), com a exceção
`s3:ObjectCreationOperation` para as etapas do multipart, que não aceitam cabeçalhos condicionais.
Com essa política, `CopyObject` para o prefixo falha (403 sem o cabeçalho, 501 com ele). Ela é
opcional e só faz sentido restrita a `*/_delta_log/*`, para barrar escritores que não sejam Delta.

Consistência: o S3 dá leitura e listagem consistentes após a escrita, e o protocolo depende disso
para enxergar o commit recém-criado. Nada a configurar.

Versionamento e Object Lock: não são necessários. Com versionamento ligado, cada arquivo que o
`vacuum` apaga vira versão não corrente e continua cobrando; uma regra de ciclo de vida que expire
versões não correntes resolve. Object Lock em modo de retenção impede o `vacuum` de apagar; o commit
nunca sobrescreve um objeto, então não é afetado.

Ciclo de vida: nenhuma regra de expiração sob os prefixos das tabelas, porque apagar um arquivo
referenciado corrompe a tabela e a viagem no tempo. Abortar multipart incompleto depois de alguns
dias é seguro. Transições de classe de armazenamento só no prefixo `arquivo/`; Intelligent-Tiering
é seguro porque não apaga.

Criptografia: SSE-S3 é transparente. SSE-KMS exige as permissões de KMS nos dois papéis e, quando a
política do bucket exige uma chave específica, as opções `aws_server_side_encryption` (`AES256`,
`aws:kms`, `aws:kms:dsse`), `aws_sse_kms_key_id` e `aws_sse_bucket_key_enabled` em
`storage_options`; no DuckDB a chave vai na opção `KMS_KEY_ID` do secret S3 (não verificado). Quando a
chave é apenas o padrão do bucket, nenhuma opção é necessária: no bucket do projeto (SSE-KMS com
bucket key) o delta-rs, o DuckDB e o `boto3` gravaram e leram sem configuração, e cada objeto saiu
com a chave do projeto.

Região e endpoint: `AWS_REGION` é obrigatória para o delta-rs; `AWS_ENDPOINT_URL` só para serviços
compatíveis. Se o espaço do SageMaker chega ao S3 por um endpoint de VPC e a política do bucket
condiciona `aws:SourceVpce`, os três clientes passam pelo mesmo endpoint; o Redshift usa o próprio
caminho de rede.

Requisições: um `PutObject` por commit em `_delta_log/` e dezenas de `GET` por leitura, longe dos
limites por prefixo. O custo é por requisição, o que reforça arquivos grandes e checkpoints em dia.

Verificado em 2026-09-19 no espaço do SageMaker Unified Studio do projeto, com o papel do projeto
(`datazone_usr_role_...`) e o bucket do projeto, sob o prefixo `dev/` do projeto: `DeltaTable(uri)`
(`ListBucket` e `GetObject`), `write_deltalake` (`PutObject` condicional e multipart),
`vacuum(dry_run=False)` (`DeleteObject`) e `delta_scan` com um secret `credential_chain` no DuckDB.
`GetBucketVersioning` e `ListAllMyBuckets` são negados ao papel, o que não afeta a biblioteca.
`COPY ... MANIFEST` e `UNLOAD` no prefixo da tabela passaram no ambiente alvo em 2026-09-20 e
2026-09-21 com as credenciais de quem chama, porque o namespace do Redshift não tem papel IAM
([O S3 alcançado pelas credenciais de quem chama](#o-s3-alcancado-pelas-credenciais-de-quem-chama)).

Credenciais no SageMaker Unified Studio: o espaço fornece as credenciais do papel do projeto pelo
endpoint de contêiner (`AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`, método `container-role` no `boto3`),
e o espaço sai para a internet por um proxy HTTP (`HTTP_PROXY`, `HTTPS_PROXY` e `no_proxy`, que
lista o endpoint de credenciais e os serviços da AWS). O delta-rs encontra o endpoint de contêiner e
abre a tabela pela cadeia padrão, sem `storage_options`; o aviso `aws_config::profile::credentials`
de uma falha mostra que a cadeia também consulta o perfil `default` de `~/.aws/config`
(`credential_source = EcsContainer`), que aponta para o mesmo endpoint, mas não lê a região dele:
sem `AWS_REGION` nem `AWS_DEFAULT_REGION` a listagem foi a `us-east-1` com `region = us-west-2` no
perfil, e com `HOME` vazio e `AWS_REGION` a tabela abriu (2026-09-20). O cliente HTTP do delta-rs
lê `HTTP_PROXY` e `HTTPS_PROXY` nas duas grafias, mas lê `NO_PROXY` e, só quando ela está ausente,
`no_proxy`: com `NO_PROXY` vazia, a chamada ao endpoint de credenciais vai pelo proxy e falha com
`Non-success status from HTTP credential provider` (`StatusCode(403)`). Foi essa a falha do início
da verificação de 2026-09-19, isolada em 2026-09-20: num shell aberto pela extensão do Claude Code no
Code Editor, `NO_PROXY` existe vazia, e num terminal do Code Editor ela tem a lista de `no_proxy`.
Com `NO_PROXY` ausente, exportada de `no_proxy` ou reduzida a `169.254.170.2`, ou sem as variáveis
de proxy, a chamada passa (em 2026-09-19 também passou com `AWS_CONTAINER_CREDENTIALS_FULL_URI`),
com deltalake 1.5.0 e 1.6.4 e com os dois interpretadores. O teste `test_delta_rs_credential_chain`
em `tests/` registra as cinco variantes no ambiente onde roda, e a biblioteca exporta `NO_PROXY` a
partir de `no_proxy` ao iniciar, quando a maiúscula está ausente ou vazia. As credenciais
temporárias que o `boto3` resolve também abrem a tabela quando passadas em `storage_options`
(`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, `AWS_REGION`;
`test_delta_rs_storage_options_fallback`), mas a biblioteca não as põe lá: a cadeia padrão do
delta-rs as renova na `DeltaTable` que a execução segura, e um trio congelado expiraria em cerca de
uma hora e circularia num dicionário que um log imprime. O DuckDB com `PROVIDER credential_chain`
e o `boto3` nunca falharam.

### Tipos suportados

Tipos primitivos do protocolo: `boolean`, `byte`, `short`, `integer`, `long`, `float`, `double`,
`decimal(p, s)`, `string`, `binary`, `date`, `timestamp`, `timestamp_ntz`; e os compostos `struct`,
`array`, `map` e, nas versões recentes do protocolo, `variant`. Não há inteiros sem sinal, `uuid`,
`json`, `interval` nem `char(n)`/`varchar(n)` com comprimento. A correspondência com o contrato,
verificada gravando um esquema Arrow:

| Arrow | Delta | Parquet físico gravado pelo delta-rs | DuckDB lê como |
| --- | --- | --- | --- |
| `int16` | `short` | `INT32` | `SMALLINT` |
| `int32` | `integer` | `INT32` | `INTEGER` |
| `int64` | `long` | `INT64` | `BIGINT` |
| `bool` | `boolean` | `BOOLEAN` | `BOOLEAN` |
| `float64` | `double` | `DOUBLE` | `DOUBLE` |
| `decimal128(18, 2)` | `decimal(18,2)` | `INT64` | `DECIMAL(18,2)` |
| `string` | `string` | `BYTE_ARRAY` | `VARCHAR` |
| `date32` | `date` | `INT32` | `DATE` |
| `timestamp[us]` | `timestamp_ntz` | `INT64` | `TIMESTAMP` |
| `timestamp[us, tz=UTC]` | `timestamp` | `INT64` | `TIMESTAMP WITH TIME ZONE` |

A tabela completa, com as colunas do SQLAlchemy e do Redshift, é a [tabela de mapeamento de
tipos][tipos].

#### DECIMAL com escala fixa

`decimal(p, s)` aceita precisão até 38. O delta-rs grava `decimal(18, 2)` no tipo físico `INT64`,
como o DuckDB; o PyArrow grava `FIXED_LEN_BYTE_ARRAY`. Um `append` com `decimal(20, 4)` numa coluna
`decimal(18, 2)` é recusado (`SchemaMismatchError: Cannot cast`). O DuckDB lê `DECIMAL(18,2)` e soma
em `DECIMAL(38,2)`.

#### JSON e VARIANT

Não há tipo JSON. Um documento entra como `string`; quando o esquema Arrow traz a extensão
`arrow.json` (`pa.json_(pa.string())`), o campo Delta continua `string` e guarda
`ARROW:extension:name = arrow.json` nos metadados, e `schema().to_arrow()` devolve `string` simples.
O delta-rs grava o arquivo com o tipo lógico `String`; um arquivo do PyArrow ou do DuckDB com o tipo
lógico `JSON`, registrado por `create_write_transaction`, é lido pelos dois leitores na mesma tabela.
O `delta_scan` mostra `VARCHAR`, e `->>`, `json_extract` e `json_valid` funcionam sobre ele. Nem o
Arrow nem o Delta validam o texto: a validação é do `::JSON` do DuckDB na materialização e do
`JSON_PARSE` do Redshift na carga, e a auditoria roda `json_valid` antes de publicar. O tratamento
por camada está na [tabela de mapeamento de tipos][tipos].

```python
import json
import pyarrow as pa
from deltalake import write_deltalake

# Serializa os documentos antes do cast: um dict do pandas viraria struct.
documents = [{"origem": "sistema A", "tags": ["x"]}, None]
column = pa.array([json.dumps(d) if d is not None else None for d in documents], pa.string())
data = pa.table({"id_evento": pa.array([1, 2], pa.int64()), "meta": column.cast(pa.json_(pa.string()))})
write_deltalake("eventos", data, mode="append")

con.sql("SELECT id_evento, meta->>'origem' AS origem, json_valid(meta) AS valido FROM delta_scan('eventos')")
```

O tipo `variant` existe no protocolo recente e o DuckDB o lê; a escrita pelo delta-rs não foi
verificada. Fora do contrato até haver caso de uso.

#### Datas e timestamps

`timestamp` é um instante ajustado a UTC; `timestamp_ntz` é um relógio de parede sem fuso. Um Arrow
`timestamp[us]` sem fuso vira `timestamp_ntz`, e a primeira coluna desse tipo eleva o protocolo da
tabela a leitor 3 e escritor 7 com o recurso `timestampNtz`, que o DuckDB lê. Um `timestamp[ns]` do
pandas é aceito e gravado em microssegundos sem aviso; um fuso `America/Sao_Paulo` é aceito e
gravado como o mesmo instante em UTC. O contrato mantém o cast explícito para microssegundos e UTC
antes de gravar.

### DDL

Não há DDL em SQL. A tabela é criada por `DeltaTable.create`, que grava o commit `CREATE TABLE` com o
esquema, as colunas de partição, o nome, a descrição e as propriedades; `mode="ignore"` repete a
chamada sem criar versão nova, o que torna a criação idempotente. A biblioteca deriva tudo do modelo
SQLAlchemy.

#### Criação da tabela a partir do modelo SQLAlchemy

O `Table` do modelo já produz o esquema Arrow ([SQLAlchemy](#sqlalchemy)), e
`DeltaTable.create` aceita um esquema Arrow diretamente; os metadados de campo do Arrow chegam ao
esquema Delta (um `comment` gravado no Arrow voltou em `DeltaTable.schema()`). As opções físicas
ficam em `Table.info["serialize_db"]` (`serialize_db.schema.TableOptions`):

```python
import pyarrow as pa
import sqlalchemy as sa
from deltalake import DeltaTable
from serialize_db.schema import arrow_schema   # seção SQLAlchemy: tipos, nulidade e PARQUET:field_id

def delta_schema(model) -> pa.Schema:
    """Esquema Arrow do contrato com o comentário de cada coluna nos metadados do campo."""
    return pa.schema([
        field.with_metadata({**field.metadata, "comment": column.comment}) if column.comment else field
        for field, column in zip(arrow_schema(model), model.__table__.columns)
    ])

PROPERTIES = {
    "delta.logRetentionDuration": "interval 3650 days",
    "delta.deletedFileRetentionDuration": "interval 3650 days",
    "delta.checkpointInterval": "10",
}

def create_delta_table(model, uri: str, storage_options: dict[str, str] | None = None) -> DeltaTable:
    table = model.__table__
    options = table.info.get("serialize_db", {})
    dt = DeltaTable.create(
        uri, delta_schema(model), mode="ignore",
        partition_by=options.get("partition_by", []),
        name=table.name, description=table.comment,
        configuration=PROPERTIES, storage_options=storage_options,
    )
    existing = {k.removeprefix("delta.constraints.") for k in dt.metadata().configuration
                  if k.startswith("delta.constraints.")}
    for constraint in table.constraints:
        if isinstance(constraint, sa.CheckConstraint) and constraint.name not in existing:
            dt.alter.add_constraint({constraint.name: str(constraint.sqltext)})
    return dt
```

Regras da derivação:

- A coluna de partição é uma coluna comum do modelo (`mes: Mapped[str] = mapped_column(String(7))`),
  preenchida pela biblioteca a partir de `data_ref` na escrita e conferida na auditoria. O Delta não
  tem partição oculta.
- `PRIMARY KEY`, `UNIQUE`, `FOREIGN KEY` e índices não existem no formato e são ignorados; a
  auditoria por consulta os verifica, como nos outros bancos. `NOT NULL` vira `nullable=False` e é
  aplicado pelo escritor. `CheckConstraint` vira `delta.constraints.<nome>`, avaliada pelo delta-rs
  na escrita; a expressão precisa ser SQL que o delta-rs entenda (`valor >= 0`).
- O comprimento de `String(n)` não existe no Delta; a auditoria de tamanho continua sendo a barreira
  para o `VARCHAR(n)` do Redshift.
- `server_default` do modelo não vira default no Delta; a biblioteca preenche o valor antes de gravar.
- `dt.schema().to_json()` de cada tabela é gravado em `schema/<tabela>.delta.json`, versionado e
  comparado por um teste; a mudança de modelo aparece no diff do PR.

#### Propriedades e protocolo

| Propriedade | Efeito | Valor sugerido |
| --- | --- | --- |
| `delta.logRetentionDuration` | Idade mínima dos arquivos de log que `cleanup_metadata` preserva; limita a viagem no tempo. | `interval 3650 days`, para manter o histórico. |
| `delta.deletedFileRetentionDuration` | Idade mínima que `vacuum` exige antes de apagar um arquivo removido; limita `restore` e a viagem no tempo das versões comuns. | `interval 400 days`; os snapshots do banco são protegidos por `keep_versions`. |
| `delta.checkpointInterval` | Commits entre checkpoints automáticos. | `10`. |
| `delta.appendOnly` | Recusa `delete`, `update` e `overwrite`. | Não usar: a substituição do mês é um `overwrite`. |
| `delta.enableDeletionVectors` | Exclusões por vetor em vez de reescrita. | Não habilitar: os arquivos deixariam de ser carregáveis pelo `COPY`. |
| `delta.columnMapping.mode` | Renomear e remover colunas sem reescrever. | Não habilitar: a escrita no delta-rs está incompleta e o DuckDB não lista o recurso. |

Uma tabela do contrato nasce com protocolo leitor 1 e escritor 2, sobe para leitor 3 e escritor 7 com
`timestampNtz` quando tem `DateTime` sem fuso, e mantém `writer_features` sem column mapping nem
vetores de exclusão. Esse é o envelope que a extensão `delta` do DuckDB lê.

### Evolução de esquema

O esquema muda por um commit de `metaData`, e cada versão lê os arquivos com o esquema daquela
versão: `delta_scan(uri, version := 0)` mostrou seis colunas onde a versão atual mostra sete. Um
arquivo antigo que não tem uma coluna nova é lido com nulo nela; a correspondência é por nome. O que
o delta-rs faz e o que a biblioteca precisa impor:

| Mudança | Como fazer | Comportamento verificado | Regra da biblioteca |
| --- | --- | --- | --- |
| Adicionar coluna anulável | `dt.alter.add_columns([Field(...)])`, commit `ADD COLUMN`, só metadados; ou `write_deltalake(..., schema_mode="merge")` junto com dados. | A coluna entra no fim do esquema; as linhas antigas leem nulo. | Aplicada automaticamente na reconciliação. Um valor para as linhas antigas é um `update` com predicado. |
| Adicionar coluna `NOT NULL` | `add_columns` com `nullable=False`. | Aceito numa tabela com 220.000 linhas; a coluna lê nula em todas, e o `append` seguinte de dados lidos da própria tabela falha com `declared as non-nullable but contains null values`. | Recusada em tabela com dados. |
| Relaxar `NOT NULL` | `dt.alter.drop_column_not_null("coluna")`, commit `CHANGE COLUMN`. | Só metadados. | Aplicada automaticamente. |
| Mudar tipo | `write_deltalake(mode="overwrite", schema_mode="overwrite")` com a tabela inteira. | O `append` converte os dados para o tipo da tabela em vez de mudá-lo: `int32`, `double` e `string` entraram numa coluna `long`. Só a reescrita mudou `long` para `double`. | Só por ordem explícita de reescrita. A verificação de tipos é o cast seguro para o esquema Arrow do contrato, antes de gravar. |
| Renomear ou remover coluna | Reescrita da tabela inteira com o esquema novo, num commit: `write_deltalake(mode="overwrite", schema_mode="overwrite")`, ou `COPY ... PARTITION_BY` do DuckDB mais `create_write_transaction(mode="overwrite", schema=...)`. Só por metadados exigiria column mapping, que o delta-rs não grava (`drop_columns` no PR 4732, aberto). | O commit leva `remove` de todos os arquivos vivos, `add` dos novos e `metaData`; a versão anterior lê com o esquema antigo. Com `predicate` de um mês, o delta-rs aceita e troca o esquema da tabela toda. | Só por ordem explícita de reescrita, sem predicado. |
| Restrição `CHECK` | `add_constraint`, `drop_constraint`. | Commit `ADD CONSTRAINT`; propriedade `delta.constraints.<nome>`. | Aplicada automaticamente. |
| Colunas de partição | Não há alteração; exige recriar a tabela. | | Só por ordem explícita. |
| Recursos de protocolo | `dt.alter.add_feature(...)` ou implícito (`timestampNtz`). | Sobe `minReaderVersion` e `minWriterVersion`. | Só recursos que o DuckDB lê. |

A reescrita que renomeia ou remove coluna foi medida numa tabela de doze meses, `valor` renomeada
para `valor_bruto` e `descricao` removida. O `write_deltalake` alimentado pelo `RecordBatchReader`
de `to_pyarrow_dataset().scanner(columns={...})` cresceu em memória com a tabela. O
`COPY (SELECT id_operacao, data_ref, valor AS valor_bruto, mes FROM delta_scan(uri)) TO uri
(FORMAT parquet, PARTITION_BY (mes), APPEND, FILENAME_PATTERN 'part-{uuid}', RETURN_STATS)` do
DuckDB, registrado por `create_write_transaction(actions, mode="overwrite", schema=novo,
partition_by=["mes"])`, produziu o mesmo commit, `remove` de 12, `add` de 12 e `metaData`, com
memória constante; é o caminho para uma tabela que não cabe na máquina. Os dois leitores leram os
doze meses com `valor_bruto` preenchida, e a versão anterior continuou lendo `valor` e `descricao`.
Os arquivos antigos ficam no disco até o `vacuum` (25 arquivos para 12 no snapshot), e um snapshot
do banco preso por `keep_versions` os mantém.

| Caminho | 12 arquivos, 135 MB, 12.000.000 linhas | 12 arquivos, 269 MB, 24.000.000 linhas |
| --- | --- | --- |
| `write_deltalake(reader, mode="overwrite", schema_mode="overwrite")` | 0,3 s, RSS máximo 1.140 MB | 0,7 s, RSS máximo 1.960 MB |
| `COPY ... (RETURN_STATS)` mais `create_write_transaction` | 0,4 s, RSS máximo 581 MB | 0,6 s, RSS máximo 605 MB |

Reescrever um mês por vez não serve. `write_deltalake(mode="overwrite", schema_mode="overwrite",
predicate="mes = '2026-02'")` foi aceito com um `add`, um `remove` e um `metaData`, e os outros onze
meses passaram a ler `valor_bruto` como nulo no delta-rs e no DuckDB, com os valores ainda dentro
dos arquivos sob o nome antigo; o `restore` da versão anterior desfez. A biblioteca recusa
`predicate` junto com `schema_mode="overwrite"`. O `dt.alter` do delta-rs 1.6.4 oferece
`add_columns`, `add_constraint`, `add_feature`, `drop_column_not_null`, `drop_constraint`,
`set_column_metadata`, `set_table_description`, `set_table_name` e `set_table_properties`; não há
`rename_column` nem `drop_columns`. O nome, a `description` e os comentários de coluna atravessam
todo `write_deltalake(mode="overwrite")`, com e sem predicado, e cada `set_table_description` ou
`set_column_metadata` sai num commit de `commitInfo` e `metaData`, sem tocar nos dados
(2026-09-22): a documentação da tabela sobrevive à publicação de partição e é sincronizada
com o modelo pela reconciliação.

A reconciliação é o comando da biblioteca que substitui a migração: compara `arrow_schema(Table)`
com `dt.schema()`, aplica o diff aditivo, recusa o destrutivo com a instrução de reescrita, e repete
o mesmo diff nas tabelas publicadas no Redshift (`ALTER TABLE ADD COLUMN`, que acrescenta no fim, ou
recriação e recarga). O `COPY` de Parquet liga as colunas por posição, e a biblioteca passa a cada
`COPY` a lista das colunas do rodapé, com um manifesto por lista, que leva cada coluna do arquivo à
de mesmo nome e deixa nula a que um arquivo anterior a uma coluna nova não tem; o `FILLRECORD`,
que carregou no ambiente alvo em 2026-09-21 um arquivo anterior a uma coluna nova com ela nula,
como a lista, segue em todo `COPY` da biblioteca ([Regras do COPY para
Parquet](#regras-do-copy-para-parquet)).

### O que substitui o Alembic

| Recurso do Alembic | Equivalente com o Delta como fonte da verdade |
| --- | --- |
| `revision --autogenerate`, diff entre modelo e banco | Reconciliação: `arrow_schema(Table)` contra `dt.schema()`. |
| `upgrade head` | Aplicação do diff aditivo (`add_columns`, `add_constraint`, `drop_column_not_null`); o destrutivo exige reescrita explícita. |
| `downgrade` | `dt.restore(version)`, que volta esquema e dados juntos, dentro da retenção. |
| Tabela `alembic_version` | A versão do log e o `commitInfo` de cada mudança (`ADD COLUMN`, `CHANGE COLUMN`, `RESTORE`). |
| Scripts revisados no PR | `schema/<tabela>.delta.json` gerado do modelo e versionado; o diff aparece no PR. |
| Migração de dados em SQL | `update` com predicado no Delta, ou reexecução dos meses afetados. |
| DDL das tabelas do Redshift | A mesma reconciliação, sobre tabelas derivadas e reconstruíveis a partir do Delta. |

O Alembic sai por quatro razões. A tabela do lago não tem DDL em SQL a migrar; o esquema é criado do
contrato. As tabelas do sandbox são recriadas a cada execução, e as publicadas no Redshift são
derivadas: um erro se corrige recarregando a partir do Delta, não com um script de downgrade. As
regras que o delta-rs não impõe (coluna `NOT NULL` nova, tipo por cast) precisam viver na biblioteca
de qualquer modo, e o Alembic seria um segundo mecanismo para o mesmo diff. E o suporte do Alembic aos
dois dialetos já era fraco: o DuckDB exige o `DefaultImpl` ([Suporte a
SQLAlchemy](#suporte-a-sqlalchemy-2) do DuckDB), e o Redshift
executa `executemany` linha a linha.

### Transações, commits, conflitos e restauração

Cada operação do delta-rs (`write_deltalake`, `update`, `delete`, `merge`, `alter.*`, `restore`) é
uma transação: grava os arquivos novos, depois tenta criar o próximo arquivo de log. Se o processo
morre antes do commit, os arquivos ficam órfãos, fora do snapshot, e o `vacuum` os remove. Não há
transação aberta entre chamadas nem transação entre tabelas.

Metadados e transações de aplicação:

```python
from deltalake import write_deltalake
from deltalake.transaction import CommitProperties, Transaction

props = CommitProperties(
    custom_metadata={"serialize_db_execution_id": "exec-42",
                     "serialize_db_input_versions": '{"cad_operacoes": 3}'},
    app_transactions=[Transaction(app_id="pipeline", version=42)],
)
write_deltalake(uri, data, mode="overwrite", predicate="mes = '2026-08'", commit_properties=props)

dt = DeltaTable(uri)
dt.history(1)[0]["serialize_db_execution_id"]  # 'exec-42'
dt.transaction_version("pipeline")             # 42
```

Os metadados personalizados aparecem no `commitInfo` e voltam em `history()`. A ação `txn` registra a
última versão de aplicação por `app_id`, mas o delta-rs não recusa uma segunda escrita com a mesma
versão (a repetição foi aceita e duplicou as linhas): a idempotência é da biblioteca, que consulta
`transaction_version` antes de escrever, ou, mais simples, do próprio `overwrite` com predicado, que
substitui o mês em vez de acrescentar.

Concorrência otimista, verificada com duas `DeltaTable` carregadas na mesma versão:

| Escritor 1 | Escritor 2 | Resultado |
| --- | --- | --- |
| `append` | `append` | Os dois commits entram, em versões consecutivas. |
| `overwrite` com `predicate="mes = '2026-05'"` | `overwrite` com o mesmo predicado | O segundo falha: `CommitFailedError: a concurrent transactions added new data. This transaction's query must be rerun`. |
| `overwrite` de `mes = '2026-06'` | `overwrite` de `mes = '2026-07'` | Os dois entram. |

Restauração e histórico:

- `dt.load_as_version(12)` ou `load_as_version(datetime)` carrega um snapshot antigo para leitura;
  no DuckDB, `delta_scan(uri, version := 12)` ou `ATTACH ... (VERSION 12)`.
- `dt.restore(12)` cria um commit `RESTORE` que devolve à tabela os arquivos e o esquema da versão 12;
  a versão nova é `atual + 1`, e nada é apagado. Exige que os arquivos da versão 12 ainda existam;
  `ignore_missing_files=True` restaura o que sobrou.
- `dt.vacuum(retention_hours, dry_run=True)` lista os arquivos removidos há mais tempo que a
  retenção; `dry_run=False` apaga. Uma retenção menor que `delta.deletedFileRetentionDuration` é
  recusada (`minimum retention for vacuum is configured to be greater than 168 hours` numa tabela sem
  a propriedade) a menos que `enforce_retention_duration=False`. Depois do `vacuum`, a viagem no
  tempo e o `restore` deixam de alcançar as versões cujos arquivos saíram, salvo as protegidas por
  `keep_versions`, na seção "Manutenção e retenção".
- `dt.history()` lista os commits; `dt.load_cdf()` lê o change data feed quando a tabela o habilita.

### SELECT, INSERT, UPDATE e DELETE

O delta-rs opera com tabelas Arrow (PyArrow, pandas, Polars pelo PyCapsule) e com predicados em SQL.

```python
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

options = {"AWS_REGION": "sa-east-1"}          # credenciais: ambiente, IMDS, contêiner ou explícitas
uri = "s3://bucket/prd/cad_operacoes"

# INSERT: acrescenta arquivos; o esquema Arrow precisa casar com o da tabela.
write_deltalake(uri, data, mode="append", storage_options=options)

# Substituição do mês: remove os arquivos do predicado e acrescenta os novos, num commit.
write_deltalake(uri, month_data, mode="overwrite", predicate="mes = '2026-08'", storage_options=options)

# SELECT com poda por partição e por estatísticas, projeção e versão.
dt = DeltaTable(uri, storage_options=options)
august = dt.to_pyarrow_table(filters=[("mes", "=", "2026-08")], columns=["id_operacao", "valor"])
dataset = dt.to_pyarrow_dataset()             # para registrar no DuckDB ou varrer em lotes
previous = DeltaTable(uri, version=12, storage_options=options)

# UPDATE e DELETE por predicado: reescrevem os arquivos atingidos.
dt.update(predicate="mes = '2026-08' AND id_operacao = 42", updates={"descricao": "'corrigido'"})
dt.delete(predicate="mes = '2026-08' AND id_cliente = 7")

# MERGE (upsert) a partir de uma tabela Arrow.
(dt.merge(source=source, predicate="t.id_operacao = s.id_operacao AND t.mes = s.mes",
          source_alias="s", target_alias="t")
   .when_matched_update_all()
   .when_not_matched_insert_all()
   .execute())
```

Comportamentos verificados:

- `append` sem `schema_mode` recusa uma coluna a mais (`SchemaMismatchError`) e valores nulos em
  coluna não anulável (`1 rows failed validation check`), mas converte tipos compatíveis para o tipo
  da tabela em silêncio.
- `overwrite` com `predicate` recusa dados fora do predicado (`5 rows failed validation check`) e
  registra o predicado no `commitInfo`.
- `update`, `delete` e `merge` devolvem métricas (`num_updated_rows`, `num_deleted_rows`,
  `num_target_rows_inserted`, `num_target_files_skipped_during_scan`) e não alteram o protocolo:
  sem vetores de exclusão, os arquivos continuam carregáveis pelo `COPY`.
- Os predicados são SQL do DataFusion; literais de string entre aspas simples, datas como
  `DATE '2026-08-01'`.

### Ingestão de dados

Caminhos para dentro de uma tabela Delta, do mais ao menos comum no pipeline:

1. Arrow em memória ou em streaming. `write_deltalake` aceita um `RecordBatchReader`, e o DuckDB
   produz um com `con.execute(sql).to_arrow_reader(50_000)`: 200.000 linhas geradas pelo DuckDB
   entraram num único arquivo sem materializar a tabela em Python. O cast seguro para o esquema do
   contrato acontece na consulta do DuckDB ou em `Table.cast(schema, safe=True)`.
2. Arquivos gravados por outro escritor, registrados sem cópia por `create_write_transaction`:

```python
import json, time
from deltalake.transaction import AddAction

def register_file(dt: DeltaTable, relative_path: str, size: int, month: str, stats: dict) -> None:
    action = AddAction(path=relative_path, size=size, partition_values={"mes": month},
                     modification_time=int(time.time() * 1000), data_change=True,
                     stats=json.dumps(stats))
    dt.create_write_transaction([action], mode="append", schema=dt.schema(), partition_by=["mes"])
```

   O arquivo precisa estar dentro da pasta da tabela, na subpasta da partição, sem a coluna de
   partição e com as colunas do esquema, em qualquer ordem, porque os leitores casam por nome. O
   `create_write_transaction` não confere nada disso, nem a existência do arquivo nem a estatística:
   um caminho inexistente, uma estatística falsa e um arquivo sem uma coluna `NOT NULL` commitam, e os
   leitores obedecem à ação (as conferências que a biblioteca faz antes do commit
   estão em `serialize_db.delta.register_files`). `estatisticas` segue o JSON da ação `add`
   (`numRecords`, `minValues`, `maxValues`, `nullCount`, sem as colunas de partição); o DuckDB os
   fornece por `COPY ... (RETURN_STATS)`, e um arquivo alheio os fornece pelo rodapé Parquet
   (`pq.read_metadata`). Com as estatísticas, a poda funcionou nos dois leitores: `file_uris` com
   `id_operacao >= 990000` devolveu lista vazia, e o DuckDB mostrou `Scanning Files: 0/12`. O
   mínimo e o máximo entram só dos tipos que o JSON transcreve exato — inteiro, data, `Double` e
   texto —, porque o log os guarda como valor JSON: um `decimal(18, 2)` de 18 dígitos
   significativos vira um dobro e perde precisão, e um máximo abaixo do valor real poda o arquivo
   que tem a linha (adiante, "As estatísticas por tipo"). Uma estatística ausente só deixa de
   podar.
3. `convert_to_deltalake(uri, partition_by=..., partition_strategy="hive")` cria o log sobre uma pasta
   Parquet existente, sem reescrever. Serve para a carga inicial só se os arquivos já têm os tipos, a
   ordem de colunas e o layout Hive do contrato; não foi testado.
4. `INSERT INTO` numa tabela anexada no DuckDB, descrito adiante.

A carga inicial dos Parquet atuais é o caminho 1, tabela a tabela e mês a mês, com cast para o
contrato (os modelos usam `Double` onde o contrato pede `Numeric(18, 2)`).

#### As estatísticas por tipo

O escritor do delta-rs grava `minValues` e `maxValues` como valor JSON, e a transcrição não é exata
em todo tipo (medido em 2026-09-22, delta-rs 1.6.4):

| Tipo | O que o log guarda | Consequência na poda |
| --- | --- | --- |
| Inteiro, data, `double`, texto | O valor exato; `-1e+308`, `0.30000000000000004` e um texto de 41 caracteres saíram inteiros, `NaN` fica fora do mínimo e do máximo, e o infinito vira `null` | A poda acha a linha, menos a do `NaN` num filtro por intervalo do `delta_scan`: o DuckDB ordena o `NaN` acima de todo número, e `valor > 3` deu 0 linhas com o arquivo podado pelo máximo 2,0, enquanto `valor >= 2` deu 2 (2026-09-23). O rodapé do arquivo também tem o máximo sem o `NaN`, e o leitor Parquet do DuckDB poda o grupo de linhas por ele mesmo sem estatística no log |
| `decimal(p, s)` | Um número JSON: `123456789012345.21` virou `123456789012345.2` | `WHERE valor = 123456789012345.21` devolveu **zero linhas** no delta-rs e no `delta_scan`, com a linha dentro do arquivo |
| `timestamp` | O texto truncado em milissegundos: `2026-08-31 23:59:59.999999` virou `2026-08-31 23:59:59.999` | Os dois leitores acharam a linha mesmo assim |

O defeito do `decimal` é do escritor, então uma coluna `Numeric` larga gravada por
`write_deltalake` carrega a mesma poda; o modelo cliente não tem nenhuma, porque as colunas
numéricas são `Double`. `serialize_db.delta.register_files` registra mínimo e máximo
só dos quatro tipos exatos.

A coluna sem mínimo e máximo no log deixa de podar no `delta_scan`, e o dataset do delta-rs a lê
como garantia: `to_pyarrow_dataset()` monta a de cada fragmento com `coluna >= null` e
`coluna <= null`, e o PyArrow pula o arquivo em todo filtro de valor sobre ela, como
`to_pyarrow_table` e `to_pandas` com `filters`. Sem o `nullCount` da coluna no log, o `IS NULL`
também pula o arquivo, e o `IS NOT NULL` o devolve inteiro, nulos incluídos. O
`delta.dataSkippingStatsColumns` sem a coluna a tira da garantia (sonda de 2026-09-25 em
`probes/consistencia/`; a pendência está em `.claude/memory/OPEN_QUESTIONS.md`).

O `NaN` segue convenções diferentes no rodapé Parquet e no log. A especificação do Parquet
(`parquet.thrift`) manda o escritor deixar o `NaN` fora do mínimo e do máximo e, desde o
PARQUET-2249 (2026-05-26), contá-lo em `nan_count`; o leitor sem `nan_count` supõe que pode haver
`NaN`, e ignora o mínimo e o máximo numa busca que o `NaN` satisfaz. O protocolo Delta define
`maxValues` como o maior valor válido do arquivo, sem contagem de `NaN`; o delta-kernel-rs grava o
`NaN` como máximo, e o Delta Spark descarta o mínimo e o máximo de ponto flutuante que colhe do
rodapé de escritores que deixam o `NaN` de fora (PR #7101, 2026-06-27). O delta-rs copia o máximo do
rodapé para o log. A biblioteca grava sem mínimo e máximo, no rodapé e no log, as colunas `Double`
com valor não finito em cada partição ([issue #59](https://github.com/felipenoris/serialize-db/issues/59),
2026-09-23).

### Exportação para Parquet

Os arquivos de dados já são Parquet. Exportar é listar os arquivos do snapshot que interessa:
`dt.file_uris(file_pruning_predicate="mes IN ('2026-07', '2026-08')")` devolve as URIs, e
`get_add_actions(flatten=True)` acrescenta `size_bytes` e `num_records`. Essa lista alimenta o
manifesto do `COPY` do Redshift e qualquer leitor Parquet. Os arquivos do delta-rs têm as colunas
não anuláveis como `required`, estatísticas em todas as colunas, Snappy e um row group por arquivo
até o tamanho alvo do escritor (120.000 linhas ficaram num row group). O `optimize.compact` regrava
em ZSTD, e um `WriterProperties` sem `compression` grava sem compressão (delta-rs 1.6.6,
2026-10-05); a biblioteca passa `compression="SNAPPY"` nas propriedades que tiram o mínimo e o
máximo das colunas `Double` com valor não finito. O `COPY` do Redshift carregou uma partição
compactada em ZSTD no ambiente alvo em 2026-10-06 (`test_publication_loads_a_compacted_partition`
de `tests/test_publication.py`).

Uma exportação para outro layout (um arquivo por mês com `FIELD_IDS` e `KV_METADATA`, por exemplo) é
um `COPY (SELECT ... FROM delta_scan(uri) WHERE ...) TO ...` do DuckDB, com as opções de
[Parquet](#parquet).

### Exportação do snapshot para pastas Parquet por mês

Sair do Delta e voltar às pastas Parquet por mês, como as que o Hive lê no HDFS, é exportar o
snapshot atual: as partições de todos os meses, na versão atual, sem o log. O histórico de versões
fica para trás; um snapshot do banco que precise sobreviver é exportado à parte, tabela a tabela,
a partir de `DeltaTable(uri, version=v)`, como em "Snapshots do banco". Uma tabela particionada por
dia segue o mesmo caminho, com a coluna de partição diária no lugar de `mes`.

Copiar a pasta da tabela não serve. Ela guarda todos os arquivos já gravados, inclusive os que
commits posteriores tiraram do snapshot (meses substituídos, arquivos reescritos por `update`,
`delete` e `merge`, arquivos pequenos compactados pelo `optimize`), até que o `vacuum` os apague;
guarda o `_delta_log/`, cujos checkpoints também são Parquet; e, se os recursos estivessem
habilitados, guardaria arquivos de change data feed e de vetores de exclusão. Na verificação abaixo,
uma tabela com 20 commits tinha 20 arquivos de dados no disco para 14 no snapshot, e a leitura da
pasta inteira devolveu 330.000 linhas a mais.

O que decide entre copiar e reescrever é o esquema dos arquivos vivos. Cada arquivo tem o esquema da
época em que foi gravado: depois de um `ADD COLUMN`, os arquivos anteriores não têm a coluna, e só os
meses reescritos depois passam a tê-la (4 de 14 na verificação). O Delta preenche nulo na leitura;
fora dele, isso fica a cargo do leitor. Leitores por nome preenchem nulo: o Hive por padrão
(`parquet.column.index.access=false`), o Spark e o DuckDB com `union_by_name`. Leitores posicionais,
como o `COPY` do Redshift, não. Sem `union_by_name`, o DuckDB toma o esquema do primeiro arquivo do
glob, e as colunas que só existem em arquivos posteriores somem em silêncio. Mudança de tipo não
deixa arquivo heterogêneo, porque só acontece por reescrita da tabela inteira. Column mapping deixaria
os nomes físicos `col-<uuid>` nos arquivos; o projeto não o habilita.

Copiar pelo log é a opção que não lê dados. A lista `get_add_actions()` do snapshot dá o caminho
relativo de cada arquivo vivo, já no layout Hive `mes=2026-02/part-....parquet`, e o valor da
partição. Copiar exatamente esses arquivos, sem o log, produz a pasta por mês; no S3 é a
transferência gerenciada do `boto3` por arquivo (`CopyObject` ou `UploadPartCopy`), sem baixar. O
`path` da ação `add` é uma URI, decodificada com `unquote`
antes de usar.

```python
import shutil
from pathlib import Path
from urllib.parse import unquote

import pyarrow as pa
from deltalake import DeltaTable

def copy_snapshot(dt: DeltaTable, dest: Path) -> int:
    root = Path(dt.table_uri.removeprefix("file://"))
    actions = pa.table(dt.get_add_actions(flatten=True)).to_pylist()
    for action in actions:
        relative = unquote(action["path"])       # mes=2026-02/part-....parquet
        target = dest / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(root / relative, target)    # no S3: copy_object(Bucket, Key, CopySource), sem baixar
    return len(actions)

dt = DeltaTable(uri)
copy_snapshot(dt, Path("export_a"))              # 14 arquivos em 0,014 s
```

Os arquivos saem como o delta-rs os gravou: sem a coluna de partição dentro, `DECIMAL(18, 2)` como
`INT64`, colunas não anuláveis `required`, com estatísticas. O Hive deriva `mes` do diretório e
precisa da tabela declarada com todas as colunas atuais e de `MSCK REPAIR TABLE` ou
`ALTER TABLE ... ADD PARTITION` para enxergar as pastas. A cópia serve quando os leitores são por
nome, ou quando nenhum `ADD COLUMN` aconteceu desde a última reescrita de todos os meses.

Reescrever pelo snapshot é a opção que normaliza. O DuckDB lê o snapshot por `delta_scan` e grava as
pastas com um `COPY` particionado: cada arquivo sai com o esquema atual, as colunas acrescentadas
preenchidas com nulo nos meses antigos, as linhas excluídas ausentes e um arquivo por mês. Sem
`WRITE_PARTITION_COLUMNS`, a coluna de partição fica fora dos arquivos, como o Hive e o Spark esperam
de uma pasta particionada. No S3, `delta_scan('s3://...')` e `COPY ... TO 's3://...'` usam o mesmo
secret, e as opções de sobrescrita não apagam prefixos: o destino é limpo antes.

```python
import duckdb

con = duckdb.connect()
con.sql(f"""
    COPY (SELECT * FROM delta_scan('{uri}'))
    TO 'export_b' (FORMAT parquet, PARTITION_BY (mes), OVERWRITE_OR_IGNORE)
""")                                             # mes=2026-02/data_0.parquet, um por mês, 0,071 s

# Um COPY por mês limita a memória, é reexecutável e aceita as opções da seção Parquet.
for (month,) in con.sql(f"SELECT DISTINCT mes FROM delta_scan('{uri}') ORDER BY mes").fetchall():
    Path(f"export_c/mes={month}").mkdir(parents=True, exist_ok=True)
    con.sql(f"""
        COPY (SELECT * EXCLUDE (mes) FROM delta_scan('{uri}') WHERE mes = '{month}')
        TO 'export_c/mes={month}/data_0.parquet' (FORMAT parquet)
    """)                                         # 14 meses em 0,171 s
```

Sem o DuckDB, o PyArrow faz o mesmo a partir do dataset do delta-rs, e grava `DECIMAL` como
`FIXED_LEN_BYTE_ARRAY`:

```python
import pyarrow.dataset as ds

ds.write_dataset(
    dt.to_pyarrow_dataset(), "export_d", format="parquet",
    partitioning=ds.partitioning(pa.schema([("mes", pa.string())]), flavor="hive"),
    existing_data_behavior="delete_matching",
)                                                # mes=2026-02/part-0.parquet, 0,097 s
```

Verificação com 1.329.900 linhas em 14 meses depois de 20 commits (12 appends, substituição de um
mês, `ADD COLUMN`, append com a coluna nova, `update`, `delete`, três appends pequenos e
`optimize.compact`), com checkpoint gravado:

| Verificação | Resultado |
| --- | --- |
| Arquivos de dados no disco e no snapshot | 20 no disco, 14 no snapshot; `vacuum(dry_run=True)` listou os 6 a mais. |
| `read_parquet('tabela/mes=*/*.parquet')` | 1.659.900 linhas, 330.000 além do snapshot: o mês substituído, os arquivos anteriores ao `update` e ao `delete` e os três pequenos compactados. |
| `read_parquet('tabela/**/*.parquet', union_by_name=true)` | Leu também o checkpoint de `_delta_log/` e somou suas 22 linhas sem erro. |
| Arquivos vivos com a coluna acrescentada | 4 de 14: o mês novo e os três reescritos depois do `ADD COLUMN`. |
| Cópia pelo log | 14 arquivos em 0,014 s; lida com `union_by_name=true`, contagem, soma e nulos iguais aos do `delta_scan`. Sem `union_by_name`, o DuckDB toma o esquema do primeiro arquivo do glob e omite a coluna nova quando ele não a tem. |
| `COPY ... PARTITION_BY (mes)` do DuckDB, 11 threads | 14 arquivos `data_0.parquet`, um por mês, em 0,071 s; sem `mes` dentro dos arquivos; `WRITE_PARTITION_COLUMNS true` o inclui; lida sem `union_by_name`, igual ao `delta_scan`. |
| Um `COPY` por mês | 14 arquivos em 0,171 s; igual ao `delta_scan`. |
| `pyarrow.dataset.write_dataset` a partir de `to_pyarrow_dataset()` | 14 arquivos `part-0.parquet` em 0,097 s; sem `mes` dentro; igual ao `delta_scan`. |
| Tipo físico de `valor` | `INT64` no delta-rs e no DuckDB; `FIXED_LEN_BYTE_ARRAY` no PyArrow. |

### Pipeline com o Delta como fonte da verdade

1. **Início da execução.** A biblioteca abre cada tabela de entrada e registra
   `versions[table] = dt.version()`. Toda leitura da execução usa essas versões, o que dá uma visão
   consistente entre tabelas mesmo que outra execução publique no meio.
2. **Ingestão seletiva no motor.** No DuckDB, `ATTACH uri AS t (TYPE delta, VERSION v)` ou uma view
   sobre `delta_scan(uri, version := v)`, com o nome que o modelo espera; os filtros de mês do
   pipeline chegam ao scan e só os arquivos necessários são lidos. As tabelas consultadas muitas
   vezes são materializadas pela DDL do modelo seguida de
   `INSERT INTO t BY NAME SELECT * FROM delta_scan(...) WHERE mes IN (...)`, numa transação.
   No Redshift, `COPY ... MANIFEST` dos arquivos dessas versões e desses meses numa tabela do sandbox
   com prefixo da execução.
3. **Execução.** O pipeline roda no sandbox, com statements Core do SQLAlchemy e lógica Python;
   resultados intermediários ficam no sandbox, não no Delta.
4. **Auditoria.** Contagens, nulos, unicidade de chaves, `mes` igual a `strftime(data_ref, '%Y-%m')`,
   limites de tipo, no motor.
5. **Publicação.** Por tabela e por mês: do DuckDB, `write_deltalake(mode="overwrite",
   predicate="mes = ...")` com o `RecordBatchReader` da consulta, ou `COPY ... TO` na subpasta do
   mês mais `create_write_transaction`; do Redshift, `UNLOAD` num prefixo novo dentro da subpasta do
   mês mais `create_write_transaction`. Cada commit leva `serialize_db_execution_id` e
   `serialize_db_input_versions` em `custom_metadata`. Uma reexecução repete os mesmos `overwrite` e
   é idempotente; um conflito de commit no mesmo mês significa outra execução publicando a mesma
   tabela, e a execução aborta.
6. **Publicação no Redshift para clientes.** A diferença entre a versão publicada e a pedida (ações
   `add` e `remove` de dados) diz quais meses recarregar: `DELETE` do mês e `COPY ... MANIFEST` dos
   arquivos novos, numa transação por tabela, sem atomicidade
   entre as tabelas, como no Delta; a versão pedida vem do snapshot de
   `serialize-db publish_redshift --snapshot` ou `--channel`, e a versão publicada de cada tabela
   fica numa tabela de controle.
7. **Manutenção.** `optimize.compact` nos meses com muitos arquivos pequenos e `vacuum` com
   `keep_versions` dos snapshots do banco, como descrito em "Manutenção e retenção".

### Manipulação a partir do DuckDB

```sql
INSTALL delta; LOAD delta;
CREATE SECRET (TYPE s3, PROVIDER credential_chain);                      -- credenciais da sessão

SELECT mes, count(*) FROM delta_scan('s3://bucket/prd/cad_operacoes') WHERE mes >= '2026-07' GROUP BY 1;
SELECT count(*) FROM delta_scan('s3://bucket/prd/cad_operacoes', version := 12);

ATTACH 's3://bucket/prd/cad_operacoes' AS cad_operacoes (TYPE delta, PIN_SNAPSHOT true);
SELECT * FROM cad_operacoes AT (VERSION => 12) LIMIT 5;
CREATE TABLE trabalho AS SELECT * FROM cad_operacoes WHERE mes IN ('2026-07', '2026-08');

INSERT INTO cad_operacoes SELECT ...;                                    -- append cego
```

Comportamentos verificados:

- `delta_scan` empurra filtros de partição e de estatísticas (`Scanning Files: 0/12` para um filtro
  fora do intervalo de todas as colunas) e projeção. Na coluna de partição, `=`, `IN` de um valor,
  `BETWEEN` e `>=` podam, e `IN` de mais de um valor e `OR` abrem todos os arquivos; o intervalo somado
  ao `IN` poda pelo intervalo, e o `EXPLAIN ANALYZE` dessa forma falha com `InternalException:
  ... total_files inconsistent!`, com a consulta rodando (2026-09-23, log `FileSystem` do DuckDB).
  Lê vetores de exclusão, tipos primitivos, structs
  e `VARIANT`; expõe a coluna de partição como coluna comum; mostra todas as colunas como anuláveis.
- `delta_scan(uri, version := n)` e `ATTACH ... (VERSION n)` leem versões antigas;
  `delta_scan(...) AT (VERSION => n)` não é aceito. `ATTACH ... (PIN_SNAPSHOT true)` fixa a versão:
  depois de um commit externo, a tabela anexada continuou em 220.076 linhas e um `delta_scan` novo
  viu 220.081.
- `INSERT INTO` numa tabela anexada é a única escrita: cria um commit cujo `commitInfo` o delta-rs
  mostra como `UNKNOWN`, grava `duckdb_<uuid>_0.parquet` na subpasta da partição, com estatísticas na
  ação `add` e com a coluna de partição dentro do arquivo, ao contrário do delta-rs. Um `COPY` do
  Redshift sobre arquivos dos dois escritores falharia por número de colunas; a biblioteca escreve
  por um único caminho, o delta-rs ou `COPY ... TO` mais `create_write_transaction`. `UPDATE`,
  `DELETE`, `ALTER` e `CREATE TABLE` em Delta não existem no DuckDB.
- `con.register("t", dt.to_pyarrow_dataset())` é o caminho sem a extensão: o DuckDB consulta o
  dataset Arrow do delta-rs com poda por partição.
- `COPY (consulta) TO 'uri/mes=2026-09/exec_abc.parquet' (FORMAT parquet, RETURN_STATS)` grava com o
  escritor paralelo do DuckDB e devolve `count`, `file_size_bytes` e `column_statistics` (mapa com
  `min`, `max`, `null_count` por coluna, nomes entre aspas e valores em texto), que viram a
  `AddAction` do registro. O arquivo do DuckDB marca todas as colunas como `optional`; a nulidade
  continua garantida pelo esquema Delta e pela auditoria. Num `DOUBLE` com `NaN`, o `RETURN_STATS`
  traz o maior número como máximo e `has_nan: true` só quando o `NaN` está no último grupo de linhas
  do arquivo; o rodapé grava sem mínimo e máximo todo grupo com `NaN`. O infinito sai `inf`; o texto longo sai
  truncado em 256 caracteres, com o máximo de 255 e o último incrementado, acima do valor real, e o
  texto multibyte longo sai sem mínimo e máximo (2026-09-23).
- O secret guarda a chave resolvida no `CREATE SECRET`, e o `delta_scan` lê o log com ela, sem
  renová-la: numa conexão mais longa que a credencial do contêiner, o primeiro `delta_scan` depois
  da expiração falhou com `DeltaKernel ObjectStoreError (8)`, e o `REFRESH auto` do
  `credential_chain` só renovou o secret numa leitura pelo `httpfs` (2026-09-25,
  `probes/credentials.py`). A biblioteca cria o secret com a chave da credencial do `boto3` e o motor
  DuckDB o recria quando ela troca (`serialize_db.engine.duckdb.DuckDBEngine`).

### Manipulação a partir do Redshift

O Redshift não lê o log. Sem esquema externo (o Spectrum exige `CREATE` no banco, negado ao projeto),
a biblioteca traduz o snapshot em comandos que o Redshift entende.

Carga de uma tabela do sandbox ou de publicação:

```python
import json

def manifest(dt: DeltaTable, months: set[str]) -> bytes:
    root = dt.table_uri.rstrip("/")
    actions = pa.table(dt.get_add_actions(flatten=True)).to_pylist()
    return json.dumps({"entries": [
        {"url": f"{root}/{a['path']}", "mandatory": True, "meta": {"content_length": a["size_bytes"]}}
        for a in actions if a["partition.mes"] in months
    ]}).encode()
```

```sql
BEGIN;
DELETE FROM prd_cad_operacoes WHERE mes = '2026-08';
CREATE TEMPORARY TABLE staging_cad_operacoes (                             -- sem a coluna mes
    id_operacao BIGINT NOT NULL, data_ref DATE NOT NULL, id_cliente BIGINT NOT NULL,
    valor NUMERIC(18, 2) NOT NULL, descricao VARCHAR(200));
COPY staging_cad_operacoes (id_operacao, data_ref, id_cliente, valor, descricao)  -- do rodapé
    FROM 's3://bucket/publicacao/exec-42/cad_operacoes/2026-08/1.manifest'
    IAM_ROLE 'arn:aws:iam::123456789012:role/papel' FORMAT AS PARQUET MANIFEST FILLRECORD;
INSERT INTO prd_cad_operacoes (id_operacao, data_ref, id_cliente, valor, descricao, mes)
SELECT id_operacao, data_ref, id_cliente, valor, descricao, '2026-08' FROM staging_cad_operacoes;
COMMIT;
```

A tabela de staging existe porque a coluna de partição não está nos arquivos e o `COPY` só lê o
conteúdo deles; a lista de colunas no `COPY` de Parquet funciona (2026-09-21), e a biblioteca a
passa com as colunas do rodapé de cada grupo de arquivos, um manifesto por lista, mas ela não
fornece o valor da coluna ausente, então a staging fica, e o
`INSERT` lista as colunas na ordem do contrato, com o valor da partição no lugar da coluna. A
publicação incremental compara as ações da versão publicada com as da pedida e recarrega só os meses
que mudaram, numa transação por tabela, sem atomicidade entre as tabelas; a versão publicada de cada
tabela fica numa tabela de controle
(`serialize_db_publications(table_name, delta_version, execution_id, published_at)`).

Escrita de volta, quando o sandbox é o Redshift:

```sql
UNLOAD ('SELECT id_operacao, data_ref, id_cliente, valor, descricao, mes
         FROM exec_42_cad_operacoes WHERE mes = ''2026-08''')
TO 's3://bucket/prd/cad_operacoes/'
IAM_ROLE 'arn:aws:iam::123456789012:role/papel'
FORMAT AS PARQUET PARTITION BY (mes) MANIFEST VERBOSE;
```

`PARTITION BY` grava em `mes=2026-08/` e retira a coluna do arquivo, que é a convenção do Delta. A
biblioteca chega ao mesmo layout sem ele: o `select` deixa a coluna de partição de fora, e o destino
é um prefixo novo por tentativa dentro da pasta da partição, montado por ela. O manifesto verboso
traz `content_length` e `record_count` por arquivo; `minValues` e `maxValues` vêm
do rodapé Parquet de cada arquivo, lido com `pq.read_metadata`. Um `create_write_transaction` com
`mode="overwrite"` e `partition_filters` do mês substitui os arquivos anteriores. Quatro regras
mantêm esse caminho carregável nos dois sentidos: nenhuma linha fora dos arquivos (sem vetores de
exclusão), coluna de partição derivável de uma coluna do arquivo, `DECIMAL` em `INT64` aceito pelo
`COPY` (carregou no ambiente alvo em 2026-09-21) e colunas na ordem do esquema Delta.

### Recomendações de performance

Medições em disco local, com os arquivos no cache do sistema, 3.000.000 de linhas em 12 meses e
52,9 MB em 12 arquivos; os tempos medem CPU e o custo do log, não a latência do S3:

| Operação | Tempo |
| --- | --- |
| `write_deltalake` a partir do `RecordBatchReader` do DuckDB (geração incluída) | 0,31 s |
| `COPY ... TO` particionado do DuckDB com os mesmos dados | 0,67 s |
| Agregação de três meses por `delta_scan` | 0,010 s |
| A mesma agregação por `read_parquet` com partição Hive | 0,006 s |
| Materializar os três meses numa tabela do DuckDB | 0,03 s |
| A mesma agregação na tabela materializada | 0,007 s |
| Join de um mês com uma dimensão de 100.000 linhas: `delta_scan` | 0,010 s |
| O mesmo join na tabela materializada | 0,004 s |
| 20 consultas pontuais (`mes` e `id_cliente`): `delta_scan` | 0,05 s |
| As mesmas por `ATTACH ... (PIN_SNAPSHOT true)` | 0,04 s |
| As mesmas na tabela materializada | abaixo de 0,01 s |
| Agregação de três meses pelo dataset Arrow do delta-rs registrado no DuckDB | 0,011 s |
| `to_pyarrow_table` de um mês (246.570 linhas) pelo delta-rs | 0,013 s |

O que as medições sustentam:

- Ler no lugar custa o mesmo que ler Parquet solto: o log e as estatísticas pagam a si mesmos pela
  poda. A ingestão no DuckDB deixa de ser uma cópia obrigatória e vira uma view ou um `ATTACH`.
- Materializar continua valendo em três casos: tabelas consultadas repetidas vezes na execução (as
  consultas pontuais foram dez vezes mais rápidas na tabela, e no S3 cada `delta_scan` refaz
  requisições), joins repetidos com a mesma tabela grande, e quando o pipeline precisa de índice ou
  ordenação física. A regra prática: `CREATE TABLE AS` com o filtro de meses para as tabelas de
  fato do pipeline, view para as dimensões e para leituras únicas. A medição no S3, abaixo, confirma
  a regra: uma consulta pontual por `delta_scan` custa cerca de 0,3 s no S3 e menos de 1 ms na
  tabela materializada, e materializar os 300.000 registros custou o mesmo que uma agregação.
- Um arquivo por mês por tabela basta enquanto o mês couber em um ou dois arquivos de 100 MB a 1 GB,
  que é a faixa que o DuckDB e o Redshift preferem. `optimize.compact(partition_filters=...)` junta
  os arquivos pequenos de um mês que cabem juntos no tamanho alvo (`target_size`, ou a propriedade
  `delta.targetFileSize` da tabela, 100 MB sem ela); o arquivo que não cabe com outro fica como
  está, e sem nada a juntar não há commit (sondagem de 2026-09-27).
  `optimize.z_order(["id_cliente"])` reordena dentro do mês quando os filtros forem por outra
  coluna. Os dois criam versões e arquivos novos quando reescrevem.
- Manter a ordenação pela chave do modelo (`ORDER BY` na consulta que gera o mês) faz as estatísticas
  por arquivo e por row group podarem melhor e comprime mais.
- `PIN_SNAPSHOT` e uma única `DeltaTable` por tabela e execução evitam reler o log a cada consulta.
- No S3, o custo dominante é o número de requisições: poucos arquivos grandes, checkpoints em dia e
  `delta.checkpointInterval` baixo o bastante para o log entre checkpoints ficar curto.

Medições no S3, em 2026-09-19, do espaço do SageMaker Unified Studio (4 threads) para o bucket do
projeto na mesma região, com 300.010 linhas em dois meses, 7,6 MB em três arquivos, deltalake 1.6.4
e DuckDB 1.5.5; os tempos medem a latência do S3 e o custo de reler o log, não CPU:

| Operação | Tempo |
| --- | --- |
| `write_deltalake` de 300.000 linhas, `overwrite` particionado por `mes` | 1,1 s |
| `write_deltalake` de 10 linhas, `append` (um commit condicional) | 0,5 s |
| `to_pyarrow_table` da tabela inteira pelo delta-rs | 0,44 s |
| Agregação por `delta_scan`, primeira leitura | 0,77 s |
| A mesma agregação, leituras seguintes | 0,3 s |
| A mesma agregação com filtro por `mes` (`Scanning Files: 1/3`) | 0,28 s |
| A mesma agregação por `read_parquet` com glob e partição Hive | 0,06 s |
| Materializar a tabela inteira com `CREATE TABLE AS` de `delta_scan` | 0,29 s |
| A mesma agregação na tabela materializada | 0,002 s |
| 20 consultas pontuais (`id_operacao`): `delta_scan` | 6,0 s |
| As mesmas com filtro por `mes` | 6,1 s |
| As mesmas por `ATTACH ... (PIN_SNAPSHOT true)` | 3,1 s |
| As mesmas por `read_parquet` com glob | 1,3 s |
| As mesmas na tabela materializada | 0,013 s |
| `vacuum(dry_run=False)` de cinco arquivos | 0,56 s |

Cada `delta_scan` no S3 relê o log antes de ler os dados, e é isso que separa os 0,3 s por consulta
pontual dos 0,065 s do `read_parquet`; `PIN_SNAPSHOT` corta a releitura pela metade, não a elimina.
A ingestão no motor DuckDB fica então em dois modos: a tabela materializada, pela DDL do modelo e
`INSERT ... BY NAME` com filtro de meses numa transação, para toda tabela que o pipeline consulta
mais de uma vez, e a view por `delta_scan` para leitura única; a medição acima materializou por
`CREATE TABLE AS`.

### Manutenção e retenção

#### O que cresce e o que limpa

Duas coisas crescem: os arquivos de dados substituídos, que ficam no disco até o `vacuum`, e o log,
um JSON por commit mais um checkpoint a cada `delta.checkpointInterval` commits (dez commits do teste
somaram 27.039 bytes de log). Ler uma versão antiga exige as duas: um checkpoint anterior ou igual a
ela mais os JSON até ela, e os arquivos de dados que ela referencia.

`vacuum(retention_hours, dry_run, enforce_retention_duration, full, keep_versions)`:

- Lista e apaga os arquivos que saíram do snapshot (ações `remove`) há mais tempo que a retenção.
  `retention_hours` menor que `delta.deletedFileRetentionDuration` exige
  `enforce_retention_duration=False`. `dry_run=True` (padrão) só lista.
- `full=True` acrescenta os arquivos da pasta que nenhum log menciona, os órfãos de escritas
  interrompidas. No teste, o modo padrão listou só os dois arquivos removidos; `full=True` listou
  também `orfao.parquet` e `_temporario.parquet`.
- `keep_versions=[...]` preserva os arquivos que as versões listadas referenciam, ainda que estejam
  fora da retenção.
- A execução grava dois commits, `VACUUM START` e `VACUUM END`.

A limpeza do log é automática: ao criar um checkpoint, o delta-rs apaga os arquivos de log mais
velhos que `delta.logRetentionDuration`. Verificado com `interval 0 days` e checkpoint a cada dois
commits: depois de cinco commits sobraram `00000000000000000005.json`, o checkpoint 5 e
`_last_checkpoint`, e a versão 0 deixou de abrir (`No files in log segment`). Com o padrão de 30
dias, toda versão mais velha que isso fica ilegível no checkpoint seguinte, independentemente do
`vacuum`. `cleanup_metadata()` faz a mesma limpeza sob demanda, e
`PostCommitHookProperties(cleanup_expired_logs=False)` a desliga numa escrita. Como o log custa
quilobytes por commit, a tabela do contrato o mantém por `interval 3650 days`.

#### Snapshots do banco

O pipeline mensal cria uma versão por tabela a cada execução. Um snapshot do banco é o conjunto das
versões de cada tabela num instante escolhido, registrado pela biblioteca porque o Delta não tem
snapshot de banco, só de tabela; a periodicidade é de quem o marca, por exemplo o fim de cada
trimestre. A política é guardar essas versões por prazo longo e descartar as intermediárias entre um
snapshot e outro. O Delta não tem tags nem branches (o Iceberg tem, com retenção própria); o
snapshot do banco é um número de versão por tabela, e os mecanismos são estes:

1. **Marcar o snapshot.** A execução marcada grava `custom_metadata={"serialize_db_snapshot": "2026T1"}`
   em cada commit, e a biblioteca registra `{snapshot: {tabela: versão}}` em
   `_serialize_db/snapshots.json` (`serialize_db.delta.snapshot`). O histórico
   também acha a versão (`[h for h in dt.history() if h.get("serialize_db_snapshot")]` devolveu
   `(2, '2026T1')`), mas o arquivo de controle dispensa varrer o log.
2. **Descartar o que está entre snapshots.** `vacuum` com a retenção das versões comuns e
   `keep_versions` com as versões dos snapshots. Verificado com o snapshot na versão 2 e duas
   correções posteriores de fevereiro: o dry run sem `keep_versions` listou dois arquivos; com
   `keep_versions=[2]` listou um, o que só a versão 3 referenciava. Depois do `vacuum`, a versão 2
   leu os três meses corretos, a versão 3 falhou por arquivo ausente, e as versões 4 e atual leram.
3. **Custo.** Um snapshot guardado custa só os arquivos que as execuções seguintes substituíram,
   os meses corrigidos depois dele, não uma cópia da tabela. `optimize.compact` e `z_order` depois
   de um snapshot reescrevem arquivos que ele continua referenciando e dobram esses meses; a
   compactação roda antes do snapshot.
4. **Ler um snapshot.** `DeltaTable(uri, version=v)` no delta-rs; `delta_scan(uri, version := v)` ou
   `ATTACH ... (VERSION v)` no DuckDB; um manifesto de `COPY` gerado de
   `DeltaTable(uri, version=v).get_add_actions()` no Redshift. `load_as_version(datetime)` acha a
   versão vigente num instante pelo `timestamp` dos commits.
5. **Voltar a um snapshot.** `dt.restore(v)` torna a versão do snapshot o estado atual num commit
   novo, sem apagar as versões posteriores (verificado com `restore(2)` seguido de `restore(5)`).
6. **Arquivar por prazo mais longo.** Uma cópia profunda do snapshot numa pasta de arquivo,
   `write_deltalake("s3://bucket/arquivo/2026T1/cad_operacoes",
   DeltaTable(uri, version=v).to_pyarrow_dataset().scanner().to_reader(), mode="overwrite",
   partition_by=["mes"])`, cria uma tabela independente na versão 0 (verificado: três arquivos, as
   mesmas somas). A cópia sai da lista de `keep_versions`, e a pasta de arquivo pode receber uma
   regra de ciclo de vida para classe de armazenamento mais barata, o que a tabela viva não pode,
   porque seus arquivos são compartilhados entre versões.

```python
import json
from deltalake import DeltaTable

# O arquivo de controle mapeia snapshot -> {tabela: versão}; o vacuum preserva essas versões.
def vacuum_keeping_snapshots(dt: DeltaTable, control: dict, table: str, retention_hours: int = 24 * 400,
                             apply: bool = False) -> list[str]:
    versions = sorted({v[table] for v in control["snapshots"].values() if table in v})
    return dt.vacuum(retention_hours=retention_hours, enforce_retention_duration=False,
                     dry_run=not apply, keep_versions=versions)

control = json.load(open("_serialize_db/snapshots.json"))   # {"snapshots": {"2026T1": {"cad_operacoes": 2, ...}}}
vacuum_keeping_snapshots(DeltaTable("cad_operacoes"), control, "cad_operacoes")   # lista sem apagar
```

Configuração que decorre disso: `delta.logRetentionDuration` em `interval 3650 days`;
`delta.deletedFileRetentionDuration` na janela das versões comuns, por exemplo `interval 400 days`,
que cobre uma reexecução de qualquer mês do ano anterior; e o `vacuum` mensal com `keep_versions`
lido do arquivo de controle. Uma tabela nova entra no snapshot no mesmo commit que a marca.

| Quando | O quê |
| --- | --- |
| A cada execução | Checkpoint automático; `custom_metadata` com `serialize_db_execution_id` e `serialize_db_input_versions`. |
| Snapshot do banco, na periodicidade do processo | `custom_metadata={"serialize_db_snapshot": ...}` nos commits e a entrada em `_serialize_db/snapshots.json`. |
| Mensal | `vacuum(dry_run=True, keep_versions=snapshots)` revisado e depois executado; `full=True` de tempos em tempos para os órfãos. |
| Antes de um snapshot | `optimize.compact` nos meses com arquivos pequenos. |
| Anual | Cópia profunda dos snapshots mais velhos que o prazo da tabela viva para a pasta de arquivo, e retirada de `keep_versions`. |
| Nunca em produção | `vacuum` com `enforce_retention_duration=False` sem `keep_versions` calculado. |

### Realocação e cópia do banco

A pasta do banco pode ser copiada, compactada ou movida inteira, entre discos, entre buckets ou entre
disco e S3, e as tabelas continuam consistentes. Verificado copiando a pasta de um banco com uma
tabela em 20 versões para outro caminho: a cópia abriu na versão 20 com as mesmas 470.081 linhas,
pelo delta-rs e pelo DuckDB, com o histórico intacto e a viagem no tempo à versão 0 funcionando; uma
escrita na cópia criou a versão 21 sem tocar o original. As razões:

- As ações `add` e `remove` guardam caminhos relativos à pasta da tabela (nenhum caminho absoluto no
  log do teste), e os checkpoints repetem esses caminhos.
- O `commitInfo` do `CREATE TABLE` registra o `location` absoluto da criação, mas é informativo e
  nenhum leitor o usa.
- O `id` da tabela em `metaData` viaja com a cópia; ele identifica a tabela em catálogos, não no
  sistema de arquivos.

Duas condições preservam a propriedade. Nenhum arquivo é registrado fora da pasta da tabela: uma
`AddAction` com URI absoluta é válida no protocolo, mas quebra na realocação, e a biblioteca só
registra caminhos relativos. E a cópia leva `_delta_log/` inteira, inclusive `_last_checkpoint`; uma
cópia só dos Parquet perde a tabela. Com `aws s3 sync` ou `cp --recursive` entre prefixos, a
estrutura relativa se mantém.

```bash
aws s3 sync s3://bucket/projeto/delta/prd/ s3://bucket/copias/2026-09-19/prd/
```

```python
# A cópia abre onde estiver, com a mesma versão; nenhum caminho precisa ser reescrito.
DeltaTable("s3://bucket/copias/2026-09-19/prd/cad_operacoes").version()
```

O Iceberg é o contraste: `metadata.json` guarda o `location` da tabela, e os manifests guardam o
`file_path` absoluto de cada arquivo (`file:/...` no teste com PyIceberg). Mover a pasta exige
reescrever os metadados ou um leitor tolerante, como a opção `allow_moved_paths` do DuckDB.

### Suporte a SQLAlchemy

Não há dialeto SQLAlchemy para Delta, e não é preciso um. O `Table` do modelo é o contrato de onde
saem o esquema Delta, o esquema Arrow e o DDL do sandbox; o motor que executa SQL sobre a tabela
Delta é o DuckDB ou o Redshift, cada um com o dialeto de [SQLAlchemy](#sqlalchemy).

- Leitura: uma view com o nome da tabela do modelo sobre `delta_scan(uri, version := v)` no DuckDB,
  e o `select()` Core compilado pelo `duckdb_engine` roda sem mudança; o resultado sai em Arrow pela
  conexão DuckDB (`to_arrow_table()` ou `to_arrow_reader()`), preservando `decimal128` e `date32`. No
  Redshift, a tabela do sandbox carregada por `COPY` é uma tabela comum.
- Escrita: o `select()` que produz o mês é compilado com `literal_binds`, executado pelo DuckDB como
  `RecordBatchReader` e entregue a `write_deltalake`; nenhuma linha passa por `executemany`.
- O ORM como unidade de trabalho (instâncias, `session.add`) não tem papel; o pipeline não o usa.

### Referências

Delta Lake e delta-rs:

- <https://github.com/delta-io/delta/blob/master/PROTOCOL.md>
- <https://delta-io.github.io/delta-rs/usage/writing/>
- <https://delta-io.github.io/delta-rs/api/delta_table/>
- <https://delta-io.github.io/delta-rs/api/delta_table/delta_table_alterer/>
- <https://delta-io.github.io/delta-rs/integrations/object-storage/s3/>
- <https://delta-io.github.io/delta-rs/integrations/object-storage/hdfs/>
- <https://github.com/delta-io/delta-rs/releases>
- <https://github.com/delta-io/delta-rs/discussions/4482>
- <https://github.com/delta-io/delta-rs/pull/4732>
- <https://github.com/delta-io/delta-rs/issues/3936>
- <https://docs.rs/object_store/latest/object_store/aws/struct.AmazonS3Builder.html>
- <https://docs.rs/object_store/latest/object_store/aws/enum.AmazonS3ConfigKey.html>
- <https://docs.rs/object_store/latest/src/object_store/aws/builder.rs.html>

Implementações do protocolo e uso sem Spark:

- <https://delta.io/learn/getting-started/>
- <https://delta.io/blog/delta-lake-without-spark/>
- <https://delta.io/integrations/>
- <https://docs.delta.io/latest/delta-storage.html>
- <https://pypi.org/project/delta-spark/>
- <https://delta-io.github.io/delta-rs/feature-table/>
- <https://github.com/delta-io/delta-rs> (`README.md`, `Cargo.toml` e `crates/core/Cargo.toml`)
- <https://github.com/delta-io/delta-rs/issues/4512>
- <https://github.com/delta-io/delta-rs/issues/4613>
- <https://github.com/delta-io/delta-rs/issues/4657>
- <https://github.com/delta-io/delta-kernel-rs>
- <https://github.com/duckdb/duckdb-delta>

DuckDB:

- <https://duckdb.org/docs/current/core_extensions/delta.html>
- <https://duckdb.org/docs/current/data/partitioning/partitioned_writes.html>
- <https://cwiki.apache.org/confluence/display/Hive/Parquet>
- <https://duckdb.org/docs/current/core_extensions/iceberg/overview.html>

Iceberg e Redshift:

- <https://py.iceberg.apache.org/configuration/>
- <https://py.iceberg.apache.org/api/>
- <https://docs.aws.amazon.com/redshift/latest/dg/c-spectrum-external-tables.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/copy-usage_notes-copy-from-columnar.html>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_UNLOAD.html>

S3:

- <https://aws.amazon.com/about-aws/whats-new/2024/08/amazon-s3-conditional-writes>
- <https://aws.amazon.com/about-aws/whats-new/2024/11/amazon-s3-functionality-conditional-writes>
- <https://docs.aws.amazon.com/boto3/latest/reference/services/s3/client/put_object.html>
- <https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes-enforce.html>
- <https://aws.amazon.com/about-aws/whats-new/2024/11/amazon-s3-enforcement-conditional-write-operations-general-purpose-buckets/>

## DuckDB

O DuckDB é um banco analítico embutido no processo Python, sem servidor. Esta seção descreve como
ele organiza os dados, os tipos que interessam ao contrato, o DDL, a manipulação de dados com pandas e
Arrow, a ingestão em volume, a exportação para Parquet, as recomendações de performance e o suporte a
SQLAlchemy. O papel do DuckDB no projeto é o sandbox da execução (`serialize_db.execution`), um
banco em memória ou num arquivo local que recebe os dados novos, roda as auditorias e exporta as
partições aprovadas. Com
`memory_limit`, as operações maiores que a memória vão para `temp_directory`, no EBS do espaço.

As afirmações sobre comportamento vêm da documentação oficial. Os exemplos e as medições foram
executados em 2026-09-18 com DuckDB 1.5.5, pandas 3.0.6, pyarrow 25.0.1, polars 1.44.2, SQLAlchemy
2.0.54 e duckdb_engine 0.17.0, sobre a mesma amostra do [Parquet](#parquet):
300.000 linhas da tabela `operacoes`, com as colunas `id_operacao`, `data_ref`, `id_cliente`, `valor`
e `descricao`.

### Organização dos dados

#### Armazenamento colunar em row groups

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

#### Diferenças para o PostgreSQL

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

#### Efeitos nas formas de manipular os dados

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

### Tipos suportados

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

#### DECIMAL com escala fixa

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
[SQLAlchemy](#sqlalchemy) e o modelo `Operacao` da seção sobre o suporte a SQLAlchemy, adiante:

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

#### JSON

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

No contrato ([tabela de mapeamento de tipos][tipos]), a coluna é
`sa.JSON().with_variant(SUPER(), "redshift")`, que compila `JSON` no DuckDB, e o texto JSON é a forma de troca. O DuckDB é o único ponto da cadeia que
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

### DDL

#### CREATE TABLE

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
`Operacao` e o caso do `SERIAL` estão na seção sobre o suporte a SQLAlchemy, adiante.

#### ALTER TABLE

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

#### DROP TABLE

```sql
DROP TABLE IF EXISTS operacoes_antigas;
DROP SCHEMA execucao_abc123 CASCADE;
```

`RESTRICT`, o padrão, recusa remover um objeto com dependentes rastreados (esquema com tabelas,
sequências, tipos, views); `CASCADE` remove todos. Views não são rastreadas: uma view sobre tabela
removida fica inválida e falha na consulta. O espaço da tabela vira blocos livres do arquivo, visíveis
em `PRAGMA database_size`; o arquivo não diminui. Para encolher, o caminho é copiar o banco para um
arquivo novo com `COPY FROM DATABASE origem TO destino` ou exportar e importar.

#### Índices, chaves e restrições

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

A política de restrições do projeto (`serialize_db.schema.ddl`) omite `PRIMARY KEY`, `UNIQUE` e
`FOREIGN KEY` no sandbox e verifica unicidade na auditoria. A medição da seção de ingestão confirma o custo: a carga de
300.000 linhas via Arrow levou 0,008 s sem chave e 0,073 s com `PRIMARY KEY (id_operacao, data_ref)`.

Restrições existentes se comportam como no PostgreSQL, com dois limites: um `UPDATE` que passa por
chave é verificado bloco a bloco, e `UPDATE my_table SET i = i + 1` numa tabela com 3.000 chaves
sequenciais falha com `Duplicate key "i: 2048"`; uma tabela com chave estrangeira pode acusar violação
ao atualizar um valor aninhado na tabela referenciada. O contorno documentado é `DELETE ... RETURNING`
seguido de `INSERT` na mesma transação.

#### Documentação do esquema

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

### SELECT, INSERT, UPDATE e DELETE

A conexão Python (`duckdb.connect()`) enxerga DataFrames pandas e polars, tabelas, datasets,
scanners e `RecordBatchReader` do Arrow pelo nome da variável Python, como se fossem tabelas
(replacement scan). Objetos guardados em dicionários ou atributos entram com
`con.register('nome', obj)`. A precedência é: objetos registrados, tabelas e views do banco,
variáveis Python. `SET python_enable_replacements = false` desliga a busca por variáveis. Esses
objetos são só de leitura: `INSERT` e `UPDATE` sobre um DataFrame não existem.

#### SELECT com saída em Arrow e pandas

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
enquanto a consulta continua (2026-09-23, `tests/test_engine_duckdb.py`).

Parâmetros: `?` posicional, `$1` numerado e reutilizável, `$nome` nomeado com um dicionário. A API
relacional (`con.sql(...)`, `con.table('operacoes').filter(...)`) monta consultas preguiçosas e
compõe relações por nome.

#### INSERT

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
que é nativo e não traz a leitura antecipada de um gerador Python (2026-09-22,
`tests/proof_of_concept/test_duckdb.py`, `tests/test_engine_duckdb.py`).

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

#### UPDATE e DELETE

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

### Ingestão de dados

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

A fonte da verdade é a tabela Delta ([Delta Lake](#delta-lake)), e o sandbox recebe só os meses da
execução:

```python
# Meses da tabela Delta carregados no sandbox: snapshot fixado, filtro por partição e tabela local.
import duckdb

uri = "s3://bucket/prd/cad_operacoes"                 # ou o caminho de uma pasta local
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

### Exportação para Parquet

O `COPY ... TO` grava Parquet com escritor paralelo. As opções relevantes ao projeto, detalhadas no
[Parquet](#parquet):

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
  inteiro, sem truncar, e uma coluna só de nulos não traz `min` nem `max` (2026-09-22).
- `WRITE_BLOOM_FILTER` (padrão verdadeiro) grava filtros Bloom para colunas codificadas por
  dicionário; `PARQUET_VERSION V2` habilita as codificações mais novas.
- O escritor da versão 1.5.5 declara todas as colunas como `optional`, mesmo `NOT NULL`, grava
  `DECIMAL(18, 2)` como `INT64` e não grava page index, como mostram os metadados lidos no
  [Parquet](#parquet). O leitor de outros sistemas recebe essas propriedades; a
  conferência de `NOT NULL` fica na auditoria.

A API relacional oferece o atalho `con.sql(query).write_parquet('arquivo.parquet')`. A
[gravação com ordenação](#filtros-por-chaves) pela chave de ordenação melhora a compressão e a poda por
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
aceito dentro da consulta do `COPY`. É o `COPY` de
`serialize_db.engine.duckdb.DuckDBEngine.export_partition`, e os mesmos valores alimentam a
`AddAction` do registro do arquivo na tabela Delta ([Delta Lake](#delta-lake)).

### Recomendações de performance

#### Ingestão

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
  para 50 a 60 % da RAM e desligar `preserve_insertion_order`. O guia de falta de memória da versão
  1.4 separa a `OutOfMemoryException` do DuckDB (`failed to pin block of size ...`) do processo
  morto pelo sistema, com `Killed` no terminal, e é para o segundo caso que pede de 50% a 60%:
  parte das operações foge do gerenciador de buffers e reserva memória além do limite. Os índices
  ART ficam fora dele, e `list()` e `string_agg()` não transbordam para o disco. A ordenação
  refeita na versão 1.4 usa o layout de páginas que transborda do hash join e da agregação, grava os
  runs ordenados no disco página a página e os junta por k-way merge; no DuckDB 1.5.5, o RSS do
  `COPY` ordenado de 52.654.607 linhas passou do limite em 13% a 21% (2026-09-24).
- Disco: SSD ou NVMe; EBS serve; a documentação desaconselha o formato nativo em modo leitura e
  escrita sobre NFS e SMB.

Os limites entram na abertura da conexão ou por `SET`. A máquina muda de tamanho, e o pacote os
lê do ambiente na abertura, nunca de um valor fixo (instrução do usuário de 2026-09-24):
`environment_limits()`, de `serialize_db.resources`, que `serialize_db.engine.duckdb` publica, dá
`threads` igual às CPUs que o processo pode usar e `memory_limit` igual a metade da memória que ele
ainda pode usar, pelas leituras de `serialize_db.engine.duckdb.environment_limits`; o motor e as
conexões do DuckDB de `serialize_db.delta` a aplicam, e a memória lida abaixo de 2 MiB, ou
negativa, é recusada com `SandboxError`, porque o DuckDB 1.5.5 não abre com `0MiB` e lê um valor
negativo como o padrão dele, 80% da memória da máquina (sonda de 2026-10-01). Num contêiner Linux
de 4 vCPUs com limite de cgroup de 13,4 GiB (2026-09-24):

```python
# Memória, threads e pasta de transbordo definidos na abertura da conexão e conferidos no catálogo.
import duckdb
from serialize_db.engine.duckdb import environment_limits

limits = environment_limits()   # as CPUs do processo e metade da memória que ele ainda pode usar
# {'threads': 4, 'memory_limit': '6771MiB'}
con = duckdb.connect("sandbox.duckdb", config={
    **limits,
    "temp_directory": "sandbox_tmp",          # transbordo das operações maiores que a memória
    "preserve_insertion_order": False,        # libera o reordenamento e reduz a memória
})
con.sql("SELECT name, value FROM duckdb_settings() WHERE name IN "
        "('memory_limit', 'threads', 'temp_directory', 'preserve_insertion_order') ORDER BY name").fetchall()
# [('memory_limit', '6.6 GiB'), ('preserve_insertion_order', 'false'), ('temp_directory', 'sandbox_tmp'), ('threads', '4')]
con.execute("SET threads = 2; SET memory_limit = '4GB'")     # ajuste em tempo de execução
con.sql("SELECT current_setting('threads'), current_setting('memory_limit')").fetchone()   # (2, '3.7 GiB')
```

O `memory_limit` só aceita valor com unidade: `'60%'` e `'60'` são recusados com `Parser Error:
Unknown unit for memory` (DuckDB 1.5.5, 2026-09-22), e quem quer uma fração da máquina a calcula
antes. O padrão é 80% da memória que o DuckDB detecta: 14,3 GiB numa máquina que o `os.sysconf` do
Python lê como 18,0 GiB, e 6,1 GiB dos 7,6 GiB do ambiente alvo. Desde a versão 1.3 (a PR
duckdb/duckdb#16608, de 2025-03-14), a detecção lê o limite de memória do cgroup, em
`memory.limit_in_bytes` no v1 e em `memory.max` no v2, e o padrão de `threads` lê a cota de CPU,
`cpu.cfs_quota_us` sobre `cpu.cfs_period_us` ou `cpu.max`, arredondada para cima; a versão 1.1.3
lia a memória da máquina hospedeira dentro do contêiner (duckdb/duckdb#15080). No contêiner de 4
vCPUs com limite de cgroup v1 de 14.345.912.320 bytes, o DuckDB 1.5.5 escolheu 10,6 GiB, 80% do
limite, e 4 threads (2026-09-24). Os 80% supõem o DuckDB sozinho no processo e na máquina: o
PyArrow, o delta-rs e o código do cliente ficam fora do limite, e um processo com o limite
anterior ainda aberto segura a memória dele.

O DuckDB só devolve ao sistema a memória de uma conexão quando ela fecha: depois de um
`CREATE TABLE ... AS SELECT ... ORDER BY` de 20.000.000 de linhas de um Parquet local, o RSS do
processo ficou em 1.188 MB, continuou em 1.188 MB depois do `DROP TABLE` e voltou a 208 MB no
`close` (partindo de 191 MB, 2026-09-24). Um script que roda consultas pesadas por unidade de
trabalho e mede outro processo entre elas fecha a conexão por unidade.

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

#### Organização das tabelas para filtros por chave e joins

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

#### Diferenças de abordagem em relação a bancos relacionais tradicionais

Num banco relacional de linha, a performance vem de índices, de transações curtas e de atualizações
pontuais. No DuckDB, ela vem da ordenação física dos dados, dos tipos, do tamanho dos lotes e da
memória disponível. Não há tuning de índices para joins, não há `VACUUM` para recuperar espaço, e as
tabelas se comportam melhor quando reconstruídas ou substituídas por partição do que quando
atualizadas linha a linha. O único processo escritor e o controle otimista de concorrência
dispensam bloqueios, mas exigem reexecutar a transação em caso de conflito. As restrições existem,
porém custam na carga e não ajudam nas consultas, o que inverte a prática habitual de declarar todas
as chaves.

### Suporte a SQLAlchemy

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

#### Modelo, DDL e consulta que devolve um DataFrame

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

#### Ingestão de um DataFrame numa tabela definida pelo ORM

```python
import pyarrow as pa
from sqlalchemy import insert
from sqlalchemy.orm import Session

def arrow_schema(model) -> pa.Schema:
    """Esquema Arrow derivado das colunas do modelo (serialize_db.schema.arrow_schema)."""
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

### Referências

- Documentação do DuckDB, versão 1.5: <https://duckdb.org/docs/current/>. Páginas usadas: formato de
  armazenamento, vetores, tipos de dados (visão geral, numéricos, texto, JSON), instruções `CREATE
  TABLE`, `ALTER TABLE`, `DROP`, `COMMENT ON`, `CREATE INDEX`, `CREATE SEQUENCE`, `INSERT`,
  `UPDATE`, `DELETE`, `MERGE INTO`, transações, concorrência, restrições, índices, compatibilidade
  com PostgreSQL, guias de performance (esquema, indexação, joins, ajuste de cargas, ambiente,
  memória, falta de memória), guias do cliente Python (ingestão, conversão, pandas, Arrow, polars,
  DB-API, tipos) e `COPY`.
- Blog do DuckDB: a gestão de memória (<https://duckdb.org/2024/07/09/memory-management>), a versão
  1.3 com o cache de arquivos externos (<https://duckdb.org/2025/05/21/announcing-duckdb-130>) e a
  ordenação refeita da versão 1.4 (<https://duckdb.org/2025/09/24/sorting-again>).
- Documentação do SQLAlchemy 2.0: <https://docs.sqlalchemy.org/en/20/>.
- Repositório do dialeto `duckdb_engine`: <https://github.com/Mause/duckdb_engine>.
- Documentação do pandas sobre `read_sql`, `to_sql` e o backend pyarrow:
  <https://pandas.pydata.org/docs/>.
- Documentação do PyArrow: <https://arrow.apache.org/docs/python/>.
- Tutorial de referência do projeto: <https://github.com/felipenoris/etl-cookbook-tutorial>.
- Lista completa das páginas consultadas: [REFERENCES.md][references].

## Redshift

O Amazon Redshift é o banco de publicação do projeto e uma das duas engines de execução do pipeline.
Esta seção descreve a conectividade do ambiente alvo, como ele organiza os dados, os tipos que
interessam ao contrato, o DDL, a manipulação de dados com pandas e Arrow, a ingestão em volume, a
exportação para Parquet, as recomendações de performance e o suporte a SQLAlchemy. As afirmações vêm
da documentação oficial, consultada em 2026-09-18 e, na seção de conectividade, em 2026-09-20, e do
código dos pacotes `redshift_connector` 2.1.16, `sqlalchemy-redshift` 1.0.0 e `awswrangler` 3.17.1.
Os dois caminhos de conexão rodaram no ambiente alvo em 2026-09-20, e `probes/redshift.py` repete
as mesmas chamadas; os exemplos de SQLAlchemy foram compilados sem conexão a um cluster.

### Comandos utilitários para diagnóstico

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
| `SELECT pg_last_unload_count();` | Linhas descarregadas pelo último `UNLOAD` concluído na sessão; `0` sem `UNLOAD` concluído ou quando o último falhou durante a descarga. |
| `SELECT * FROM sys_load_error_detail ORDER BY start_time DESC LIMIT 20;` | Erros de carga, inclusive em workgroups serverless; `stl_load_errors` cobre só clusters provisionados e é negada a um usuário comum no ambiente alvo (SQLSTATE 42501, leitura de 2026-09-20), assim como `stv_slices`. |
| `EXPLAIN <consulta>;` | Plano de execução, com os rótulos de redistribuição `DS_DIST_*`. |

A biblioteca não lê `pg_last_copy_count()` nem `sys_load_error_detail`: cada `COPY` dela lê um
manifesto de entradas `mandatory`, e o arquivo ausente ou o erro de carga sobem do próprio `COPY`.
A função abaixo lê os dois na mesma conexão, depois de um `COPY`, e não foi executada:

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

### Conectividade no ambiente alvo

O ambiente alvo expõe um workgroup serverless, e o esquema do projeto vem de um datashare; os dois
caminhos de conexão rodaram lá em 2026-09-20.

#### Credencial temporária do workgroup

`redshift-serverless:GetWorkgroup` devolve o endereço e a porta do endpoint, e
`redshift-serverless:GetCredentials` devolve o par usuário e senha derivado da identidade IAM de
quem chama. Não há senha
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

#### Os limites de sessão do serverless

O Redshift Serverless encerra por `SESSION TIMEOUT` a sessão ociosa há mais de 3.600 s e a transação
aberta e inativa há mais de 21.600 s, e interrompe a consulta que passa de 86.399 s;
`ALTER USER ... SESSION TIMEOUT` troca o limite por usuário, de 60 s a 20 dias, só para sessões
novas, e exige superusuário ou o privilégio `ALTER USER`; `stv_sessions` mostra o limite da sessão
corrente. Toda consulta conta como atividade cobrada, uma de keepalive inclusive, com o mínimo de
60 s de RPU (páginas de faturamento, de considerações e de `ALTER USER` do serverless, lidas em
2026-09-22). O motor Redshift (`serialize_db.engine.redshift.RedshiftEngine`) guarda uma sessão
por execução: uma fase do pipeline fora do banco mais longa que uma hora perde a sessão, o motor
reconecta no comando seguinte, e o que se perde é a tabela temporária que o pipeline tenha criado
nela; as `exec_<id>_*` são permanentes. Numa transação que o cliente deixou aberta, o comando
seguinte levanta o `InterfaceError` do driver, porque a transação se perdeu, e o outro reconecta.

#### O cache de prepared statements e a leitura do resultado no driver

O `redshift_connector` (2.1.16) guarda um prepared statement nomeado por texto de comando
(`Connection.execute`, chave `(operation, params)`) e o reaproveita no `execute` seguinte do mesmo
texto, com `Bind` e `Execute` sem novo `Parse`; ele fecha e descarta os guardados quando o servidor
confirma um `ALTER`, `CREATE`, `DROP` ou `ROLLBACK` (`handle_COMMAND_COMPLETE`), e não depois de
`TRUNCATE`. Numa tabela do datashare, o `Execute` de um statement preparado antes de um `TRUNCATE`
da tabela recebeu `XX000`, `[Data Sharing] Error Code 34510: Concurrent DDL committed on
datalake_rw_shared.sbx_aco_decon.<tabela> between Prepare and Execute` (rotina
`relocalize_data_sharing_cached_rtes`), nas duas execuções da suíte de 2026-09-21 às 12:08 e às
12:10: o datashare recusa o statement preparado antes do DDL em vez de
replanejar. Com `max_prepared_statements=0` o driver usa o statement sem nome, preparado logo antes
de cada execução, e não guarda nada; a suíte e a biblioteca conectam assim, e a suíte passou limpa
às 13:35 e às 13:39 do mesmo dia. Na conexão com o cache, a repetição depois do `TRUNCATE` e a
segunda repetição receberam `34510` (a entrada guardada fica até um comando que o driver reconhece),
a repetição depois de um `ALTER TABLE ... ADD COLUMN` passou, e a mesma sequência numa tabela
temporária criada depois do `USE` passou: a recusa é do datashare. Um DDL de outra sessão sobre a
mesma tabela tem o mesmo efeito num statement guardado, e o driver não o enxerga.

O `execute` lê o resultado inteiro antes de devolver: `EXECUTE_MSG` pede o portal sem limite de
linhas, `handle_messages` só termina em `READY_FOR_QUERY`, cada `DATA_ROW` entra em
`cursor._cached_rows`, e `fetchmany` fatia essa fila (`Cursor.__next__`). A memória de uma consulta
é a do resultado inteiro em objetos Python, antes do primeiro `fetchmany` (a suíte leu as 5 linhas
na fila em 2026-09-21, às 13:35 e às 13:39); o `stream` do motor Redshift vai sempre por `UNLOAD`,
e o `query` pelo cursor.

O `cursor.description` do driver devolve só o nome e o OID de cada coluna, `(nome, oid, None, None,
None, None, None)`. O `type_modifier` que a mensagem `RowDescription` traz, com a precisão e a
escala do `NUMERIC`, fica em `cursor.ps["row_desc"]`, e o próprio driver o usa para decodificar o
`NUMERIC` binário, com a escala `(type_modifier - 4) & 0xFFFF` (`Cursor.truncated_row_desc`,
leitura do código do 2.1.16 em 2026-09-23). O `SUPER` chega como texto.

#### Data API

A Data API executa SQL por HTTPS, sem a porta 5439, e é assíncrona: `ExecuteStatement` devolve o
identificador na hora, `DescribeStatement` é consultado até o estado ser `FINISHED`, `FAILED` ou
`ABORTED`, e `GetStatementResult` devolve o resultado paginado
(caminho que rodou no ambiente alvo em 2026-09-20). Ela serve a comandos e a
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

#### O esquema do projeto num banco de datashare

O esquema do projeto está num banco de datashare. Uma sessão conectada ao banco local cita a tabela
por nome em três partes, `banco.esquema.tabela`; `USE <banco>` troca o banco da sessão, e a partir
dele `esquema.tabela` basta, que é como o `CREATE TABLE`, o `COPY` e o `UNLOAD` passaram no ambiente
alvo em 2026-09-20. A restrição
documentada, de que só o nome em três partes vale, se aplica a quem não está conectado ao banco
compartilhado. `current_database()` continua a responder o banco da conexão depois do `USE` (leitura de
2026-09-21 no ambiente alvo por `probes/redshift.py`, confirmada pelo usuário no mesmo dia): a
troca é confirmada pela resolução de um nome em duas partes, como o `CREATE TABLE` dos exemplos,
e não por essa função. `svv_redshift_databases` diz o tipo de
cada banco (`local` ou `shared`) e o nível de isolamento; `svv_all_schemas` diz em que banco está
cada esquema. `has_schema_privilege` e `svv_table_info` enxergam o banco da sessão: antes do `USE`,
o local, e num esquema compartilhado quem concede `USAGE` e `CREATE` é o produtor e a lista de
tabelas vem de `svv_all_tables`, que cruza bancos. Depois do `USE`,
`has_schema_privilege('sbx_aco_decon', 'CREATE')` respondeu `false`, sem erro, no esquema em que o
`CREATE TABLE` passa (suíte de 2026-09-21 no ambiente alvo, seis execuções): a
função não serve de teste do privilégio num esquema de datashare, e a prova é o próprio `CREATE`.
`information_schema.columns` também enxerga só o banco da conexão: depois do `USE` respondeu vazio
para uma tabela recém-criada em `sbx_aco_decon` (suíte, 2026-09-21, cinco execuções); `svv_all_columns` cruza os
bancos, e o `cursor.description` de um `select ... limit 0` descreve a tabela sem visão de catálogo.
Depois do `USE`, `svv_table_info` respondeu `permission denied for relation svv_table_info`
(42501) ao papel do projeto (`probes/redshift.py`, `RS-8`, 2026-09-23).

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
  `PARTITION BY (<coluna>) MANIFEST VERBOSE` em 2026-09-21, este em 0,8 s sobre 500.000 linhas.
- `COPY ... FORMAT AS PARQUET MANIFEST` passou numa tabela do datashare em 2026-09-21, 500.000
  linhas em 4,6 s a partir de um manifesto de uma entrada apontando para um arquivo gravado pelo
  delta-rs: o manifesto se comporta como numa tabela local.
- A escrita de uma transação vai para um banco só, e um comando múltiplo fora de um bloco de
  transação não é aceito: a transação da publicação abre com `BEGIN` explícito, e a tabela de
  controle mora no mesmo banco das tabelas publicadas. A regra é da página
  [Considerations for data sharing reads and writes](https://docs.aws.amazon.com/redshift/latest/dg/considerations-datashare-reads-writes.html)
  ("Note that the writes in a transaction are only supported to a single database", relida em
  2026-09-23), e nenhuma execução no ambiente alvo a mediu: a página não diz em que banco fica uma
  tabela temporária criada depois do `USE`, nem se escrever nela conta como escrever noutro banco.
- `VIEW` e `MATERIALIZED VIEW` não podem ser criadas, alteradas nem apagadas num banco de datashare.
- `TRUNCATE` numa tabela remota é transacional, ao contrário do `TRUNCATE` local, que confirma
  sozinho. Ele é DDL para o datashare: um comando preparado antes dele e executado depois recebe
  `34510`, `Concurrent DDL committed ... between Prepare and Execute` (2026-09-21), o que o cache de
  prepared statements do `redshift_connector` produz sozinho (seção "O cache de prepared statements
  e a leitura do resultado no driver"); uma tabela temporária criada depois do `USE` aceita a mesma
  sequência.
- O consumidor não altera nem apaga o datashare, e não põe um objeto dele em outro datashare.

#### O S3 alcançado pelas credenciais de quem chama

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

O que o ambiente alvo respondeu a esses requisitos, lido em 2026-09-20 por `probes/redshift.py`:
o patch `1.0.436211` atende; o isolamento do banco
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

### Organização dos dados

#### Armazenamento colunar e processamento paralelo

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

#### Diferenças para o PostgreSQL

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
| `NaN` em `DOUBLE PRECISION` | Igual a si mesmo e acima de todo número. | Igual a si mesmo numa constante; diferente de tudo na varredura de uma tabela, como no IEEE: `x NOT IN ('NaN'::float8)` deixa passar o `NaN` da tabela (2026-09-23). |
| Tamanho de comando | Sem limite prático. | 16 MB por comando SQL; 4 MB por linha de entrada no `COPY`. |
| Identificadores | 63 bytes. | 127 bytes; 1.600 colunas por tabela. |
| Subconsultas em `INSERT ... VALUES` de várias linhas | Aceitas. | Rejeitadas. |
| `UPDATE ... FROM` com outer join | Aceito. | Rejeitado; a alternativa é subconsulta no `WHERE`. |

#### Efeitos nas formas de manipular os dados

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

### Tipos suportados

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
na [tabela de mapeamento de tipos][tipos].

O tipo que o dialeto `sqlalchemy-redshift` emite no DDL para cada tipo do contrato, compilado nesta
sessão. `dialect` e `sql()` servem aos exemplos seguintes desta seção:

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
`VARCHAR(36)` do contrato para `Uuid` exige `String(36)` ou uma regra `@compiles`. A biblioteca não
compila o DDL pelo dialeto: `serialize_db.schema.sql_type` escreve `VARCHAR(65535)` para `Text`,
`VARCHAR(36)` para `Uuid` e `SUPER` para `JSON`, e recusa `LargeBinary`.

#### DECIMAL com escala fixa

`DECIMAL(precisao, escala)` guarda até 38 dígitos; a precisão padrão é 18 e a escala padrão é 0, e
a escala vai de 0 à precisão, até 37. A representação depende da precisão: até 19 dígitos, inteiro
de 8 bytes; de 20 a 38, inteiro de 16 bytes, que ocupa o dobro em disco e torna as consultas mais
lentas. A documentação pede que a precisão máxima não seja atribuída sem necessidade.
`DECIMAL(18, 2)`, o tipo do contrato para valores contábeis, fica em 8 bytes nos dois bancos; o
`decimal128(18, 2)` do Arrow, que o representa nos arquivos e na memória, ocupa 16 bytes por valor.

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

A carga por `COPY` de Parquet exige um `DECIMAL` do arquivo compatível com a coluna. A seção
[COPY a partir de Parquet](#copy-a-partir-de-parquet) registra a correspondência lida no ambiente
alvo em 2026-09-21: `DECIMAL(18, 2)` em `INT64` e `TIMESTAMP` em `INT64` de microssegundos carregam, e
`FIXED_LEN_BYTE_ARRAY` fica sem leitura.

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

#### JSON e o tipo SUPER

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

No contrato ([tabela de mapeamento de tipos][tipos]), a coluna é
`sa.JSON().with_variant(SUPER(), "redshift")`: o dialeto compila `SUPER` no `CREATE TABLE`, e o `duckdb_engine` compila `JSON`. Os arquivos do Delta
trazem o documento como texto; a carga passa pela staging `VARCHAR(65535)` e o `INSERT ... SELECT`
aplica `JSON_PARSE`; a exportação devolve texto com `JSON_SERIALIZE`. O `COPY` de Parquet com a
coluna em texto numa coluna `SUPER` é recusado sem `SERIALIZETOJSON` (`SUPER column in COPY query
requires SERIALIZETOJSON option`, ambiente alvo, 2026-09-21), e um documento de 80.901 bytes entrou
por `INSERT ... JSON_PARSE(%s)` com parâmetro, acima do teto do `VARCHAR`. Com
`SERIALIZETOJSON`, o mesmo `COPY` recusa a string acima do teto (`1224 String value exceeds the max
size of 65535 bytes`): um Parquet com o documento em texto não leva um documento acima de 65.535
bytes a `SUPER`. `COPY ... FORMAT JSON 'auto'` de um arquivo JSON de uma linha, com o documento como
objeto, carregou os 80.901 bytes (`json_typeof` `object`), lido às 13:35 e às 13:39. O contrato
fixa o teto de 65.535 bytes por documento, recusado pelo `cast` e contado pela auditoria; os
caminhos por JSON ficam para uma tabela que precise de documentos maiores.

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

load_sql = """INSERT INTO prd_eventos (id_evento, meta, mes)
SELECT id_evento, JSON_PARSE(meta), '2026-08' FROM staging_eventos"""
unload_sql = """UNLOAD ('SELECT id_evento, JSON_SERIALIZE(meta) AS meta, mes FROM exec_42_eventos')
TO 's3://bucket/prd/eventos/' IAM_ROLE 'arn:aws:iam::123456789012:role/papel'
FORMAT AS PARQUET PARTITION BY (mes) MANIFEST VERBOSE"""
```

### DDL

#### CREATE TABLE

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

`TO` e `TIMESTAMP` estão na lista de palavras reservadas do Redshift, e são colunas do modelo
cliente: o script que rodou no ambiente alvo cria `cad_contratos` com `"to"` entre aspas, o
dialeto do SQLAlchemy cita `"to"` e `"timestamp"` sozinho, inclusive em `DISTKEY` e `SORTKEY`, e a biblioteca
cita todo identificador que emite (`serialize_db.schema.quoted`).

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
- Tabelas temporárias vivem num esquema da sessão, o primeiro do `search_path`, morrem com ela,
  aceitam nome igual ao de uma permanente, que fica à sombra até ser qualificada pelo esquema, e
  recebem codificação `RAW` por padrão, salvo `ENCODE` por coluna; `svv_table_info` não as lista. Um
  nome iniciado por `#` cria uma tabela temporária. O sandbox da biblioteca não as usa; o pipeline
  pode criá-las na sessão única do motor.
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

#### ALTER TABLE

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
  Ele também não está na lista do que a escrita por datashare aceita; a
  publicação (`serialize_db.publication`) trata a largura que muda como diff destrutivo, e
  `test_redshift.py::test_alter_column_type_on_the_share` lê o comando no esquema do datashare.
- `ALTER DISTKEY`, `ALTER DISTSTYLE` e `ALTER SORTKEY` não rodam junto com `VACUUM`, não valem para
  tabelas temporárias nem com chave interleaved, e retiram a tabela da otimização automática quando
  ela estava em `AUTO`. Uma chave compound pode virar interleaved apenas recriando a tabela.
- `ADD CONSTRAINT PRIMARY KEY` exige colunas `NOT NULL`. Os nomes das restrições estão em
  `information_schema.table_constraints`.
- Combinações num só comando reduzem o tempo: `ALTER SORTKEY (...), ALTER DISTKEY coluna`.

O SQLAlchemy não tem construto para `ADD COLUMN`. Sem Alembic, a biblioteca monta esse comando como
texto, com a especificação da coluna por `serialize_db.schema.column_ddl`, a mesma do `CREATE
TABLE`; o compilador do dialeto também a rende, com `DEFAULT` e
`ENCODE`, e o Core tem os construtos para restrições e `sa.DDL` para as demais cláusulas:

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

#### DROP TABLE

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

#### Chaves, restrições e índices

Não há índices. `PRIMARY KEY`, `UNIQUE` e `FOREIGN KEY` são declaradas para o planejador, que as usa
para decorrelacionar subconsultas, ordenar e eliminar joins, e supõe que valem: uma chave primária
duplicada faz `SELECT DISTINCT` devolver duplicatas. A regra da documentação é declará-las apenas
quando o processo de carga garante a integridade, e a política de restrições do
projeto declara só a `PRIMARY KEY` da tabela publicada, por `published_ddl`; o sandbox não
declara chave, e a auditoria confere as chaves do modelo. `NOT NULL` é aplicado e vale como
validação de carga: um `COPY` que tenta gravar `NULL` numa coluna `NOT NULL` falha.

#### Documentação do esquema

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

### SELECT, INSERT, UPDATE e DELETE

Todo comando tem 16 MB no máximo. Os dados de entrada e saída do pipeline são DataFrames pandas com
backend pyarrow ou tabelas Arrow; a conexão de referência é o `redshift_connector`.

#### SELECT

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
[SQLAlchemy](#sqlalchemy)):

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

#### INSERT

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

#### UPDATE, DELETE e MERGE

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

#### Transações concorrentes

O Redshift aceita escritas concorrentes com locks de tabela e isolamento serializável. Duas
transações são concorrentes quando a segunda começa antes de a primeira confirmar, e cada uma
trabalha sobre o snapshot confirmado quando ela o tomou. O que a documentação diz, lida em
2026-09-23:

- O snapshot nasce no primeiro `SELECT`, no primeiro DML (`COPY`, `DELETE`, `INSERT`, `UPDATE`,
  `TRUNCATE`) ou no primeiro `ALTER TABLE`, `CREATE TABLE`, `DROP TABLE` ou `TRUNCATE TABLE` da
  transação, não no `BEGIN`.
- Os níveis são `SNAPSHOT`, o padrão de clusters e workgroups novos, e `SERIALIZABLE`. Sob o
  snapshot, dois `UPDATE` de linhas distintas da mesma tabela confirmam os dois; sob o
  serializável, o segundo é abortado com `ERROR:1023 DETAIL: Serializable isolation violation on
  table`. `STV_DB_ISOLATION_LEVEL` informa o nível de cada banco, e a escrita por datashare exige
  isolamento de snapshot no banco do produtor.
- `DELETE` e `UPDATE` concorrentes na mesma tabela esperam nos dois níveis: o segundo roda depois
  que o primeiro solta o lock, e o snapshot dele nasce depois disso quando o comando é o primeiro da
  transação. `COPY` e `INSERT` concorrentes na mesma tabela correm juntos sob o snapshot até os dois
  precisarem gravar, e daí em sequência; sob o serializável, o segundo espera o primeiro.
- Uma transação solta os locks de todas as tabelas de uma vez, no fim. Duas transações que escrevem
  as mesmas tabelas em ordens diferentes podem travar uma à outra, e sob o snapshot também
  `INSERT` ou `COPY` concorrentes seguidos, numa delas, de `UPDATE`, `DELETE`, `MERGE` ou DDL na
  mesma tabela. A documentação manda escrever as tabelas sempre na mesma ordem, separar o comando
  que pede o lock exclusivo numa transação própria, ou repetir a transação.
- `LOCK <tabela>` toma o lock `ACCESS EXCLUSIVE` até o fim da transação e é a forma documentada de
  forçar a ordem: todas as tabelas da transação, sempre na mesma ordem, no início dela. O `LOCK` não
  está na lista de comandos que a escrita por datashare aceita num consumidor.
- `1018 Relation does not exist` é a transação lendo uma tabela que outra criou depois do snapshot
  dela; as tabelas de catálogo (`pg_*`) não seguem o isolamento das tabelas do usuário.

A tabela de controle `serialize_db_publications` é a única que dois ambientes escrevem
(`serialize_db.publication.publish_redshift`), e o banco do datashare informou isolamento `UNKNOWN` em
`svv_redshift_databases` (2026-09-20). `tests/proof_of_concept/test_redshift_transactions.py` leu
no esquema do datashare, em duas execuções de 2026-09-23: escritas em tabelas
distintas não esperam; linhas distintas da tabela de controle confirmam as duas transações, e o
`DELETE` da segunda espera o `COMMIT` da primeira; o `CREATE TABLE` de um nome que outra transação
criou espera o `COMMIT` dela; o `DELETE` das linhas que outra transação confirmada trocou recebe
`1023 Serializable isolation violation`; o `LOCK` é recusado (`0A000 Operation is not supported
through datashares`); e o `UPDATE` condicionado à versão lida espera o `COMMIT` da outra e afeta 0
linhas. A publicação lê a linha de controle no início da transação e a grava no fim.

### Ingestão de dados

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
verdadeiro; a documentação do `COPY` descreve a lista de colunas apenas para arquivos planos, e o
ambiente alvo a aceitou com Parquet em 2026-09-21 (tabela "Regras do COPY para Parquet"). O driver
ADBC para Redshift (versão 1.7.0, 2026-09-09) faz ingestão em bloco e leitura em
Arrow e merece um benchmark contra o fluxo acima.

#### Regras do COPY para Parquet

| Regra da documentação | Consequência para a biblioteca |
| --- | --- |
| Colunas são associadas por posição, e a quantidade precisa coincidir com a tabela. | Os arquivos de uma tabela Delta nem sempre têm as colunas do modelo na ordem dele: o anterior a uma coluna nova não a tem, e o delta-rs grava a coluna nova no fim. Todo `COPY` da biblioteca passa a lista das colunas do arquivo, a regra seguinte. Lido no ambiente alvo em 2026-09-21: um arquivo de cinco colunas numa tabela de seis reprova com `Spectrum Scan Error` 15007, `Unmatched number of columns`. |
| A lista de colunas, `COPY tabela (colunas) FROM ... FORMAT AS PARQUET MANIFEST`, é aceita (2026-09-21): as colunas do arquivo entram nas listadas, por posição, e a coluna fora da lista fica nula. `FILLRECORD` carregou o mesmo arquivo com o mesmo resultado, 100 linhas e a coluna nova nula (13:35 e 13:39). | Um arquivo anterior a uma coluna nova entra por `FILLRECORD`, que só completa as colunas do fim, ou pela lista de colunas, que exige um `COPY` por lista; nenhum dos dois fornece o valor de uma coluna ausente. `FILLRECORD` entra em todo `COPY` da biblioteca, e todo `COPY` passa também a lista das colunas do arquivo, para que uma coluna nova no meio do modelo ou uma reordenação não desloque as seguintes: o do `appender` do motor Redshift, com as colunas do primeiro lote, e o da carga do Delta, no `ingest`, no `pinned_delta` e na publicação, um por manifesto de `copy_manifest`, com as colunas do rodapé. O ambiente alvo leu cada um sozinho em 2026-09-21 e os dois juntos em 2026-09-28 (`test_appender_loads_a_batch_without_a_middle_column` e os casos da coluna nova no meio e da reordenação de `tests/test_engine_redshift.py` e `tests/test_publication.py`). |
| A lista de colunas não pode deixar de fora uma coluna `NOT NULL` sem `DEFAULT` nem nomear uma coluna que a tabela não tem: o `COPY` é recusado com `42601 NOT NULL column without DEFAULT must be included in column list` e com `42703 column "extra" of relation ... does not exist` (ambiente alvo, 2026-10-06). | O lote do `appender` sem uma coluna `NOT NULL` falha sem linha: no `COPY` direto com o `42601`, e na staging da tabela com JSON, que aceita nulo em toda coluna, no `INSERT` com `XX000 Cannot insert a NULL value into column codigo`. A coluna do Delta que o modelo não tem faz o `COPY` do `ingest` falhar com o `42703`, porque a staging é criada do modelo (`test_appender_refuses_a_batch_without_a_not_null_column` e `test_ingest_refuses_a_delta_column_outside_the_model` de `tests/test_engine_redshift.py`). |
| Só existem as colunas gravadas no arquivo. | A coluna de partição `mes` não está nos arquivos do Delta: a carga passa por uma staging sem `mes` e por `INSERT ... SELECT ..., '<mes>'` ([Delta Lake](#delta-lake)). |
| Uma string maior que o `VARCHAR` de destino aborta o `COPY` (2026-09-21): `Spectrum Scan Error` 15007, e `sys_load_error_detail` diz `The length of the data column descricao is longer than the length defined in the table. Table: 200, Data: 300`. | A auditoria de tamanho (`serialize_db.audit`) é a barreira; `TRUNCATECOLUMNS` não é aceito com Parquet (`0A000`, `TRUNCATECOLUMNS argument is not supported for PARQUET based COPY`, 2026-09-21). |
| Parâmetros aceitos: `ACCEPTINVCHARS`, `FILLRECORD`, `FROM`, `IAM_ROLE`, `STATUPDATE`, `MANIFEST`, `EXPLICIT_IDS`. `MAXERROR`, `NOLOAD` e `COMPUPDATE` não são aceitos, e não há compressão automática. | O primeiro erro aborta o `COPY`. A validação acontece antes, no Arrow. |
| `MANIFEST` é aceito. | O `COPY` carrega exatamente os arquivos gravados pela biblioteca, e não o que mais estiver na pasta: o Delta guarda as versões anteriores até o `vacuum`. Exercitado no datashare em 2026-09-21. |
| Sem `MANIFEST`, o caminho é um prefixo de chave: `custdata.txt` alcança `custdata.txt.1` e `custdata.txt.bak`, e a documentação recomenda o manifesto quando o prefixo pode alcançar arquivos indesejados. | No ambiente alvo, o `COPY` de Parquet de um caminho sem objeto saiu sem erro em 2026-09-28 [inferido: sem carregar nada]. Todo `COPY` da biblioteca lê um manifesto de entradas `mandatory`, o do `appender` do motor Redshift com a única entrada do arquivo dele, e a entrada ausente fez o `COPY` falhar no alvo com `Spectrum Scan Error: File not found` e o SQLSTATE `XX000` (2026-09-28 às 23:09). |
| O bucket precisa estar na mesma região do Redshift. | Configuração da infraestrutura. |
| O `COPY` de Parquet usa URLs pré-assinadas válidas por 1 hora. | Políticas IAM do bucket não podem bloquear URLs pré-assinadas. |
| O `COPY` grava `NULL` em coluna `NOT NULL` só se o arquivo trouxer `NULL`; a falha aborta a carga. | `NOT NULL` do modelo é a última barreira; a auditoria no Arrow vem antes. |

Com o Delta Lake como fonte da verdade ([Delta Lake](#delta-lake)), os arquivos de dados não trazem a
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

destination = Operacao.__table__.to_metadata(sa.MetaData(), name="prd_operacoes")
staging = sa.Table("prd_operacoes_staging", destination.metadata,
                   *[sa.Column(c.name, c.type, nullable=c.nullable) for c in destination.columns if c.name != "mes"],
                   prefixes=["TEMPORARY"])
transaction = [
    sa.delete(destination).where(destination.c.mes == "2026-08"),
    CreateTable(staging),
    sa.text("COPY prd_operacoes_staging FROM 's3://bucket/publicacao/exec-42/operacoes/2026-08.manifest' "
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
DELETE FROM prd_operacoes WHERE prd_operacoes.mes = '2026-08'
CREATE TEMPORARY TABLE prd_operacoes_staging (id_operacao BIGINT NOT NULL, data_ref DATE NOT NULL,
    id_cliente BIGINT NOT NULL, valor NUMERIC(18, 2) NOT NULL, descricao VARCHAR(200))
COPY prd_operacoes_staging FROM 's3://bucket/publicacao/exec-42/operacoes/2026-08.manifest'
    IAM_ROLE 'arn:aws:iam::123456789012:role/papel' FORMAT AS PARQUET MANIFEST
INSERT INTO prd_operacoes (id_operacao, data_ref, id_cliente, valor, descricao, mes)
    SELECT prd_operacoes_staging.id_operacao, prd_operacoes_staging.data_ref, prd_operacoes_staging.id_cliente,
        prd_operacoes_staging.valor, prd_operacoes_staging.descricao, '2026-08' AS mes FROM prd_operacoes_staging
```

A staging temporária dispensa a codificação e as restrições do modelo: tabelas temporárias recebem
`RAW`, e a auditoria acontece no destino. A lista de colunas no `COPY` de Parquet funciona
(2026-09-21), mas não fornece o valor da coluna ausente: a staging fica para a coluna de partição.

### Exportação para Parquet

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
  seguem a convenção Hive. Os arquivos saem `<prefixo>/<coluna>=<valor>/<slice>_part_<nn>.parquet`,
  e o número da slice muda entre execuções (`0064_part_00.parquet` e `0000_part_00.parquet` para a
  mesma tabela no ambiente alvo, 2026-09-21).
- `CLEANPATH` apaga de forma permanente os arquivos das pastas que recebem dados novos;
  `ALLOWOVERWRITE` sobrescreve arquivos existentes; sem os dois, o comando falha se o destino tiver
  arquivos. O destino é conferido como prefixo (2026-09-21): o mesmo prefixo e um prefixo pai com
  arquivos abaixo reprovam com `Specified unload destination on S3 is not empty. Consider using a
  different bucket / prefix, manually removing the target files in S3, or using the ALLOWOVERWRITE
  option`, e um subprefixo novo dentro de uma pasta com arquivos passa.
- `MANIFEST VERBOSE` lista os arquivos, os nomes e tipos das colunas, as linhas por arquivo e o
  total; o manifesto simples lista só as URLs e serve ao `COPY ... MANIFEST`.
- `PARQUET` não combina com `DELIMITER`, `FIXEDWIDTH`, `ADDQUOTES`, `ESCAPE`, `NULL AS`, `HEADER`,
  `GZIP`, `BZIP2` nem `ZSTD`; `ENCRYPTED` só com SSE-KMS.
- Colunas `TIMESTAMPTZ` perdem a informação de fuso; `VARBYTE`, `GEOMETRY` e `HLLSKETCH` só saem em
  texto ou CSV.
- O `SELECT` externo não aceita `LIMIT` (`42601 Limit clause is not supported`, 2026-09-23). O
  `SELECT` é um literal que trata a contrabarra como escape, como a documentação mostra ao
  escapar a aspa com `\'`: a aspa e a contrabarra da consulta entram dobradas. Com só a aspa
  dobrada, a contrabarra de um literal da consulta chegou sem par, e o filtro não achou a linha
  (2026-09-23).
- Um resultado vazio passa sem gravar arquivo nem manifesto (2026-09-23);
  `pg_last_unload_count()` na mesma sessão separa esse caso de um manifesto que falta.
- O Parquet é até 2 vezes mais rápido de descarregar e ocupa até 6 vezes menos espaço no S3 que
  texto.

O `UNLOAD` do projeto grava uma partição por comando, sem `PARTITION BY` e com a coluna de partição
fora do `SELECT`, num prefixo novo por tentativa dentro da pasta da partição,
`<coluna>=<valor>/<execution_id>_<uuid>/`, com `MANIFEST VERBOSE` e sem `MAXFILESIZE`, cujo padrão
é 6,2 GB. O `SELECT` lista as colunas na ordem do modelo, com o JSON serializado em texto por
`JSON_SERIALIZE`, e `ORDER BY` pela chave de ordenação. A biblioteca confere o manifesto do `UNLOAD`
e o rodapé de cada arquivo antes de registrá-los no log do Delta, e relê a versão depois (seção "O
manifesto entre o log do Delta e o Redshift"). `CLEANPATH` não é usado: arquivos de execuções
abortadas ficam fora do log e saem pelo `vacuum`.

A documentação do `UNLOAD` não informa os tipos físicos Parquet, a obrigatoriedade das colunas nem a
presença de estatísticas, e os três afetam o registro dos arquivos no log do Delta.
A leitura do rodapé de um arquivo no ambiente alvo em 2026-09-21 respondeu os três:

| Coluna do `UNLOAD` | Tipo físico Parquet | Tipo lógico |
| --- | --- | --- |
| `BIGINT` | `INT64` | `Int(bitWidth=64, isSigned=true)` |
| `DATE` | `INT32` | `Date` |
| `VARCHAR` | `BYTE_ARRAY` | `String` |
| `DOUBLE PRECISION` | `DOUBLE` | nenhum |
| `DECIMAL(18, 2)` | `FIXED_LEN_BYTE_ARRAY(8)` | `Decimal(precision=18, scale=2)` |
| `TIMESTAMP` | `INT96` | nenhum |
| `SUPER` | `BYTE_ARRAY` | `JSON`, lido pelo pyarrow como `extension<arrow.json>` com o texto de cada valor (2026-09-23) |

Toda coluna sai `optional`, inclusive as declaradas `NOT NULL` na tabela de origem: a nulidade do
Delta vem do esquema da tabela, não dos arquivos. As estatísticas de mínimo e máximo estão
presentes, o que faz valer preencher `minValues` e `maxValues` na `AddAction`. Num grupo de linhas
com `NaN`, o mínimo e o máximo do `DOUBLE PRECISION` deixam o `NaN` de fora, como no rodapé do
pyarrow, e o leitor Parquet do DuckDB poda o grupo pelo máximo e perde a linha; com os infinitos,
o rodapé os traz (2026-09-23, [issue #59](https://github.com/felipenoris/serialize-db/issues/59)).

Os dois tipos físicos que divergem do resto do projeto:

- `DECIMAL(18, 2)` sai em `FIXED_LEN_BYTE_ARRAY(8)`, como o PyArrow grava, e não em `INT64`, como
  gravam o delta-rs, o DuckLake e o DuckDB. Uma tabela Delta que
  recebe arquivos dos dois escritores fica com duas codificações físicas da mesma coluna lógica;
  os leitores leem as duas, porque o tipo lógico é o mesmo.
- `TIMESTAMP` sai em `INT96`, que o formato Parquet marca como obsoleto e que o contrato do projeto
  tira na carga inicial (`serialize_db.parquet_import`). O `UNLOAD` o traz de volta, e o `INT96` não carrega
  estatística de mínimo e máximo: a coluna de timestamp de um arquivo do `UNLOAD` não poda. Um
  arquivo `INT96` registrado numa tabela Delta declarada `timestamp_ntz` foi lido de volta pelo
  delta-rs e pelo `delta_scan` do DuckDB como `timestamp[us]`, com os valores intactos
  (2026-09-21, macOS).

O `UNLOAD` fragmenta por slice: 500.000 linhas em seis colunas saíram em 32 arquivos, sem
`MAXFILESIZE`, que é um teto e não um piso. Quem controla a quantidade é `PARALLEL OFF`, que grava
em série num arquivo só e respeita o `ORDER BY`, ou uma compactação posterior na camada Delta.
Os 32 arquivos saíram de um `UNLOAD ... PARTITION BY` sem `ORDER BY`, de uma tabela
`DISTSTYLE KEY` (2026-09-21). O `UNLOAD` em paralelo do `SELECT` da exportação, sem
`PARTITION BY` e com `ORDER BY` pela chave de ordenação, gravou um arquivo só de 1.000.000 a
33.239.719 linhas de `cad_lancamentos` (17,4 MB a 559,0 MB), no tempo do `PARALLEL OFF` (razão de
0,98 a 0,99 em 2026-10-05 e em 2026-10-09 e de 0,98 a 1,01 em 2026-10-07), sobre a tabela do
sandbox, em `DISTSTYLE AUTO`, e cópias dela por `CREATE TABLE AS ... LIMIT`; a leitura não separa
o efeito do `ORDER BY`, do `PARTITION BY` e da distribuição.

O `UNLOAD` lê tabelas do sandbox no banco local, fora das regras de escrita por datashare. Sem
acesso do Redshift ao S3, a exportação lê o mês em Arrow pelo driver ADBC e grava o Parquet com o
pyarrow e o papel do projeto. O `awswrangler.redshift.unload` executa o `UNLOAD` e lê os arquivos de
volta num DataFrame; `unload_to_files` só descarrega.

Com o Delta como fonte da verdade ([Delta Lake](#delta-lake)), o `UNLOAD` grava com `PARTITION BY (mes)`
na pasta da tabela, e a biblioteca registra os arquivos no log do Delta depois de conferir o
manifesto verboso. O `schema.elements` lista também a coluna de partição, que o `PARTITION BY` tira
dos arquivos (`mes` como `character varying` de `max_length` 7 na suíte de 2026-09-21): a lista
esperada da conferência de `register_files` a inclui. Uma amostra do
manifesto, reduzida aos campos que a biblioteca lê, no leiaute que a documentação descreve (URL,
`content_length` e `record_count` por entrada, `schema.elements` com nome e tipo, total em `meta`):

```json
{
  "entries": [
    {"url": "s3://bucket/prd/operacoes/mes=2026-08/0000_part_00.parquet",
     "meta": {"content_length": 33554432, "record_count": 180000}},
    {"url": "s3://bucket/prd/operacoes/mes=2026-08/0001_part_00.parquet",
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
          "TO 's3://bucket/prd/operacoes/' IAM_ROLE 'arn:aws:iam::123456789012:role/papel'\n"
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
TO 's3://bucket/prd/operacoes/' IAM_ROLE 'arn:aws:iam::123456789012:role/papel'
FORMAT AS PARQUET PARTITION BY (mes) MANIFEST VERBOSE
```

Os nomes de tipo da amostra e a presença da coluna de partição em `schema` sob `PARTITION BY` não
foram verificados; por isso a lista esperada é um parâmetro. `UnloadFromSelect` do dialeto não tem
`PARTITION BY` nem `VERBOSE`, e o comando fica em texto.

### O manifesto entre o log do Delta e o Redshift

O formato do manifesto é da AWS, descrito na referência do `COPY` e na do `UNLOAD`. O Delta não
participa dele nos dois sentidos: o que o log guarda são ações `add`, e o manifesto que o Delta tem,
o `GENERATE symlink_format_manifest`, é outro formato, de texto, que serve ao Spectrum e não ao
`COPY`, e que o delta-rs não implementa ([Delta Lake](#delta-lake)). As duas conversões abaixo montam e
leem o formato da AWS a partir do log e para o log; as duas rodaram no ambiente alvo em
2026-09-21, numa tabela do banco de datashare.

O manifesto é um objeto com `entries`, uma entrada por arquivo:

| Campo | Quando é exigido |
| --- | --- |
| `url` | Sempre; a URL `s3://` completa do arquivo. |
| `mandatory` | Opcional, falso por omissão, e um arquivo ausente é pulado em silêncio; com `true`, o `COPY` termina quando não acha o arquivo da entrada. A biblioteca grava `true` em toda entrada. |
| `meta.content_length` | Nos formatos colunares, Parquet e ORC. |
| `meta.record_count` | Só no manifesto que o `UNLOAD ... MANIFEST VERBOSE` grava. |

O manifesto ausente ou malformado faz o `COPY` falhar.

#### Do log do Delta para o manifesto do COPY

A biblioteca lê as ações `add` da versão (`get_add_actions()`, uma linha por arquivo vivo) e o
rodapé de cada arquivo, e grava um manifesto por lista de colunas dos rodapés, que o `COPY` do
manifesto nomeia. É `serialize_db.delta.copy_manifest`, usado pela ingestão do motor Redshift
(`serialize_db.engine.redshift.RedshiftEngine.ingest`) e pela publicação
(`serialize_db.publication.publish_redshift`).

| Ação `add` | Entrada do manifesto |
| --- | --- |
| `path`, relativo à pasta da tabela | `url`, a URI da tabela mais o caminho |
| `size_bytes` | `meta.content_length` |
| nenhuma | `mandatory: true`, porque o log afirma que o arquivo existe: sumiu um, o `COPY` falha em vez de carregar de menos |
| `num_records` | fica fora; a soma é o valor a comparar com a soma dos `pg_last_copy_count()` dos `COPY` da partição, um por manifesto, antes do `commit`, conta que a biblioteca não faz |
| nenhuma; o rodapé do arquivo | o manifesto em que a entrada fica, o da lista das colunas do rodapé, que o `COPY` desse manifesto nomeia |
| `partition.<coluna>` | fica fora, e é a razão da staging: o valor da partição não está no arquivo, o `COPY` só carrega as colunas dele, e o `INSERT INTO <tabela> (<colunas>) SELECT ..., '<valor>', ... FROM <staging>` põe o valor no lugar da coluna |
| `min`, `max`, `null_count` | ficam fora; o `COPY` não lê estatística |

Escolher os arquivos é filtrar as ações `add` por `partition.<coluna>` antes de montar as entradas,
o que é a diferença entre publicar uma partição e publicar a tabela.

#### Do manifesto do UNLOAD para o log do Delta

O caminho de volta não produz manifesto nenhum: ele grava um commit. `create_write_transaction`
escreve uma versão nova do log com uma ação `add` por entrada, e os arquivos que o `UNLOAD` gravou
ficam onde estão, sem cópia nem reescrita. É `serialize_db.delta.register_files`, usado por
`serialize_db.engine.redshift.RedshiftEngine.export_partition`.

| Entrada do manifesto | `AddAction` |
| --- | --- |
| `url`, absoluta | `path`, **relativo à pasta da tabela** |
| `meta.content_length` | `size` |
| `meta.record_count` | `stats.numRecords` |
| nenhuma | `partition_values`, o valor da partição exportada, que a biblioteca também põe no caminho, `<coluna>=<valor>/` na convenção Hive |
| nenhuma | `modification_time`, do `LastModified` do objeto no S3 |
| nenhuma | `data_change`, conceito do Delta sem contraparte |
| nenhuma | `stats.minValues`, `maxValues` e `nullCount`, lidos do rodapé Parquet |

Tirar o prefixo da URL é a conversão que sustenta o resto: o log do Delta guarda caminhos relativos
à pasta da tabela, e nenhum caminho absoluto ([Delta Lake](#delta-lake)), que é o que permite mover a
pasta. Um caminho absoluto registrado ali quebra a tabela no primeiro `mv`.

O manifesto não tem estatística, e os rodapés do `UNLOAD` têm: preencher `minValues` e `maxValues`
custa uma leitura de rodapé por arquivo e é o que faz o `delta_scan` podar. Sem elas a poda é só por
partição. A coluna de timestamp é a exceção, porque o `INT96` do `UNLOAD` não carrega estatística.

O `schema.elements` do manifesto verboso traz o nome e o tipo de cada coluna, e é a primeira
conferência antes do commit: um `cast` errado no `select` do `UNLOAD` aparece ali, não na primeira
leitura da tabela meses depois. Sob `PARTITION BY`, o bloco listou também a coluna de partição, que
os arquivos não têm (suíte de 2026-09-21); sem ele, como a biblioteca grava, a lista esperada é a do
`select`, que a próxima execução da suíte confere. A conferência a que o leitor obedece é a do
rodapé, abaixo.

#### As conferências antes do commit e a releitura depois

O `create_write_transaction` grava a ação como a recebe, e os leitores obedecem à ação, não ao
arquivo (sondagem de 2026-09-21): o caminho inexistente commita e derruba a leitura
da partição; a estatística falsa poda o arquivo certo no delta-rs, no `delta_scan` e no DataFusion,
sem erro; a coluna `NOT NULL` ausente do arquivo lê nulo; o tipo que não converte falha só quando a
coluna é lida. O `write_deltalake` recusa cada um desses casos, e é o que o registro perde. A
biblioteca repõe a conferência antes do commit, só com o rodapé de cada arquivo
(as conferências de `serialize_db.delta.register_files`): o arquivo
existe no caminho que o leitor resolve, com o tamanho da entrada; o esquema do rodapé bate com o da
tabela nome a nome, com o `INT96` e o `FIXED_LEN_BYTE_ARRAY` entre os tipos físicos admitidos; o
valor de partição do caminho é o pedido; a soma de linhas dos rodapés é a do manifesto e a do
`count(*)` da fonte; mínimo e máximo só das colunas cuja transcrição tem teste, omitidos nas demais.
Depois do commit a versão é relida pelo delta-rs e pelo `delta_scan`, e uma diferença volta por
`restore`; o snapshot e a publicação no Redshift esperam a releitura.

A alternativa sem registro é reler os arquivos do `UNLOAD` e gravar por `write_deltalake`, que faz
essas conferências sozinho e normaliza os tipos físicos, ao custo de passar os dados pela máquina
local: é a troca do motor Redshift para `serialize_db.delta.publish_partition` na partição com
`Double` não finito, cujo rodapé do `UNLOAD` deixa o `NaN` fora do máximo.

### Recomendações de performance

#### Ingestão

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

#### Organização das tabelas para filtros por chave e joins

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

No ambiente alvo, o join de `prd_cad_lancamentos` (283.835.836 linhas) com `prd_cad_contas` (101
linhas) por `id_conta`, as duas publicadas em `DISTSTYLE AUTO`, leu `XN Hash Join DS_DIST_ALL_NONE`
em 2026-10-09: a tabela de contas está em `ALL`, e as tabelas publicadas seguem sem `DISTKEY`.

**Escrita das consultas.** Sem `SELECT *`; predicados sobre a chave de ordenação; o mesmo filtro
repetido nas duas tabelas de um join, mesmo que redundante, para que ambas sejam podadas; sem funções
sobre colunas nos predicados; `GROUP BY` pelas colunas da chave de ordenação, na ordem da chave, para
habilitar a agregação em uma fase; `GROUP BY` e `ORDER BY` com as colunas na mesma ordem; cross
joins evitados; subconsulta em vez de join quando a segunda tabela só filtra e devolve menos de
cerca de 200 linhas; comparação em vez de `LIKE`, e `LIKE` em vez de `SIMILAR TO`.

#### Diferenças de abordagem em relação a bancos relacionais tradicionais

O ajuste de um banco relacional de linha passa por índices, transações curtas e atualizações
pontuais. No Redshift, ele passa pela distribuição e pela ordenação física, pela codificação e pelo
tamanho dos lotes. As chaves declaradas não protegem os dados, mas mudam os planos; a integridade é
responsabilidade da carga. As escritas concorrentes seguem snapshot isolation por padrão ou o nível
serializável, que aborta a segunda transação conflitante; os dois favorecem um escritor por tabela.
Os comandos têm limite de 16 MB, o que exclui `INSERT` gigantes. E o custo de `UPDATE` e `DELETE`
inclui a reordenação e a recuperação de espaço posteriores, motivo para substituir partições por
período em vez de alterá-las.

### Suporte a SQLAlchemy

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

#### Parâmetros específicos do Redshift no modelo

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
(`serialize_db.schema.table_options`) e as aplica na compilação do DDL com um gancho do
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

#### Consulta que devolve um DataFrame

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

#### Ingestão de um DataFrame numa tabela definida pelo ORM

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
aceita não foi verificado, e a biblioteca monta os dois comandos como texto, com `IAM_ROLE` ou com
as credenciais de quem chama (`serialize_db.engine.redshift.RedshiftConfig`). O
`insert(...).values(lista)` gera um único `INSERT ...
VALUES (...), (...)`, o multi-row insert da documentação, dentro do limite de 16 MB por comando;
`session.execute(insert(Operacao), lista)`, o bulk insert do ORM, cairia no `executemany` linha a
linha do `redshift_connector`.

### Referências

- Guia do desenvolvedor do Amazon Redshift: <https://docs.aws.amazon.com/redshift/latest/dg/>.
  Páginas usadas: armazenamento colunar, Redshift e PostgreSQL (recursos implementados de outra
  forma, recursos e tipos não suportados), tipos de dados (numéricos, caracteres, `SUPER`, dados
  semiestruturados e limites), `CREATE TABLE`, `CREATE TABLE AS`, `ALTER TABLE`, `DROP TABLE`,
  `COMMENT`, restrições, chaves de ordenação e distribuição, `SELECT`, `INSERT`, `UPDATE`, `DELETE`,
  `MERGE`, `TRUNCATE`, tabelas de staging, carga de dados e boas práticas, `COPY`, `UNLOAD`,
  `VACUUM`, `ANALYZE`, codificações de compressão, otimização automática de tabelas, planejamento e
  desempenho de consultas, `EXPLAIN`, isolamento e escritas concorrentes, os exemplos de escrita
  concorrente, os erros de isolamento, `LOCK`, os comandos aceitos e recusados pela escrita por
  datashare, `PG_LAST_COPY_COUNT`, `STL_LOAD_ERRORS` e `SYS_LOAD_ERROR_DETAIL`.
- Guia de gerenciamento do Amazon Redshift, conector Python e Data API:
  <https://docs.aws.amazon.com/redshift/latest/mgmt/>.
- Repositório do `redshift_connector`: <https://github.com/aws/amazon-redshift-python-driver>.
- Repositório do `sqlalchemy-redshift`: <https://github.com/sqlalchemy-redshift/sqlalchemy-redshift>
  e documentação em <https://sqlalchemy-redshift.readthedocs.io/en/latest/>.
- Repositório do `awswrangler` (AWS SDK for pandas): <https://github.com/aws/aws-sdk-pandas>.
- Driver ADBC para Redshift: <https://adbc-drivers.org/drivers/redshift/>.
- Documentação do SQLAlchemy 2.0: <https://docs.sqlalchemy.org/en/20/>.
- Lista completa das páginas consultadas: [REFERENCES.md][references].

## SQLAlchemy

O SQLAlchemy é a camada que descreve o esquema do projeto e gera o SQL para os dois bancos. Ele tem
duas partes: o Core, com `MetaData`, `Table`, `Column`, os tipos e os construtores de comandos, e o
ORM, com as classes mapeadas e a `Session`. Os modelos ORM são o contrato de esquema do projeto
(`serialize_db.schema.check_models`): deles derivam o DDL do DuckDB e do Redshift, o esquema Arrow dos arquivos Parquet e as
auditorias. Esta seção resume os conceitos usados pela biblioteca, as opções de customização e o
comportamento com Redshift, DuckDB e Parquet. A seção [Papel do SQLAlchemy na
biblioteca](#papel-do-sqlalchemy-na-biblioteca) registra o que cada parte entrega ao projeto, a
compilação pelo dialeto em tempo de execução, que é o caminho padrão, e o texto SQL gerado, a
opção de migração para fora do SQLAlchemy.

As afirmações vêm da documentação oficial da versão 2.0, consultada em 2026-09-18. Os exemplos foram
executados com SQLAlchemy 2.0.54, duckdb_engine 0.17.0 sobre DuckDB 1.5.5 e sqlalchemy-redshift 1.0.0;
os comandos do Redshift foram apenas compilados, sem conexão a um cluster. Os exemplos com Arrow e
Delta Lake rodaram em 2026-09-19 com PyArrow 25.0.1 e deltalake 1.6.4.

### Esquema, metadata e reflexão

#### MetaData, Table e Column

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
tipo num dialeto só. A [tabela de tipos do contrato](../serialize_db.html#tabela-de-mapeamento-de-tipos) fixa a correspondência com Arrow,
Delta, DuckDB e Redshift.

Restrições e índices são objetos: `PrimaryKeyConstraint`, `ForeignKey` na coluna ou
`ForeignKeyConstraint` na tabela, `UniqueConstraint`, `CheckConstraint`, `Index`. Desde a versão
2.0, `restricao.ddl_if(dialect="redshift")` limita a emissão a um dialeto, o que implementa a
política de restrições (chaves no Redshift, nenhuma no DuckDB) sem dois modelos:

```python
PrimaryKeyConstraint("id_operacao", "data_ref").ddl_if(dialect="redshift")
```

#### Restrições adiáveis

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
| Redshift | A sintaxe do `CREATE TABLE` não tem `DEFERRABLE` nem `INITIALLY`, e chaves primárias, únicas e estrangeiras são informativas, nunca verificadas. Se o parser aceita e ignora a cláusula não foi verificado; o DDL da biblioteca não a emite (`serialize_db.schema.ddl`). |

Os modelos de referência em `tests/reference_model/` declaram `deferrable=True, initially='DEFERRED'` em todas as
chaves estrangeiras de `model_base_contabil.py` e `model_base_gerencial.py`, inclusive nas compostas,
e em nenhuma de `model_db_projetado.py`. No DuckDB o `create_all` passa, porque a cláusula é
descartada; no Redshift ela não existe. O `create_all` do modelo cliente no DuckDB falhava em
`rel_contrato_operacao` enquanto as colunas apontadas pelas chaves estrangeiras compostas eram de
índice único: o DuckDB exige chave primária ou `UNIQUE` nas colunas apontadas, na mesma ordem
(leituras de 2026-09-22); o modelo cliente as declara como `UniqueConstraint`
desde então, e `check_models` confere o alvo de cada chave estrangeira. A
política de restrições dispensa a cláusula: no sandbox as chaves
estrangeiras ficam de fora e a auditoria verifica a integridade referencial sob pedido
(`foreign_keys=True`), com a tabela referenciada ingerida na versão fixada, antes da publicação: é a
verificação adiada feita pelo próprio pipeline. No Redshift só a tabela publicada declara chave, a
`PRIMARY KEY` informativa de `published_ddl`, sem `deferrable`; o sandbox não declara nenhuma.

O nome do esquema vai em `Table.schema` ou em `MetaData(schema=...)`; `BLANK_SCHEMA` exclui uma tabela
do padrão. A opção de execução `schema_translate_map` troca nomes de esquema por conexão, útil quando
cada execução do pipeline tem o próprio esquema (`execucao_abc123`) e os modelos declaram um nome
lógico. Prefixos no nome da tabela, o caminho quando só há um esquema no Redshift, exigem gerar o
nome em `__tablename__`.

#### DDL

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

A tabela do Delta Lake, a fonte da verdade do projeto ([Delta Lake](#delta-lake)), nasce sem DDL em SQL.
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
na seção [Evolução de esquema](#evolucao-de-esquema) do Delta Lake, `delta_table.alter.add_columns`
exige `deltalake.schema.Field`; um `pyarrow.Field` é recusado com
`'Field' object is not an instance of 'Field'`.

#### Reflexão

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

#### Customização do comportamento

| Mecanismo | Uso |
| --- | --- |
| `Table.info`, `Column.info` | Dicionários livres, guardados com o objeto e ignorados pelo DDL. É o canal para metadados da aplicação, lidos por eventos e ganchos de compilação. |
| `Table.comment`, `Column.comment` | Emitidos como `COMMENT ON` pelos dialetos que suportam comentários; refletidos de volta. |
| `Column.key`, `Column.doc` | Nome alternativo no Python e documentação interna, sem efeito no banco. |
| `MetaData(naming_convention=...)` | Nomes determinísticos de restrições e índices (`"pk": "%(table_name)s_pk"`). |
| Argumentos `<dialeto>_<opcao>` | Opções de DDL por dialeto (`redshift_sortkey`, `postgresql_partition_by`). Com o dialeto instalado, um argumento que ele não aceita é `ArgumentError`; sem o dialeto, o argumento é aceito com o aviso `Can't validate argument` e não produz DDL. |
| `Table.implicit_returning=False` | Desliga `RETURNING` para a tabela, para backends com gatilhos ou sem suporte. |
| `TypeDecorator` | Tipo derivado com `process_bind_param` e `process_result_value`; `cache_ok = True` para participar do cache de compilação. Serve, por exemplo, para forçar UTC em `DateTime` ou serializar JSON. |
| `@compiles(Construct, "<dialeto>")` | Troca a compilação de um tipo, de um comando ou de um DDL num dialeto. Exemplo: [gancho que aplica `Table.info` ao `CREATE TABLE` do Redshift](#parametros-especificos-do-redshift-no-modelo). |
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

### Statements de insert, update, delete e select

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

### SQL gerado a partir de um comando

Todo comando do SQLAlchemy, DDL ou DML, vira texto por `compile()`, sem abrir conexão. O resultado
é um `Compiled`: `str(compiled)` é o SQL, `compiled.params` traz os valores dos parâmetros e
`compiled.positiontup` a ordem deles nos dialetos posicionais. É assim que esta página mostra
comandos do Redshift sem cluster e que uma depuração confere o texto que o DuckDB recebe. Os
exemplos desta seção são autocontidos e rodaram com SQLAlchemy 2.0.54, duckdb_engine 0.17.0,
sqlalchemy-redshift 1.0.0, redshift_connector 2.1.16 e DuckDB 1.5.5, por
`uv run --no-project --python 3.13 --with ...`.

#### Dialetos e estilos de parâmetro

| Dialeto passado a `compile()` | Marcador | `positiontup` | Quando usar |
| --- | --- | --- | --- |
| Nenhum (`str(stmt)` ou `stmt.compile()`) | `:mes_1` | `None` | SQL genérico do dialeto padrão; renderiza construções que um banco pode não ter. |
| `duckdb_engine.Dialect()` | `%(mes_1)s` | `None` | Sintaxe do DuckDB sem engine; o marcador não é o da execução. |
| `create_engine("duckdb:///:memory:").dialect`, ou `stmt.compile(engine)` | `$1` | lista | O que o DuckDB recebe: o `paramstyle` `numeric_dollar` só é definido quando o engine carrega o DBAPI. |
| `RedshiftDialect_redshift_connector()` | `%s` | lista | O que o `redshift_connector` envia (`paramstyle` `format`). |
| `create_mock_engine(url, executor).dialect` | o do engine real | | DDL completo de `create_all` sem conexão. |

`compile_kwargs={"literal_binds": True}` embute os valores no texto, para ler ou colar num cliente
SQL. A documentação restringe o recurso a tipos simples e avisa que a conversão não é segura contra
entrada não confiável: o texto com valores embutidos é para depuração, não para execução.

#### DDL por dialeto

```python
"""Gera o DDL de um modelo para o DuckDB e para o Redshift, sem conexão com banco."""
import datetime as dt
import decimal

import duckdb_engine
import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.schema import AddConstraint, CreateTable, DropConstraint, DropTable, SetColumnComment, SetTableComment
from sqlalchemy_redshift.dialect import RedshiftDialect_redshift_connector


class Base(DeclarativeBase):
    pass


class Operacao(Base):
    __tablename__ = "operacoes"
    __table_args__ = (
        sa.PrimaryKeyConstraint("id_operacao", "data_ref", name="operacoes_pk"),
        sa.CheckConstraint("valor >= 0", name="ck_operacoes_valor"),
        {"comment": "Operações do mês"},
    )
    id_operacao: Mapped[int] = mapped_column(sa.BigInteger, autoincrement=False)
    data_ref: Mapped[dt.date]
    id_cliente: Mapped[int] = mapped_column(sa.BigInteger, comment="Chave do cliente")
    valor: Mapped[decimal.Decimal] = mapped_column(sa.Numeric(18, 2))
    descricao: Mapped[str | None] = mapped_column(sa.String(200))
    mes: Mapped[str] = mapped_column(sa.String(7))


DIALECTS = {"duckdb": duckdb_engine.Dialect(), "redshift": RedshiftDialect_redshift_connector()}


def sql(command, dialect_name: str) -> str:
    """Texto SQL de um comando no dialeto pedido, com os valores embutidos."""
    compiled = command.compile(dialect=DIALECTS[dialect_name], compile_kwargs={"literal_binds": True})
    return str(compiled).strip()


table = Operacao.__table__
check = next(c for c in table.constraints if isinstance(c, sa.CheckConstraint))
for name in DIALECTS:
    print(f"-- {name}")
    print(sql(CreateTable(table, if_not_exists=True), name))
    print(sql(SetTableComment(table), name))
    print(sql(SetColumnComment(table.c.id_cliente), name))
    print(sql(DropConstraint(check), name))
    print(sql(AddConstraint(check), name))
    print(sql(DropTable(table, if_exists=True), name))
```

Saída:

```
-- duckdb
CREATE TABLE IF NOT EXISTS operacoes (
    id_operacao BIGINT NOT NULL,
    data_ref DATE NOT NULL,
    id_cliente BIGINT NOT NULL,
    valor NUMERIC(18, 2) NOT NULL,
    descricao VARCHAR(200),
    mes VARCHAR(7) NOT NULL,
    CONSTRAINT operacoes_pk PRIMARY KEY (id_operacao, data_ref),
    CONSTRAINT ck_operacoes_valor CHECK (valor >= 0)
)
COMMENT ON TABLE operacoes IS 'Operações do mês'
COMMENT ON COLUMN operacoes.id_cliente IS 'Chave do cliente'
ALTER TABLE operacoes DROP CONSTRAINT ck_operacoes_valor
ALTER TABLE operacoes ADD CONSTRAINT ck_operacoes_valor CHECK (valor >= 0)
DROP TABLE IF EXISTS operacoes
-- redshift
CREATE TABLE IF NOT EXISTS operacoes (
    id_operacao BIGINT NOT NULL,
    data_ref DATE NOT NULL,
    id_cliente BIGINT NOT NULL,
    valor NUMERIC(18, 2) NOT NULL,
    descricao VARCHAR(200),
    mes VARCHAR(7) NOT NULL,
    CONSTRAINT operacoes_pk PRIMARY KEY (id_operacao, data_ref)
)
COMMENT ON TABLE operacoes IS 'Operações do mês'
COMMENT ON COLUMN operacoes.id_cliente IS 'Chave do cliente'
ALTER TABLE operacoes DROP CONSTRAINT ck_operacoes_valor
ALTER TABLE operacoes ADD CONSTRAINT ck_operacoes_valor CHECK (valor >= 0)
DROP TABLE IF EXISTS operacoes
```

O `CREATE TABLE` do dialeto do Redshift omite o `CHECK`, mas `AddConstraint` do mesmo
`CheckConstraint` gera o `ALTER TABLE ... ADD CONSTRAINT ... CHECK`, que o Redshift recusaria: a
compilação confere o que o dialeto sabe renderizar, não o que o servidor aceita.
`if_not_exists=True` e `if_exists=True` entram no texto dos dois dialetos.

#### Sequência completa do create_all

`create_mock_engine` cria um engine que entrega cada comando a uma função em vez de executá-lo.
`metadata.create_all` e `drop_all` passam por ele na ordem de dependência, com `COMMENT ON` e
`CREATE INDEX` em comandos separados; `checkfirst=False` é obrigatório, porque o engine simulado não
consulta o catálogo.

```python
"""Imprime a sequência completa de DDL que create_all emitiria, por dialeto, com um engine simulado."""
import sqlalchemy as sa

metadata = sa.MetaData()
operations = sa.Table(
    "operacoes", metadata,
    sa.Column("id_operacao", sa.BigInteger, primary_key=True, autoincrement=False),
    sa.Column("data_ref", sa.Date, primary_key=True),
    sa.Column("id_cliente", sa.BigInteger, nullable=False, comment="Chave do cliente"),
    sa.Column("valor", sa.Numeric(18, 2), nullable=False),
    sa.Column("descricao", sa.String(200)),
    sa.Column("mes", sa.String(7), nullable=False),
    comment="Operações do mês",
)
sa.Index("ix_operacoes_mes", operations.c.mes)

for url in ("duckdb://", "redshift+redshift_connector://"):
    def dump(command, *multiparams, **params):
        print(str(command.compile(dialect=mock.dialect)).strip() + ";")
    mock = sa.create_mock_engine(url, dump)
    print(f"-- {url}")
    metadata.create_all(mock, checkfirst=False)
    metadata.drop_all(mock, checkfirst=False)
```

Saída:

```
-- duckdb://
CREATE TABLE operacoes (
    id_operacao BIGINT NOT NULL,
    data_ref DATE NOT NULL,
    id_cliente BIGINT NOT NULL,
    valor NUMERIC(18, 2) NOT NULL,
    descricao VARCHAR(200),
    mes VARCHAR(7) NOT NULL,
    PRIMARY KEY (id_operacao, data_ref)
);
CREATE INDEX ix_operacoes_mes ON operacoes (mes);
COMMENT ON TABLE operacoes IS 'Operações do mês';
COMMENT ON COLUMN operacoes.id_cliente IS 'Chave do cliente';
DROP TABLE operacoes;
-- redshift+redshift_connector://
CREATE TABLE operacoes (
    id_operacao BIGINT NOT NULL,
    data_ref DATE NOT NULL,
    id_cliente BIGINT NOT NULL,
    valor NUMERIC(18, 2) NOT NULL,
    descricao VARCHAR(200),
    mes VARCHAR(7) NOT NULL,
    PRIMARY KEY (id_operacao, data_ref)
);
CREATE INDEX ix_operacoes_mes ON operacoes (mes);
COMMENT ON TABLE operacoes IS 'Operações do mês';
COMMENT ON COLUMN operacoes.id_cliente IS 'Chave do cliente';
DROP TABLE operacoes;
```

O `CREATE INDEX` sai também para o Redshift, que não tem índices. `Index(...).ddl_if(dialect="duckdb")`
e `CheckConstraint(...).ddl_if(dialect="duckdb")` restringem o comando a um dialeto: com os dois, o
`create_all` simulado do Redshift emitiu só o `CREATE TABLE` com a chave primária.

#### INSERT, UPDATE, DELETE e SELECT por dialeto

```python
"""Gera o SQL de insert, update, delete e select para o DuckDB e para o Redshift, com e sem valores embutidos."""
import datetime as dt
import decimal

import duckdb_engine
import sqlalchemy as sa
from sqlalchemy_redshift.dialect import RedshiftDialect_redshift_connector

metadata = sa.MetaData()
operations = sa.Table(
    "operacoes", metadata,
    sa.Column("id_operacao", sa.BigInteger, primary_key=True, autoincrement=False),
    sa.Column("data_ref", sa.Date, primary_key=True),
    sa.Column("id_cliente", sa.BigInteger, nullable=False),
    sa.Column("valor", sa.Numeric(18, 2), nullable=False),
    sa.Column("descricao", sa.String(200)),
    sa.Column("mes", sa.String(7), nullable=False),
)
staging = operations.to_metadata(sa.MetaData(), name="staging_operacoes")

DIALECTS = {"duckdb": duckdb_engine.Dialect(), "redshift": RedshiftDialect_redshift_connector()}


def show(title: str, command, **compile_kwargs) -> None:
    """Imprime o comando compilado em cada dialeto; sem literal_binds, imprime também os parâmetros."""
    print(f"== {title}")
    for name, dialect in DIALECTS.items():
        compiled = command.compile(dialect=dialect, compile_kwargs=compile_kwargs)
        print(f"-- {name}: {' '.join(str(compiled).split())}")
        if not compile_kwargs.get("literal_binds"):
            print(f"   params={compiled.params} positiontup={compiled.positiontup}")


row = {"id_operacao": 1, "data_ref": dt.date(2026, 8, 1), "id_cliente": 100,
       "valor": decimal.Decimal("10.50"), "descricao": None, "mes": "2026-08"}
row2 = {**row, "id_operacao": 2, "valor": decimal.Decimal("4.25"), "descricao": "estorno"}

show("insert de uma linha", sa.insert(operations).values(**row))
show("insert de uma linha, valores embutidos", sa.insert(operations).values(**row), literal_binds=True)
show("insert multi-linha", sa.insert(operations).values([row, row2]), literal_binds=True)
show("insert para executemany (valores só na execução)", sa.insert(operations))
show("insert ... select", sa.insert(operations).from_select(
    list(operations.columns.keys()), sa.select(staging).where(staging.c.mes == "2026-08")), literal_binds=True)
show("update", sa.update(operations).where(operations.c.id_operacao == 1).values(descricao="ajustada"), literal_binds=True)
show("delete", sa.delete(operations).where(operations.c.mes == "2026-08"), literal_binds=True)
query = (sa.select(operations.c.id_cliente, sa.func.sum(operations.c.valor).label("total"))
         .where(operations.c.mes == "2026-08").group_by(operations.c.id_cliente).order_by(operations.c.id_cliente))
show("select com agregação", query, literal_binds=True)
in_query = sa.select(operations.c.id_operacao).where(operations.c.id_cliente.in_([100, 200]))
show("select com IN, compilação normal", in_query)
show("select com IN, render_postcompile", in_query, render_postcompile=True)
show("select com IN, valores embutidos", in_query, literal_binds=True)
raw = sa.text("SELECT count(*) FROM operacoes WHERE data_ref >= :start").bindparams(start=dt.date(2026, 8, 1))
show("text com bindparams", raw)
show("text com bindparams, valores embutidos", raw, literal_binds=True)
print("== sem dialeto: str(query)")
print(" ".join(str(query).split()))
```

Saída:

```
== insert de uma linha
-- duckdb: INSERT INTO operacoes (id_operacao, data_ref, id_cliente, valor, descricao, mes) VALUES (%(id_operacao)s, %(data_ref)s, %(id_cliente)s, %(valor)s, %(descricao)s, %(mes)s)
   params={'id_operacao': 1, 'data_ref': datetime.date(2026, 8, 1), 'id_cliente': 100, 'valor': Decimal('10.50'), 'descricao': None, 'mes': '2026-08'} positiontup=None
-- redshift: INSERT INTO operacoes (id_operacao, data_ref, id_cliente, valor, descricao, mes) VALUES (%s, %s, %s, %s, %s, %s)
   params={'id_operacao': 1, 'data_ref': datetime.date(2026, 8, 1), 'id_cliente': 100, 'valor': Decimal('10.50'), 'descricao': None, 'mes': '2026-08'} positiontup=['id_operacao', 'data_ref', 'id_cliente', 'valor', 'descricao', 'mes']
== insert de uma linha, valores embutidos
-- duckdb: INSERT INTO operacoes (id_operacao, data_ref, id_cliente, valor, descricao, mes) VALUES (1, '2026-08-01', 100, 10.50, NULL, '2026-08')
-- redshift: INSERT INTO operacoes (id_operacao, data_ref, id_cliente, valor, descricao, mes) VALUES (1, '2026-08-01', 100, 10.50, NULL, '2026-08')
== insert multi-linha
-- duckdb: INSERT INTO operacoes (id_operacao, data_ref, id_cliente, valor, descricao, mes) VALUES (1, '2026-08-01', 100, 10.50, NULL, '2026-08'), (2, '2026-08-01', 100, 4.25, 'estorno', '2026-08')
-- redshift: INSERT INTO operacoes (id_operacao, data_ref, id_cliente, valor, descricao, mes) VALUES (1, '2026-08-01', 100, 10.50, NULL, '2026-08'), (2, '2026-08-01', 100, 4.25, 'estorno', '2026-08')
== insert para executemany (valores só na execução)
-- duckdb: INSERT INTO operacoes (id_operacao, data_ref, id_cliente, valor, descricao, mes) VALUES (%(id_operacao)s, %(data_ref)s, %(id_cliente)s, %(valor)s, %(descricao)s, %(mes)s)
   params={'id_operacao': None, 'data_ref': None, 'id_cliente': None, 'valor': None, 'descricao': None, 'mes': None} positiontup=None
-- redshift: INSERT INTO operacoes (id_operacao, data_ref, id_cliente, valor, descricao, mes) VALUES (%s, %s, %s, %s, %s, %s)
   params={'id_operacao': None, 'data_ref': None, 'id_cliente': None, 'valor': None, 'descricao': None, 'mes': None} positiontup=['id_operacao', 'data_ref', 'id_cliente', 'valor', 'descricao', 'mes']
== insert ... select
-- duckdb: INSERT INTO operacoes (id_operacao, data_ref, id_cliente, valor, descricao, mes) SELECT staging_operacoes.id_operacao, staging_operacoes.data_ref, staging_operacoes.id_cliente, staging_operacoes.valor, staging_operacoes.descricao, staging_operacoes.mes FROM staging_operacoes WHERE staging_operacoes.mes = '2026-08'
-- redshift: INSERT INTO operacoes (id_operacao, data_ref, id_cliente, valor, descricao, mes) SELECT staging_operacoes.id_operacao, staging_operacoes.data_ref, staging_operacoes.id_cliente, staging_operacoes.valor, staging_operacoes.descricao, staging_operacoes.mes FROM staging_operacoes WHERE staging_operacoes.mes = '2026-08'
== update
-- duckdb: UPDATE operacoes SET descricao='ajustada' WHERE operacoes.id_operacao = 1
-- redshift: UPDATE operacoes SET descricao='ajustada' WHERE operacoes.id_operacao = 1
== delete
-- duckdb: DELETE FROM operacoes WHERE operacoes.mes = '2026-08'
-- redshift: DELETE FROM operacoes WHERE operacoes.mes = '2026-08'
== select com agregação
-- duckdb: SELECT operacoes.id_cliente, sum(operacoes.valor) AS total FROM operacoes WHERE operacoes.mes = '2026-08' GROUP BY operacoes.id_cliente ORDER BY operacoes.id_cliente
-- redshift: SELECT operacoes.id_cliente, sum(operacoes.valor) AS total FROM operacoes WHERE operacoes.mes = '2026-08' GROUP BY operacoes.id_cliente ORDER BY operacoes.id_cliente
== select com IN, compilação normal
-- duckdb: SELECT operacoes.id_operacao FROM operacoes WHERE operacoes.id_cliente IN (__[POSTCOMPILE_id_cliente_1])
   params={'id_cliente_1': [100, 200]} positiontup=None
-- redshift: SELECT operacoes.id_operacao FROM operacoes WHERE operacoes.id_cliente IN (__[POSTCOMPILE_id_cliente_1])
   params={'id_cliente_1': [100, 200]} positiontup=['id_cliente_1']
== select com IN, render_postcompile
-- duckdb: SELECT operacoes.id_operacao FROM operacoes WHERE operacoes.id_cliente IN (%(id_cliente_1_1)s, %(id_cliente_1_2)s)
   params={'id_cliente_1_1': 100, 'id_cliente_1_2': 200} positiontup=None
-- redshift: SELECT operacoes.id_operacao FROM operacoes WHERE operacoes.id_cliente IN (%s, %s)
   params={'id_cliente_1_1': 100, 'id_cliente_1_2': 200} positiontup=['id_cliente_1_1', 'id_cliente_1_2']
== select com IN, valores embutidos
-- duckdb: SELECT operacoes.id_operacao FROM operacoes WHERE operacoes.id_cliente IN (100, 200)
-- redshift: SELECT operacoes.id_operacao FROM operacoes WHERE operacoes.id_cliente IN (100, 200)
== text com bindparams
-- duckdb: SELECT count(*) FROM operacoes WHERE data_ref >= %(start)s
   params={'start': datetime.date(2026, 8, 1)} positiontup=None
-- redshift: SELECT count(*) FROM operacoes WHERE data_ref >= %s
   params={'start': datetime.date(2026, 8, 1)} positiontup=['start']
== text com bindparams, valores embutidos
-- duckdb: SELECT count(*) FROM operacoes WHERE data_ref >= '2026-08-01'
-- redshift: SELECT count(*) FROM operacoes WHERE data_ref >= '2026-08-01'
== sem dialeto: str(query)
SELECT operacoes.id_cliente, sum(operacoes.valor) AS total FROM operacoes WHERE operacoes.mes = :mes_1 GROUP BY operacoes.id_cliente ORDER BY operacoes.id_cliente
```

O que a saída mostra:

- Com `literal_binds`, o texto é o mesmo nos dois dialetos para esses comandos. As diferenças entre
  os bancos estão em construções específicas (`RETURNING`, `ON CONFLICT`, `LIMIT` compilado como
  literal no dialeto do Redshift), tratadas na
  [seção de statements](#statements-de-insert-update-delete-e-select) e na
  [seção de suporte](#suporte-a-redshift-duckdb-e-arquivos-parquet).
- `insert(operations)` sem `values` é o comando do `executemany`: um marcador por coluna e `params`
  todos `None`. Os valores só existem na execução, e o log do engine, abaixo, é o lugar de vê-los.
- `IN` com lista compila como `__[POSTCOMPILE_id_cliente_1]`, um marcador expandido na execução.
  `render_postcompile=True` mostra a expansão com um parâmetro por item; `literal_binds` liga
  `render_postcompile` sozinho.
- `text(...).bindparams(...)` também aceita `literal_binds`. A documentação registra que um
  `bindparam()` sem valor não pode ser embutido.
- `str(query)` sem dialeto usa `:mes_1`, o estilo do dialeto padrão.

#### O SQL executado de fato

`create_engine(..., echo=True)` registra cada comando enviado, com os parâmetros e as reescritas do
próprio SQLAlchemy, como o `insertmanyvalues` que junta as linhas do `executemany` num só `INSERT`:

```python
"""Mostra o SQL executado de fato por um engine DuckDB em memória, com echo=True e com compile(bind=engine)."""
import datetime as dt
import decimal

import sqlalchemy as sa

metadata = sa.MetaData()
operations = sa.Table(
    "operacoes", metadata,
    sa.Column("id_operacao", sa.BigInteger, primary_key=True, autoincrement=False),
    sa.Column("valor", sa.Numeric(18, 2), nullable=False),
    sa.Column("mes", sa.String(7), nullable=False),
)
engine = sa.create_engine("duckdb:///:memory:", echo=True)
metadata.create_all(engine)
query = sa.select(sa.func.sum(operations.c.valor)).where(operations.c.mes == "2026-08")
print("compile(bind=engine):", query.compile(engine), query.compile(engine).params)
with engine.begin() as conn:
    conn.execute(sa.insert(operations), [{"id_operacao": 1, "valor": decimal.Decimal("10.50"), "mes": "2026-08"},
                                         {"id_operacao": 2, "valor": decimal.Decimal("4.25"), "mes": "2026-08"}])
    print("total:", conn.execute(query).scalar())
```

Saída, sem os comandos de criação da tabela e sem os horários:

```
compile(bind=engine): SELECT sum(operacoes.valor) AS sum_1
FROM operacoes
WHERE operacoes.mes = $1 {'mes_1': '2026-08'}
INFO sqlalchemy.engine.Engine INSERT INTO operacoes (id_operacao, valor, mes) VALUES ($1, $2, $3), ($4, $5, $6)
INFO sqlalchemy.engine.Engine [dialect duckdb+duckdb_engine does not support caching 0.00004s (insertmanyvalues) 1/1 (unordered)] (1, 10.5, '2026-08', 2, 4.25, '2026-08')
INFO sqlalchemy.engine.Engine SELECT sum(operacoes.valor) AS sum_1
FROM operacoes
WHERE operacoes.mes = $1
INFO sqlalchemy.engine.Engine [dialect duckdb+duckdb_engine does not support caching 0.00004s] ('2026-08',)
total: 14.75
```

Os parâmetros do log mostram `10.5` e `4.25`, não `Decimal('10.50')`: o `duckdb_engine` e o dialeto
do Redshift declaram `supports_native_decimal = False`, então `Numeric` recebe o processador de
entrada `to_float` e o de saída `DecimalResultProcessor`, que formata um `float`. O texto compilado
com `literal_binds` conserva o decimal (`10.50`), mas a execução com parâmetros passa por `float`
nos dois sentidos. Medido no DuckDB com `Numeric(18, 2)`: até 15 dígitos significativos o valor volta
igual; `99999999999999.99` (16 dígitos) foi gravado como `99999999999999.98`; `999999999999999.99`
(17) virou `1000000000000000.00`; `9999999999999999.99` (18) falhou no `INSERT` com
`Could not cast value 10000000000000000.000000 to DECIMAL(18,2)`. O mesmo valor de 18 dígitos
gravado pela tabela Arrow registrada na conexão bruta ficou exato, e lido pelo Core voltou como
`10000000000000000.00`. Acima de 15 dígitos, a escrita e a leitura no DuckDB passam pelo Arrow
([O dialeto do DuckDB](#o-dialeto-do-duckdb)); no Redshift, `COPY` e `UNLOAD` por Parquet não passam pelo dialeto, e
o `insert(Modelo).values(lista)` de lotes pequenos envia `float` pelos parâmetros.

### ORM: modelos e DDL

#### Mapeamento declarativo

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

#### Metadados da aplicação no modelo

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

### ORM: insert, update, delete e select

#### Session e unidade de trabalho

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

#### Operações em lote

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

#### Chaves geradas no servidor

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

### Suporte a Redshift, DuckDB e arquivos Parquet

#### O dialeto do Redshift

O dialeto externo `sqlalchemy-redshift` (versão 1.0.0) aparece na lista de dialetos externos da
documentação do SQLAlchemy. A seção [Suporte a SQLAlchemy](#suporte-a-sqlalchemy-3) do Redshift
detalha o dialeto; o resumo para o
contrato:

| Tema | Comportamento |
| --- | --- |
| DDL | `redshift_diststyle`, `redshift_distkey`, `redshift_sortkey`, `redshift_interleaved_sortkey` na tabela; `redshift_encode`, `redshift_distkey`, `redshift_sortkey`, `redshift_identity` na coluna. `PRIMARY KEY`, `UNIQUE` e `FOREIGN KEY` saem no DDL e são informativas no banco. |
| Tipos | `Numeric(18, 2)` vira `NUMERIC(18, 2)`; `String(n)` vira `VARCHAR(n)`, com `n` em bytes; `String()` sem comprimento vira `VARCHAR`, que o Redshift trata como `VARCHAR(256)`. O dialeto compila `Text` como `TEXT`, também `VARCHAR(256)` no Redshift, então o `VARCHAR(65535)` da tabela do contrato exige `String(65535)` ou uma regra `@compiles(Text, "redshift")`. `JSON` compila como `JSON`, que o Redshift não tem; o contrato usa `JSON().with_variant(SUPER(), "redshift")`, que compila `SUPER` ([tabela de mapeamento de tipos][tipos]). |
| Precisão de `Numeric` | O dialeto declara `supports_native_decimal = False`: parâmetros `Decimal` viram `float` na ida (`to_float`) e os resultados voltam por `float`, exato até 15 dígitos significativos. `COPY` e `UNLOAD` por Parquet não passam pelo dialeto ([medição](#o-sql-executado-de-fato)). |
| Importação | `CopyCommand(Table, ...)` gera o `COPY ... FORMAT AS PARQUET MANIFEST` a partir do `Table` do modelo; a validação do esquema acontece no Arrow, antes do Parquet. |
| Exportação | `UnloadFromSelect(select(Modelo)...)` gera o `UNLOAD` da consulta do modelo. |
| Inserção pelo ORM | Volumes pequenos com `insert(Modelo).values(lista)` em lotes; o bulk insert do ORM cai no `executemany` linha a linha do `redshift_connector`. |
| Chaves | Nenhuma geração no servidor recuperável; chaves de negócio no cliente. Um `server_default` exige `__table_args__ = {"implicit_returning": False}`, senão o ORM emite `RETURNING`. |

#### O dialeto do DuckDB

O dialeto `duckdb_engine` (versão 0.17.0) não consta da lista de dialetos externos da documentação do
SQLAlchemy e deriva do dialeto PostgreSQL com psycopg2. A seção [Suporte a
SQLAlchemy](#suporte-a-sqlalchemy-2) do DuckDB traz os detalhes; o resumo para o contrato:

| Tema | Comportamento |
| --- | --- |
| DDL | `create_all` funciona com chaves, `NOT NULL`, comentários e sequências; `Identity` e `use_alter` falham, e `DEFERRABLE` é descartado nas restrições de tabela; `String(n)` perde o comprimento. Uma chave primária `Integer` de uma coluna com o `autoincrement` padrão sai como `SERIAL` e falha com `Type with name SERIAL does not exist!`, então as chaves do cliente levam `autoincrement=False`. Restrições ficam fora pela política do projeto (`ddl_if(dialect="redshift")`). |
| Reflexão | Colunas e comentários sim; chave primária e índices não. |
| Importação | `INSERT INTO tabela BY NAME SELECT * FROM entrada` com a tabela Arrow registrada na conexão bruta (`conn.connection.dbapi_connection.register`), porque variáveis Python não são visíveis pelo engine. Volumes pequenos pelo bulk insert do ORM (0,98 s para 50.000 linhas, 0,77 s com `render_nulls=True`). |
| Exportação | `COPY (...) TO 'arquivo.parquet'` em `text()`, com a consulta do modelo compilada com `literal_binds`. |
| Leitura | `pd.read_sql` com `coerce_float=False` para `Decimal`; ou o SQL compilado executado pela conexão DuckDB com `to_arrow_table()` para manter `decimal128` e `date32`. |
| Precisão de `Numeric` | `supports_native_decimal = False`: `insert` e `select` pelo engine passam `Decimal` por `float` nos dois sentidos, exato até 15 dígitos significativos; 16 dígitos perdem o último centavo e 18 falham no `INSERT`. A tabela Arrow registrada na conexão bruta e `to_arrow_table()` conservam `decimal128(18, 2)` ([medição](#o-sql-executado-de-fato)). |
| Transações | Banco em memória com `SingletonThreadPool`: conexões abertas sem fechar deixam transações pendentes. |

#### O esquema Arrow dos arquivos Parquet

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
            return pa.string()
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
  subclasse de `Numeric`. `Text` cai em `String`. `JSON` vira `string`, texto sem anotação: a extensão `arrow.json`
  ficou fora do contrato em 2026-09-20, porque o dtype dela no pandas não tem os kernels de `.str` e
  nenhum motor a devolve; o `with_variant(SUPER(), "redshift")` do contrato não muda o tipo genérico,
  e `isinstance(sa_type, t.JSON)` continua verdadeiro. O metadado `PARQUET:field_id` é o que o PyArrow grava como `field_id` no Parquet;
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
  [Parquet](#parquet) descreve os metadados e as opções de leitura.

### Papel do SQLAlchemy na biblioteca

O SQLAlchemy está no projeto por compatibilidade com o pipeline existente: os modelos declarativos
definem cada tabela, e statements Core de `select` e `insert` movem DataFrames. As subseções
seguintes registram o que cada parte da biblioteca entrega ao projeto, a recomendação sem essa
premissa e a substituição do dialeto por texto gerado, que passou a ser a opção de migração para
fora do SQLAlchemy, não o caminho padrão: o pipeline submete o
statement Core ao motor, que o compila pelo dialeto com os parâmetros do cliente.

#### O que cada parte entrega

| Parte | Uso no projeto | Veredito |
| --- | --- | --- |
| Engine, cursor e processadores de resultado | Fora do caminho dos dados por decisão: os dados passam por Arrow, Parquet, `COPY` e `UNLOAD`. Pelo DBAPI, `Numeric` passa por `float` nos dois sentidos e perde o último centavo a partir de 16 dígitos ([medição](#o-sql-executado-de-fato)); `executemany` custa uma ida ao servidor por linha no Redshift. | A maior parte da biblioteca não é usada, e não deve ser. |
| DDL pelos dialetos externos | Funciona com remendos: `Identity` some no Redshift e é rejeitado pelo DuckDB, `Text` vira `VARCHAR(256)`, `CHECK` cai no `CREATE TABLE` do Redshift enquanto `AddConstraint` e `CREATE INDEX` ainda saem, `DEFERRABLE` é descartado, `DISTKEY` e `SORTKEY` exigem argumentos do dialeto ou um hook `@compiles`, e JSON exige `with_variant`. Os dois dialetos são projetos de terceiros, e `duckdb_engine` está sem lançamento desde 2025-03-29. | O DDL do projeto é `CREATE TABLE` com tipos, `NOT NULL` e opções físicas, e a [tabela de mapeamento de tipos][tipos] é a especificação de um renderizador de poucas dezenas de linhas por destino. O SQLAlchemy não conhece Arrow nem Delta; metade do mapeamento é escrita à mão de qualquer forma. |
| Statements Core | O ponto forte do Core é compor consultas dinamicamente e compilar por dialeto. As consultas de um pipeline mensal têm forma fixa, parametrizada pelo mês, e são analíticas: janela e CTE o Core tem, e `QUALIFY`, `PIVOT` e `ASOF JOIN` caem em `text()`. O dialeto do Redshift deriva de `PGDialect` e compila sem erro `DISTINCT ON`, `ON CONFLICT DO NOTHING` e o índice de array `tags[1]`, que o Redshift não tem. | A portabilidade é a mesma do SQLGlot, e nenhum dos dois sabe o que o Redshift não suporta: os testes de integração no Redshift são obrigatórios nos dois casos. |
| Modelos declarativos | A declaração de cada tabela, com o tipo Python em `Mapped[]`, os comentários e `Table.info["serialize_db"]`; já existem para as tabelas do pipeline. | A parte que fica: o contrato de esquema, do qual a biblioteca deriva o esquema Arrow, o esquema Delta e o DDL. |

#### Recomendação sem a compatibilidade com o pipeline

O SQLAlchemy 2.0 não é uma biblioteca defasada. Ele tem a forma errada para este projeto: um ORM
transacional cuja camada de execução o desenho contorna, com dois dialetos de terceiros para
motores analíticos. Sem o código existente, a biblioteca ficaria assim:

- O contrato numa declaração própria, com o esquema Arrow como forma canônica: o Delta o recebe em
  `DeltaTable.create`, o DuckDB mapeia Arrow nativamente, e o Redshift ganha um renderizador de DDL
  a partir da tabela de tipos. As opções físicas ficam em metadados da biblioteca, como hoje.
  Nenhum dialeto de terceiros, nenhum `@compiles`.
- As consultas em SQL escrito à mão, um arquivo por consulta, no dialeto do DuckDB, que roda local
  nos testes. O SQLGlot entra como ferramenta, não como DSL: `qualify` contra o esquema do contrato
  acusa coluna inexistente no teste, a única garantia que o Core dava sobre texto; transpila para o
  Redshift; reescreve os nós de tabela para aplicar o prefixo do ambiente; e um teste acusa
  construções só do DuckDB. A consulta rara que precisa de composição dinâmica usa o construtor do
  SQLGlot, que gera o mesmo `SELECT` nos dois dialetos.
- Os parâmetros de execução são o mês e o prefixo. O DuckDB recebe `$nome` com um dicionário, e o
  `redshift_connector` recebe `:nome` com `cursor.paramstyle = "named"` ([Suporte a
  SQLAlchemy](#suporte-a-sqlalchemy-3) do Redshift).
- Ibis, a resposta moderna para consulta portável em Python, não tem backend Redshift; SQLMesh e dbt
  assumem a orquestração inteira. Sobra o SQLGlot.
- Testes nos dois motores, já obrigatórios com o SQLAlchemy.

Com o pipeline existente, os modelos ficam como contrato, porque existem e são legíveis, e o Core
fica para o `SELECT` e o `INSERT ... SELECT` já escritos. O SQLAlchemy vira ferramenta de tempo de
geração: produz o DDL e o texto SQL de cada dialeto, e nenhum dado passa pelo engine. Consulta nova
e complexa pode nascer em texto validado por SQLGlot desde já; as duas formas convivem, porque o
produto das duas é uma string executada pelo DuckDB ou pelo `redshift_connector`.

#### O texto SQL gerado como opção de migração

O pipeline compila hoje cada statement Core pelo dialeto a cada execução, e esse continua o caminho
padrão: o motor compila a cópia prefixada com os parâmetros do cliente. A biblioteca também gera o texto SQL de cada dialeto, para um pipeline que queira sair
do SQLAlchemy uma interação com o banco por vez: o texto
gerado entra no repositório do pipeline, revisado no diff; um teste o compara com uma nova geração
enquanto o statement Core existir; e a chamada que compilava o statement passa a executar o texto.
No fim desse caminho, o SQLAlchemy fica nos modelos e na geração; `duckdb_engine` e
`sqlalchemy-redshift` continuam dependências de execução da biblioteca, porque os motores compilam
pelo dialeto o statement Core que recebem. As primitivas, em `serialize_db.sql`:

| Primitiva | O que faz |
| --- | --- |
| `prefixed(statement, metadata, prefix)` | Troca cada tabela do contrato num statement pronto pela cópia com o prefixo do sandbox, por `replacement_traverse`; todo nome da cópia vai citado, `quoted_name(quote=True)`, com o sentinela `{prefix}` dentro das aspas como no DDL. |
| `render(statement, dialect, metadata, prefix)` | Texto do dialeto com as constantes embutidas e cada `bindparam` sem valor como `:nome`, o mesmo statement que roda num `Connection` do cliente; o `bindparam` com valor sai como constante. |
| `write_sql_files(statements, metadata, directory)` | `sql/<nome>.duckdb.sql` e `sql/<nome>.redshift.sql`, comparados por teste como os arquivos de esquema. |
| `read_sql(directory, name, dialect, prefix)` e `bind(sql, params, dialect)` | O texto versionado com o sentinela trocado pelo `prefix` informado, obrigatório, e os marcadores `:nome` reescritos para o motor (`$nome` no DuckDB, `paramstyle = "named"` no `redshift_connector`), com toda região citada intacta; a `query` e o `stream` dos motores rodam o texto pronto. |

Os comportamentos do compilador que definem `render`, verificados em 2026-09-19 com SQLAlchemy
2.0.54, duckdb_engine 0.17.0, sqlalchemy-redshift 1.0.0 e DuckDB 1.5.5:

- Um dialeto avulso usa `pyformat` (`duckdb_engine.Dialect()`) ou `format`
  (`RedshiftDialect_redshift_connector()`), e com eles `literal_binds` dobra o `%` dos literais:
  `LIKE 'A%'` sai `LIKE 'A%%'` e `strftime(data_ref, '%Y-%m')` sai `'%%Y-%%m'`. O texto está certo
  como comando com parâmetros do DBAPI e errado como SQL. `Dialect(paramstyle="named")` desliga a
  dobra nos dois dialetos.
- `bindparam("mes")` sem valor e `text("mes = :mes")` não falham sob `literal_binds`: viram
  `mes = NULL`. O `SAWarning` sai só numa comparação por `=`; no `text()`, no `LIKE`, no
  `coalesce`, na coluna de um `select` e no `VALUES` de um `INSERT` o `NULL` sai calado, e
  `compiled.binds`, sem `literal_binds`, marca o parâmetro `required` nas sete formas (leitura de
  2026-09-22). `render` troca cada `BindParameter` com `required=True` por
  `literal_column(":nome")` por `replacement_traverse` antes de compilar, e o texto sai
  `mes = :mes` nos dois casos, sem tocar no filtro de avisos do processo (2026-09-22).
- O dialeto avulso do DuckDB herda do PostgreSQL o `_backslash_escapes` verdadeiro e dobra a
  contrabarra de cada constante que escreve, `'a\\b'` para `a\b`, também no `ESCAPE` do `LIKE`;
  o DuckDB lê as duas, e o `=` não acha a linha. O duckdb-engine não o desliga ao conectar,
  porque o `initialize` dele pula o do PostgreSQL. `render` e o motor DuckDB o fixam em falso no
  dialeto do DuckDB, e o do Redshift segue verdadeiro (leitura de 2026-10-03).
- Um nome de tabela com `{` é citado, `"{prefix}cad_operacoes"`; `quoted_name(..., quote=False)` o
  deixa sem aspas nos dois dialetos.
- Um esquema `banco.esquema`, o nome em três partes do datashare do ambiente alvo, cai na mesma
  regra: `MetaData(schema="datalake_rw_shared.sbx_aco_decon")` compila
  `CREATE TABLE "datalake_rw_shared.sbx_aco_decon".operacoes`, um identificador só entre aspas, e
  `MetaData(schema=quoted_name("datalake_rw_shared.sbx_aco_decon", False))` compila
  `datalake_rw_shared.sbx_aco_decon.operacoes` no DDL, no `select` e no `insert` (medido em
  2026-09-20 com o dialeto do Redshift, `test_sqlalchemy.py::test_three_part_name_needs_quoted_name_without_quotes`).
  O dialeto do SQL Server quebra um esquema com ponto em partes; o do Redshift, derivado do
  PostgreSQL, não.

O bloco abaixo é o ensaio de 2026-09-19, com a cópia `quote=False` e o `SAWarning` transformado em
erro; a forma atual das primitivas está em `serialize_db.sql`.

```python
"""Renderiza um select do Core como texto de cada dialeto: constantes embutidas, mês como parâmetro, prefixo do sandbox como sentinela; executa o texto no DuckDB."""
import decimal
import re
import warnings

import duckdb
import duckdb_engine
import sqlalchemy as sa
from sqlalchemy.exc import SAWarning
from sqlalchemy.sql import quoted_name
from sqlalchemy.sql.visitors import replacement_traverse
from sqlalchemy_redshift.dialect import RedshiftDialect_redshift_connector

metadata = sa.MetaData()
operations = sa.Table(
    "cad_operacoes", metadata,
    sa.Column("id_operacao", sa.BigInteger, primary_key=True, autoincrement=False),
    sa.Column("id_cliente", sa.BigInteger, nullable=False),
    sa.Column("valor", sa.Numeric(18, 2), nullable=False),
    sa.Column("mes", sa.String(7), nullable=False),
)
clients = sa.Table(
    "cad_clientes", metadata,
    sa.Column("id_cliente", sa.BigInteger, primary_key=True, autoincrement=False),
    sa.Column("nome", sa.String(200), nullable=False),
    sa.Column("mes", sa.String(7), nullable=False),
)
DIALECTS = {"duckdb": duckdb_engine.Dialect(paramstyle="named"),
            "redshift": RedshiftDialect_redshift_connector(paramstyle="named")}


def param(name, type_=None):
    """Parâmetro de execução: o texto :nome atravessa literal_binds e é resolvido na execução."""
    return sa.literal_column(f":{name}", type_=type_)


def prefixed(statement, metadata, prefix="{prefix}"):
    """Troca cada tabela do contrato pela cópia com o prefixo do sandbox; o sentinela sai sem aspas."""
    copies = {t: t.to_metadata(sa.MetaData(), name=quoted_name(f"{prefix}{t.name}", quote=False))
              for t in metadata.tables.values()}

    def replace(element):
        if isinstance(element, sa.Table):
            return copies.get(element)
        if isinstance(element, sa.Column) and element.table in copies:
            return copies[element.table].c[element.name]
        return None

    return replacement_traverse(statement, {}, replace)


def render(statement, dialect, metadata, prefix="{prefix}"):
    """Texto do dialeto com as constantes embutidas; um bindparam sem valor viraria NULL, então é erro."""
    with warnings.catch_warnings():
        warnings.simplefilter("error", SAWarning)
        compiled = prefixed(statement, metadata, prefix).compile(
            dialect=DIALECTS[dialect], compile_kwargs={"literal_binds": True})
    return str(compiled)


query = (
    sa.select(clients.c.nome, sa.func.sum(operations.c.valor).label("total"))
    .join_from(operations, clients, operations.c.id_cliente == clients.c.id_cliente)
    .where(operations.c.mes == param("mes", sa.String(7)), operations.c.valor > decimal.Decimal("100.00"),
           clients.c.nome.like("A%"))
    .group_by(clients.c.nome).order_by(clients.c.nome)
)
texts = {dialect: render(query, dialect, metadata) for dialect in DIALECTS}
print(f"-- duckdb\n{texts['duckdb']}\n")
print("-- redshift:", "o mesmo texto" if texts["redshift"] == texts["duckdb"] else texts["redshift"], "\n")
try:
    render(sa.select(operations.c.id_cliente).where(operations.c.mes == sa.bindparam("mes")), "duckdb", metadata)
except SAWarning as e:
    print("bindparam sem valor:", str(e).split(";")[0], "\n")

con = duckdb.connect()
con.execute("CREATE TABLE exec_42_cad_operacoes (id_operacao BIGINT, id_cliente BIGINT, valor DECIMAL(18,2), mes VARCHAR)")
con.execute("CREATE TABLE exec_42_cad_clientes (id_cliente BIGINT, nome VARCHAR, mes VARCHAR)")
con.execute("INSERT INTO exec_42_cad_operacoes VALUES (1, 7, 150.00, '2026-08'), (2, 7, 50.00, '2026-08'), (3, 9, 200.00, '2026-07'), (4, 9, 120.00, '2026-08')")
con.execute("INSERT INTO exec_42_cad_clientes VALUES (7, 'Alfa', '2026-08'), (9, 'Beta', '2026-08')")
params = {"mes": "2026-08"}
sql = re.sub(rf"(?<!:):({'|'.join(params)})\b", r"$\1", render(query, "duckdb", metadata, prefix="exec_42_"))
print(sql, "\n")
print(con.execute(sql, params).fetchall())
```

Saída:

```
-- duckdb
SELECT {prefix}cad_clientes.nome, sum({prefix}cad_operacoes.valor) AS total
FROM {prefix}cad_operacoes JOIN {prefix}cad_clientes ON {prefix}cad_operacoes.id_cliente = {prefix}cad_clientes.id_cliente
WHERE {prefix}cad_operacoes.mes = :mes AND {prefix}cad_operacoes.valor > 100.00 AND {prefix}cad_clientes.nome LIKE 'A%' GROUP BY {prefix}cad_clientes.nome ORDER BY {prefix}cad_clientes.nome

-- redshift: o mesmo texto

bindparam sem valor: Bound parameter 'mes' rendering literal NULL in a SQL expression

SELECT exec_42_cad_clientes.nome, sum(exec_42_cad_operacoes.valor) AS total
FROM exec_42_cad_operacoes JOIN exec_42_cad_clientes ON exec_42_cad_operacoes.id_cliente = exec_42_cad_clientes.id_cliente
WHERE exec_42_cad_operacoes.mes = $mes AND exec_42_cad_operacoes.valor > 100.00 AND exec_42_cad_clientes.nome LIKE 'A%' GROUP BY exec_42_cad_clientes.nome ORDER BY exec_42_cad_clientes.nome

[('Alfa', Decimal('150.00'))]
```

No Redshift, o mesmo texto roda com `cursor.paramstyle = "named"` e o dicionário de parâmetros; o
exemplo foi compilado, não executado num cluster.

### Referências

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
- FAQ do SQLAlchemy sobre renderizar expressões como texto (`literal_binds`, `render_postcompile`):
  <https://docs.sqlalchemy.org/en/20/faq/sqlexpressions.html>; `create_mock_engine` na página de
  conexões: <https://docs.sqlalchemy.org/en/20/core/connections.html>.
- Lista completa das páginas consultadas: [REFERENCES.md][references].

[references]: https://github.com/felipenoris/serialize-db/blob/main/REFERENCES.md
[tipos]: ../serialize_db.html#tabela-de-mapeamento-de-tipos
