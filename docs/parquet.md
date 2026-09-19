# Arquivos Parquet

Este documento explica a estrutura de um arquivo Parquet, os metadados que ele carrega, como
inspecioná-los com DuckDB e PyArrow, como o particionamento funciona e como usar tudo isso para
acelerar consultas e para importar e exportar dados no DuckDB e no Redshift com um esquema definido.

As fontes são a especificação do formato (repositório `apache/parquet-format`, arquivo
`parquet.thrift`), a documentação do DuckDB, do PyArrow e do Redshift, e os artigos listados em
[REFERENCES.md](../REFERENCES.md). Os exemplos e as medições foram executados em 2026-09-18 com DuckDB
1.5.5 e PyArrow 25.0.1 sobre um arquivo de exemplo com 300.000 linhas, 3 row groups de 100.000 linhas
e 5 colunas (`id_operacao`, `data_ref`, `id_cliente`, `valor`, `descricao`), ordenado por
`data_ref, id_operacao`, no modelo de [schema.md](schema.md).

## Estrutura do arquivo

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

## Metadados do arquivo Parquet

Todas as estruturas de metadados são definidas em Thrift no arquivo `parquet.thrift` e serializadas
com `TCompactProtocol`. Há dois grupos: os metadados do rodapé (`FileMetaData` e tudo que ele contém)
e os cabeçalhos de página, gravados junto dos dados. O page index, os filtros Bloom e os metadados de
criptografia ficam fora do rodapé, e o rodapé guarda apenas os offsets para eles.

### FileMetaData

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

Nos exemplos em Python deste documento, `tabela` é a tabela Arrow do mês com as cinco colunas do
contrato, `esquema` é o esquema dela (`arrow_schema` de [sqlalchemy.md](sqlalchemy.md), sem a coluna
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

### Schema

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
`String`, e a tabela Delta aceita arquivos das duas formas ([campos JSON](schema.md)).

```python
import pyarrow as pa
import pyarrow.parquet as pq

# A extensão arrow.json vira o tipo lógico JSON no rodapé; o DuckDB lê a coluna como JSON.
docs = pa.array(['{"origem": "A"}', None], pa.string()).cast(pa.json_(pa.string()))
pq.write_table(pa.table({"meta": docs}), "eventos.parquet")
pq.ParquetFile("eventos.parquet").schema.column(0).logical_type            # JSON
con.sql("DESCRIBE SELECT * FROM 'eventos.parquet'").fetchall()[0][1]      # 'JSON'
```

### Row group

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

### ColumnMetaData

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
entra no log ([delta.md](delta.md), ingestão):

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

O dicionário é o argumento `stats` de `register_file` em [delta.md](delta.md). Com um
arquivo de agosto registrado assim, `dt.file_uris(file_pruning_predicate="data_ref >= '2026-09-15'")`
devolveu lista vazia, e `mes = '2026-08'` devolveu só esse arquivo.

### Page index

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

### Bloom filters

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

### Page headers

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

### Outros metadados

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

table_with_metadata = table.replace_schema_metadata({"serialize_db_version": "0.1.0", "execution_id": "abc123"})
pq.write_table(table_with_metadata, "operacoes.parquet")
print(pq.read_metadata("operacoes.parquet").metadata.keys())
# dict_keys([b'ARROW:schema', b'execution_id', b'serialize_db_version'])
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

## Inspeção dos metadados com DuckDB

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
-- serialize_db_version │ 0.1.0
-- execution_id         │ abc123
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

## Inspeção dos metadados com PyArrow

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

## Particionamento de arquivos Parquet

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
  tamanhos parecidos. O mês como unidade de escrita, em [guia.md](guia.md), segue essa regra.

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
([delta.md](delta.md)), e o particionamento dentro do arquivo, que são os row groups ordenados. As duas camadas
se combinam: o diretório elimina meses, e as estatísticas dos row groups eliminam faixas dentro do
mês.

## Otimização do Parquet para performance de consultas

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

### Filtros por chaves

Chave aqui é a chave primária do modelo ou um identificador usado em busca pontual (`WHERE
id_operacao = 42`, `WHERE id_cliente IN (...)`).

Ordenar pela chave na gravação. Estatísticas min/max só podam quando as faixas dos row groups são
estreitas e disjuntas, e isso depende da ordem física. A documentação do DuckDB compara um inteiro
sequencial, que permite pular todos os row groups menos um, com um UUID aleatório, que obriga a ler
todos. No arquivo de exemplo, ordenado por `data_ref, id_operacao`, o filtro por agosto leu 1 dos 3
row groups (a consulta sobre `parquet_metadata` na seção de inspeção mostra os dois pulados), enquanto
`id_cliente`, aleatória, tem `1` a `5000` em todos. Com chave composta, a segunda coluna só poda
dentro de faixas da primeira; a `sort_key` de [schema.md](schema.md) (`data_ref`,
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

### Filtros por valores de colunas

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
monetários ficam em `DECIMAL`, como no contrato de [schema.md](schema.md).

Strings. Estatísticas de strings comparam bytes sem sinal e podem ser truncadas pelo gravador (o
DuckDB expõe `min_is_exact` e `max_is_exact`). O DuckDB reescreve `LIKE 'op-1%'` como
`descricao >= 'op-1' AND descricao < 'op-2'` e empurra o intervalo ao scan (visível em `EXPLAIN`);
um `LIKE '%sufixo'` não vira intervalo e lê tudo. Colunas de texto longo devem ficar fora das
consultas que não as usam, porque o custo de decodificar e descomprimir strings domina.

Projeção. Selecionar só as colunas necessárias é a otimização com mais efeito em tabelas largas:
cada coluna é um column chunk separado, e colunas aninhadas são folhas separadas. `SELECT *` sobre
arquivos remotos baixa o arquivo inteiro.

### Joins com outras tabelas

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
[duckdb.md](duckdb.md) já descreve. `SET disabled_optimizers = 'join_order,build_side_probe_side'`
força a ordem escrita quando o plano sai ruim.

Mesma chave, mesmo tipo, mesma partição. Chaves de join com tipos diferentes (`INT32` de um lado,
`INT64` do outro, ou `VARCHAR` contra inteiro) exigem cast e impedem o filtro dinâmico de chegar ao
scan. Os dois lados devem sair do mesmo contrato de tipos. Particionar as duas tabelas pelo mesmo
critério (mês) e filtrar pelo mês na consulta reduz os dois lados antes do join; nenhum dos leitores
faz join por partição sozinho, então o predicado precisa estar na consulta.

No Redshift, as decisões de join ficam no banco, não no arquivo: `DISTKEY` na chave de join e `SORTKEY`
nas colunas de filtro, conforme [schema.md](schema.md). Para tabelas externas, a documentação do
Spectrum recomenda manter as tabelas de dimensão locais e as tabelas de fato no S3, definir `numRows`
em `TABLE PROPERTIES` (ou usar as estatísticas de coluna do Glue) para o otimizador, e escrever
predicados e agregações que o Spectrum consiga executar na camada de leitura; `DISTINCT` e `ORDER
BY` não descem.

### Outros tópicos de otimização

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
(`store_decimal_as_integer` no PyArrow; o DuckDB já grava assim) e strings com dicionário. O
[contrato de tipos](schema.md) já fixa isso.

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

## Importação e exportação de Parquet no DuckDB

### Leitura direta

`read_parquet` (ou o caminho terminado em `.parquet`) lê um arquivo, uma lista ou um glob, com
projeção e filtros empurrados ao leitor. Parâmetros: `binary_as_string`, `can_have_nan`,
`encryption_config`, `file_row_number`, `hive_partitioning`, `union_by_name` e `schema`. A coluna
virtual `filename` existe desde a 1.3.0. Uma `VIEW` sobre `read_parquet` consulta os arquivos no
lugar, e `CREATE TABLE ... AS FROM read_parquet(...)` os carrega com os tipos inferidos.

### Importação com esquema definido

O esquema definido é a tabela criada antes da carga, com tipos, `NOT NULL` e, quando vale o custo
medido em [schema.md](schema.md), chaves. O que o DuckDB verifica ao carregar, medido na 1.5.5:

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

Chaves primárias e únicas são verificadas por índice ART na inserção, ao custo medido em
[schema.md](schema.md); a auditoria por consulta depois da carga é a alternativa. `CHECK` está
disponível. Para cargas maiores que a memória, `SET preserve_insertion_order = false` reduz o uso de
memória em `COPY` e em `CREATE TABLE AS`.

`EXPORT DATABASE 'dir' (FORMAT parquet)` e `IMPORT DATABASE 'dir'` movem um banco inteiro: o
diretório recebe `schema.sql` com o DDL (`CREATE TABLE t(id BIGINT NOT NULL, ...)`), `load.sql` com
um `COPY ... FROM` por tabela e um Parquet por tabela. A importação recria as tabelas com as
restrições e carrega os arquivos por posição.

### Exportação com esquema definido

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
ordenação, `FIELD_IDS` com os identificadores do modelo e `KV_METADATA` com a identificação da
execução. O que o DuckDB não consegue expressar: `REQUIRED` (toda coluna sai `optional`) e
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
    FIELD_IDS {id_operacao: 1, data_ref: 2, id_cliente: 3, valor: 4, descricao: 5},
    KV_METADATA {serialize_db_version: '0.1.0', execution_id: 'abc123'},
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

## Importação e exportação de Parquet no Redshift

O Redshift oferece três caminhos: `COPY` carrega arquivos do S3 numa tabela; `UNLOAD` grava o
resultado de uma consulta no S3; o Redshift Spectrum consulta arquivos no lugar por tabelas externas e
também grava, por `CREATE EXTERNAL TABLE ... AS` e `INSERT` em tabela externa. As regras abaixo vêm
da documentação oficial; o que ela não diz está marcado como pendente da prova de conceito.

### COPY a partir de Parquet

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
  a lista. Pendente da prova de conceito: se a lista de colunas é aceita com Parquet.
- Só estas opções: `ACCEPTINVCHARS`, `FILLRECORD`, `FROM`, `IAM_ROLE`, `STATUPDATE`, `MANIFEST`,
  `EXPLICIT_IDS`. `MAXERROR`, `IGNOREALLERRORS`, `ACCEPTANYDATE` e `REGION` não são aceitos.
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

O manifesto sai da lista de arquivos do snapshot Delta ([delta.md](delta.md), exportação):

```python
# O manifesto do COPY sai da lista de arquivos do snapshot Delta; content_length é obrigatório em cada entrada.
import json
import pyarrow as pa
from deltalake import DeltaTable

dt = DeltaTable("s3://bucket/operacoes/", storage_options=s3_options)  # credenciais conforme delta.md
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
| Tipo do Parquet compatível com a coluna | Exigido. A documentação não publica a tabela de correspondência entre tipos Parquet e tipos Redshift; os tipos físicos de `TIMESTAMP` (`INT64` em microssegundos) e `DECIMAL` (`INT64` ou `FIXED_LEN_BYTE_ARRAY`) ficam pendentes da prova de conceito, como registrado em [redshift.md](redshift.md). |
| Número e ordem das colunas | Exigidos, por posição. |
| `NOT NULL` | Aplicado; um `NULL` em coluna `NOT NULL` falha o comando. |
| Comprimento de `VARCHAR` | Em bytes. `TRUNCATECOLUMNS` não está na lista de opções aceitas para Parquet, então um valor maior que a coluna falha. Pendente da prova de conceito. |
| `PRIMARY KEY`, `UNIQUE`, `FOREIGN KEY` | Informativas; não são verificadas. Duplicatas entram e depois produzem resultados errados no planejador, como descrito em [schema.md](schema.md). |

A carga com verificação segue [redshift.md](redshift.md): Arrow com o esquema do modelo, Parquet
numa área de staging, `COPY` numa tabela do sandbox da execução, `pg_last_copy_count()` contra a
contagem enviada e auditoria de unicidade por consulta antes de publicar.

### UNLOAD para Parquet

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
  `VARBYTE` só saem em texto ou CSV. A perda do fuso em `TIMESTAMPTZ` está registrada em
  [redshift.md](redshift.md).

O esquema definido entra pela consulta: colunas na ordem do modelo, casts para os tipos do contrato
e `ORDER BY` pela chave de ordenação. A documentação do `UNLOAD` não menciona `field_id`, metadados
chave-valor nem `sorting_columns`, não informa os tipos físicos gerados nem a presença de
estatísticas min/max, e [redshift.md](redshift.md) deixa isso para a prova de conceito. A verificação depois da
descarga lê o manifesto e o rodapé de cada arquivo com PyArrow ou DuckDB (`parquet_schema` e
`parquet_metadata`) e compara com o modelo.

### Tabelas externas do Redshift Spectrum

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
  declara a regra para Parquet puro, e a mensagem de erro cita a coluna pelo nome. Pendente da prova
  de conceito.
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
da tabela Delta ([delta.md](delta.md)).
