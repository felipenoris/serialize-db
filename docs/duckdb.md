# DuckDB

## Sandbox, inserção e exportação

- O sandbox é um banco em memória ou num arquivo local. Com `memory_limit`, operações maiores que a
  memória vão para `temp_directory`, no EBS do espaço.
- O DuckDB lê tabelas Arrow sem cópia. `INSERT INTO <tabela> BY NAME SELECT * FROM entrada`, com
  `entrada` registrada na conexão, associa as colunas por nome. DataFrames pandas e polars são
  convertidos para Arrow com o esquema do modelo antes da inserção, o que evita surpresas de dtype como
  inteiros com nulos convertidos para float.
- A exportação usa o writer paralelo do DuckDB. `FILE_SIZE_BYTES` divide o mês em vários arquivos, e
  `FIELD_IDS` grava os field IDs da tabela Iceberg:

```sql
COPY (
    SELECT <colunas com cast> FROM <tabela> WHERE <mês> ORDER BY <chave de ordenação>
) TO '<local da tabela>/data/<mes>/<id_execucao>' (
    FORMAT parquet,
    COMPRESSION zstd,
    FILE_SIZE_BYTES '<tamanho alvo>',
    FIELD_IDS {<coluna>: <field ID>}
);
```

