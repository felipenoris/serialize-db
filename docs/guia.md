
# Guia geral de implementação

## Boas práticas de ETL aplicáveis

### Partições imutáveis e substituição idempotente

O mês (`YYYY-MM`) é a unidade de escrita das tabelas particionadas. Uma reexecução ou correção grava o
mês inteiro de novo e substitui o anterior. Reexecuções ficam idempotentes, e a exportação incremental
se reduz a publicar os meses gravados.

### Write-Audit-Publish

Os dados novos são gravados numa área invisível aos leitores, auditados (contagem de linhas, nulos,
unicidade de chaves, limites de tipo) e só então publicados. Uma auditoria reprovada interrompe a
execução sem alterar as tabelas permanentes. O [sandbox da execução](#sandbox-da-execução) é essa área.

### Contrato de esquema

O esquema vive num único lugar, os modelos ORM, e dele derivam o DDL, o esquema Arrow, o esquema
Iceberg e as auditorias. O [etl-cookbook-tutorial](https://github.com/felipenoris/etl-cookbook-tutorial)
chama esse uso de "modelos como contrato".

### Commit atômico nos metadados

O S3 não renomeia diretórios de forma atômica, e listar um prefixo mistura arquivos antigos, novos e
parciais. Em aberto: como garantir atualização segura dos metadados?

## Esquema a partir dos modelos ORM

Gerar o DDL a partir dos modelos. O DDL gerado cobre as tabelas do sandbox no DuckDB e no
Redshift e o esquema Iceberg. As vantagens do DDL manual, revisão explícita e opções físicas, vêm de
dois mecanismos:

- Opções físicas no próprio modelo, num espaço de nomes da biblioteca em `Table.info`. A chave de
  ordenação vira `SORTKEY` no Redshift e `ORDER BY` na gravação dos arquivos.
- Arquivos `.sql` com o DDL gerado por backend e um JSON com o esquema Iceberg, versionados no
  repositório do pipeline e comparados por um teste. Uma mudança no modelo aparece no diff.

```python
class Operacao(Base):
    __tablename__ = "operacoes"
    __table_args__ = {
        "info": {
            "serialize_db": {
                "particionamento": {"coluna": "data_ref", "transformacao": "month"},
                "chave_ordenacao": ["data_ref", "id_operacao"],
                "redshift": {"diststyle": "KEY", "distkey": "id_cliente"},
            }
        }
    }

    id_operacao: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    data_ref: Mapped[date] = mapped_column(primary_key=True)
    id_cliente: Mapped[int] = mapped_column(BigInteger)
    valor: Mapped[Decimal] = mapped_column(Numeric(18, 2))
    descricao: Mapped[str | None] = mapped_column(String(200))
```

O `sqlalchemy-redshift` aceita `redshift_diststyle`, `redshift_distkey`, `redshift_sortkey` e
`redshift_interleaved_sortkey` como argumentos de `Table`. Guardar as opções em `info` mantém os
modelos neutros quando o dialeto do Redshift não está instalado.

### Política de restrições

| Restrição | Sandbox DuckDB | Sandbox Redshift | Tabela Iceberg |
| --- | --- | --- | --- |
| `NOT NULL` | Declarada. | Declarada e aplicada pelo banco. | Campo opcional; a auditoria verifica. |
| `PRIMARY KEY`, `UNIQUE` | Omitida; a auditoria verifica os meses novos. | Declarada quando auditada; informativa. | Não declarada. |
| `FOREIGN KEY` | Omitida; auditoria opcional. | Declarada quando auditada; informativa. | Não declarada. |

Unicidade, chave primária e chave estrangeira são informativas no Redshift. O planejador usa essas
chaves para decorrelacionar subconsultas, ordenar e eliminar joins, e supõe que elas são válidas. Com
chaves inválidas, consultas retornam resultados errados; a documentação cita um `SELECT DISTINCT` que
devolve duplicatas. `NOT NULL` é aplicado. Tabelas Iceberg no Redshift não aceitam restrições.

A medição publicada na documentação do DuckDB, com 554 milhões de linhas, justifica omitir chaves no
DuckDB:

| Operação | Tempo |
| --- | --- |
| Carga com chave primária | 461,6 s |
| Carga sem chave primária | 121,0 s |
| Criação da chave primária após a carga | 242,0 s |

A documentação recomenda não declarar restrições no DuckDB, exceto para garantir integridade. Índices
ART precisam caber em memória durante a criação.

### Tipos no contrato

| SQLAlchemy | Arrow | Iceberg | DuckDB | Redshift | Observação |
| --- | --- | --- | --- | --- | --- |
| `SmallInteger` | `int16` | `int` | `SMALLINT` | `SMALLINT` | O Iceberg não tem inteiro de 16 bits; a gravação converte para `int32`. |
| `Integer` | `int32` | `int` | `INTEGER` | `INTEGER` | |
| `BigInteger` | `int64` | `long` | `BIGINT` | `BIGINT` | Tipos sem sinal do Arrow e do DuckDB ficam fora do contrato. |
| `Boolean` | `bool` | `boolean` | `BOOLEAN` | `BOOLEAN` | |
| `Double` | `float64` | `double` | `DOUBLE` | `DOUBLE PRECISION` | Descarregar e recarregar pelo Redshift pode perder precisão. |
| `Numeric(p, s)` | `decimal128(p, s)` | `decimal(p, s)` | `DECIMAL(p, s)` | `DECIMAL(p, s)` | `p` até 38. |
| `String(n)` | `string` | `string` | `VARCHAR` | `VARCHAR(n)` | `n` em bytes no Redshift; auditoria de tamanho. |
| `Text` | `string` | `string` | `VARCHAR` | `VARCHAR(65535)` | `TEXT` no Redshift vira `VARCHAR(256)`. |
| `Date` | `date32` | `date` | `DATE` | `DATE` | |
| `DateTime` | `timestamp[us]` | `timestamp` | `TIMESTAMP` | `TIMESTAMP` | Cast explícito de nanossegundos (padrão do pandas) para microssegundos; o `add_files` rejeita nanossegundos. O Athena lê com precisão de milissegundos. |
| `DateTime(timezone=True)` | `timestamp[us, tz=UTC]` | `timestamptz` | `TIMESTAMPTZ` | `TIMESTAMPTZ` | Gravar sempre em UTC; o `UNLOAD` descarta o fuso. |
| `Uuid` | `string` | `string` | `VARCHAR` | `VARCHAR(36)` | O Redshift não tem tipo UUID. |
| `JSON`, `LargeBinary`, `ARRAY`, `Interval` | | | | | Fora do contrato até haver um caso de uso. |

## Redshift

Diagnóstico pelo SQL, na conexão da equipe:

| Comando | O que mostra |
| --- | --- |
| `SELECT current_user;` | Usuário do banco da sessão. |
| `SELECT user_name, superuser, createdb FROM svv_user_info WHERE user_name = current_user;` | Se o usuário da sessão é superusuário e se pode criar bancos. |
| `SELECT database_name, database_type, database_isolation_level FROM svv_redshift_databases;` | Bancos acessíveis, locais ou compartilhados, e o nível de isolamento de cada um. |
| `SELECT database_name, schema_name, schema_type FROM svv_all_schemas;` | Esquemas locais, externos e compartilhados; usuários comuns só veem os próprios dados. |
| `SHOW GRANTS FOR <usuario> FROM DATABASE <banco>;` | Permissões do usuário no banco compartilhado. |
| `SELECT default_iam_role();` | Papel IAM padrão, usado por `IAM_ROLE default` no `COPY` e no `UNLOAD`. |
| `SHOW data_catalog_auto_mount;` | Se o `awsdatacatalog` está montado no workgroup. |

### Diagnóstico do Redshift do projeto

Resultados na conexão da equipe, em 2026-09-13:

| Comando | Resultado | Conclusão |
| --- | --- | --- |
| `SELECT current_user;` | `admin` | A conexão por segredo entra como `admin`. O campo `adminUsername` de `aws redshift-serverless get-namespace` mostra se esse é o administrador do namespace. |
| `svv_redshift_databases` | Banco local `dev`, com `Snapshot Isolation`, e o banco compartilhado, com isolamento `UNKNOWN`. | O workgroup tem um banco local para o sandbox. |
| `SHOW GRANTS FOR admin FROM DATABASE <banco compartilhado>;` | Nenhuma linha. | O comando não esclarece as permissões recebidas pelo datashare. |
| `SELECT default_iam_role();` | `none` | `IAM_ROLE default` não funciona. `COPY` e `UNLOAD` precisam do ARN de um papel associado ao namespace. |
| `SHOW data_catalog_auto_mount;` | `on` | O `awsdatacatalog` está montado, mas só atende sessões com identidade IAM. |
| `CREATE SCHEMA` no banco compartilhado | `Permission denied on producer to execute create schema` | O produtor não concede `CREATE` no banco ao datashare. |

### Carga de DataFrames com COPY

A documentação do Redshift recomenda `COPY` para cargas e `INSERT` com várias linhas apenas quando
`COPY` não é possível. O `INSERT` grande da biblioteca atual vira este fluxo:

1. Converter o DataFrame numa tabela Arrow com o esquema do modelo (cast seguro).
2. Gravar Parquet em `<caminho S3 do projeto>/staging/<id_execucao>/<tabela>/`, fora dos locais das
   tabelas Iceberg.
3. `COPY execucao_<id>.<tabela> FROM '<manifesto>' IAM_ROLE '<arn>' FORMAT AS PARQUET MANIFEST`, ou com
   `CREDENTIALS` enquanto o namespace não tiver papel associado.
4. Comparar `pg_last_copy_count()` com o número de linhas enviadas.

A biblioteca apaga o staging da execução ao terminar, e a rotina de limpeza remove o staging de execuções
que falharam. Uma regra de ciclo de vida no bucket do domínio dependeria do administrador.

Implementações existentes servem de referência ou dependência:

- `awswrangler.redshift.copy` (versão 3.17.1, 2026-08-03) executa o mesmo fluxo.
- O driver ADBC para Redshift (versão 1.7.0, 2026-09-09) faz ingestão em bloco e leitura em Arrow. Ele
  merece um benchmark contra o fluxo acima.

### Regras do COPY para Parquet

| Regra da documentação | Consequência para a biblioteca |
| --- | --- |
| Colunas são associadas por posição, e a quantidade precisa coincidir com a tabela. | A ordem das colunas no Parquet é a ordem do modelo. Os dois derivam do mesmo `Table`. |
| Só existem as colunas gravadas no arquivo. | Colunas de partição ficam dentro do arquivo. |
| Parâmetros aceitos: `ACCEPTINVCHARS`, `FILLRECORD`, `FROM`, `IAM_ROLE`, `CREDENTIALS`, `STATUPDATE`, `MANIFEST`, `EXPLICIT_IDS`. `MAXERROR` não é aceito. | O primeiro erro aborta o `COPY`. A validação acontece antes, no Arrow. |
| `MANIFEST` é aceito. | O `COPY` carrega exatamente os arquivos gravados pela biblioteca. |
| O bucket precisa estar na mesma região do Redshift. | Configuração da infraestrutura. |
| O `COPY` de Parquet usa URLs pré-assinadas válidas por 1 hora. | Políticas IAM do bucket não podem bloquear URLs pré-assinadas. |

### Exportação com UNLOAD

Comportamento do `UNLOAD ... FORMAT AS PARQUET` segundo a documentação:

- O `UNLOAD` grava Parquet 1.0. Cada row group é comprimido com SNAPPY. O row group padrão tem 32 MB;
  `ROWGROUPSIZE` aceita de 32 MB a 128 MB em alguns tipos de nó.
- `MAXFILESIZE` aceita de 5 MB a 6,2 GB (padrão 6,2 GB) e é arredondado para baixo até um múltiplo de
  32 MB.
- Com `PARTITION BY`, as colunas de partição saem dos arquivos, exceto com `INCLUDE`.
- `CLEANPATH` apaga de forma permanente os arquivos das pastas de partição que recebem dados novos.
- Colunas `TIMESTAMPTZ` perdem a informação de fuso horário.
- O `SELECT` externo não aceita `LIMIT`.
- `MANIFEST VERBOSE` lista os arquivos, os nomes e tipos das colunas e as linhas por arquivo.
- Colunas `VARBYTE`, `GEOMETRY` e `HLLSKETCH` só saem em texto ou CSV.

Sugestão: um `UNLOAD` por mês, sem `PARTITION BY`, com destino
`<local da tabela>/data/<mes>/<id_execucao>_`, `MANIFEST VERBOSE` e `MAXFILESIZE` igual ao tamanho alvo
da tabela. O `SELECT` lista as colunas na ordem do modelo, com casts para os tipos do contrato e
`ORDER BY` pela chave de ordenação. A biblioteca confere o manifesto do `UNLOAD` e os rodapés dos
arquivos antes da publicação. `CLEANPATH` não é usado: arquivos de execuções abortadas saem pela remoção
de órfãos, do Glue ou da biblioteca.

A documentação do `UNLOAD` não informa os tipos físicos Parquet de `TIMESTAMP` e `DECIMAL`, a
obrigatoriedade das colunas nem a presença de estatísticas de mínimo e máximo. Os três afetam o
`add_files`, e a prova de conceito verifica.

O `UNLOAD` lê tabelas do sandbox no banco local, fora das regras de escrita por datashare. Sem acesso do
Redshift ao S3, a exportação lê o mês em Arrow pelo driver ADBC e grava o Parquet com o pyarrow e o
papel do projeto.

## DuckDB

### Sandbox, inserção e exportação

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

## Portabilidade de SQL entre DuckDB e Redshift

- As consultas usam construções do SQLAlchemy, não SQL em texto.
- Funções com nomes ou semânticas diferentes nos dois bancos ganham uma regra `@compiles` por dialeto.
  A lista sai do código atual do pipeline.
- O DuckDB aceita construções do PostgreSQL ausentes no Redshift, como arrays e `ON CONFLICT`. Uma
  consulta que roda no DuckDB pode falhar no Redshift, então as consultas do pipeline precisam de
  testes de integração no Redshift com uma amostra pequena.