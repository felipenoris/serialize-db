# Estratégia de implementação

Este documento registra as razões da escolha das camadas da biblioteca e as comparações entre as
ferramentas candidatas: qual camada gerencia os arquivos Parquet no S3 sem um serviço de catálogo,
qual camada gera o SQL que roda no DuckDB e no Redshift, o que substitui a migração de esquema e se
uma extensão em Rust com PyO3 compensa. A decisão que sai daqui, o estado do projeto, as etapas e o
pipeline mensal estão em [`PLAN.md`](PLAN.md); o uso do Delta, em [`delta.md`](delta.md). As fontes
estão em [`REFERENCES.md`](../REFERENCES.md). As verificações locais deste documento rodaram em
2026-09-19 com Python 3.13, deltalake 1.6.4, DuckDB 1.5.5 com as extensões `delta` e `ducklake`,
PyArrow 25.0.1 e SQLGlot 30.18.0, num macOS arm64; a prova de conceito no S3, feita no espaço do
projeto, está em `PLAN.md`.

As premissas, declaradas pelo usuário, contra as quais as ferramentas foram comparadas: o pipeline é
majoritariamente lógica Python, com o SQLAlchemy só nos modelos declarativos e em statements Core
que movem DataFrames; nenhum serviço de catálogo está habilitado; o projeto tem um único esquema no
Redshift; as execuções de desenvolvimento e de produção gravam tabelas separadas; renomear ou
remover colunas é raro; os Parquet são a fonte da verdade; o ambiente de destino não tem internet.

## O que um formato de tabela faz pela biblioteca

Um formato de tabela sobre Parquet (Iceberg, Delta Lake, DuckLake) resolve os problemas seguintes:

1. A lista de arquivos que compõem a tabela em cada versão, sem depender de listar o prefixo no S3.
2. A troca atômica dessa lista, para que um leitor nunca veja uma execução pela metade.
3. O esquema com identificadores estáveis de coluna, para ler arquivos antigos depois de uma
   evolução.
4. A especificação de partição e os valores de partição de cada arquivo.
5. As estatísticas por arquivo, para pular arquivos na leitura.
6. O histórico de versões e a política de retenção.
7. A manutenção: remoção de arquivos órfãos e compactação.

O desenho atual dos documentos (diretórios Hive, arquivos nomeados pela execução, manifestos e a
pergunta em aberto de [`guia.md`](guia.md) sobre o commit atômico) reimplementa os itens 1, 2, 4, 6 e
7. A primitiva que faltava no S3 existe desde 2024: `PutObject` com `If-None-Match: *` (2024-08-20)
cria um objeto só se ele não existe, e com `If-Match: <etag>` (2024-11-25) substitui só se a versão
atual é a esperada. O `boto3` expõe as duas como `IfNoneMatch` e `IfMatch` em `put_object`; a falha
devolve `412 Precondition Failed`, e um conflito durante a gravação devolve
`409 ConditionalRequestConflict`, a ser refeito depois de reler o ETag. Um log de commits próprio é,
portanto, viável, mas é exatamente o log do Delta Lake, que já existe em Rust com bindings Python.

## Camadas de tabela sem serviço dedicado

### Delta Lake com delta-rs

O Delta Lake guarda o log de transações em `_delta_log/` na raiz da tabela: um JSON por commit,
nomeado pela versão com 20 dígitos, checkpoints em Parquet e o ponteiro `_last_checkpoint`. As ações
`add`, `remove`, `metaData` e `protocol` reconstroem o estado por replay. O protocolo exige do
armazenamento só exclusão mútua na criação do arquivo de versão, isto é, put-if-absent. Os valores de
partição ficam na ação `add`, não nos arquivos de dados.

O `deltalake` (pacote Python do delta-rs, versão 1.6.4 de 2026-09-18) escreve e lê esse log sem
serviço externo. Ele grava em disco local, S3 e compatíveis (MinIO, R2), Azure, GCS, HDFS, LakeFS e,
por OpenDAL, outros; a exigência para vários escritores é o put-if-absent, que o S3 tem desde 2024.
Toda requisição ao S3 é assinada, e o cliente do delta-rs busca as credenciais em variáveis de
ambiente, credenciais de contêiner, web identity, metadados da instância ou `storage_options`, não em
perfis de `~/.aws/config`:

- Desde a versão 1.6.0 (2026-05-19) o `S3DynamoDbLogStore` foi removido, e a escrita condicional do
  S3 é o modo padrão de commit no S3. A discussão 4482 do repositório orienta a retirar
  `AWS_S3_LOCKING_PROVIDER` e `DELTA_DYNAMO_*` das opções; quem precisa do DynamoDB fica no ramo
  0.32.x. A página "Writing to S3 with a locking provider" da documentação ainda descreve o DynamoDB
  como obrigatório e `AWS_S3_ALLOW_UNSAFE_RENAME` como saída, e está defasada em relação à 1.6.0.
- As credenciais vêm de variáveis de ambiente, de `storage_options` ou dos metadados da instância; a
  documentação afirma que o escritor não usa o `boto3` e não lê `~/.aws/config`, mas o aviso
  `aws_config::profile::credentials` de uma falha mostra a cadeia consultando o perfil `default`. No
  espaço do SageMaker Unified Studio o escritor encontra as credenciais do contêiner do projeto; a
  falha com 403 de 2026-09-19 vinha de uma `NO_PROXY` vazia, que o cliente HTTP do delta-rs lê antes
  de `no_proxy`, e os detalhes estão em [`delta.md`](delta.md).
- `write_deltalake(data, mode=..., partition_by=..., predicate=..., schema_mode=...)` aceita tabela
  PyArrow, DataFrame pandas ou iterador de `RecordBatch`; `mode="overwrite"` com `predicate` substitui
  só as linhas que casam com o predicado e rejeita dados fora dele; `schema_mode="merge"` acrescenta
  colunas. O Polars grava com `DataFrame.write_delta`, sobre a mesma biblioteca.
- `DeltaTable` expõe `file_uris(file_pruning_predicate=...)`, `get_add_actions()`, `schema()`,
  `history()`, `load_as_version()`, `vacuum()`, `optimize`, `delete`, `update`, `merge`, `restore` e
  `create()`. `DeltaTable.alter` tem `add_columns`, `add_constraint`, `drop_constraint`,
  `add_feature`, `set_table_properties`, `drop_column_not_null` e `set_column_metadata`; não há
  `rename_column`. O `drop_columns` está no PR 4732, aberto, e exige column mapping; a escrita em
  tabelas com column mapping é o issue 3936, aberto.
- `DeltaTable.create_write_transaction(actions, mode, schema, partition_by)` registra arquivos
  Parquet já gravados por meio de `AddAction(path, size, partition_values, modification_time,
  data_change, stats)`.

A extensão `delta` do DuckDB, baseada no `delta-kernel-rs`, lê a tabela com `delta_scan('s3://...')` ou
`ATTACH ... (TYPE delta)`, com pulo de arquivos por partição, pulo de row groups por estatísticas,
projeção, vetores de exclusão, tipos primitivos, structs e `VARIANT`, e S3 por secrets do DuckDB. A
escrita é só `INSERT INTO` (append cego). As opções do `ATTACH` são `VERSION`, `PIN_SNAPSHOT` e
`PUSHDOWN_FILTERS`. Column mapping não consta da lista de recursos. As plataformas são Linux amd64 e
arm64, macOS e Windows amd64.

Verificado localmente:

| Verificação | Resultado |
| --- | --- |
| Gravação particionada por `mes` e substituição de `mes = '2026-02'` com `mode="overwrite", predicate=...` | O mês ficou com um único arquivo novo; o histórico registra `predicate` e `partitionBy`. |
| Dados fora do predicado | `DeltaError: 5 rows failed validation check`. |
| Nulo em coluna declarada `nullable=False` | `DeltaError: 1 rows failed validation check`. |
| Coluna a mais sem `schema_mode` | `SchemaMismatchError: number of fields does not match: 7 vs 6`. |
| `schema_mode="merge"` com coluna nova | Coluna acrescentada ao fim do esquema. |
| `alter.add_constraint({"valor_nao_negativo": "valor >= 0"})` | Propriedade `delta.constraints.valor_nao_negativo` na tabela. |
| Arquivo Parquet gravado pelo delta-rs | Sem a coluna de partição; `DECIMAL(18, 2)` como `INT64`; `DATE` como `INT32`; colunas não anuláveis `required`; estatísticas presentes; Snappy; um row group para 120.000 linhas. |
| `delta_scan` no DuckDB | Tipos preservados (`DECIMAL(18,2)`); todas as colunas aparecem anuláveis; `EXPLAIN` mostra o filtro `mes='2026-02'` empurrado ao scan. |
| Viagem no tempo no DuckDB | `delta_scan(caminho, version := 0)` e `ATTACH ... (TYPE delta)` com `AT (VERSION => 0)` funcionam; `delta_scan(...) AT (...)` não. |
| `create_write_transaction` com um arquivo gravado pelo PyArrow em `mes=2026-04/` | Versão 4 criada; o DuckDB lê o arquivo registrado. |
| Tipos do contrato gravados de um esquema Arrow | `int16` vira `short`, `int32` `integer`, `int64` `long`, `bool` `boolean`, `float64` `double`, `decimal128(18, 2)` `decimal(18,2)`, `string` `string`, `date32` `date`, `timestamp[us]` `timestamp_ntz`, `timestamp[us, tz=UTC]` `timestamp`. A coluna `timestamp_ntz` eleva o protocolo a leitor 3 e escritor 7 com o recurso `timestampNtz`. Um `timestamp[ns]` é aceito e gravado em microssegundos; um fuso `America/Sao_Paulo` é aceito e lido como o mesmo instante. O DuckDB lê `SMALLINT`, `INTEGER`, `BIGINT`, `BOOLEAN`, `DOUBLE`, `DECIMAL(18,2)`, `VARCHAR`, `DATE`, `TIMESTAMP` e `TIMESTAMP WITH TIME ZONE`. A tabela de tipos de [`schema.md`](schema.md) traz a coluna Delta. |
| `DeltaTable.create(schema, partition_by=..., name=..., description=..., configuration=...)` | Commit `CREATE TABLE` na versão 0 com nulidade, comentários de coluna em `metadata`, nome, descrição e as propriedades `delta.logRetentionDuration` e `delta.deletedFileRetentionDuration`; `mode="ignore"` repete a chamada sem criar versão nova. |
| Esquema na viagem no tempo | `delta_scan(caminho, version := 0)` mostra as seis colunas da versão 0; a versão atual mostra sete. |
| `restore(1)` | Nova versão 5 com os arquivos e o esquema da versão 1; operação `RESTORE` no histórico. |
| `vacuum(retention_hours=0, dry_run=True)` | Recusado: `minimum retention for vacuum is configured to be greater than 168 hours`; com `enforce_retention_duration=False`, o dry run listou três arquivos. |
| `convert_to_deltalake(uri, partition_by=..., partition_strategy="hive")` | Existe na API para registrar um diretório Parquet no lugar, sem reescrever; não foi testado. |
| `alter.add_columns` com campo `nullable=False` numa tabela com 220.000 linhas | Aceito, operação `ADD COLUMN`; a coluna lê nula em todas as linhas, e o `append` seguinte de dados lidos da própria tabela falha com `declared as non-nullable but contains null values`. |
| `alter.drop_column_not_null` | Aceito; operação `CHANGE COLUMN`, só metadados. |
| `append` com `id_cliente` em `int32`, `double` ou `string` numa coluna `long` | Aceito com e sem `schema_mode="merge"`: os dados são convertidos para o tipo da tabela, que não muda. `decimal(20, 4)` numa coluna `decimal(18, 2)`: `SchemaMismatchError: Cannot cast`. |
| `mode="overwrite"` com `schema_mode="overwrite"` e `id_cliente` em `double` | Aceito; a tabela inteira é reescrita e a coluna passa a `double`. |

### DuckLake

O DuckLake guarda os metadados em tabelas SQL de um banco de catálogo e os dados em Parquet. A versão
1.0 (2026-04-13) declara a especificação estável com compatibilidade retroativa garantida e exige
DuckDB 1.5.2 ou superior. Os catálogos são DuckDB (um único cliente), SQLite (vários clientes locais,
sem escrita concorrente, com retentativa automática), PostgreSQL (vários usuários remotos) e MySQL
(não recomendado pela documentação). A documentação da `dlt` resume: SQLite rápido e local, DuckDB
só em modo sequencial, PostgreSQL o único catálogo de produção com paralelismo.

- O formato não tem índices, `PRIMARY KEY`, `FOREIGN KEY`, `UNIQUE` nem `CHECK`; `NOT NULL` é aplicado.
- A evolução de esquema cobre adicionar, remover e renomear colunas e promoções sem perda de tipo; os
  arquivos antigos são lidos pelos `field_id` gravados no Parquet e pela tabela `ducklake_column`.
- Partições por `identity`, `bucket`, `year`, `month`, `day` e `hour`; os valores ficam em
  `ducklake_file_partition_value`, e as estatísticas por arquivo em `ducklake_file_column_stats`.
- Concorrência otimista: dois escritores tentam o mesmo `snapshot_id`, um viola a chave primária do
  catálogo, e a transação é refeita só nos metadados quando não há conflito lógico; com conflito, a
  transação aborta. Um catálogo no Redshift não serviria: o mecanismo depende de uma chave primária
  aplicada, e o Redshift não aplica chaves.
- `ducklake_add_data_files` registra arquivos existentes sem copiar; a propriedade do arquivo passa
  ao DuckLake, que pode apagá-lo na compactação ou na limpeza; tipos mais estreitos que os da tabela
  são aceitos.
- O data inlining grava inserções pequenas no próprio catálogo, não em Parquet, e está ligado por
  padrão com `DATA_INLINING_ROW_LIMIT` igual a 10 linhas; `ducklake_flush_inlined_data` materializa em
  Parquet.
- Manutenção: `ducklake_expire_snapshots`, `ducklake_cleanup_old_files`, `ducklake_merge_adjacent_files`.
- Um catálogo SQLite no S3 não é suportado (discussão 519: "sqlite can not work on an object store").

Verificado localmente com a extensão `ducklake` do DuckDB 1.5.5 (versão `d8a1881e`, metadados
`version 1.0`):

| Verificação | Resultado |
| --- | --- |
| `PRIMARY KEY` no `CREATE TABLE` | `Not implemented Error: PRIMARY KEY/UNIQUE constraints are not supported in DuckLake`. |
| Nulo em coluna `NOT NULL` | `Constraint Error: NOT NULL constraint failed`. |
| `DELETE` do mês inteiro e `INSERT` do mês novo numa transação | O arquivo antigo recebeu `end_snapshot`, o novo entrou, e nenhum arquivo de exclusão foi criado. |
| `AT (VERSION => 2)`, `ADD COLUMN ... DEFAULT`, `RENAME COLUMN` | Funcionam; `ducklake_column` guarda as versões da coluna por `begin_snapshot` e `end_snapshot`. |
| Arquivo Parquet gravado pelo DuckLake | `PARQUET:field_id` em cada coluna; `DECIMAL(18, 2)` como `INT64`; estatísticas presentes; Snappy. |
| Partição por `year(data_ref), month(data_ref)` | Diretórios `year=2026/month=1/`; a coluna `data_ref` continua dentro do arquivo. |
| Partição por identidade em `mes` | Diretórios `mes=2026-01/`; a coluna `mes` continua dentro do arquivo. |
| `INSERT` de 10 linhas | Nenhum arquivo Parquet gravado: as linhas foram inlined no catálogo. |
| `ducklake_add_data_files` | Arquivo fora do `DATA_PATH` numa tabela sem partição: aceito, com caminho absoluto. Arquivo em `mes=2026-03/` numa tabela particionada por identidade: aceito. Arquivo numa tabela particionada por `year()`/`month()`: `invalid partition value for the table configuration`; a causa não foi isolada. |
| Catálogo em SQLite | `ATTACH 'ducklake:sqlite:...'` funciona. |
| Catálogo servido por HTTP | `ATTACH 'ducklake:http://.../meta.ducklake' (READ_ONLY)` funciona, com e sem `OVERRIDE_DATA_PATH`; o arquivo de metadados também abre por `ATTACH 'http://...' (READ_ONLY)`. |

Sem PostgreSQL, o catálogo é um arquivo DuckDB ou SQLite, e cabe à biblioteca movê-lo: baixar do S3
com o ETag, anexar, executar, fechar a conexão (o `.wal` precisa ser descarregado) e subir com
`IfMatch`. Um `412` significa que outra execução publicou antes, e a execução é refeita. Os leitores
anexam o catálogo no S3 só de leitura, sem cópia local. O modelo é de um escritor por vez.

### Iceberg sem serviço de catálogo

O Iceberg exige um catálogo porque cada commit gera um novo `metadata.json`, e o ponteiro para o
atual é trocado de forma atômica fora dos arquivos da tabela. Sem serviço, o ponteiro cabe num
catálogo em arquivo: o PyIceberg 0.12.0 tem o catálogo `sql`, que com `sqlite:///caminho` guarda uma
linha por tabela (`metadata_location` e `previous_metadata_location`) e nada mais; o arquivo do teste
tem 20.480 bytes. Os demais catálogos são `rest`, `in-memory` (sem acesso concorrente, fora de
produção), `hive`, `glue` e `dynamodb`, e a configuração `py-catalog-impl` carrega uma implementação
própria da classe `Catalog`, que tem 23 métodos abstratos. `StaticTable.from_metadata` lê uma tabela
pelo arquivo de metadados, sem catálogo e só para leitura. A extensão `iceberg` do DuckDB lê por
`iceberg_scan`, sem catálogo e só de leitura; a escrita pelo DuckDB exige um catálogo REST anexado;
`iceberg_to_ducklake` copia os metadados de um catálogo Iceberg para um DuckLake. A `dlt` grava
Iceberg com um catálogo SQLite em memória por tabela. O Redshift lê Iceberg só por esquema externo no
Glue ou no Lake Formation, e o catálogo Glue do PyIceberg registra uma tabela existente com
`register_table(identificador, metadata_location)`.

Verificado localmente com PyIceberg 0.12.0 (extras `sql-sqlite` e `pyiceberg-core`) e a extensão
`iceberg` do DuckDB 1.5.5:

| Verificação | Resultado |
| --- | --- |
| Tabela particionada por `month(data_ref)` e `append` de dois meses | Um arquivo por mês em `data/data_ref_month=2026-01/`; a coluna `data_ref` fica dentro do arquivo, e nenhuma coluna `mes` existe. |
| `overwrite(data, overwrite_filter=...)` de fevereiro | Um commit com dois snapshots (`delete` e `append`); o arquivo antigo de fevereiro saiu do snapshot atual. |
| `append` sem o extra `pyiceberg-core` | `ImportError` na transformação de partição; o extra traz a implementação em Rust. |
| `update_schema()` com `rename_column` e `add_column` | Esquema com `field_id` de 1 a 6; o arquivo antigo mantém o nome `descricao`, e o DuckDB mostra `descr`. |
| Arquivo Parquet gravado pelo PyIceberg | `PARQUET:field_id` em cada coluna; `DECIMAL(18, 2)` como `INT64`; um row group para 100.000 linhas. |
| `add_files` com um arquivo gravado pelo PyArrow | Registrado sem cópia; o DuckDB lê as 5.000 linhas do mês 3. |
| `iceberg_scan` no DuckDB | Lê com os tipos corretos quando recebe o caminho do `metadata.json`. Sem o caminho falha: o PyIceberg não grava `version-hint.text`, e a adivinhação de versão do DuckDB procura `v<N>.metadata.json`, enquanto o PyIceberg nomeia `<N>-<uuid>.metadata.json`. |
| `StaticTable.from_metadata` com filtro de fevereiro | 120.000 linhas, sem catálogo. |
| Metadados de cinco commits | Cinco `metadata.json` e nove Avro, 49.721 bytes. |

O custo do Iceberg sem serviço é o mesmo do DuckLake: o arquivo do catálogo é movido pela biblioteca
(baixar com o ETag, anexar, subir com `IfMatch`), com um escritor por vez, e o DuckDB recebe da
biblioteca o caminho do `metadata.json`. Um catálogo próprio que guarde o ponteiro num objeto do S3
trocado com `IfMatch` dispensaria o SQLite e aceitaria vários escritores, ao preço de implementar a
classe `Catalog`. O que o Iceberg dá em troca: partição oculta por `month(data_ref)`, evolução
completa de esquema, `add_files` e a tabela registrável no Glue mais tarde sem conversão, com o
Redshift passando a lê-la por esquema externo. O caminho com Glue foi o plano anterior (PR 2).

### Hudi e Lance

O `hudi` 0.5.0 (hudi-rs) lê snapshots, viagem no tempo e consultas incrementais; o README não descreve
escrita. O Redshift Spectrum lê só Copy-on-Write e só com o Glue. O Lance (`pylance` 12.0.0) é um
formato colunar próprio, não Parquet, e sai pelo requisito de que os Parquet sejam a fonte da verdade.

### Diretórios Hive com manifesto próprio

O caminho sem formato de tabela grava `tabela/mes=YYYY-MM/<execucao>.parquet`, um manifesto JSON por
tabela com a lista de arquivos, e um ponteiro `current` trocado com `IfMatch`. Ele precisa implementar
a leitura por `field_id` para evoluir o esquema, as estatísticas por arquivo para podar, a retenção e a
remoção de órfãos, e ganha só a ausência de dependência. O `delta-rs` faz isso com a mesma primitiva
do S3.

### Comparação para os requisitos do projeto

| Requisito | Delta Lake (delta-rs) | DuckLake (catálogo em arquivo) | Iceberg (catálogo SQLite em arquivo) | Hive com manifesto próprio |
| --- | --- | --- | --- | --- |
| Serviço externo | Nenhum. | Nenhum; PostgreSQL só para vários escritores. | Nenhum; REST só para escrever pelo DuckDB. | Nenhum. |
| Commit atômico no S3 | Nativo, put-if-absent no log. | Upload do catálogo com `IfMatch`, feito pela biblioteca. | Upload do catálogo, feito pela biblioteca. | Ponteiro com `IfMatch`, feito pela biblioteca. |
| Vários escritores | Sim. | Não. | Não; sim com um catálogo próprio de ponteiro no S3. | Não. |
| Leitura no DuckDB | `delta_scan`, poda por partição e estatísticas, viagem no tempo. | Tabela nativa; catálogo no S3 só de leitura. | `iceberg_scan` com o caminho do `metadata.json` vindo do catálogo; só leitura. | `read_parquet` com lista de arquivos. |
| Escrita a partir do DuckDB | Arrow para `write_deltalake`; `INSERT INTO` só append. | `INSERT`, `DELETE`, `ALTER` nativos. | PyIceberg com Arrow. | `COPY ... TO` mais manifesto. |
| Carga no Redshift | `COPY ... MANIFEST` da lista de `file_uris`. | `COPY ... MANIFEST` de `ducklake_data_file`. | `COPY ... MANIFEST` da lista de `plan_files()`. | `COPY ... MANIFEST`. |
| Registro de arquivos do `UNLOAD` | `create_write_transaction`. | `ducklake_add_data_files`. | `add_files`. | Entrada no manifesto. |
| Substituição idempotente do mês | `overwrite` com `predicate`. | `DELETE` e `INSERT` numa transação. | `overwrite` com filtro. | Trocar a lista do mês. |
| `NOT NULL` e `CHECK` na escrita | Aplicados pelo escritor. | `NOT NULL` aplicado. | Campos `required`. | Só no Arrow. |
| Evolução de esquema | Adicionar coluna; renomear e remover dependem de column mapping, incompleto no delta-rs. | Adicionar, remover, renomear, promover tipo. | Completa. | Manual por `field_id`. |
| Conjunto de tabelas e transação entre tabelas | Nenhum no formato: esquema e commit são de uma tabela; o conjunto vive no `MetaData` do SQLAlchemy e nas versões que a biblioteca registra em cada commit. | Schemas com tabelas no catálogo SQL e transação ACID sobre todos eles; o snapshot é do catálogo inteiro. | Namespaces no catálogo; o commit de várias tabelas existe no catálogo REST, não no PyIceberg com SQLite. | Nenhum; o manifesto próprio teria de cobrir o conjunto. |
| Coluna de partição dentro do arquivo | Não. | Sim. | Sim. | Escolha da biblioteca. |
| Maturidade | Protocolo de 2019; leitores em Spark, Athena, Polars, DataFusion, DuckDB, dlt. | Especificação 1.0 de 2026-04; leitores DuckDB e MotherDuck. | Amplo; registrável no Glue depois por `register_table`. | Só a biblioteca. |

## Caminho para o Redshift com qualquer camada

O Redshift só entra por `COPY ... FORMAT AS PARQUET MANIFEST`, porque esquemas externos exigem `CREATE`
no banco, negado ao projeto (diagnóstico de 2026-09-13 no PR 2). A biblioteca monta o manifesto com a
lista de arquivos da versão publicada: no Delta, `file_uris` e o `size` de `get_add_actions()` para
`content_length`; no DuckLake, `path` e `file_size_bytes` de `ducklake_data_file` com `end_snapshot`
nulo. A publicação incremental é a diferença entre duas versões: as ações `add` novas no Delta, ou
`begin_snapshot` maior que o último snapshot publicado no DuckLake.

Estas regras mantêm os arquivos legíveis pelo `COPY`:

- Nenhuma linha fora dos arquivos: sem vetores de exclusão (não habilitar `delta.enableDeletionVectors`;
  substituir meses inteiros, o que no teste não gerou arquivos de exclusão no DuckLake) e sem data
  inlining (`DATA_INLINING_ROW_LIMIT 0` ou `ducklake_flush_inlined_data` antes de publicar).
- A coluna de partição deriva de uma coluna do arquivo (`mes` de `data_ref`), porque o Delta não a
  grava nos dados e o `COPY` não lê diretórios. No DuckLake, a coluna permanece no arquivo.
- `DECIMAL(18, 2)` sai como `INT64` do delta-rs, do DuckLake e do próprio DuckDB; o PyArrow grava
  `FIXED_LEN_BYTE_ARRAY`. A prova de conceito do `COPY` com `DECIMAL` em `INT64`, pendente em
  [`parquet.md`](parquet.md), cobre os três escritores.
- O `COPY` é posicional e exige o mesmo número de colunas. Uma coluna nova entra no fim do esquema
  nos dois formatos, e os arquivos antigos ficam com uma coluna a menos. `FILLRECORD` consta das opções
  aceitas para Parquet e preencheria as colunas finais ausentes; se não funcionar, a alternativa é
  reescrever os meses antigos ou carregar por geração de esquema com lista de colunas. Pendente da
  prova de conceito.

No sentido inverso, o `UNLOAD ... PARTITION BY (mes)` grava diretórios Hive sem a coluna de partição,
que é a convenção do Delta, e o `MANIFEST VERBOSE` traz `content_length` e `record_count` para a
`AddAction`; com `INCLUDE`, a coluna fica no arquivo, que é a convenção do DuckLake para
`ducklake_add_data_files`.

## Camada de SQL portável

### SQLAlchemy Core

O estado atual. Os dois dialetos existem (`sqlalchemy-redshift` 1.0.0 e `duckdb_engine` 0.17.0, este
sem lançamento desde 2025-03-29) e seus limites estão em [`sqlalchemy.md`](sqlalchemy.md),
[`duckdb.md`](duckdb.md) e [`redshift.md`](redshift.md): sem cache de statements, `executemany` linha
a linha no Redshift, `SERIAL` rejeitado e `DEFERRABLE` descartado pelo DuckDB, `TEXT` como `VARCHAR(256)`,
`RETURNING` emitido pelo ORM. Nenhum deles impede o uso do Core para `SELECT` e `INSERT ... SELECT`, e
o `Table` do modelo continua uma boa fonte do contrato (esquema Arrow, DDL dos dois bancos e, agora,
o esquema Delta ou DuckLake).

A substituição gradual do dialeto em tempo de execução está decidida: a biblioteca gera o texto SQL
de cada dialeto a partir dos statements Core (`param`, `prefixed`, `render`, `write_sql_files`), o
texto entra versionado no repositório do pipeline, e cada chamada que compilava um statement passa a
executar o texto gerado, uma interação com o banco por vez. Ao fim, o SQLAlchemy fica nos modelos e
na geração, e os dois dialetos deixam de ser dependências de execução. O que cada parte do
SQLAlchemy entrega ao projeto, a recomendação sem a compatibilidade com o pipeline e os
comportamentos do compilador que a geração contorna estão em [`sqlalchemy.md`](sqlalchemy.md).

### SQLGlot

O SQLGlot 30.18.0 é analisador, transpilador e construtor de SQL com DuckDB e Redshift entre os
dialetos oficiais, otimizador com `qualify` e `annotate_types` sobre um esquema, e linhagem por
coluna. Transpilações verificadas de DuckDB para Redshift:

| DuckDB | Redshift |
| --- | --- |
| `strftime(data_ref, '%Y-%m')` | `TO_CHAR(data_ref, 'YYYY-MM')` |
| `epoch_ms(ts)` | `(TIMESTAMP 'epoch' + (ts / POWER(10, 3)) * INTERVAL '1 SECOND')` |
| `datediff('day', a, b)` | `DATEDIFF(DAY, a, b)` |
| `x::DECIMAL(18,2)` | `CAST(x AS DECIMAL(18, 2))` |
| `try_cast(x AS BIGINT)` | `TRY_CAST(x AS BIGINT)`, que o Redshift tem. |
| `nome VARCHAR(200)` em `CREATE TABLE` | `VARCHAR(MAX)`, porque o DuckDB ignora o comprimento. |
| `INSERT INTO destino BY NAME ...`, `list_aggregate(l, 'sum')` | Passam inalterados, e o Redshift não os tem. |

O construtor gera o mesmo `SELECT` nos dois dialetos a partir de uma expressão, e
`exp.DataType.build("VARCHAR(200)")` vira `VARCHAR(200)` no Redshift e `TEXT(200)` no DuckDB. O
SQLGlot traduz nomes e sintaxe de funções, mas não sabe o que o destino suporta: a conclusão de
[`schema.md`](schema.md), testes de integração no Redshift, permanece. Usos para a biblioteca:
gerar DDL por dialeto a partir do esquema Arrow, se o SQLAlchemy sair do contrato, e um teste que
analisa o SQL do pipeline e acusa construções só do DuckDB.

### SQLMesh

O SQLMesh 0.236.2 roda o mesmo projeto no DuckDB e no Redshift transpilando com SQLGlot. Ele resolve
o namespace num único esquema: `environment_suffix_target: table` cria `esquema.tabela__dev`, e
`physical_schema_mapping` fixa o esquema das tabelas físicas. Ele exige um banco de estado: a
documentação recomenda PostgreSQL, aceita DuckDB para um usuário e desaconselha o próprio warehouse.
Modelos Python devolvem DataFrames pandas, e o adaptador os carrega em lotes de `DEFAULT_BATCH_SIZE`
igual a 10.000 linhas por `INSERT`; no Redshift o código anota que `VALUES` não é suportado e passa por
tabela temporária; não há caminho por S3 e `COPY`. O SQLMesh assume a orquestração, o estado, os nomes
das tabelas e os ambientes, isto é, o sandbox da biblioteca. Ele cabe se o pipeline for
majoritariamente transformações SQL; a ingestão e a exportação de Parquet continuam sendo o trabalho
desta biblioteca.

### dbt

`dbt-core` 1.12.5 com `dbt-duckdb` 1.11.0 e `dbt-redshift` 1.11.1. O `dbt-duckdb` materializa modelos
como Parquet (`materialized='external'`, com `partition_by` nas opções do `COPY`), lê Parquet no S3
como fonte (`external_location`) e tem plugins experimentais para Delta e Iceberg. O dbt não transpila:
as diferenças entre dialetos ficam em macros com `adapter.dispatch`. Modelos Python só existem em
Snowflake, BigQuery e Databricks.

### Ibis, Polars e DataFusion

O Ibis 12.0.0 não tem backend Redshift, então não serve para rodar o pipeline nos dois motores. Polars
1.44.2 e DataFusion 54.0.0 (Python) executam localmente sobre Parquet e Delta, com leitura e gravação
Hive e streaming, e valem como motores de cálculo em Python, não como camada portável de SQL.

### dlt

A `dlt` 1.30.0 carrega fontes externas com evolução de esquema e contratos, e tem destinos Redshift
(`INSERT` por padrão, ou staging no S3 com `COPY` de Parquet ou JSONL), DuckDB, DuckLake e sistema de
arquivos com Delta e Iceberg. Ela resolve ingestão de fontes, não o ciclo de ida e volta pelo sandbox,
e serve de referência de implementação para o destino Redshift e para o DuckLake.

## Contrato de esquema, auditoria e migração

O contrato pode continuar nos modelos SQLAlchemy, dos quais derivam o esquema Arrow
([`sqlalchemy.md`](sqlalchemy.md)), o esquema Delta (o `write_deltalake` recebe o esquema Arrow) ou o
DDL do DuckLake, e o DDL do sandbox nos dois bancos. Se o SQLAlchemy sair, o esquema Arrow assume o
papel de fonte, e o SQLGlot gera o DDL por dialeto. Nas duas formas, as opções físicas (partição, chave
de ordenação, `DISTKEY`) ficam em metadados da biblioteca, como [`schema.md`](schema.md) já propõe.

A aplicação do contrato acontece em três pontos: o cast seguro para Arrow antes de gravar, o `NOT NULL`
e o `CHECK` do formato de tabela na gravação, e a auditoria por consulta no motor antes de publicar.
Ferramentas que cobrem a auditoria sem código próprio: Pandera 0.33.1 valida DataFrames pandas,
polars, PyArrow e Ibis com `DataFrameModel`; Soda Core roda checks SodaCL em Redshift e DuckDB;
`datacontract-cli` 1.2.0 exporta um contrato (Open Data Contract Standard) para DDL de Redshift e
DuckDB, dbt e SodaCL e testa contra DuckDB sobre arquivos no S3 e contra Redshift.

A migração de esquema muda de natureza. A tabela Delta não tem DDL em SQL: `DeltaTable.create` recebe
o esquema derivado do contrato, com nulidade, comentários de coluna em `metadata`, colunas de
partição, nome, descrição e propriedades, e `mode="ignore"` torna a criação idempotente. As tabelas do
sandbox são recriadas a cada execução a partir do contrato. A evolução das tabelas permanentes é uma
reconciliação entre o contrato e o esquema atual da tabela: uma diferença aditiva (coluna nova
anulável) é aplicada por `alter.add_columns` ou `schema_mode="merge"`, as linhas existentes leem nulo
nela, e um valor para elas é um `update` com predicado; uma diferença destrutiva (renomear, remover,
mudar tipo) exige reescrever a tabela com `schema_mode="overwrite"`, e a biblioteca recusa aplicá-la
sem essa ordem explícita. Duas regras ficam na biblioteca porque o delta-rs não as impõe: coluna nova
`NOT NULL` é recusada em tabela com dados, já que o `add_columns` a aceita e deixa a tabela
inconsistente; e a verificação de tipos acontece no cast seguro para o esquema Arrow do contrato,
antes da gravação, porque o `append` converte os dados para o tipo da tabela em vez de acusar a
diferença. As tabelas
publicadas no Redshift seguem o mesmo diff: `ALTER TABLE ADD COLUMN` no caso aditivo, recriação e
recarga no destrutivo. A viagem no tempo não é migração: ela lê uma versão antiga com o esquema
daquela versão, e `restore` volta dados e esquema a uma versão anterior como um commit novo. O alcance
é limitado por `delta.logRetentionDuration` e `delta.deletedFileRetentionDuration`, e `vacuum` recusa
uma retenção abaixo da configurada (168 horas por padrão) sem `enforce_retention_duration=False`. O
que o Alembic dava, o histórico revisável e o diff entre modelo e banco, vem do arquivo de esquema
gerado e versionado no repositório ([`schema.md`](schema.md)) e do histórico do Delta. O Alembic sai.
O Atlas cobre o Redshift só no plano pago e não cobre o DuckDB.

## Por que o Alembic sai

O Alembic resolvia um problema que o desenho com Delta não tem: aplicar mudanças de esquema a um
banco que é a única cópia dos dados, com histórico dos scripts e caminho de volta. Quatro razões o
retiram da biblioteca:

1. A tabela do lago não tem DDL em SQL a migrar. O esquema é criado do modelo por
   `DeltaTable.create`, e cada mudança é um commit de `metaData` no log, versionado junto com os
   dados; uma versão antiga é lida com o esquema daquela versão.
2. As outras cópias são derivadas. As tabelas do sandbox são recriadas a cada execução, e as
   publicadas no Redshift são recarregadas do Delta; um erro de esquema se corrige recarregando ou
   com `restore`, não com um script de downgrade.
3. As regras de evolução precisam viver na biblioteca de qualquer modo, porque o delta-rs não as
   impõe: `add_columns` aceita uma coluna `NOT NULL` numa tabela com dados e a deixa nula em todas as
   linhas, e o `append` converte os dados para o tipo da tabela em vez de acusar a diferença. A
   reconciliação que aplica o diff aditivo e recusa o destrutivo é um único mecanismo; o Alembic
   seria um segundo para o mesmo diff.
4. O suporte do Alembic aos dois dialetos já era fraco: o DuckDB exige um `DefaultImpl` próprio, e o
   Redshift executa `executemany` linha a linha.

O que se perde é a migração de dados escrita em SQL, como um backfill; no pipeline isso é uma
reexecução dos meses afetados ou um `update` com predicado no Delta. A correspondência recurso a
recurso está em [`delta.md`](delta.md).

## Rust e PyO3

A cadeia de ferramentas existe e é simples de adotar: `uv init --build-backend maturin` gera
`Cargo.toml`, `src/lib.rs`, o pacote Python e o stub `_core.pyi`, porque o `uv_build` só constrói
Python puro. O `pyo3` está na 0.29.2 e o `pyo3-arrow` 0.19.0 troca Arrow sem cópia com PyArrow, Polars
e DuckDB pelo PyCapsule Interface, sem depender do PyArrow em tempo de execução. As crates do domínio:
`arrow` e `parquet` 60.0.0, `object_store` 0.14.2 (`PutMode::Create` e `PutMode::Update` com ETag no
S3), `delta_kernel` 0.28.0, `deltalake` 0.32.4, `iceberg` 0.10.1, `hudi` 0.5.0, `datafusion` 55.1.0,
`sqlparser` 0.63.0, `polars` 0.55.2.

Nenhum componente da biblioteca fica sem implementação chamável do Python: o commit no S3 (delta-rs,
ou `boto3` com `IfMatch`), a leitura e gravação de Parquet com estatísticas (PyArrow, DuckDB), a
transpilação (SQLGlot) e o cálculo vetorizado (DuckDB, Polars). O Rust já entra pelas wheels do
`deltalake`, da extensão `delta` do DuckDB e do Polars. Uma extensão própria compensa quando um perfil
mostrar um laço Python quente que nem SQL nem Polars expressam, por exemplo regras de projeção
contrato a contrato, ou quando for preciso um log store fora do que o delta-rs oferece. O custo é
construir wheels para Linux amd64 (SageMaker) a partir do macOS, com `maturin` e compilação cruzada
ou CI, e acompanhar as mudanças de API do `pyo3`.

## Escolha das camadas

A comparação leva ao Delta Lake por `deltalake` como camada de tabela e ao SQLAlchemy nos modelos e
no Core, com o texto SQL gerado por dialeto substituindo a compilação em tempo de execução; a
decisão, com as consequências de cada premissa e as partes da biblioteca, está em
[`PLAN.md`](PLAN.md). As razões:

- **Delta Lake.** É o único formato sem serviço que atende com código pronto: o commit atômico é o
  put-if-absent do S3, nativo no log; vários escritores; `delta_scan` no DuckDB com poda por
  partição e por estatísticas e viagem no tempo; `overwrite` com predicado para a substituição
  idempotente do mês; `NOT NULL` e `CHECK` aplicados pelo escritor; `create_write_transaction` para
  registrar os arquivos do `UNLOAD`; e o escritor Python mais completo sem JVM (`merge`, `update`,
  `delete`, `optimize`, `restore`, `vacuum`, constraints). O que falta, renomear e remover colunas
  sem reescrever, pesa pouco num pipeline que raramente o faz.
- **DuckLake**, não adotado: suas vantagens, renomear e remover colunas sem reescrever dados e a
  tabela nativa no DuckDB, não pesam aqui, e seu preço permanece: o catálogo em arquivo movido pela
  biblioteca, com um escritor por vez, o inlining desligado antes de publicar e um ecossistema de
  leitores menor.
- **Iceberg com catálogo SQLite**, documentado como a alternativa que troca a ausência de código
  próprio do Delta pela sincronização do arquivo do catálogo. Volta à mesa se o Glue ou o S3 Tables
  forem habilitados: a tabela seria registrada por `register_table` sem conversão, e o Redshift
  passaria a lê-la por esquema externo. A diferença de fundo entre os dois formatos, o ponteiro da
  versão atual implícito no log do Delta e trocado no catálogo do Iceberg, está em
  [`delta.md`](delta.md).
- **Diretórios Hive com manifesto próprio**, não adotado: reimplementa o que o delta-rs faz com a
  mesma primitiva do S3.
- **SQLAlchemy Core** permanece porque o pipeline já o usa nos modelos e em `select` e `insert`, e
  o `Table` do modelo é a fonte do esquema Arrow, do esquema Delta e do DDL dos dois motores. O que
  muda é o caminho dos DataFrames, que passa por Arrow, e a compilação em tempo de execução, que o
  texto gerado por dialeto substitui uma interação por vez ([`sqlalchemy.md`](sqlalchemy.md)). O
  `insert(...)` executado com listas de linhas sai, porque no Redshift ele vira uma ida ao servidor
  por linha.
- **SQLGlot puro** no lugar do SQLAlchemy exigiria reescrever as consultas sem ganho de
  portabilidade, porque os dois exigem os mesmos testes no Redshift; o SQLGlot fica opcional, como
  teste de compatibilidade.
- **SQLMesh e dbt** saem, porque o pipeline é majoritariamente lógica Python e não transformações
  SQL; o SQLMesh ainda assumiria a orquestração, o estado e os nomes das tabelas, que são o sandbox
  da biblioteca.
- **Alembic** sai pelas razões da seção "Por que o Alembic sai": a reconciliação entre o modelo e
  o log é o único mecanismo de evolução.
- **Rust com PyO3** só quando um perfil mostrar um laço Python quente que nem SQL nem Polars
  expressam (seção "Rust e PyO3").

### Maturidade do Delta e o gatilho de reavaliação

O Delta não é uma escolha defasada, e a avaliação de 2026-09-19 registra por quê. O delta-spark
4.4.0 é de 2026-08-20, o deltalake 1.6.4 de 2026-09-18, e o protocolo continua ganhando recursos
(`catalogManaged`, `clustering`, `rowTracking`, `variantType`). O Iceberg virou o padrão neutro de
intercâmbio, adotado por S3 Tables, Athena, Redshift, Glue, Snowflake e BigQuery, e os dois formatos
convergem em vez de se substituir: a Databricks, ao comprar a Tabular em junho de 2024, declarou a
meta de um padrão único em vários anos, e a versão 3 da especificação do Iceberg incorporou vetores
de exclusão, variant, linhagem de linha e valores padrão, enquanto a versão 4, em rascunho, traz
caminhos relativos nos metadados, tudo já existente no Delta. Para um projeto novo com catálogo e
vários fornecedores, o Iceberg seria a escolha. Aqui a conta inverte: o Iceberg sem catálogo está
fora da especificação, que exige a troca atômica do ponteiro no catálogo, e o escritor Python sem
JVM é mais completo no delta-rs (`merge`, `update`, `delete`, `optimize`, `restore`, `vacuum`,
constraints) do que no PyIceberg (append, overwrite com filtro, delete, upsert). O risco a observar é
o recurso `catalogManaged`, que move o commit para um catálogo, o modelo do Iceberg; o commit por
arquivo continua no protocolo e é o que o delta-rs usa.

A decisão é reversível porque os dados são Parquet comum com `data_ref` dentro dos arquivos: um
Iceberg particionado por `month(data_ref)` os registra com `add_files` sem reescrever, e o Apache
XTable converte os metadados Delta em Iceberg sem tocar nos dados, num jar Java sem Spark. O UniForm
não serve: exige Spark e column mapping, que troca os nomes físicos das colunas e quebra o `COPY` do
Redshift, e é somente leitura para clientes Iceberg. As regras que mantêm a saída aberta já estão nas
etapas: sem vetores de exclusão, sem column mapping, um único escritor, caminhos relativos. O gatilho
de reavaliação é a disponibilidade do Glue, do S3 Tables ou de um catálogo acessível ao Redshift;
nesse dia a tabela de "Comparação para os requisitos do projeto" é refeita com o Iceberg registrável,
e a saída do Delta para pastas Parquet está em [`delta.md`](delta.md).

## Referências

Delta Lake:

- <https://delta-io.github.io/delta-rs/usage/writing/>
- <https://delta-io.github.io/delta-rs/usage/writing/writing-to-s3-with-locking-provider/>
- <https://delta-io.github.io/delta-rs/integrations/object-storage/s3/>
- <https://delta-io.github.io/delta-rs/api/delta_table/>
- <https://delta-io.github.io/delta-rs/api/delta_table/delta_table_alterer/>
- <https://github.com/delta-io/delta-rs/releases>
- <https://github.com/delta-io/delta-rs/discussions/4482>
- <https://github.com/delta-io/delta-rs/issues/4464>
- <https://github.com/delta-io/delta-rs/pull/4732>
- <https://github.com/delta-io/delta-rs/issues/3936>
- <https://github.com/delta-io/delta/blob/master/PROTOCOL.md>
- <https://duckdb.org/docs/current/core_extensions/delta.html>
- <https://docs.pola.rs/api/python/stable/reference/api/polars.DataFrame.write_delta.html>

DuckLake:

- <https://ducklake.select/docs/stable/duckdb/introduction>
- <https://ducklake.select/docs/stable/duckdb/usage/choosing_a_catalog_database>
- <https://ducklake.select/docs/stable/duckdb/usage/schema_evolution>
- <https://ducklake.select/docs/stable/duckdb/metadata/adding_files>
- <https://ducklake.select/docs/stable/duckdb/advanced_features/partitioning>
- <https://ducklake.select/docs/stable/duckdb/advanced_features/conflict_resolution>
- <https://ducklake.select/docs/stable/duckdb/advanced_features/data_inlining>
- <https://ducklake.select/docs/stable/specification/tables/ducklake_data_file>
- <https://ducklake.select/2026/04/13/ducklake-10/>
- <https://github.com/duckdb/ducklake/discussions/519>
- <https://dlthub.com/docs/dlt-ecosystem/destinations/ducklake>

Iceberg, Hudi e S3:

- <https://py.iceberg.apache.org/configuration/>
- <https://py.iceberg.apache.org/api/>
- <https://duckdb.org/docs/current/core_extensions/iceberg/overview.html>
- <https://dlthub.com/docs/dlt-ecosystem/destinations/iceberg>
- <https://github.com/apache/hudi-rs/blob/main/README.md>
- <https://docs.aws.amazon.com/redshift/latest/dg/c-spectrum-external-tables.html>
- <https://aws.amazon.com/about-aws/whats-new/2024/08/amazon-s3-conditional-writes>
- <https://aws.amazon.com/about-aws/whats-new/2024/11/amazon-s3-functionality-conditional-writes>
- <https://docs.aws.amazon.com/boto3/latest/reference/services/s3/client/put_object.html>

SQL e contrato:

- <https://github.com/tobymao/sqlglot/blob/main/README.md>
- <https://sqlmesh.readthedocs.io/en/stable/guides/configuration/>
- <https://sqlmesh.readthedocs.io/en/stable/reference/configuration/>
- <https://sqlmesh.readthedocs.io/en/stable/integrations/engines/redshift/>
- <https://sqlmesh.readthedocs.io/en/stable/concepts/models/python_models/>
- <https://github.com/TobikoData/sqlmesh/blob/main/sqlmesh/core/engine_adapter/base.py>
- <https://github.com/TobikoData/sqlmesh/blob/main/sqlmesh/core/engine_adapter/redshift.py>
- <https://github.com/duckdb/dbt-duckdb/blob/master/README.md>
- <https://docs.getdbt.com/docs/build/python-models>
- <https://ibis-project.org/support_matrix>
- <https://dlthub.com/docs/dlt-ecosystem/destinations/redshift>
- <https://pandera.readthedocs.io/en/stable/>
- <https://github.com/datacontract/datacontract-cli/blob/main/README.md>
- <https://docs.soda.io/reference/data-source-reference-for-soda-core>
- <https://atlasgo.io/guides/redshift>
- <https://docs.aws.amazon.com/redshift/latest/dg/r_TRY_CAST.html>

Rust e PyO3:

- <https://docs.astral.sh/uv/concepts/build-backend/>
- <https://docs.astral.sh/uv/concepts/projects/init/>
- <https://docs.rs/pyo3-arrow/latest/pyo3_arrow/>
- <https://docs.rs/object_store/latest/object_store/enum.PutMode.html>
- <https://docs.rs/object_store/latest/object_store/aws/enum.S3ConditionalPut.html>
- <https://datafusion.apache.org/python/>
