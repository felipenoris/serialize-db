# Delta Lake

O Delta Lake é a fonte da verdade do banco: cada tabela é uma pasta de arquivos Parquet mais um log
de transações, no S3 do projeto ou em disco local. A biblioteca grava e lê pelo pacote `deltalake`
(delta-rs, Rust com bindings Python), o DuckDB lê e anexa pela extensão `delta`, e o Redshift entra
por `COPY` e `UNLOAD` sobre os arquivos que o log lista. As razões da escolha estão em
[`estrategia.md`](estrategia.md). As verificações deste documento rodaram em 2026-09-19 com Python
3.13, deltalake 1.6.4, DuckDB 1.5.5, PyArrow 25.0.1, num macOS arm64 com 11 threads e disco local;
nada rodou contra o S3 nem contra o Redshift.

## Comandos utilitários para diagnóstico

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

## Organização dos dados

### Estrutura da pasta de uma tabela

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

### Log, snapshot e checkpoint

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

Nos documentos, "snapshot da tabela" é esse estado de uma versão, e "snapshot do banco" é o conjunto
`{tabela: versão}` que a biblioteca registra fora do log. O que a biblioteca guarda por conta
própria, os metadados de cada commit, `_serialize_db/snapshots.json` e a tabela
`serialize_db_publications`, está em [`serialize-db.md`](serialize-db.md).

### Diferenças para o PostgreSQL

- Não há atualização no lugar. Toda escrita cria arquivos novos e um commit; `UPDATE`, `DELETE` e
  `MERGE` reescrevem os arquivos que contêm as linhas afetadas (no teste, um `UPDATE` de uma linha
  reescreveu um arquivo de dez linhas: `num_added_files: 1, num_removed_files: 1, num_copied_rows: 9`).
- Não há índices, chaves primárias, únicas nem estrangeiras. O que existe é `NOT NULL` e `CHECK`,
  aplicados pelo escritor, e as estatísticas por arquivo, que fazem o papel do índice na leitura.
- Não há banco nem schema como conjunto de tabelas. O esquema do Delta é a lista de colunas de uma
  tabela, na ação `metaData` do log dela, e o formato não sabe quais tabelas formam o banco. A pasta
  com uma subpasta por tabela agrupa os arquivos, e o `MetaData` do SQLAlchemy é o único lugar que
  declara o conjunto e as relações entre as tabelas, por isso é o contrato. Nos documentos, "esquema
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

### Efeitos nas formas de manipular os dados

O desenho de [`guia.md`](guia.md), meses imutáveis substituídos por inteiro, é o uso natural do
formato: `write_deltalake(mode="overwrite", predicate="mes = '2026-08'")` troca os arquivos do mês
num commit. Operações linha a linha existem (`update`, `delete`, `merge`) e servem para correções
pontuais, mas cada uma reescreve arquivos inteiros e cria uma versão; um pipeline que as usasse por
linha produziria milhares de commits e de arquivos pequenos. Os dados entram e saem em Arrow, em
lotes; o DuckDB lê a tabela no lugar e serve de motor de consulta e de cálculo.

## Delta Lake e Iceberg

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
mova, como mostra [`estrategia.md`](estrategia.md). O catálogo Hadoop, baseado só em arquivos, depende
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

## Implementações do protocolo

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

Sem Spark o pipeline não perde recurso nem velocidade. No desenho deste documento o delta-rs só lê e
grava o log e registra arquivos; o cálculo é do DuckDB ou do Redshift, e a seção de performance mede
a leitura por `delta_scan` a milissegundos da leitura direta do Parquet. O Spark ganharia só ao
distribuir o cálculo num cluster, e o caso dos dados maiores que a máquina é atendido ingerindo só as
partições necessárias ou executando no Redshift. O risco fica no caminho de escrita: um escritor Spark
ou Databricks que habilite vetores de exclusão nas mesmas tabelas invalida o `COPY` do Redshift e
expõe o delta-rs aos bugs abertos de leitura desse recurso, os issues 4613 e 4657. Por isso a
biblioteca é o único escritor, regra registrada em [`estrategia.md`](estrategia.md).

## Requisitos do S3

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
[`parquet.md`](parquet.md)), e o bucket fica na região do namespace do Redshift.

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
`COPY ... MANIFEST` pelo papel do Redshift e `UNLOAD` no prefixo da tabela aguardam uma conexão
Redshift no projeto.

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
partir de `no_proxy` ao iniciar, quando a maiúscula está ausente ou vazia, e mantém como reserva as
credenciais temporárias que o `boto3` resolve, passadas em `storage_options` (`AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, `AWS_REGION`) e renovadas a cada abertura da tabela,
porque expiram. O DuckDB com `PROVIDER credential_chain` e o `boto3` nunca falharam.

## Tipos suportados

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

A tabela completa, com as colunas do SQLAlchemy e do Redshift, está em [`schema.md`](schema.md).

### DECIMAL com escala fixa

`decimal(p, s)` aceita precisão até 38. O delta-rs grava `decimal(18, 2)` no tipo físico `INT64`,
como o DuckDB; o PyArrow grava `FIXED_LEN_BYTE_ARRAY`. Um `append` com `decimal(20, 4)` numa coluna
`decimal(18, 2)` é recusado (`SchemaMismatchError: Cannot cast`). O DuckDB lê `DECIMAL(18,2)` e soma
em `DECIMAL(38,2)`.

### JSON e VARIANT

Não há tipo JSON. Um documento entra como `string`; quando o esquema Arrow traz a extensão
`arrow.json` (`pa.json_(pa.string())`), o campo Delta continua `string` e guarda
`ARROW:extension:name = arrow.json` nos metadados, e `schema().to_arrow()` devolve `string` simples.
O delta-rs grava o arquivo com o tipo lógico `String`; um arquivo do PyArrow ou do DuckDB com o tipo
lógico `JSON`, registrado por `create_write_transaction`, é lido pelos dois leitores na mesma tabela.
O `delta_scan` mostra `VARCHAR`, e `->>`, `json_extract` e `json_valid` funcionam sobre ele. Nem o
Arrow nem o Delta validam o texto: a validação é do `::JSON` do DuckDB na materialização e do
`JSON_PARSE` do Redshift na carga, e a auditoria roda `json_valid` antes de publicar. O tratamento
por camada está em [`schema.md`](schema.md).

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

### Datas e timestamps

`timestamp` é um instante ajustado a UTC; `timestamp_ntz` é um relógio de parede sem fuso. Um Arrow
`timestamp[us]` sem fuso vira `timestamp_ntz`, e a primeira coluna desse tipo eleva o protocolo da
tabela a leitor 3 e escritor 7 com o recurso `timestampNtz`, que o DuckDB lê. Um `timestamp[ns]` do
pandas é aceito e gravado em microssegundos sem aviso; um fuso `America/Sao_Paulo` é aceito e
gravado como o mesmo instante em UTC. O contrato mantém o cast explícito para microssegundos e UTC
antes de gravar.

## DDL

Não há DDL em SQL. A tabela é criada por `DeltaTable.create`, que grava o commit `CREATE TABLE` com o
esquema, as colunas de partição, o nome, a descrição e as propriedades; `mode="ignore"` repete a
chamada sem criar versão nova, o que torna a criação idempotente. A biblioteca deriva tudo do modelo
SQLAlchemy.

### Criação da tabela a partir do modelo SQLAlchemy

O `Table` do modelo já produz o esquema Arrow ([`sqlalchemy.md`](sqlalchemy.md)), e
`DeltaTable.create` aceita um esquema Arrow diretamente; os metadados de campo do Arrow chegam ao
esquema Delta (um `comment` gravado no Arrow voltou em `DeltaTable.schema()`). As opções físicas
ficam em `Table.info["serialize_db"]`, como [`schema.md`](schema.md) propõe:

```python
import pyarrow as pa
import sqlalchemy as sa
from deltalake import DeltaTable
from serialize_db.schema import arrow_schema   # plan/sqlalchemy.md: tipos, nulidade e PARQUET:field_id

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

### Propriedades e protocolo

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

## Evolução de esquema

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
`rename_column` nem `drop_columns`.

A reconciliação é o comando da biblioteca que substitui a migração: compara `arrow_schema(Table)`
com `dt.schema()`, aplica o diff aditivo, recusa o destrutivo com a instrução de reescrita, e repete
o mesmo diff nas tabelas publicadas no Redshift (`ALTER TABLE ADD COLUMN`, que acrescenta no fim, ou
recriação e recarga). A ordem das colunas no Redshift segue a ordem do esquema Delta, porque o `COPY`
é posicional; a carga de arquivos anteriores a uma coluna nova vai por lista de colunas, confirmada
no ambiente alvo em 2026-09-21, ou por `FILLRECORD`, que carregou o mesmo arquivo com a coluna nova nula
([redshift.md](redshift.md)).

## O que substitui o Alembic

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
dois dialetos já era fraco: o DuckDB exige o `DefaultImpl` de [`duckdb.md`](duckdb.md), e o Redshift
executa `executemany` linha a linha.

## Transações, commits, conflitos e restauração

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

## SELECT, INSERT, UPDATE e DELETE

O delta-rs opera com tabelas Arrow (PyArrow, pandas, Polars pelo PyCapsule) e com predicados em SQL.

```python
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

options = {"AWS_REGION": "sa-east-1"}          # credenciais: ambiente, IMDS, contêiner ou explícitas
uri = "s3://bucket/prod/cad_operacoes"

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

## Ingestão de dados

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
   leitores obedecem à ação ([POC.md](POC.md); as conferências que a biblioteca faz antes do commit
   estão em [PLAN-STAGE-3.md](PLAN-STAGE-3.md)). `estatisticas` segue o JSON da ação `add`
   (`numRecords`, `minValues`, `maxValues`, `nullCount`, sem as colunas de partição); o DuckDB os
   fornece por `COPY ... (RETURN_STATS)`, e um arquivo alheio os fornece pelo rodapé Parquet
   (`pq.read_metadata`). Com as estatísticas, a poda funcionou nos dois leitores: `file_uris` com
   `id_operacao >= 990000` devolveu lista vazia, e o DuckDB mostrou `Scanning Files: 0/12`.
3. `convert_to_deltalake(uri, partition_by=..., partition_strategy="hive")` cria o log sobre uma pasta
   Parquet existente, sem reescrever. Serve para a carga inicial só se os arquivos já têm os tipos, a
   ordem de colunas e o layout Hive do contrato; não foi testado.
4. `INSERT INTO` numa tabela anexada no DuckDB, descrito adiante.

A carga inicial dos Parquet atuais é o caminho 1, tabela a tabela e mês a mês, com cast para o
contrato (os modelos usam `Double` onde o contrato pede `Numeric(18, 2)`).

## Exportação para Parquet

Os arquivos de dados já são Parquet. Exportar é listar os arquivos do snapshot que interessa:
`dt.file_uris(file_pruning_predicate="mes IN ('2026-07', '2026-08')")` devolve as URIs, e
`get_add_actions(flatten=True)` acrescenta `size_bytes` e `num_records`. Essa lista alimenta o
manifesto do `COPY` do Redshift e qualquer leitor Parquet. Os arquivos do delta-rs têm as colunas não
anuláveis como `required`, estatísticas em todas as colunas, Snappy e um row group por arquivo até o
tamanho alvo do escritor (120.000 linhas ficaram num row group).

Uma exportação para outro layout (um arquivo por mês com `FIELD_IDS` e `KV_METADATA`, por exemplo) é
um `COPY (SELECT ... FROM delta_scan(uri) WHERE ...) TO ...` do DuckDB, com as opções de
[`parquet.md`](parquet.md).

## Exportação do snapshot para pastas Parquet por mês

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
partição. Copiar exatamente esses arquivos, sem o log, produz a pasta por mês; no S3 é um
`CopyObject` por arquivo, sem baixar. O `path` da ação `add` é uma URI, decodificada com `unquote`
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

# Um COPY por mês limita a memória, é reexecutável e aceita as opções de parquet.md.
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

## Pipeline com o Delta como fonte da verdade

1. **Início da execução.** A biblioteca abre cada tabela de entrada e registra
   `versions[table] = dt.version()`. Toda leitura da execução usa essas versões, o que dá uma visão
   consistente entre tabelas mesmo que outra execução publique no meio.
2. **Ingestão seletiva no motor.** No DuckDB, `ATTACH uri AS t (TYPE delta, VERSION v)` ou uma view
   sobre `delta_scan(uri, version := v)`, com o nome que o modelo espera; os filtros de mês do
   pipeline chegam ao scan e só os arquivos necessários são lidos. As tabelas consultadas muitas
   vezes são materializadas com `CREATE TABLE t AS SELECT * FROM delta_scan(...) WHERE mes IN (...)`.
   No Redshift, `COPY ... MANIFEST` dos arquivos dessas versões e desses meses numa tabela do sandbox
   com prefixo da execução.
3. **Execução.** O pipeline roda no sandbox, com statements Core do SQLAlchemy e lógica Python;
   resultados intermediários ficam no sandbox, não no Delta.
4. **Auditoria.** Contagens, nulos, unicidade de chaves, `mes` igual a `strftime(data_ref, '%Y-%m')`,
   limites de tipo, no motor.
5. **Publicação.** Por tabela e por mês: do DuckDB, `write_deltalake(mode="overwrite",
   predicate="mes = ...")` com o `RecordBatchReader` da consulta, ou `COPY ... TO` na subpasta do
   mês mais `create_write_transaction`; do Redshift, `UNLOAD ... PARTITION BY (mes)` na pasta da
   tabela mais `create_write_transaction`. Cada commit leva `serialize_db_execution_id` e
   `serialize_db_input_versions` em `custom_metadata`. Uma reexecução repete os mesmos `overwrite` e
   é idempotente; um conflito de commit no mesmo mês significa outra execução publicando a mesma
   tabela, e a execução aborta.
6. **Publicação no Redshift para clientes.** A diferença entre a versão publicada e a atual (ações
   `add` novas) diz quais meses recarregar: `DELETE` do mês e `COPY ... MANIFEST` dos arquivos novos,
   para todas as tabelas da execução numa única transação, o que dá aos clientes a atomicidade entre
   tabelas que o Delta não tem; a versão publicada de cada tabela fica numa tabela de controle.
7. **Manutenção.** `optimize.compact` nos meses com muitos arquivos pequenos e `vacuum` com
   `keep_versions` dos snapshots do banco, como descrito em "Manutenção e retenção".

## Manipulação a partir do DuckDB

```sql
INSTALL delta; LOAD delta;
CREATE SECRET (TYPE s3, PROVIDER credential_chain);                      -- credenciais da sessão

SELECT mes, count(*) FROM delta_scan('s3://bucket/prod/cad_operacoes') WHERE mes >= '2026-07' GROUP BY 1;
SELECT count(*) FROM delta_scan('s3://bucket/prod/cad_operacoes', version := 12);

ATTACH 's3://bucket/prod/cad_operacoes' AS cad_operacoes (TYPE delta, PIN_SNAPSHOT true);
SELECT * FROM cad_operacoes AT (VERSION => 12) LIMIT 5;
CREATE TABLE trabalho AS SELECT * FROM cad_operacoes WHERE mes IN ('2026-07', '2026-08');

INSERT INTO cad_operacoes SELECT ...;                                    -- append cego
```

Comportamentos verificados:

- `delta_scan` empurra filtros de partição e de estatísticas (`Scanning Files: 0/12` para um filtro
  fora do intervalo de todas as colunas) e projeção; lê vetores de exclusão, tipos primitivos, structs
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
  continua garantida pelo esquema Delta e pela auditoria.

## Manipulação a partir do Redshift

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
DELETE FROM prod_cad_operacoes WHERE mes = '2026-08';
CREATE TEMPORARY TABLE staging_cad_operacoes (                             -- sem a coluna mes
    id_operacao BIGINT NOT NULL, data_ref DATE NOT NULL, id_cliente BIGINT NOT NULL,
    valor NUMERIC(18, 2) NOT NULL, descricao VARCHAR(200));
COPY staging_cad_operacoes FROM 's3://bucket/publicacao/exec-42/cad_operacoes/2026-08.manifest'
    IAM_ROLE 'arn:aws:iam::123456789012:role/papel' FORMAT AS PARQUET MANIFEST;
INSERT INTO prod_cad_operacoes SELECT *, '2026-08' FROM staging_cad_operacoes;
COMMIT;
```

A tabela de staging existe porque a coluna de partição não está nos arquivos e o `COPY` só lê o
conteúdo deles; a lista de colunas no `COPY` de Parquet funciona (2026-09-21), mas não fornece o
valor da coluna ausente, então a staging fica. A
publicação incremental compara as ações `add` da versão publicada com as da atual e recarrega só os
meses que mudaram, de todas as tabelas da execução numa única transação; a versão publicada de cada
tabela fica numa tabela de controle
(`serialize_db_publications(table_name, delta_version, execution_id, published_at)`).

Escrita de volta, quando o sandbox é o Redshift:

```sql
UNLOAD ('SELECT id_operacao, data_ref, id_cliente, valor, descricao, mes
         FROM exec_42_cad_operacoes WHERE mes = ''2026-08''')
TO 's3://bucket/prod/cad_operacoes/'
IAM_ROLE 'arn:aws:iam::123456789012:role/papel'
FORMAT AS PARQUET PARTITION BY (mes) MANIFEST VERBOSE;
```

`PARTITION BY` grava em `mes=2026-08/` e retira a coluna do arquivo, que é a convenção do Delta. O
manifesto verboso traz `content_length` e `record_count` por arquivo; `minValues` e `maxValues` vêm
do rodapé Parquet de cada arquivo, lido com `pq.read_metadata`. Um `create_write_transaction` com
`mode="overwrite"` e `partition_filters` do mês substitui os arquivos anteriores. Quatro regras
mantêm esse caminho carregável nos dois sentidos: nenhuma linha fora dos arquivos (sem vetores de
exclusão), coluna de partição derivável de uma coluna do arquivo, `DECIMAL` em `INT64` aceito pelo
`COPY` (pendente) e colunas na ordem do esquema Delta.

## Recomendações de performance

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
  arquivos pequenos de um mês; `optimize.z_order(["id_cliente"])` reordena dentro do mês quando os
  filtros forem por outra coluna. Os dois criam versões e arquivos novos.
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
A ingestão no motor DuckDB fica então em dois modos: `CREATE TABLE AS` com filtro de meses para toda
tabela que o pipeline consulta mais de uma vez, view por `delta_scan` para leitura única.

## Manutenção e retenção

### O que cresce e o que limpa

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

### Snapshots do banco

O pipeline mensal cria uma versão por tabela a cada execução. Um snapshot do banco é o conjunto das
versões de cada tabela num instante escolhido, registrado pela biblioteca porque o Delta não tem
snapshot de banco, só de tabela; a periodicidade é de quem o marca, por exemplo o fim de cada
trimestre. A política é guardar essas versões por prazo longo e descartar as intermediárias entre um
snapshot e outro. O Delta não tem tags nem branches (o Iceberg tem, com retenção própria); o
snapshot do banco é um número de versão por tabela, e os mecanismos são estes:

1. **Marcar o snapshot.** A execução marcada grava `custom_metadata={"serialize_db_snapshot": "2026T1"}`
   em cada commit, e a biblioteca registra `{snapshot: {tabela: versão}}` em
   `_serialize_db/snapshots.json`, descrito em [`serialize-db.md`](serialize-db.md). O histórico
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

## Realocação e cópia do banco

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
aws s3 sync s3://bucket/projeto/delta/prod/ s3://bucket/copias/2026-09-19/prod/
```

```python
# A cópia abre onde estiver, com a mesma versão; nenhum caminho precisa ser reescrito.
DeltaTable("s3://bucket/copias/2026-09-19/prod/cad_operacoes").version()
```

O Iceberg é o contraste: `metadata.json` guarda o `location` da tabela, e os manifests guardam o
`file_path` absoluto de cada arquivo (`file:/...` no teste com PyIceberg). Mover a pasta exige
reescrever os metadados ou um leitor tolerante, como a opção `allow_moved_paths` do DuckDB.

## Suporte a SQLAlchemy

Não há dialeto SQLAlchemy para Delta, e não é preciso um. O `Table` do modelo é o contrato de onde
saem o esquema Delta, o esquema Arrow e o DDL do sandbox; o motor que executa SQL sobre a tabela
Delta é o DuckDB ou o Redshift, cada um com o dialeto de [`sqlalchemy.md`](sqlalchemy.md).

- Leitura: uma view com o nome da tabela do modelo sobre `delta_scan(uri, version := v)` no DuckDB,
  e o `select()` Core compilado pelo `duckdb_engine` roda sem mudança; o resultado sai em Arrow pela
  conexão DuckDB (`to_arrow_table()` ou `to_arrow_reader()`), preservando `decimal128` e `date32`. No
  Redshift, a tabela do sandbox carregada por `COPY` é uma tabela comum.
- Escrita: o `select()` que produz o mês é compilado com `literal_binds`, executado pelo DuckDB como
  `RecordBatchReader` e entregue a `write_deltalake`; nenhuma linha passa por `executemany`.
- O ORM como unidade de trabalho (instâncias, `session.add`) não tem papel; o pipeline não o usa.

## Referências

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
