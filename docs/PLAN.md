# Plano de implementação

Este documento registra o que a biblioteca é e como ela chega lá: as decisões, o estado do projeto,
as etapas de implementação com as primitivas de cada módulo e o pipeline de atualização mensal. As
razões das decisões e as comparações entre ferramentas estão em [`estrategia.md`](estrategia.md); o
comportamento verificado do Delta, em [`delta.md`](delta.md); os motores, em [`duckdb.md`](duckdb.md)
e [`redshift.md`](redshift.md); o esquema a partir dos modelos, em [`schema.md`](schema.md) e
[`sqlalchemy.md`](sqlalchemy.md); as funcionalidades, os metadados próprios e o fluxo de cada caso de
uso, em [`serialize-db.md`](serialize-db.md). O estado descrito aqui é o de 2026-09-19.

## Decisões

A biblioteca mantém um banco analítico como tabelas Delta Lake, gravadas e lidas pelo pacote
`deltalake` (delta-rs) sobre Parquet, no bucket do projeto ou numa pasta local. Os modelos
declarativos do SQLAlchemy são o contrato de esquema: deles saem o esquema Arrow, o esquema Delta e o
DDL do sandbox nos dois motores. Os statements Core do pipeline continuam válidos, e o texto SQL
gerado por dialeto os substitui, uma interação com o banco por vez, até o SQLAlchemy terminar nos
modelos e na geração. DataFrames entram e saem por Arrow: no DuckDB,
`INSERT ... BY NAME SELECT * FROM <tabela Arrow>` e `to_arrow_reader()`; no Redshift, Parquet no S3
mais `COPY ... MANIFEST` e ADBC ou `UNLOAD`. A evolução do esquema é uma reconciliação entre o
modelo e o log da tabela, sem Alembic.

As premissas, declaradas pelo usuário, e o que cada uma fixa:

- **O pipeline é majoritariamente lógica Python.** O SQLAlchemy define os modelos e gera `select` e
  `insert` que movem DataFrames; nenhuma classe ORM é instanciada. O SQLAlchemy fica como contrato e
  Core; o SQLMesh e o dbt saem; o `insert(...)` com listas de linhas sai, porque no Redshift ele
  vira uma ida ao servidor por linha.
- **Nenhum serviço de catálogo está habilitado.** A camada de tabela não depende de serviço, e o
  Delta atende sem código próprio. O Iceberg com catálogo em arquivo fica documentado em
  `estrategia.md` e volta à mesa se o Glue ou o S3 Tables forem habilitados; `probes/catalog.py`
  mede esse gatilho.
- **O Delta é a fonte da verdade depois da carga inicial.** A tabela é criada do modelo por
  `DeltaTable.create`, sem DDL em SQL; a evolução e o histórico ficam no log.
- **Execuções de desenvolvimento e de produção gravam tabelas separadas.** Um caminho por ambiente,
  `<raiz>/<ambiente>/<tabela>/`, e o prefixo do ambiente nas tabelas do Redshift; a concorrência
  que resta é entre execuções do mesmo ambiente, que o log serializa.
- **Renomear ou remover colunas é raro.** A evolução é aditiva; o caso raro reescreve a tabela
  inteira num commit e recria a tabela publicada, sem esperar o column mapping do delta-rs.
- **Os dois armazenamentos são suportados.** Toda primitiva recebe a URI de uma pasta local ou de
  um prefixo S3; a pasta local é o ambiente dos testes e do desenvolvimento sem AWS.
- **Um teste só grava onde o usuário autorizou.** A variável de raiz de cada suíte é a
  autorização: sem ela a suíte é pulada, com ela o que impede a escrita é falha, e `pytest` sem
  variável não grava arquivo algum.

As partes da biblioteca: o esquema a partir dos modelos; o SQL gerado por dialeto; a camada Delta
sobre os dois armazenamentos; a ingestão seletiva e o sandbox por execução em cada motor; a execução
com auditoria e publicação; a publicação para clientes no Redshift; a carga inicial dos Parquet
atuais; a operação (snapshots do banco, `vacuum`, compactação, arquivo). O que sai do desenho
anterior: o ORM para cargas linha a linha, as chaves estrangeiras `DEFERRABLE`, o `Identity`, os
manifestos próprios e o Alembic. O SQLGlot fica opcional, como teste de compatibilidade.

## Estado do projeto

### O repositório

| Artefato | Estado |
| --- | --- |
| `docs/` | Completa: `parquet.md`, `duckdb.md`, `redshift.md`, `sqlalchemy.md`, `schema.md`, `delta.md`, `guia.md`, `estrategia.md`, `serialize-db.md` e este plano. Todo bloco Python dos documentos rodou com as versões fixadas em `pyproject.toml`; os comandos do Redshift foram compilados, não executados. |
| `src/serialize_db/model/` | Os modelos do pipeline, a primeira instância do contrato e o material dos testes: `model_base_contabil.py`, `model_base_gerencial.py` e `model_db_projetado.py`. Dois não importam (`from lib_base_contabil import Base` e `from lib_base_gerencial import Base`, módulos que o repositório não tem). Os modelos usam `Double` onde o contrato pede `Numeric(18, 2)`, deixam o `autoincrement` padrão nas chaves inteiras (o `duckdb_engine` emite `SERIAL`, que o DuckDB rejeita), declaram chaves estrangeiras `DEFERRABLE INITIALLY DEFERRED` (o DuckDB descarta a cláusula, o Redshift não a tem) e não têm a coluna `mes`, comentários nem `Table.info["serialize_db"]`. A etapa 1 os corrige. |
| `src/serialize_db/__init__.py` | Só o `main` de exemplo do `uv init`. `pyproject.toml` não declara dependência de execução; o grupo `dev` fixa pytest, deltalake 1.6.4, DuckDB 1.5.5, PyArrow 25.0.1, boto3, redshift-connector, SQLAlchemy 2.0.54, duckdb-engine 0.17.0, sqlalchemy-redshift 1.0.0 e pandas 3.0.6. |
| `tests/` | `conftest.py` com a regra de autorização e as fixtures dos três alvos (pasta local, bucket, Redshift); nenhum teste do pacote ainda. `tests/proof_of_concept/` com a prova de conceito da camada Delta nos dois armazenamentos (`delta.py`, `test_local.py`, `test_s3.py`), as suítes de estudo das bibliotecas externas (`test_sqlalchemy.py`, `test_duckdb.py`, `test_pyarrow.py`, `test_deltalake.py`), comentadas passo a passo por serem o material de aprendizado de quem dará manutenção, e `test_redshift.py`, os itens Redshift da etapa 0, escrito antes de haver conexão e ainda não executado. Sem variável, 18 testes passam e 39 são pulados; com a raiz local, 39 passam e 18 são pulados (2026-09-19, macOS). |
| `probes/` | Leituras do ambiente, só de leitura: `space.py`, `bucket.py`, `diagnose_aws.py`, `redshift.py` e `catalog.py` sobre `probelib.py`, com o resultado em `probes/output/` para colar na conversa. Validados aqui só nos caminhos de falha; nenhum resultado do ambiente de destino foi lido ainda. |
| `prepare_offline.sh` | Deixa a pasta autossuficiente para o destino sem internet (`.python/`, `.venv/`, `.duckdb/`); verificado extraindo o pacote em outro caminho e rodando a suíte local com proxies mortos. |

### O que a prova de conceito verificou

Em 2026-09-19, no espaço do SageMaker Unified Studio do projeto, contra o bucket do projeto na
mesma região, com deltalake 1.6.4, DuckDB 1.5.5 e PyArrow 25.0.1 (a suíte
`tests/proof_of_concept/test_s3.py`, 10 testes em 27 s):

- O delta-rs encontra as credenciais do contêiner do projeto pela cadeia padrão. Uma falha 403 na
  chamada de credenciais, no início da verificação, foi contornada com `NO_PROXY` em maiúsculas e
  não se repetiu; as credenciais do `boto3` em `storage_options` funcionam e ficam como reserva. O
  DuckDB (`credential_chain`) e o `boto3` nunca falharam.
- `write_deltalake` (`overwrite` particionado e `append` por commit condicional), `DeltaTable`,
  `vacuum` e `delta_scan` no bucket, com a criptografia SSE-KMS padrão do bucket aplicada sem opção
  alguma. O DuckDB lê `BIGINT`, `INTEGER`, `DECIMAL(18,2)`, `TIMESTAMP` (de `timestamp_ntz`) e
  `VARCHAR`.
- Put condicional pelo `boto3`: `IfNoneMatch='*'` e `IfMatch=<etag>` aceitos; a repetição de cada
  um devolve `PreconditionFailed` 412.
- Tempos no S3 (300.010 linhas, 3 arquivos): agregação por `delta_scan` em 0,3 s (0,77 s fria),
  `read_parquet` 0,06 s, tabela materializada 0,002 s (0,29 s para criar); 20 consultas pontuais em
  6,0 s por `delta_scan`, 3,1 s por `ATTACH ... PIN_SNAPSHOT`, 1,3 s por `read_parquet` e 0,013 s
  na tabela. Toda tabela consultada mais de uma vez é materializada.

Na pasta local, em macOS e no pacote extraído em outro caminho: o commit atômico em disco (dois
escritores na mesma versão: o segundo `overwrite` do mesmo mês falha com `CommitFailedError`; meses
diferentes e `append` mais `append` comitam os dois), os caminhos relativos do log com a realocação
da pasta e a abertura sem variáveis `AWS_*`. Os comportamentos do delta-rs que as etapas assumem
(substituição por predicado, `schema_mode`, `add_columns`, cast no `append`, `restore`, `vacuum`,
`keep_versions`, exportação por mês) foram verificados localmente e estão em `delta.md`.

### O que as leituras do ambiente mostraram

O espaço do SageMaker Unified Studio, lido em 2026-09-19: credenciais pelo endpoint do contêiner
(`AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`), região `us-west-2`, saída para a rede por
`proxy.awsds.internal:3128` com só `no_proxy` em minúsculas definida, bucket do projeto com SSE-KMS
e bucket key, papel sem `ListAllMyBuckets` nem `GetBucketVersioning`, conexões S3, Athena, Glue
Spark, Spark Connect e Lakehouse, e nenhuma conexão, cluster ou workgroup Redshift. O Python do
sistema é 3.12 com deltalake 1.5.0 e DuckDB 1.5.4; o `uv` alcança o PyPI pelo proxy. Consequências
no plano: a biblioteca exporta `NO_PROXY` a partir de `no_proxy`, não depende de listar buckets nem
de ler o versionamento, e a etapa 5 espera a conexão Redshift.

O ambiente definitivo não tem internet e pode não ter proxy, só um endpoint VPC do S3 (declaração
do usuário de 2026-09-19). Consequências, ainda não confirmadas por uma leitura de
`probes/diagnose_aws.py` nesse ambiente: o botocore lê `AWS_DEFAULT_REGION` ou o perfil, nunca
`AWS_REGION`, e sem região usa o endpoint global `s3.amazonaws.com`, que o endpoint VPC regional não
atende; o delta-rs lê as duas variáveis e sem nenhuma consulta o IMDS e cai em `us-east-1`; o STS
pode estar inalcançável; nenhuma extensão do DuckDB pode ser baixada. A biblioteca normaliza a
região nos dois sentidos, não chama o STS e carrega as extensões só da pasta configurada.

### Pendências

- **Redshift.** A prova de conceito espera uma conexão no projeto; os itens estão na etapa 0. A
  primeira leitura de `probes/redshift.py` fixa como a etapa 5 conecta (senha ou IAM, cluster ou
  serverless) e qual papel o `COPY` usa.
- **Manutenção da suíte S3.** Se `diagnose_aws.py` confirmar o cenário sem proxy: exportar
  `AWS_DEFAULT_REGION` a partir de `AWS_REGION`, tornar a chamada ao STS opcional com espera curta e
  passar `AWS_ENDPOINT_URL` ao secret do DuckDB. A etapa 3 implementa o mesmo na biblioteca.
- **`Text` no Redshift.** O `sqlalchemy-redshift` compila `Text` como `TEXT`, que o Redshift guarda
  como `VARCHAR(256)`. A etapa 1 emite `VARCHAR(65535)` por uma regra `@compiles(Text, "redshift")`
  em `ddl`, em vez de exigir `String(65535)` nos modelos; a escolha ainda não foi confirmada pelo
  usuário.

## Regras que as etapas obedecem

Cada regra vem de um comportamento verificado, registrado no documento citado.

- A coluna de partição `mes` vive na ação `add`, não no arquivo de dados: ela deriva de uma coluna
  do arquivo, e o Redshift a recebe por uma staging sem `mes` e `INSERT ... SELECT *, '<mes>'`, ou
  por lista de colunas no `COPY` se a prova de conceito a confirmar (`delta.md`).
- `DECIMAL(18, 2)` sai como `INT64` do delta-rs e do DuckDB; o `COPY` desse tipo físico é o primeiro
  item da prova de conceito no Redshift (`parquet.md`).
- O delta-rs não impõe duas regras de evolução: `add_columns` aceita coluna `NOT NULL` em tabela
  com dados e a deixa nula, e o `append` converte os dados para o tipo da tabela em vez de acusar a
  diferença. `reconcile` recusa a primeira, e `cast` aplica os tipos antes de gravar (`delta.md`).
- O log é limpo no checkpoint com `delta.logRetentionDuration` de 30 dias por padrão; a tabela
  nasce com `interval 3650 days`, `delta.deletedFileRetentionDuration` fica em `interval 400 days`,
  e `keep_versions` protege os snapshots do banco (`delta.md`).
- Dois `overwrite` do mesmo mês conflitam (`CommitFailedError`); meses diferentes e `append`
  entram. Uma execução por ambiente por vez, e o conflito é o sinal de que houve duas; a ação `txn`
  não impede repetição, e a idempotência é do `overwrite` por mês (`delta.md`).
- A biblioteca escreve por um único caminho, delta-rs ou `COPY ... (RETURN_STATS)` mais
  `create_write_transaction`: o `INSERT INTO` do DuckDB numa tabela Delta grava a coluna de
  partição dentro do arquivo e quebraria o `COPY` posicional (`delta.md`).
- Sem vetores de exclusão, sem column mapping, sem `Identity`, caminhos relativos no log e nunca
  um arquivo registrado por URI absoluta: as regras que mantêm o `COPY` do Redshift lendo os
  arquivos e a saída do Delta aberta (`delta.md`, `estrategia.md`).
- Ler no lugar custa o mesmo que ler Parquet solto; cada `delta_scan` relê o log, e toda tabela
  consultada mais de uma vez é materializada no DuckDB (`delta.md`).
- O delta-rs não lê `~/.aws/config`; as credenciais vêm do ambiente, do contêiner, do IMDS ou de
  `storage_options`, e a região precisa estar em `AWS_REGION` ou `AWS_DEFAULT_REGION`.
  `storage_options` leva `max_retries` e `retry_timeout` para uma rede morta falhar em 10 s em vez
  de 59 s (`README.md`).
- O DuckDB carrega extensões só da pasta configurada, com `autoinstall_known_extensions` e
  `autoload_known_extensions` desligados: o `LOAD` de uma extensão conhecida baixaria a extensão
  para `~/.duckdb` sem aviso, e o destino não tem internet (`README.md`).
- Um campo JSON é `string` no Delta e texto nos arquivos; `JSON` no DuckDB e `SUPER` no Redshift
  são tipos do motor, aplicados na carga; a auditoria confere `json_valid` antes de publicar, porque
  nem o Arrow nem o Delta validam (`schema.md`).
- O SQLGlot transpila funções, não garante suporte; os testes de integração no Redshift continuam
  (`estrategia.md`).
- O pacote `sagemaker-studio` fica fora do projeto: arrasta `deltalake`, `duckdb` e `pandas` sem
  versão fixa, e numa instalação de teste rebaixou o `duckdb` para 1.5.1. A raiz do banco é
  configuração explícita, nunca inferida do projeto (`probes/README.md`).

## Organização do pacote

| Módulo | Etapa | Conteúdo |
| --- | --- | --- |
| `serialize_db.model` | existente | Os modelos declarativos do pipeline, corrigidos na etapa 1. |
| `serialize_db.schema` | 1 | O esquema a partir dos modelos: Arrow, Delta, DDL por dialeto, opções físicas, cast seguro, arquivos gerados. |
| `serialize_db.sql` | 2 | O texto SQL por dialeto a partir de statements Core: parâmetro, prefixo, renderização, arquivos gerados. |
| `serialize_db.storage` | 3 | Os dois armazenamentos atrás de uma interface: URIs, leitura e escrita condicional, cópia, listagem, `storage_options` e o secret do DuckDB. |
| `serialize_db.delta` | 3 | A camada Delta: criação, publicação por mês, registro de arquivos, reconciliação, reescrita, manifesto, diferença de versões, snapshots, `vacuum`, compactação, cópia profunda, exportação. |
| `serialize_db.engine` | 4 e 5 | O protocolo `Engine` e os motores `duckdb` e `redshift`, com a mesma interface. |
| `serialize_db.execution` | 6 | `Database` e `Execution`, o ciclo de uma execução. |
| `serialize_db.load` | 7 | A carga inicial dos Parquet atuais. |
| `serialize_db.cli` | 6 a 9 | `serialize-db run`, `schema`, `sql`, `load`, `publish`, `snapshot`, `vacuum`, `compact`, `archive`, `export` e `history`. |

Dependências: `pyproject.toml` passa a declarar as de execução, `sqlalchemy`, `deltalake`, `duckdb`,
`pyarrow` e `boto3`, nas versões fixadas pelos documentos, mais `duckdb-engine` e
`sqlalchemy-redshift` enquanto houver compilação em tempo de execução (hoje só no grupo `dev`, para
as suítes de estudo); `redshift-connector` entra no extra `redshift`, e `sqlglot` no grupo `dev`. `prepare_offline.sh` passa a instalar os extras
(`--all-extras`) e é rodado de novo a cada mudança.

Configuração: argumentos explícitos de `Database` e da linha de comando, com as variáveis
`SERIALIZE_DB_ROOT`, `SERIALIZE_DB_ENVIRONMENT`, `SERIALIZE_DB_ENGINE`,
`SERIALIZE_DB_DUCKDB_EXTENSIONS` e `SERIALIZE_DB_REDSHIFT_*` (as de `probes/redshift.py`) como
padrão; nenhum arquivo de configuração.

Testes: `tests/` na raiz testa o pacote, um módulo de teste por módulo do pacote;
`tests/proof_of_concept/` guarda as provas de conceito e os testes das bibliotecas externas,
comentados passo a passo porque também são o material de estudo das APIs. Um teste que não grava (esquema, renderização, DuckDB em memória) roda sem variável; um teste que
grava usa a fixture `local_location`, sob `SERIALIZE_DB_TEST_LOCAL_ROOT`, e é pulado sem ela; o
marcador `s3` repete no bucket os testes que dependem do armazenamento, sob
`SERIALIZE_DB_TEST_S3_ROOT`; o marcador `redshift` roda só com `SERIALIZE_DB_TEST_REDSHIFT_SCHEMA`,
o esquema onde a suíte pode criar tabelas `serialize_db_test_<id>_*`, e conecta pelas variáveis
`SERIALIZE_DB_REDSHIFT_*`. Os arquivos gerados dos modelos do projeto (`schema/` e `sql/` na raiz
do repositório) são comparados por teste com uma geração nova, sem gravar.

## Etapas

Cada etapa entrega um módulo com testes. As etapas 1 a 4 e 6 rodam em pastas locais, sem AWS; a
etapa 5 e a parte Redshift da etapa 0 exigem a conexão; a etapa 7 exige os Parquet de origem.

| Etapa | Entrega | Critério de aceite |
| --- | --- | --- |
| 0. Prova de conceito na AWS | `tests/proof_of_concept/`: S3 verificado, Redshift pendente. | Cada item respondido em `delta.md` e `redshift.md`; nenhum bloqueio sem alternativa. |
| 1. `schema` | Modelos corrigidos; esquema Arrow, Delta e DDL; cast; arquivos `schema/`. | `create_all` no DuckDB em memória passa; o teste de diff falha quando um modelo muda sem regenerar; `cast` recusa perda de precisão, texto longo e nulo em `NOT NULL`. |
| 2. `sql` | `param`, `prefixed`, `render`, `bind`, `write_sql_files`. | O texto de um statement com parâmetro, `%` em literal e prefixo roda no DuckDB com `$nome`; o teste de diff dos arquivos `sql/`. |
| 3. `storage` e `delta` | Os dois armazenamentos; a camada Delta inteira. | Testes locais de substituição do mês, conflito, reconciliação aditiva e destrutiva, reescrita num commit, `keep_versions`, exportação por mês e realocação; os mesmos no bucket com `-m s3`. |
| 4. Motor DuckDB | Conexão, ingestão, consulta, execução de texto, carga, auditoria, exportação do mês. | O pipeline de exemplo roda em memória sobre um Delta local. |
| 5. Motor Redshift | O mesmo protocolo com sandbox `exec_<id>_`, `COPY ... MANIFEST` e `UNLOAD`. | SQL gerado coberto por testes sem cluster; integração com amostra, marcador `redshift`. |
| 6. Execução e linha de comando | `Database`, `Execution`, `serialize-db run`. | Reexecução idempotente; auditoria reprovada não altera o Delta; conflito abortado com mensagem. |
| 7. Carga inicial | Migração dos Parquet atuais por tabela e por mês, com relatório. | Contagens e somas por mês iguais entre origem e Delta. |
| 8. Publicação para clientes | Tabelas `<ambiente>_*` no Redshift, `version_diff`, transação única, `serialize_db_publications`. | Um mês alterado recarrega só esse mês. |
| 9. Operação | Snapshots, `vacuum`, compactação, arquivo, exportação, `history`, runbook, `pdoc`. | Runbook escrito e testes de manutenção passando. |

### Etapa 0: prova de conceito na AWS

Os itens de S3 estão verificados (seção "O que a prova de conceito verificou"). Os de Redshift
estão em `tests/proof_of_concept/test_redshift.py`, marcador `redshift`, escrito antes de haver
conexão e ainda não executado; ele roda quando `probes/redshift.py` mostrar a conexão, com os
arquivos sob `SERIALIZE_DB_TEST_S3_ROOT` e as tabelas no esquema de `SERIALIZE_DB_TEST_REDSHIFT_SCHEMA`:

- `COPY ... FORMAT AS PARQUET MANIFEST` de arquivos gravados pelo delta-rs: `DECIMAL(18, 2)` em
  `INT64`, `timestamp_ntz` em `INT64` de microssegundos, o que acontece com uma string acima do
  `VARCHAR` de destino (truncar ou abortar), a lista de colunas no `COPY`, `FILLRECORD` para
  arquivos anteriores a uma coluna nova, e `SUPER` direto do `COPY` para documentos acima de
  65.535 bytes.
- `UNLOAD ... PARTITION BY (mes) MANIFEST VERBOSE`: os tipos físicos de `TIMESTAMP` e `DECIMAL`, se
  as colunas saem `required`, se há estatísticas de mínimo e máximo, e o registro dos arquivos por
  `create_write_transaction`, lido pelo DuckDB.
- Se o Redshift Spectrum mapeia colunas Parquet por nome ou por posição, só para registro; o
  projeto não cria esquemas externos.

Antes de qualquer etapa na AWS, os probes rodam no ambiente e o resultado é colado na conversa:
`space.py` e `diagnose_aws.py` para a suíte S3, `bucket.py` para a raiz escolhida, `redshift.py`
para a etapa 5.

### Etapa 1: `schema`

O módulo `serialize_db.schema` deriva dos modelos tudo o que os outros módulos precisam saber sobre
uma tabela. A etapa começa pelos modelos de `src/serialize_db/model/`: `Base` importável de um
módulo só, `Numeric(18, 2)` nas colunas monetárias, `autoincrement=False` nas chaves inteiras,
chaves estrangeiras sem `DEFERRABLE`, a coluna `mes` (`String(7)`) nas tabelas particionadas,
comentários de tabela e de coluna, e `Table.info["serialize_db"]` com `partition_by`, `sort_key` e
`redshift`, como em `schema.md`.

| Primitiva | O que faz |
| --- | --- |
| `arrow_schema(table)` | O `pa.Schema` do `Table`: os tipos da tabela de `schema.md`, a nulidade, o comentário de cada coluna em `metadata` do campo, `PARQUET:field_id` e a marca `arrow.json` nos campos JSON. |
| `delta_schema(table)` | O `deltalake.Schema` derivado do Arrow: `decimal(18,2)`, `timestamp_ntz` para `DateTime` sem fuso e `timestamp` para o com fuso, `string` para JSON e UUID, comentários preservados. |
| `ddl(table, dialect, prefix="")` | `CREATE TABLE` para `duckdb` ou `redshift`: sem `DEFERRABLE` nem `Identity`, `CHECK` só no DuckDB, chaves só quando `table_options` as pede, `SORTKEY`, `DISTSTYLE` e `DISTKEY` no Redshift, `Text` como `VARCHAR(65535)`, `Uuid` como `VARCHAR(36)`, `JSON` como `SUPER`; `prefix` renomeia a tabela para o sandbox. |
| `table_options(table)` | O `TableOptions` (`partition_by`, `sort_key`, `redshift`, `keys`) lido de `Table.info["serialize_db"]` com os padrões da biblioteca: partição por `mes` quando a coluna existe, nenhuma chave declarada. |
| `cast(reader, table)` | O `RecordBatchReader` convertido lote a lote para `arrow_schema(table)` com `safe=True`: serializa `dict` em JSON, leva timestamps a microssegundos e UTC, recusa perda de precisão, texto acima de `String(n)` e nulo em coluna `NOT NULL`. |
| `check_models(metadata)` | A lista de violações do contrato nos modelos: tipo fora da tabela de tipos, `Double` em coluna monetária, `autoincrement` em chave inteira, `DEFERRABLE`, `Identity`, tabela particionada sem `mes`, coluna sem comentário. Vazia nos modelos corrigidos. |
| `schema_files(metadata)` | `{"<tabela>.delta.json": ..., "<tabela>.duckdb.sql": ..., "<tabela>.redshift.sql": ...}` em memória. |
| `write_schema_files(metadata, directory)` | Grava `schema_files` em `directory`; `serialize-db schema write` grava e `serialize-db schema check` compara sem gravar. |

Testes: `tests/test_schema.py`, sem gravar, sobre os modelos do projeto e sobre um modelo de teste
com todos os tipos; `create_all` no DuckDB em memória com o DDL de cada modelo; o diff dos arquivos
`schema/` versionados; `write_schema_files` sob a raiz local. Dependências: `sqlalchemy`, `pyarrow`,
`deltalake`, `duckdb`, `duckdb-engine` e `sqlalchemy-redshift`.

### Etapa 2: `sql`

O módulo `serialize_db.sql` gera o texto SQL de cada dialeto a partir de um statement Core, para a
substituição gradual da compilação em tempo de execução descrita em `sqlalchemy.md`.

| Primitiva | O que faz |
| --- | --- |
| `param(name, type_=None)` | `literal_column(":nome", type_)`: o parâmetro de execução, que atravessa `literal_binds` e chega ao texto como `:nome`. |
| `prefixed(statement, metadata, prefix="{prefix}")` | A cópia do statement com cada tabela do contrato trocada pela cópia com o prefixo, por `replacement_traverse`; o nome sai sem aspas (`quoted_name(quote=False)`). |
| `render(statement, dialect, metadata, prefix="{prefix}")` | O texto de `duckdb` ou `redshift` com as constantes embutidas e os parâmetros como `:nome`, compilado por um dialeto com `paramstyle="named"`, que não dobra o `%` dos literais; um `bindparam` sem valor é erro, porque o compilador o renderia como `NULL`. |
| `bind(sql, params, style)` | O texto com `:nome` reescrito para o estilo do motor (`$nome` no DuckDB; inalterado no `redshift_connector` com `paramstyle = "named"`) e o dicionário conferido: parâmetro faltante ou sobrando é erro. |
| `sql_files(statements, metadata)` | `{"<nome>.duckdb.sql": ..., "<nome>.redshift.sql": ...}` de um dicionário `{nome: statement}`. |
| `write_sql_files(statements, metadata, directory)` | Grava `sql_files`; `serialize-db sql write` e `serialize-db sql check`. |
| `read_sql(directory, name, dialect)` | O texto versionado, para o `execute` dos motores. |

Testes: `tests/test_sql.py`, sem gravar: o statement de `sqlalchemy.md` (parâmetro, `%` em literal,
prefixo) renderizado nos dois dialetos e executado no DuckDB em memória com `$mes`; `bindparam` sem
valor e parâmetro faltante como erros; o diff de `sql/`. Opcional: `sqlglot.parse_one(texto,
dialect)` como teste de que o texto do Redshift analisa.

### Etapa 3: `storage` e `delta`

`serialize_db.storage` esconde a diferença entre a pasta local e o S3; é a divisão de
`tests/conftest.py` (`LocalLocation`, `S3Location`) levada à biblioteca.

| Primitiva | O que faz |
| --- | --- |
| `Storage.for_uri(uri)` | `LocalStorage` para um caminho ou `file://`, `S3Storage` para `s3://bucket/prefixo`. |
| `join(*parts)`, `exists(path)`, `list_files(prefix, suffix)`, `delete(paths)` | Caminhos relativos à raiz; a listagem exclui `_delta_log/`. |
| `read_text(path)`, `write_text(path, text, if_match=None, if_none_match=False)` | Escrita condicional: `IfMatch` e `IfNoneMatch` no S3 (412 vira `ConflictError`); `O_EXCL` e `os.replace` na pasta local. É a escrita de `_serialize_db/snapshots.json`. |
| `copy(source, destination)` | `CopyObject` no S3, `shutil.copy2` na pasta local; a exportação sem ler dados. |
| `storage_options()` | As opções do delta-rs: região, `AWS_ENDPOINT_URL`, `max_retries`, `retry_timeout`, `timeout`, as chaves de SSE quando configuradas, e as credenciais do `boto3` só na reserva. |
| `duckdb_setup(connection)` | `LOAD httpfs; LOAD delta; LOAD aws` e o secret `credential_chain` com `REGION` e `ENDPOINT`; só `LOAD delta` na pasta local. |
| `prepare_environment()` | Exporta `NO_PROXY` a partir de `no_proxy`, copia a região entre `AWS_REGION` e `AWS_DEFAULT_REGION` nos dois sentidos, respeita `AWS_ENDPOINT_URL`; devolve o que mudou, para o log. Chamada por `Database`. |

`serialize_db.delta` é a camada de tabela; `uri` é a pasta da tabela, `table` o `Table` do modelo,
`reader` um `RecordBatchReader`.

| Primitiva | O que faz |
| --- | --- |
| `create_table(uri, table)` | `DeltaTable.create(mode="ignore")` com `delta_schema`, `partition_by`, nome, descrição e as propriedades `delta.logRetentionDuration = interval 3650 days` e `delta.deletedFileRetentionDuration = interval 400 days`; sem vetores de exclusão nem column mapping. |
| `open(uri, version=None)` | A `DeltaTable` numa versão; a execução abre cada tabela uma vez e guarda a versão. |
| `commit_metadata(execution_id, input_versions, snapshot=None)` | O dicionário de `CommitProperties(custom_metadata=...)`: `serialize_db_execution_id`, `serialize_db_input_versions` e `serialize_db_snapshot`. |
| `publish_month(uri, month, reader, metadata)` | `write_deltalake(mode="overwrite", predicate="mes = '<mes>'")` do `reader` já passado por `cast`; `CommitFailedError` sobe como `ExecutionConflict`. |
| `register_files(uri, files, months, metadata)` | `create_write_transaction(mode="overwrite", partition_filters=...)` com uma `AddAction` por arquivo: caminho relativo à pasta da tabela, tamanho, valores de partição e estatísticas do `RETURN_STATS` do DuckDB ou do rodapé Parquet. |
| `schema_diff(table, dt)` | O `SchemaDiff` entre `arrow_schema(table)` e `dt.schema()`: coluna nova anulável, `NOT NULL` relaxado e `CHECK` são aditivos; coluna `NOT NULL` nova em tabela com dados, renomeação, remoção e mudança de tipo são destrutivos. |
| `reconcile(uri, table)` | Aplica o diff aditivo (`add_columns`, `drop_column_not_null`, `add_constraint`) e recusa o destrutivo com a mensagem que aponta `rewrite`. |
| `rewrite(uri, table)` | A tabela inteira com o esquema do contrato num único commit e sem predicado: `COPY ... PARTITION_BY (mes) ... RETURN_STATS` do DuckDB a partir de `delta_scan` mais `create_write_transaction(mode="overwrite", schema=...)`, com memória constante. |
| `copy_manifest(uri, version, months, destination)` | O manifesto do `COPY` do Redshift (`url` e `meta.content_length` de `get_add_actions()`), gravado sob `publicacao/`. |
| `version_diff(uri, published, current)` | Os meses com ações `add` entre as duas versões. |
| `snapshot(root, name, versions)` | A entrada `{name: versions}` em `_serialize_db/snapshots.json`, gravada com `write_text(if_match=...)`. |
| `vacuum_keeping_snapshots(uri, control, retention_hours=9600, apply=False)` | `vacuum` com `keep_versions` das versões do arquivo de controle; lista por padrão e apaga com `apply=True`. |
| `compact(uri, months)` | `optimize.compact` dos meses com arquivos pequenos, antes de um snapshot. |
| `deep_copy(uri, version, destination)` | Tabela nova na versão 0 com os dados de uma versão, para a pasta de arquivo. |
| `export_snapshot(uri, destination, version=None, mode="copy")` | Pastas `mes=.../` sem o log: `copy` copia os arquivos que o log lista; `rewrite` reescreve pelo `COPY` particionado do DuckDB. |

Testes: `tests/test_storage.py` e `tests/test_delta.py` sob a raiz local, com os mesmos casos no
bucket por `-m s3`: substituição do mês e idempotência, conflito entre dois escritores,
reconciliação aditiva e recusa da destrutiva, `rewrite` num commit sem predicado com a versão
anterior legível, `keep_versions`, `export_snapshot` nos dois modos, realocação da pasta e a escrita
condicional do arquivo de controle.

### Etapa 4: motor DuckDB

`serialize_db.engine` declara o protocolo `Engine`; a execução não sabe qual motor está por trás.
`serialize_db.engine.duckdb` o implementa.

| Primitiva | DuckDB |
| --- | --- |
| `connect(config)` | Banco em arquivo `<pasta temporária>/<execution_id>.duckdb` ou em memória; `extension_directory` de `SERIALIZE_DB_DUCKDB_EXTENSIONS` (ou `.duckdb/` da pasta preparada), `autoinstall_known_extensions` e `autoload_known_extensions` desligados; `storage.duckdb_setup`; `threads`, `memory_limit`, `temp_directory` e `preserve_insertion_order = false`. |
| `ingest(table, uri, version, months=None, materialize=False)` | View com o nome do modelo sobre `delta_scan(uri, version := v)`, ou `CREATE TABLE ... AS SELECT ... FROM delta_scan(...) WHERE mes IN (...)` com `materialize=True`. |
| `query(statement)` | O statement Core compilado para o dialeto e executado; o resultado por `to_arrow_reader()`. |
| `execute(sql, params)` | O texto gerado por `render`: `{prefix}` vira vazio, `:nome` vira `$nome` por `sql.bind`, e o resultado volta por `to_arrow_reader()`. |
| `load(table, reader)` | `INSERT ... BY NAME SELECT * FROM <tabela Arrow>` numa tabela do sandbox criada por `ddl(table, "duckdb")`. |
| `audit(table, months)` | O `AuditReport` por consulta: contagem, nulos em `NOT NULL`, unicidade das chaves de `table_options`, `mes = strftime(data_ref, '%Y-%m')`, limites de tipo, `json_valid` e totais de controle; `passed` falso interrompe a execução. |
| `export_month(table, month)` | O `reader` do mês, passado por `cast`, para `publish_month`; ou `COPY ... TO '<uri>/mes=<mes>/<execution_id>.parquet' (FORMAT parquet, RETURN_STATS)` mais `register_files` para a tabela que não cabe na memória. |
| `cleanup()` | Fecha a conexão e apaga o arquivo do banco e a pasta de transbordo. |

Testes: `tests/test_engine_duckdb.py` sob a raiz local: o pipeline de exemplo (doze meses
materializados e dimensões em view, um `select` com `join`, auditoria, exportação do mês) sobre um
Delta local criado no teste; auditoria que reprova um mês com chave duplicada; `execute` com `%`
em literal.

### Etapa 5: motor Redshift

`serialize_db.engine.redshift` implementa o mesmo protocolo. A conexão vem de `RedshiftConfig`:
senha (`host`, `port`, `database`, `user`, `password`) ou IAM (`cluster` ou `workgroup`), o
`iam_role` do `COPY` e do `UNLOAD`, e `schema`, com os padrões nas variáveis
`SERIALIZE_DB_REDSHIFT_*`.

| Primitiva | Redshift |
| --- | --- |
| `connect(config)` | `redshift_connector.connect` com `timeout`; `search_path` no esquema; `cursor.paramstyle = "named"`. |
| `ingest(table, uri, version, months=None, materialize=True)` | `copy_manifest` dos arquivos desses meses, `COPY ... FORMAT AS PARQUET MANIFEST IAM_ROLE ...` numa staging sem `mes` criada por `ddl`, e `INSERT INTO exec_<id>_<tabela> SELECT *, '<mes>'`; `JSON_PARSE` nas colunas `SUPER`. |
| `query(statement)` | O statement compilado para o Redshift; o resultado em Arrow a partir das tuplas com o esquema do statement, ou por `UNLOAD` acima de um limite de linhas. |
| `execute(sql, params)` | `{prefix}` vira `exec_<id>_`, e o texto roda com o dicionário. |
| `load(table, reader)` | Parquet em `staging/<execution_id>/` pelo PyArrow mais `COPY`; `insert(...).values(lista)` numa única ida para lotes pequenos. |
| `audit(table, months)` | As mesmas consultas, compiladas para o Redshift. |
| `export_month(table, month)` | `UNLOAD ('<select do contrato>') TO '<uri>/' PARTITION BY (mes) FORMAT PARQUET MANIFEST VERBOSE` mais `register_files` com as estatísticas do rodapé Parquet. |
| `cleanup()` | `DROP TABLE` de `exec_<id>_*` e da staging; os objetos de `staging/<execution_id>/` apagados. |

O identificador de execução entra no nome do sandbox normalizado para `[a-z0-9_]`, dentro dos
127 bytes de um identificador do Redshift. Testes: `tests/test_engine_redshift.py` compara o SQL
gerado (`COPY`, `INSERT ... SELECT`, `UNLOAD`, DDL da staging) com texto esperado, sem cluster; os
testes marcados `redshift` rodam a mesma sequência com uma amostra no esquema autorizado, depois do
`test_redshift.py` da etapa 0.

### Etapa 6: execução e linha de comando

`serialize_db.execution` é o ciclo de uma execução; `serialize_db.cli` o expõe.

| Primitiva | O que faz |
| --- | --- |
| `Database(root, environment, metadata, storage_options=None)` | A raiz do banco, o ambiente (`prod`, `dev`) e o `MetaData` dos modelos; `uri(table)` é `<root>/<ambiente>/<tabela>/`, mais o arquivo de controle e os prefixos `staging/`, `publicacao/` e `arquivo/`; chama `prepare_environment` e cria `Storage.for_uri(root)`. |
| `Execution(db, engine, month, execution_id)` | Gerenciador de contexto: na entrada abre as tabelas de entrada, fixa `versions` e cria o sandbox; na saída descarta o sandbox e grava o resumo no log. |
| `run.previous_months(n)` | Os `n` meses até `run.month`, inclusive. |
| `run.ingest(*tables, months=None, materialize=False)` | `engine.ingest` de cada tabela na versão fixada; sem `months`, a tabela inteira. |
| `run.sandbox` | O motor, onde o pipeline chama `query`, `execute` e `load`. |
| `run.audit(table, months)` | `engine.audit`; a reprovação levanta `AuditFailed` e encerra sem tocar o Delta. |
| `run.publish(table, months)` | `create_table` se não existir, `reconcile`, depois `export_month` e `publish_month` por mês com `commit_metadata`; avança `versions[table]`. |
| `run.publish_redshift(*tables)` | A publicação da etapa 8. |
| `run.snapshot(name)` | Marca a execução: `serialize_db_snapshot` nos commits e `snapshot(root, name, versions)` no encerramento. |
| `serialize-db run` | `--root`, `--environment`, `--engine`, `--month`, `--execution-id` e `modulo:funcao` do pipeline, que recebe `run`; código de saída 0, 1 na reprovação da auditoria, 2 no conflito. |

O log é o `logging` padrão com um resumo por execução: identificador, mês, versões lidas, versões
gravadas e tempo por passo. Testes: `tests/test_execution.py` sob a raiz local com o motor DuckDB:
a reexecução com o mesmo `execution_id` produz o mesmo estado; a auditoria reprovada deixa a versão
da tabela como estava; de duas execuções publicando o mesmo mês, a segunda aborta com
`ExecutionConflict`.

### Etapa 7: carga inicial

A migração dos Parquet atuais para o Delta, uma passagem por tabela e por mês, reexecutável, em
`serialize_db.load`.

| Primitiva | O que faz |
| --- | --- |
| `initial_load(db, table, source, months=None)` | `create_table`; para cada mês, o DuckDB lê `source/mes=<mes>/*.parquet` (ou o layout que a origem tiver), `cast` converte para o contrato (os modelos atuais usam `Double` onde o contrato pede `Numeric(18, 2)`) e `publish_month` grava em lotes. Uma carga interrompida recomeça do mês seguinte ao último publicado. |
| `load_report(db, table, source)` | Contagem e somas das colunas numéricas por mês, na origem e no Delta; a carga só termina quando coincidem. |
| `serialize-db load` | `--table`, `--source` e `--months`. |

`convert_to_deltalake` registra os arquivos no lugar, sem reescrever, só quando eles já têm os
tipos, a ordem de colunas e o layout Hive do contrato; não foi testado e não é o caminho padrão.
Depois da carga os leitores abrem o Delta, e as pastas de origem ficam como cópia até a primeira
publicação no Redshift. Testes: `tests/test_load.py` com Parquet gerados no teste sob a raiz local,
incluindo uma origem em `Double` e uma carga interrompida.

### Etapa 8: publicação para clientes

As tabelas publicadas, `<ambiente>_<tabela>` no esquema único, são derivadas do Delta; nada é
escrito nelas por outro caminho.

| Primitiva | O que faz |
| --- | --- |
| `serialize_db_publications` | `CREATE TABLE IF NOT EXISTS serialize_db_publications (table_name VARCHAR(127), delta_version BIGINT, execution_id VARCHAR(127), published_at TIMESTAMP)`. |
| `run.publish_redshift(*tables)` | Para cada tabela, `version_diff` entre a versão em `serialize_db_publications` e a atual (na primeira publicação, todos os meses); a reconciliação da tabela publicada (`ALTER TABLE ADD COLUMN` no fim, porque o `COPY` é posicional; recriação e recarga no diff destrutivo); numa única transação, por tabela e mês: `DELETE` do mês, `COPY ... MANIFEST` na staging, `INSERT ... SELECT *, '<mes>'` e a linha de controle. |
| `publication_status(db)` | A versão publicada contra a atual de cada tabela, para o operador. |
| `serialize-db publish` | A publicação fora de uma execução, por exemplo depois de uma correção. |

Testes: o SQL da transação comparado com texto esperado, sem cluster; integração marcada
`redshift`.

### Etapa 9: operação

As primitivas são as da etapa 3; a etapa entrega a rotina e a documentação.

| Rotina | Quando | Comando |
| --- | --- | --- |
| Snapshot do banco | Na periodicidade do processo, por exemplo o fim do trimestre. | `run.snapshot("2026T3")` na execução marcada. |
| Compactação | Antes de um snapshot, nunca depois. | `serialize-db compact --months ...`. |
| `vacuum` | Mensal: lista com `keep_versions` do arquivo de controle, revisada, depois aplicada; `--full` de tempos em tempos para os órfãos. | `serialize-db vacuum [--apply] [--full]`. |
| Arquivo | Anual: `deep_copy` dos snapshots mais velhos que o prazo da tabela viva para `arquivo/<nome>/<tabela>/`, a entrada sai de `snapshots.json`, a pasta recebe a regra de ciclo de vida. | `serialize-db archive <nome>`. |
| Exportação | Sob demanda: pastas Parquet por mês de um snapshot, `copy` ou `rewrite`. | `serialize-db export`. |
| Monitoração | `history()` de cada tabela com os metadados da biblioteca. | `serialize-db history`. |

A documentação da API sai do `pdoc`; o runbook lista cada rotina com o comando, o que conferir
antes e o que esperar depois. Testes: `tests/test_operation.py` sob a raiz local: `vacuum` com
`keep_versions` preserva a versão do snapshot e remove a intermediária; `compact` antes do snapshot;
`deep_copy` com as mesmas somas.

## Pipeline de atualização mensal

Uma execução de exemplo: `exec-2026-09-05`, ambiente `prod`, motor DuckDB, mês de referência
`2026-08`. As entradas são `cad_lancamentos` (os doze meses até 2026-08), `cad_contratos`,
`cad_operacoes`, `rel_contrato_operacao` e as tabelas `dom_*`; a saída ilustrativa é
`cad_lancamentos` do banco projetado, mês 2026-08. Os números de versão são ilustrativos.

| Passo | O que acontece | Artefatos |
| --- | --- | --- |
| 1. Abertura | Lê `_serialize_db/snapshots.json` e `serialize_db_publications`; abre cada tabela de entrada e registra a versão. | `versions = {cad_lancamentos: 143, cad_contratos: 88, ...}` gravado no log da execução. |
| 2. Ingestão | DuckDB: views com os nomes dos modelos sobre `delta_scan(uri, version := 143)`; `cad_lancamentos` materializada com `WHERE mes BETWEEN '2025-09' AND '2026-08'`; dimensões como views. Redshift: `COPY ... MANIFEST` dos arquivos desses meses em `exec_2026_09_05_cad_lancamentos`, via staging. | Sandbox em `/tmp/exec-2026-09-05.duckdb` ou tabelas com prefixo no esquema único. |
| 3. Execução | O pipeline roda statements Core, texto gerado e lógica Python sobre o sandbox; intermediários ficam no sandbox. | Tabela `cad_lancamentos_projetados` no sandbox, mês 2026-08. |
| 4. Auditoria | Contagem, nulos, unicidade da chave, `mes = strftime(data_ref, '%Y-%m')`, limites de tipo, `json_valid`, totais de controle. | Relatório da execução; reprovação encerra sem tocar o Delta. |
| 5. Publicação no Delta | `reconcile` e `publish_month(uri, "2026-08", reader, commit_metadata(...))`; do Redshift, `UNLOAD ... PARTITION BY (mes)` mais `register_files`. | `cad_lancamentos` projetado passa da versão 57 para 58; um arquivo em `mes=2026-08/`. |
| 6. Publicação no Redshift | `version_diff(57, 58)` aponta o mês 2026-08; `DELETE` do mês, `COPY ... MANIFEST` na staging, `INSERT ... SELECT *, '2026-08'`; controle atualizado. | `prod_cad_lancamentos_projetados` com o mês novo; `serialize_db_publications` em 58. |
| 7. Snapshot do banco | Só na execução marcada, por exemplo a do fim do trimestre: `serialize_db_snapshot = "2026T3"` nos commits e a entrada em `_serialize_db/snapshots.json`. | Versões do snapshot protegidas por `keep_versions`. |
| 8. Encerramento | Sandbox descartado, staging apagado, resumo no log. | Execução idempotente: repetir os passos 5 e 6 reproduz o mesmo estado. |

A API da etapa 6, ainda não implementada:

```python
from serialize_db import Database, Execution
from pipeline import compute_projections
from pipeline.models import Contrato, Lancamento, LancamentoProjetado, Operacao, RelContratoOperacao

db = Database("s3://bucket/projeto/delta", environment="prod")
with Execution(db, engine="duckdb", month="2026-08", execution_id="exec-2026-09-05") as run:
    run.ingest(Lancamento, months=run.previous_months(12), materialize=True)
    run.ingest(Contrato, Operacao, RelContratoOperacao)              # views sobre a versão fixada
    compute_projections(run.sandbox)                                 # statements Core, texto gerado e Python
    run.audit(LancamentoProjetado, months=["2026-08"])
    run.publish(LancamentoProjetado, months=["2026-08"])             # overwrite por mês, metadados
    run.publish_redshift(LancamentoProjetado)                        # só os meses alterados
```

Uma reexecução com o mesmo `execution_id` repete os `overwrite` dos mesmos meses e produz o mesmo
estado. Uma correção de um mês antigo é a mesma chamada com outro `month` e um `execution_id` novo:
`publish_redshift` recarrega só esse mês, e as versões intermediárias entre snapshots do banco saem
no `vacuum` mensal. A execução no Redshift é o mesmo ciclo com `engine="redshift"`: o sandbox são as
tabelas `exec_<id>_*`, a ingestão é `COPY ... MANIFEST`, e a publicação sai por `UNLOAD` mais
`register_files`, sem passar pela máquina local.

## Ordem do trabalho

1. Etapas 1 e 2, em pastas locais, com os modelos corrigidos e os arquivos `schema/` e `sql/` do
   projeto versionados.
2. Etapa 3, depois 4 e 6: um pipeline completo em disco local, o critério de aceite da etapa 6
   sobre o motor DuckDB.
3. Em paralelo, no ambiente de destino: `probes/space.py`, `diagnose_aws.py` e `bucket.py`; a
   manutenção da suíte S3 se confirmada; `tests/proof_of_concept/` e os testes `-m s3` das etapas 3
   e 4 no bucket.
4. Quando `probes/redshift.py` mostrar a conexão: o `test_redshift.py` da etapa 0, depois as etapas
   5 e 8.
5. Etapa 7 quando os Parquet de origem estiverem acessíveis; etapa 9 por último, com o runbook.
