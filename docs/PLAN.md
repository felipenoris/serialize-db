# Plano de implementação

Este documento registra o que a biblioteca é e como ela chega lá: as decisões, o estado do projeto,
as etapas de implementação e o pipeline de atualização mensal. O plano de cada etapa, com as
primitivas do módulo, os testes e as provas de conceito que o exercitam, está num arquivo próprio,
de [`PLAN-STAGE-0.md`](PLAN-STAGE-0.md) a [`PLAN-STAGE-9.md`](PLAN-STAGE-9.md), que a seção
"Etapas" indexa. As razões das decisões e as comparações entre ferramentas estão em
[`estrategia.md`](estrategia.md); o comportamento verificado do Delta, em [`delta.md`](delta.md); os
motores, em [`duckdb.md`](duckdb.md) e [`redshift.md`](redshift.md); o esquema a partir dos modelos,
em [`schema.md`](schema.md) e [`sqlalchemy.md`](sqlalchemy.md); as funcionalidades, os metadados
próprios e o fluxo de cada caso de uso, em [`serialize-db.md`](serialize-db.md). O estado descrito
aqui é o de 2026-09-20.

## Decisões

A biblioteca mantém um banco analítico como tabelas Delta Lake, gravadas e lidas pelo pacote
`deltalake` (delta-rs) sobre Parquet, no bucket do projeto ou numa pasta local. Os modelos
declarativos do SQLAlchemy são o contrato de esquema: deles saem o esquema Arrow, o esquema Delta e
o DDL do sandbox nos dois motores. Chave primária, unicidade e chave estrangeira não entram nesse
DDL, porque o Parquet não as tem, o DuckDB as cobra na carga e o Redshift só as registra: quem as
aplica é a auditoria da execução, com consultas derivadas dos mesmos modelos, e a reprovação impede
a publicação. Os statements Core do pipeline continuam válidos, e o texto SQL gerado por dialeto os
substitui, uma interação com o banco por vez, até o SQLAlchemy terminar nos modelos e na geração.
Os dados cruzam a fronteira da biblioteca como `pyarrow.Table`, nos dois sentidos: no DuckDB,
`INSERT ... BY NAME SELECT * FROM <tabela Arrow>` e `to_arrow_table()`; no Redshift, Parquet no S3
mais `COPY ... MANIFEST` e ADBC ou `UNLOAD`. A evolução do esquema é uma reconciliação entre o
modelo e o log da tabela, sem Alembic.

As premissas, declaradas pelo usuário, e o que cada uma fixa:

- **O pipeline é majoritariamente lógica Python.** O SQLAlchemy define os modelos e gera `select` e
  `insert` que movem tabelas; nenhuma classe ORM é instanciada. O SQLAlchemy fica como contrato e
  Core; o SQLMesh e o dbt saem; o `insert(...)` com listas de linhas sai, porque no Redshift ele
  vira uma ida ao servidor por linha.
- **A troca de dados com o código cliente é por `pyarrow.Table`** (declaração de 2026-09-20). Quem
  grava passa uma `pa.Table` a `load`; quem lê passa um statement Core ou um texto SQL a `query` ou
  a `execute` e recebe uma `pa.Table`. Nenhuma instância ORM, lista de linhas ou DataFrame atravessa
  a fronteira: `query` compila o statement pelo dialeto e o executa na conexão crua, sem `Session`,
  e `select(Lancamento)` é aceito como statement Core. O pandas com backend pyarrow é o formato dos
  pipelines (declaração do usuário de 2026-09-20), e a regra apoia-se na hipótese, medida na seção
  seguinte, de que a conversão é barata: 2,3 ms sem cópia para 300.000 linhas. O pandas não entra
  nas dependências de execução.
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

### A troca de dados com o código cliente

A fronteira da biblioteca é a `pyarrow.Table`. O pipeline grava com `run.sandbox.load(Modelo, data)`
e lê com `run.sandbox.query(statement)` ou `run.sandbox.execute(texto, params)`, que devolvem uma
`pa.Table`; o caminho de um resultado até o Delta é `load`, `audit` e `publish`. Nenhuma primitiva
pública recebe ou devolve um DataFrame, uma lista de linhas ou uma instância ORM. Dentro da
biblioteca, o que não cabe na memória corre por `RecordBatchReader` (`export_month` para
`publish_month`, `rewrite`, a carga inicial), sob a regra de que a conexão não roda outro comando
enquanto o leitor é consumido, porque o leitor do DuckDB esvazia sem erro no comando seguinte
(`duckdb.md`); a `pa.Table` devolvida ao cliente não tem esse defeito.

A hipótese da regra, de que converter para pandas é barato, foi medida em 2026-09-20 em macOS arm64
com pandas 3.0.6, PyArrow 25.0.1 e DuckDB 1.5.5, sobre 300.000 linhas de `operacoes` com sete
colunas (`int64`, `date32`, `int64`, `decimal128(18, 2)`, `string`, `timestamp[us]`, `string`), uma
execução de cada conversão:

| Conversão | Tempo | Cópia | Tipos |
| --- | --- | --- | --- |
| `table.to_pandas(types_mapper=pd.ArrowDtype)` | 2,3 ms | Nenhuma: os buffers de cada coluna são os da tabela, 128 bytes alocados. | `int64[pyarrow]`, `date32[day][pyarrow]`, `decimal128(18, 2)[pyarrow]`, `string[pyarrow]`, `timestamp[us][pyarrow]`; inteiro com nulo continua inteiro, com `<NA>`. |
| `table.to_pandas()` | 37,6 ms | 12 MB. | `valor` em `object` de `Decimal`, `data_ref` em `object` de `date` (`datetime64[ms]` com `date_as_object=False`), `descricao` em `str`; inteiro com nulo vira `float64`. |
| `pa.Table.from_pandas(df, preserve_index=False)` do DataFrame com `ArrowDtype` | 0,6 ms | Nenhuma. | Os mesmos tipos, todo campo anulável e sem metadados; `cast` devolve o esquema do contrato em menos de 0,1 ms. |
| `pa.Table.from_pandas` do DataFrame com backend numpy | 9,7 ms | Parcial: 3,8 MB alocados para uma tabela de 8,6 MB, numa versão de três colunas. | Inferidos: `double` para `float64` e para inteiro com `NaN` (o `NaN` vira nulo), `timestamp[ms]` ou `[ns]` para datas, `large_string` para o `str` do pandas 3, `struct` para uma coluna de `dict`. |

A aritmética do pandas sobre `decimal128[pyarrow]` fica em decimal: `sum` em 1,4 ms e
`groupby().sum()` em 24 ms devolvem `Decimal` e `decimal128(18, 2)`; `valor * 2` dá
`decimal128(38, 2)`, `valor * Decimal("1.1")` dá `decimal128(21, 3)` e `valor * 1.1` dá `double`;
`merge` preserva os dtypes. Sobre `object` de `Decimal`, `sum` levou 14,3 ms. `round(2)` mantém a
escala do dtype, e o cast seguro para `decimal128(18, 2)` então aceita; sem o `round`, recusa com
`Rescaling Decimal value would cause data loss`.

O que a sondagem fixa em `cast`:

- `Table.cast(schema, safe=True)` recusa nulo em campo `nullable=False` (`Casting field ... with
  null values to non-nullable`), estouro de inteiro e escala perdida em decimal, e exige os mesmos
  nomes na mesma ordem: `cast` seleciona e reordena as colunas do contrato presentes e deixa as
  ausentes para o `BY NAME`. Um inteiro numa coluna `Numeric(18, 2)` passa por `decimal128(21, 2)`,
  porque o cast direto pede precisão 21.
- `safe=True` não acusa duas perdas: `double` para `decimal128(18, 2)` arredonda o valor binário
  exato (`1.236` vira `1.24` e `2.675` vira `2.67`, como o `round` do Python) e `timestamp` para
  `date32` descarta a hora. `cast` aceita as duas conversões só quando nada se perde: um `double` é
  representável na escala quando `pc.round(x, 2)` o devolve igual (os 300.000 valores `k/100.0`
  passaram em 1,1 ms, e os 270.000 de `k/1000.0` com terceira casa foram apontados), e um
  `timestamp` cabe em `date` quando a ida e volta o devolve igual; fora disso, recusa com a
  mensagem que pede o arredondamento explícito no cliente. `pc.round` e o cast discordam em
  `2.675` (2,68 contra 2,67), então a carga inicial (etapa 7) arredonda os `Double` dos modelos
  atuais por `pc.round(x, 2)` antes do cast e registra a regra no relatório.
- Um documento JSON é `string` no contrato Arrow, sem a extensão `arrow.json` (decisão do usuário de
  2026-09-20): no pandas com backend pyarrow, o dtype da extensão não tem os kernels de `.str`
  (`utf8_length` e `match_substring_regex` falham com `ArrowNotImplementedError`), nenhum motor a
  devolve, e o DuckDB valida o texto ao carregar numa coluna `JSON` (`Malformed JSON`). O documento
  entra já serializado: uma coluna de `dict` vira `struct` com a união das chaves (`{"k": 1}` volta
  como `{"k": 1, "x": null}`), dicts heterogêneos falham na inferência e `pa.array(dicts,
  pa.string())` falha; `json.dumps` de 300.000 documentos levou 204 ms. `cast` recusa `struct`,
  `list` e `map` numa coluna JSON.
- `large_string` vira `string`; `timestamp[ns]` vira `[us]` quando a parte perdida é zero e é
  recusado quando não é.

## Estado do projeto

### O repositório

| Artefato | Estado |
| --- | --- |
| `docs/` | Completa: `parquet.md`, `duckdb.md`, `redshift.md`, `sqlalchemy.md`, `schema.md`, `delta.md`, `guia.md`, `estrategia.md`, `serialize-db.md` e este plano. Todo bloco Python dos documentos rodou com as versões fixadas em `pyproject.toml`; os comandos do Redshift foram compilados, não executados. |
| `tests/model/` | O modelo de referência: os modelos do pipeline (`model_base_contabil.py`, `model_base_gerencial.py` e `model_db_projetado.py`), movidos do pacote para os testes em 2026-09-20. Ele faz o papel da biblioteca cliente: os testes o entregam à API do pacote como um pipeline entregaria os seus modelos, e o pacote não contém modelo algum. Dois não importam (`from lib_base_contabil import Base` e `from lib_base_gerencial import Base`, módulos que o repositório não tem). Os modelos usam `Double` onde o contrato pede `Numeric(18, 2)`, deixam o `autoincrement` padrão nas chaves inteiras (o `duckdb_engine` emite `SERIAL`, que o DuckDB rejeita), declaram chaves estrangeiras `DEFERRABLE INITIALLY DEFERRED` (o DuckDB descarta a cláusula, o Redshift não a tem) e não têm a coluna `mes`, comentários nem `Table.info["serialize_db"]`. A etapa 1 os corrige. |
| `src/serialize_db/__init__.py` | Só o `main` de exemplo do `uv init`; o pacote não tem outro módulo. `pyproject.toml` não declara dependência de execução; o grupo `dev` fixa pytest, deltalake 1.6.4, DuckDB 1.5.5, PyArrow 25.0.1, boto3, redshift-connector, SQLAlchemy 2.0.54, duckdb-engine 0.17.0, sqlalchemy-redshift 1.0.0 e pandas 3.0.6. |
| `tests/` | `conftest.py` com a regra de autorização e as fixtures dos três alvos (pasta local, bucket, Redshift); nenhum teste do pacote ainda; `test_probes.py` testa as funções puras dos probes com respostas fabricadas, sem rede. `tests/proof_of_concept/` com a prova de conceito da camada Delta nos dois armazenamentos (`poc_delta.py`, `test_local.py`, `test_s3.py`), as suítes de estudo das bibliotecas externas e da stdlib (`test_sqlalchemy.py`, `test_duckdb.py`, `test_pyarrow.py`, `test_deltalake.py`, `test_stdlib.py`, `test_concurrency.py`, `test_parallel.py`), comentadas passo a passo por serem o material de aprendizado de quem dará manutenção, e `test_redshift.py`, os itens Redshift da etapa 0, escrito antes de haver conexão e ainda não executado. Cada etapa abaixo lista as provas de conceito que exercitam as suas APIs. Sem variável, 71 testes passam e 57 são pulados; com a raiz local, 109 passam e 19 são pulados (2026-09-20, macOS). No espaço, em 2026-09-20, uma sessão com a raiz local e a raiz S3 gravou o JSON de `SERIALIZE_DB_TEST_REPORT` com as medições das duas raízes, sem a contagem por resultado nem o registro da limpeza, que o relatório passou a ter (`session.`, `local.cleanup`, `s3.cleanup`); a execução das 04:52 UTC, após as correções desta sessão, registrou 104 testes passados e 7 pulados em 35 s e a limpeza das duas raízes; sem variável, 63 passam e 48 são pulados. |
| `probes/` | Leituras do ambiente, só de leitura: `space.py`, `bucket.py`, `diagnose_aws.py`, `redshift.py` e `catalog.py` sobre `probelib.py`, com o resultado em `probes/output/` para colar na conversa. Quatro execuções no laboratório em 2026-09-20, sem Redshift, corrigiram a leitura das conexões, as leituras de rede, o rótulo dos IPs públicos, a montagem de `~/shared` e o Object Lock, acrescentaram por tabela Delta os arquivos, commits e último objeto, as sessões da suíte S3 e as versões não correntes sob a raiz, e a quarta isolou o 403 do delta-rs (`NO_PROXY` vazia) e fez `diagnose_aws.py` rodar o delta-rs como encontrado e como a suíte; o ambiente de destino ainda não foi lido. |
| `prepare_offline.sh` | Deixa a pasta autossuficiente para o destino sem internet (`.python/`, `.venv/`, `.duckdb/`); verificado extraindo o pacote em outro caminho e rodando a suíte local com proxies mortos. |

### O que a prova de conceito verificou

Em 2026-09-19, no espaço do SageMaker Unified Studio do projeto, contra o bucket do projeto na
mesma região, com deltalake 1.6.4, DuckDB 1.5.5 e PyArrow 25.0.1 (a suíte
`tests/proof_of_concept/test_s3.py`, 10 testes em 27 s):

- O delta-rs encontra as credenciais do contêiner do projeto pela cadeia padrão. Uma falha 403 na
  chamada de credenciais, no início da verificação, foi contornada com `NO_PROXY` em maiúsculas; a
  causa ficou isolada em 2026-09-20 (abaixo). As credenciais do `boto3` em `storage_options`
  funcionam e ficam como reserva. O DuckDB (`credential_chain`) e o `boto3` nunca falharam.
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

Em 2026-09-20, no mesmo espaço, com `NO_PROXY` já presente no ambiente, a sessão inteira (suítes
local e S3) gravou o relatório JSON: as três variantes da cadeia de credenciais do delta-rs passaram
(ambiente como encontrado, `NO_PROXY` exportada, proxies retirados); no bucket, 20 consultas
pontuais em 5,5 s por `delta_scan`, 3,0 s por `ATTACH ... PIN_SNAPSHOT`, 1,2 s por `read_parquet` e
0,017 s na tabela materializada, agregação em 0,40 s fria e 0,26 s quente, `overwrite` em 0,64 s,
`append` em 0,42 s e `vacuum` em 0,45 s; na pasta local do espaço, as mesmas 20 consultas em 0,31 s
por `delta_scan`, 0,25 s por `ATTACH`, 0,19 s por `read_parquet` e 0,016 s na tabela. A regra de
materializar vale nos dois armazenamentos, e no S3 a diferença é de 300 vezes. O `executemany` de
5.000 linhas levou 2,9 s contra 0,002 s por Arrow.

Na quarta execução de 2026-09-20 (04:41 UTC, 103 testes passados e 7 pulados em 34 s, num shell
aberto pela extensão do Claude Code, onde `NO_PROXY` existe vazia), `diagnose_aws.py` reproduziu o
403 do delta-rs e isolou a causa: o cliente HTTP do delta-rs lê `HTTP_PROXY` e `HTTPS_PROXY` nas
duas grafias, mas lê `NO_PROXY` e, só quando ela está ausente, `no_proxy`; vazia, ela anula as
exceções e a chamada ao endpoint de credenciais vai pelo proxy. Com `NO_PROXY` ausente, exportada de
`no_proxy` ou reduzida a `169.254.170.2`, ou sem as variáveis de proxy, a chamada passa. A variante
`as_found` do teste removia `NO_PROXY` em vez de devolvê-la ao valor encontrado, e por isso passava;
o teste agora registra cinco variantes (`as_found`, `no_proxy_exported`, `no_proxy_absent`,
`no_proxy_empty`, `proxy_unset`) e `environment.no_proxy_as_found`, e o probe roda o delta-rs como
encontrado e como a suíte. Sem `AWS_REGION` nem `AWS_DEFAULT_REGION`, o delta-rs foi a
`us-east-1` apesar de `region = us-west-2` no perfil `default`; com `HOME` vazio e `AWS_REGION`,
abriu a tabela: o perfil serve à cadeia de credenciais, não à região. A limpeza da suíte S3 ficou
confirmada: `s3.cleanup` registrou 15
objetos apagados, e `bucket.py` só encontrou a sessão mantida por `SERIALIZE_DB_TEST_KEEP` pela
execução anterior.

Na pasta local, em macOS e no pacote extraído em outro caminho: o commit atômico em disco (dois
escritores na mesma versão: o segundo `overwrite` do mesmo mês falha com `CommitFailedError`; meses
diferentes e `append` mais `append` comitam os dois), os caminhos relativos do log com a realocação
da pasta e a abertura sem variáveis `AWS_*`. Os comportamentos do delta-rs que as etapas assumem
(substituição por predicado, `schema_mode`, `add_columns`, cast no `append`, `restore`, `vacuum`,
`keep_versions`, exportação por mês) foram verificados localmente e estão em `delta.md`.

### O que as leituras do ambiente mostraram

O espaço do SageMaker Unified Studio, lido em 2026-09-19 e quatro vezes em 2026-09-20 pelos probes:
credenciais pelo endpoint do contêiner (`AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`), emitidas por
cerca de uma hora (a expiração 04:19:03 UTC foi lida às 03:23 e às 03:44) e renovadas pelo endpoint;
região `us-west-2` em `AWS_REGION` e `AWS_DEFAULT_REGION`; saída para a rede por
`proxy.awsds.internal:3128`, com `no_proxy` sempre presente e `NO_PROXY` dependente do shell: igual
a `no_proxy` num terminal do Code Editor (03:23 e 03:44 de 2026-09-20), vazia num shell aberto pela
extensão do Claude Code (04:40; a leitura de 2026-09-19, "só a minúscula", não distinguia vazia de
ausente); IMDS bloqueado; `pypi.org` e `github.com` não resolvem, e o `uv` alcança o PyPI
pelo proxy; endpoints VPC de interface para STS, Glue, Athena, KMS, Secrets Manager, DataZone, Lake
Formation e S3 Tables, e nenhum para `redshift`, `redshift-serverless` e `redshift-data`, que
resolvem para IP público; 4 vCPUs, 15,4 GiB, 61 GiB livres em `HOME` e 37 GiB em `/tmp`, `ulimit -n`
99999; `~/shared` é um link para a montagem `fuse.s3fs` (`rw`) do prefixo `shared/` do bucket; o
DuckDB nasce com 4 threads, `memory_limit` 12,3 GiB e `temp_directory` `.tmp`, relativo à pasta
corrente. O bucket do projeto é versionado (pela amostra com `VersionId`), com SSE-KMS pela chave do
projeto e bucket key, SSE-C bloqueado e acesso público bloqueado; o papel não lista buckets
(`ListAllMyBuckets`), não lê versionamento, ciclo de vida, política, propriedade, Object Lock nem
uploads incompletos, não simula políticas nem descreve a chave, e a suíte S3 provou listar, ler,
gravar, copiar e apagar sob a raiz e a chave KMS pela escrita. Conexões do projeto: IAM, três S3
(`dev/`, `shared/` e o lake), Athena, Glue Spark, Spark Connect, Lakehouse e workflows; nenhuma
conexão, cluster ou workgroup Redshift. O Glue tem o banco `mydatabase` com uma tabela Parquet, o
Athena três workgroups, e o Lake Formation e o S3 Tables negam: o gatilho de reavaliação de
`estrategia.md` não disparou. O Python do sistema é 3.12 com deltalake 1.5.0, DuckDB 1.5.4, SQLGlot
28.10.1 e awswrangler 3.17.0.

Consequências no plano: a biblioteca exporta `NO_PROXY` a partir de `no_proxy` quando a maiúscula
está ausente ou vazia, e não depende de
listar buckets nem de ler o versionamento; `storage_options()` resolve as credenciais de novo a cada
chamada, porque uma emissão dura uma hora e a reserva com credenciais fixas expiraria numa execução
longa (etapa 3); o motor DuckDB fixa `temp_directory` numa pasta com espaço conferido, porque o
padrão é relativo à pasta corrente (etapa 4); a etapa 5 conecta por senha por padrão e trata a
autenticação por IAM e a Data API como opcionais, porque dependem das APIs do Redshift, sem endpoint
VPC no laboratório (`RS-14`); e o `vacuum` num bucket versionado só libera espaço com a regra
`NoncurrentVersionExpiration`, que o papel não lê e `BK-14` mede pelo acumulado (etapa 9).

O ambiente definitivo não tem internet e pode não ter proxy, só um endpoint VPC do S3 (declaração do
usuário de 2026-09-19). Consequências, ainda não confirmadas por uma leitura de
`probes/diagnose_aws.py` nesse ambiente: o botocore lê `AWS_DEFAULT_REGION` ou o perfil, nunca
`AWS_REGION`, e sem região usa o endpoint global `s3.amazonaws.com`, que o endpoint VPC regional não
atende; o delta-rs lê as duas variáveis e sem nenhuma cai em `us-east-1`, ignorando a região do
perfil; sem variáveis de proxy ele alcança o endpoint de credenciais diretamente (variante
`proxy_unset`), e a exportação de `NO_PROXY` não muda nada; o STS pode estar inalcançável; nenhuma
extensão do DuckDB pode ser baixada. A biblioteca normaliza a
região nos dois sentidos, não chama o STS e carrega as extensões só da pasta configurada. Se as APIs
do Redshift também não tiverem endpoint lá, resta a conexão por senha na porta 5439, que não passa
por elas (`RS-14`).

### Pendências

- **Redshift.** A prova de conceito espera uma conexão no projeto; os itens estão na etapa 0. A
  primeira leitura de `probes/redshift.py` fixa como a etapa 5 conecta (senha ou IAM, cluster ou
  serverless) e qual papel o `COPY` usa; no laboratório as APIs do Redshift não têm endpoint VPC
  (`RS-14`), então a etapa 5 nasce com a conexão por senha e trata IAM e Data API como opcionais.
- **Versões não correntes.** O bucket é versionado e o papel não lê o ciclo de vida: cada exclusão
  (o `vacuum`, a limpeza da suíte S3) deixa uma versão não corrente invisível à listagem. `BK-14`
  conta o acumulado, e a regra `NoncurrentVersionExpiration` sob a raiz, junto com
  `AbortIncompleteMultipartUpload`, é pergunta para quem administra o bucket.
- **Credenciais de uma hora.** Nenhuma execução mais longa que uma emissão rodou ainda; a etapa 3
  renova `storage_options` a cada chamada, e a primeira execução longa no espaço confirma que o
  delta-rs e o `boto3` renovam pela cadeia padrão.
- **Manutenção da suíte S3.** Se `diagnose_aws.py` confirmar o cenário sem proxy: exportar
  `AWS_DEFAULT_REGION` a partir de `AWS_REGION`, tornar a chamada ao STS opcional com espera curta e
  passar `AWS_ENDPOINT_URL` ao secret do DuckDB. A etapa 3 implementa o mesmo na biblioteca.
- **`Text` no Redshift.** O `sqlalchemy-redshift` compila `Text` como `TEXT`, que o Redshift guarda
  como `VARCHAR(256)`. A etapa 1 emite `VARCHAR(65535)` por uma regra `@compiles(Text, "redshift")`
  em `ddl`, em vez de exigir `String(65535)` nos modelos; a escolha ainda não foi confirmada pelo
  usuário.
- **Barreira por tabela.** Um cliente que dispara `load` numa thread e esquece o `result()` lê o
  estado anterior em silêncio, porque o DuckDB não espera. A guarda: `load` marca a tabela em voo,
  e `query` e `execute` esperam as tabelas em voo que o statement referencia, tiradas por
  `find_tables` do statement Core ou do sentinela `{prefix}` do texto gerado
  (`test_parallel.py::test_table_barrier_delays_the_read_until_the_load_lands`). Fica fora das
  etapas até existir um pipeline paralelo real.

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
- O log é limpo no checkpoint com `delta.logRetentionDuration` de 30 dias por padrão (`delta.md`;
  uma sondagem de 2026-09-19 com `interval 0 days`, seis commits e um checkpoint manteve a versão 0
  legível, então a limpeza não vira asserção); a tabela nasce com `interval 3650 days`,
  `delta.deletedFileRetentionDuration` fica em `interval 400 days`, e `keep_versions` protege os
  snapshots do banco.
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
- O delta-rs não lê a região de `~/.aws/config`: com `region = us-west-2` no perfil `default` e
  sem `AWS_REGION` nem `AWS_DEFAULT_REGION`, foi a `us-east-1` (2026-09-20); a cadeia de credenciais
  consulta o perfil (`credential_source = EcsContainer`, aviso `aws_config::profile::credentials`)
  mas encontra o contêiner sem ele (`HOME` vazio e `AWS_REGION` bastaram). A região precisa estar
  em `AWS_REGION` ou `AWS_DEFAULT_REGION`; as credenciais vêm do ambiente, do contêiner, do IMDS ou
  de `storage_options`. `storage_options` leva `max_retries` e `retry_timeout` para uma rede morta
  falhar em 10 s em vez de 59 s (`README.md`, `delta.md`).
- O cliente HTTP do delta-rs lê `HTTP_PROXY` e `HTTPS_PROXY` nas duas grafias e `NO_PROXY` antes de
  `no_proxy`; vazia, `NO_PROXY` anula as exceções e a chamada ao endpoint de credenciais vai pelo
  proxy (403). `prepare_environment` exporta `NO_PROXY` de `no_proxy` quando a maiúscula está
  ausente ou vazia, antes da primeira abertura de tabela; exportar depois do `import deltalake`
  basta, porque a suíte importa na coleta e exporta na fixture da sessão (`delta.md`).
- O DuckDB carrega extensões só da pasta configurada, com `autoinstall_known_extensions` e
  `autoload_known_extensions` desligados: o `LOAD` de uma extensão conhecida baixaria a extensão
  para `~/.duckdb` sem aviso, e o destino não tem internet (`README.md`).
- Um campo JSON é `string` no esquema Arrow do contrato, sem a extensão `arrow.json`, `string` no
  Delta e texto nos arquivos; `JSON` no DuckDB e `SUPER` no Redshift são tipos do motor, aplicados na
  carga; a auditoria confere `json_valid` antes de publicar, porque nem o Arrow nem o Delta validam
  (`schema.md`).
- O SQLGlot transpila funções, não garante suporte; os testes de integração no Redshift continuam
  (`estrategia.md`).
- O pacote `sagemaker-studio` fica fora do projeto: arrasta `deltalake`, `duckdb` e `pandas` sem
  versão fixa, e numa instalação de teste rebaixou o `duckdb` para 1.5.1. A raiz do banco é
  configuração explícita, nunca inferida do projeto (`probes/README.md`).
- A fronteira com o código cliente é a `pa.Table`: `load` recebe uma, `query` e `execute` devolvem
  uma, e nenhuma primitiva pública recebe ou devolve DataFrame, lista de linhas ou instância ORM;
  os `RecordBatchReader` ficam nas primitivas internas, e a conexão não roda outro comando enquanto
  um leitor é consumido (seção "A troca de dados com o código cliente").
- A API é síncrona: toda primitiva bloqueia até o efeito estar visível para a chamada seguinte, de
  qualquer thread, com autocommit por comando nos dois motores, e nenhuma é `async`, porque nenhum
  dos quatro drivers tem API assíncrona em Python. O paralelismo é do código cliente, com
  `concurrent.futures`, e `Future.result()` expressa a dependência entre um `load` e a leitura que o
  segue; a biblioteca não tem scheduler nem grafo de tarefas. O DuckDB, o delta-rs e o PyArrow
  liberam o GIL no trabalho nativo, então threads bastam, e uma extensão em Rust não entra por
  paralelismo (`test_concurrency.py`, `serialize-db.md`, seção "Paralelismo").
- O motor guarda uma conexão por thread: `duckdb` e `redshift_connector` declaram `threadsafety` 1,
  uma conexão DuckDB compartilhada entrega a uma thread o resultado da outra sem erro, um banco em
  memória só é compartilhado por `cursor()` da conexão que o abriu, um segundo `connect(arquivo)`
  com outra configuração ou `read_only` é recusado, e `threads` é da instância. Cada thread recebe
  um `cursor()` do DuckDB ou uma conexão Redshift num `threading.local`, criados no primeiro uso e
  fechados em `cleanup`; `run.sandbox.connection` expõe a conexão crua da thread; o estado mutável
  de `Execution` fica sob lock; o cliente não cria conexão para o sandbox, e a biblioteca não cria
  `Engine` do SQLAlchemy (`test_concurrency.py`, `test_parallel.py`).
- Uma chamada nativa que solta e retoma o GIL ao lado de uma thread em Python puro espera o
  intervalo de troca a cada retomada: 200 `os.stat` levaram 0,3 s contra 0,2 ms, e o
  `import pyarrow.dataset` que `pq.read_table` faz na primeira chamada levou 15 s contra 0,19 s. A
  biblioteca importa seus módulos na abertura, e o cliente não roda laços Python puros ao lado das
  threads da biblioteca que fazem chamadas curtas, como o `redshift_connector` lendo pelo socket e o
  `boto3`; `sys.setswitchinterval` é o ajuste (`test_concurrency.py`).
- As chaves inteiras vêm de `run.next_ids(table, n)`: faixas contíguas sob lock, a partir de
  `max_key + 1` na versão fixada, lido de `max.<coluna>` das ações `add` e pela varredura da coluna
  quando um arquivo não tem a estatística; a tabela vazia começa em 1. Os ids de uma reexecução
  diferem, e a unicidade continua na auditoria; `publish` confere que a versão da tabela ainda é a
  fixada e aborta com `ExecutionConflict`, para que duas execuções abertas na mesma versão não
  publiquem a mesma faixa (`test_parallel.py`).

## Organização do pacote

| Módulo | Etapa | Conteúdo |
| --- | --- | --- |
| `serialize_db.schema` | 1 | O esquema a partir dos modelos: Arrow, Delta, DDL por dialeto, opções físicas, cast seguro, arquivos gerados. |
| `serialize_db.sql` | 2 | O texto SQL por dialeto a partir de statements Core: parâmetro, prefixo, renderização, arquivos gerados. |
| `serialize_db.storage` | 3 | Os dois armazenamentos atrás de uma interface: URIs, leitura e escrita condicional, cópia, listagem, `storage_options` e o secret do DuckDB. |
| `serialize_db.delta` | 3 | A camada Delta: criação, publicação por mês, registro de arquivos, reconciliação, reescrita, manifesto, diferença de versões, snapshots, `vacuum`, compactação, cópia profunda, exportação. |
| `serialize_db.audit` | 4 | As verificações derivadas do contrato: chaves, nulos, limites de tipo, JSON e totais; o texto SQL por dialeto e o `AuditReport`. |
| `serialize_db.engine` | 4 e 5 | O protocolo `Engine` e os motores `duckdb` e `redshift`, com a mesma interface. |
| `serialize_db.execution` | 6 | `Database` e `Execution`, o ciclo de uma execução. |
| `serialize_db.load` | 7 | A carga inicial dos Parquet atuais. |
| `serialize_db.cli` | 6 a 9 | `serialize-db run`, `schema`, `sql`, `audit`, `load`, `publish`, `snapshot`, `vacuum`, `compact`, `archive`, `export` e `history`. |

Dependências: `pyproject.toml` passa a declarar as de execução, `sqlalchemy`, `deltalake`, `duckdb`,
`pyarrow` e `boto3`, nas versões fixadas pelos documentos, mais `duckdb-engine` e
`sqlalchemy-redshift` enquanto houver compilação em tempo de execução (hoje só no grupo `dev`, para
as suítes de estudo); `redshift-connector` entra no extra `redshift`, e `sqlglot` no grupo `dev`; o pandas
fica no grupo `dev`, para o teste do ciclo com `ArrowDtype`, porque a biblioteca não o importa.
`prepare_offline.sh` passa a instalar os extras
(`--all-extras`) e é rodado de novo a cada mudança.

Configuração: argumentos explícitos de `Database` e da linha de comando, com as variáveis
`SERIALIZE_DB_ROOT`, `SERIALIZE_DB_ENVIRONMENT`, `SERIALIZE_DB_ENGINE`,
`SERIALIZE_DB_DUCKDB_EXTENSIONS` e `SERIALIZE_DB_REDSHIFT_*` (as de `probes/redshift.py`) como
padrão; nenhum arquivo de configuração.

Testes: `tests/` na raiz testa o pacote, um módulo de teste por módulo do pacote;
`tests/proof_of_concept/` guarda as provas de conceito e os testes das bibliotecas externas,
comentados passo a passo porque também são o material de estudo das APIs; `tests/model/` é o modelo
de referência, que faz o papel da biblioteca cliente: os testes do pacote o entregam à API como um
pipeline entregaria os seus modelos. Um teste que não grava (esquema, renderização, DuckDB em memória) roda sem variável; um teste que
grava usa a fixture `local_location`, sob `SERIALIZE_DB_TEST_LOCAL_ROOT`, e é pulado sem ela, e o
fim da sessão imprime, sem erro, o comando que autoriza cada suíte pulada e o que ela grava; o
marcador `s3` repete no bucket os testes que dependem do armazenamento, sob
`SERIALIZE_DB_TEST_S3_ROOT`; o marcador `redshift` roda só com `SERIALIZE_DB_TEST_REDSHIFT_SCHEMA`,
o esquema onde a suíte pode criar tabelas `serialize_db_test_<id>_*`, e conecta pelas variáveis
`SERIALIZE_DB_REDSHIFT_*`. Os arquivos gerados do modelo de referência (`tests/model/schema/` e
`tests/model/sql/`, o que um pipeline versionaria na raiz do seu repositório) são comparados por
teste com uma geração nova, sem gravar.

## Etapas

Cada etapa entrega um módulo com testes. As etapas 1 a 4 e 6 rodam em pastas locais, sem AWS; a
etapa 5 e a parte Redshift da etapa 0 exigem a conexão; a etapa 7 exige os Parquet de origem.

| Etapa | Entrega | Critério de aceite |
| --- | --- | --- |
| 0. Prova de conceito na AWS | `tests/proof_of_concept/`: S3 verificado, Redshift pendente. | Cada item respondido em `delta.md` e `redshift.md`; nenhum bloqueio sem alternativa. |
| 1. `schema` | Modelo de referência corrigido; esquema Arrow, Delta e DDL; cast; os arquivos `schema/` do modelo de referência. | `create_all` no DuckDB em memória passa; o teste de diff falha quando um modelo muda sem regenerar; `cast` recusa perda de precisão, `double` fora da escala, texto longo e nulo em `NOT NULL`. |
| 2. `sql` | `param`, `prefixed`, `render`, `bind`, `write_sql_files`. | O texto de um statement com parâmetro, `%` em literal e prefixo roda no DuckDB com `$nome`; o teste de diff dos arquivos `sql/`. |
| 3. `storage` e `delta` | Os dois armazenamentos; a camada Delta inteira. | Testes locais de substituição do mês, conflito, reconciliação aditiva e destrutiva, reescrita num commit, `keep_versions`, exportação por mês e realocação; os mesmos no bucket com `-m s3`. |
| 4. `audit` e motor DuckDB | As verificações do contrato e seu texto por dialeto; conexão, ingestão, consulta, execução de texto, carga, auditoria, exportação do mês. | O pipeline de exemplo roda em memória sobre um Delta local; a auditoria reprova a chave repetida entre o mês novo e um mês já publicado. |
| 5. Motor Redshift | O mesmo protocolo com sandbox `exec_<id>_`, `COPY ... MANIFEST` e `UNLOAD`. | SQL gerado coberto por testes sem cluster; integração com amostra, marcador `redshift`. |
| 6. Execução e linha de comando | `Database`, `Execution`, `serialize-db run`. | Reexecução idempotente; auditoria reprovada não altera o Delta; conflito abortado com mensagem. |
| 7. Carga inicial | Migração dos Parquet atuais por tabela e por mês, com relatório. | Contagens e somas por mês iguais entre origem e Delta. |
| 8. Publicação para clientes | Tabelas `<ambiente>_*` no Redshift, `version_diff`, transação única, `serialize_db_publications`. | Um mês alterado recarrega só esse mês. |
| 9. Operação | Snapshots, `vacuum`, compactação, arquivo, exportação, `history`, runbook, `pdoc`. | Runbook escrito e testes de manutenção passando. |

O plano de cada etapa, com as primitivas do módulo, os testes e as provas de conceito, está num
arquivo próprio:

- [Etapa 0: prova de conceito na AWS](PLAN-STAGE-0.md)
- [Etapa 1: `schema`](PLAN-STAGE-1.md)
- [Etapa 2: `sql`](PLAN-STAGE-2.md)
- [Etapa 3: `storage` e `delta`](PLAN-STAGE-3.md)
- [Etapa 4: `audit` e motor DuckDB](PLAN-STAGE-4.md)
- [Etapa 5: motor Redshift](PLAN-STAGE-5.md)
- [Etapa 6: execução e linha de comando](PLAN-STAGE-6.md)
- [Etapa 7: carga inicial](PLAN-STAGE-7.md)
- [Etapa 8: publicação para clientes](PLAN-STAGE-8.md)
- [Etapa 9: operação](PLAN-STAGE-9.md)

## Pipeline de atualização mensal

Uma execução de exemplo: `exec-2026-09-05`, ambiente `prod`, motor DuckDB, mês de referência
`2026-08`. As entradas são `cad_lancamentos` (os doze meses até 2026-08), `cad_contratos`,
`cad_operacoes`, `rel_contrato_operacao` e as tabelas `dom_*`; a saída ilustrativa é
`cad_lancamentos` do banco projetado, mês 2026-08. Os números de versão são ilustrativos.

| Passo | O que acontece | Artefatos |
| --- | --- | --- |
| 1. Abertura | Lê `_serialize_db/snapshots.json` e `serialize_db_publications`; abre cada tabela de entrada e registra a versão. | `versions = {cad_lancamentos: 143, cad_contratos: 88, ...}` gravado no log da execução. |
| 2. Ingestão | DuckDB: views com os nomes dos modelos sobre `delta_scan(uri, version := 143)`; `cad_lancamentos` materializada com `WHERE mes BETWEEN '2025-09' AND '2026-08'`; dimensões como views. Redshift: `COPY ... MANIFEST` dos arquivos desses meses em `exec_2026_09_05_cad_lancamentos`, via staging. | Sandbox em `/tmp/exec-2026-09-05.duckdb` ou tabelas com prefixo no esquema único. |
| 3. Execução | O pipeline roda statements Core, texto gerado e lógica Python sobre o sandbox; o que sai para o Python sai como `pa.Table` por `query` ou `execute` e volta por `load`; intermediários ficam no sandbox. | Tabela `cad_lancamentos_projetados` no sandbox, mês 2026-08. |
| 4. Auditoria | Contagem, nulos, unicidade da chave contra os demais meses da versão 57, `mes = strftime(data_ref, '%Y-%m')`, limites de tipo, `json_valid`, totais de controle. | Relatório com o SQL de cada verificação no log da execução; reprovação encerra sem tocar o Delta. |
| 5. Publicação no Delta | `reconcile` e `publish_month(uri, "2026-08", data, commit_metadata(...))`; do Redshift, `UNLOAD ... PARTITION BY (mes)` mais `register_files`. | `cad_lancamentos` projetado passa da versão 57 para 58; um arquivo em `mes=2026-08/`. |
| 6. Publicação no Redshift | `version_diff(57, 58)` aponta o mês 2026-08; `DELETE` do mês, `COPY ... MANIFEST` na staging, `INSERT ... SELECT *, '2026-08'`; controle atualizado. | `prod_cad_lancamentos_projetados` com o mês novo; `serialize_db_publications` em 58. |
| 7. Snapshot do banco | Só na execução marcada, por exemplo a do fim do trimestre: `serialize_db_snapshot = "2026T3"` nos commits e a entrada em `_serialize_db/snapshots.json`. | Versões do snapshot protegidas por `keep_versions`. |
| 8. Encerramento | Sandbox descartado, staging apagado, resumo no log. | Execução idempotente: repetir os passos 5 e 6 reproduz o mesmo estado. |

A API da etapa 6, ainda não implementada:

```python
import pandas as pd
import pyarrow as pa
from sqlalchemy import select

from serialize_db import Database, Execution
from pipeline import compute_in_sandbox, project
from pipeline.models import Contrato, Lancamento, LancamentoProjetado, Operacao, RelContratoOperacao

db = Database("s3://bucket/projeto/delta", environment="prod")
with Execution(db, engine="duckdb", month="2026-08", execution_id="exec-2026-09-05") as run:
    run.ingest(Lancamento, months=run.previous_months(12), materialize=True)
    run.ingest(Contrato, Operacao, RelContratoOperacao)              # views sobre a versão fixada
    compute_in_sandbox(run.sandbox)                                  # statements Core e texto gerado, por query e execute
    entries = run.sandbox.query(select(Lancamento).where(Lancamento.mes == "2026-08"))   # pa.Table
    frame = entries.to_pandas(types_mapper=pd.ArrowDtype)            # sem cópia; decimal128 e date32 mantidos
    projected = project(frame)                                       # lógica Python; devolve um DataFrame
    projected["id_lancamento"] = run.next_ids(LancamentoProjetado, len(projected))   # faixa contígua, sob lock
    run.sandbox.load(LancamentoProjetado, pa.Table.from_pandas(projected, preserve_index=False))
    run.audit(LancamentoProjetado, months=["2026-08"])               # exigida por publish
    run.publish(LancamentoProjetado, months=["2026-08"])             # overwrite por mês, metadados
    run.publish_redshift(LancamentoProjetado)                        # só os meses alterados
```

O DataFrame que `project` devolve mantém os dtypes `ArrowDtype` de `frame`, e `from_pandas` os
devolve ao Arrow sem cópia; uma coluna calculada em `float64` chega como `double`, e `load` a recusa
numa coluna `Numeric` enquanto houver valor fora da escala, até o pipeline arredondar.

Uma reexecução com o mesmo `execution_id` repete os `overwrite` dos mesmos meses e produz as mesmas
linhas; os ids podem diferir, porque `next_ids` recomeça do máximo da versão fixada. Uma correção de um mês antigo é a mesma chamada com outro `month` e um `execution_id` novo:
`publish_redshift` recarrega só esse mês, e as versões intermediárias entre snapshots do banco saem
no `vacuum` mensal. A execução no Redshift é o mesmo ciclo com `engine="redshift"`: o sandbox são as
tabelas `exec_<id>_*`, a ingestão é `COPY ... MANIFEST`, e a publicação sai por `UNLOAD` mais
`register_files`, sem passar pela máquina local.

## Ordem do trabalho

1. Etapas 1 e 2, em pastas locais, com o modelo de referência corrigido e os seus arquivos
   `schema/` e `sql/` versionados em `tests/model/`.
2. Etapa 3, depois 4 e 6: um pipeline completo em disco local, o critério de aceite da etapa 6
   sobre o motor DuckDB.
3. Em paralelo, no ambiente de destino: os probes (no laboratório rodaram três vezes em 2026-09-20,
   e as suítes local e S3 uma vez); a manutenção da suíte S3 se confirmada;
   `tests/proof_of_concept/` e os testes `-m s3` das etapas 3 e 4 no bucket.
4. Quando `probes/redshift.py` mostrar a conexão: o `test_redshift.py` da etapa 0, depois as etapas
   5 e 8.
5. Etapa 7 quando os Parquet de origem estiverem acessíveis; etapa 9 por último, com o runbook.
