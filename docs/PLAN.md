# Plano de implementação

Este documento registra o que a biblioteca é e como ela chega lá: as decisões, o estado do projeto,
as etapas de implementação com as primitivas de cada módulo e o pipeline de atualização mensal. As
razões das decisões e as comparações entre ferramentas estão em [`estrategia.md`](estrategia.md); o
comportamento verificado do Delta, em [`delta.md`](delta.md); os motores, em [`duckdb.md`](duckdb.md)
e [`redshift.md`](redshift.md); o esquema a partir dos modelos, em [`schema.md`](schema.md) e
[`sqlalchemy.md`](sqlalchemy.md); as funcionalidades, os metadados próprios e o fluxo de cada caso de
uso, em [`serialize-db.md`](serialize-db.md). O estado descrito aqui é o de 2026-09-20.

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
| `tests/` | `conftest.py` com a regra de autorização e as fixtures dos três alvos (pasta local, bucket, Redshift); nenhum teste do pacote ainda; `test_probes.py` testa as funções puras dos probes com respostas fabricadas, sem rede. `tests/proof_of_concept/` com a prova de conceito da camada Delta nos dois armazenamentos (`poc_delta.py`, `test_local.py`, `test_s3.py`), as suítes de estudo das bibliotecas externas e da stdlib (`test_sqlalchemy.py`, `test_duckdb.py`, `test_pyarrow.py`, `test_deltalake.py`, `test_stdlib.py`), comentadas passo a passo por serem o material de aprendizado de quem dará manutenção, e `test_redshift.py`, os itens Redshift da etapa 0, escrito antes de haver conexão e ainda não executado. Cada etapa abaixo lista as provas de conceito que exercitam as suas APIs. Sem variável, 64 testes passam e 48 são pulados; com a raiz local, 94 passam e 18 são pulados (2026-09-20, macOS). No espaço, em 2026-09-20, uma sessão com a raiz local e a raiz S3 gravou o JSON de `SERIALIZE_DB_TEST_REPORT` com as medições das duas raízes, sem a contagem por resultado nem o registro da limpeza, que o relatório passou a ter (`session.`, `local.cleanup`, `s3.cleanup`); a execução das 04:52 UTC, após as correções desta sessão, registrou 104 testes passados e 7 pulados em 35 s e a limpeza das duas raízes; sem variável, 63 passam e 48 são pulados. |
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
uma tabela. A etapa começa pelo modelo de referência de `tests/model/`, corrigido como a biblioteca
cliente o escreveria: `Base` importável de um módulo só, `Numeric(18, 2)` nas colunas monetárias, `autoincrement=False` nas chaves inteiras,
chaves estrangeiras sem `DEFERRABLE`, a coluna `mes` (`String(7)`) nas tabelas particionadas,
comentários de tabela e de coluna, e `Table.info["serialize_db"]` com `partition_by`, `sort_key` e
`redshift`, como em `schema.md`.

| Primitiva | O que faz |
| --- | --- |
| `arrow_schema(table)` | O `pa.Schema` do `Table`: os tipos da tabela de `schema.md`, a nulidade, o comentário de cada coluna em `metadata` do campo, `PARQUET:field_id`; um campo JSON é `string`, sem a extensão `arrow.json`. |
| `delta_schema(table)` | O `deltalake.Schema` derivado do Arrow: `decimal(18,2)`, `timestamp_ntz` para `DateTime` sem fuso e `timestamp` para o com fuso, `string` para JSON e UUID, comentários preservados. |
| `ddl(table, dialect, prefix="")` | `CREATE TABLE` para `duckdb` ou `redshift`: sem `DEFERRABLE` nem `Identity`, `CHECK` só no DuckDB, chaves só quando `table_options` as pede, `SORTKEY`, `DISTSTYLE` e `DISTKEY` no Redshift, `Text` como `VARCHAR(65535)`, `Uuid` como `VARCHAR(36)`, `JSON` como `SUPER`; `prefix` renomeia a tabela para o sandbox. |
| `table_options(table)` | O `TableOptions` (`partition_by`, `sort_key`, `redshift`, `keys`) lido de `Table.info["serialize_db"]` com os padrões da biblioteca: partição por `mes` quando a coluna existe e as chaves do próprio modelo, `table.primary_key` e os `UniqueConstraint`; `keys` só acrescenta uma chave de negócio ou exclui uma delas, sempre de forma explícita. |
| `cast(data, table)` | A `pa.Table`, ou o `RecordBatchReader` lote a lote, convertida para `arrow_schema(table)` com `safe=True` e devolvida no mesmo tipo: as colunas do contrato presentes, na ordem do contrato; `large_string` para `string`, timestamps a microssegundos e UTC, inteiro em `Numeric` por `decimal128(21, 2)`; recusa perda de precisão, `double` fora da escala e `timestamp` com hora numa coluna `Date` (as duas perdas que `safe=True` não acusa), `struct` numa coluna JSON, texto acima de `String(n)` e nulo em coluna `NOT NULL`, com a mensagem que diz o que o cliente faz antes de chamar. |
| `check_models(metadata)` | A lista de violações do contrato nos modelos: tipo fora da tabela de tipos, `Double` em coluna monetária, `autoincrement` em chave inteira, `DEFERRABLE`, `Identity`, tabela particionada sem `mes`, tabela sem chave primária e sem `keys`, coluna sem comentário. Vazia nos modelos corrigidos. |
| `schema_files(metadata)` | `{"<tabela>.delta.json": ..., "<tabela>.duckdb.sql": ..., "<tabela>.redshift.sql": ...}` em memória. |
| `write_schema_files(metadata, directory)` | Grava `schema_files` em `directory`; `serialize-db schema write` grava e `serialize-db schema check` compara sem gravar. |

Testes: `tests/test_schema.py`, sem gravar, sobre o modelo de referência e sobre um modelo de teste
com todos os tipos; `create_all` no DuckDB em memória com o DDL de cada modelo; o diff dos arquivos
`tests/model/schema/` versionados; `write_schema_files` sob a raiz local. Dependências: `sqlalchemy`, `pyarrow`,
`deltalake`, `duckdb`, `duckdb-engine` e `sqlalchemy-redshift`. Provas de conceito:
`test_sqlalchemy.py` (`test_declarative_model_exposes_table`, `test_ddl_per_dialect`,
`test_create_all_and_reflection`, `test_arrow_and_delta_schema_from_table`, com o mapa de tipos e
`Schema.from_arrow().to_json()`, `test_sandbox_copy_of_table_and_schema_files_diff`),
`test_pyarrow.py` (`test_schema_metadata_and_from_pylist`, `test_safe_cast_refuses_data_loss`, com as
perdas que o cast seguro não acusa, e `test_arrow_table_round_trips_through_pandas_without_copy`) e
`test_stdlib.py` (`test_generated_files_diff`, `test_decimal_totals`).

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
valor e parâmetro faltante como erros; o diff de `tests/model/sql/`. Opcional: `sqlglot.parse_one(texto,
dialect)` como teste de que o texto do Redshift analisa. Provas de conceito:
`test_sqlalchemy.py` (`test_generated_sql_text_per_dialect`, `test_redshift_dialect_compiles_dml`) e
`test_stdlib.py::test_generated_files_diff`.

### Etapa 3: `storage` e `delta`

`serialize_db.storage` esconde a diferença entre a pasta local e o S3; é a divisão de
`tests/conftest.py` (`LocalLocation`, `S3Location`) levada à biblioteca.

| Primitiva | O que faz |
| --- | --- |
| `Storage.for_uri(uri)` | `LocalStorage` para um caminho ou `file://`, `S3Storage` para `s3://bucket/prefixo`. |
| `join(*parts)`, `exists(path)`, `list_files(prefix, suffix)`, `delete(paths)` | Caminhos relativos à raiz; a listagem exclui `_delta_log/`. |
| `read_text(path)`, `write_text(path, text, if_match=None, if_none_match=False)` | Escrita condicional: `IfMatch` e `IfNoneMatch` no S3 (412 vira `ConflictError`); `O_EXCL` e `os.replace` na pasta local. É a escrita de `_serialize_db/snapshots.json`. |
| `copy(source, destination)` | `CopyObject` no S3, `shutil.copy2` na pasta local; a exportação sem ler dados. |
| `storage_options()` | As opções do delta-rs: região, `AWS_ENDPOINT_URL`, `max_retries`, `retry_timeout`, `timeout`, as chaves de SSE quando configuradas, e as credenciais do `boto3` só na reserva; resolvidas a cada chamada, nunca guardadas, porque as credenciais do contêiner duram cerca de uma hora. |
| `duckdb_setup(connection)` | `LOAD httpfs; LOAD delta; LOAD aws` e o secret `credential_chain` com `REGION` e `ENDPOINT`; só `LOAD delta` na pasta local. |
| `prepare_environment()` | Exporta `NO_PROXY` a partir de `no_proxy` quando a maiúscula está ausente ou vazia, copia a região entre `AWS_REGION` e `AWS_DEFAULT_REGION` nos dois sentidos, respeita `AWS_ENDPOINT_URL`; devolve o que mudou, para o log. Chamada por `Database`. |

`serialize_db.delta` é a camada de tabela; `uri` é a pasta da tabela, `table` o `Table` do modelo,
`data` uma `pa.Table` ou um `RecordBatchReader`.

| Primitiva | O que faz |
| --- | --- |
| `create_table(uri, table)` | `DeltaTable.create(mode="ignore")` com `delta_schema`, `partition_by`, nome, descrição e as propriedades `delta.logRetentionDuration = interval 3650 days` e `delta.deletedFileRetentionDuration = interval 400 days`; sem vetores de exclusão nem column mapping. |
| `open(uri, version=None)` | A `DeltaTable` numa versão; a execução abre cada tabela uma vez e guarda a versão. |
| `commit_metadata(execution_id, input_versions, snapshot=None)` | O dicionário de `CommitProperties(custom_metadata=...)`: `serialize_db_execution_id`, `serialize_db_input_versions` e `serialize_db_snapshot`. |
| `publish_month(uri, month, data, metadata)` | `write_deltalake(mode="overwrite", predicate="mes = '<mes>'")` de `data` já passado por `cast`; `CommitFailedError` sobe como `ExecutionConflict`. |
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
condicional do arquivo de controle. Provas de conceito: `test_stdlib.py` (`test_storage_uris`,
`test_exclusive_create_atomic_replace_and_fingerprint`, `test_json_control_file_and_commit_metadata`,
`test_group_log_actions_by_month`, `test_prepare_environment`), `test_s3.py` (`test_conditional_put`,
`test_boto3_list_copy_delete`, `test_delta_rs_storage_options_fallback`), `test_local.py`
(`test_commit_is_atomic_on_disk`, `test_folder_relocates`) e `test_deltalake.py` inteiro: criação
idempotente, predicado e nulidade, evolução com `drop_column_not_null` (recebe o nome da coluna),
`restore`, `AddAction`, `vacuum`, `version_diff`, compactação e checkpoint, exportação por cópia e a
reescrita pelo `COPY ... APPEND true, FILENAME_PATTERN, RETURN_STATS` do DuckDB registrada num
commit `overwrite` com esquema novo e estatísticas tipadas, que o DuckDB usa para podar.

### Etapa 4: `audit` e motor DuckDB

`serialize_db.audit` monta as verificações a partir do contrato e não sabe qual motor as roda;
`serialize_db.engine` declara o protocolo `Engine`, que a execução também não conhece, e
`serialize_db.engine.duckdb` o implementa. O protocolo fixa os tipos da fronteira: `load` recebe uma
`pa.Table`, `query` e `execute` devolvem uma `pa.Table`, e os `RecordBatchReader` ficam nas
primitivas internas.

Chave primária, unicidade e chave estrangeira ficam fora do DDL dos dois sandboxes (`schema.md`), e
a auditoria é onde elas são aplicadas. Cada verificação sai do `Table`, sem declaração adicional no
modelo:

| Verificação | De onde sai | Escopo |
| --- | --- | --- |
| Nulo em coluna `NOT NULL` | `column.nullable` | Os meses da execução. |
| Chave repetida | `table.primary_key` e os `UniqueConstraint`, mais o que `keys` acrescenta | Os meses da execução quando as colunas da chave incluem a coluna de partição; a tabela inteira quando não incluem. |
| Órfão de chave estrangeira | `table.foreign_keys` | Só com `foreign_keys=True`; a tabela referenciada entra na versão fixada pela execução. |
| `mes` fora de `data_ref` | a coluna de partição de `table_options` | Os meses da execução. |
| Texto acima de `String(n)` e valor fora do `Numeric(18, 2)` | os tipos de `schema.md` | Os meses da execução. |
| Documento JSON inválido | as colunas JSON, que nem o Arrow nem o Delta validam | Os meses da execução. |
| Totais de controle | as colunas `Numeric` | Os meses da execução. |

Uma chave primária não é mensal: conferi-la só nos meses da execução não é unicidade. Quando as
colunas da chave não incluem a coluna de partição, a verificação compara o sandbox com os demais
meses da versão fixada — `delta_scan(uri, version := v) WHERE mes NOT IN (...)` no DuckDB, uma
staging só com as colunas da chave, carregada por `COPY ... MANIFEST`, no Redshift. Custa uma
passagem nas colunas da chave da tabela inteira; `key_scope="month"` a reduz aos meses da execução,
e a escolha entra no relatório.

A chave estrangeira precisa da tabela referenciada, e só o que o pipeline usa é ingerido:
`foreign_keys=True` ingere a coluna referenciada das tabelas que faltarem no sandbox, na versão
fixada, e o anti-join roda contra ela. Sem o argumento, órfão nenhum é procurado, e o relatório
registra a verificação como não executada.

| Primitiva | O que faz |
| --- | --- |
| `checks(table, months=None, foreign_keys=False, key_scope=None)` | A lista de `Check` (nome, statement Core, o que reprova): os defeitos de linha num `count(*) FILTER` por coluna na mesma passagem, uma consulta por chave e uma por chave estrangeira. |
| `audit_sql(table, dialect, **opcoes)` | `{nome: texto}` por `sql.render`, sem conexão e sem motor: o SQL que a auditoria vai rodar, para depuração. |
| `audit_files(metadata, **opcoes)` e `write_audit_files(metadata, directory, **opcoes)` | `{"<tabela>.audit.duckdb.sql": ..., "<tabela>.audit.redshift.sql": ...}` em memória e gravados, como os arquivos de `schema` e de `sql`; versionar a pasta é opcional e faz um modelo alterado aparecer no diff. |
| `AuditReport` | Por verificação: nome, o SQL rodado, a contagem de defeitos, uma amostra das linhas reprovadas e o veredito; `passed` é a conjunção, e `report.sql()` devolve o texto de todas. |

| Primitiva | DuckDB |
| --- | --- |
| `connect(config)` | Banco em arquivo `<pasta temporária>/<execution_id>.duckdb` ou em memória; `extension_directory` de `SERIALIZE_DB_DUCKDB_EXTENSIONS` (ou `.duckdb/` da pasta preparada), `autoinstall_known_extensions` e `autoload_known_extensions` desligados; `storage.duckdb_setup`; `threads`, `memory_limit`, `temp_directory` e `preserve_insertion_order = false`; `temp_directory` sempre explícito, com o espaço livre conferido e registrado no log, porque o padrão `.tmp` é relativo à pasta corrente. |
| `ingest(table, uri, version, months=None, materialize=False)` | View com o nome do modelo sobre `delta_scan(uri, version := v)`, ou `CREATE TABLE ... AS SELECT ... FROM delta_scan(...) WHERE mes IN (...)` com `materialize=True`. |
| `query(statement, **params)` | O statement Core compilado para o dialeto, com as tabelas do contrato trocadas pelas do sandbox por `sql.prefixed`, e executado na conexão crua, sem `Session`; o resultado é a `pa.Table` de `to_arrow_table()`, com os tipos do motor (`decimal128(18, 2)`, `date32`, JSON como `string`). |
| `execute(sql, params)` | O texto gerado por `render` ou escrito pelo pipeline: `{prefix}` vira vazio, `:nome` vira `$nome` por `sql.bind`, e o resultado volta como `pa.Table` por `to_arrow_table()`; um comando sem resultado devolve a tabela `Count` ou `Success` do DuckDB. |
| `load(table, data)` | `cast(data, table)` e `INSERT ... BY NAME SELECT * FROM <tabela Arrow registrada>` numa tabela do sandbox criada por `ddl(table, "duckdb")`; `data` é uma `pa.Table`, e outro tipo é recusado com a mensagem que aponta `pa.Table.from_pandas`. |
| `audit(table, months, **opcoes)` | Roda o texto de `audit.audit_sql(table, "duckdb")` — `json_valid` e `strftime(data_ref, '%Y-%m')` são as funções do dialeto — e monta o `AuditReport`; a comparação com os demais meses sai de `delta_scan` na versão fixada, e `passed` falso interrompe a execução. |
| `export_month(table, month)` | O `RecordBatchReader` do mês, passado por `cast`, para `publish_month`, sem outro comando na conexão até o fim da escrita; ou `COPY ... TO '<uri>/mes=<mes>/<execution_id>.parquet' (FORMAT parquet, RETURN_STATS)` mais `register_files` para a tabela que não cabe na memória. |
| `cleanup()` | Fecha a conexão e apaga o arquivo do banco e a pasta de transbordo. |

Testes: `tests/test_audit.py`, sem gravar: o texto de cada verificação nos dois dialetos, o diff dos
arquivos gerados, a chave lida do `primary_key` do modelo e o escopo escolhido pelas colunas da
chave. `tests/test_engine_duckdb.py` sob a raiz local: o pipeline de exemplo (doze meses
materializados e dimensões em view, um `select` com `join`, auditoria, exportação do mês) sobre um
Delta local criado no teste; o ciclo `query`, `to_pandas(types_mapper=pd.ArrowDtype)`, `from_pandas` e
`load` com `decimal128(18, 2)` e `date32` mantidos, e `load` recusando um DataFrame; auditoria que
reprova a chave repetida dentro do mês e a repetida
contra um mês já publicado, e o órfão de chave estrangeira com a tabela referenciada fora do
sandbox; `execute` com `%` em literal. Provas de conceito: `test_duckdb.py` (configuração, Arrow na
entrada e na saída, o leitor esvaziado pelo comando seguinte, `DECIMAL`, JSON, `executemany`, `COPY`
com `RETURN_STATS` e particionado, banco em arquivo, `test_audit_queries`), `poc_delta.py`
(`delta_scan`, poda, tempos, `ATTACH ... PIN_SNAPSHOT`),
`test_deltalake.py::test_duckdb_view_pins_version_and_reader_feeds_write`
(a view presa a `version := v` e o `to_arrow_reader()` no `write_deltalake` com predicado),
`test_sqlalchemy.py::test_arrow_path_on_raw_connection` e
`test_stdlib.py::test_engine_protocol_and_config_dataclass`.

### Etapa 5: motor Redshift

`serialize_db.engine.redshift` implementa o mesmo protocolo. A conexão vem de `RedshiftConfig`:
senha (`host`, `port`, `database`, `user`, `password`) ou IAM (`cluster` ou `workgroup`), o
`iam_role` do `COPY` e do `UNLOAD`, e `schema`, com os padrões nas variáveis
`SERIALIZE_DB_REDSHIFT_*`. A senha é o caminho padrão, lida das variáveis ou do secret da conexão do
projeto no Secrets Manager, que tem endpoint VPC no laboratório; a autenticação por IAM e a Data API
precisam das APIs do Redshift, sem endpoint VPC no laboratório (`RS-14`), e ficam opcionais.

| Primitiva | Redshift |
| --- | --- |
| `connect(config)` | `redshift_connector.connect` com `timeout`; `search_path` no esquema; `cursor.paramstyle = "named"`. |
| `ingest(table, uri, version, months=None, materialize=True)` | `copy_manifest` dos arquivos desses meses, `COPY ... FORMAT AS PARQUET MANIFEST IAM_ROLE ...` numa staging sem `mes` criada por `ddl`, e `INSERT INTO exec_<id>_<tabela> SELECT *, '<mes>'`; `JSON_PARSE` nas colunas `SUPER`. |
| `query(statement, **params)` | O statement compilado para o Redshift e executado na conexão crua; o resultado é uma `pa.Table` montada das tuplas do cursor com o esquema do statement (`from_pylist` de dicionários por nome), ou por `UNLOAD` acima de um limite de linhas. |
| `execute(sql, params)` | `{prefix}` vira `exec_<id>_`, o texto roda com o dicionário, e o resultado volta como `pa.Table`, vazia para um comando sem resultado. |
| `load(table, data)` | `cast(data, table)`, Parquet em `staging/<execution_id>/` por `pq.write_table` mais `COPY`; abaixo de um limite de linhas, um único `INSERT` multilinha montado de `to_pylist()`, numa ida ao servidor. |
| `audit(table, months, **opcoes)` | O mesmo texto compilado para o Redshift; os demais meses e a tabela referenciada entram em stagings só com as colunas da chave, por `COPY ... MANIFEST`. |
| `export_month(table, month)` | `UNLOAD ('<select do contrato>') TO '<uri>/' PARTITION BY (mes) FORMAT PARQUET MANIFEST VERBOSE` mais `register_files` com as estatísticas do rodapé Parquet. |
| `cleanup()` | `DROP TABLE` de `exec_<id>_*` e da staging; os objetos de `staging/<execution_id>/` apagados. |

O identificador de execução entra no nome do sandbox normalizado para `[a-z0-9_]`, dentro dos
127 bytes de um identificador do Redshift. Testes: `tests/test_engine_redshift.py` compara o SQL
gerado (`COPY`, `INSERT ... SELECT`, `UNLOAD`, DDL da staging) com texto esperado, sem cluster; os
testes marcados `redshift` rodam a mesma sequência com uma amostra no esquema autorizado, depois do
`test_redshift.py` da etapa 0. Provas de conceito: `test_redshift.py` (sessão e `paramstyle`
nomeado, DDL, `COPY ... MANIFEST`, lista de colunas e `FILLRECORD`, `VARCHAR`, `SUPER`, `UNLOAD`),
`test_sqlalchemy.py` (`test_redshift_dialect_compiles_dml`,
`test_sandbox_copy_of_table_and_schema_files_diff`) e `test_stdlib.py::test_execution_identifiers`
(o prefixo do sandbox).

### Etapa 6: execução e linha de comando

`serialize_db.execution` é o ciclo de uma execução; `serialize_db.cli` o expõe.

| Primitiva | O que faz |
| --- | --- |
| `Database(root, environment, metadata, storage_options=None)` | A raiz do banco, o ambiente (`prod`, `dev`) e o `MetaData` dos modelos; `uri(table)` é `<root>/<ambiente>/<tabela>/`, mais o arquivo de controle e os prefixos `staging/`, `publicacao/` e `arquivo/`; chama `prepare_environment` e cria `Storage.for_uri(root)`. |
| `Execution(db, engine, month, execution_id)` | Gerenciador de contexto: na entrada abre as tabelas de entrada, fixa `versions` e cria o sandbox; na saída descarta o sandbox e grava o resumo no log. |
| `run.previous_months(n)` | Os `n` meses até `run.month`, inclusive. |
| `run.ingest(*tables, months=None, materialize=False)` | `engine.ingest` de cada tabela na versão fixada; sem `months`, a tabela inteira. |
| `run.sandbox` | O motor, onde o pipeline chama `query`, `execute` e `load`: `pa.Table` na saída dos dois primeiros e na entrada do último. |
| `run.audit(table, months, foreign_keys=False, key_scope=None)` | `engine.audit`; a reprovação levanta `AuditFailed` e encerra sem tocar o Delta, e o relatório, com o SQL de cada verificação, vai para o log. |
| `run.publish(table, months, audit=True)` | Exige a auditoria aprovada dessa tabela nesses meses na própria execução, e `audit=False` dispensa a exigência e fica no log; depois `create_table` se não existir, `reconcile`, `export_month` e `publish_month` por mês com `commit_metadata`; avança `versions[table]`. |
| `run.publish_redshift(*tables)` | A publicação da etapa 8. |
| `run.snapshot(name)` | Marca a execução: `serialize_db_snapshot` nos commits e `snapshot(root, name, versions)` no encerramento. |
| `serialize-db run` | `--root`, `--environment`, `--engine`, `--month`, `--execution-id` e `modulo:funcao` do pipeline, que recebe `run`; código de saída 0, 1 na reprovação da auditoria, 2 no conflito. |
| `serialize-db audit` | `--table`, `--months`, `--foreign-keys` e `--key-scope`; com `--sql` imprime o texto das verificações do dialeto escolhido e não abre conexão nem armazenamento, e `--write <pasta>` grava os arquivos das duas variantes. Sem `--sql`, roda a auditoria sobre a versão publicada e imprime o relatório. |

O log é o `logging` padrão com um resumo por execução: identificador, mês, versões lidas, versões
gravadas e tempo por passo. Testes: `tests/test_execution.py` sob a raiz local com o motor DuckDB:
a reexecução com o mesmo `execution_id` produz o mesmo estado; a auditoria reprovada deixa a versão
da tabela como estava; de duas execuções publicando o mesmo mês, a segunda aborta com
`ExecutionConflict`. Provas de conceito: `test_stdlib.py` (`test_month_arithmetic`,
`test_execution_identifiers`, `test_context_manager_cleans_up_on_failure`,
`test_entry_point_by_import_string`, `test_command_line_parsing`, `test_execution_log`,
`test_prepare_environment`, `test_json_control_file_and_commit_metadata`) e
`test_deltalake.py::test_time_travel_and_restore` (os metadados de commit no histórico).

### Etapa 7: carga inicial

A migração dos Parquet atuais para o Delta, uma passagem por tabela e por mês, reexecutável, em
`serialize_db.load`.

| Primitiva | O que faz |
| --- | --- |
| `initial_load(db, table, source, months=None)` | `create_table`; para cada mês, o DuckDB lê `source/mes=<mes>/*.parquet` (ou o layout que a origem tiver), `pc.round(x, 2)` leva os `Double` da origem à escala do contrato (os modelos atuais usam `Double` onde o contrato pede `Numeric(18, 2)`; o cast de `double` arredonda sem acusar, e a regra fica explícita e no relatório), `cast` converte para o contrato e `publish_month` grava em lotes. Uma carga interrompida recomeça do mês seguinte ao último publicado. |
| `load_report(db, table, source)` | Contagem e somas das colunas numéricas por mês, na origem e no Delta; a carga só termina quando coincidem. |
| `serialize-db load` | `--table`, `--source` e `--months`. |

`convert_to_deltalake` registra os arquivos no lugar, sem reescrever, só quando eles já têm os
tipos, a ordem de colunas e o layout Hive do contrato; não foi testado e não é o caminho padrão.
Depois da carga os leitores abrem o Delta, e as pastas de origem ficam como cópia até a primeira
publicação no Redshift. Testes: `tests/test_load.py` com Parquet gerados no teste sob a raiz local,
incluindo uma origem em `Double` e uma carga interrompida. Provas de conceito:
`test_deltalake.py::test_initial_load_from_parquet_folders` (o cast na consulta do DuckDB, o mês
por `overwrite` com predicado, a retomada pelos meses já presentes e o relatório de contagens e
somas), `test_pyarrow.py` (`test_hive_partitioned_dataset`,
`test_parquet_streaming_read_filters_and_pandas`) e
`test_duckdb.py::test_decimal_from_pandas_sample_versus_arrow_schema`.

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
`redshift`. Provas de conceito: `test_deltalake.py::test_version_diff`,
`test_stdlib.py::test_group_log_actions_by_month` e `test_redshift.py::test_copy_manifest_from_delta_files`
(a transação da publicação repete o `COPY` na staging e o `INSERT` com o mês).

### Etapa 9: operação

As primitivas são as da etapa 3; a etapa entrega a rotina e a documentação.

| Rotina | Quando | Comando |
| --- | --- | --- |
| Snapshot do banco | Na periodicidade do processo, por exemplo o fim do trimestre. | `run.snapshot("2026T3")` na execução marcada. |
| Compactação | Antes de um snapshot, nunca depois. | `serialize-db compact --months ...`. |
| `vacuum` | Mensal: lista com `keep_versions` do arquivo de controle, revisada, depois aplicada; `--full` de tempos em tempos para os órfãos. Num bucket versionado o espaço só é liberado pela regra `NoncurrentVersionExpiration`; `probes/bucket.py` (`BK-14`) mostra o acumulado. | `serialize-db vacuum [--apply] [--full]`. |
| Arquivo | Anual: `deep_copy` dos snapshots mais velhos que o prazo da tabela viva para `arquivo/<nome>/<tabela>/`, a entrada sai de `snapshots.json`, a pasta recebe a regra de ciclo de vida. | `serialize-db archive <nome>`. |
| Exportação | Sob demanda: pastas Parquet por mês de um snapshot, `copy` ou `rewrite`. | `serialize-db export`. |
| Auditoria avulsa | Depois de uma correção, e quando o SQL de uma verificação precisa ser lido. | `serialize-db audit --table ... [--sql]`. |
| Monitoração | `history()` de cada tabela com os metadados da biblioteca. | `serialize-db history`. |

A documentação da API sai do `pdoc`; o runbook lista cada rotina com o comando, o que conferir
antes e o que esperar depois. Testes: `tests/test_operation.py` sob a raiz local: `vacuum` com
`keep_versions` preserva a versão do snapshot e remove a intermediária; `compact` antes do snapshot;
`deep_copy` com as mesmas somas. Provas de conceito: `test_deltalake.py`
(`test_vacuum_with_keep_versions`, `test_compact_and_checkpoint`, `test_dataset_reader_and_deep_copy`,
`test_export_snapshot_by_copying_files`, `test_log_files`) e
`test_stdlib.py::test_exclusive_create_atomic_replace_and_fingerprint` (o arquivo de controle).

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
    run.sandbox.load(LancamentoProjetado, pa.Table.from_pandas(projected, preserve_index=False))
    run.audit(LancamentoProjetado, months=["2026-08"])               # exigida por publish
    run.publish(LancamentoProjetado, months=["2026-08"])             # overwrite por mês, metadados
    run.publish_redshift(LancamentoProjetado)                        # só os meses alterados
```

O DataFrame que `project` devolve mantém os dtypes `ArrowDtype` de `frame`, e `from_pandas` os
devolve ao Arrow sem cópia; uma coluna calculada em `float64` chega como `double`, e `load` a recusa
numa coluna `Numeric` enquanto houver valor fora da escala, até o pipeline arredondar.

Uma reexecução com o mesmo `execution_id` repete os `overwrite` dos mesmos meses e produz o mesmo
estado. Uma correção de um mês antigo é a mesma chamada com outro `month` e um `execution_id` novo:
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
