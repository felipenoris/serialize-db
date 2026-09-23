# `probes/`: leituras do ambiente

Scripts só de leitura que fotografam o que o ambiente oferece à biblioteca: credenciais, região,
rede, o projeto do SageMaker Unified Studio, o bucket, o Redshift, os serviços de catálogo e a
estrutura da base Parquet de origem. Nenhum
deles cria, altera ou apaga um recurso. Cada um roda com o interpretador da pasta preparada, imprime
o relatório no terminal e o grava em `output/<script>_<data-hora>.txt`, pasta fora do git, para ser
colado na conversa com o assistente. O formato segue os scripts de leitura de
[felipenoris/AWS-DataScience](https://github.com/felipenoris/AWS-DataScience), pasta `aws/`:
seções numeradas, cada chamada ecoada acima do seu resultado ou do seu erro, identificadores
reaproveitados como `NOME=valor`, a tabela de checagens (`fail` primeiro, depois `note`, depois
`pass`) e a seção final "Chamadas que falharam", para um bloco vazio nunca significar "negado".
Uma chamada marcada `expected` sai como `-- SEM RESULTADO` e fica fora dessa seção e do código de
saída: a visão de sistema negada a um usuário comum e o pacote ausente fora de um espaço são leitura
do ambiente, não defeito a corrigir.
Código de saída: 0 toda checagem passou, 1 alguma chamada falhou, 2 alguma checagem reprovou.

Um relatório que uma etapa pendente ainda consulta é guardado em `plan/readings/`, indexado por
[`plan/readings/README.md`](../plan/readings/README.md); `output/` fica fora do git.

```
.venv/bin/python probes/space.py
.venv/bin/python probes/bucket.py s3://bucket/prefixo
.venv/bin/python probes/diagnose_aws.py s3://bucket/prefixo
.venv/bin/python probes/redshift.py s3://bucket/prefixo
.venv/bin/python probes/catalog.py
.venv/bin/python probes/parquet_source.py /caminho/da/base
.venv/bin/python probes/parquet_source.py /caminho/da/base --text-bytes
```

## Os scripts

| Script | O que lê | Chamadas | Saída |
| --- | --- | --- | --- |
| `space.py` | O espaço visto de dentro: método e expiração das credenciais do `boto3` (com o tempo restante), região como o `boto3` a resolve, endpoint de credenciais do contêiner, IMDS, STS; o projeto por `sagemaker_studio.Project()` (papel, chave KMS, raiz S3, uma linha por conexão e os dados completos das conexões Redshift), lido neste interpretador ou no do sistema; DNS dos endpoints regionais e se resolvem para IP privado (endpoint VPC de interface), TCP até o S3 e o proxy, internet; CPUs, memória, disco (repositório, `HOME`, `/tmp` e a pasta temporária do Python, onde fica o sandbox), limite de arquivos abertos, `~/shared` (montagem pelo caminho real e por `/proc/mounts`, com o tipo e `rw` ou `ro`), comandos; Python e pacotes deste interpretador e do sistema contra as dependências de execução e o grupo `dev` de `pyproject.toml` (cada pacote presente, e na versão fixada quando ela é `==`), mais os opcionais das etapas seguintes (ADBC, pdoc); DuckDB com as extensões que carregam da pasta configurada, sem instalação automática, e o proxy da sessão, com o endereço separado das credenciais. | `sts:GetCallerIdentity`; as chamadas que `sagemaker_studio` faz. | `output/space_*.txt` |
| `bucket.py` | O bucket sob a raiz: região, versionamento (pela API ou, com ela negada, pelo `VersionId` de uma amostra, com a consequência para o `vacuum`), criptografia padrão, Object Lock com o custo para o `vacuum`, bloqueio de acesso público, propriedade de objetos; as regras de ciclo de vida e se alguma expiração alcança a raiz; o inventário sob a raiz por pasta de primeiro nível, as tabelas Delta (pastas com `_delta_log`, com arquivos de dados, bytes, commits no log, checkpoints e último objeto), as sessões da suíte S3 (`serialize-db-poc/<id>`) que ainda existem, classes de armazenamento, objeto mais recente, a criptografia de uma amostra e o que o versionamento acumulou sob a raiz (versões não correntes e marcadores de exclusão, invisíveis à listagem comum); as permissões do papel sob a raiz pela simulação de política do IAM (`ListBucket`, `GetObject`, `PutObject`, `DeleteObject`, `AbortMultipartUpload`, `kms:GenerateDataKey`, `kms:Decrypt`) ou, com a simulação negada ou o IAM sem resposta ao teste de alcance, o que a própria execução provou; a chave KMS padrão, pedida ao KMS só quando ele responde; a política do bucket com os `Deny` condicionados; os uploads multipart incompletos. | `s3:HeadBucket`, `GetBucketVersioning`, `GetBucketEncryption`, `GetObjectLockConfiguration`, `GetPublicAccessBlock`, `GetBucketOwnershipControls`, `GetBucketLifecycleConfiguration`, `ListBucket`, `ListBucketVersions`, `HeadObject`, `GetBucketPolicy`, `ListMultipartUploads`; `sts:GetCallerIdentity`; `iam:SimulatePrincipalPolicy`; `kms:DescribeKey`. | `output/bucket_*.txt` |
| `diagnose_aws.py` | O acesso que a suíte S3 exige: variáveis, região como o `boto3` e o delta-rs a resolvem, DNS, credenciais, a listagem de `<raiz>/serialize-db-poc/` pelo `boto3`, pelo delta-rs (como encontrado e, com `NO_PROXY` ausente ou vazia ao lado de `no_proxy`, como a suíte, com `NO_PROXY` exportada de `no_proxy`) e pelo DuckDB (este com as subpastas, porque `*` não cruza `/`, e com o proxy separado das credenciais), e o STS; o resumo diz se a suíte precisa de manutenção. Formato próprio, anterior a `probelib.py`. | `s3:ListBucket`, `sts:GetCallerIdentity`. | `output/diagnose_aws_*.txt` |
| `redshift.py` | O Redshift visto de dentro: as variáveis `SERIALIZE_DB_REDSHIFT_*` e a conexão Redshift do projeto, com os dados dela como dicionário (banco, workgroup, URL JDBC e o secret de usuário e senha, lido sem imprimir a senha); o workgroup configurado lido por `GetWorkgroup`, que dá o endereço da conexão, o namespace com o papel IAM padrão e os associados, a lista de workgroups e os clusters provisionados como fotografia (o ambiente alvo não tem nenhum); a Data API pelo ciclo completo de `examples/redshift_data_api.py` com `select 1`, o caminho alternativo à porta 5439; DNS e TCP, e se as APIs têm endpoint VPC de interface, sem o qual a credencial temporária e a Data API dependem da internet; a sessão aberta como `examples/redshift_native.py` (`GetWorkgroup`, `GetCredentials`, `redshift_connector.connect`) ou pelo par informado em `_USER` e `_PASSWORD` na mesma chamada, e nela, antes do `USE`, a versão (o patch), usuário, banco, `search_path`, esquemas, `SUPER`, as configurações da sessão, os privilégios no banco da conexão (`CREATE`, `TEMP`), `sys_load_error_detail` e os esquemas externos; os bancos visíveis e em qual deles está o esquema do projeto, os três requisitos da escrita num datashare, o `USE` no banco do datashare conferido pela resolução de um nome em duas partes, porque `current_database()` não reflete a troca, (`examples/redshift_copy_unload.py`) e, depois dele, os privilégios no esquema e as tabelas com o prefixo da biblioteca; as credenciais de quem chama com a expiração, e o alcance sobre a raiz S3 informada, pela simulação de política do IAM da identidade que a biblioteca manda ao S3: quem chama, ou o papel de `_IAM_ROLE`. Só consulta visões de sistema e troca o banco da sessão com `USE`; a credencial derivada da identidade IAM pode criar o usuário do banco, e a checagem `RS-4` o diz. | `redshift-serverless:GetWorkgroup`, `GetCredentials`, `GetNamespace`, `ListWorkgroups`; `redshift:DescribeClusters`; `redshift-data:ExecuteStatement`, `DescribeStatement`, `GetStatementResult`; `secretsmanager:GetSecretValue` para o secret da conexão do projeto; `sts:GetCallerIdentity`; `iam:SimulatePrincipalPolicy`; consultas `select` e `USE` no banco. | `output/redshift_*.txt` |
| `catalog.py` | O gatilho de reavaliação de `plan/estrategia.md`: se o Glue (bancos, tabelas por formato, catálogos federados), o Athena (workgroups), o Lake Formation (locais registrados) e o S3 Tables respondem ao papel do projeto. | `glue:GetDatabases`, `GetTables`, `GetCatalogs`; `athena:ListWorkGroups`, `GetWorkGroup`; `lakeformation:ListResources`; `s3tables:ListTableBuckets`. | `output/catalog_*.txt` |
| `parquet_source.py` | A estrutura da base Parquet de origem da carga inicial, uma pasta por tabela: as pastas de tabela e o que não é Parquet; por tabela, arquivos, bytes, linhas, partições e quantos esquemas distintos; o esquema do grupo majoritário com tipo Arrow, nulidade, tipo físico, tipo lógico e `field_id`; as divergências de esquema entre arquivos, coluna a coluna e com todos os arquivos divergentes nomeados; as colunas de partição, os seus valores e se também estão dentro dos arquivos; por coluna, linhas, nulos, mínimo, máximo e distintos somados do rodapé; row groups, compressão, codificação, escritor e metadados do rodapé; com `--sample N`, a cardinalidade e o comprimento de texto que o rodapé não guarda, em caracteres e o máximo em bytes, a medida do `VARCHAR(n)` do Redshift e do `cast`; com `--text-bytes`, o maior texto de cada coluna em todas as linhas de todos os arquivos, em bytes e em caracteres, a varredura que dá o maior texto de cada coluna da base, a medida do `String(n)` do contrato. Lê o rodapé de todo arquivo de toda partição. | Nenhuma chamada AWS num caminho local; numa URI `s3://`, as leituras do `pyarrow.fs` (`ListBucket`, `GetObject`). | `output/parquet_source_*.txt` |

`probelib.py` é a biblioteca comum: o relatório (`Report`), as esperas curtas do `boto3`
(`short_config`), a classificação "o serviço respondeu com erro" contra "sem resposta", DNS e TCP, a
leitura do projeto por `sagemaker_studio` no primeiro interpretador que tem o pacote
(`project_snapshot`, com os dados de cada conexão convertidos em dicionário) e o mascaramento de
segredos na saída. DNS, TCP e a internet são leituras nas tabelas, nunca chamadas falhadas: no
ambiente destino, sem internet, `pypi.org` não resolve e o IMDS não responde, e isso não é falha do
probe; um IP público é rotulado como possível gateway endpoint só para o S3 e o DynamoDB, e como
dependente da internet ou do proxy para os demais serviços. Cada chamada que falha deixa o motivo
curto (`negado (AccessDenied)`, `sem resposta`, `erro local`) na checagem que a interpreta. O pacote
`sagemaker-studio` não entra no projeto: ele arrasta `deltalake`, `duckdb` e `pandas` sem versão
fixa, e numa instalação de teste rebaixou o `duckdb` para 1.5.1.

## Variáveis de `redshift.py`

| Variável | Uso |
| --- | --- |
| `SERIALIZE_DB_REDSHIFT_WORKGROUP` | Workgroup serverless, com `_DATABASE`: o endereço vem de `GetWorkgroup` e a credencial temporária de `GetCredentials`, o caminho de `examples/redshift_native.py`. |
| `SERIALIZE_DB_REDSHIFT_HOST`, `_PORT` (5439), `_DATABASE`, `_USER`, `_PASSWORD` | O par usuário e senha informado, na mesma chamada `redshift_connector.connect`; o secret da conexão do projeto o preenche. O IAM interno do `redshift_connector` e o cluster provisionado não são caminhos do probe nem da suíte: ninguém os executou no ambiente alvo, que não tem cluster. |
| `SERIALIZE_DB_REDSHIFT_SHARE_DATABASE` | Banco do datashare que guarda o esquema do projeto, quando não é o da conexão: a sessão roda `USE` nele (`RS-19`) e cita `esquema.tabela`; o nome em três partes fica para a Data API. Sem ele, `RS-16` toma o banco da leitura de `svv_all_schemas`. |
| `SERIALIZE_DB_REDSHIFT_SCHEMA` | Esquema do projeto, para os privilégios `USAGE` e `CREATE` e a lista de tabelas. |
| `SERIALIZE_DB_REDSHIFT_IAM_ROLE` | Papel do `COPY` e do `UNLOAD` na suíte de testes, ou `default`; sem ela, os dois levam as credenciais de quem chama (`RS-18`). `RS-11` simula a identidade que a variável escolhe, e um ARN que o namespace não tem é apontado (`RS-6`). |
| `SERIALIZE_DB_REDSHIFT_CONNECTION` | Nome da conexão Redshift do projeto; sem ela, a única conexão Redshift, se houver. As variáveis têm precedência sobre a conexão. |
| argumento `s3://bucket/prefixo`, `SERIALIZE_DB_ROOT` ou `SERIALIZE_DB_TEST_S3_ROOT` | A raiz sobre a qual o papel do `COPY` e do `UNLOAD` é simulado (`RS-11`), nessa ordem de precedência. |

Sem variável e sem conexão no projeto, o script lista o que as APIs mostram e não conecta.

## Nomenclatura das variáveis

Toda variável que o projeto exige começa com `SERIALIZE_DB_`: `SERIALIZE_DB_ROOT` e as demais da
biblioteca ([`PLAN.md`](../plan/PLAN.md)), `SERIALIZE_DB_REDSHIFT_*` para a conexão,
`SERIALIZE_DB_TEST_*` para a autorização de cada suíte e `SERIALIZE_DB_DUCKDB_EXTENSIONS`. As
variáveis sem prefixo que os probes leem são padrões de terceiros, lidos por quem os define e não
pelo projeto: `AWS_REGION` e `AWS_DEFAULT_REGION` (botocore e delta-rs), `AWS_ENDPOINT_URL*`,
`AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`, `HTTP_PROXY`, `HTTPS_PROXY` e `NO_PROXY` nas duas grafias.
Renomeá-las quebraria as bibliotecas que as consultam, então elas ficam como estão.

A raiz S3 que um probe fotografa sai de `probelib.s3_root`, na ordem argumento, `SERIALIZE_DB_ROOT`
(a raiz da biblioteca, onde ela escreveria) e `SERIALIZE_DB_TEST_S3_ROOT` (a autorização da suíte
S3, que costuma apontar para o mesmo lugar). Uma `SERIALIZE_DB_ROOT` de pasta local é ignorada por
estes probes, que leem S3. `bucket.py` inventaria o que há sob a raiz, `diagnose_aws.py` lista
`<raiz>/serialize-db-poc/` como a suíte faz e `redshift.py` simula sobre ela o papel do `COPY`;
nenhum probe cria pasta ou objeto. No espaço do SageMaker, a raiz é a área de trabalho `dev/` do
projeto, que `space.py` imprime como `s3_root` na seção do projeto, ou uma subpasta dela reservada
aos testes. Fora disso os probes precisam só das credenciais e da região que o `boto3` resolve,
presentes no espaço. `redshift.py` sem as variáveis e sem a conexão do projeto registra as seções da
sessão como `note`. `AWS_DEFAULT_REGION` é a variável que o botocore lê; sem ela e sem perfil, as
APIs do Redshift são procuradas na região errada. O JSON de `SERIALIZE_DB_TEST_REPORT` da suíte
acompanha os relatórios dos probes na conversa.

## Onde está a resposta

| Pergunta | Script e checagem |
| --- | --- |
| O `boto3` encontra credenciais, e por qual método? | `space.py`, `SP-1` |
| A região está configurada de um jeito que o `boto3` lê (`AWS_DEFAULT_REGION` ou perfil)? | `space.py`, `SP-2`; `diagnose_aws.py`, resumo |
| O STS responde? | `space.py`, `SP-3`; `diagnose_aws.py`, resumo |
| O espaço tem projeto, e o projeto tem conexão Redshift? | `space.py`, `SP-4` e `SP-5` |
| Os endpoints resolvem para IP privado (endpoint VPC de interface) ou público (gateway endpoint só para o S3 e o DynamoDB; internet ou proxy para os demais)? | `space.py` seção 3, `bucket.py` seção 1, `redshift.py` seção 3, `catalog.py` seção 1 |
| Há internet? Há proxy, e ele responde? | `space.py`, `SP-7` e seção 3 |
| A pasta preparada tem o Python, as dependências de execução e o grupo `dev` de `pyproject.toml`, e as extensões do DuckDB carregam? | `space.py`, `SP-8`, `SP-9` e `SP-10` |
| Quanta CPU, memória e disco a máquina tem? | `space.py` seção 4 |
| O bucket está acessível, na região do `boto3`, e alguma regra de ciclo de vida expira objetos sob a raiz? | `bucket.py`, `BK-1`, `BK-2` e `BK-3` |
| Que criptografia e versionamento o bucket aplica, e o que o versionamento custa ao `vacuum`? | `bucket.py`, `BK-4` (pela API ou pela amostra), `BK-5` e `BK-7` |
| O que já existe sob a raiz, quantas tabelas Delta, e em que estado cada uma (arquivos, commits, último objeto)? | `bucket.py` seção 3, `BK-6` |
| A suíte S3 roda como está neste ambiente? | `diagnose_aws.py`, resumo |
| O delta-rs encontra as credenciais com o ambiente como está, ou só com `NO_PROXY` exportada de `no_proxy`? | `diagnose_aws.py`, seção S3 e resumo |
| Que proxy a sessão do DuckDB usa, e ele tem usuário e senha? | `space.py` seção 6, `diagnose_aws.py` seção S3 |
| O papel tem `ListBucket`, `GetObject`, `PutObject` e `DeleteObject` sob a raiz, e as ações do KMS sobre a chave? | `bucket.py`, `BK-8`; com o IAM inalcançável ou a simulação negada, o que a execução provou e a suíte S3 |
| A chave KMS padrão do bucket está habilitada? | `bucket.py`, `BK-9`; com o KMS inalcançável, a escrita da suíte S3 |
| A política do bucket tem `Deny` condicionado a criptografia ou a TLS? Há uploads multipart incompletos sob a raiz? | `bucket.py`, `BK-10` e `BK-11` |
| O bucket tem Object Lock, e o que ele custa ao `vacuum`? | `bucket.py`, `BK-12` |
| Sobrou pasta de alguma sessão da suíte S3 sob a raiz? | `bucket.py`, `BK-13` |
| Quanto o versionamento já acumulou sob a raiz (versões não correntes e marcadores de exclusão)? | `bucket.py`, `BK-14` |
| As credenciais expiram? Quanto disco a pasta temporária do sandbox tem, e quantos arquivos um processo pode abrir? | `space.py`, seções 1 e 4 |
| Como o ambiente expõe o Redshift, e o que falta para conectar? | `redshift.py`, `RS-1` e seção 2 |
| O namespace tem papel IAM para `COPY` e `UNLOAD`, padrão ou associado? | `redshift.py`, `RS-6`; sem papel, o comando leva as credenciais de quem chama, e `RS-18` as lê, com a expiração |
| A sessão abre, e o papel tem `USAGE` e `CREATE` no esquema? | `redshift.py`, `RS-4` e `RS-5` |
| A credencial temporária do workgroup é emitida, para qual usuário e até quando? | `redshift.py`, `RS-15` |
| O esquema do projeto está no banco da conexão ou num banco de datashare, e quais são o nome na sessão e o nome em três partes? | `redshift.py`, `RS-16` |
| O consumidor atende aos requisitos da escrita num datashare (patch 186, isolamento de snapshot, 64 slices)? | `redshift.py`, `RS-17` |
| `SUPER` está disponível? | `redshift.py`, `RS-7` |
| O usuário tem `CREATE` e `TEMP` no banco? | `redshift.py`, `RS-9` |
| A Data API responde, para o caso de a porta 5439 estar fechada? | `redshift.py`, `RS-10` |
| As APIs do Redshift têm endpoint VPC, ou a autenticação por IAM e a Data API dependem da internet? | `redshift.py`, `RS-14` |
| Quem vai alcançar a raiz S3 no `COPY` (a identidade da sessão, ou o papel de `SERIALIZE_DB_REDSHIFT_IAM_ROLE`) tem permissão? | `redshift.py`, `RS-11` |
| `USE` faz `esquema.tabela` resolver no banco do datashare (a prova é um `select` numa tabela dele, porque `current_database()` continua respondendo o banco da conexão), e `has_schema_privilege` e `svv_table_info` alcançam o esquema depois dele? | `redshift.py`, `RS-19`, `RS-5` e `RS-8` |
| Um `COPY` reprovado pode ser diagnosticado por `sys_load_error_detail`? | `redshift.py`, `RS-12` |
| Há esquemas externos (Spectrum) no banco, e qual é o patch do Redshift? | `redshift.py`, `RS-13` e `REDSHIFT_VERSION` |
| O Glue, o Athena, o Lake Formation ou o S3 Tables passaram a responder ao papel do projeto? | `catalog.py`, `CT-1` a `CT-6` |
| Quais são as tabelas da base de origem, e que campos e tipos cada uma tem? | `parquet_source.py`, seções 2 e 3 |
| Todos os arquivos de todas as partições de uma tabela têm o mesmo esquema? | `parquet_source.py`, `PQ-3` e seção 4 |
| Como a base está particionada, e a coluna de partição também está dentro dos arquivos? | `parquet_source.py`, `PQ-4` e `PQ-5` |
| Que faixas de valores, nulos e cardinalidade uma base fictícia precisa reproduzir? | `parquet_source.py`, seções 6 e 8 |
| Qual é o maior texto de cada coluna, a medida do `String(n)` do modelo? | `parquet_source.py --text-bytes`, seção 9 |

## Acrescentar um probe

- Só leitura. Um script que altera algo não pertence a esta pasta.
- Construa sobre `probelib.py`: `Report` para o arquivo, as seções, as chamadas ecoadas, as
  checagens e o código de saída; `short_config` em todo cliente `boto3`, porque sem rede o padrão
  espera 60 s por tentativa; `run_python` para o que precisa de espera limitada (delta-rs, DuckDB).
- `probelib.endpoint_reachable` antes de chamar um serviço que pode não ter endpoint VPC: o
  `connect_timeout` do botocore vale em cada endereço que o nome resolve, vezes as tentativas, e o
  teste TCP de 2 s vai a um endereço só. Sem resposta, a checagem vira `note` e a chamada não é
  feita; com proxy configurado o teste direto não decide, e a chamada é feita.
- Uma seção por assunto, numerada pelo `Report`; identificadores reaproveitados como `NOME=valor`;
  toda chamada por `report.call`, para a falha ir para a seção final.
- Checagens com prefixo próprio de duas letras (`SP`, `BK`, `RS`, `CT`, `PQ`), `note` para o ausente e
  `fail` para o que impede a biblioteca.
- Uma seção que quebra não cala as outras: `main` captura a exceção e a registra como falha.
- Uma função por seção, na ordem do relatório, com docstring que nomeia a seção e as checagens que
  ela emite; dentro dela, um bloco por checagem, precedido do comentário que diz a regra aplicada e
  o que a negação significa. Um `render` que não cabe numa linha vira função nomeada
  (`render_head_bucket`, `render_rows`), com docstring dizendo quais campos mostra.
- `tests/test_probes.py` testa as funções puras dos probes com respostas fabricadas, sem rede: uma
  função nova que decide algo sobre o que leu ganha um caso lá, e
  `uv run pytest tests/test_probes.py` roda em segundos.
- Acrescente a linha na tabela dos scripts e as perguntas na tabela acima.
