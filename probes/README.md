# `probes/`: leituras do ambiente

Scripts só de leitura que fotografam o que o ambiente oferece à biblioteca: credenciais, região,
rede, o projeto do SageMaker Unified Studio, o bucket, o Redshift e os serviços de catálogo. Nenhum
deles cria, altera ou apaga um recurso. Cada um roda com o interpretador da pasta preparada, imprime
o relatório no terminal e o grava em `output/<script>_<data-hora>.txt`, pasta fora do git, para ser
colado na conversa com o assistente. O formato segue os scripts de leitura de
[felipenoris/AWS-DataScience](https://github.com/felipenoris/AWS-DataScience), pasta `aws/`:
seções numeradas, cada chamada ecoada acima do seu resultado ou do seu erro, identificadores
reaproveitados como `NOME=valor`, a tabela de checagens (`fail` primeiro, depois `note`, depois
`pass`) e a seção final "Chamadas que falharam", para um bloco vazio nunca significar "negado".
Código de saída: 0 toda checagem passou, 1 alguma chamada falhou, 2 alguma checagem reprovou.

```
.venv/bin/python probes/space.py
.venv/bin/python probes/bucket.py s3://bucket/prefixo
.venv/bin/python probes/diagnose_aws.py s3://bucket/prefixo
.venv/bin/python probes/redshift.py s3://bucket/prefixo
.venv/bin/python probes/catalog.py
```

## Os scripts

| Script | O que lê | Chamadas | Saída |
| --- | --- | --- | --- |
| `space.py` | O espaço visto de dentro: método e expiração das credenciais do `boto3` (com o tempo restante), região como o `boto3` a resolve, endpoint de credenciais do contêiner, IMDS, STS; o projeto por `sagemaker_studio.Project()` (papel, chave KMS, raiz S3, uma linha por conexão e os dados completos das conexões Redshift), lido neste interpretador ou no do sistema; DNS dos endpoints regionais e se resolvem para IP privado (endpoint VPC de interface), TCP até o S3 e o proxy, internet; CPUs, memória, disco (repositório, `HOME`, `/tmp` e a pasta temporária do Python, onde fica o sandbox), limite de arquivos abertos, `~/shared` (montagem pelo caminho real e por `/proc/mounts`, com o tipo e `rw` ou `ro`), comandos; Python e pacotes deste interpretador e do sistema contra o grupo `dev` de `pyproject.toml` (cada pacote presente, e na versão fixada quando ela é `==`), mais os opcionais das etapas seguintes (ADBC, SQLGlot, pdoc); DuckDB com as extensões que carregam da pasta configurada, sem instalação automática. | `sts:GetCallerIdentity`; as chamadas que `sagemaker_studio` faz. | `output/space_*.txt` |
| `bucket.py` | O bucket sob a raiz: região, versionamento (pela API ou, com ela negada, pelo `VersionId` de uma amostra, com a consequência para o `vacuum`), criptografia padrão, Object Lock com o custo para o `vacuum`, bloqueio de acesso público, propriedade de objetos; as regras de ciclo de vida e se alguma expiração alcança a raiz; o inventário sob a raiz por pasta de primeiro nível, as tabelas Delta (pastas com `_delta_log`, com arquivos de dados, bytes, commits no log, checkpoints e último objeto), as sessões da suíte S3 (`serialize-db-poc/<id>`) que ainda existem, classes de armazenamento, objeto mais recente, a criptografia de uma amostra e o que o versionamento acumulou sob a raiz (versões não correntes e marcadores de exclusão, invisíveis à listagem comum); as permissões do papel sob a raiz pela simulação de política do IAM (`ListBucket`, `GetObject`, `PutObject`, `DeleteObject`, `AbortMultipartUpload`, `kms:GenerateDataKey`, `kms:Decrypt`) ou, com a simulação negada, o que a própria execução provou; a chave KMS padrão; a política do bucket com os `Deny` condicionados; os uploads multipart incompletos. | `s3:HeadBucket`, `GetBucketVersioning`, `GetBucketEncryption`, `GetObjectLockConfiguration`, `GetPublicAccessBlock`, `GetBucketOwnershipControls`, `GetBucketLifecycleConfiguration`, `ListBucket`, `ListBucketVersions`, `HeadObject`, `GetBucketPolicy`, `ListMultipartUploads`; `sts:GetCallerIdentity`; `iam:SimulatePrincipalPolicy`; `kms:DescribeKey`. | `output/bucket_*.txt` |
| `diagnose_aws.py` | O acesso que a suíte S3 exige: variáveis, região como o `boto3` e o delta-rs a resolvem, DNS, credenciais, a listagem de `<raiz>/serialize-db-poc/` pelo `boto3`, pelo delta-rs e pelo DuckDB (este com as subpastas, porque `*` não cruza `/`), e o STS; o resumo diz se a suíte precisa de manutenção. Formato próprio, anterior a `probelib.py`. | `s3:ListBucket`, `sts:GetCallerIdentity`. | `output/diagnose_aws_*.txt` |
| `redshift.py` | O Redshift visto de dentro: as variáveis `SERIALIZE_DB_REDSHIFT_*` e a conexão Redshift do projeto, com os dados dela como dicionário (banco, workgroup ou cluster, URL JDBC e o secret de usuário e senha, lido sem imprimir a senha); clusters e workgroups com o papel IAM padrão do `COPY` e do `UNLOAD`, os nós e o roteamento VPC; a Data API como caminho alternativo à porta 5439; DNS e TCP, e se as APIs têm endpoint VPC de interface, sem o qual a autenticação por IAM e a Data API dependem da internet; a sessão por senha ou por IAM com a versão (o patch), usuário, banco, `search_path`, esquemas, privilégios no esquema do projeto e no banco (`CREATE`, `TEMP`), tabelas com o prefixo da biblioteca, `SUPER`, as configurações da sessão, `stl_load_errors` e os esquemas externos; o papel do `COPY` sobre a raiz S3 informada, pela simulação de política do IAM. Só consulta visões de sistema; a autenticação por IAM pode criar o usuário do banco, e a checagem `RS-4` o diz. | `redshift:DescribeClusters`, `redshift-serverless:ListWorkgroups`, `GetNamespace`, `redshift-data:ListDatabases`; `redshift:GetClusterCredentials` ou `redshift-serverless:GetCredentials` na autenticação por IAM; `secretsmanager:GetSecretValue` para o secret da conexão do projeto; `iam:SimulatePrincipalPolicy`; consultas `select` no banco. | `output/redshift_*.txt` |
| `catalog.py` | O gatilho de reavaliação de `docs/estrategia.md`: se o Glue (bancos, tabelas por formato, catálogos federados), o Athena (workgroups), o Lake Formation (locais registrados) e o S3 Tables respondem ao papel do projeto. | `glue:GetDatabases`, `GetTables`, `GetCatalogs`; `athena:ListWorkGroups`, `GetWorkGroup`; `lakeformation:ListResources`; `s3tables:ListTableBuckets`. | `output/catalog_*.txt` |

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
| `SERIALIZE_DB_REDSHIFT_HOST`, `_PORT` (5439), `_DATABASE`, `_USER`, `_PASSWORD` | Conexão por senha. |
| `SERIALIZE_DB_REDSHIFT_CLUSTER` | Identificador do cluster provisionado, autenticação por IAM (`redshift:GetClusterCredentials`), com `_DATABASE` e `_USER`. |
| `SERIALIZE_DB_REDSHIFT_WORKGROUP` | Workgroup serverless, autenticação por IAM (`redshift-serverless:GetCredentials`), com `_DATABASE`. |
| `SERIALIZE_DB_REDSHIFT_SCHEMA` | Esquema do projeto, para os privilégios `USAGE` e `CREATE` e a lista de tabelas. |
| `SERIALIZE_DB_REDSHIFT_CONNECTION` | Nome da conexão Redshift do projeto; sem ela, a única conexão Redshift, se houver. As variáveis têm precedência sobre a conexão. |
| argumento `s3://bucket/prefixo`, ou `SERIALIZE_DB_TEST_S3_ROOT` | A raiz sobre a qual o papel padrão do `COPY` e do `UNLOAD` é simulado (`RS-11`). |

Sem variável e sem conexão no projeto, o script lista o que as APIs mostram e não conecta.

## Onde está a resposta

| Pergunta | Script e checagem |
| --- | --- |
| O `boto3` encontra credenciais, e por qual método? | `space.py`, `SP-1` |
| A região está configurada de um jeito que o `boto3` lê (`AWS_DEFAULT_REGION` ou perfil)? | `space.py`, `SP-2`; `diagnose_aws.py`, resumo |
| O STS responde? | `space.py`, `SP-3`; `diagnose_aws.py`, resumo |
| O espaço tem projeto, e o projeto tem conexão Redshift? | `space.py`, `SP-4` e `SP-5` |
| Os endpoints resolvem para IP privado (endpoint VPC de interface) ou público (gateway endpoint só para o S3 e o DynamoDB; internet ou proxy para os demais)? | `space.py` seção 3, `bucket.py` seção 1, `redshift.py` seção 3, `catalog.py` seção 1 |
| Há internet? Há proxy, e ele responde? | `space.py`, `SP-7` e seção 3 |
| A pasta preparada tem o Python e o grupo `dev` de `pyproject.toml`, e as extensões do DuckDB carregam? | `space.py`, `SP-8`, `SP-9` e `SP-10` |
| Quanta CPU, memória e disco a máquina tem? | `space.py` seção 4 |
| O bucket está acessível, na região do `boto3`, e alguma regra de ciclo de vida expira objetos sob a raiz? | `bucket.py`, `BK-1`, `BK-2` e `BK-3` |
| Que criptografia e versionamento o bucket aplica, e o que o versionamento custa ao `vacuum`? | `bucket.py`, `BK-4` (pela API ou pela amostra), `BK-5` e `BK-7` |
| O que já existe sob a raiz, quantas tabelas Delta, e em que estado cada uma (arquivos, commits, último objeto)? | `bucket.py` seção 3, `BK-6` |
| A suíte S3 roda como está neste ambiente? | `diagnose_aws.py`, resumo |
| O papel tem `ListBucket`, `GetObject`, `PutObject` e `DeleteObject` sob a raiz, e as ações do KMS sobre a chave? | `bucket.py`, `BK-8`; sem simulação, o que a execução provou e a suíte S3 |
| A chave KMS padrão do bucket está habilitada? | `bucket.py`, `BK-9` |
| A política do bucket tem `Deny` condicionado a criptografia ou a TLS? Há uploads multipart incompletos sob a raiz? | `bucket.py`, `BK-10` e `BK-11` |
| O bucket tem Object Lock, e o que ele custa ao `vacuum`? | `bucket.py`, `BK-12` |
| Sobrou pasta de alguma sessão da suíte S3 sob a raiz? | `bucket.py`, `BK-13` |
| Quanto o versionamento já acumulou sob a raiz (versões não correntes e marcadores de exclusão)? | `bucket.py`, `BK-14` |
| As credenciais expiram? Quanto disco a pasta temporária do sandbox tem, e quantos arquivos um processo pode abrir? | `space.py`, seções 1 e 4 |
| Como o ambiente expõe o Redshift, e o que falta para conectar? | `redshift.py`, `RS-1` e seção 2 |
| O cluster ou o workgroup tem papel IAM padrão para `COPY` e `UNLOAD`? | `redshift.py`, `RS-6` |
| A sessão abre, e o papel tem `USAGE` e `CREATE` no esquema? | `redshift.py`, `RS-4` e `RS-5` |
| `SUPER` está disponível? | `redshift.py`, `RS-7` |
| O usuário tem `CREATE` e `TEMP` no banco? | `redshift.py`, `RS-9` |
| A Data API responde, para o caso de a porta 5439 estar fechada? | `redshift.py`, `RS-10` |
| As APIs do Redshift têm endpoint VPC, ou a autenticação por IAM e a Data API dependem da internet? | `redshift.py`, `RS-14` |
| O papel do `COPY` alcança a raiz S3? | `redshift.py`, `RS-11` |
| Um `COPY` reprovado pode ser diagnosticado por `stl_load_errors`? | `redshift.py`, `RS-12` |
| Há esquemas externos (Spectrum) no banco, e qual é o patch do Redshift? | `redshift.py`, `RS-13` e `REDSHIFT_VERSION` |
| O Glue, o Athena, o Lake Formation ou o S3 Tables passaram a responder ao papel do projeto? | `catalog.py`, `CT-1` a `CT-6` |

## Acrescentar um probe

- Só leitura. Um script que altera algo não pertence a esta pasta.
- Construa sobre `probelib.py`: `Report` para o arquivo, as seções, as chamadas ecoadas, as
  checagens e o código de saída; `short_config` em todo cliente `boto3`, porque sem rede o padrão
  espera 60 s por tentativa; `run_python` para o que precisa de espera limitada (delta-rs, DuckDB).
- Uma seção por assunto, numerada pelo `Report`; identificadores reaproveitados como `NOME=valor`;
  toda chamada por `report.call`, para a falha ir para a seção final.
- Checagens com prefixo próprio de duas letras (`SP`, `BK`, `RS`, `CT`), `note` para o ausente e
  `fail` para o que impede a biblioteca.
- Uma seção que quebra não cala as outras: `main` captura a exceção e a registra como falha.
- Acrescente a linha na tabela dos scripts e as perguntas na tabela acima.
