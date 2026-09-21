# O que as provas de conceito, as suítes e os probes mostraram

Este documento registra o resultado de cada execução: as provas de conceito na AWS e em disco local,
as suítes de `tests/`, e as leituras do ambiente pelos probes, com a data de cada medição e as
consequências que ela teve no plano. O comportamento de cada tecnologia fica no documento do seu
assunto ([`delta.md`](delta.md), [`duckdb.md`](duckdb.md), [`redshift.md`](redshift.md),
[`parquet.md`](parquet.md), [`sqlalchemy.md`](sqlalchemy.md)), e as asserções, nas suítes de
`tests/proof_of_concept/`. Um achado que contraria [`PLAN.md`](PLAN.md) ou um arquivo de etapa
dispara a revisão desse arquivo. O estado da implementação está em
[`CURRENT_STATE.md`](CURRENT_STATE.md), e o que continua sem resposta, em
[`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md).

## O que a prova de conceito verificou

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

## O que as leituras do ambiente mostraram

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
longa ([etapa 3](PLAN-STAGE-3.md)); o motor DuckDB fixa `temp_directory` numa pasta com espaço
conferido, porque o padrão é relativo à pasta corrente ([etapa 4](PLAN-STAGE-4.md)); a
[etapa 5](PLAN-STAGE-5.md) nasceu conectando por senha, porque a autenticação por IAM e a Data API
dependem das APIs do Redshift, sem endpoint VPC no laboratório (`RS-14`) — decisão revista em
2026-09-20 pelos exemplos do ambiente alvo, que conectam pela credencial temporária do workgroup
(seção seguinte); e o `vacuum` num bucket versionado só libera espaço com a regra
`NoncurrentVersionExpiration`, que o papel não lê e `BK-14` mede pelo acumulado
([etapa 9](PLAN-STAGE-9.md)).

O ambiente definitivo não tem internet e pode não ter proxy, só um endpoint VPC do S3 (declaração do
usuário de 2026-09-19). Consequências, ainda não confirmadas por uma leitura de
`probes/diagnose_aws.py` nesse ambiente: o botocore lê `AWS_DEFAULT_REGION` ou o perfil, nunca
`AWS_REGION`, e sem região usa o endpoint global `s3.amazonaws.com`, que o endpoint VPC regional não
atende; o delta-rs lê as duas variáveis e sem nenhuma cai em `us-east-1`, ignorando a região do
perfil; sem variáveis de proxy ele alcança o endpoint de credenciais diretamente (variante
`proxy_unset`), e a exportação de `NO_PROXY` não muda nada; o STS pode estar inalcançável; nenhuma
extensão do DuckDB pode ser baixada. A biblioteca normaliza a
região nos dois sentidos, não chama o STS e carrega as extensões só da pasta configurada. A conexão do ambiente alvo pede
`redshift-serverless:GetWorkgroup` e `GetCredentials` antes da porta 5439: se essas APIs não tiverem
endpoint VPC lá, o que `RS-14` mede, resta a conexão por senha, que não passa por elas.

Outras leituras do espaço do laboratório em 2026-09-20: o IMDS responde `EINVAL`; a sessão de pytest das 03:43 UTC, com as
duas raízes, passou as três variantes de credencial do delta-rs daquele momento; o `.venv` preparado
estava sem cinco pacotes do grupo `dev` até um `uv sync --group dev`; `~/shared` resolve para
`/mnt/custom-file-systems/s3/shared`, uma montagem `fuse.s3fs`; o Athena nega `GetWorkGroup` no
grupo `primary`; a credencial do contêiner dura cerca de uma hora (expiração 04:19:03, lida às 03:23
e às 03:44); o `bucket.py` depois da sessão das 04:41 encontrou só a sessão que o
`SERIALIZE_DB_TEST_KEEP` preservou na execução das 04:38.

## O que os exemplos de conexão com o Redshift mostraram

Em 2026-09-20 o usuário executou no ambiente alvo dois scripts de conexão, guardados como foram
executados em [`../examples/`](../examples/): um pela Data API e outro pelo protocolo nativo. Os
dois terminaram com sucesso, e o relatório de execução não foi transcrito: o que eles fixam são os
parâmetros do ambiente e os caminhos que funcionam, não medições.

O ambiente alvo não é o laboratório lido nas seções acima. A região é `sa-east-1`, o Redshift é
serverless no workgroup `controladoria-wg`, a conexão é no banco `dev`, e o esquema do projeto é
`sbx_aco_decon` no banco `datalake_rw_shared`, citado por nome em três partes
`datalake_rw_shared.sbx_aco_decon.<tabela>`. O laboratório de `us-west-2` não tem Redshift nenhum.

O caminho nativo é `redshift-serverless:GetWorkgroup` para o endereço e a porta,
`redshift-serverless:GetCredentials` para o par usuário e senha derivado da identidade IAM, e
`redshift_connector.connect` com esse par. Não há senha guardada, a credencial dura no máximo uma
hora e o usuário do banco sai da role. O caminho pela Data API é assíncrono, `ExecuteStatement`,
`DescribeStatement` até o estado final e `GetStatementResult` paginado, e devolve cada célula como
um dicionário de um item, com `DECIMAL` e data e hora em texto.

Consequências no plano, nesta mesma unidade de trabalho:

- A [etapa 5](PLAN-STAGE-5.md) conecta pela credencial temporária do workgroup, e não por senha,
  que era o padrão escrito quando o projeto ainda não tinha Redshift; o IAM interno do
  `redshift_connector` ficou de reserva até a revisão dos probes do mesmo dia, que o tirou do plano,
  junto com o cluster provisionado, por ninguém os ter executado no ambiente alvo.
- A Data API fica fora da biblioteca, por decisão do usuário de 2026-09-20: ela perde o tipo do
  contrato e limita o resultado a 500 MB. Ela continua no probe (`RS-10`), na suíte e nos exemplos,
  como prova de que existe caminho sem a porta 5439.
- Toda tabela do Redshift é citada por nome em três partes. No SQLAlchemy, o esquema com ponto só
  atravessa com `quoted_name(..., quote=False)`, medido em 2026-09-20
  ([`sqlalchemy.md`](sqlalchemy.md)).
- A escrita num banco de datashare é restrita: `COPY` só sem `COMPUPDATE`, escrita num banco só por
  transação, sem `VIEW`, e `UNLOAD` fora da lista de comandos suportados ([`redshift.md`](redshift.md)).
  A [etapa 8](PLAN-STAGE-8.md) abre a transação com `BEGIN` explícito. A leitura de "`COPY` sem
  `COMPUPDATE`" como "emitir `COMPUPDATE OFF`" durou até o experimento de `COPY` e `UNLOAD` da seção
  adiante, que passou sem cláusula alguma.
- `probes/redshift.py` ganhou `RS-15` (credencial temporária), `RS-16` (o banco do esquema, local ou
  de datashare) e `RS-17` (os três requisitos da escrita num datashare: patch 186, isolamento de
  snapshot e 64 slices), e `RS-10` passou a rodar o ciclo completo da Data API com `select 1`.
- A suíte `tests/proof_of_concept/test_redshift.py` conecta pelo caminho testado e ganhou um teste
  do banco do esquema e um do ciclo da Data API. O nome em três partes e o `COMPUPDATE OFF` que ela
  passou a emitir cederam lugar, na seção adiante, ao `USE` e ao `COPY` sem cláusula.

O que os exemplos não respondem, e o probe e a suíte respondem na primeira execução: se o produtor
concedeu escrita no datashare, se o consumidor atende aos três requisitos, se o `UNLOAD` de uma
tabela do datashare é aceito, e qual papel IAM o `COPY` usa ([`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md)).

## O que a leitura do Redshift do ambiente alvo mostrou

Em 2026-09-20, às 20:37 UTC, `probes/redshift.py` rodou no ambiente alvo (Linux x86_64, Python
3.13.15) com as variáveis do workgroup. Os relatórios estão em
[`readings/`](readings/): a leitura das 20:37 e a repetição das 20:43 com a raiz S3 informada, que
mudou duas linhas. Os números abaixo saem deles.

**O que respondeu.** Workgroup `controladoria-wg` no namespace `controladoria-ns`, conta
138071776059, capacidade base 8, sem acesso público e com roteamento VPC melhorado; nenhum cluster
provisionado. Os três endpoints regionais do Redshift e o host do workgroup resolvem para IP privado
(`10.100.x.x`): o ambiente alvo tem endpoint VPC de interface para todos, e `RS-14` passou, o que
responde a dúvida que a leitura do laboratório deixou — a credencial temporária e a Data API
funcionam ali sem internet. A porta 5439 abriu em 0,00 s. A credencial temporária saiu para o
usuário `IAMR:user-533cbaba-...@3hpfa7636y4qor`, válida por uma hora, e a sessão abriu com ela. O
ciclo da Data API devolveu `select 1` em 23 ms. A versão é `1.0.436211`, muito acima do patch 186
que a escrita em datashare exige. `SUPER` e `JSON_PARSE` respondem.

**O esquema e o banco.** `svv_redshift_databases` mostra `dev` local com isolamento de snapshot e
`datalake_rw_shared` do tipo `shared`, vindo do datashare `controladoria_rw_datashare` da conta
produtora 390403891846, com isolamento `UNKNOWN`. `svv_all_schemas` põe `sbx_aco_decon` só nesse
banco, tipo `shared`: o nome em três partes está confirmado, e `RS-16` passou. `svv_all_tables`
lista três tabelas lá (`teste`, `teste3`, `new_table`), nenhuma com o prefixo da biblioteca, então a
sessão lê o esquema. A ACL do banco compartilhado nomeia só a role administrativa do SSO, não a
identidade da sessão.

**O que bloqueia.** O namespace não tem papel IAM padrão nem papel IAM associado: `COPY` e `UNLOAD`
sobre o S3 não rodam até o administrador associar um, porque o Redshift só aceita papel associado ao
namespace, com ou sem ARN explícito. `RS-6` reprova por isso, e é a pendência de
[`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md) com dono fora do projeto.

**O que a sessão pode e não pode.** `has_database_privilege(dev, CREATE)` é falso e `TEMP` é
verdadeiro, e o usuário não é superusuário nem cria banco: o sandbox de execução não pode ser uma
tabela comum em `dev`, e restam a tabela temporária e o próprio datashare, se o produtor tiver
concedido `CREATE`. Os esquemas locais visíveis são só `catalog_history` e `public`, ambos de
`rdsdb`.

**O que a sessão não consegue ler.** `stv_slices` e `stl_load_errors` são negadas a um usuário comum
(SQLSTATE 42501): o requisito de 64 slices da escrita em datashare não é verificável daqui, e a
visão de erros de carga dos clusters provisionados também não. `pg_settings` do serverless não
trouxe `timezone` nem `enable_case_sensitive_identifier`, que só `SHOW` responde.

**O diagnóstico de um `COPY` reprovado existe.** Na leitura das 20:37, `sys_load_error_detail`
estourou o tempo limite de 10 s e derrubou a conexão; na repetição das 20:43, com a raiz S3
informada, ela respondeu `0` em 1,5 s, e `svv_external_schemas` respondeu `0` logo depois
([`readings/redshift-2026-09-20-2043.txt`](readings/redshift-2026-09-20-2043.txt)). A visão é
legível, o primeiro tempo limite era o defeito do probe, e a etapa 5 lê o motivo de uma carga
recusada na própria sessão por `sys_load_error_detail`, nunca por `stl_load_errors`.

**Os defeitos do probe que esta execução expôs**, corrigidos na mesma unidade de trabalho:

- `RS-17` reprovava com o isolamento `UNKNOWN` do banco compartilhado. O isolamento exigido é o do
  banco que recebe a escrita, que fica no produtor, e o consumidor não o enxerga: `UNKNOWN` passou a
  ser leitura ausente, e a checagem sai como `note`.
- O tempo limite de leitura derruba o socket do `redshift_connector`, e as consultas seguintes
  devolviam `cannot read from timed out object`, um erro que não explica nada — foi ele que fez a
  primeira leitura declarar `svv_external_schemas` ilegível, quando a conexão é que tinha caído. A
  primeira perda passou a ser registrada, e as leituras seguintes dizem que a conexão caiu; a espera
  subiu de 10 s para 30 s.
- Cinco chamadas cuja falha é leitura do ambiente (o pacote `sagemaker_studio` fora de um espaço, as
  visões de sistema negadas a um usuário comum) entravam na seção final e no código de saída, que
  existem para o que precisa de manutenção. `Report.call` ganhou `expected=True`, que as imprime como
  `-- SEM RESULTADO` e as mantém fora da contagem.
- `RS-6` dizia só "nenhum papel padrão". Passou a separar o papel padrão, que `IAM_ROLE default`
  usa, do papel apenas associado, que serve por ARN, do caso do ambiente alvo, nenhum dos dois.

## O que o experimento de `COPY` e `UNLOAD` no datashare mostrou

Em 2026-09-20 o usuário executou no ambiente alvo um terceiro script, guardado em
[`../examples/redshift_copy_unload.py`](../examples/redshift_copy_unload.py): ele conecta pela
credencial temporária, troca o banco da sessão, cria `cad_contas` no esquema do datashare, carrega
por `COPY` a pasta Parquet da base de origem, lê o resultado e descarrega por `UNLOAD`. Passou
inteiro, e com isso fecha três questões que estavam abertas.

**O produtor concedeu escrita.** `CREATE TABLE`, `COPY`, `SELECT` e `UNLOAD` passaram em
`sbx_aco_decon`. A pergunta sobre o `GRANT` do produtor sai de
[`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md) respondida, e com ela a dúvida sobre os requisitos do
consumidor: o patch atende, e o isolamento e os slices, que a sessão não consegue ler, não
impediram a escrita.

**`USE <banco>` troca o banco da sessão.** Depois dele, `esquema.tabela` basta, e foi assim que o
`CREATE`, o `COPY` e o `UNLOAD` rodaram. A restrição documentada, de que só o nome em três partes
vale, se aplica a quem não está conectado ao banco compartilhado. O nome em três partes continua
valendo para quem abre a sessão em outro banco, como a Data API. A [etapa 5](PLAN-STAGE-5.md)
passou a rodar o `USE` em `connect`, e `qualified` voltou a duas partes.

**O `COPY` e o `UNLOAD` levam as credenciais de quem chama**, por `ACCESS_KEY_ID`,
`SECRET_ACCESS_KEY` e `SESSION_TOKEN`, em vez de `IAM_ROLE`. Isso destrava o ambiente alvo sem
depender de ninguém: o namespace não tem papel IAM associado, e sem papel associado nem um ARN
explícito funciona, mas a identidade da sessão alcança o S3. O bloqueio que `RS-6` reprovava deixa
de ser bloqueio, e a checagem virou `note`; `RS-11` passou a simular a identidade de quem chama
quando não há papel associado, e `RS-18` lê se essas credenciais existem e se têm `SESSION_TOKEN`.
Como elas expiram em cerca de uma hora, a cláusula é montada por comando, e como o texto do comando
carrega segredo, ele não entra em log, em relatório nem em arquivo: `tests/conftest.py` mascara toda
cláusula de credencial antes de gravar o relatório da sessão.

**O `COPY` sem cláusula `COMPUPDATE` é o que funciona.** A documentação lista "`COPY` sem
`COMPUPDATE`" entre os comandos que a escrita num datashare aceita, e eu tinha lido isso como
"emitir `COMPUPDATE OFF`". O comando que passou não tem cláusula alguma, e a suíte e as etapas 5 e 8
passaram a emiti-lo assim. Não há `COMPUPDATE OFF` a testar: o `COPY` de Parquet não aceita o
parâmetro nem aplica compressão automática ([`redshift.md`](redshift.md), "Regras do COPY para
Parquet"), e a regra do datashare está satisfeita por construção.

Duas leituras menores que o script deixou: o `COPY` lê um prefixo de pasta Parquet direto, sem
manifesto, e converte `int32` da origem para a coluna `BIGINT` do contrato; e o DDL de `cad_contas`
gerado do esquema da base de origem ([`../tests/source_db_projetado.py`](../tests/source_db_projetado.py))
foi aceito como está, com chave primária e unicidade informativas e `DISTSTYLE ALL`.

## O que os comandos com manifesto no datashare mostraram

Em 2026-09-21 o usuário executou [`../examples/redshift_manifest.py`](../examples/redshift_manifest.py)
no ambiente alvo, sobre 500.000 linhas da partição `data_str=2026-02-28` de `cad_contratos`. O ciclo
inteiro passou: a partição Parquet virou tabela Delta, o manifesto do `COPY` saiu das ações `add`, o
`COPY ... MANIFEST` carregou a staging, o `INSERT` acrescentou a coluna de partição, o
`UNLOAD ... PARTITION BY ... MANIFEST VERBOSE` gravou de volta e `create_write_transaction`
registrou os arquivos numa tabela Delta que devolveu as 500.000 linhas.

| Comando | Tempo |
| --- | --- |
| `USE datalake_rw_shared` | 10,8 s |
| `CREATE TABLE` da staging | 0,9 s |
| `COPY ... FORMAT AS PARQUET MANIFEST` de 500.000 linhas | 4,6 s |
| `INSERT ... SELECT *, '<valor>'` | 3,5 s |
| `UNLOAD ... PARTITION BY (data_str) MANIFEST VERBOSE` | 0,8 s |

O `USE` é o primeiro comando da sessão e pagou 10,8 s; os seguintes ficaram abaixo de 5 s. É a
medição que confirma a retirada do `timeout` de 10 s do socket em 2026-09-20: ele teria matado a
conexão no primeiro comando, antes de qualquer trabalho.

As duas perguntas que estavam em [`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md) foram respondidas, e as
duas com sim:

- `COPY ... FORMAT AS PARQUET MANIFEST` numa tabela de datashare se comporta como numa tabela local.
  O manifesto tinha uma entrada, apontando para o arquivo que o delta-rs gravou, com
  `meta.content_length` igual ao `size_bytes` da ação `add`.
- `UNLOAD ... PARTITION BY (<coluna>) MANIFEST VERBOSE` a partir de uma tabela de datashare é
  aceito. A alternativa que a [etapa 5](PLAN-STAGE-5.md) guardava, um `UNLOAD` por partição, deixa
  de ser necessária.

O `PARTITION BY` gravou na convenção Hive, `data_str=2026-02-28/`, com a coluna de partição fora dos
arquivos, e o manifesto em `<prefixo>/manifest`, sem extensão. É o leiaute que o
`partition_values` da `AddAction` lê do caminho.

### Os arquivos que o UNLOAD grava

O rodapé de um arquivo respondeu os três itens da [etapa 0](PLAN-STAGE-0.md):

| Coluna | Tipo físico | Tipo lógico |
| --- | --- | --- |
| `id_contrato BIGINT` | `INT64` | `Int(bitWidth=64, isSigned=true)` |
| `data DATE` | `INT32` | `Date` |
| `contrato VARCHAR(100)` | `BYTE_ARRAY` | `String` |
| `taxa_juros_fixos DOUBLE PRECISION` | `DOUBLE` | nenhum |
| `taxa_decimal DECIMAL(18, 2)` | `FIXED_LEN_BYTE_ARRAY(8)` | `Decimal(precision=18, scale=2)` |
| `data_hora TIMESTAMP` | `INT96` | nenhum |

Toda coluna saiu `optional`, inclusive `id_contrato`, `data` e `contrato`, que eram `NOT NULL` na
tabela de origem. As estatísticas de mínimo e máximo estão presentes (lido em `id_contrato`).

O `DECIMAL(18, 2)` em `FIXED_LEN_BYTE_ARRAY(8)` é o que o PyArrow grava e não o que gravam o
delta-rs, o DuckLake e o DuckDB, que usam `INT64` ([`estrategia.md`](estrategia.md)): uma tabela
Delta alimentada pelo `UNLOAD` e pela biblioteca fica com duas codificações físicas da mesma coluna
lógica, que os leitores leem igual.

O `TIMESTAMP` em `INT96` é o achado que contraria o contrato: [`schema.md`](schema.md) fixa
`timestamp_ntz` de microssegundos, a carga inicial tira o `INT96` da base de origem, e o `UNLOAD` o
traz de volta na exportação. O modelo de referência tem colunas `timestamp`, então o caso é real.
Uma sondagem no macOS no mesmo dia registrou um arquivo `INT96` numa tabela Delta declarada
`timestamp_ntz`, pelo mesmo `create_write_transaction` do exemplo, e leu a coluna de volta: o
delta-rs devolveu `timestamp[us]` e o `delta_scan` do DuckDB devolveu `TIMESTAMP`, os dois com os
valores idênticos aos gravados. O custo é a estatística: o `INT96` não tem mínimo e máximo, então a
coluna de timestamp de um arquivo do `UNLOAD` não poda.

### A fragmentação por slice

As 500.000 linhas saíram em 32 arquivos, cerca de 15.600 linhas cada, sem `MAXFILESIZE`. O
`MAXFILESIZE` é teto e não piso, então não junta arquivo: quem controla a quantidade é
`PARALLEL OFF`, que grava em série num arquivo só e respeita o `ORDER BY`, ou uma compactação
posterior na camada Delta. O `export_partition` da [etapa 5](PLAN-STAGE-5.md) recebeu a
consequência.

A soma de `meta.record_count` das entradas bateu com as 500.000 linhas, que é a conferência que o
`register_files` faz antes do commit. O exemplo gravou `minValues`, `maxValues` e `nullCount`
vazios e ficou como rodou, conforme a regra de [`../examples/README.md`](../examples/README.md); o
`register_files` da [etapa 3](PLAN-STAGE-3.md) os preenche do rodapé, que agora se sabe que os tem.


### O que o `create_write_transaction` confere

Uma sondagem no macOS em 2026-09-21 (deltalake 1.6.4, DuckDB 1.5.5, PyArrow 25.0.1) registrou ações
`add` erradas de propósito, e `tests/proof_of_concept/test_deltalake.py` guarda os casos como
asserções (`test_create_write_transaction_trusts_path_and_stats`,
`test_create_write_transaction_trusts_file_schema`, `test_compact_rewrites_files_from_another_writer`):

| Ação registrada | Commit | delta-rs (`to_pyarrow_table`) | DuckDB (`delta_scan`) | DataFusion (`QueryBuilder`) |
| --- | --- | --- | --- | --- |
| Caminho que não existe | Passa | Erro em toda leitura que toca a partição; `filters=` em outra partição lê normal | `IOException` na leitura da tabela; `WHERE` em outra partição lê normal | não medido |
| Estatística falsa (`id_operacao` de 160000 a 160009 declarado de 900000 a 900010, `numRecords` 999) | Passa | `filters=` na faixa verdadeira devolve 0 linhas | 0 linhas e `Scanning Files: 0/3`; `count(*)` sem filtro lê os arquivos (1.010) | 0 linhas; `count(*)` sem filtro responde 1.999 pelas ações |
| Arquivo sem uma coluna `NOT NULL` | Passa | Nulo em toda linha | Nulo em toda linha | não medido |
| `valor` como texto não numérico numa coluna `decimal` | Passa | `ArrowInvalid` ao ler a partição | `count(*)` passa, a leitura da coluna falha | não medido |
| Coluna a mais, `int32` numa coluna `long`, colunas em outra ordem, `timestamp[ns]` numa `timestamp[us]` | Passa | Lê certo | Lê certo | não medido |

O `write_deltalake` com os mesmos dez nulos na coluna `NOT NULL` recusa (`DeltaError: 10 rows failed
validation check`). `restore(0)` sobre a tabela com o caminho inexistente voltou a ler. O erro do
delta-rs no caminho inexistente depende do `size` da ação, que o leitor também obedece: 1 byte
declarado dá o erro do rodapé, um tamanho plausível dá `FileNotFoundError`. `mode="overwrite"` com
`partition_filters` no `create_write_transaction` removeu só o arquivo da partição filtrada.
`get_add_actions()` devolve `max.id_operacao` 900.010 e a soma de `num_records` 1.999, os valores da
ação. E `optimize.compact` sobre dois arquivos registrados com `INT96` e `FIXED_LEN_BYTE_ARRAY`
gravou um arquivo com os dois em `INT64` e estatística em toda coluna, timestamp incluído. As
consequências estão em [`PLAN-STAGE-3.md`](PLAN-STAGE-3.md), seção "As conferências do registro de
arquivos", em [`PLAN-STAGE-5.md`](PLAN-STAGE-5.md) (`export_mode`) e em [`redshift.md`](redshift.md).

## O que a leitura da base de origem mostrou

Em 2026-09-20, `probes/parquet_source.py --sample 2000` leu a base de desenvolvimento
`db_projetado` (`/mnt/bndes_grupos_bases_analise_financeira/databases/dsv/db_projetado`, Linux,
Python 3.13.15, o `.venv` do projeto): 14 pastas de tabela, 205 arquivos, 3.757.237.689 bytes,
187.340.644 linhas, `schema.json` solto na raiz, nenhum arquivo ilegível, nenhuma chamada falhada e
nenhuma checagem reprovada. Todo arquivo de cada tabela tem o mesmo esquema (`PQ-3`), cada tabela
usa um só estilo e uma só profundidade de partição (`PQ-4`), a coluna de partição nunca está dentro
do arquivo (`PQ-5`), e 7 das 78 colunas têm arquivo sem mínimo e máximo (`PQ-6`): os dois
`timestamp` em `INT96`, `meta` e `data_fim_validade`, inteiramente nulas, e `data_assinatura`,
`id_negocio` e `departamento` nos arquivos em que estão inteiramente nulas.

O layout: as dez tabelas sem partição (`alembic_version`, `cad_aliquotas`, `cad_contas`, as cinco
`dom_*`, `meta_update_status`, `rel_contas_hierarquias`) têm um único `chunk_0.parquet` na raiz da
tabela; `cad_contratos` (8 arquivos, 6,5 milhões de linhas), `cad_operacoes` (13, 11,0 milhões) e
`rel_contrato_operacao` (30, 27,9 milhões) são Hive por `data_str` com os valores `2026-02-28`,
`2026-03-31` e `2026-06-30`, e `cad_lancamentos` (144 arquivos, 141,9 milhões de linhas, 2,83 GB) é
Hive por `data_base_str` com `2026-01-31` a mais; o valor do caminho é igual a `data`, ou a
`data_base`, em toda linha da partição, e `meta_update_status` registra cada carga com a partição
em JSON (`{"data_base": {"__type__": "date", "value": "2026-01-31"}}`). Os arquivos têm até
1.000.000 de linhas num row group, `chunk_<n>` sem zeros à esquerda, SNAPPY, `PLAIN` e `RLE` sem
dicionário, `parquet-cpp-arrow`, formato 1.0, a chave `pandas` no rodapé e nenhum `field_id`. Os
seis tipos: `int32` (30 colunas), `string` (23), `date32` (10), `double` (10), `bool` (3) e
`timestamp[ns]` em `INT96` (2).

O modelo de referência de `tests/model/` bate com os arquivos: as 12 tabelas, as colunas na mesma
ordem, os tipos da tabela de `schema.md` e a nulidade, exceto sete colunas de `cad_contratos`
anuláveis nos arquivos e `NOT NULL` no modelo, sem nulo nos dados; `alembic_version` e
`meta_update_status` só existem na origem. Os valores: `valor` de `cad_lancamentos` vai de
`-11.846.195.394,628` a `11.846.195.394,628`, com três casas; `fator` de `cad_aliquotas` tem cinco
(`0,59895`); `id_lancamento` chega a 1.113.599.996 em `int32` com 141,9 milhões de linhas (ids
esparsos) e `id_rel_contrato_operacao` a 556.941.030; `data` de `cad_lancamentos` vai até
`2026-12-31` enquanto `data_base` para em `2026-06-30`; `data_assinatura` é nula em 53,9% das
linhas, `id_negocio` em 59,8%, `meta` em todas; `id_veiculo` é sempre 1 e `id_mensuracao` sempre 2;
seis colunas `double` têm `-0.0` como mínimo; `cad_lancamentos.contrato` tem `desemb-999`, acima do
máximo de `cad_contratos.contrato`, e a partição `data_base` 2026-01-31 não tem `cad_contratos`
dessa data, então a chave estrangeira do modelo tem órfãos; `rel_contrato_operacao.contrato` tem
mínimo abaixo do de `cad_contratos`.

A sondagem do mesmo dia (macOS, PyArrow 25.0.1, DuckDB 1.5.5): o PyArrow lê o `INT96` como
`timestamp[ns]` e mantém a parte sub-microssegundo; `coerce_int96_timestamp_unit="us"` e o
`TIMESTAMP` do DuckDB a truncam em silêncio; `cast(safe=True)` de `[ns]` para `[us]` recusa quando
ela não é zero; o `INT96` não tem estatística; o formato 1.0 sem `INT96` recusa nanossegundos
(`would lose data`); as codificações da origem saem de `use_dictionary=False`, `version="1.0"` e
`use_deprecated_int96_timestamps=True`. O `read_parquet` do DuckDB com `hive_partitioning=true`
converte `data_str` a `DATE` (`hive_types_autocast`); o dataset do PyArrow a mantém `string`.
`tests/source_db_projetado.py` reproduz a estrutura em 64 arquivos e 425 KB, e o probe rodado sobre
ela imprime a seção 3 idêntica à da base real.

O `schema.json` da raiz, colado pelo usuário em 2026-09-20, é o controle de esquema da biblioteca
anterior no formato da reflexão do SQLAlchemy: por tabela, as colunas com tipo (`INTEGER`,
`VARCHAR`, `DATE`, `BOOLEAN`, `DOUBLE_PRECISION`, `TIMESTAMP`), nulidade e chave primária, as chaves
estrangeiras, os índices e as restrições de unicidade. A nulidade dos arquivos é a dele, inclusive
nas sete colunas de `cad_contratos`; as chaves estrangeiras compostas do modelo de referência
(`cad_contratos` para `rel_contrato_operacao`, `rel_contrato_operacao` para `cad_operacoes`,
`cad_lancamentos` para `cad_contratos`) não constam nele, o que explica os órfãos; os índices e as
restrições de unicidade são os do modelo. `tests/source_db_projetado_schema.json` é a cópia, e
`tests/test_source_db_projetado.py` confere que as colunas, os tipos e a nulidade dele são os dos
arquivos.

Consequências no plano, pelas decisões do usuário de 2026-09-20 registradas nas premissas de
[`PLAN.md`](PLAN.md): a partição é por data em texto `AAAA-MM-DD`, declarada pelo modelo com a
coluna de data de que deriva, e o vocabulário de mês do plano virou partição
([`PLAN-STAGE-1.md`](PLAN-STAGE-1.md) a [`PLAN-STAGE-9.md`](PLAN-STAGE-9.md),
[`serialize-db.md`](serialize-db.md), [`schema.md`](schema.md)); as colunas numéricas continuam
`Double`, sem arredondamento, com `Numeric(18, 2)` como melhoria futura; as chaves passam a
`int64`; o `timestamp` `INT96` vira `INT64` de microssegundos; a nulidade é a do modelo; as
inconsistências da base de desenvolvimento são ignoradas, e a base fictícia é consistente, com a
relação N×N de `rel_contrato_operacao` e `fator_rateio` somando 1 por operação. A memória por
partição de `cad_lancamentos` continua em [`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md).

## O que o proxy com autenticação mostrou

Em 2026-09-20, `prepare_offline.sh` parou na instalação das extensões do DuckDB, na rede
corporativa do usuário, com `InvalidInputException: Failed to parse http_proxy
'http://<usuário>:<senha>@proxy01.bndes.net:8080' into a host and port`. Uma sonda no scratchpad
com DuckDB 1.5.5 (macOS arm64, um proxy de mentira em `127.0.0.1` que registra o pedido e responde
407) mediu o comportamento:

- O DuckDB tem três configurações de proxy, `http_proxy`, `http_proxy_username` e
  `http_proxy_password`, e nenhuma equivalente a `NO_PROXY`. A descrição de `http_proxy` em
  `duckdb_settings()` é "HTTP proxy host (defaults to the HTTP_PROXY environment variable when
  unset)": só a grafia maiúscula é lida, e a minúscula sozinha não tem efeito nenhum.
- O valor lido do ambiente não aparece em `duckdb_settings()`, que mostra `''`; ele é interpretado
  na hora do pedido HTTP, e é aí que o endereço com credenciais embutidas é recusado.
- `SET http_proxy` sobrepõe a variável de ambiente e aceita `host:porta` e `http://host:porta`, as
  duas formas com o mesmo resultado. Com `http_proxy_username` e `http_proxy_password`, o pedido
  chega ao proxy com `Proxy-Authorization: Basic` sobre `usuário:senha` sem URL-encode.

Uma segunda sonda, no mesmo dia, mediu o alcance do erro e o que o DuckDB lê:

- O erro não é do download de extensão: um `glob('s3://.../**')` pelo `httpfs`, com as extensões já
  em disco, falha com a mesma `InvalidInputException`. Toda chamada HTTP do DuckDB passa por ali.
- O DuckDB ignora `HTTPS_PROXY` e a grafia minúscula: com só uma delas, e com o mesmo endereço com
  credenciais, o pedido saiu direto, sem erro e sem proxy.

A separação é de `probelib.duckdb_proxy`, usada pelo `prepare_offline.sh`, por `probes/space.py` e
pelo subprocesso de `probes/diagnose_aws.py`: tira o usuário e a senha do endereço, lê-os das
variáveis `username` e `password` e, sem elas, usa os embutidos com URL-decode. Ela lê só
`HTTP_PROXY`, a variável e a grafia que o DuckDB lê, porque as configurações não têm exceção
equivalente a `NO_PROXY` e tirar o endereço de outra variável mandaria ao proxy o tráfego que hoje
sai direto; as demais grafias presentes entram na leitura do relatório. As sondas exercitaram
credenciais nas variáveis, só embutidas, ausentes, endereço sem esquema, endereço sem host e
ambiente sem proxy, e a execução do script sem proxy instalou `httpfs`, `delta` e `aws`.

A mesma leitura achou um vazamento nos relatórios: `environment_rows` do `probelib` e
`show_environment` do `diagnose_aws` imprimiam `HTTP_PROXY` inteiro, com a senha embutida, num
arquivo feito para ser colado na conversa. `hide_credentials` troca o usuário e a senha por `***` em
toda variável de proxy.

O procedimento de uso, as variáveis e os comandos de empacotar e extrair passaram para o cabeçalho
do script, e a seção "Ambiente sem internet" do `README.md` aponta para ele. A etapa 3 aplica a
mesma separação em `duckdb_setup` ([`PLAN-STAGE-3.md`](PLAN-STAGE-3.md)).

## O que o encerramento depois de uma leitura Delta mostrou

Em 2026-09-20, em macOS arm64 com deltalake 1.6.4 e PyArrow 25.0.1, um script que termina logo
depois de `DeltaTable(uri).to_pyarrow_table()` **não encerra**: `sample` mostra a thread principal
em `exit` → `__cxa_finalize_ranges` → `arrow::internal::ThreadPool::~ThreadPool` →
`Shutdown` → `condition_variable::wait`, e uma worker do Arrow parada numa cadeia de callbacks do
Acero (`AsyncTaskSchedulerImpl::OnTaskFinished`). É corrida, não travamento do comando: três
execuções seguidas penduraram, e meio segundo de qualquer trabalho depois da leitura desfaz o
problema (0,8 s contra 15 s sem saída).

| Caso | Saída |
| --- | --- |
| `to_pyarrow_table()` e encerrar | não encerra (3 de 3) |
| `to_pyarrow_table()` e 0,5 s de espera | 0,8 s |
| `to_pyarrow_table()` e 3 s de espera | 3,3 s |
| `to_pyarrow_dataset().to_table()` e encerrar | 0,3 s |
| `to_pyarrow_dataset().scanner().to_reader().read_all()` e encerrar | 0,2 s |
| `to_pyarrow_dataset().count_rows()` e encerrar | 0,2 s |
| `write_deltalake`, `get_add_actions`, `create_write_transaction` e encerrar | 0,3 s cada |

A partição não importa: a tabela sem partição pendura igual. `pq.read_table`, `ds.dataset().head()`
e `ds.dataset().to_table()` sozinhos encerram limpos, então o gatilho é o caminho de leitura do
`to_pyarrow_table` do delta-rs, não o Parquet nem o dataset do PyArrow.

A suíte nunca viu isso porque o pytest sempre tem trabalho depois da última leitura; quem vê é um
script ou um comando que lê e termina. A consequência está nas regras de [`PLAN.md`](PLAN.md): um
programa que encerra logo depois de ler uma tabela Delta lê por `to_pyarrow_dataset()`. Foi assim
que o defeito apareceu — o ensaio local de
[`../examples/redshift_manifest.py`](../examples/redshift_manifest.py) imprimiu tudo e ficou 30
minutos sem encerrar.

## O que a fronteira por lotes mostrou

Em 2026-09-20, no macOS arm64 com DuckDB 1.5.5 (`threads = 2`), PyArrow 25.0.1 e pandas 3.0.6, uma
sondagem no scratchpad e as asserções acrescentadas a `test_pyarrow.py`, `test_duckdb.py` e
`test_parallel.py` mediram o que a troca de dados por `RecordBatch` exige. A revisão do plano está em
[`PLAN.md`](PLAN.md), seção "A troca de dados com o código cliente", nas etapas
[1](PLAN-STAGE-1.md), [3](PLAN-STAGE-3.md), [4](PLAN-STAGE-4.md), [5](PLAN-STAGE-5.md) e
[6](PLAN-STAGE-6.md), e em [`serialize-db.md`](serialize-db.md), seção "Lotes em streaming".

- O leitor de `to_arrow_reader` num `cursor()` próprio entregou o snapshot da consulta (1.000.000 de
  linhas) enquanto outro cursor inseria dez linhas na mesma tabela, criava, alterava e apagava
  tabelas; no mesmo cursor, o comando seguinte o esvazia. Fechar o cursor no meio não o interrompeu
  (900.010 linhas lidas depois), e cem `cursor()` mais `close()` levaram 0,4 ms.
- 20.000.000 de linhas em três colunas: `to_arrow_table` em 0,59 s, 485 MB de tabela e 567 MB de
  processo (`ru_maxrss`, um subprocesso por cenário); `to_arrow_reader(100_000)` em 0,54 s e 83 MB;
  com uma thread de pré-busca e fila de dois lotes, 0,47 s e 89 MB. Sem `ORDER BY`, o primeiro lote
  em 3 ms; com `ORDER BY`, 2,4 s no `execute` e o primeiro lote em seguida. A suíte repete com
  10.000.000 de linhas: 322 MB contra 83 MB.
- Pré-busca em thread contra a sequência, 6.000.000 de linhas em lotes de 200.000: com o trabalho em
  pandas por lote, 0,201 s contra 0,160 s (1,25x; só a leitura, 0,157 s); com um laço Python puro
  sobre 20.000 valores por lote, 0,250 s contra 0,162 s (1,55x).
- Escrita de 6.000.000 de linhas: um `INSERT` sobre um leitor da fila numa thread, 0,167 s em lotes
  de 200.000, 0,201 s em lotes de 100.000 e 0,288 s em lotes de 20.000; um `INSERT` por lote numa
  transação na thread auxiliar, 0,219 s, 0,412 s e 0,532 s; um `INSERT` por lote na thread do
  cliente, sem threads, 0,343 s em lotes de 200.000. O `INSERT` por lote numa transação não deixou
  linha visível a outro cursor antes do `commit`, e o `rollback` na falha do lote 2 deixou 0 linhas.
  O pipeline de três estágios sobre 3.000.000 de linhas, na suíte: 0,127 s encadeado, 0,178 s lote a
  lote sem threads, 0,131 s pela tabela inteira.
- O `INSERT` sobre um leitor de gerador Python é atômico (a falha no lote 20 deixou a tabela com as
  50.000 linhas anteriores, com a mensagem do cliente dentro da `InvalidInputException`), mas o
  `arrow_scan` o puxa por uma thread de leitura antecipada do Arrow (`BackgroundGenerator`, na pilha
  nativa lida com `sample`): o gerador rodou em outra thread, tinha entregado de 5 a 15 lotes quando
  o comando falhou no primeiro e chegou a 10 ou 20 depois da falha. Um gerador preso num
  `queue.get()` sem prazo nessa thread pendurou o processo na saída, no destrutor do pool de threads
  do Arrow; com prazo, a thread ainda chamando Python na saída foi pendurada pelo CPython 3.13
  (`PyThread_hang_thread`), com o mesmo resultado. O `Loader` da suíte insere lote a lote numa
  transação e não entrega gerador ao DuckDB; as threads dos esboços não referenciam o objeto, para um
  stream ou loader abandonado ser coletado e a thread terminar.
- `RecordBatchReader.from_batches` não confere os lotes contra o esquema declarado:
  `read_next_batch` devolve o lote como veio, `read_all` acusa `Schema at index 0 was different`, e o
  `arrow_scan` lê os buffers pelo esquema declarado, então `(1, 1.0)` com as colunas trocadas entrou
  como `(4607182418800017408, 5e-324)`; uma coluna a mais ou a menos falha. A nulidade do esquema
  Arrow não é conferida pelo DuckDB; a coluna `NOT NULL` da tabela é. `RecordBatch.cast` recusa o
  mesmo que `Table.cast`. O `close()` de um leitor sobre gerador não encerra o gerador, que só termina
  no descarte.
- As conversões do lote não copiam: `to_batches(max_chunksize=100_000)` de 300.000 linhas em
  0,04 ms, `Table.from_batches` em 0,003 ms, `RecordBatch.to_pandas(types_mapper=pd.ArrowDtype)` de
  100.000 linhas em 1,4 ms e `RecordBatch.from_pandas` em 0,5 ms, com os buffers compartilhados. Um
  objeto com `__arrow_c_stream__` é aceito por `RecordBatchReader.from_stream`, pelo `register` do
  DuckDB e por `write_deltalake`.
- Um erro que a consulta encontra no meio da leitura chega ao Python como `OSError` com a mensagem
  do DuckDB (`Conversion Error: Could not convert string 'x' to INT32`), não como
  `duckdb.ConversionException`.

Consequências: a fronteira do plano passou de `pa.Table` a lotes `RecordBatch` com `stream` e
`loader`, e a `pa.Table` ficou como conveniência; `loader` insere lote a lote numa transação; o
`cast` por lote virou barreira de segurança; a regra das threads da biblioteca admite a auxiliar de
cada stream e loader; e o `fetchmany` do `redshift_connector` entrou em
[`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md). Suíte: 110 passam e 60 são pulados sem variável; 148 e
22 com a raiz local (2026-09-20, macOS).

## O que a leitura do ambiente alvo em 2026-09-21 mostrou

Em 2026-09-21, entre 03:47 e 03:51 UTC, o usuário executou os cinco probes no ambiente alvo (Linux
x86_64, Python 3.13.15, o `.venv` do projeto preparado por `prepare_offline.sh`), com a raiz
`s3://bndes-aco-models-138071776059/dzd-5qqmzj3amjp657/3hpfa7636y4qor/shared/serialize-db-tests`.
Os relatórios estão em `secrets/probes-aws-bn/`, fora do git, por escolha do usuário
([`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md)); os números abaixo saem deles.

**A máquina** (`space.py`): 2 vCPUs, 7,6 GiB de memória, 29,8 GiB livres de 37,0 GiB num disco só,
que serve `HOME`, `/tmp` e o repositório; `ulimit -n` 65536. O DuckDB 1.5.5 `linux_amd64` nasce com
2 threads, `memory_limit` 6,1 GiB e `temp_directory` `.tmp`, e carregou `httpfs`, `delta` e `aws` de
`.duckdb/` da pasta preparada (`SP-10`); o `.venv` tem o grupo `dev` nas versões fixadas (`SP-9`).
`uv`, `git`, `aws` e `duckdb` estão no `PATH`, `gh` não; `~/shared` não existe. O Python do sistema
(`/opt/conda`) é 3.12.14 com deltalake 1.6.3, DuckDB 1.5.1, PyArrow 21.0.0 e `sagemaker_studio`
1.1.32, que falha com `ProfileNotFound (DomainExecutionRoleCreds)`: as conexões do projeto não são
legíveis dali, e a biblioteca não depende delas.

**A rede** (`space.py`, `diagnose_aws.py`, `catalog.py`): nenhuma variável de proxy, em nenhuma
grafia, e a internet inalcançável (`Network is unreachable` para o PyPI em 20 s), o que confirma a
declaração de 2026-09-19. Os nomes do S3 (`s3.sa-east-1.amazonaws.com`, o global e o do bucket)
resolvem para IP público e a porta 443 conecta em 0,00 s: é o endpoint de gateway. Têm endpoint de
interface com DNS privado o STS, as três APIs do Redshift e o host do workgroup, o Glue, o Athena, o
Secrets Manager e o DataZone. Resolvem para IP público e não respondem o KMS (`describe_key` esperou
80 s), o IAM (`iam.amazonaws.com`, `simulate_principal_policy` esperou 10 s em `bucket.py` e em
`redshift.py`), o SageMaker, o Lake Formation (30 s) e o S3 Tables (31 s). As credenciais vêm do
endpoint do contêiner (`container-role`), duram cerca de uma hora (expiração 04:31:35 lida às 03:48),
e `AWS_REGION` e `AWS_DEFAULT_REGION` estão as duas em `sa-east-1`, sem `~/.aws/config`.

**O diagnóstico da suíte S3** (`diagnose_aws.py`): o `boto3`, o delta-rs como encontrado e o DuckDB
com as extensões da pasta listaram o prefixo; a região saiu igual nos dois clientes; o STS respondeu
pelo endpoint privado; "suíte S3 como está". A pergunta de manutenção da suíte S3 para o cenário sem
proxy sai de [`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md) respondida: nada a fazer, e
`prepare_environment` da [etapa 3](PLAN-STAGE-3.md) fica como está, para um ambiente que tenha só
uma das variáveis.

**O bucket** (`bucket.py`): `bndes-aco-models-138071776059` em `sa-east-1`, SSE-KMS pela chave
`55dd0bd2-f2f2-44be-9a62-bd4264a2ef45` com bucket key, SSE-C bloqueado, acesso público bloqueado,
versionado pela amostra (`VersionId` no marcador de pasta); o papel não lê versionamento, Object Lock,
propriedade, ciclo de vida, política nem uploads incompletos, como no laboratório. Sob a raiz de
testes há um marcador de pasta de 2026-09-20 e nada mais: nenhuma tabela Delta, nenhuma sessão da
suíte, nenhuma versão não corrente. A suíte S3 ainda não rodou lá.

**O catálogo** (`catalog.py`): o Glue tem o banco `glue_db_5feoihj3bbzkt7` com uma tabela Parquet e
nenhum catálogo federado; o Athena tem três workgroups, com `GetWorkGroup` negado em `primary`; o
Lake Formation e o S3 Tables não respondem. O gatilho de reavaliação de
[`estrategia.md`](estrategia.md) não disparou.

**O Redshift** (`redshift.py`): o que a leitura de 2026-09-20 mostrou se repetiu (workgroup,
namespace sem papel IAM, endpoints privados, credencial temporária de uma hora, versão `1.0.436211`,
`CREATE` negado e `TEMP` permitido em `dev`, `stv_slices` negada), e `sys_load_error_detail`
respondeu `0` em 2,4 s. Leituras novas: `enable_case_sensitive_identifier` `off`, `datestyle`
`ISO, MDY`, `statement_timeout` 0, `wlm_query_slot_count` 1, `search_path` `$user, public`. O
`select 1` da Data API ficou 30 s em `PICKED` sem terminar, contra 23 ms em 2026-09-20. E `RS-19`
reprovou: depois de `USE datalake_rw_shared`, `select current_database()` respondeu `dev`. O
critério estava errado, não o `USE`: os exemplos de 2026-09-20 e 2026-09-21 rodaram `CREATE`,
`COPY`, `INSERT` e `UNLOAD` por `sbx_aco_decon.<tabela>` depois do mesmo `USE`, e o usuário
confirmou no mesmo dia que o `USE` vale e `current_database()` não o reflete. Como `RS-5` e `RS-8`
dependiam de `RS-19`, `has_schema_privilege` e `svv_table_info` continuam por ler depois do `USE`.

Consequências no plano, nesta mesma unidade de trabalho:

- A [etapa 5](PLAN-STAGE-5.md) confirma o `USE` resolvendo um nome em duas partes (o `CREATE TABLE IF
  NOT EXISTS` da tabela de controle), nunca por `current_database()`; `RS-19` passou a resolver uma
  tabela listada por `svv_all_tables` e a registrar `current_database()` como leitura, e
  `tests/proof_of_concept/test_redshift.py` deixou de exigir o banco do datashare nessa função.
- A [etapa 4](PLAN-STAGE-4.md) nasce com o banco DuckDB em arquivo, `memory_limit` explícito abaixo
  dos 6,1 GiB que o DuckDB tomaria e `temp_directory` conferido: 7,6 GiB e 29,8 GiB livres não cabem
  uma tabela materializada de doze partições de `cad_lancamentos` em memória, e cabem em disco. O
  `export_mode="register"` fica reforçado como caminho das partições grandes
  ([etapa 7](PLAN-STAGE-7.md)).
- [`PLAN.md`](PLAN.md) ganhou a regra do ambiente alvo: a biblioteca não chama o IAM nem o KMS, a
  permissão sobre a raiz é provada pela primeira escrita, e a criptografia SSE-KMS é aplicada pelo S3.
- Os probes deixaram de chamar o IAM e o KMS sem antes testar o endereço. O `connect_timeout` do
  botocore vale em cada endereço que o nome resolve, vezes as tentativas: `short_config()`, 5 s e
  duas tentativas, dá os 10 s do IAM, que resolve para um endereço, e os 80 s do KMS, cujo endpoint
  regional resolve para oito (contagem do macOS em 2026-09-21; a leitura do alvo não registrou os
  endereços). Medição do mesmo dia contra `10.255.255.1`, privado e sem rota: `describe_key` esperou
  10,0 s com a configuração antiga e 2,0 s com `short_config(2, 5, 1)`, e `probelib.endpoint_reachable`
  respondeu em 2,0 s. As duas chamadas passaram a `short_config(2, 5, 1)` atrás desse teste, e sem
  resposta `BK-8`, `BK-9` e `RS-11` ficam como leitura. A espera no alvo, onde os nomes resolvem para
  outros endereços, a próxima execução dos probes mede.
- A Data API em `PICKED` e a decisão de copiar ou não os relatórios para `docs/readings/` continuam em
  [`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md).

## O que os rascunhos das etapas mostraram

Em 2026-09-21, no macOS arm64 com deltalake 1.6.4, DuckDB 1.5.5, PyArrow 25.0.1, SQLAlchemy 2.0.54,
duckdb-engine 0.17.0 e sqlalchemy-redshift 1.0.0, os rascunhos das etapas 1 a 9 rodaram no
scratchpad e estão em cada `PLAN-STAGE-<n>.md`, seção "Rascunhos executados". O que eles mostraram
além do que já estava medido:

- O commit de `optimize.compact` grava `dataChange` falso nas ações `add` e `remove`, com
  `partitionValues`; `version_diff` lê o log e ignora essas ações, então uma compactação não recarrega
  partição nenhuma no Redshift. A compactação de uma partição com um só arquivo não commita, e a
  versão não muda; `numFilesAdded` e `numFilesRemoved` das métricas conferem com o log (1 e 3).
- `alter.add_columns` recebe o tipo Delta do campo (`Field(nome, <PrimitiveType>)`); o texto do tipo
  (`PrimitiveType("string")`) é recusado com `invalid type string`.
- `get_add_actions(flatten=True)` devolve uma tabela `arro3`, que `pa.table(...)` converte pelo
  PyCapsule sem cópia; `pc.max` sobre a coluna `arro3` falha.
- Um lote de `fetchmany` montado por colunas (`zip(*linhas)` e `pa.array(coluna, type=...)`) levou
  0,03 s para 200.000 linhas em quatro colunas (`int64`, `decimal128(18, 2)`, `date32`, `string`),
  contra 0,10 s por `RecordBatch.from_pylist` de dicionários; a primeira chamada de cada forma paga
  a importação preguiçosa (0,16 s e 0,24 s). O `stream` do motor Redshift monta por colunas.
- `RecordBatch.from_arrays(colunas, schema=...)` converte cada coluna ao tipo do esquema e levanta
  `ArrowInvalid` numa escala perdida (`Rescaling Decimal value would cause data loss`); o `cast` da
  etapa 1 embrulha a chamada e devolve `ContractError`.
- O `autoincrement` padrão de uma coluna é a string `"auto"`, não `True`; `check_models` reprova os
  dois numa chave inteira.
- O DDL do `duckdb_engine` escreve `NUMERIC(18, 2)`, `DOUBLE PRECISION` e `TEXT`, que o DuckDB
  registra como `DECIMAL(18,2)`, `DOUBLE` e `VARCHAR`.
- `vacuum` com a retenção de 400 dias não lista nada numa tabela com versões intermediárias de hoje:
  a retenção é a janela em que toda versão continua legível, e `keep_versions` só faz diferença fora
  dela; com retenção zero e o snapshot preso, 5 de 6 arquivos saem, a versão do snapshot lê e a
  intermediária falha com `FileNotFoundError`. O `vacuum` grava dois commits (`VACUUM START` e
  `VACUUM END`), sem metadados da biblioteca.
- Uma execução que confere a versão por igualdade abortaria depois de um `vacuum`, um `compact` ou
  um `reconcile` de outra sessão; `Execution.publish` passou a conferir por `version_diff`
  ([etapa 6](PLAN-STAGE-6.md)).

## O que a primeira execução da suíte Redshift mostrou

Em 2026-09-21, às 10:50 UTC, o usuário rodou `pytest -m redshift` no ambiente alvo (Linux x86_64,
kernel 6.12 do Amazon Linux 2023, Python 3.13.15, o `.venv` da pasta preparada: deltalake 1.6.4,
DuckDB 1.5.5, PyArrow 25.0.1, boto3 1.43.98, SQLAlchemy 2.0.54, pandas 3.0.6, pytest 9.1.1), com a
raiz `s3://bndes-aco-models-138071776059/dzd-5qqmzj3amjp657/3hpfa7636y4qor/shared/serialize-db-tests`
e a credencial temporária do workgroup. A sessão durou 10,7 s: um teste passou e dez reprovaram. O
relatório está em [`readings/redshift-suite-2026-09-21-1050.json`](readings/redshift-suite-2026-09-21-1050.json);
a primeira tentativa não gravou o JSON e o usuário repetiu a suíte. O `conftest` criava o arquivo
sem criar a pasta, que os probes criam e a suíte não criava; ele passou a criá-la.

**A causa das dez reprovações é uma transação só.** `connect_redshift` rodava `USE
datalake_rw_shared` antes de a fixture ligar o autocommit. Com ele desligado, o
`redshift_connector` emite `begin transaction` antes do primeiro `execute` (`cursor.py`: `if not
in_transaction and not autocommit`), e ligá-lo depois não fecha a transação aberta: a sessão inteira
correu nela. No terceiro teste, `select count(*) from stv_slices` foi negada (`42501`, a mesma leitura
do probe), e um erro do servidor aborta a transação. O `CREATE TABLE` seguinte, os oito testes
restantes e os onze `DROP TABLE IF EXISTS` da limpeza receberam `25P02`, `current transaction is
aborted, commands ignored until end of transaction block`. Os exemplos não passaram por isso:
`redshift_manifest.py` liga o autocommit antes do `USE`, e `redshift_copy_unload.py` não encontrou
erro algum dentro da sua transação.

O JSON registrava só a contagem; a saída do terminal, colada pelo usuário, completou o quadro. O
teste que passou foi `test_cursor_fetchmany_feeds_record_batches`: cinco linhas em fatias de duas,
cada fatia um `RecordBatch` com `int64`, `decimal128(18, 2)` e `date32` (a memória numa consulta
grande continua por medir, [etapa 5](PLAN-STAGE-5.md)). O primeiro teste reprovou por asserção,
`assert False is True`: `has_schema_privilege('sbx_aco_decon', 'CREATE')` respondeu `false` depois do
`USE`, sem erro, no esquema em que o `CREATE TABLE` dos exemplos passou. É a leitura `RS-5`, numa
execução só: a função não responde pelo privilégio num esquema de datashare, a biblioteca não a usa,
e a prova do privilégio é o próprio `CREATE`. Os outros nove reprovaram com `25P02`. Desde então o
JSON leva a mensagem de cada teste reprovado (`failed.<teste>`).

**O que a execução respondeu**, apesar das reprovações:

- A suíte conecta pelo caminho de `examples/redshift_native.py`: usuário
  `IAMR:user-533cbaba-4061-70a6-7967-78dc06230c13@3hpfa7636y4qor`, versão `1.0.436211`, que o driver
  devolve com um byte nulo no fim (`Redshift 1.0.436211\0`).
- Depois do `USE`, `current_schema()` é nulo e `current_database()` continua `dev`, como no probe.
  `svv_redshift_databases` lista `datalake_rw_shared` como `shared` com isolamento `UNKNOWN` e `dev`
  como `local` com `Snapshot Isolation`; `svv_all_schemas` põe `sbx_aco_decon` em
  `datalake_rw_shared`, tipo `shared`; `stv_slices` é negada (`42501`).
- O delta-rs gravou as tabelas dos testes sob a raiz (`write_deltalake` vem antes do primeiro comando
  Redshift de cada teste) e a limpeza da suíte apagou 17 objetos sob `serialize-db-poc/0f5ea6d3`: a
  primeira escrita da suíte no bucket do ambiente alvo, pelo endpoint de gateway e pelas credenciais
  do contêiner, sem proxy.
- O DuckDB 1.5.5 abriu com 2 threads e as extensões de `.duckdb/` da pasta preparada.

**Consequências**, nesta unidade de trabalho: o autocommit passou para `connect_redshift`, antes do
`USE` e de qualquer comando, também nas conexões por thread do teste paralelo; a limpeza faz
`rollback` quando encontra uma transação aberta; `has_schema_privilege` deixou de ser asserção no
primeiro teste e virou a leitura `redshift.has_schema_privilege_create`, porque é a pergunta `RS-5`;
o `conftest` cria a pasta do relatório e grava a mensagem de cada teste reprovado;
`tests/test_conftest_redshift.py` fixa a ordem com um `redshift_connector` fabricado. As perguntas
da [etapa 0](PLAN-STAGE-0.md) continuam sem resposta até a próxima execução, a primeira das duas que
a etapa exige, exceto a leitura de `has_schema_privilege`, que a segunda execução repete.

## O que a segunda execução da suíte Redshift mostrou

Em 2026-09-21, às 11:28 UTC, com o `conftest` corrigido, a suíte rodou de novo no ambiente alvo:
sete testes passaram e quatro reprovaram em 50,3 s, e a limpeza apagou as onze tabelas e 23 objetos.
O relatório está em [`readings/redshift-suite-2026-09-21-1128.json`](readings/redshift-suite-2026-09-21-1128.json).

**As duas causas das reprovações são da suíte, não do Redshift:**

- `COPY ... MANIFEST` falhou nos três testes que o usam, e a leitura do `VARCHAR` excedido registrou
  o mesmo erro: `Spectrum Scan Error: File not found`, com a URL `…/operacoes//mes%3D2026-01/…`.
  `write_manifest` montava a URL como `f"{table.table_uri}/{path}"`, e `DeltaTable.table_uri` termina
  em barra (sonda local do mesmo dia: `file:///…/tabela/`); uma chave S3 com `//` é outra chave. O
  exemplo que passou, [`../examples/redshift_manifest.py`](../examples/redshift_manifest.py), monta a
  URL a partir da sua própria string, sem a barra. O `%3D` é o Redshift codificando o `=` ao pedir o
  objeto: `sys_load_error_detail` mostra as duas formas, e o delta-rs 1.6.4 grava e devolve
  `mes=2026-01/…` sem codificar, no log e em `get_add_actions`, também para uma `AddAction` registrada
  com o caminho cru (sonda local). A suíte passou a tirar a barra, a decodificar o `path` e a
  registrar a primeira URL de cada manifesto.
- `test_sqlalchemy_ddl_creates_table`: o `CREATE TABLE` passou e `information_schema.columns`
  respondeu vazio para o esquema do datashare depois do `USE`: a visão enxerga só o banco da conexão,
  como `has_schema_privilege`. O teste passou a conferir as colunas pelo `cursor.description` de um
  `select ... limit 0` e a ler `svv_all_columns`, que cruza os bancos, como leitura.

**O que passou, e o que respondeu:**

- `has_schema_privilege('sbx_aco_decon', 'CREATE')` respondeu `false` pela segunda vez: a leitura é
  permanente.
- O ida e volta por `esquema.tabela` depois do `USE` (`CREATE`, `INSERT`, `SELECT`) passou.
- `SUPER` recebe `JSON_PARSE`, devolve `meta.sistema` como `"A"` e `JSON_SERIALIZE` volta ao texto,
  numa tabela do datashare.
- `UNLOAD ... PARTITION BY (mes) MANIFEST VERBOSE ALLOWOVERWRITE` passou de novo, e o
  `schema.elements` do manifesto lista a coluna de partição `mes` (`character varying`, `max_length`
  7), que os arquivos não têm: a conferência de `register_files` recebe a lista com a coluna de
  partição. Os tipos físicos repetiram (`INT96`, `FIXED_LEN_BYTE_ARRAY`), com mínimo e máximo;
  `create_write_transaction` registrou os arquivos, o delta-rs leu a tabela de volta
  (`to_pyarrow_table`) e o `delta_scan` do DuckDB contou as 6 linhas.
- A Data API executou o `select` em 444 ms: o `PICKED` de 30 s do probe foi transitório.
- O `fetchmany` em fatias passou de novo, e `stl_load_errors` continua negada enquanto
  `sys_load_error_detail` explica um `COPY` reprovado.

**Consequências**: além das duas correções, a suíte ganhou as leituras que a tabela da
[etapa 0](PLAN-STAGE-0.md) prometia e os testes não faziam: os nomes dos arquivos do `UNLOAD` e três
destinos sem `ALLOWOVERWRITE` (o mesmo prefixo, um prefixo pai com arquivos abaixo, um subprefixo
novo dentro de uma pasta com arquivos), e um documento acima de 65.535 bytes em `SUPER` por `INSERT`
e por `COPY` direto de um Parquet. As perguntas do `COPY` (tipos, lista de colunas, `FILLRECORD`,
`VARCHAR` excedido, paralelo) esperam a próxima execução.
