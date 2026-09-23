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
  funcionam, e a decisão do usuário de 2026-09-22 deixou a biblioteca só com a cadeia padrão. O
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

Em 2026-09-20, uma sessão no espaço com a raiz local e a raiz S3 gravou o relatório de
`SERIALIZE_DB_TEST_REPORT` com as medições das duas raízes, e a execução das 04:52 UTC, depois das
correções do dia, registrou 104 testes passados e 7 pulados em 35 s, com a limpeza das duas raízes
(`local.cleanup`, `s3.cleanup`); sem variável, 63 passaram e 48 foram pulados.

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
listar buckets nem de ler o versionamento; `storage_options()` é resolvido de novo a cada chamada e
não leva credencial alguma, porque uma emissão dura uma hora e um trio congelado expiraria numa
execução longa, enquanto a cadeia padrão renova o `DeltaTable` que a execução segura
([etapa 3](PLAN-STAGE-3.md)); o motor DuckDB fixa `temp_directory` numa pasta com espaço
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
dicionário, `parquet-cpp-arrow`, formato 1.0, a chave `pandas` no rodapé de todo arquivo e nenhum
`field_id`. Os
seis tipos: `int32` (30 colunas), `string` (23), `date32` (10), `double` (10), `bool` (3) e
`timestamp[ns]` em `INT96` (2).

O modelo de referência de `tests/reference_model/` bate com os arquivos: as 12 tabelas, as colunas na mesma
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
relação N×N de `rel_contrato_operacao` e `fator_rateio` somando 1 por contrato (a direção que a
medição do usuário de 2026-09-21 fixou; a seção abaixo). A memória por
partição de `cad_lancamentos` continua em [`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md).

## O que a leitura da base de produção mostrou

Em 2026-09-21, às 13:54 UTC, `probes/parquet_source.py --sample 5000` leu no ambiente alvo a base de
produção `db_projetado`
(`s3://bndes-aco-models-138071776059/dzd-5qqmzj3amjp657/3hpfa7636y4qor/shared/bndes_grupos_bases_analise_financeira/databases/prd/db_projetado`,
`S3FileSystem`, a listagem em 0,1 s, os 205 rodapés lidos; o relatório está em
[`readings/parquet_source-2026-09-21-1354.txt`](readings/parquet_source-2026-09-21-1354.txt)): 14
pastas de tabela, 205 arquivos, 3.771.538.655 bytes, 187.340.531 linhas, `schema.json` solto na
raiz, nenhum arquivo ilegível, nenhuma chamada falhada e nenhuma checagem reprovada. A seção 3 é
idêntica à da base de desenvolvimento, coluna a coluna (as 78 colunas, os tipos, a nulidade, os
tipos físicos, nenhum `field_id`; conferido por script contra a transcrição de
`tests/test_source_db_projetado.py`), as partições são as mesmas (`data_str` com `2026-02-28`,
`2026-03-31` e `2026-06-30`; `data_base_str` com `2026-01-31` a mais), o valor do caminho não está
dentro do arquivo, o layout físico é o mesmo (um row group, SNAPPY, `PLAIN` e `RLE`,
`parquet-cpp-arrow`, formato 1.0, `INT96`), as sete colunas sem mínimo e máximo são as mesmas, e o
`schema.json` é o mesmo controle de esquema. O modelo de referência de `tests/reference_model/`
bate com ela como com a de desenvolvimento: as 12 tabelas com as colunas na mesma ordem, os mesmos
tipos e a mesma nulidade, exceto as sete colunas de `cad_contratos` anuláveis nos arquivos e
`NOT NULL` no modelo, sem nulo nos dados; nenhuma coluna `NOT NULL` do modelo tem nulo;
`alembic_version` e `meta_update_status` só existem na origem; os órfãos das chaves estrangeiras
compostas se repetem (`data_base` 2026-01-31 sem `cad_contratos`, o contrato `desemb-999`); e as
chaves únicas e estrangeiras transcritas em `tests/test_source_db_projetado.py` são as do modelo
(`tests/test_reference_model.py` fixa as duas conferências, lendo o modelo pelo SQLAlchemy).

As duas bases diferem nos dados, não na estrutura:

| O que | Desenvolvimento (2026-09-20) | Produção (2026-09-21) |
| --- | --- | --- |
| Linhas | 187.340.644 | 187.340.531: `cad_contas` 97 em vez de 102 (`numero` até `T.4` em vez de `T.5`), `rel_contas_hierarquias` 89 em vez de 93, `cad_lancamentos` 141.901.795 em vez de 141.901.899; as 104 linhas a menos tinham `sistema` e `contrato` nulos (27.391 e 20.958 nulos em vez de 27.495 e 21.062). |
| Bytes | 3.757.237.689 | 3.771.538.655 |
| Ids máximos | `id_contrato` 88.853.864, `id_operacao` 154.461.887, `id_lancamento` 1.113.599.996, `id_rel_contrato_operacao` 556.941.030 | 78.342.969, 136.235.442, 952.517.158 e 490.576.085; os mínimos são iguais. |
| `cad_aliquotas.id` | 2 a 16 | 1 a 26, com os mesmos 15 pares de contas e os mesmos fatores. |
| `valor` de `cad_lancamentos` | `±11846195394.628` | `±11846195394.62801` |
| `meta_update_status` | ids até 182, a última carga em 2026-09-03 | ids até 161, a última carga em 2026-09-14 |
| Chave `pandas` no rodapé | Em todo arquivo | Em parte: `cad_contratos` 5 de 8, `cad_lancamentos` 111 de 144, `cad_operacoes` 9 de 13, `rel_contrato_operacao` 16 de 30; nenhuma em `alembic_version` e `meta_update_status`; as demais tabelas 1 de 1. |

Os arquivos sem a chave `pandas` na produção são 3, 33, 4 e 14 por tabela; em `cad_contratos`,
`cad_operacoes` e `rel_contrato_operacao` é o número de arquivos de uma partição (`2026-03-31` ou
`2026-06-30`), o relatório não diz quais arquivos são, e a hipótese de que a carga de 2026-09-14
gravou sem passar pelo pandas não foi conferida. O escritor é `parquet-cpp-arrow` nos dois casos, e
a chave não muda a leitura: o `read_parquet` do DuckDB e o dataset do PyArrow a ignoram, e `cast`
segue o contrato.

Consequências: a carga inicial ([`PLAN-STAGE-7.md`](PLAN-STAGE-7.md)) lê a mesma estrutura nos dois
ambientes, e nenhuma diferença entre as bases muda uma primitiva; `tests/source_db_projetado.py`
continua a reproduzir a seção 3 e as partições, comuns às duas, com os valores que diferem (ids
máximos, contagens, `valor`) na leitura de desenvolvimento, e passou a gravar parte dos arquivos
sem a chave `pandas` (a última partição de cada tabela particionada, `alembic_version` e
`meta_update_status`), conferido pelo probe sobre a base fictícia no mesmo dia. O modelo de
referência fica como está, por decisão do usuário de 2026-09-21 (`tests/model/` passou a
`tests/reference_model/`), então a cópia corrigida da [etapa 1](PLAN-STAGE-1.md), o modelo cliente,
vai para `tests/client_model/` (confirmada pelo usuário no mesmo dia e escrita em seguida); a
revisão mudou [`PLAN.md`](PLAN.md), [`PLAN-STAGE-1.md`](PLAN-STAGE-1.md),
[`PLAN-STAGE-2.md`](PLAN-STAGE-2.md), [`PLAN-STAGE-4.md`](PLAN-STAGE-4.md),
[`PLAN-STAGE-7.md`](PLAN-STAGE-7.md), [`serialize-db.md`](serialize-db.md),
[`sqlalchemy.md`](sqlalchemy.md) e [`CURRENT_STATE.md`](CURRENT_STATE.md).

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
[6](PLAN-STAGE-6.md), e em [`serialize-db.md`](serialize-db.md), seção "Paralelismo". O desenho por
cursor que ela mediu deu lugar à sessão única em 2026-09-22 (seção "O que a sessão única mostrou").

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
- A [etapa 4](PLAN-STAGE-4.md) nasce com o banco DuckDB em arquivo e `temp_directory` conferido:
  7,6 GiB e 29,8 GiB livres não cabem uma tabela materializada de doze partições de
  `cad_lancamentos` em memória, e cabem em disco. É o banco em arquivo que tira a tabela
  materializada da memória; o `memory_limit` ficou no padrão do DuckDB por decisão do usuário de
  2026-09-22, e o motor registra no log o valor que o DuckDB escolheu. O `export_mode="register"`
  fica reforçado como caminho das partições grandes ([etapa 7](PLAN-STAGE-7.md)).
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
- A Data API em `PICKED` e a decisão de copiar ou não os relatórios para `plan/readings/` continuam em
  [`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md).

## O que os rascunhos das etapas mostraram

Em 2026-09-21, no macOS arm64 com deltalake 1.6.4, DuckDB 1.5.5, PyArrow 25.0.1, SQLAlchemy 2.0.54,
duckdb-engine 0.17.0 e sqlalchemy-redshift 1.0.0, os rascunhos das etapas 1 a 9 rodaram no
scratchpad; os das etapas 3 a 9 estão em cada `PLAN-STAGE-<n>.md`, seção "Rascunhos executados", e
os das etapas 1 e 2 deram lugar aos módulos `serialize_db.schema` e `serialize_db.sql`. O que eles
mostraram além do que já estava medido:

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
- Duas colunas do modelo cliente são palavras reservadas, lidas em 2026-09-21 no macOS com o DuckDB
  1.5.5: `duckdb_keywords()` classifica `to` (`cad_contratos`) como `reserved`, `timestamp`
  (`cad_lancamentos`) como `column_name` e `data` como `unreserved`; `CREATE TABLE t1 (to
  VARCHAR(2))` falha com `Parser Error: syntax error at or near "to"`, e `"to"`, `timestamp` e
  `"timestamp"` passam. A lista de palavras reservadas do Redshift tem `TO` e `TIMESTAMP`, e
  `examples/redshift_manifest.py` já cita `"to"`. O `duckdb_engine` e o `sqlalchemy-redshift` citam
  `"to"` no `CREATE TABLE` e no `select` (`cad_contratos."to"`), o do Redshift também `"timestamp"`,
  e `redshift_distkey="to"` sai como `DISTKEY ("to") SORTKEY ("to", data)`. O DDL do modelo
  cliente compilado pelo `duckdb_engine` executou as 12 tabelas no DuckDB em memória (23 colunas
  `BIGINT`, 24 `VARCHAR`, 10 `DATE`, 10 `DOUBLE`, 6 `INTEGER`, 3 `BOOLEAN`, 1 `TIMESTAMP`). A
  biblioteca cita todo identificador que emite ([etapa 1](PLAN-STAGE-1.md)).
- O rascunho da etapa 1 na forma do módulo, com o DDL gerado pela tabela de tipos e sem dialeto,
  executou no DuckDB em memória um `CREATE TABLE` com os 15 tipos do contrato e os nomes entre
  aspas, `"{prefix}cad_operacoes"` inclusive: `information_schema.columns` leu `DECIMAL(18, 2)`
  como `DECIMAL(18,2)`, `TIMESTAMPTZ` como `TIMESTAMP WITH TIME ZONE`, `VARCHAR(100)` e
  `VARCHAR(36)` como `VARCHAR`, e `JSON` como `JSON`.
- `pc.all` de uma coluna vazia devolve nulo: o caminho do leitor de `cast`, que deriva o esquema de
  saída de `reader.schema.empty_table()`, recusou a tabela vazia como `timestamp com hora numa
  coluna Date` na primeira execução do rascunho, que até então só chamava `cast` com um lote;
  `pc.all(..., min_count=0)` devolve verdadeiro para a coluna vazia, e o leitor de dois lotes saiu
  com as quatro linhas.
- A implementação da etapa 1 (2026-09-21, macOS, deltalake 1.6.4) mostrou que `Schema.to_json()`
  do delta-rs serializa os metadados de cada campo em ordem arbitrária: duas gerações seguidas do
  mesmo modelo deram `{"parquet.field.id":1,"comment":...}` e `{"comment":...,"parquet.field.id":2}`,
  e o diff dos arquivos versionados reprovou; ele também grava `PARQUET:field_id` como
  `parquet.field.id` inteiro. `schema_files` grava o JSON canônico (`json.dumps` com
  `sort_keys=True` e `indent=2`). O modelo de referência tem 12 tabelas, 73 colunas, 14 chaves
  estrangeiras (12 `DEFERRABLE`) e 20 colunas `String` sem comprimento; `check_models` lista 129
  violações nele e nenhuma no modelo cliente, e o DDL das 12 tabelas do modelo cliente executou
  num DuckDB em memória, `"to"` inclusive.
- A migração adiantada (`scripts/migrate_parquet_to_delta.py`, 2026-09-21, macOS, DuckDB 1.5.5
  com a extensão `delta` `45c4087`, deltalake 1.6.4) fez a primeira leitura por `delta_scan` de
  uma tabela criada por `delta_schema`, e toda coluna veio nula, com a contagem certa pelo
  `numRecords` do log, enquanto o delta-rs e o `read_parquet` do mesmo arquivo liam os valores.
  A sonda cruzou o esquema Delta com e sem `parquet.field.id` (a chave em que
  `Schema.from_arrow` guarda o `PARQUET:field_id` do Arrow) com três escritores: o `COPY` do
  DuckDB, que grava sem `field_id`, e o `write_deltalake` com e sem `field_id` no arquivo. Com a
  chave no esquema, os três arquivos leram nulo em toda coluna; sem ela, os três leram os
  valores. `delta_schema` passou a tirar o `PARQUET:field_id` antes do `from_arrow`, os 12
  `.delta.json` versionados perderam o `parquet.field.id`, e `tests/test_schema.py` afirma a
  ausência. A regra que a leitura deixou está em `CLAUDE.md`: uma decisão de esquema é relida por
  todo leitor que o pipeline usa, com um arquivo de cada escritor. No mesmo dia o script mostrou
  que o `octet_length` do DuckDB só existe para `BLOB` (a medida em bytes de um texto é
  `strlen`) e que uma exceção levantada pelo leitor que `write_deltalake` consome volta como
  `DeltaError` com a mensagem original dentro do texto, não como a exceção original: por isso a
  migração confere nulos e comprimentos numa consulta do DuckDB antes de gravar, nos dois modos,
  e o `cast` fica atrás do `rewrite` como segunda guarda.

## O que a primeira execução da suíte Redshift mostrou

Em 2026-09-21, às 10:50 UTC, o usuário rodou `pytest -m redshift` no ambiente alvo (Linux x86_64,
kernel 6.12 do Amazon Linux 2023, Python 3.13.15, o `.venv` da pasta preparada: deltalake 1.6.4,
DuckDB 1.5.5, PyArrow 25.0.1, boto3 1.43.98, SQLAlchemy 2.0.54, pandas 3.0.6, pytest 9.1.1), com a
raiz `s3://bndes-aco-models-138071776059/dzd-5qqmzj3amjp657/3hpfa7636y4qor/shared/serialize-db-tests`
e a credencial temporária do workgroup. A sessão durou 10,7 s: um teste passou e dez reprovaram. O
relatório não fica em `readings/`: cada leitura do ambiente que ele trazia se repete nas execuções
limpas das 13:35 e das 13:39; a primeira tentativa não gravou o JSON e o usuário repetiu a suíte. O `conftest` criava o arquivo
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
da [etapa 0](PLAN-STAGE-0.md) ficaram para as execuções seguintes, exceto a leitura de
`has_schema_privilege`, que todas repetiram.

## O que a segunda execução da suíte Redshift mostrou

Em 2026-09-21, às 11:28 UTC, com o `conftest` corrigido, a suíte rodou de novo no ambiente alvo:
sete testes passaram e quatro reprovaram em 50,3 s, e a limpeza apagou as onze tabelas e 23 objetos.
O relatório não fica em `readings/`: cada leitura do ambiente que ele trazia se repete nas
execuções limpas das 13:35 e das 13:39.

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
`VARCHAR` excedido, paralelo) foram respondidas às 12:08 e às 12:10, na seção seguinte.

## O que as execuções da suíte Redshift das 12:08 e das 12:10 mostraram

Em 2026-09-21, às 12:08 e às 12:10 UTC, com a suíte corrigida pela segunda execução, o usuário rodou
`pytest -m redshift` duas vezes seguidas no ambiente alvo: dez testes passaram e um reprovou em cada
uma (60,2 s e 58,1 s), e a limpeza apagou 38 objetos e as tabelas de cada sessão. Os relatórios
não ficam em `readings/`: cada leitura do ambiente que eles traziam se repete nas execuções limpas
das 13:35 e das 13:39, que reproduzem de propósito o `34510` registrado aqui como reprovação. As
duas execuções concordam em cada leitura, e o que segue vale como permanente.

**A reprovação é do cache de prepared statements do driver, e se repetiu.**
`test_copy_column_list_and_fillrecord` repete por tentativa `TRUNCATE`, `COPY` e
`select count(*), count(*) - count(canal)`. Na terceira volta, a do `FILLRECORD`, o `COPY` passou e a
contagem recebeu `XX000`, `[Data Sharing] Error Code 34510: Concurrent DDL committed on
datalake_rw_shared.sbx_aco_decon.serialize_db_poc_<id>_evoluida between Prepare and Execute` (rotina
`relocalize_data_sharing_cached_rtes`, `federation_api.cpp`). O `redshift_connector` 2.1.16 guarda um
prepared statement nomeado por texto de comando (`Connection.execute`, chave `(operation, params)`),
o reaproveita no `execute` seguinte do mesmo texto com `Bind` e `Execute` sem novo `Parse`, e só
fecha e descarta os guardados quando o servidor confirma um `ALTER`, `CREATE`, `DROP` ou `ROLLBACK`
(`handle_COMMAND_COMPLETE`); `TRUNCATE` não está na lista. A contagem foi preparada na segunda volta,
o `TRUNCATE` da terceira é o DDL, e o datashare recusa executar um statement preparado antes dele,
em vez de replanejar como o banco local. Os `TRUNCATE` repetidos passaram porque um comando
utilitário não tem entradas de tabela no plano. **Consequências**: `connect_redshift` passa
`max_prepared_statements=0`, com que o driver prepara o statement sem nome logo antes de cada
execução e não guarda nada (`get_statement_name_bin`; o bloco que grava no cache só corre acima de
zero), a mesma regra do `connect` da [etapa 5](PLAN-STAGE-5.md); a suíte ganhou
`test_repeated_statement_after_truncate_and_the_driver_cache`, que exercita o mesmo texto antes e
depois de um `TRUNCATE` na conexão da sessão e registra o que uma conexão com o cache do driver
recebe, também na repetição, depois de um `ALTER` e numa tabela temporária do banco da conexão. A
leitura da terceira volta ficou sem registro porque a contagem vinha antes de `record`; o `COPY` é
registrado antes dela desde então. As mensagens de erro do relatório passaram a levar o SQLSTATE, o
campo `M` e o detalhe `D` numa linha, em vez dos 200 primeiros caracteres do dicionário.

**A leitura do código do driver respondeu a pergunta do `fetchmany`**: `EXECUTE_MSG` pede o portal
sem limite de linhas, `handle_messages` só termina em `READY_FOR_QUERY`, cada `DATA_ROW` entra em
`cursor._cached_rows` e `fetchmany` fatia essa fila (`Cursor.__next__`). O `execute` materializa o
resultado inteiro em objetos Python, e o `stream` do motor Redshift limita a memória só por
`UNLOAD`; as execuções das 13:35 e das 13:39 leram 5 linhas na fila antes do primeiro `fetchmany`.

**O que as execuções responderam**, as perguntas do `COPY` da [etapa 0](PLAN-STAGE-0.md):

- `COPY ... FORMAT AS PARQUET MANIFEST` de arquivos gravados pelo delta-rs carregou `DECIMAL(18, 2)`
  em `INT64` e `timestamp_ntz` em `INT64` de microssegundos: 500 linhas por mês, a soma do `DECIMAL`
  e o menor `timestamp` iguais aos da amostra. A URL de cada manifesto, registrada, é
  `.../operacoes/mes=2026-01/part-00000-...-c000.snappy.parquet`, sem barra dobrada.
- Um arquivo com cinco colunas numa tabela de seis: o `COPY` posicional reprova com
  `Spectrum Scan Error` 15007, `Unmatched number of columns`; com lista de colunas,
  `COPY tabela (id_operacao, data_ref, id_cliente, valor, descricao) FROM ... FORMAT AS PARQUET MANIFEST`
  carregou as 100 linhas com `canal` nulo em todas; com `FILLRECORD` o `COPY` passou, e as execuções
  das 13:35 e das 13:39 leram as 100 linhas que ele carregou, com `canal` nulo.
- Uma string de 300 bytes numa coluna `VARCHAR(200)`: o `COPY` aborta com `Spectrum Scan Error`
  15007, e `sys_load_error_detail` explica, `The length of the data column descricao is longer than
  the length defined in the table. Table: 200, Data: 300`, com o nome do arquivo em
  `https://s3.sa-east-1.amazonaws.com/...` e o `=` como `%3D`; `stl_load_errors` continua negada.
- `SUPER`: `INSERT ... JSON_PARSE(%s)` de um documento de 80.901 bytes passou (`json_size` 80901),
  acima do teto do `VARCHAR`; o `COPY` de um Parquet com a coluna em texto numa coluna `SUPER` é
  recusado sem `SERIALIZETOJSON` (`SUPER column in COPY query requires SERIALIZETOJSON option`). A
  cláusula, e o `COPY ... FORMAT JSON 'auto'` de um documento como objeto, foram lidos às 13:35 e às
  13:39.
- `UNLOAD ... PARTITION BY (mes) MANIFEST VERBOSE` nomeia os arquivos
  `mes=<valor>/<slice>_part_<nn>.parquet`: `0064_part_00.parquet` às 12:08 e `0000_part_00.parquet`
  às 12:10, o número da slice muda entre execuções. Sem `ALLOWOVERWRITE`, o destino é conferido como
  prefixo: o mesmo prefixo e o prefixo pai, com arquivos abaixo, reprovam com `Specified unload
  destination on S3 is not empty. Consider using a different bucket / prefix, manually removing the
  target files in S3, or using the ALLOWOVERWRITE option`; um subprefixo novo dentro de uma pasta com
  arquivos passa.
- Dois `COPY ... MANIFEST` de 1.000 linhas em tabelas distintas, cada um numa conexão: 4,5 s e
  3,6 s; dois `UNLOAD`: 1,9 s e 1,5 s.
- A Data API respondeu em 610 ms e 177 ms; `has_schema_privilege` respondeu `false` pela terceira e
  pela quarta vez; `information_schema.columns` vazia e `svv_all_columns` com as sete colunas; o
  `fetchmany` em fatias, o `SUPER` pequeno e o registro do `UNLOAD` no Delta passaram de novo.

**Consequências nos documentos**: [`redshift.md`](redshift.md) recebe as regras lidas (posição com
contagem de colunas, lista de colunas, o `VARCHAR` que aborta, `SERIALIZETOJSON`, o destino do
`UNLOAD` por prefixo e os nomes dos arquivos, o cache e a leitura do resultado no driver); a
[etapa 5](PLAN-STAGE-5.md) muda o destino de `export_partition` para `<uri>/<execution_id>/<valor>/`,
porque `<uri>/<execution_id>/` deixa de estar vazio depois da primeira partição da execução, e passa
`max_prepared_statements=0` no `connect`; a [etapa 8](PLAN-STAGE-8.md) tem a lista de colunas
confirmada e ganha a decisão do teto do campo JSON; a [etapa 4](PLAN-STAGE-4.md) tem na auditoria de
tamanho a barreira; [`schema.md`](schema.md), [`parquet.md`](parquet.md), [`delta.md`](delta.md),
[`estrategia.md`](estrategia.md) e [`serialize-db.md`](serialize-db.md) perdem as pendências do
`COPY`; [`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md) perde as perguntas do `COPY`, do destino do
`UNLOAD` e do `fetchmany`, e listou as leituras que as execuções das 13:35 e das 13:39 fizeram.

## O que as execuções limpas da suíte Redshift mostraram

Em 2026-09-21, às 13:35 e às 13:39 UTC, com a conexão sem o cache de prepared statements, o usuário
rodou `pytest -m redshift` duas vezes seguidas no ambiente alvo: os doze testes passaram nas duas
(92,4 s e 61,8 s), e a limpeza apagou 42 e 43 objetos e as tabelas de cada sessão. Os relatórios
estão em [`readings/redshift-suite-2026-09-21-1335.json`](readings/redshift-suite-2026-09-21-1335.json)
e [`readings/redshift-suite-2026-09-21-1339.json`](readings/redshift-suite-2026-09-21-1339.json).
As duas execuções concordam em cada leitura, e são as duas execuções limpas que a
[etapa 0](PLAN-STAGE-0.md) exigia: a etapa fecha com elas.

**O que as execuções leram**, além de repetir cada leitura das 12:08 e das 12:10:

- O mesmo `select count(*)` passou antes e depois de um `TRUNCATE` na conexão da sessão, com
  `max_prepared_statements=0`: o `34510` era do cache do driver. Na conexão com o cache, a
  repetição depois do `TRUNCATE` recebeu `34510`, a segunda repetição também (a entrada guardada
  fica até um comando que o driver reconhece), a repetição depois de um `ALTER TABLE ... ADD COLUMN`
  passou, e a mesma sequência numa tabela temporária criada depois do `USE` passou: a recusa é do
  datashare, e uma tabela temporária pode ser criada, consultada e truncada na sessão depois do
  `USE`.
- `cursor._cached_rows` tinha as 5 linhas antes do primeiro `fetchmany`: o `execute` materializa o
  resultado, como o código do driver diz.
- `COPY ... FORMAT AS PARQUET MANIFEST FILLRECORD` carregou o arquivo de cinco colunas na tabela de
  seis: 100 linhas, `canal` nulo em todas, o mesmo resultado da lista de colunas.
- `TRUNCATECOLUMNS` não é aceito com Parquet: `0A000`, `TRUNCATECOLUMNS argument is not supported
  for PARQUET based COPY`. Numa string acima do `VARCHAR`, o `COPY` só aborta.
- `COPY ... FORMAT AS PARQUET SERIALIZETOJSON` de um Parquet com o documento de 80.901 bytes em
  texto numa coluna `SUPER` recusou com `1224 String value exceeds the max size of 65535 bytes`: um
  Parquet com o documento em texto não leva um documento acima do teto do `VARCHAR` a `SUPER`, com
  ou sem a cláusula. `COPY ... FORMAT JSON 'auto'` de um arquivo JSON de uma linha com o documento
  como objeto carregou: `json_typeof` `object`, `json_size` 80901.
- A Data API respondeu em 270 ms e 240 ms; dois `COPY` paralelos em 4,3 s e 3,8 s, dois `UNLOAD` em
  1,6 s e 1,5 s; o `UNLOAD` nomeou `0064_part_00.parquet` às 13:35 e `0000_part_00.parquet` às
  13:39, como às 12:08 e às 12:10; `has_schema_privilege` respondeu `false` pela quinta e pela sexta
  vez.

**Consequências**: o que as duas execuções leram igual virou asserção na suíte (a fila do cursor, o
posicional que reprova, a lista de colunas e o `FILLRECORD` com 100 linhas, o `COPY` que aborta no
`VARCHAR`, o objeto carregado por `FORMAT JSON 'auto'`); a [etapa 0](PLAN-STAGE-0.md) está
concluída, com `svv_table_info` depois do `USE` (`RS-8`) como a única leitura que resta, do probe,
sem etapa que dependa dela; a [etapa 5](PLAN-STAGE-5.md) e a [etapa 8](PLAN-STAGE-8.md) propõem
`FILLRECORD` em todo `COPY` da biblioteca, porque o manifesto de uma partição pode listar arquivos
anteriores e posteriores a uma coluna nova e a lista de colunas exigiria um `COPY` por contagem de
colunas; a decisão do teto do campo JSON na [etapa 8](PLAN-STAGE-8.md) recebe as duas leituras do
`SUPER`; [`redshift.md`](redshift.md), [`parquet.md`](parquet.md) e [`schema.md`](schema.md) recebem
os fatos; [`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md) perde a lista das leituras da suíte.

## O que o `pdoc` mostrou de um módulo com `__all__`

Uma sonda de 2026-09-21, um módulo de três funções (`__all__ = ["publico"]`, mais `protegido` e
`_privado`) gerado pelo `pdoc` 16.0.0: a página traz só `publico`. Um nome sem prefixo que fica
fora do `__all__` não aparece na documentação, como não aparece o prefixado.

**Consequência**: a regra dos três níveis de [`PLAN.md`](PLAN.md) entrega o que ela promete, a
documentação com a interface pública e nada mais, desde que o módulo com um nome protegido declare
o `__all__`; sem `__all__`, o `pdoc` mostraria todo nome sem prefixo. As páginas do pacote em
2026-09-21 trazem os quinze nomes do `__all__` de `serialize_db.schema`, o `main` de
`serialize_db.cli` e o `ContractError` de `serialize_db.errors`.


## O que o destaque de código do `pdoc` mostrou

A geração de 2026-09-22 com `--docformat restructuredtext`: o bloco indentado depois de `Exemplo:`
sai como `<pre><code>` sem classe e sem destaque, e o bloco de `.. code-block:: python` sai como
`<div class="pdoc-code codehilite">` com cada elemento num `<span>`, que a folha de estilo embutida
na página colore. O literal `::` do reStructuredText não serve: o `pdoc` deixa os dois pontos no
texto e o bloco continua sem destaque. O `docstrings.py` do `pdoc` 16.0.0 traduz a diretiva numa
cerca Markdown com a linguagem, e só a cerca com linguagem chega ao Pygments. As páginas tinham 41
blocos com destaque e 29 sem; depois da mudança, 70 com destaque e nenhum sem.

**Consequência**: cada exemplo das docstrings de `serialize_db.schema`, `serialize_db.sql` e
`serialize_db.cli` abre com `.. code-block:: python`, o da linha de comando com
`.. code-block:: shell`, e as três cercas sem linguagem de [`index.md`](../docs/index.md) ganharam
`shell`.


## O que a pasta preparada mostrou na migração adiantada

Em 2026-09-21, no laboratório (macOS arm64) sobre a pasta preparada por `prepare_offline.sh`
(Python 3.13.15 em `.python/`, deltalake 1.6.4, DuckDB 1.5.5, PyArrow 25.0.1, SQLAlchemy 2.0.54 e
pandas 3.0.6 no `.venv/`, as extensões em `.duckdb/v1.5.5/osx_arm64/`), num ambiente despido:
`env -i`, `HOME` numa pasta vazia, `HTTP_PROXY`, `HTTPS_PROXY` e as duas grafias minúsculas
apontadas para `127.0.0.1:1`, porta fechada, e só `.venv/bin/python`:

- `tests/test_migrate_parquet_to_delta.py`: 16 passados em 0,95 s com
  `SERIALIZE_DB_TEST_LOCAL_ROOT` na pasta da sessão; sem a variável, 16 pulados.
- O comando do [`README.md`](../README.md) sobre a base fictícia (`PYTHONPATH=tests`,
  `--metadata client_model:Base.metadata`, origem e raiz em pasta local) saiu com 0, cada tabela
  com as partições conferidas e as conversões `int32 -> int64` no relatório.
- `LOAD delta`, `LOAD httpfs` e `LOAD aws` com `autoinstall_known_extensions` e
  `autoload_known_extensions` em `false`: as três carregaram de `.duckdb/v1.5.5/osx_arm64/`, sem
  tentativa de download. A extensão `delta` é carregada em toda execução, porque o relatório lê a
  raiz por `delta_scan`.
- `CREATE SECRET migracao (TYPE s3, PROVIDER credential_chain, REGION 'us-east-1')` reprovou com
  `Secret Validation Failure: during 'create' using the following: Credential Chain: 'config'`: a
  cadeia sem credencial alguma no ambiente despido, não extensão faltando. O secret só nasce
  quando a origem ou a raiz é `s3://`.

**Consequência**: `prepare_offline.sh` baixa tudo o que `scripts/migrate_parquet_to_delta.py`
precisa — o Python de `.python-version`, o `duckdb` do grupo `dev`, o `deltalake`, o `pyarrow` e o
`sqlalchemy` das dependências de execução, pelo `uv sync --all-groups`, e as extensões `delta`,
`httpfs` e `aws`. O que o script ainda exige no alvo não é download: as credenciais da AWS que a
`credential_chain` encontra, `AWS_REGION` ou `AWS_DEFAULT_REGION`, o `PYTHONPATH=tests` do modelo
cliente (que vem no `tar` do repositório, não do script) e a preparação em Linux x86_64, porque as
extensões são por versão do DuckDB e por plataforma.

### Origem e destino, pasta ou `s3://`, nas quatro combinações

`open_location` decide pelo `://`: `pafs.FileSystem.from_uri` ou a pasta local resolvida, e o resto
do script só conhece `Location` — a URI que o DuckDB e o delta-rs recebem, o sistema de arquivos do
PyArrow que lista e o caminho na forma dele. `pq.ParquetFile` recebe o sistema de arquivos da
`Location`, e o caminho que entra no log é relativo à pasta da tabela
(`<coluna>=<valor>/carga_inicial_<uuid>.parquet`), o que mantém a tabela realocável. `uses_s3` é a
disjunção das duas raízes: basta uma ser `s3://` para o DuckDB carregar `httpfs` e `aws`, criar o
secret e o delta-rs receber `AWS_REGION`. Numa raiz local com origem no S3, o delta-rs recebe
`storage_options={"AWS_REGION": ...}` numa pasta, e a opção é inócua: medido no mesmo dia e
ambiente, `DeltaTable.create` em 0,00 s, `write_deltalake` em 0,13 s e a leitura das duas linhas.
Locais por construção ficam só `ensure_folder`, que não tem pasta a criar no S3, e o `--report`,
que grava o JSON num arquivo local.

Nenhuma execução exercitou o ramo S3 do script: `tests/test_migrate_parquet_to_delta.py` é `local`
inteiro. Do caminho, o ambiente alvo já mostrou a leitura da base de produção por
`probes/parquet_source.py` em 2026-09-21, que abre a origem com o mesmo
`pafs.FileSystem.from_uri`; o `COPY ... TO 's3://...' (RETURN_STATS)`, o `create_write_transaction`
sobre a raiz S3 e o `delta_scan` dela esperam a execução no alvo
([`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md), etapa 7).

## O que a medição do rateio na base de produção mostrou

Em 2026-09-21 o usuário agrupou `rel_contrato_operacao` da base de produção pela chave do contrato,
`(sistema, contrato)`, e o somatório de `fator_rateio` deu 1: **cada contrato é rateado entre as
suas N operações**, e não cada operação entre os seus contratos. A consulta que o cliente faz segue
a mesma direção, do contrato para as operações dele; a inversa não é pedida.

A leitura contradizia o que o repositório registrava. Corrigidos no mesmo dia:
`tests/source_db_projetado.py`, que rateava cada operação entre os contratos dela e passou a
ratear cada contrato entre as operações dele, com `apportionment` por contrato;
`tests/test_source_db_projetado.py`, que soma por `(data, sistema, contrato)`; o comentário de
`fator_rateio` e o da tabela no modelo cliente, com o `rel_contrato_operacao.delta.json` versionado
regerado; e as frases desta página e de [`CURRENT_STATE.md`](CURRENT_STATE.md). A estrutura não
mudou: a relação continua N×N, o par `(data, operacao, sistema, contrato)` continua único na base
fictícia, e o número de linhas por partição é o mesmo.

A medição não muda a `sort_key` de `rel_contrato_operacao`, `data, sistema, contrato, operacao`
(decisão do usuário de 2026-09-21): o prefixo dela é exatamente o contrato, a chave da consulta.
Também não salva a chave estrangeira que `cad_contratos` declara para `rel_contrato_operacao` no
modelo de referência: com N operações por contrato, `(data, sistema, contrato)` continua não único
no destino, e a chave saiu do modelo cliente (decisão do usuário de 2026-09-21,
[`PLAN-STAGE-1.md`](PLAN-STAGE-1.md)).

## O que a revisão de código de 2026-09-21 mostrou

A revisão do código existente contra os padrões do repositório (macOS, DuckDB 1.5.5, deltalake
1.6.4, PyArrow 25.0.1) mediu duas coisas antes de corrigir.

**O commit não precisa da tabela Delta reaberta.** `scripts/migrate_parquet_to_delta.py` reabria a
`DeltaTable` depois de cada partição. A sonda criou uma tabela particionada e chamou
`create_write_transaction` três vezes no mesmo objeto: o objeto ficou na versão 0 e com 0 arquivos
em `file_uris()` do começo ao fim, enquanto o log no armazenamento passou por 1, 2 e 3 com um
arquivo por commit. **Cada commit resolve a versão no log do armazenamento**, e não na versão que o
objeto abriu; o objeto em memória não reflete os próprios commits. A reabertura por partição saiu,
e os 16 casos de `tests/test_migrate_parquet_to_delta.py` continuam passando, com as versões
`2` e `4` que o teste da retomada afirma. O que o objeto em memória guarda importa em
`loaded_partitions`, que a carga lê uma vez antes do laço.

**A refatoração de um probe é conferida pelo relatório dele.** `probes/parquet_source.py` rodou
sobre a base fictícia de `tests/source_db_projetado.py` em pasta local (`--sample 5 --text-bytes`,
758 linhas de relatório) antes e depois de `read_footer` e `text_lengths` perderem o aninhamento:
as duas saídas são idênticas fora a data e o caminho do arquivo gravado. A base fictícia tem o que
o relatório precisa exercitar: `INT96` sem estatística, partição Hive, a chave `pandas` em parte
dos arquivos e colunas de texto não ASCII.

As correções que as duas medições acompanharam estão em
[`CURRENT_STATE.md`](CURRENT_STATE.md); nenhuma mudou o comportamento observável do pacote, e as
suítes passaram nas duas configurações: sem variável, 160 passados e 81 pulados; com a raiz local,
218 passados e 23 pulados.

## O que o ensaio do SQLGlot mostrou

Em 2026-09-21, numa venv avulsa no macOS arm64 (`uv run --no-project --python 3.13 --with sqlglot`),
o SQLGlot 30.18.0 analisou pelo dialeto `redshift` o texto que a [etapa 2](PLAN-STAGE-2.md) gera. O
pacote é Python puro: nenhum `.so`, 5,4 MB, nenhuma dependência além dele.

| Texto | `sqlglot.parse_one(texto, dialect="redshift")` |
| --- | --- |
| O texto gerado, com o sentinela `{prefix}` | `ParseError` na coluna 26 |
| O mesmo texto com o prefixo trocado | Aceita |
| `WHERE data = :data_str` e `WHERE data = $data_str` | Aceita os dois marcadores |
| `cad_contratos."to"` e `cad_lancamentos."timestamp"` | Aceita |
| `WHERE area = 'TI`, a aspa desbalanceada | `TokenError` |
| `t."taxa $base"`, o estrago da expressão sem as aspas duplas | Aceita |
| `INSERT INTO destino BY NAME SELECT * FROM origem` | Aceita, e o Redshift não tem |
| `list_aggregate(l, 'sum')` | Aceita, e o Redshift não tem |

O texto da primeira linha da tabela é o do rascunho de então, com a cópia `quote=False` e o
sentinela nu, `{prefix}cad_contas`; com a cópia citada da decisão do mesmo dia, o sentinela fica
dentro das aspas de um identificador, e o arquivo versionado analisa (leitura de 2026-09-22, na
seção da implementação da etapa 2). O teste `test_redshift_text_parses_with_sqlglot` pega string
malformada, e não pega nem o identificador estragado que a decisão das aspas fechou nem construção
que o Redshift não suporta, o que [`estrategia.md`](estrategia.md) já registrava da avaliação do
SQLGlot como camada. Enquanto os
statements forem portáveis, o texto do Redshift é igual ao do DuckDB, que a suíte executa: a lacuna
nasce no primeiro statement cujos dois textos diferem. O usuário decidiu em 2026-09-22 incluí-lo
desde já: `sqlglot==30.18.0` no grupo `dev`, e o teste deixa de ser opcional
([`PLAN-STAGE-2.md`](PLAN-STAGE-2.md)).

## O que a revisão da etapa 2 mostrou

Em 2026-09-21, no macOS arm64 com SQLAlchemy 2.0.54, duckdb-engine 0.17.0, sqlalchemy-redshift
1.0.0 e DuckDB 1.5.5, a revisão do rascunho da [etapa 2](PLAN-STAGE-2.md) mediu três coisas.

**Os três dialetos compilam o mesmo texto, fora a citação de `"timestamp"`.** O `SELECT` portável do
rascunho, um `INSERT ... SELECT DISTINCT` e um `SELECT` sobre `cad_contratos."to"` e
`cad_lancamentos."timestamp"` compilaram por `duckdb_engine.Dialect(paramstyle="named")`,
`RedshiftDialect_redshift_connector(paramstyle="named")` e
`sqlalchemy.dialects.postgresql.dialect(paramstyle="named")`: os dois primeiros statements saíram
idênticos nos três; no terceiro, os três citam `"to"` e só o dialeto do Redshift cita
`"timestamp"`, reservada só lá. Com `paramstyle="named"`, `LIKE 'TI:%'` sai com um `%` nos três.

**O dialeto `postgresql` com uma cópia que cita todo nome gera todo identificador entre aspas.** A
cópia da tabela com `quoted_name(f"{prefix}{nome}", quote=True)` e cada coluna como
`sa.Column(quoted_name(nome, quote=True), tipo)`, trocada no statement por `replacement_traverse`,
deu `SELECT "{prefix}cad_contas"."numero", sum("{prefix}cad_lancamentos"."valor") AS total FROM
"{prefix}cad_lancamentos" JOIN ...`, `INSERT INTO "{prefix}cad_contas" ("id_conta", "numero") SELECT
DISTINCT ...`, `CAST("{prefix}cad_lancamentos"."valor" AS NUMERIC(18, 2))` e
`"{prefix}cad_contratos"."to", "{prefix}cad_lancamentos"."timestamp"`. O texto com
`prefix="exec_42_"` e `:data_base_str` em `$data_base_str` rodou no DuckDB sobre
`"exec_42_cad_lancamentos"` e `"exec_42_cad_contas"` criadas com o DDL citado da etapa 1 e devolveu
`[('1.1', 150.0)]`, o resultado do rascunho; o statement original continuou sem prefixo e sem aspas.

**O `bindparam` sem valor aparece em `compiled.binds` com `required=True`.** Sem `literal_binds`,
`sa.bindparam("area")` compila com `binds == {"area": (None, True)}` (valor, `required`);
`sa.bindparam("area", value="x")` com `("x", False)`; uma constante e um `in_` com lista entram com
nomes anônimos e `required=False`; `param()`, um `literal_column`, não aparece. Com
`literal_binds=True`, `binds` fica vazio nos cinco casos, o texto do primeiro sai `t.area = NULL` e
um `SAWarning` avisa. A leitura de `binds` na compilação sem `literal_binds` substitui o
`warnings.catch_warnings` do rascunho anterior: a documentação do módulo `warnings` diz que, com
`context_aware_warnings` falso, o gerenciador modifica os atributos globais do módulo e não é
seguro num programa concorrente, com threads ou corrotinas; o flag e a variável de contexto
existem desde o Python 3.14, e o projeto roda 3.13.

O rascunho da etapa 2 foi reescrito com essas leituras, na forma do módulo, e rodou de novo: o mesmo
texto, o mesmo resultado no DuckDB e as mesmas recusas, mais a do sentinela restante, o alvo de um
`INSERT ... SELECT` prefixado e as tabelas de `referenced_tables` lidas do statement e do texto.

**A cópia prefixada com `quote=True` faz os dois dialetos citarem todo identificador do contrato.**
Depois de o usuário manter `duckdb_engine.Dialect` e `RedshiftDialect_redshift_connector` como
compiladores de `render` (2026-09-21), a mesma cópia — `quoted_name(f"{prefix}{nome}", quote=True)`
no nome da tabela e `quoted_name(nome, quote=True)` em cada coluna — compilou pelos dois: o `SELECT`
portável saiu idêntico nos dois dialetos, `SELECT "{prefix}cad_contas"."numero",
sum("{prefix}cad_lancamentos"."valor") AS total FROM "{prefix}cad_lancamentos" JOIN ...`, e o das
colunas reservadas saiu `"{prefix}cad_contratos"."to", "{prefix}cad_lancamentos"."timestamp"` nos
dois, sem depender da lista de palavras reservadas de cada dialeto. O sentinela dentro das aspas
continua legível por `\{prefix\}(\w+)`, que devolveu `cad_contas` e `cad_lancamentos` nos dois
textos, e o texto com `prefix="exec_42_"` rodou no DuckDB sobre as tabelas do DDL citado da etapa 1
e devolveu `[('1.1', 150.0)]`. Com `quote=False`, a forma do rascunho, o DML compilado cita só o
que o dialeto reserva: `"to"` nos dois e `"timestamp"` só no Redshift.

Com a decisão do usuário de 2026-09-21 pela cópia com `quote=True`, o rascunho da etapa 2 rodou de
novo: o mesmo `[('1.1', 150.0)]`, as mesmas recusas, os dois textos iguais, e toda tabela e coluna
do contrato entre aspas, o `INSERT ... SELECT` inclusive (`INSERT INTO "{prefix}cad_contas"
("id_conta", "numero") ...`).

## O que a implementação da etapa 2 mostrou

Em 2026-09-22, no macOS arm64 com SQLAlchemy 2.0.54, duckdb-engine 0.17.0, sqlalchemy-redshift
1.0.0, DuckDB 1.5.5 e sqlglot 30.18.0, o módulo `serialize_db.sql` foi escrito na forma do rascunho
revisado de [`PLAN-STAGE-2.md`](PLAN-STAGE-2.md) e rodou sobre os quatro statements do pipeline
fictício de `tests/client_model/statements.py` (`saldos_por_conta`, `rateio_por_operacao`,
`lancamentos_por_contrato` e `veiculos_novos`).

**Os quatro statements saem idênticos nos dois dialetos e rodam no DuckDB sobre o DDL da etapa 1.**
O `SELECT` com junção, parâmetro e coluna booleana, o das três condições de junção com `LIKE 'TI%'`
e `!= '1:2'`, o das colunas `to` e `timestamp` com `CAST(... AS NUMERIC(18, 2))` e o
`INSERT ... SELECT DISTINCT` com `NOT EXISTS` sobre a própria tabela alvo compilaram byte a byte
iguais por `duckdb_engine.Dialect(paramstyle="named")` e
`RedshiftDialect_redshift_connector(paramstyle="named")`, e `referenced_tables` leu o mesmo
conjunto do statement e do texto. Com `prefix=""` e com `prefix="exec_42_"`, os quatro rodaram num
DuckDB em memória sobre as 12 tabelas criadas por `schema.ddl`; `veiculos_novos` inseriu
`(5, 'veículo 5')` a partir de um lançamento e, repetido, não inseriu de novo. Duas gerações de
`serialize-db sql write` deram arquivos iguais.

**O arquivo versionado com o sentinela analisa pelo SQLGlot.** `sqlglot.parse_one(texto,
dialect="redshift")` aceitou o texto dos quatro statements com o sentinela, `"{prefix}cad_contas"`:
com a cópia `quote=True` da decisão de 2026-09-21, o sentinela fica dentro das aspas de um
identificador, que o analisador aceita. O `ParseError` na coluna 26 do ensaio de 2026-09-21 foi
lido sobre o texto do rascunho de então, com `quote=False` e o sentinela nu, e a frase "o arquivo
versionado não analisa, por causa do sentinela" atravessou a decisão das aspas até
[`PLAN-STAGE-2.md`](PLAN-STAGE-2.md) e a decisão do usuário de 2026-09-22. A frase saiu do plano, e
`test_redshift_text_parses_with_sqlglot` analisa o arquivo versionado de cada statement e o texto
com o prefixo vazio; a aspa desbalanceada continua `TokenError`.

**O compilador deixa um espaço antes de cada quebra de linha.** `str(compiled)` sai com `" \nFROM"`
e `" \nWHERE"` nos dois dialetos; `render` apara o fim de cada linha, para o arquivo versionado
sobreviver a um editor que apara o fim das linhas sem produzir diff em `sql check`.

**Os dialetos entram sem o driver do Redshift.** `RedshiftDialect_redshift_connector` importa o
`redshift_connector` só em `import_dbapi`, que `compile` não chama: `sqlalchemy-redshift` entrou nas
dependências de execução e `redshift-connector` ficou no grupo `dev`. `duckdb_engine` importa o
`duckdb` ao ser importado, então `duckdb==1.5.5` entrou nas dependências de execução com
`duckdb-engine==0.17.0`, fixado como o grupo `dev` já fixava. A mudança de grupo reprovou
`tests/test_probes.py::test_dev_requirements_read_the_pinned_versions_of_pyproject`, que esperava
`sqlalchemy_redshift` entre os pacotes do grupo `dev` que `probes/space.py` (`SP-9`) compara com as
versões instaladas; o probe passou a ler as dependências de execução junto com o grupo `dev`.

As suítes depois da etapa: sem variável, 174 passam e 82 são pulados; com a raiz local, 233 passam
e 23 são pulados (macOS, 2026-09-22); `tests/test_sql.py` tem 15 casos, um deles `local`.

## O que o probe das decisões da etapa 4 mostrou

Em 2026-09-22, no macOS arm64 com DuckDB 1.5.5 e PyArrow 25.0.1, dois probes leram o que o rascunho
da [etapa 4](PLAN-STAGE-4.md) supunha sobre o `loader` e sobre o `memory_limit`, antes das decisões
do usuário do mesmo dia.

**O `memory_limit` não aceita porcentagem.** `SET memory_limit = '60%'` e `SET memory_limit = '60'`
são recusados com `Parser Error: Unknown unit for memory: '%' (expected: KB, MB, GB, TB for 1000^i
units or KiB, MiB, GiB, TiB for 1024^i units)`; `'4.5GiB'` passa. O padrão da máquina foi
`14.3 GiB`, e `os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES')` leu 18,0 GiB, cujos 80% são
14,4 GiB: as duas leituras concordam dentro do arredondamento do texto que o DuckDB mostra, como
concordam no ambiente alvo (7,6 e 6,1 GiB). Uma fração da máquina, portanto, é conta do Python, não
do DuckDB. O usuário decidiu no mesmo dia deixar o `memory_limit` no padrão do DuckDB e registrar no
log o valor que ele escolheu.

**O `CREATE TABLE IF NOT EXISTS` não guarda o nome do sandbox.** Sobre uma view, ele passa e não
cria nada, e o `INSERT ... BY NAME` seguinte morre com `CatalogException: Catalog Error: destino is
not an table`. Sobre uma tabela de outro formato, ele também passa: a tabela `(id BIGINT, outra
VARCHAR)` continuou com as suas colunas, e o `INSERT ... BY NAME` de um lote `(id, valor)` deu
`BinderException: Binder Error: Table "destino" does not have a column with name "valor"`. No caminho
oposto, uma tabela com uma coluna a mais aceitou o lote e preencheu a coluna que sobra com nulo, sem
dizer nada. A tabela que o `ingest` materializa por `CREATE TABLE AS SELECT` não tem o `NOT NULL` do
contrato — `is_nullable` verdadeiro em todas as colunas —, então um `loader` que acrescentasse nela
deixaria entrar o nulo que o DDL do contrato recusa, e só a auditoria o pegaria. O usuário decidiu
que o `loader` recusa o nome ocupado com `SandboxError`, e a recusa trouxe `published(table)`, por
onde o pipeline lê as partições publicadas da tabela cujo nome no sandbox pertence ao `loader`.

**Um lote registrado ocupa um nome de view.** `con.register("lote", ...)` aparece em
`duckdb_views()` ao lado das views do `ingest`, e `information_schema.tables` classifica os dois
como `VIEW`, contra `BASE TABLE` das tabelas. A guarda do `loader` lê daí o que existe, e o nome com
que o `loader` registra cada lote não pode ser o de uma tabela do modelo.

## O que as sondagens das decisões da etapa 3 mostraram

Quatro sondagens no macOS em 2026-09-22 (deltalake 1.6.4, DuckDB 1.5.5, PyArrow 25.0.1) mediram o
que as decisões da [etapa 3](PLAN-STAGE-3.md) esperavam, e
`tests/proof_of_concept/test_deltalake.py` guarda as duas primeiras como asserções
(`test_description_and_comments_survive_overwrite`, `test_written_stats_lose_the_row_on_decimal`).

**A descrição, o nome e os comentários de coluna atravessam o `overwrite`.** Uma tabela criada com
`description="Operações por data-base"`, `name="cad_operacoes"` e comentário em cada campo manteve
os três depois de um `write_deltalake(mode="overwrite")` com predicado e de outro sem predicado. O
`publish_partition` da etapa 3 não apaga a documentação, e a pergunta de a guardar ou não no Delta
passou a ser só de sincronizar com o modelo. `alter.set_table_description` e
`alter.set_column_metadata` mudam cada um, e o commit resultante traz só `commitInfo` e `metaData`,
sem tocar nos dados. Consequência: `create_table` passa o comentário da tabela em `description` e
`reconcile` sincroniza a descrição e os comentários (decisão do usuário do mesmo dia).

**O próprio delta-rs perde a linha na estatística de `decimal`.** Uma tabela com
`decimal(18, 2)` valendo `123456789012345.21` saiu com `"valor": 123456789012345.2` em
`maxValues`, porque o escritor grava mínimo e máximo como número JSON, e o dobro não representa 18
dígitos significativos. `WHERE valor = 123456789012345.21` devolveu **zero linhas** no delta-rs
(`to_pyarrow_dataset`) e no `delta_scan` do DuckDB, com a linha dentro do arquivo: os dois podam
pelo máximo abaixo do valor real. O `timestamp[us]` sai truncado em milissegundos
(`2026-08-31 23:59:59.999999` vira `"2026-08-31 23:59:59.999"`), e mesmo assim os dois leitores
acharam a linha. `float64` e `string` transcrevem exato, inclusive `0.30000000000000004`,
`1e+308` e um texto de 41 caracteres, e `NaN` fica fora de `minValues` e `maxValues`. Consequência:
`register_files` registra mínimo e máximo das colunas inteiras, de data, `Double` e `String`, e
deixa `decimal` e `timestamp` de fora (decisão do usuário do mesmo dia). O defeito é do escritor,
não do registro: uma coluna `Numeric` larga gravada por `publish_partition` carrega a mesma poda, e
o modelo cliente não tem nenhuma, porque as colunas numéricas são `Double` (decisão de 2026-09-20).

**O que o `RETURN_STATS` do DuckDB devolve por tipo.** Uma sondagem no macOS em 2026-09-22 (DuckDB
1.5.5) gravou uma tabela com `BIGINT`, `DATE`, `DOUBLE` e `VARCHAR` por `COPY ... (FORMAT parquet,
RETURN_STATS)` e leu `column_statistics`: os valores saem como texto, com as chaves
`column_size_bytes`, `min`, `max`, `null_count` e `num_values`, e o nome da coluna vem entre aspas.
O `DOUBLE` faz o percurso de ida e volta por `float` (`-1e+308` e `1234567890.1234567`, o valor
exato do dobro), e a coluna com `NaN` ganha `has_nan: "true"` com mínimo e máximo dos demais
valores. O `VARCHAR` sai inteiro, sem truncar: um texto de 200 caracteres apareceu por completo em
`max`. Uma coluna só de nulos não traz `min` nem `max`. Um `NaN` registrado com mínimo e máximo dos
outros valores não atrapalha a poda: `isnan(taxa)` achou a linha no delta-rs e no `delta_scan`, que
divergem só em `taxa > 2` (o DuckDB conta o `NaN`, o Arrow não). Consequência: `delta_stats` de
`scripts/migrate_parquet_to_delta.py` passou a registrar mínimo e máximo de inteiro, data, `Double`
e texto, pela decisão da [etapa 3](PLAN-STAGE-3.md) do mesmo dia, com `stat_converter` escolhendo a
conversão e `tests/test_migrate_parquet_to_delta.py::test_registered_stats_carry_the_four_exact_types`
conferindo os valores contra o arquivo; a coluna `meta` de `cad_lancamentos`, sempre nula na base de
origem, fica sem extremos.

## O que a sonda das tabelas temporárias do DuckDB mostrou

Em 2026-09-22, no macOS (DuckDB 1.5.5), uma sonda criou `CREATE TEMP TABLE temporaria` e
`CREATE TABLE comum` num banco em arquivo e leu as duas de quatro lugares. A conexão raiz lê as
duas; um `cursor()` da mesma conexão lê `comum` e recebe `Catalog Error: Table with name
temporaria does not exist!` na temporária, e uma temporária criada nesse cursor é invisível à
raiz; um segundo `duckdb.connect` ao mesmo arquivo, no mesmo processo, lê `comum` e não lê a
temporária; a conexão raiz usada de outra thread lê a temporária. `duckdb_tables()` lista a
temporária no catálogo `temp`, esquema `main`, com `temporary` verdadeiro, só na conexão que a
criou. A documentação diz o mesmo: "Temporary tables are session scoped, meaning that only the
specific connection that created them can access them", e elas "reside in memory rather than on
disk even when connecting to a persistent DuckDB", com transbordo para `temp_directory`. No
`redshift_connector` 2.1.16, `Cursor.execute` delega a `Connection.execute` (leitura do código):
os cursores de uma conexão são a mesma sessão, e uma temporária criada num deles vale para os
outros.

**Consequência**: o sandbox dos dois motores fica de tabelas comuns, como o plano previa. No
DuckDB, o motor da [etapa 4](PLAN-STAGE-4.md) dá um `cursor()` a cada thread, a cada `stream` e a
cada `loader`, e uma temporária de um cursor não alcança os outros; no Redshift, o usuário
reverteu em 2026-09-22 a frase do `CLAUDE.md` que propunha temporárias para separar a execução
dos dados publicados, e as tabelas `exec_<id>_*` continuam no esquema do datashare
([etapa 5](PLAN-STAGE-5.md)). `ddl` ganhou `temporary=True`, que emite `CREATE TEMP TABLE` nos
dois dialetos, a pedido do usuário do mesmo dia e sem uso no plano;
`tests/test_schema.py::test_ddl_temporary_table` afirma a leitura: a tabela nasce no catálogo
`temp`, e o `cursor()` não a vê.

## O que as sondas dos statements num `Connection` mostraram

Em 2026-09-22, no macOS (SQLAlchemy 2.0.54, duckdb-engine 0.17.0, sqlalchemy-redshift 1.0.0,
DuckDB 1.5.5, redshift-connector 2.1.16), quatro sondas leram o que um statement Core do modelo
cliente faz fora e dentro da biblioteca:

- **Os estilos de parâmetro.** `duckdb.paramstyle` é `qmark`, `duckdb_engine.Dialect()` compila
  em `pyformat` e `Dialect(paramstyle="named")` em `named`; `redshift_connector.paramstyle` e
  `RedshiftDialect_redshift_connector()` são `format`. Um `select` com `bindparam("minimo")` e um
  `LIKE 'TI%'`, compilado por `Dialect(paramstyle="named")` sem `literal_binds`, saiu como
  `... WHERE cad_contas.area LIKE :area_1 AND cad_contas.id_conta > :minimo`, com `"to"` citado
  pelo dialeto sozinho; `compiled.construct_params({"minimo": 5})` devolveu
  `{'area_1': 'TI%', 'minimo': 5}`, e o texto com `:nome` reescrito em `$nome` rodou na conexão
  crua do DuckDB com esse dicionário. É o caminho padrão dos motores desde a decisão do usuário do
  mesmo dia: a cópia prefixada compilada com os parâmetros do cliente, sem `render`.
- **O `Connection` criado fora da biblioteca.** Num
  `sqlalchemy.create_engine("duckdb:///:memory:")`, `cad_contas` e `cad_contratos` do modelo
  cliente foram criadas por `Table.create`, um `select` com `sa.bindparam("id")` rodou por
  `connection.execute(query, {"id": 1})`, e `select(cad_contratos.c.to)` compilou como
  `cad_contratos."to"`. O mesmo `select` com `sql.param("id", sa.BigInteger)` falhou:
  `ProgrammingError ... Parser Error: syntax error at or near ":"`, porque o `literal_column(":id")`
  chega ao driver como texto. Um statement escrito com `param` serve aos arquivos e não a um
  `Connection`; um com `bindparam` serve ao `Connection` e aos motores, e `render` o recusa.
- **`render` com `bindparam`.** `replacement_traverse` trocando cada `BindParameter` com
  `required=True` por `literal_column(":nome", type_)` antes de compilar com `literal_binds` deu
  `... WHERE cad_contas.id_conta = :id AND cad_contas.nome LIKE 'C%'`, com o statement original
  intacto (`:id` e `:nome_1` na compilação normal). É a implementação de `render` desde
  2026-09-22 (decisão do usuário, [etapa 2](PLAN-STAGE-2.md)), e a sonda da implementação leu
  cada forma de `bindparam`: sem valor, `:nome`; com valor, constante; o mesmo `bindparam` duas
  vezes, `:nome` nas duas; `text("data_str = :mes")`, `:mes`; `in_` com lista, constantes;
  `bindparam("area", value=None)`, `NULL` com o `SAWarning`, porque o valor foi dado;
  `bindparam("Data Base")`, `:Data Base`, que `render` recusa como nome inválido. Os oito
  arquivos de `tests/client_model/sql/` saíram iguais com `sa.bindparam` no lugar de `param`, em
  duas gerações, e o statement de teste rodou num `create_engine("duckdb:///:memory:")` com o
  dicionário de parâmetros.
- **`create_all` do modelo cliente no DuckDB.** `Base.metadata.create_all(connection)` falhou em
  `rel_contrato_operacao`: `Binder Error: Failed to create foreign key: referenced table
  "cad_operacoes" does not have a primary key or unique constraint on the columns
  data,operacao`. A chave estrangeira composta aponta as colunas do índice único
  `ix_operacoes_data_operacao`, e `cad_lancamentos` faz o mesmo com
  `ix_contratos_data_sistema_contrato`; a página de `CREATE TABLE` do Redshift diz "The
  referenced columns must be the columns of a unique or primary key constraint in the referenced
  table". A biblioteca não emite chaves no DDL e `keys` lê índice e constraint por igual, então
  nada muda para ela. O dono do modelo decidiu no mesmo dia: os dois índices viraram
  `UniqueConstraint` e `check_models` recusa a chave estrangeira sem chave no alvo
  ([etapa 1](PLAN-STAGE-1.md)). A sonda da regra, no DuckDB 1.5.5: `FOREIGN KEY (b, a)
  REFERENCES alvo (b, a)` contra `UNIQUE (a, b)` é recusada (`Binder Error: Failed to create
  foreign key: referenced table "alvo" does not have a primary key or unique constraint on the
  columns b,a`), então a regra compara as colunas na ordem; um índice único como alvo é recusado
  (`there is no primary key or unique constraint for referenced table "alvo2"`); o modelo cliente
  inteiro, com as duas `UniqueConstraint`, é criado por `create_all` num
  `create_engine("duckdb:///:memory:")`, e os 36 arquivos de esquema versionados saíram iguais
  em duas gerações, porque o DDL da biblioteca não emite chaves.

**Consequência**: o caminho padrão dos motores das etapas [4](PLAN-STAGE-4.md) e
[5](PLAN-STAGE-5.md) é o statement Core compilado pela cópia prefixada com os parâmetros do
cliente, e o texto SQL versionado é a opção de migração para fora do SQLAlchemy (decisão do
usuário de 2026-09-22); o requisito que a acompanha, um modelo e um statement que um
`sqlalchemy.Connection` criado fora da biblioteca aceite, fechou os dois itens em 2026-09-22
(decisões do usuário: `render` troca o `bindparam` sem valor por `:nome`, com `param` fora do
módulo; os dois índices únicos compostos do modelo cliente viraram `UniqueConstraint`, e
`check_models` confere o alvo de cada chave estrangeira).

## O que a revisão de código de 2026-09-22 mostrou

Em 2026-09-22, no macOS (PyArrow 25.0.1, deltalake 1.6.4), as sondas da revisão do pacote leram o
`cast` com as entradas que um pipeline em pandas produz e a versão que cada escrita do delta-rs
deixa no objeto `DeltaTable`.

- **O texto fora do tipo `string` escapava da medida de bytes.** `pa.types.is_string` é falso para
  `large_string`, o tipo que `pa.Table.from_pandas` dá ao `str` do pandas 3, e para `string_view`
  e dicionário; `_refuse_silent_losses` só media o texto quando a coluna chegava em `string`, então
  `"xyz"` em `large_string` entrava numa coluna `String(2)` e 65.536 bytes numa coluna `Text`, e só
  o `COPY` da publicação os recusaria. `pc.binary_length` não aceita `string_view` nem dicionário
  (`ArrowNotImplementedError`). A medida passou para depois da conversão, sobre a coluna já em
  `string` (`_refuse_long_text`), e os três tipos são recusados.
- **Um tipo sem conversão saía como erro do PyArrow.** `struct` numa coluna `Integer`, `list` numa
  `Date`, `bool` numa `Date` e `date32` numa `BigInteger` levantam `ArrowNotImplementedError`
  (subclasse de `NotImplementedError`, não de `ValueError`), que o `except (pa.ArrowInvalid,
  ValueError)` não pegava. Viram `ContractError` com a tabela e a coluna.
- **O desvio do inteiro para `Numeric` servia só a `Numeric(18, 2)`.** O cast direto de inteiro
  para `decimal128(p, s)` exige que `p` comporte qualquer valor do tipo inteiro, não os presentes:
  19 dígitos mais a escala num `int64`, 10 num `int32`. O desvio `decimal128(p + 3, s)` acertava a
  precisão 21 de `Numeric(18, 2)` por coincidência; `Numeric(10, 4)`, `Numeric(12, 0)`,
  `Numeric(5, 2)` e `Numeric(20, 10)` levantavam `ArrowInvalid` (`Precision is not great enough`)
  fora do `try`, e `Numeric(36, 10)` e `Numeric(38, 2)` `ValueError` (`precision should be between
  1 and 38`). O desvio passou a `decimal128(38, s)`, e o segundo cast recusa o valor que não cabe
  em `p` (`1000` em `Numeric(5, 2)`: `Decimal value does not fit in precision 5`).
- **O fuso some em silêncio.** `timestamp[us, tz=America/Sao_Paulo]` convertido para
  `timestamp[us]` passa com `safe=True` e guarda o instante UTC como hora local, e o inverso
  assume UTC. O `cast` aceita os dois; a decisão fica em [`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md).
- **O nulo em `NOT NULL` é `ValueError` simples.** `RecordBatch.cast` do esquema levanta
  `ValueError`, não `ArrowInvalid`, e `ArrowInvalid` já é subclasse de `ValueError`: o `except` do
  cast do esquema passou a `ValueError`.
- **A versão depois de cada escrita.** `write_deltalake(dt, ...)` com o objeto `DeltaTable` deixa
  `dt.version()` na versão do próprio commit, mesmo com o commit de outro escritor entre os dois
  (1, depois 3 com o 2 de outro objeto). `create_write_transaction` não atualiza o objeto (3 no
  objeto, 4 no log), como a revisão de 2026-09-21 já lera. `DeltaTable(uri).version()` depois da
  escrita, a forma dos rascunhos da [etapa 3](PLAN-STAGE-3.md), devolve a última versão do log, que
  pode ser a de outro escritor.

**Consequência**: os três defeitos do `cast` estão corrigidos em `serialize_db.schema`, com os
casos em `tests/test_schema.py` que reprovavam no código anterior (seis) e passam no novo;
`publish_partition` da etapa 3 escreve pelo objeto `DeltaTable` e devolve `dt.version()`, e
`register_files` relê a versão depois do commit.

## O que a sessão única mostrou

Em 2026-09-22, no macOS (DuckDB 1.5.5 com `threads = 2`, PyArrow 25.0.1, pandas 3.0.6), as sondas no
scratchpad e os esboços reescritos de `test_parallel.py` mediram se os dois motores podem ter uma
sessão por execução sob um lock, como o usuário decidiu no mesmo dia, sem perder a troca por lotes
em que o cliente trabalha no lote atual enquanto a biblioteca lê o seguinte ou grava o anterior.

- **O leitor do DuckDB não sobrevive a outro comando na mesma conexão.** O comando seguinte o
  esvazia, sem erro (`test_duckdb.py::test_arrow_reader_is_invalidated_by_the_next_command`, de
  2026-09-20). Um stream que segurasse o leitor seguraria a sessão enquanto o cliente trabalha, e
  o `close` de um `loader` aberto no mesmo `with`, que sai antes do stream, esperaria por ela. O
  stream da sessão única consome o leitor inteiro num arquivo antes de soltar o lock.
- **O arquivo intermediário.** Com Parquet nos dois sentidos, o pipeline de três estágios sobre
  3.000.000 de linhas levou 0,699 s num banco em arquivo: o `ParquetWriter` padrão gastou 0,240 s e
  o `INSERT ... BY NAME` de `read_parquet` 0,319 s. Com Arrow IPC, o resultado de 3.000.000 de linhas
  foi gravado em 0,069 s e lido em 0,005 s, contra 0,054 s do leitor direto. Em 20.000.000 de linhas
  de três colunas, o Arrow IPC sem compressão teve 478 MB (escrita 0,612 s, leitura 0,026 s), com LZ4
  162 MB (0,697 s e 0,054 s) e com ZSTD 97 MB (0,791 s e 0,143 s): LZ4 é o formato do arquivo.
- **A comparação justa.** A primeira sonda comparou a sessão única num banco em arquivo com os
  esboços por cursor num banco em memória; no mesmo banco, melhor de três execuções do pipeline de
  três estágios sobre 3.000.000 de linhas: em memória, 0,112 s por cursor contra 0,135 s na sessão
  única com Arrow IPC; em arquivo, o padrão da [etapa 4](PLAN-STAGE-4.md), 0,565 s por cursor contra
  0,400 s na sessão única, porque um `INSERT` único sobre o leitor do arquivo custa menos que um
  `INSERT` por lote numa transação num banco em arquivo. Na suíte, sobre um banco em arquivo: 0,427 s
  pela tabela inteira, 0,687 s lote a lote sem threads e 0,436 s encadeado.
- **A memória.** 20.000.000 de linhas: a tabela inteira em 567 MB e 0,594 s, o leitor direto em
  83 MB e 0,576 s, o arquivo sem compressão em 89 MB e 0,63 s. Na suíte, 10.000.000 de linhas: o
  arquivo com LZ4 em 106 MB e 0,408 s (0,381 s até o arquivo fechar, 81 MB de arquivo), contra 83 MB
  e 0,316 s do leitor direto e 322 MB e 0,341 s da tabela inteira. A consulta termina antes do
  primeiro lote.
- **O que não trava.** Um comando no meio de um stream, a tabela temporária lida pelo stream, a
  saída antecipada do laço com o `loader` no mesmo `with` (0,013 s), duas threads de cliente com
  stream e loader ao mesmo tempo (as duas terminaram com 1.000.000 de linhas cada, em 0,072 s) e uma
  primitiva chamada dentro de `session()` na mesma thread, pelo `RLock`. Um stream de 10 linhas pelo
  arquivo custou 0,3 ms.
- **O que a sessão única deixa de fazer.** Quatro tabelas de 150.000 linhas ingeridas por
  `delta_scan` em disco local levaram 0,061 s em série e 0,017 s em quatro cursores; quatro cargas
  Arrow pedidas por quatro threads levaram 0,071 s, e quatro `COPY ... TO`, 0,029 s, em série na
  sessão (`test_parallel.py`).

**Consequência**: os dois motores têm uma sessão por execução sob um `RLock` que as primitivas tomam
e soltam e que `session()` dá ao cliente; `stream` grava o resultado num arquivo Arrow IPC com LZ4
antes de soltar o lock, o `loader` grava os lotes num arquivo fora da sessão e os insere num comando
no `close`, e `query` e `execute` devolvem `to_arrow_table()` sob o lock ([`PLAN.md`](PLAN.md),
etapas [4](PLAN-STAGE-4.md), [5](PLAN-STAGE-5.md) e [6](PLAN-STAGE-6.md)). A ingestão de várias
tabelas passa a ser em série, e `max_workers` saiu de `ingest`; a medição no S3 está em
[`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md).

## O que a revisão de legibilidade dos testes de 2026-09-22 mostrou

Em 2026-09-22, no macOS (PyArrow 25.0.1, pytest 9.1.1), cada mudança da revisão de legibilidade
dos testes do pacote foi conferida pelo instrumento que a valida.

- **A base fictícia refatorada é a mesma, byte a byte.** `tests/source_db_projetado.py` gravou a
  base no scratchpad antes e depois da troca dos nomes e da quebra das expressões: os 63 arquivos
  saíram idênticos (`diff -r`), e `probes/parquet_source.py` com `--sample 5 --text-bytes` deu dois
  relatórios de 758 linhas, iguais fora a data e o caminho. O sorteio de `taxa_juros_fixos` continua
  um por linha de `cad_contratos`, usado numa linha em quatro, porque os valores de
  `cad_lancamentos` saem da mesma sequência do gerador.
- **O teste do `conftest` Redshift herdava a porta do ambiente.** O terceiro teste de
  `tests/test_conftest_redshift.py` não apagava `SERIALIZE_DB_REDSHIFT_PORT` e lia a do shell: com
  `SERIALIZE_DB_REDSHIFT_PORT=abc`, a versão anterior reprovou com `ValueError: invalid literal for
  int() with base 10: 'abc'`, e a nova, que apaga as sete `SERIALIZE_DB_REDSHIFT_*` antes de definir
  as do teste, passou. Nenhum dos três deixava a variável definida para os testes seguintes.
- **O `pythonpath` do pytest resolve pela raiz do repositório.** Com `pythonpath = ["scripts",
  "probes"]` em `pyproject.toml`, `tests/test_probes.py` e `tests/test_migrate_parquet_to_delta.py`
  importam os scripts pelo nome também com o pytest chamado de dentro de `tests/`, e o comando da
  esteira de testes passou os 99 casos do pacote com a raiz local.

**Consequência**: nenhuma mudança alterou o que as suítes conferem, e as contagens ficaram as da
sessão única: sem variável, 183 passam e 89 são pulados; com a raiz local, 249 passam e 23 são
pulados.

## O que a revisão das suítes de estudo de 2026-09-22 mostrou

Em 2026-09-22, no macOS (SQLAlchemy 2.0.54, duckdb-engine 0.17.0, sqlalchemy-redshift 1.0.0,
DuckDB 1.5.5, deltalake 1.6.4, PyArrow 25.0.1), as suítes de `tests/proof_of_concept/` passaram a
ensinar as abordagens das decisões de 2026-09-21 e 2026-09-22, e cada comportamento novo nelas foi
lido por uma sonda antes de virar asserção.

- **O `SAWarning` do `bindparam` sem valor só sai numa comparação por `=`.** Sob `literal_binds`,
  `coluna = :mes` e `upper(coluna) = upper(:mes)` saem `= NULL` com o aviso; `LIKE :padrao`,
  `coalesce(coluna, :mes)`, `select(:mes)`, o `VALUES` de um `INSERT` e `text("mes = :mes")` saem
  `NULL` sem aviso algum. Sem `literal_binds`, `compiled.binds` marca o parâmetro `required` nas
  sete formas. Uma guarda pelo aviso, a do rascunho anterior da etapa 2, deixaria passar cinco das
  sete, e [`sqlalchemy.md`](sqlalchemy.md) dizia que o `text()` também avisa.
- **As releituras concordaram com o plano.** `Schema.from_arrow` leva o `PARQUET:field_id` do Arrow
  ao esquema Delta como `parquet.field.id` inteiro, e sem a chave no Arrow ela não aparece. No log,
  o `overwrite` com predicado grava `remove` e `add` com `dataChange` verdadeiro, o `delete` que
  reescreve um arquivo também, e o `optimize.compact` grava os dois com `dataChange` falso. O
  `RETURN_STATS` do DuckDB traz mínimo e máximo como texto em todo tipo, o `decimal` exato
  (`123456789012345.21`), e nenhum dos dois numa coluna só de nulos; registrados só os de inteiro,
  data, `double` e texto, como faz `stat_converter`, `get_add_actions(flatten=True)` mostra
  `min.valor` e `min.data_ref` nulos, e o DuckDB poda pelo inteiro e pelo texto (`Scanning Files:
  0/2`). `strlen('ação ação')` dá 13 e `length`, 9, e `octet_length` recusa `VARCHAR` com
  `BinderException`; no PyArrow, `'ç' * 101` mede 202 em `binary_length` e 101 em `utf8_length`.
  Com `hive_partitioning = true`, `data_str=2026-08-31` é lido como `DATE`, numa pasta só inclusive,
  `hive_types_autocast = false` o mantém `VARCHAR`, e `mes=2026-01` fica `VARCHAR`.
- **O rascunho da auditoria da etapa 4 media o texto em caracteres.** Com `text_bytes`, `strlen` no
  DuckDB e `octet_length` no Redshift, que a página da função mede em bytes num `VARCHAR`, o
  rascunho rodou de novo com o mesmo resultado, e o valor `'ação ação'` (9 caracteres, 13 bytes)
  numa coluna `String(10)` só é acusado pela medida em bytes.

**Consequência**: [`sqlalchemy.md`](sqlalchemy.md) passou a dizer que o aviso sai só na comparação
por `=`, e [`PLAN-STAGE-2.md`](PLAN-STAGE-2.md) acrescenta esse fato à razão de `render` ler
`compiled.binds`; [`PLAN-STAGE-4.md`](PLAN-STAGE-4.md) mede o texto da auditoria em bytes, no
rascunho, na estratégia e nos testes. `test_sqlalchemy.py` tem 15 casos; sem variável, 186 passam e
89 são pulados; com a raiz local, 252 passam e 23 são pulados.
## O que o stream lote a lote e as sessões a mais mostraram

Em 2026-09-23, no macOS (DuckDB 1.5.5, PyArrow 25.0.1, pandas 3.0.6), uma sonda no scratchpad
comparou três desenhos de `stream` sobre o mesmo banco em arquivo, com os mesmos dados e lotes de
100.000 linhas, melhor de três execuções: o cursor próprio por stream, anterior à sessão única, com
a thread que puxa do leitor vivo; o transbordo do resultado inteiro antes do primeiro lote, o
desenho de 2026-09-22; e o desenho atual de `test_parallel.py`, em que uma thread roda a consulta
sob o lock e grava cada lote no arquivo assim que o DuckDB o entrega, e o cliente lê cada lote
gravado. A tabela tem 20.000.000 de linhas em três colunas; a consulta sem operador bloqueante
devolve 13.333.333 linhas em 134 lotes, e o trabalho do cliente por lote é um `sleep`, que solta o
GIL como o trabalho nativo, ou a conversão para pandas com duas colunas calculadas.

| Consulta e trabalho por lote, `threads = 2` | Cursor próprio | Transbordo inteiro | Lote a lote |
| --- | --- | --- | --- |
| Sem operador bloqueante, sem trabalho | 1º lote em 0,003 s, total 0,303 s | 0,472 s, 0,516 s | 0,005 s, 0,482 s |
| Sem operador bloqueante, 2 ms | 0,003 s, 0,339 s | 0,466 s, 0,810 s | 0,005 s, 0,490 s |
| Sem operador bloqueante, 5 ms | 0,005 s, 0,842 s | 0,494 s, 1,330 s | 0,012 s, 0,939 s |
| Sem operador bloqueante, pandas | 0,003 s, 0,306 s | 0,473 s, 0,580 s | 0,005 s, 0,495 s |
| `ORDER BY`, sem trabalho | 0,458 s, 0,755 s | 0,995 s, 1,041 s | 0,459 s, 0,960 s |
| `ORDER BY`, 5 ms | 0,457 s, 1,279 s | 0,973 s, 1,815 s | 0,485 s, 1,405 s |

- **O cursor próprio é o mais rápido** e é o que a sessão única não pode ter: ele segura a conexão
  enquanto o cliente trabalha. O transbordo inteiro soma a consulta ao trabalho do cliente, e o lote
  a lote os sobrepõe, pagando a escrita do arquivo no caminho da consulta, cerca de 0,18 s nestas
  13.333.333 linhas. Com 5 ms por lote, o lote a lote ficou 0,097 s atrás do cursor e 0,391 s à
  frente do transbordo inteiro; o primeiro lote chegou em 5 ms, contra 472 ms.
- **Com `threads = 8`** a ordem não mudou: sem trabalho, 0,299 s, 0,549 s e 0,511 s; com 5 ms por
  lote, 0,834 s, 1,348 s e 0,935 s; com `ORDER BY`, o primeiro lote em 0,164 s, 0,711 s e 0,169 s.
- **O `loader` com o arquivo e o `INSERT` único ganha do cursor próprio com um `INSERT` por lote
  numa transação enquanto o cliente produz depressa, e empata quando o trabalho do cliente domina**,
  6.000.000 de linhas em 60 lotes: sem trabalho, 0,747 s contra 1,101 s; com 2 ms por lote, 0,836 s
  contra 1,121 s; com 5 ms, 1,115 s contra 1,150 s. Com `threads = 8`, 0,883 s contra 1,116 s,
  0,949 s contra 1,125 s e 1,192 s contra 1,124 s.
- **O esquema do fluxo Arrow IPC só entra no arquivo com o primeiro lote**, ou no fechamento de um
  resultado vazio: abrir o leitor antes disso falhou com `Tried reading schema message, was null or
  length 0`, e o stream espera o primeiro lote gravado, ou o fim da consulta, antes de abrir o
  arquivo.
- **A memória**, na suíte (`test_duckdb.py::test_spooled_stream_bounds_memory`, um subprocesso,
  10.000.000 de linhas, quatro execuções): 94 MB nas três primeiras e 97 MB na quarta, o primeiro
  lote em 0,005 s a 0,006 s e a consulta em 0,369 s a 0,388 s, com 81 MB de arquivo, contra 322 MB
  da tabela inteira.
- **Na suíte** (`test_parallel.py`, quatro execuções): o primeiro lote de 2.000.000 de linhas chegou
  entre 0,004 s e 0,008 s, com a consulta ainda rodando, de um total de 0,066 s a 0,068 s com um
  comando da sessão no meio de cada lote; numa delas, o `close` depois do primeiro lote parou a
  consulta com 3 de 3.000 lotes gravados; o erro de conversão na linha 2.900.000 chegou como `OSError` depois de 27
  lotes; o pipeline de três estágios sobre 3.000.000 de linhas levou 0,433 s pela tabela inteira,
  0,691 s lote a lote dentro de `session()` e 0,406 s encadeado. A sessão de `new_session`, um
  `cursor()` da conexão, viu a tabela confirmada pela principal, recusou a temporária dela com
  `CatalogException` e rodou enquanto a principal estava num bloco `session()`; quatro tabelas de
  150.000 linhas entraram por `delta_scan` em 0,017 s em quatro sessões a mais, contra 0,066 s em
  série na sessão principal.

**Consequência**: o `stream` do DuckDB grava cada lote enquanto a consulta roda, `prefetch` saiu da
assinatura, e o `BatchStream` de `test_parallel.py` é a referência ([`PLAN.md`](PLAN.md), etapas
[4](PLAN-STAGE-4.md) e [5](PLAN-STAGE-5.md)); os dois motores ganharam `new_session()`, e
`run.ingest` de mais de uma tabela abre uma sessão a mais por tabela ([etapa 6](PLAN-STAGE-6.md)).
O item da ingestão de várias tabelas na sessão única saiu de [`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md).

## O que a sonda do pool de threads do DuckDB mostrou

Em 2026-09-23, no macOS com 11 núcleos (DuckDB 1.5.5), uma sonda no scratchpad mediu o que uma
sessão a mais acrescenta ao paralelismo do DuckDB, respondendo se cada sessão poderia ganhar threads
próprias.

- **O `threads` é da instância, e muda em execução.** `duckdb_settings()` dá `threads` e
  `external_threads` com escopo `GLOBAL`; `SET SESSION threads = 3` foi recusado com `Catalog Error:
  option "threads" cannot be set locally`; `SET threads = 3` e `SET GLOBAL threads = 3` foram
  aceitos, e outro cursor leu 3 em seguida.
- **A thread que chama cada sessão executa a consulta dela.** Numa varredura de 100.000.000 de
  linhas em memória (`SELECT sum(hash(id)) FROM t`, melhor de três), uma consulta sozinha e duas e
  quatro juntas, cada uma num cursor e numa thread Python: com `threads = 1`, 0,608 s, 0,622 s e
  0,639 s; com 2, 0,310 s, 0,473 s e 0,600 s; com 4, 0,159 s, 0,269 s e 0,449 s; com 8, 0,097 s,
  0,175 s e 0,326 s; com 11, o padrão, 0,079 s, 0,160 s e 0,313 s; com 22, 0,078 s, 0,156 s e
  0,307 s.
- **Com `threads` igual aos núcleos, uma consulta grande já ocupa a máquina**: duas sessões juntas
  levaram 2,01 vezes uma, o tempo delas em série, e o dobro dos núcleos não mudou nada.
- **Uma consulta que não se paraleliza deixa núcleos para as outras sessões**:
  `SELECT sum(hash(range)) FROM range(300_000_000)` levou de 1,78 s a 1,94 s com qualquer `threads`
  de 1 a 22, porque a função `range` gera as linhas numa thread só, e quatro juntas levaram de 1,07 a
  1,13 vez uma.
- A documentação do DuckDB ("How to Tune Workloads") diz que o paralelismo é por row group de
  122.880 linhas, que a leitura de arquivos remotos é síncrona, uma requisição HTTP por thread, e
  recomenda `threads` de 2 a 5 vezes os núcleos para ela.

A suíte repete o escopo como asserção e os tempos como leitura
(`test_concurrency.py::test_duckdb_thread_pool_is_global_and_each_caller_joins_it`, 20.000.000 de
linhas): com `threads = 1`, 0,122 s uma sessão e 0,128 s quatro juntas; com 11, 0,016 s e 0,061 s.

**Consequência**: a regra da sessão a mais em [`PLAN.md`](PLAN.md), a seção do paralelismo de
[`serialize-db.md`](serialize-db.md) e a estratégia do motor na [etapa 4](PLAN-STAGE-4.md) dizem que o
pool é da instância e que a sessão a mais ganha nas consultas pequenas, nos operadores que não se
paralelizam e na espera do S3; o `threads` da leitura do S3 entrou em
[`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md), à espera de uma medição no ambiente alvo. As medições estão
em [`duckdb.md`](duckdb.md).

## O que as sondas da revisão das etapas 3 e 4 mostraram

Em 2026-09-23, no macOS arm64 (M3 Pro com 11 núcleos e 18 GiB; Python 3.13.15, DuckDB 1.5.5,
PyArrow 25.0.1, deltalake 1.6.4, SQLAlchemy 2.0.54, duckdb-engine 0.17.0), sondas no scratchpad
leram o que as etapas [3](PLAN-STAGE-3.md) e [4](PLAN-STAGE-4.md) supõem antes da implementação. As
asserções estão nas suítes de estudo citadas em cada item.

- **O `IN` de lista não roda pelo caminho de compilação dos motores.** Compilado sem
  `literal_binds`, `coluna.in_([...])` sai `IN (__[POSTCOMPILE_mes_1])`, e o DuckDB recusa o texto
  com `Parameter argument/count mismatch`, e o `NOT IN` também; a auditoria roda o texto de
  `render`, com `literal_binds`, e não passa por esse caminho. Com os parâmetros do
  cliente dados por `statement.params(**params)` e `render_postcompile=True`, a lista sai expandida,
  o `bindparam(..., expanding=True)` do cliente também, e `construct_params()` devolve todos os
  valores. Um nome que o statement não tem é ignorado em silêncio, e o valor que falta é
  `InvalidRequestError` na compilação. No estilo `qmark`, o do driver do DuckDB, o texto roda com a
  lista de `positiontup`, sem a reescrita de `:nome` em `$nome`, que só lê nomes
  `[a-z_][a-z0-9_]*`; um `?` dentro de um literal de `text()` passa intacto
  (`test_sqlalchemy.py::test_in_list_needs_render_postcompile_on_the_engine_path`).
- **Uma `GenericFunction` se registra em `sa.func` para o processo inteiro.** Depois da classe
  `json_valid(GenericFunction)` com `@compiles` para o Redshift, como no rascunho da auditoria, o
  `sa.func.json_valid(meta)` do próprio cliente passou de `json_valid(meta)` a
  `is_valid_json(meta)` no Redshift. Uma subclasse de `FunctionElement` com `name` e `@compiles`
  por dialeto, como o `month_of` de [`sqlalchemy.md`](sqlalchemy.md), compila igual e deixa o
  `sa.func` do cliente intacto
  (`test_sqlalchemy.py::test_generic_function_subclass_registers_in_sa_func_for_the_whole_process`).
- **`published` compila para `delta_scan`.** `sa.func.delta_scan(sa.literal(uri),
  sa.literal_column("version := 0")).table_valued(*colunas)` rodou num `JOIN` com `NOT IN`, com o
  caminho como parâmetro ou como literal; `delta_scan($p, version := $v)` também roda.
- **A poda do `delta_scan` depende da forma do predicado.** Pelo log `FileSystem` do DuckDB, numa
  tabela de 12 partições com um arquivo cada: `=` e `IN` de um valor abriram uma pasta; `IN` de dois
  ou três valores e `OR` de dois `=` abriram as 12; `BETWEEN` e `>=` abriram as do intervalo, e
  `BETWEEN` somado ao `IN` também. Uma view sobre `delta_scan` com o `WHERE ... IN` leu as 12. O
  `EXPLAIN ANALYZE` de `BETWEEN` somado ao `IN` falha na extensão `delta` com `InternalException:
  ... total_files inconsistent!`, e a consulta e o `CREATE TABLE AS` com o mesmo predicado rodam,
  com a conexão usável depois
  (`test_deltalake.py::test_delta_scan_prunes_by_equality_and_range_not_by_in_list`).
- **`FileSystem.from_uri` vai à rede num `s3://` sem região.** Num ambiente despido (sem `AWS_*`,
  proxies num porto fechado, `HOME` vazio), `from_uri("s3://<bucket>/raiz")` respondeu em 0,49 s com
  `OSError: Bucket ... not found`, a consulta da região do bucket; com `?region=us-east-1` na URI,
  ou com `S3FileSystem(region=...)`, a construção levou 0,00 s, sem rede.
- **O predicado do `overwrite` aceita o nome entre aspas duplas**: `"data_str" = '2026-08-31'` e
  `"to" = 'a'`, a coluna de nome reservado.
- **Dois `create_write_transaction(overwrite)` da mesma partição conflitam.** A partir da mesma
  versão, o segundo recebeu `CommitFailedError: ... a concurrent transaction deleted data this
  operation read`, e a partição ficou com o arquivo do primeiro. O método devolve `None`, e o objeto
  que o chamou continua na versão lida
  (`test_deltalake.py::test_two_registrations_of_the_same_partition_conflict`).
- **O `RETURN_STATS` deixa o `NaN` fora do máximo, e o registro perde a linha.** Um `DOUBLE` com
  `NaN` sai com `has_nan: true` e o maior número como máximo. Registrado por `stat_converter`, o
  máximo 2,0 fez o `delta_scan ... WHERE valor > 3` devolver 0 linhas, contra 1 na tabela do
  sandbox, porque o DuckDB ordena o `NaN` acima de todo número. O infinito sai `inf`, e `float` o
  leva a `Infinity` no JSON do log, que não é JSON válido; o delta-rs o leu como estatística
  ausente. O texto longo sai truncado em 256 caracteres, com o máximo de 255 caracteres e o último
  incrementado, acima do valor real, e o texto multibyte longo sai sem mínimo e máximo: a poda por
  texto não perde linha (`test_duckdb.py::test_return_stats_leave_nan_out_and_bound_long_text`,
  `test_deltalake.py::test_nan_statistics_hide_rows_from_delta_scan`).
- **O `write_deltalake` também grava o máximo sem o `NaN`**, e o `delta_scan` responde conforme a
  poda: `valor > 3` deu 0 linhas, com o arquivo podado pelo máximo 2,0, e `valor >= 2` deu 2, com o
  `NaN` dentro. Numa tabela Arrow registrada no DuckDB, o filtro empurrado ao PyArrow dá `NaN > 3`
  falso, e as duas consultas deram 0 e 1. No infinito, o delta-rs grava `null` no extremo, e a poda
  acha a linha. A soma de controle da auditoria, `sum(CAST(valor AS DECIMAL(38, 6)))`, falha com
  `ConversionException: Could not cast value nan to DECIMAL(38,6)`, e com `inf` do mesmo modo,
  também sob `FILTER (WHERE isfinite(valor))`, porque o `CAST` é avaliado antes do filtro; um `CASE
  WHEN isfinite(valor)` soma só os finitos
  (`test_duckdb.py::test_control_total_fails_on_nan_and_infinity`).
- **`interrupt()` para de outra thread uma consulta presa num operador bloqueante.** Uma ordenação
  de 60.000.000 de linhas, cujo primeiro lote sai em 1,63 s, parou 2 ms depois do pedido com
  `InterruptException` (1,4 ms na suíte, com 20.000.000); a conexão continuou usável, um
  `interrupt()` ocioso não afetou o comando seguinte, e o da conexão não parou a consulta de um
  cursor dela (`test_duckdb.py::test_interrupt_stops_a_blocking_query_from_another_thread`).
- **`cursor()` não espera a consulta em curso na conexão**: voltou em 0,04 ms com uma ordenação
  rodando noutra thread, e o cursor consultou o mesmo banco
  (`test_duckdb.py::test_cursor_opens_while_the_connection_runs_a_query`).

**Consequência**: [`PLAN-STAGE-3.md`](PLAN-STAGE-3.md) constrói o `S3FileSystem` com a região do
ambiente, converte o `CommitFailedError` de `register_files` e de `rewrite` em `ExecutionConflict`
e deixa fora do registro o extremo infinito; [`PLAN-STAGE-4.md`](PLAN-STAGE-4.md) e
[`PLAN-STAGE-5.md`](PLAN-STAGE-5.md) compilam o statement com `render_postcompile=True` sobre
`statement.params(**params)` e conferem os nomes; a `ingest` com partições soma o intervalo delas ao
`IN`; `new_session()` cria o cursor sem o lock da sessão; a soma de controle da auditoria corre só
sobre os finitos; as funções da auditoria são subclasses de `FunctionElement`; e
[`PLAN.md`](PLAN.md) registra a poda por forma de predicado. Esperam o usuário, em
[`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md): o `Double` não finito no contrato da
[etapa 1](PLAN-STAGE-1.md), o estilo `qmark` e o `interrupt()` no `close` do stream, entre as
propostas da [etapa 4](PLAN-STAGE-4.md).

## O que as estratégias de stream, loader e ingestão mostraram

Em 2026-09-23, no mesmo macOS, uma sonda no scratchpad comparou desenhos de `stream` e de `loader`
sobre a sessão única e a ingestão de várias tabelas, cada caso num processo, melhor de três
execuções, sobre bancos em arquivo, com os mesmos dados e lotes de 100.000 linhas; `threads = 2`,
o ambiente alvo, salvo onde a tabela diz outro valor. O trabalho do cliente por lote é um `sleep`,
que solta o GIL, um laço Python puro calibrado, ou a conversão para pandas com uma coluna calculada.

**`stream`.** 20.000.000 de linhas em três colunas; a consulta sem operador bloqueante devolve
13.333.333 linhas em 134 lotes. O total em segundos, com o primeiro lote entre 0,004 s e 0,009 s em
todos os desenhos; o arquivo com LZ4 teve 178 MB, e sem compressão, 486 MB.

| Desenho | Sem trabalho | 5 ms de `sleep` | 5 ms de Python puro | pandas |
| --- | --- | --- | --- | --- |
| Atual: arquivo IPC com LZ4, lote a lote | 0,598 | 0,926 | 0,996 | 0,629 |
| Arquivo IPC sem compressão | 0,436 | 0,956 | 0,862 | 0,466 |
| Fila em memória sem limite, sem arquivo | 0,394 | 0,817, com 527 MB de pico | 0,694 | 0,404 |
| Híbrido: memória até 256 MiB, arquivo LZ4 depois | 0,407 | 0,825, com 515 MB de pico | 0,709 | 0,418 |
| Híbrido: memória até 64 MiB | 0,413 | 0,902, com 126 MB em arquivo | 0,673 | 0,420 |
| Cursor próprio, sem a sessão única | 0,399 | 0,839 | 0,673 | 0,408 |
| Sequencial, a consulta e o trabalho na mesma thread | 0,399 | 1,319 | 1,068 | 0,555 |

- **A compressão LZ4 no caminho da consulta é o custo do desenho atual**: 0,2 s em 13.333.333
  linhas, e o lock da sessão fica tomado por esse tempo a mais. Com trabalho em pandas, o desenho
  atual (0,629 s) ficou atrás do sequencial (0,555 s).
- **Com Python puro no cliente, a thread auxiliar espera o intervalo de troca do GIL** a cada
  retomada: 0,996 s contra 0,673 s do cursor e 1,068 s do sequencial. Com
  `sys.setswitchinterval(0.0005)`, o desenho atual fez 0,788 s, o arquivo sem compressão 0,759 s, a
  memória 0,605 s, o cursor 0,679 s e o híbrido de 256 MiB 0,717 s. Com 20 ms de Python puro por
  lote, 2,723 s, 2,594 s no híbrido e 2,550 s no cursor, perto dos 2,68 s de trabalho.
- **O híbrido fica no tempo do cursor** enquanto o cliente acompanha a consulta, com a memória
  limitada pelo orçamento, e grava no arquivo o que passa dele quando o cliente se atrasa. A fila
  sem limite chegou a 527 MB de pico com o `sleep` de 5 ms, contra 267 MB sem trabalho.
- Com `threads = 8`, sem trabalho e com 5 ms de `sleep`: o atual 0,520 s e 0,935 s, a memória
  0,258 s e 0,825 s, o cursor 0,255 s e 0,836 s.

**`loader`.** 6.000.000 de linhas em quatro colunas e 60 lotes prontos em memória, do primeiro
`write` ao fim do `close`; entre parênteses, o tempo do `close`. O arquivo com LZ4 teve 79 MB, sem
compressão 263 MB, em Parquet sem compressão 158 MB e com snappy 77 MB.

| Desenho | `threads = 2` | 5 ms de `sleep` por lote | `threads = 8` |
| --- | --- | --- | --- |
| Atual: arquivo IPC com LZ4 e um `INSERT` no `close` | 0,753 (0,660) | 1,052 (0,677) | 0,439 (0,346) |
| Arquivo IPC sem compressão | 0,697 (0,660) | 1,065 (0,688) | 0,376 (0,339) |
| Parquet sem compressão e `read_parquet` | 1,180 (0,696) | 1,189 | 0,826 |
| Parquet com snappy | 1,293 (0,721) | 1,290 | 0,923 |
| Cursor próprio, `INSERT` por lote numa tabela de nome oculto e `RENAME` no `close` | 1,223 | 1,230 | 0,998 |
| Cursor próprio, `INSERT` por lote numa transação | 1,327 (0,078) | 1,331 | 1,347 |

- **O desenho atual é o mais rápido dos medidos**: o `INSERT` único sobre o leitor do arquivo escala
  com `threads` (0,660 s com 2, 0,346 s com 8), e um `INSERT` por lote custa cerca de 20 ms. A
  compressão do arquivo corre na thread auxiliar, fora do caminho do cliente, e custou 0,05 s.

**O pipeline de três estágios**, `stream`, pandas e `loader`, com o `loader` criando a tabela como a
etapa 4 prevê. Primeiro lote e total, em segundos:

| Combinação | 6.000.000 de linhas | 20.000.000 | 20.000.000 com 5 ms de `sleep` |
| --- | --- | --- | --- |
| Atual, `loader` aberto depois do stream e criando a tabela na abertura | 0,246 e 1,028 | 0,811 e 3,120 | 0,804 e 4,290 |
| Atual, `loader` aberto antes do stream | 0,006 e 0,924 | 0,006 e 2,762 | 0,006 e 3,424 |
| Atual, `loader` criando a tabela no `close` | 0,006 e 0,927 | 0,006 e 2,761 | 0,006 e 3,415 |
| Stream atual e `loader` com `INSERT` por lote em cursor próprio | 0,006 e 1,259 | 0,006 e 4,152 | 0,007 e 4,116 |
| Cursor próprio e `loader` criando a tabela no `close` | 0,004 e 0,817 | 0,004 e 2,400 | 0,004 e 3,264 |

- **O `CREATE TABLE` que o `loader` roda sob o lock na abertura espera a consulta inteira do
  stream aberto antes dele**, a ordem do exemplo mensal de [`PLAN.md`](PLAN.md): o primeiro lote
  chegou em 0,811 s, a consulta toda, em vez de 0,006 s. Criar a tabela no `close`, no mesmo bloco
  do `INSERT`, desfaz a espera e custa o mesmo que abrir o `loader` primeiro.

**A ingestão de várias tabelas.** Quatro tabelas Delta de 12 partições cada, materializadas por
`CREATE TABLE AS SELECT * FROM delta_scan(..., version := 0)` num banco em arquivo, em série na
sessão principal ou numa sessão a mais por tabela, em quatro threads. Segundos e pico de memória:

| Tabelas | `threads = 2`, em série | Sessões a mais | `threads = 11`, em série | Sessões a mais |
| --- | --- | --- | --- | --- |
| 4 × 150.000 linhas | 0,155 | 0,043 | 0,140 | 0,112 |
| 4 × 2.000.000 | 1,079 | 0,443 | 1,258, 336 MB | 0,385, 603 MB |
| 4 × 8.000.000 | 3,498 | 1,629 | 1,382, 803 MB | 1,037, 917 MB |
| 12.000.000 e 3 × 300.000 | 1,491 | 1,322 | 1,038 | 0,856 |

- **As sessões a mais ganharam em todos os tamanhos em disco local**, e a memória cresceu com
  elas. Com `threads = 2` o ganho vem também das threads que chamam cada sessão, que executam a
  consulta dela ao lado do pool, numa máquina de 11 núcleos; o ambiente alvo tem 2 vCPUs, e ali o
  ganho de CPU some e fica o da espera do S3, ainda não medida. A mistura parecida com o pipeline
  mensal, uma tabela grande e três pequenas, ganhou 1,13 e 1,21 vezes.

**Consequência**: o `stream` da sessão única tem o custo medido acima, e o híbrido o devolve ao tempo
do cursor sem perder o limite de memória; o `loader` fica como está, com a criação da tabela no
`close` para não esperar o stream; a ingestão paralela fica. As três propostas esperam o usuário em
[`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md), e [`PLAN-STAGE-4.md`](PLAN-STAGE-4.md) registra a espera
do `loader`.

## O que a soma de controle e a implementação do stream híbrido mostraram

Em 2026-09-23, no mesmo macOS, duas sondas responderam às perguntas do usuário sobre a revisão das
etapas 3 e 4.

- **A soma de `DOUBLE` muda com o número de threads, e a soma por `DECIMAL(38, 6)` não.** Em
  20.000.000 de valores de até 1,2 × 10¹⁰ com centavos, `sum(valor)` deu cinco resultados com 1, 2,
  4, 8 e 11 threads, de 3.070.199.043.647.900,5 a 3.070.199.043.661.792, e o mais distante ficou a
  13.409 da soma exata; a mesma soma em ordem crescente e decrescente, numa thread, também diferiu.
  `sum(CAST(valor AS DECIMAL(38, 6)))` deu 3.070.199.043.661.309,110470 nas cinco, e o `fsum`, a soma
  compensada do DuckDB, 3.070.199.043.661.309,0, que o `DOUBLE` não representa com centavos nessa
  grandeza. Tirar o `CAST` da soma de controle evitaria a falha no `NaN`, e a soma de controle
  deixaria de ser comparável entre execuções; a poda do `delta_scan` com `NaN` não passa por `CAST`
  algum (`test_duckdb.py::test_control_total_by_decimal_does_not_depend_on_threads`).
- **A implementação de referência do stream híbrido passou nos casos do `BatchStream` atual e nos
  do orçamento**, doze execuções seguidas da bateria: o primeiro lote com a consulta rodando, a
  ordem, o transbordo com o cliente lento e a memória nunca acima do orçamento, o `close` antecipado,
  o abandono, o erro antes e depois do primeiro lote, na memória e no arquivo, a consulta dentro de
  `session()`, o resultado vazio com o esquema do leitor, e o comando da sessão que espera só a
  consulta. A primeira versão deixava um arquivo órfão em três de seis execuções: o arquivo nasce no
  primeiro lote que não cabe no orçamento, que pode vir depois de o `__del__` apagar o caminho, e a
  thread passou a apagar o arquivo que criou quando para pelo `stop`. A thread marca o fim da
  consulta ainda com o lock tomado, então o comando que roda depois do stream já o vê terminado.
- **Contra o desenho atual, na mesma rodada** (13.333.333 linhas, `threads = 2`, melhor de três):
  sem trabalho, 0,622 s contra 0,411 s; com 5 ms de Python puro por lote, 0,965 s contra 0,673 s; com
  pandas, 0,641 s contra 0,411 s, sem lote no arquivo; com 5 ms de `sleep`, o cliente atrasado,
  1,086 s contra 0,908 s, com 96 lotes no arquivo e 297 MB de pico. Com o orçamento de 256 MiB, os
  três primeiros casos ficaram iguais e o do cliente atrasado fez 0,839 s com 522 MB de pico.

**Consequência**: [`PLAN-STAGE-1.md`](PLAN-STAGE-1.md) registra, na decisão do `Double` não finito,
que o `CAST` da soma de controle fica; [`PLAN-STAGE-4.md`](PLAN-STAGE-4.md) propõe o híbrido com
64 MiB, à espera do usuário.

## O que os esboços do stream híbrido, do cancelamento e do loader mostraram

Em 2026-09-23, no mesmo macOS, os esboços de `tests/proof_of_concept/test_parallel.py` passaram a
implementar as decisões do usuário do mesmo dia: o `BatchStream` híbrido, com os lotes em memória
até 64 MiB e o arquivo Arrow IPC com LZ4 depois; o `interrupt()` no `close` do stream e no
`cleanup` do motor; e o `Loader` que confere o nome num cursor próprio, sem o lock da sessão, e cria
a tabela no `close`, numa transação com o `INSERT`. A suíte rodou cinco vezes seguidas, 14 casos
verdes em todas.

- **O cancelamento.** O `close` depois do primeiro lote de uma varredura que acha linhas só no
  começo da tabela (`id < 150000 OR md5(id::VARCHAR) = 'x'`, 20.000.000 de linhas) terminou em 7 a
  10 ms, contra 0,729 s da mesma consulta sem o `interrupt()` na sonda; o `cleanup` com um stream
  preso numa ordenação, noutra thread, terminou em 2 ms, e a construção desse stream recebeu o erro
  do cancelamento. O `interrupt()` que alcança a leitura do Arrow chega como `OSError: INTERRUPT
  Error: Interrupted!`, e o que alcança o `execute`, como `duckdb.InterruptException`; a sessão
  continuou usável depois dos dois.
- **O orçamento.** Com o cliente mais lento que a consulta e um orçamento de dois lotes, 28 dos 30
  lotes foram para o arquivo, a memória não passou de 1,9 MB, e as 3.000.000 de linhas saíram na
  ordem.
- **O `loader` depois do stream.** Na ordem do exemplo mensal, `stream` e depois `loader` no mesmo
  `with`, o primeiro lote chegou com a consulta ainda rodando; um `CREATE TABLE` pedido à sessão logo
  depois de abrir o stream só rodou com a consulta terminada. O `ROLLBACK` desfaz o `CREATE TABLE`
  quando o `INSERT` falha, e o nome fica livre.
- **O pipeline de três estágios** sobre 3.000.000 de linhas levou de 0,365 s a 0,370 s encadeado,
  contra 0,406 s a 0,436 s do desenho anterior nas execuções da suíte de 2026-09-22 e 2026-09-23;
  0,437 s a 0,439 s pela tabela inteira e 0,652 s a 0,668 s lote a lote sem threads.
- **Dois defeitos que só a repetição mostrou.** A primeira versão do híbrido deixava o arquivo de um
  stream abandonado em três de seis execuções da bateria, e a do `Loader` escrevia um aviso de
  `AttributeError` no `__del__` quando a abertura recusava o nome, porque o evento que o `__del__`
  usa nascia depois da recusa. A thread do stream apaga o arquivo que criou quando para pelo `stop`,
  e o `Loader` cria o evento antes da conferência do nome.

**Consequência**: [`PLAN-STAGE-4.md`](PLAN-STAGE-4.md) e [`PLAN.md`](PLAN.md) descrevem o stream
híbrido, o cancelamento e o `loader` com a criação no `close`, e a etapa 4 não tem decisão
pendente.

## O que as fontes e as sondas do `NaN` nas estatísticas mostraram

Em 2026-09-23, no mesmo macOS (DuckDB 1.5.5 com a extensão `delta` 45c4087, deltalake 1.6.4,
pyarrow 25.0.1), a pergunta do usuário sobre o PARQUET-1246 foi lida nas fontes e medida por três
sondas, repetidas com o mesmo resultado.

- **A especificação do Parquet.** O PARQUET-1246 (2018, parquet-mr 1.10.0 e 1.8.3) mudou o caminho
  de leitura da implementação Java: ignora o mínimo e o máximo de `float` e `double` quando eles são
  `NaN`, porque o próprio parquet-mr gravava o `NaN` como máximo. O `parquet.thrift` manda o escritor
  deixar o `NaN` fora do mínimo e do máximo (PARQUET-1222, parquet-format 2.10.0, 2022) e, desde o
  PARQUET-2249 (commit de 2026-05-26), gravar `nan_count` mesmo quando zero; o leitor sem
  `nan_count` supõe que pode haver `NaN`, e ignora o mínimo e o máximo numa busca que o `NaN`
  satisfaz. Só uma coluna de valores todos `NaN` fica sem mínimo e máximo. A ordem nova
  `IEEE_754_TOTAL_ORDER` põe o `NaN` positivo acima de todo número e mantém o mínimo e o máximo nos
  valores que não são `NaN`.
- **O protocolo Delta.** A estatística de arquivo fica no campo `stats` da ação `add`, texto JSON no
  `_delta_log` (no checkpoint, texto JSON ou `stats_parsed` dentro de um Parquet); `maxValues` é o
  maior valor válido do arquivo, sem contagem de `NaN` e sem menção a ele. O delta-kernel-rs grava o
  `NaN` como máximo (`test_file_stats_accumulator_float_nan_ordering` em
  `default-engine/src/stats.rs`) e não usa o mínimo e o máximo do rodapé numa coluna de partição de
  ponto flutuante. O Delta Spark descarta o mínimo e o máximo de `float` e `double` que colhe do
  rodapé de escritores que deixam o `NaN` de fora, como parquet-cpp, Arrow e pyarrow, e mantém os do
  parquet-mr (PR #7101, 2026-06-27, `collectStats.skipFloatingPointFromFooter` ligado por padrão).
  Nenhuma issue do delta-rs trata do máximo sem o `NaN` que ele copia para o log.
- **Os escritores do rodapé.** Num grupo de linhas com `NaN`, o `COPY` do DuckDB grava o grupo sem
  mínimo e máximo; o pyarrow (`parquet-cpp-arrow version 25.0.1`) e o delta-rs (`parquet-rs version
  59.3.0`) gravam os dois sem o `NaN`. A tabela nativa do DuckDB guarda o `NaN` como máximo do
  segmento (`[Min: 1.0, Max: nan]` em `pragma_storage_info`).
- **O leitor Parquet do DuckDB.** Sobre `[1.5, NaN, 2.0]` e `[4, 5, 6]` em dois grupos de linhas,
  `read_parquet(...) WHERE valor > 3` devolveu 3 linhas nos arquivos do pyarrow e do delta-rs e 4 no
  do DuckDB; `valor + 0 > 3`, que não desce ao leitor, devolveu 4 nos três. O leitor poda pelo
  máximo do rodapé um grupo que tem uma linha que o próprio DuckDB ordena acima de todo número, o que
  a regra de leitura da especificação proíbe. É a issue
  [duckdb/duckdb#25521](https://github.com/duckdb/duckdb/issues/25521), aberta em 2026-09-09 e
  marcada `reproduced`, que também mostra `!=` perdendo e `<=` ganhando a linha. A tabela nativa
  devolveu 4.
- **O `delta_scan`.** Sobre dois arquivos do DuckDB registrados, `[1.5, NaN, 2.0]` e `[4, 5, 6]`,
  `valor > 3` devolveu 3 com o máximo 2,0 no log e 4 com o valor sem mínimo e máximo ou com o máximo
  `null`. A propriedade `delta.dataSkippingStatsColumns` sem a coluna não muda a leitura de um log
  que traz a estatística (3). O `write_deltalake` respeita a propriedade e grava o log sem o valor,
  mas o rodapé continua com o máximo sem o `NaN`, e o `delta_scan` perdeu a linha pela poda do grupo
  (3). Com `ColumnProperties(statistics_enabled="NONE")` na coluna, o rodapé e o log saem sem a
  estatística do valor (o `nullCount` dele também sai), e o `delta_scan` e o `read_parquet`
  devolveram 4.
- **O `has_nan` do `RETURN_STATS`.** Em 4.096 linhas gravadas em dois grupos de 2.048, o `has_nan`
  saiu falso com o `NaN` só no primeiro grupo (na linha 7 ou na linha 0) e verdadeiro com ele no
  último grupo ou nos dois; o mínimo e o máximo cobrem os números dos dois grupos. Nenhuma issue do
  DuckDB trata disso.

**Consequência**: a proposta de `register_files` omitir o mínimo e o máximo quando o `RETURN_STATS`
traz `has_nan` cai, e omitir só no log não protege o modo `rewrite`. O usuário decidiu no mesmo dia
gravar sem mínimo e máximo, no rodapé e no log, as colunas `Double` com valor não finito em cada
partição, pela contagem da auditoria. Um caso a mais mediu a regra: o `writer_properties` do
`write_deltalake` vale para a chamada, uma por partição, e o `delta_scan ... WHERE valor > 3`
devolveu a linha do `NaN` da partição sem estatística e não abriu o arquivo da outra, podado pelo
máximo 2,5. [`PLAN-STAGE-3.md`](PLAN-STAGE-3.md), [`PLAN-STAGE-4.md`](PLAN-STAGE-4.md),
[`PLAN-STAGE-6.md`](PLAN-STAGE-6.md), [`delta.md`](delta.md) e [`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md)
descrevem a regra e o que continua aberto. Os casos entraram nas suítes de estudo:
`test_duckdb.py::test_return_stats_has_nan_follows_only_the_last_row_group`,
`test_duckdb.py::test_parquet_reader_prunes_the_nan_row_group_by_the_arrow_footer`,
`test_deltalake.py::test_float_statistics_off_keep_the_nan_row` e
`test_deltalake.py::test_float_statistics_off_per_partition_keep_the_nan_row_and_the_pruning`.

## O que as sondas das decisões da etapa 6 mostraram

Em 2026-09-23, no mesmo macOS (DuckDB 1.5.5, deltalake 1.6.4, pyarrow 25.0.1), duas sondas
responderam às decisões pendentes da etapa 6, e os casos entraram nas suítes de estudo.

- **O `load` esquecido numa thread.** Com o `Loader` de referência de `test_parallel.py`, um `load`
  disparado num `ThreadPoolExecutor` sem `result()` e, logo depois, uma leitura da mesma tabela: a
  leitura falhou com `Catalog Error: Table with name destino does not exist!` na sessão principal e
  numa sessão a mais, a leitura depois da carga contou as 5.000 linhas, e um segundo `Loader` no
  mesmo nome foi recusado. A premissa do item da barreira por tabela, a leitura do estado anterior
  em silêncio, é anterior à decisão do mesmo dia de o `loader` criar a tabela no `close`: sem nome
  reaproveitável não há estado anterior, e a corrida vira um erro intermitente. O teste rodou seis
  vezes seguidas, verde em todas.
- **O valor de partição com caracteres especiais.** O `write_deltalake` com `partition_by` gravou as
  pastas `p=a%3Ab`, `p=a%25b`, `p=a%23b`, `p=a%C3%A7%C3%A3o` e `p=d%27agua` para `a:b`, `a%b`,
  `a#b`, `ação` e `d'agua`, e o `path` das ações `add` codificou a pasta de novo (`p=a%253Ab`);
  `2026-Q1` saiu igual nos dois. O predicado `p = 'd'agua'` falhou com `DeltaError: Generic
  DeltaTable error: External error: Generic error: Unterminated string literal at Line: 1, Column:
  12`, e os de `:`, `%`, `#` e acento passaram. A expressão `regexp_full_match(v,
  '[0-9A-Za-z][0-9A-Za-z_.-]*')` do DuckDB concordou com o `re.fullmatch` do Python em doze
  valores, entre eles o vazio, `..`, `-x`, `_x` e `x.y_z-1`.

**Consequência**: o usuário decidiu no mesmo dia as quatro pendências da etapa 6, e
[`PLAN-STAGE-6.md`](PLAN-STAGE-6.md) não tem decisão pendente: `--metadata` no `serialize-db run`,
`next_ids` só na chave sequencial, a regra da partição `[0-9A-Za-z][0-9A-Za-z_.-]*` no valor e no
`execution_id`, e a barreira por tabela fora das etapas, que saiu de
[`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md). O `loader` do Redshift passou a recusar o nome ocupado e
a criar a tabela no `close`, na transação do `COPY` ([`PLAN-STAGE-5.md`](PLAN-STAGE-5.md)), e
[`PLAN-STAGE-4.md`](PLAN-STAGE-4.md), [`PLAN.md`](PLAN.md), [`delta.md`](delta.md) e
[`docs/index.md`](../docs/index.md) escrevem a regra. Os casos:
`test_parallel.py::test_read_during_a_forgotten_load_fails_instead_of_reading_old_rows` e
`test_deltalake.py::test_partition_value_is_percent_encoded_in_the_folder_and_the_log`.

## O que a sonda do dataclass congelado de `Database` mostrou

Em 2026-09-23, no mesmo macOS (Python 3.13), a correção da interface da etapa 6 leu o campo
`storage` de `Database`. Num dataclass congelado, o campo `init=False` atribuído no `__post_init__`
levantou `FrozenInstanceError: cannot assign to field 'storage'`. Com `functools.cached_property`, o
atributo nasceu uma vez, no primeiro uso, a igualdade e o hash seguiram só os campos, e o campo
`root` continuou congelado.

**Consequência**: [`PLAN-STAGE-6.md`](PLAN-STAGE-6.md) declara `storage` como `cached_property`, sem
o `object.__setattr__` que o estilo do projeto evita, e o caso entrou em
`test_stdlib.py::test_frozen_dataclass_derives_an_attribute_by_cached_property`.

## O que a leitura do driver e a sonda das decisões da etapa 5 mostraram

Em 2026-09-23, a discussão das decisões pendentes da etapa 5 leu o código do `redshift_connector`
2.1.16 instalado no projeto e rodou uma sonda no mesmo macOS (pyarrow 25.0.1).

- **O `cursor.description` do driver não traz precisão nem escala.** `Cursor._getDescription`
  devolve `(nome, oid, None, None, None, None, None)` por coluna. O `type_modifier` de cada coluna
  chega na mensagem `RowDescription`, `Connection.handle_ROW_DESCRIPTION` o guarda em
  `cursor.ps["row_desc"]`, e `Cursor.truncated_row_desc` o usa para decodificar o `NUMERIC`
  binário, com a escala `(type_modifier - 4) & 0xFFFF`. O enum `RedshiftOID` tem, além dos OIDs do
  rascunho de `schema_from_description`, `REAL` (700), `BPCHAR` (1042), `TEXT` (25), `UNKNOWN` (705)
  e `SUPER` (4000), e o driver lê o `SUPER` como texto.
- **Um `decimal128(18, 2)` fixo recusa o `NUMERIC` de outra escala e de outra precisão.**
  `pa.array([Decimal('1.234567')], type=pa.decimal128(18, 2))` falhou com `ArrowInvalid: Rescaling
  Decimal value would cause data loss`, e `pa.array([Decimal('12345678901234567.00')],
  type=pa.decimal128(18, 2))` com `ArrowInvalid: Decimal type with precision 19 does not fit into
  precision inferred from first array element: 18`.

**Consequência**: o usuário decidiu no mesmo dia as quatro pendências da etapa 5, e
[`PLAN-STAGE-5.md`](PLAN-STAGE-5.md) não tem decisão pendente. `schema_from_row_description` lê a
precisão e a escala do `type_modifier`, com `redshift-connector==2.1.16` no extra `redshift`
([`PLAN.md`](PLAN.md)). A mesma discussão achou no plano o que nenhuma sonda mediu: o limite de
linhas entre `fetchmany` e `UNLOAD` não tinha como ser aplicado, porque o `execute` que revelaria o
tamanho já materializa o resultado; o destino do `rewrite`, `staging/<execution_id>/<tabela>/`,
seria recusado pelo `UNLOAD` a partir da segunda partição, porque `run.publish` exporta uma
partição por chamada; e o destino do `register` estaria ocupado na reexecução com o mesmo
`execution_id`. O `stream` passou a ir sempre por `UNLOAD`, o `load` sempre pelo `loader`, a
exportação a um prefixo novo por partição e por tentativa, sem `PARTITION BY`, e a tabela de
controle a ser criada só pelo usuário ([`PLAN-STAGE-8.md`](PLAN-STAGE-8.md)). Os comportamentos que
isso supõe no ambiente alvo esperam a próxima execução da suíte
([`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md)).

## O que a regra do `Double` não finito mudou no script de migração

Em 2026-09-23, no mesmo macOS, `scripts/migrate_parquet_to_delta.py` passou a seguir a regra da
issue #59: `check_partition` acha, na mesma consulta que confere o contrato, as colunas `Double`
com `NaN` ou infinito na partição, que saem sem mínimo e máximo no log (`register`) e no rodapé e no
log (`rewrite`, `ColumnProperties(statistics_enabled="NONE")`); o relatório soma cada `Double` só
nos valores finitos e compara os não finitos contados na origem e no Delta.

- **A base fictícia** (`tests/source_db_projetado.py`), sem valor não finito, rodou pela linha de
  comando antes e depois da mudança, nos dois modos: os relatórios JSON ficaram iguais sem os
  campos novos e sem os tempos, os campos novos saíram todos zerados, e as estatísticas das 24
  ações `add` do log saíram iguais.
- **Uma origem com `NaN` e infinito** em `valor`, em duas partições de `cad_lancamentos`
  (`test_migrate_parquet_to_delta.py::test_nonfinite_double_leaves_min_max_out_of_its_partition`,
  nos dois modos): as duas partições sem o mínimo e o máximo de `valor` no log, as demais com eles;
  o arquivo da partição do `NaN` sem os dois no rodapé nos dois modos, porque o `COPY` do DuckDB já
  os omite no grupo com `NaN`; o relatório igual nos dois lados, com um não finito em cada uma das
  duas partições; e `delta_scan ... WHERE valor > 1e300` devolveu as duas linhas.

**Consequência**: [`PLAN-STAGE-7.md`](PLAN-STAGE-7.md) descreve o script com a regra, e o item da
issue #59 em [`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md) separa a versão que rodou no ambiente alvo.
