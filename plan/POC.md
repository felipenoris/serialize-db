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
3.13.15) com as variáveis do workgroup: a leitura das 20:37 e a repetição das 20:43 com a raiz S3
informada, que mudou duas linhas. Os números abaixo saem delas, e o histórico do git guarda os
relatórios.

**O que respondeu.** Workgroup `controladoria-wg` no namespace `controladoria-ns`, conta
`<conta>`, capacidade base 8, sem acesso público e com roteamento VPC melhorado; nenhum cluster
provisionado. Os três endpoints regionais do Redshift e o host do workgroup resolvem para IP privado
(`10.100.x.x`): o ambiente alvo tem endpoint VPC de interface para todos, e `RS-14` passou, o que
responde a dúvida que a leitura do laboratório deixou — a credencial temporária e a Data API
funcionam ali sem internet. A porta 5439 abriu em 0,00 s. A credencial temporária saiu para o
usuário `IAMR:<usuário>@<projeto>`, válida por uma hora, e a sessão abriu com ela. O
ciclo da Data API devolveu `select 1` em 23 ms. A versão é `1.0.436211`, muito acima do patch 186
que a escrita em datashare exige. `SUPER` e `JSON_PARSE` respondem.

**O esquema e o banco.** `svv_redshift_databases` mostra `dev` local com isolamento de snapshot e
`datalake_rw_shared` do tipo `shared`, vindo do datashare `controladoria_rw_datashare` da conta
produtora `<conta do produtor>`, com isolamento `UNKNOWN`. `svv_all_schemas` põe `sbx_aco_decon` só nesse
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
informada, ela respondeu `0` em 1,5 s, e `svv_external_schemas` respondeu `0` logo depois. A visão é
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
(`s3://bndes-aco-models-<conta>/dzd-<domínio>/<projeto>/shared/bndes_grupos_bases_analise_financeira/databases/prd/db_projetado`,
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
`s3://bndes-aco-models-<conta>/dzd-<domínio>/<projeto>/shared/serialize-db-tests`.
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

**O bucket** (`bucket.py`): `bndes-aco-models-<conta>` em `sa-east-1`, SSE-KMS pela chave
`<chave>` com bucket key, SSE-C bloqueado, acesso público bloqueado,
versionado pela amostra (`VersionId` no marcador de pasta); o papel não lê versionamento, Object Lock,
propriedade, ciclo de vida, política nem uploads incompletos, como no laboratório. Sob a raiz de
testes há um marcador de pasta de 2026-09-20 e nada mais: nenhuma tabela Delta, nenhuma sessão da
suíte, nenhuma versão não corrente. A suíte S3 ainda não rodou lá.

**O catálogo** (`catalog.py`): o Glue tem o banco `glue_db_<id>` com uma tabela Parquet e
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
- A Data API em `PICKED` continua em [`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md). Os relatórios ficam
  fora do git (decisão do usuário de 2026-09-23).

## O que os rascunhos das etapas mostraram

Em 2026-09-21, no macOS arm64 com deltalake 1.6.4, DuckDB 1.5.5, PyArrow 25.0.1, SQLAlchemy 2.0.54,
duckdb-engine 0.17.0 e sqlalchemy-redshift 1.0.0, os rascunhos das etapas 1 a 9 rodaram no
scratchpad; cada um deu lugar ao módulo da sua etapa, e a seção "Rascunhos executados" de cada
`PLAN-STAGE-<n>.md` virou "A implementação", com os rascunhos das etapas 3 a 9 guardados no
histórico do git. O que eles mostraram além do que já estava medido:

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
raiz `s3://bndes-aco-models-<conta>/dzd-<domínio>/<projeto>/shared/serialize-db-tests`
e a credencial temporária do workgroup. A sessão durou 10,7 s: um teste passou e dez reprovaram. O
relatório não entrou no git: cada leitura do ambiente que ele trazia se repete nas execuções
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
  `IAMR:<usuário>@<projeto>`, versão `1.0.436211`, que o driver
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
O relatório não entrou no git: cada leitura do ambiente que ele trazia se repete nas
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
não entraram no git: cada leitura do ambiente que eles traziam se repete nas execuções limpas
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
(92,4 s e 61,8 s), e a limpeza apagou 42 e 43 objetos e as tabelas de cada sessão; o histórico do
git guarda os dois relatórios. As duas execuções concordam em cada leitura, e são as duas execuções limpas que a
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
  assume UTC. O `cast` aceitava os dois, e a decisão do usuário de 2026-09-23 os recusa
  ([etapa 1](PLAN-STAGE-1.md); seção "O que a sonda do fuso mostrou").
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
64 MiB, que o usuário aprovou em 2026-09-23.

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
`test_engine_duckdb.py::test_read_during_a_forgotten_load_fails_instead_of_reading_old_rows` e
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

## O que as sondas locais dos casos da suíte Redshift mostraram

Em 2026-09-23, no mesmo macOS (SQLAlchemy 2.0.54, sqlalchemy-redshift 1.0.0, redshift-connector
2.1.16, deltalake 1.6.4, DuckDB 1.5.5, pyarrow 25.0.1), a preparação dos casos da etapa 5 para a
próxima execução da suíte Redshift sondou o que dá para medir sem o Redshift.

- **O texto do `stream` por `UNLOAD`.** O statement Core com os valores dados por `params` e
  compilado com `literal_binds` e `render_postcompile` pelo `RedshiftDialect_redshift_connector`:
  - o `paramstyle` padrão, `format`, dobra o `%` dos literais (`'50%% d''agua \\ fim'`), e
    `named` o mantém (`'50% d''agua \\ fim'`); o `redshift_connector` manda sem conversão o texto
    executado sem parâmetros (`Connection.execute`, `has_bind_parameters`), e o `%%` chegaria
    assim ao servidor;
  - a aspa simples sai dobrada e a contrabarra também, nos dois estilos: o dialeto tem
    `_backslash_escapes` verdadeiro, o escape do PostgreSQL;
  - a data sai `'2026-08-31'`, o timestamp `'2026-08-31 12:30:01.123456'`, o número `10.25`, o
    `Float` `0.1` e o `IN` de lista `IN (1, 2, 3)`;
  - `compiled.binds` sai vazio sob `literal_binds`, e um `IN` de lista sem valor saiu `IN (NULL)`,
    com o `SAWarning` só na comparação; o percurso do statement (`sqlalchemy.sql.visitors.iterate`)
    achou o `BindParameter` com `required`, que some depois de `params(ids=[1])`;
  - o `text()` com `params` falhou com `CompileError: No literal value renderer is available for
    literal value "d'agua \ barra" with datatype NULL`; com cada `bindparam` criado pelo valor
    (`sa.bindparam(nome, value=valor, expanding=...)`), os tipos saíram `String`, `Date`,
    `Integer`, `Numeric` e `DateTime`, e os literais iguais aos do statement Core;
  - o `query` sem `literal_binds` sai `IN (:ids_1, :ids_2, :ids_3)`, e `construct_params()` dá os
    nomes expandidos.
- **O arquivo registrado abaixo da pasta Hive.** Numa tabela Delta local particionada por `mes`, os
  arquivos gravados em `mes=<valor>/exec_42_<uuid>/0000_part_0<n>.parquet` e registrados por
  `create_write_transaction` foram lidos pelo delta-rs (as 5 linhas, com `mes`) e pelo `delta_scan`
  (3 e 2 linhas por partição, e o filtro por `mes` também). Um arquivo órfão numa subpasta dessas
  não entrou no `vacuum` padrão, que só apaga o que o log removeu, e entrou no `vacuum(full=True)`.
- **Os casos novos num emulador.** `test_unload_to_a_hive_prefix_and_register`,
  `test_stream_by_unload_with_literal_values`, `test_unload_limit_empty_result_temp_table_and_super`,
  `test_row_description_oids_and_type_modifier`, `test_small_load_copy_cost` e
  `test_unload_footer_statistics_with_nan` rodaram contra um emulador descartável, o DuckDB no
  lugar do Redshift (o `UNLOAD` e o `COPY` traduzidos para `read_parquet` e para o `pyarrow`) e
  uma pasta no lugar do S3. O emulador confere só o código Python dos testes, e achou um defeito:
  o caso do `NaN` filtrava `valor > 2`, que o máximo 3.0 do rodapé satisfaz, e não mostraria a linha
  perdida pela poda; com `valor > 3`, o arquivo do pyarrow com o `NaN` no início, no meio e no fim
  do grupo de linhas deu 0, a perda da issue #59. No DuckDB, que não trata a contrabarra como
  escape, o caso da contrabarra saiu diferente pelo texto com literais e pelo `UNLOAD`, e o registro
  do teste mostrou as duas listas.

**Consequência**: [`PLAN-STAGE-5.md`](PLAN-STAGE-5.md) corrige o guarda do `stream`, que dizia
ler `compiled.binds`, para o percurso do statement com `required`, como `render`, e registra o
`paramstyle="named"` e o texto pronto com os `bindparam` tipados pelo valor. O que o Redshift faz
com a contrabarra dobrada, dentro e fora do `UNLOAD`, e os demais casos esperam a próxima execução
da suíte no ambiente alvo ([`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md)).

## O que a implementação da etapa 3 mostrou

Em 2026-09-23, no mesmo macOS (deltalake 1.6.4, DuckDB 1.5.5, pyarrow 25.0.1, boto3 1.43.98), a
implementação de `serialize_db.storage` e `serialize_db.delta` leu a API do delta-rs, do
`pyarrow.fs` e do DuckDB antes de cada primitiva, e os casos entraram em `tests/test_storage.py` e
`tests/test_delta.py`.

- **O retry do delta-rs contra uma rede morta.** Num subprocesso despido (`HOME` inexistente,
  credenciais fictícias, `AWS_EC2_METADATA_DISABLED`), `DeltaTable.is_deltatable` contra o endpoint
  `http://10.255.255.1:9`, que não responde, desistiu em 57,0 s com as opções padrão, em 10,3 s com
  `max_retries` 1 e `retry_timeout` 10 s, e em 10,3 s com `max_retries` 3 e o mesmo `retry_timeout`;
  contra uma porta fechada em `127.0.0.1`, em 2,4 s, 0,3 s e 0,6 s. O `retry_timeout` é o teto, e
  as três tentativas não o alongam.
- **O `pyarrow.fs`.** `S3FileSystem(region=...)` nasceu em 0,014 s sem rede; no `LocalFileSystem`,
  `delete_file` de um caminho ausente levanta `FileNotFoundError`, `copy_file` para uma pasta que
  não existe falha, e `FileSelector(..., allow_not_found=True)` de uma pasta ausente devolve a lista
  vazia. `Storage.delete` ignora o ausente, e `Storage.copy` cria a pasta local do destino.
- **O esquema Delta e o `alter`.** Os tipos de `arrow_schema(table)` voltaram iguais de
  `pa.schema(dt.schema())` em todos os tipos do contrato, com a nulidade; `create` aceita
  `description=None`; `set_column_metadata` junta a chave nova às que o campo tinha, sem apagar; o
  `Field` de `delta_schema` entra em `add_columns` com o comentário; e três `alter` seguidos no
  mesmo objeto commitaram as versões 2, 3 e 4, com o objeto na última.
- **A estatística desligada no rodapé.** Com `ColumnProperties(statistics_enabled="NONE")`, a coluna
  do arquivo do delta-rs sai com `statistics` `None` no rodapé, não com `has_min_max` falso.
- **A cópia profunda.** O esquema do leitor de `to_pyarrow_dataset().scanner().to_reader()` leva a
  nulidade e os comentários da versão, e `write_deltalake(..., mode="error", name=...,
  description=..., configuration=...)` nasceu na versão 0 com os três.
- **As estatísticas achatadas.** `get_add_actions(flatten=True)` tipa `min.<coluna>` e
  `max.<coluna>` pelo tipo da coluna (`date32`, `decimal128(18, 2)`, `timestamp[us]`), e
  `partition.<coluna>` sai `string not null`.
- **As conferências do registro.** Cada uma das dez recusas de `register_files` saiu pela
  conferência pretendida, lidas as mensagens: tamanho, linhas do rodapé, pasta da partição,
  `expected_rows`, caminho absoluto, coluna do contrato ausente, `valor` em `BYTE_ARRAY` no lugar de
  `DOUBLE`, a coluna de partição dentro do arquivo, as colunas fora da ordem do contrato e 20 nulos
  numa coluna `NOT NULL`, contados pelo rodapé. As três últimas conferências são da implementação:
  o `COPY` do Redshift lê o Parquet por posição, e a [etapa 7](PLAN-STAGE-7.md) conta com a
  recusa do nulo no modo `register`.
- **A releitura.** Um registro com o máximo da chave abaixo do real passou pelas conferências do
  rodapé, e o `read_back` o pegou pelo limite do log (`o log registra 101..105, e os dados têm
  101..110`), voltou a versão por `restore` e deixou a partição com as 10 linhas anteriores.
- **O `hive_partitioning` na leitura da exportação.** O `read_parquet` das pastas exportadas leu
  `data_str=2026-07-31` como `DATE`; com `hive_types_autocast = false`, como texto, a mesma leitura de
  `test_deltalake.py::test_initial_load_from_parquet_folders`.
- **A folha de uma coluna aninhada no rodapé** (2026-10-01). A conferência do esquema indexava o
  rodapé por `ColumnSchema.name`, o nome da folha: um arquivo com `valor` dentro de um struct `s`
  registrou a versão 1, e o delta-rs leu 20 nulos em 20 linhas, sem que `read_back` o pegasse,
  porque ela lê só as chaves; uma coluna aninhada a mais era recusada pelo nome da folha (`x` de
  `aninhada.x`, `element` de uma lista). A conferência lê o caminho da folha e recusa a coluna
  aninhada como fora do contrato (`coluna aninhada s.valor fora do contrato`).
- **O `SandboxError` da releitura depois do commit** (2026-10-01). `register_files` lia os limites
  do DuckDB na conexão da releitura, depois do commit: com a leitura forçada a recusar, a versão
  ficou gravada com o arquivo e as 5 linhas, e a releitura não rodou; antes da decisão de
  2026-10-01, a leitura negativa abria o DuckDB no padrão dele e a releitura rodava. Os limites
  passaram a ser lidos antes do commit, e a cópia profunda e a exportação por reescrita os leem
  antes de criar o destino (`test_the_memory_is_read_before_the_commit`, que reprova no código
  anterior; decisão do usuário de 2026-10-02).

**Consequência**: [`PLAN-STAGE-3.md`](PLAN-STAGE-3.md) troca a interface e os rascunhos pela seção
"A implementação" e registra o que a implementação fixou: os métodos de caminho de `Storage`
(`relative`, `uri_of`, `size`, `ensure_folder`, `open_input_file`), `duckdb_connect`, o `retry` de
`storage_options` sem `timeout`, `table_exists` e `file_from_return_stats`, as três conferências
novas, a releitura que compara o log com os leitores, e os não finitos de `rewrite`. A revisão das
etapas 4 a 9 depois da etapa 3 levou a elas a interface de `Storage` e de `delta`.

## O que a implementação da etapa 4 mostrou

Em 2026-09-23, no mesmo macOS (DuckDB 1.5.5, duckdb-engine 0.17.0, sqlalchemy-redshift 1.0.0,
SQLAlchemy 2.0.54), a implementação de `serialize_db.audit` e do motor DuckDB leu o compilador, o
driver e a documentação antes de cada primitiva, e os casos entraram em `tests/test_audit.py` e
`tests/test_engine_duckdb.py`.

- **O `FILTER` nos agregados.** O texto do Redshift que o rascunho de 2026-09-21 imprimia levava
  `count(*) FILTER (WHERE ...)`, e a sintaxe do `COUNT` na documentação do Redshift não tem a
  cláusula (`COUNT( * | expression )`, `COUNT ( [ DISTINCT | ALL ] expression )`). A auditoria conta
  por `count(CASE WHEN <defeito> THEN 1 END)`, que os dois motores aceitam, e o texto do DuckDB de
  cada verificação do modelo cliente rodou num DuckDB em memória sobre o DDL da etapa 1.
- **A regra padrão das funções.** Uma subclasse de `FunctionElement` com `@compiles` só para o
  Redshift falhou no dialeto do DuckDB com `UnsupportedCompilationError`: o `duckdb_engine` compila
  pelo `PGCompiler`, e a subclasse não tem regra padrão. Cada função da auditoria ganhou a regra
  padrão, o nome com os argumentos, e `sa.func.json_valid` continuou uma `Function` comum.
- **A ordem de inserção e o primeiro lote.** Com `preserve_insertion_order = false`, o ajuste do
  motor, a consulta `WHERE id < 150000 OR md5(id::VARCHAR) = 'x'` sobre 20.000.000 de linhas deu o
  primeiro lote só no fim com duas threads (1,093 s de 1,093 s), contra 0,373 s de 1,108 s com a
  ordem preservada; com 11 threads, 0,289 s de 0,289 s contra 0,331 s de 0,339 s. O filtro de uma
  partição entre doze deu o primeiro lote em 2 a 3 ms nos quatro ajustes, e o `CREATE TABLE AS` de
  17.000.000 de linhas levou 0,533 s e 802 MB acima da base com a ordem livre, contra 0,908 s e
  876 MB com ela (duas threads; com 11, 0,145 s e 808 MB contra 0,389 s e 955 MB), o melhor de três
  para os tempos do stream. A consulta sem `ORDER BY` sai em ordem arbitrária, e o teste do
  orçamento do stream, que conferia a ordem das linhas, passou a ordenar a consulta.
- **O teste do cancelamento.** Com a ordem livre, a consulta rara do teste herdado de
  `test_parallel.py` terminava antes do `close`, e o teste falhou em três de seis rodadas sem
  erro de interrupção; com `SET preserve_insertion_order = true` só naquela sessão, o `close`
  cancelou a consulta em 14 ms em seis rodadas seguidas.
- **O driver.** `to_arrow_table()` de um `CREATE` ou de um `INSERT` devolve a tabela `Count`, e de
  um `SET` ou de um `DROP`, `Success`; `cursor()` de um cursor abre outra conexão ao mesmo banco; o
  banco em arquivo tem um `.wal` ao lado enquanto está aberto, e só o `.duckdb` depois do `close`.
- **A carga do JSON inválido.** A coluna `JSON` do DuckDB recusa o texto inválido na carga, e o
  teste da auditoria planta o JSON inválido numa tabela criada por `CREATE TABLE AS`, com a coluna em
  `VARCHAR`: a verificação `json_*` serve à tabela que o pipeline cria por SQL e ao Redshift.

**Consequência**: [`PLAN-STAGE-4.md`](PLAN-STAGE-4.md) troca a interface e os rascunhos pela seção
"A implementação" e registra a contagem por `CASE`, a regra padrão das funções, o custo do primeiro
lote com a ordem livre, `referenced` na auditoria do motor, `totals` e `rows` no relatório, o
esquema do primeiro lote no `loader` e o `CAST` de cada coluna na exportação. O `is_finite` do
Redshift, `x NOT IN ('NaN'::float8, 'Infinity'::float8, '-Infinity'::float8)`, espera uma execução
no ambiente alvo ([`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md)).

**O `memory_limit` de `0MiB` e o negativo no DuckDB 1.5.5** (2026-10-01, o contêiner da sessão,
15,72 GiB físicos): `duckdb.connect(config={"memory_limit": "0MiB"})` recusa com
`OutOfMemoryException` (`failed to allocate data of size 32.0 KiB (8.0 KiB/0 bytes used)`), e no
motor a recusa vinha na primeira consulta, a do log da abertura, fora do `try` que apaga o que o
motor criou; `-5MiB` e `-1` abrem com `current_setting('memory_limit')` em `10.6 GiB`, o padrão do
DuckDB, 80% da memória que ele lê da máquina, e `1MiB` abre e conta `range(1000000)`. Pelas
leituras de `environment_limits` num `/proc` e num cgroup fabricados: `MemAvailable` de 1,5 MiB dá
`0MiB`, de 3 MiB dá `1MiB`, o uso do cgroup 100 MiB acima do limite dele dá `-50MiB`, e o uso igual
ao limite, sem cache, dá `0MiB`. **Consequência**: a memória lida abaixo de 2 MiB, ou negativa, é
recusada com `SandboxError` nomeando a leitura, antes da abertura, pela decisão do usuário do mesmo
dia ([`PLAN-STAGE-4.md`](PLAN-STAGE-4.md)), e o motor lê a memória só sem `memory_limit` na
configuração.

## O que a implementação da etapa 6 mostrou

Em 2026-09-23, no mesmo macOS (DuckDB 1.5.5, deltalake 1.6.4, Python 3.13), a implementação de
`serialize_db.execution` e dos subcomandos `run` e `audit` rodou os casos de
`tests/test_execution.py`, e a suíte local inteira rodou seis vezes seguidas para achar os testes
instáveis.

- **O pool da publicação.** Com todas as tabelas entregues ao `ThreadPoolExecutor` de uma vez, como
  o `publish_all` de `test_parallel.py`, e um worker só, a falha da segunda tabela chegou ao laço
  principal depois de o worker pegar a terceira: o `shutdown(cancel_futures=True)` não a cancelou, e
  a terceira tabela publicou. O esboço só passava porque cada tarefa dormia 0,5 s. `Execution`
  entrega uma tabela ao pool só com um worker livre e nenhuma falha, e o teste com um worker deu a
  primeira concluída, a segunda com a falha e a terceira cancelada em cinco rodadas.
- **A exceção com o resultado de cada tabela.** A falha sobe com o seu tipo e o resultado de cada
  tabela numa nota (`BaseException.add_note`, Python 3.11 em diante), e a linha de comando traduz o
  tipo no código de saída: `ExecutionConflict` em 2, `AuditFailed` em 1.
- **O primeiro lote e o `close` sob carga.** O teste do cancelamento de `test_parallel.py`, e o do
  motor que o repete, falharam uma vez em seis rodadas da suíte inteira, sem erro de interrupção, e
  alongar a varredura com o `md5` duplo (1,37 s depois do primeiro lote, contra 0,74 s) não bastou.
  Um reprodutor com três processos simultâneos, 15 tentativas cada, deu 44 interrupções e um caso em
  que o primeiro lote só saiu no fim da consulta, aos 4,531 s, com o segundo já na memória: não
  havia o que cancelar. Os dois testes passaram a conferir o que vale nos dois casos, a thread
  terminada e a sessão livre em menos de 1 s depois do `close`, com o erro nulo ou o da interrupção,
  e a suíte inteira passou seis vezes seguidas, com o `close` em 16 a 28 ms e o `OSError` da
  interrupção em todas; o cancelamento estrito continua conferido pelo `cleanup` com a ordenação.
- **A pasta temporária.** O `DuckDBEngine` padrão nasce numa pasta de `tempfile.mkdtemp`, fora da
  raiz autorizada dos testes: os testes da execução constroem o motor com `temp_directory` sob a
  raiz, e os da linha de comando apontam `tempfile.tempdir` para ela.

**Consequência**: [`PLAN-STAGE-6.md`](PLAN-STAGE-6.md) troca a interface e o rascunho pela seção
"A implementação" e registra o pool que entrega uma tabela por worker livre, a nota com o resultado
de cada tabela, o snapshot só na execução sem erro, `referenced` na auditoria, o motor Redshift e
`publish_redshift` fora da etapa até as etapas 5 e 8, e o `serialize-db audit` sobre um sandbox
DuckDB próprio.

## O que o texto da auditoria para o Redshift mostrou localmente

Em 2026-09-23, no mesmo macOS, a preparação do caso da auditoria na suíte Redshift renderizou o
texto de `audit_sql(..., "redshift")` e o DDL de `schema.ddl(..., "redshift")` para uma tabela com
chave primária, partição com `partition_source`, `String(n)`, `Double`, `Numeric(18, 2)` e `JSON`.

- **Os nomes saem sem esquema.** O DDL cria `"<prefixo>auditoria"` e o texto de cada verificação
  cita `"<prefixo>auditoria"."<coluna>"`, sem o esquema: no Redshift, quem os resolve é o
  `SET search_path TO <esquema>` do `connect` da etapa 5, depois do `USE`, que nunca rodou no
  esquema do datashare. A suíte sempre citou as tabelas em duas partes.
- **A coluna JSON vira `SUPER`, e a auditoria chama `is_valid_json` sobre ela.** A documentação do
  `is_valid_json` fala de uma string [uncertain se aceita `SUPER`], e a verificação de linhas junta
  todas as medidas numa consulta só: uma medida recusada derruba as demais.
- **O resto do texto**: `count(CASE WHEN (...) THEN 1 END)` em cada contador,
  `octet_length`, `~ '^[0-9A-Za-z][0-9A-Za-z_.-]*$'`, `to_char(<data>, 'YYYY-MM-DD')`, a soma
  `sum(CASE WHEN (<valor> NOT IN ('NaN'::float8, 'Infinity'::float8, '-Infinity'::float8)) THEN
  CAST(<valor> AS NUMERIC(38, 6)) END)` e a amostra por `LIMIT 20`.
- **O emulador.** `test_audit_sql_under_search_path_and_nan_comparison` rodou no emulador local da
  seção anterior, com o `SET search_path` traduzido para `USE` e macros do DuckDB no lugar de
  `is_valid_json` e de `to_char`: a tabela com os defeitos plantados deu o esperado em cada contador
  (4 linhas, 1 fora da partição, 2 não finitos, totais `4.500000` e `16.250000`), com o `NaN` igual
  a si mesmo, como o DuckDB o compara, e os textos do modelo cliente rodaram sobre as tabelas vazias.

**Consequência**: o caso lê no ambiente alvo o `search_path`, o `is_valid_json` sobre `SUPER`, a
comparação do `NaN` e cada medida isolada ([`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md), "O texto da
auditoria no Redshift"), e [`PLAN-STAGE-5.md`](PLAN-STAGE-5.md) registra que o `search_path` resolve
os nomes sem esquema.

## O que a revisão do código de 2026-09-23 mostrou

Em 2026-09-23, no mesmo macOS, a revisão de `src/` e de `tests/` contra as regras de código do
repositório rodou cada arquivo alterado sem variável e com a raiz local, e as suítes de estudo
perderam os rascunhos do pacote (decisão do usuário do mesmo dia). O que as execuções mostraram:

- **O gancho que pula as suítes.** `pytest_collection_modifyitems` conferia `marker in
  item.keywords`, e o id de um parâmetro entra nos keywords: o caso `[redshift]` de um teste
  parametrizado por dialeto foi pulado como parte da suíte Redshift. O gancho confere a marca do
  item (`get_closest_marker`), e a seleção dos 453 testes coletados saiu igual à anterior.
- **A máscara do relatório.** `record` mascarava só um valor de texto, e o `repr` de um erro
  guardado num dicionário passava. A máscara vai na saída, na impressão e no JSON: uma sessão com
  credenciais falsas num dicionário, numa lista e num texto não as mostrou em nenhuma das duas, e o
  JSON continuou válido.
- **O loader abandonado.** A thread do loader terminava sem apagar o arquivo de transbordo, que
  ficava até o `cleanup`, ao contrário do que a docstring e a [etapa 4](PLAN-STAGE-4.md) diziam. A
  thread terminada com erro apaga o arquivo, e a asserção da pasta vazia falha no código anterior.
- **Um teste gravava fora da raiz autorizada.** `test_temporary_folder_is_created_and_removed`
  criava a pasta do `DuckDBConfig()` por `tempfile.mkdtemp`, na pasta temporária do sistema; o
  teste aponta `tempfile.tempdir` para a raiz local.
- **O ambiente que um teste deixava no processo.** O `test_prepare_environment` de
  `test_stdlib.py` deixava `NO_PROXY` e `AWS_DEFAULT_REGION` definidas depois do teste: o `delenv`
  de uma variável ausente não registra o que restaurar.
- **A espera pela ordenação.** `duckdb_memory()`, lida numa sessão a mais, mostrou memória
  `ORDER_BY` cerca de 13 ms depois do início da ordenação de 20.000.000 de linhas, e o `interrupt`
  nesse ponto levantou `INTERRUPT Error: Interrupted!`; o teste do `cleanup` espera essa memória,
  com prazo de 10 s, no lugar de um `sleep` de 0,2 s. O `close` do stream interrompeu em 0,026 a
  0,030 s, com `OSError`.
- **O erro de conversão no meio do stream** chegou como `OSError` do leitor Arrow, depois de 25 ou
  26 lotes, nos dois orçamentos (seis execuções).
- **Os casos que vieram dos esboços**, agora sobre o motor: 28 de 30 lotes foram ao arquivo, com
  pico de 1,9 MB em memória; o `cleanup` levou 0,002 s; o pipeline de três estágios levou 0,55 a
  0,58 s pela `pa.Table`, 0,75 a 0,88 s em série e 0,41 a 0,46 s encadeado (três execuções).
- **A consulta DNS de `test_probes.py`.** Nomes `.invalid` sem cache levaram de 54 a 1.534 ms, a
  faixa de nomes inexistentes de `example.com` (43 a 1.368 ms), e `localhost`, 0,3 ms: a consulta
  chega ao servidor DNS, e a docstring do módulo a menciona.
- **Os valores fixados em `test_deltalake.py`**, com o deltalake 1.6.4 no macOS arm64: o
  `drop_column_not_null` grava a operação `CHANGE COLUMN` no histórico, e a leitura de uma versão
  cujo arquivo o `vacuum` apagou levanta `FileNotFoundError`; falta a confirmação no Linux.
- **A base fictícia refeita por linha.** Os construtores de `tests/source_db_projetado.py` passaram
  a montar uma linha por dicionário; as 26 tabelas, com esquema, metadados, `-0.0` e `NaN`, e os 63
  arquivos gravados saíram byte a byte iguais aos anteriores.

**Consequência**: `src/serialize_db/engine/duckdb.py` apaga o arquivo do loader abandonado,
`tests/conftest.py` confere a marca e mascara na saída, e [`CURRENT_STATE.md`](CURRENT_STATE.md)
registra as contagens novas. As correções que mudam o comportamento das suítes que só rodam no
ambiente alvo passaram pelo substituto local da seção seguinte.

## O que o substituto local das suítes S3 e Redshift mostrou

Em 2026-09-23, no mesmo macOS, as correções que as revisões de 2026-09-22 e 2026-09-23 acharam nas
suítes que só rodam no ambiente alvo rodaram num substituto local descartável, no scratchpad da
sessão: o servidor do moto 5.2.3 no lugar do S3 e do STS, e um plugin do pytest que troca
`redshift_connector.connect` por uma conexão sobre o DuckDB em memória, com o `COPY` e o `UNLOAD`
feitos pelo `boto3` e pelo `pyarrow` sobre o moto. O substituto confere só o código Python dos
testes.

- **As três suítes** (`test_s3.py`, `test_redshift.py` e `test_redshift_transactions.py`) deram 35
  aprovados e 1 pulado, a Data API sem `SERIALIZE_DB_REDSHIFT_WORKGROUP`, antes e depois das
  correções: as extrações mecânicas da revisão também rodam.
- **Cada correção do tratamento de falha**, com a falha provocada no substituto e o código anterior
  e o corrigido rodados lado a lado:
  - sem o manifesto de um `UNLOAD ... MANIFEST` que passou, o código anterior aprovou
    `test_stream_by_unload_with_literal_values` com `'unload': '[]'` no relatório, registrou `ok:
    sem manifesto linhas` para a tabela temporária e caiu com `TypeError` nos casos do `SUPER` e do
    `NaN`; o corrigido reprova os quatro casos com `o UNLOAD passou e não gravou o manifesto em
    <destino>`, por `unloaded_manifest`;
  - com o `UNLOAD ... PARTITION BY` recusado (`0A000`), o anterior pulou
    `test_unload_partition_by_and_register` pela condição `share_database`, e o corrigido reprova,
    porque o comando passou no ambiente alvo em 2026-09-21;
  - com o `INSERT` de A recusado em `test_writes_to_distinct_tables`, o anterior deixou a transação
    abortada de A aberta, com os bloqueios dela, enquanto `finish` esperava B por até 120 s
    (`FINISH_WITHIN`); o corrigido registra o `ROLLBACK` depois do erro, por `end_transaction`;
  - com o `SELECT pg_backend_pid()` recusado, o anterior caiu com `TypeError` na abertura do
    participante, no `.result[0][0]` do resultado vazio; o corrigido registra o erro em
    `redshift.transactions.pid.A` e `redshift.transactions.pid.B`, roda o cenário, e `finish`
    registra a sessão presa sem pid em vez de encerrá-la;
  - `outcome` e `reading` pegam só `SERVICE_ERRORS` (`redshift_connector.Error`,
    `botocore.exceptions.ClientError`, `DeltaError`, `pa.ArrowException` e `OSError`): um
    `ProgrammingError` saiu `ProgrammingError: 42P01 relation "x" does not exist detalhe`, e
    `ZeroDivisionError` e `AttributeError` subiram. No código anterior, um erro do próprio teste
    virava leitura, e a asserção de `test_copy_varchar_overflow`, que espera o `COPY` recusado,
    passava com ele.
- **As correções de forma e de relatório**: `describe_error` em `tests/conftest.py`, usado pelas
  duas suítes Redshift e pela limpeza, com o erro do driver em `XX000 <mensagem>` no lugar do
  dicionário cru; `redshift.cleanup` no relatório (`46 de 46 tabela(s) apagada(s) de emulador`);
  `statement_timeout` e `pid` por participante; os participantes como gerenciadores de contexto, e A
  fechada quando B não conecta; a fixture `duckdb_connection` pedindo `s3_location`, que põe
  `AWS_REGION` no ambiente antes do secret, e fechando a conexão; `run_on_own_connection` no lugar
  do `count_from` que escolhia o modo. Em `test_s3.py`: a origem das credenciais e o proxy de
  `test_boto3_credential_source` no relatório antes do STS, que pode não responder; em
  `test_data_file_encryption`, a criptografia e a chave KMS do arquivo do delta-rs comparadas com as
  de um objeto que o `boto3` grava sem opção (`None` nos dois, no moto); em
  `test_boto3_list_copy_delete`, a listagem do `list_objects_v2` comparada com
  `DeltaTable(uri).file_uris()`, e não com `storage.data_files()`, que sai do mesmo paginador; a
  docstring da extensão ausente.
- **O que o substituto não mostra**: o DuckDB não faz uma transação esperar a outra, e `b_espera_a`
  saiu `False` nos cinco cenários; o moto não devolve `ServerSideEncryption`; as credenciais vieram
  de variáveis (`env`), sem proxy; a Data API foi pulada.

**Consequência**: as correções ficam nas suítes, e a próxima execução delas no ambiente alvo lê o
que o substituto não tem: os bloqueios da [etapa 8](PLAN-STAGE-8.md), a criptografia padrão do
bucket e a Data API. [`CURRENT_STATE.md`](CURRENT_STATE.md) registra as suítes corrigidas.

## O que o substituto local versionado mostrou

Em 2026-09-23, no mesmo macOS, o substituto passou a `tests/emulator.py`, ligado por
`SERIALIZE_DB_TEST_EMULATOR` em `tests/conftest.py`, e o moto 5.2.3 entrou fixado no grupo `dev`
com o `flask` 3.1.3 e o `flask-cors` 6.0.5.

- **As dependências do moto.** O `moto[server]==5.2.3` trouxe 61 pacotes, entre eles `cfn-lint`,
  `docker`, `sympy` e `aws-xray-sdk`; o `moto[s3]` com o `flask` e o `flask-cors` trouxe 28 e
  serviu o S3 e o STS por `python -m moto.server`, no ar em 0,16 s, com o 412 da escrita
  condicional. Com só `AWS_REGION` definida, o `boto3` 1.43.98 tomou a região do perfil do usuário
  e o `CreateBucket` sem `LocationConstraint` recebeu `IllegalLocationConstraintException`: o
  substituto define também `AWS_DEFAULT_REGION`.
- **O secret do DuckDB contra o moto.** No DuckDB 1.5.5, o secret `credential_chain` com
  `REGION 'us-east-1'` foi a `https://emulador.s3.us-east-1.amazonaws.com`, porque o DuckDB não lê
  `AWS_ENDPOINT_URL`. Com só o `ENDPOINT`, o que `Storage.duckdb_setup` montava, o endereço saiu
  `https://emulador.127.0.0.1:<porta>`, sem resolução de nome; com `URL_STYLE 'path'`, erro de SSL;
  com `USE_SSL false` sem `URL_STYLE`, o mesmo nome sem resolução; com as três opções, a leitura.
  O endereço por caminho é o que o delta-rs e o PyArrow usam com um endpoint próprio.
- **O substituto sem remendos.** A versão descartável trocava `redshift_connector.connect` e
  `Storage.duckdb_setup` em tempo de execução. Na versionada, `connect_redshift` escolhe a conexão
  do substituto, a biblioteca monta o secret com o endpoint, e as duas suítes criam o secret delas
  por `duckdb_s3_secret`, com as opções da biblioteca. O código anterior a este `tests/conftest.py`
  não alcança o substituto: um lado a lado roda o arquivo de teste antigo com o `conftest.py`
  atual.
- **As execuções.** As três suítes deram 35 aprovados e 1 pulado, a Data API, com a variável só.
  Com a raiz local, a suíte inteira deu 456 aprovados e 1 pulado em 70 s, e os casos `s3` de
  `test_storage.py` e de `test_delta.py` rodaram no moto pelo `Storage.duckdb_connect`. Os três
  testes de `test_conftest_redshift.py`, que conferem o caminho do driver, reprovaram com o
  substituto ligado até o próprio teste desligá-lo. Num ambiente despido (`env -i`, com `HOME` e
  `TMPDIR` em pastas vazias e `.venv/bin/python -m pytest`), as três suítes deram o mesmo
  resultado, as duas pastas continuaram vazias, o repositório não mudou, e o relatório não trouxe
  resposta da AWS de verdade (chave inválida, assinatura, `amazonaws.com`). O moto saiu com a
  sessão em cada execução.
- **As falhas provocadas**, sobre o código corrigido: `SERIALIZE_DB_TEST_EMULATOR_NO_MANIFEST`
  reprovou os quatro casos do manifesto; `SERIALIZE_DB_TEST_EMULATOR_FAIL_SQL` com `PARTITION BY`
  reprovou `test_unload_partition_by_and_register`, com o `INSERT` de A levou ao `ROLLBACK`, e com
  `pg_backend_pid` registrou o erro dos dois participantes e rodou o cenário.

**Consequência**: `Storage.duckdb_setup` leva ao secret o endereço por caminho e, num endpoint
`http`, `USE_SSL false` ([`PLAN-STAGE-3.md`](PLAN-STAGE-3.md)), e
[`CURRENT_STATE.md`](CURRENT_STATE.md) registra o substituto e as contagens.

## O que as suítes mostraram no Linux x86_64

Em 2026-09-23, num contêiner Linux x86_64 com 4 vCPUs e 15 GiB (Python 3.13.12, DuckDB 1.5.5,
PyArrow 25.0.1), as duas medições de memória de `tests/proof_of_concept/test_duckdb.py` reprovaram
e o resto passou: sem variável, 213 aprovados e `test_streaming_query_bounds_memory` reprovado; com
a raiz local, 381 aprovados e também `test_spooled_stream_bounds_memory` reprovado. Na suíte
inteira, cada cenário marcou o mesmo pico da tabela inteira, 935 MB sem variável e 1.117 MB com a
raiz local. Com só os dois casos, o leitor em lotes marcou 195 MB e o arquivo de transbordo 199 MB,
contra 362 MB da tabela inteira e o teto de metade dela; sozinho, o caso do leitor passou três
vezes, com 164 MB e 165 MB contra 336 MB, a 3 MB do teto. No ambiente alvo, na sessão
`-m "not redshift"` do mesmo dia às 18:48 UTC (Python 3.13.15, a versão da `main`), os dois casos
reprovaram com o mesmo sintoma, a tabela inteira, o leitor e o arquivo de transbordo em 1.120 MB, e
os outros 430 casos passaram.

- **O `ru_maxrss` de um processo novo começa no pico do processo pai, no Linux.** Um filho de um
  pai com 524 MB leu 524 MB de `ru_maxrss`, com `VmHWM` de 9 MB em `/proc/self/status`, e continuou
  lendo 524 MB depois de o pai liberar a memória. A base do subprocesso da suíte, antes de qualquer
  consulta, foi de 161 MB sob um pai que tinha importado o módulo de teste, contra 53 MB sob o
  `uv run`: a leitura do leitor em lotes era o pico do pytest, não a do leitor. Lançado direto do
  shell, o `ru_maxrss` bateu com o `VmHWM` (9 MB); pelo `uv run`, herdou os 37 MB do `uv`. O macOS
  não mostrou a herança: lá a suíte leu 83 MB para o leitor, lançado pelo pytest.
- **A base do processo, lida por `VmHWM`, é de 92 MB no Linux**: o Python com 9 MB, o
  `import duckdb` com 42 MB, a conexão com 2 MB e o `import pyarrow` com 39 MB, com o `mimalloc`
  como pool padrão do PyArrow. Com a medida certa e o teto absoluto, o arquivo de transbordo ficou
  entre 132 MB e 145 MB, contra o teto de 168 MB, de que a base ocupa mais da metade.
- **O acréscimo sobre a base**, em cinco execuções dos dois casos sozinhos e duas da suíte inteira,
  com 10.000.000 de linhas: a tabela inteira com 335 MB, de 242 MB a 243 MB acima da base; o leitor
  em lotes com 102 MB, de 9 MB a 10 MB acima; o arquivo de transbordo de 130 MB a 138 MB, de 37 MB a
  45 MB acima, com 81 MB de arquivo. Um leitor que guarda todos os lotes numa lista acrescentou
  245 MB e reprovou; sob um pai com 500 MB, a medida nova leu 10 MB de acréscimo e a antiga, 673 MB
  de pico. O `import pyarrow` feito pelo `to_arrow_reader`, com a consulta já rodando, deixou o
  leitor em 103 MB sobre uma base de 53 MB.

**Consequência**: as duas sondas de `test_duckdb.py` leem o pico do próprio processo (`VmHWM` no
Linux, `ru_maxrss` no macOS), importam o PyArrow antes da base e comparam o acréscimo de cada
cenário sobre ela, com o mesmo teto de metade do acréscimo da tabela inteira. O `peak_rss_mb` de
`scripts/migrate_parquet_to_delta.py` lê o `ru_maxrss` do próprio processo: por
`.venv/bin/python`, como no `README.md` para o ambiente alvo, ele herda só o pico do shell, e pelo
`uv run`, os 37 MB do `uv`. Com a correção, as três sessões deram no Linux as contagens do macOS
([`CURRENT_STATE.md`](CURRENT_STATE.md)): 214 aprovados e 243 pulados sem variável, 383 e 74 com a
raiz local, 456 e 1 com o substituto.

## O que os probes mostraram no ambiente alvo em 2026-09-23

Em 2026-09-23, entre 19:18 e 19:19 UTC, os cinco probes rodaram de novo no ambiente alvo, a partir
da `main`, sobre a raiz `.../shared/serialize-db-tests`. Nenhuma checagem reprovou, e as chamadas
que falharam são as leituras negadas ao papel, os serviços sem rota e o pacote `sagemaker_studio`,
já conhecidos.

- **A máquina** (`space.py`): 4 vCPUs, 15,4 GiB de memória e 29,7 GiB livres de 37,0 GiB num disco
  só; o DuckDB 1.5.5 com 4 threads e `memory_limit` de 12,3 GiB. Em 2026-09-21 eram 2 vCPUs e
  7,6 GiB: a instância do espaço mudou. A `.venv` traz o grupo `dev` com o moto 5.2.3 (`SP-9`), e a
  rede é a mesma, sem proxy e sem internet, com os endpoints VPC de interface do STS, das três APIs
  do Redshift, do Glue, do Athena, do Secrets Manager e do DataZone.
- **`RS-8`** (`redshift.py`): depois do `USE datalake_rw_shared`, `svv_table_info` respondeu
  `permission denied for relation svv_table_info` (42501) ao papel do projeto. `RS-19` passou pelo
  critério novo, com `sbx_aco_decon.<tabela>` resolvendo e `current_database()` em `dev`, e `RS-5`
  leu `USAGE` e `CREATE` falsos depois do `USE`, como em 2026-09-21. A credencial temporária do
  workgroup vale uma hora (`RS-15`), e a de quem chama expirava em 56 minutos (`RS-18`).
- **O teste de alcance** (`bucket.py`, `redshift.py`): o IAM e o KMS não conectaram por TCP em 2 s,
  e `simulate_principal_policy` e `describe_key` não foram chamadas, contra 10 s e 80 s de espera
  em 2026-09-21.
- **O bucket** (`bucket.py`): SSE-KMS com bucket key (`BK-5`); sob a raiz dos probes, 219 versões
  não correntes (1.388.530 bytes) e 219 marcadores de exclusão (`BK-14`); o ciclo de vida, a
  política, os uploads multipart, o versionamento e o Object Lock negados, como em 2026-09-21. Em
  `diagnose_aws.py`, o boto3, o delta-rs e o DuckDB listaram o prefixo.
- **O catálogo** (`catalog.py`): o Glue com um banco e uma tabela Parquet, o Athena com três
  workgroups, e o Lake Formation e o S3 Tables sem resposta (`ConnectTimeoutError` em 60,6 s e
  30,2 s): o gatilho de reavaliação não disparou.

**Consequência**: a leitura da distribuição atribuída da [etapa 8](PLAN-STAGE-8.md) precisa de uma
fonte que o papel leia, e a escolha entrou nas decisões pendentes da etapa;
[`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md) perdeu os itens de `svv_table_info` e do teste de alcance.

## O que a sonda do fuso mostrou

Em 2026-09-23, no contêiner Linux x86_64 com Python 3.13.12, `duckdb` 1.5.5 e `pyarrow` 25.0.1, o
mesmo instante, 12:00 UTC, perdeu o fuso por duas camadas:

- `CAST(TIMESTAMPTZ '2026-09-23 12:00:00+00' AS TIMESTAMP)` no DuckDB, com `SET TimeZone =
  'America/Sao_Paulo'`, deu `2026-09-23 09:00:00`: a hora na zona da sessão.
- O `cast` do PyArrow de `timestamp[us, tz=America/Sao_Paulo]` para `timestamp[us]` deu
  `2026-09-23 12:00:00`: a hora UTC.
- No pandas 3.0.6, `serie.dt.tz_convert("America/Sao_Paulo").dt.tz_localize(None)` sobre o mesmo
  instante deu `09:00` em `datetime64[us]`, e o `cast` novo o aceitou numa coluna `DateTime`; a série
  com fuso foi recusada com `ContractError` (`t.quando: timestamp com fuso UTC numa coluna DateTime
  sem fuso; ...`).

**Consequência**: a hora gravada dependia da camada que tirava o fuso, e a decisão do usuário de
2026-09-23 recusa no `cast` o `timestamp` com fuso numa coluna sem fuso e o inverso
([`PLAN-STAGE-1.md`](PLAN-STAGE-1.md)). O caso de `America/Sao_Paulo` numa coluna com fuso continua
aceito, no mesmo instante em UTC. `docs/index.md` traz a conversão do pandas. Os casos novos de
`tests/test_schema.py`, de `tests/test_audit.py` e de `tests/test_engine_duckdb.py` reprovaram (cinco)
com `schema.py` e `audit.py` anteriores e passam com os novos.

## O que a sonda da retenção mostrou

Em 2026-09-23, no mesmo contêiner, com `deltalake` 1.6.4, `serialize_db.delta.create_table` numa
pasta local gravou `delta.logRetentionDuration = interval 3650 days` e
`delta.deletedFileRetentionDuration = interval 400 days` na versão 0.
`open_table(uri, storage).alter.set_table_properties({"delta.deletedFileRetentionDuration":
"interval 90 days"})` gravou a versão 1, sem ação de arquivo, com a propriedade nova e a do log
mantida, e `vacuum_keeping_snapshots(..., retention_hours=24 * 90)` listou nada, porque os arquivos
eram do mesmo dia.

**Consequência**: `docs/index.md`, seção "Retenção dos arquivos removidos", documenta a retenção de
400 dias, mantida pela decisão do usuário de 2026-09-23 ([`PLAN-STAGE-9.md`](PLAN-STAGE-9.md)), o
bucket versionado que só libera espaço com `NoncurrentVersionExpiration`, e como mudar a janela do
`vacuum` e a propriedade de uma tabela existente.

## O que o pytest sem variável gravou

Em 2026-09-23, no contêiner Linux x86_64, `test_source_db_projetado.py` (14 casos) e os 17 casos de
`test_probes.py` que gravam o relatório ou arquivos fabricados passaram a `local`, gravando numa
pasta nova sob `SERIALIZE_DB_TEST_LOCAL_ROOT` no lugar da pasta temporária do pytest (decisão do
usuário de 2026-09-23). A suíte sem variável rodou num ambiente despido (`env -i`, `HOME` e `TMPDIR`
em pastas vazias, os proxies numa porta fechada, `.venv/bin/python -m pytest -p no:cacheprovider`):
187 aprovados e 279 pulados, as duas pastas continuaram vazias, e o repositório não ganhou arquivo.

**Consequência**: a premissa de [`PLAN.md`](PLAN.md), `pytest` sem variável não grava arquivo algum,
e o cabeçalho de `tests/conftest.py` valem como estão; [`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md)
perdeu o item da pasta temporária do pytest.

## O que as suítes mostraram no ambiente alvo em 2026-09-23

Em 2026-09-23, no ambiente alvo (Python 3.13.15, deltalake 1.6.4, DuckDB 1.5.5, pyarrow 25.0.1,
boto3 1.43.98, SQLAlchemy 2.0.54), a partir da `main`, a sessão `-m "not redshift"` rodou com as
raízes local e S3 às 18:48 UTC, e a suíte `-m redshift` duas vezes, às 18:52 e às 18:55 UTC
([`readings/`](readings/README.md)). Cada execução da suíte Redshift aprovou 24 casos e reprovou
`test_stream_by_unload_with_literal_values` no caso da contrabarra. A segunda repetiu a primeira
leitura a leitura, com outros tempos e ids; o `UNLOAD` paralelo nomeou `0000_part_00.parquet` e
`0064_part_00.parquet`, e as leituras de 2026-09-21 se repetiram (o `COPY` posicional que reprova,
o `FILLRECORD` com 100 linhas, o `34510` do cache do driver, o `SUPER` acima de 65.535 bytes só
por `FORMAT JSON 'auto'`).

- **O bucket.** Na sessão das 18:48, os 430 casos além das duas medições de memória (seção "O que
  as suítes mostraram no Linux x86_64") passaram, entre eles os casos `s3` de `test_storage.py` e
  `test_delta.py` e a suíte S3: o bucket com SSE-KMS e bucket key, o arquivo de dados do delta-rs
  cifrado sem opção alguma, e a cadeia de credenciais do delta-rs nas cinco variantes, pelo papel do
  contêiner. As 20 consultas pontuais levaram 5,9 s por `delta_scan`, 2,8 s por `ATTACH ...
  PIN_SNAPSHOT`, 1,2 s por `read_parquet` e 0,020 s na tabela materializada.
- **O `UNLOAD` para um prefixo com `=`** (`test_unload_to_a_hive_prefix_and_register`): sem
  `PARTITION BY`, para `<uri>/mes=<valor>/exec_poc_<uuid>/`, passou nas duas partições, com o
  arquivo `000.parquet` do `PARALLEL OFF`; o `schema.elements` do manifesto listou as cinco colunas
  do `select`, sem a de partição; o registro e o `delta_scan` deram 3 linhas por partição; o segundo
  `UNLOAD` no mesmo destino foi recusado (`Specified unload destination on S3 is not empty`) e o de
  um `uuid` novo passou.
- **A contrabarra no `stream` por `UNLOAD`.** O caso da aspa deu as mesmas linhas pelos três
  caminhos, e o da contrabarra parou o teste: o `UNLOAD` passou sem gravar manifesto nem arquivo, e
  os outros quatro casos não rodaram. A leitura casa com o literal do `UNLOAD` tratando a
  contrabarra como escape, como a documentação mostra ao escapar a aspa do `select` com `\'`: com só
  a aspa dobrada, a contrabarra que o dialeto dobrou chega ao `select` sem par, o literal interno a
  consome, e o filtro não acha linha. O substituto local com essa regra reproduziu a falha com a
  mesma mensagem.
- **Os casos de borda do `UNLOAD`** (`test_unload_limit_empty_result_temp_table_and_super`): o
  `LIMIT` no `select` externo foi recusado com `42601 Limit clause is not supported`; o `UNLOAD` de
  um resultado vazio passou sem gravar manifesto nem objeto no prefixo; a tabela temporária da
  sessão foi lida pelo `UNLOAD` na mesma sessão (2 linhas); e a coluna `SUPER` saiu no Parquet como
  `extension<arrow.json>` no pyarrow, com o texto JSON de cada valor (`{"a":1}`). O `cast` do
  contrato converte essa coluna em `string` (sonda local do mesmo dia).
- **O `row_desc`** (`test_row_description_oids_and_type_modifier`): os OIDs 20, 23, 21, 701, 700,
  1700, 1043, 1042, 1082, 1114, 1184, 16 e 4000 para `BIGINT`, `INTEGER`, `SMALLINT`,
  `DOUBLE PRECISION`, `REAL`, `DECIMAL(18, 2)`, `VARCHAR(40)`, `CHAR(2)`, `DATE`, `TIMESTAMP`,
  `TIMESTAMPTZ`, `BOOLEAN` e `SUPER`; o `type_modifier` 1.179.654 do `DECIMAL(18, 2)`, que a fórmula
  do driver lê como precisão 18 e escala 2, 44 no `VARCHAR(40)`, 6 no `CHAR(2)`, 16.384.000 no
  `SUPER` e -1 nos demais. O `count(*)` saiu `BIGINT`; o `sum` e o `avg` do `DECIMAL(18, 2)`,
  `NUMERIC(38, 2)`; o `sum` do `DOUBLE PRECISION`, OID 701; o `CAST` para `DECIMAL(38, 6)`,
  `NUMERIC(38, 6)`; o literal de texto, OID 1043 com `type_modifier` 11; e o literal `1.5`,
  `NUMERIC(2, 1)`. O `SUPER` chega ao Python como `str`.
- **A carga pequena** (`test_small_load_copy_cost`): o melhor de três de 10 linhas levou 0,91 s e
  0,97 s pelo `COPY` e 0,53 s e 0,55 s pelo `INSERT` de várias linhas.
- **O rodapé do `UNLOAD` com `NaN`** (`test_unload_footer_statistics_with_nan`): com `1.0`, `3.0` e
  `NaN` num grupo de linhas, o `NaN` no início, no meio ou no fim, o rodapé saiu com mínimo 1.0 e
  máximo 3.0, sem o `NaN`, e o `read_parquet` do DuckDB devolveu 0 linha para `valor > 3`: o leitor
  podou o grupo pelo máximo e perdeu a linha do `NaN`, a perda da issue #59. Com `1.0`, `inf` e
  `-inf`, o rodapé saiu com `-inf` e `inf`, e o DuckDB devolveu a linha do infinito. O `UNLOAD`
  grava o rodapé como o pyarrow e o delta-rs.
- **O texto da auditoria** (`test_audit_sql_under_search_path_and_nan_comparison`): numa conexão
  própria, o `SET search_path` no esquema do datashare passou depois do `USE`, e o `CREATE TABLE`
  do `ddl` com o nome sem esquema criou a tabela nele, achada pelo nome em duas partes;
  `current_schema()` continuou nulo. Nas constantes, `'NaN'::float8 = 'NaN'::float8` deu
  verdadeiro, o `is_finite` de então (`x NOT IN ('NaN'::float8, 'Infinity'::float8,
  '-Infinity'::float8)`) deu falso ao `NaN` e aos infinitos, verdadeiro a 1,5 e nulo ao nulo, o
  `CAST` do `NaN` para `NUMERIC(38, 6)` foi recusado (`22P02`) e a soma com um `NaN` deu `nan`. Na
  tabela com os defeitos plantados, a verificação de linhas inteira foi recusada com
  `42883 function is_valid_json(super) does not exist`; medida a medida, `naofinito_valor` contou 1
  dos 2 não finitos e `total_valor` foi recusado com `NaN input (scale float to decimal)`: na
  varredura da tabela o `NaN` passou pelo `NOT IN`, como no IEEE, e chegou ao `CAST`. As demais
  medidas deram o esperado (4 linhas, 1 fora da partição, `total_preco` 16.250000), a chave
  repetida saiu com o id 2 e a amostra com a linha 4. Os textos do modelo cliente, sem coluna JSON,
  rodaram sobre as tabelas vazias.
- **As transações simultâneas** (`test_redshift_transactions.py`: A segura a transação aberta por
  10 s enquanto B roda numa thread, e o `COMMIT` de A solta B). O banco do datashare informou
  isolamento `UNKNOWN`, e `stv_db_isolation_level` foi negada (42501). Escritas em tabelas
  distintas confirmaram as duas, sem espera. Dev e prod gravando linhas distintas da tabela de
  controle confirmaram as duas, e B esperou no `DELETE` da sua linha de controle até o `COMMIT` de A
  (10,9 s e 11,1 s). Duas publicações da mesma tabela e partição: B esperou no `CREATE TABLE` da
  staging de nome fixo (10,3 s e 10,8 s) e foi abortado no `DELETE` da partição com
  `1023 Serializable isolation violation`, e a partição e a linha de controle ficaram as de A. O
  `LOCK` da tabela de controle foi recusado com `0A000 Operation is not supported through
  datashares`. O `UPDATE` da linha de controle condicionado à versão lida, como primeiro comando,
  fez B esperar até o `COMMIT` de A (11,2 s e 11,0 s) e afetar 0 linhas.

**Consequência**: o `unload_text` da suíte dobra a contrabarra além da aspa, como o `stream` da
[etapa 5](PLAN-STAGE-5.md); o manifesto ausente depois de um `UNLOAD` que passou é o resultado
vazio só quando `pg_last_unload_count()` lê 0 na mesma sessão, e o `stream` da suíte registra o
caso vazio em vez de parar. O `is_finite` do Redshift virou a comparação estrita com os infinitos,
falsa ao `NaN` pelas duas regras, e o `json_valid` virou `true` sobre a coluna `SUPER`
([etapa 4](PLAN-STAGE-4.md)). O substituto local imita o escape da contrabarra, o `UNLOAD` vazio,
`pg_last_unload_count()` e a recusa do `is_valid_json` sobre `SUPER`: o código anterior da suíte
reprovou nele a contrabarra com a mensagem do ambiente alvo, e o texto anterior da auditoria teve a
verificação de linhas recusada; a comparação do `NaN` pelo IEEE só o ambiente alvo mostra, e a
suíte ganhou a leitura `nan_na_tabela`. As correções passaram nas execuções seguintes: a
contrabarra às 22:56 e às 23:01, a contagem dos não finitos em 2026-09-24. A transação da
publicação da [etapa 8](PLAN-STAGE-8.md) abre com o `UPDATE`
condicionado (decisão do usuário de 2026-09-23), e o registro dos arquivos do `UNLOAD` da etapa 5
não cumpre a regra da issue #59 numa partição com `NaN`, porque o rodapé é o do Redshift: o usuário
decidiu no mesmo dia exportar essa partição pela troca para `publish_partition`, com aviso no log
([etapa 5](PLAN-STAGE-5.md)).

## O que a validação local do probe das threads mostrou

Em 2026-09-23, no contêiner Linux x86_64 com 4 vCPUs (DuckDB 1.5.5, deltalake 1.6.4, pyarrow
25.0.1), `probes/duckdb_threads.py` rodou sobre a base fictícia de `tests/source_db_projetado.py`
migrada por `scripts/migrate_parquet_to_delta.py` para uma pasta local e copiada para o moto do
substituto, na partição 2026-06-30 de `cad_lancamentos`, `cad_contratos`, `cad_operacoes` e
`rel_contrato_operacao` (60, 41, 30 e 52 linhas).

- **Os valores padrão**: `threads` 4, 8, 12, 16 e 20, o padrão do DuckDB vezes 1 a 5, três
  repetições, vinte configurações em processos novos em 25 s; as linhas lidas bateram com as do log
  em todas (`DT-2`), e o DuckDB aplicou cada valor (`DT-3`). Os tempos, de 0,01 s a 0,06 s, não dizem
  nada da leitura do S3.
- **Só leitura sob a raiz**: a raiz comparada por `diff -r` com uma cópia feita antes ficou igual, e
  nenhuma pasta `serialize_db_*` do motor sobrou na pasta temporária. O mesmo relatório saiu pela
  URI `s3://` do moto, com o secret do DuckDB no endpoint do substituto.
- **A configuração que falha**: com um arquivo apagado da partição de `cad_contratos` numa cópia da
  raiz, as duas medidas de várias tabelas falharam com `IOException`, entraram na tabela com a
  primeira linha do erro, em `DT-4` e na seção final, e o probe saiu com o código 1.

**Consequência**: o probe é o instrumento do item das `threads` em
[`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md) e roda no ambiente alvo depois da migração dos comandos de
`SUITE.md`, sobre o `--root` dela; [`PLAN-STAGE-4.md`](PLAN-STAGE-4.md) o nomeia como a medição do
padrão de `DuckDBConfig.threads`.

## O que a bateria de 2026-09-23 às 22:49 mostrou no ambiente alvo

Em 2026-09-23, entre 22:49 e 23:36 UTC, o usuário rodou todos os comandos de `SUITE.md` no
ambiente alvo, a partir da `main` com o #67, numa máquina de 4 vCPUs e 15.786 MB (Python 3.13.15,
DuckDB 1.5.5 com `threads` 4 e `memory_limit` de 12,3 GiB, deltalake 1.6.4, pyarrow 25.0.1),
com raízes novas sob a pasta pessoal: os cinco probes, a sessão `-m "not redshift"` com as raízes
local e S3, a suíte Redshift duas vezes, a migração de cada tabela para uma raiz Delta nova e o
probe das threads sobre ela ([`readings/`](readings/README.md)).

- **Os probes.** `space.py`, `bucket.py`, `diagnose_aws.py` e `catalog.py` repetiram as leituras
  das 19:18; o `SP-9` leu a `.venv` sem o grupo `emulator`, e o `bucket.py` leu a raiz nova, com
  um marcador de pasta e nenhuma versão não corrente. O `redshift.py` parou a seção da sessão no
  `sys_load_error_detail`: o `count(*)` dos últimos 30 dias voltou sem linha, o acesso à primeira
  linha levantou `IndexError`, e `RS-5`, `RS-8`, `RS-12`, `RS-13`, `RS-16`, `RS-17` e `RS-19`
  ficaram sem leitura nesta execução.
- **A sessão `-m "not redshift"`** (22:53): 445 aprovados, entre eles as duas medições de memória
  de `test_duckdb.py` pelo `VmHWM`: a tabela inteira com 338 MB, 243 MB acima da base, o leitor
  em lotes com 105 MB, 10 MB acima, e o arquivo de transbordo com 118 MB, 23 MB acima. O motor
  DuckDB no ambiente alvo: o primeiro lote do `stream` em 0,014 s com a consulta rodando, 27 de 30
  lotes no arquivo de transbordo com 1,9 MB de pico em memória, o `close` do stream interrompendo
  a consulta em 0,017 s, com `OSError`, o `cleanup` interrompendo a ordenação em curso em 0,021 s,
  e o pipeline de três estágios sobre
  3.000.000 de linhas em 1,533 s por tabela, 2,069 s em série e 1,205 s encadeado. As 20 consultas
  pontuais levaram 5,4 s por `delta_scan`, 2,7 s por `ATTACH ... PIN_SNAPSHOT`, 1,2 s por
  `read_parquet` e 0,018 s na tabela materializada. A limpeza apagou 200 objetos sob a raiz nova,
  e as das duas sessões Redshift 77 e 73: cada um vira uma versão não corrente e um marcador de
  exclusão no bucket versionado, onde `BK-14` não achava nenhuma às 22:50.
- **A suíte Redshift** (22:56 e 23:01): 30 aprovados em cada execução, as leituras iguais salvo
  ids e tempos.
  - O `stream` com literais deu as mesmas linhas pelos três caminhos nos seis casos, a
    contrabarra incluída. O `UNLOAD` vazio passou sem manifesto nem objeto, com
    `pg_last_unload_count()` 0, e o da tabela temporária com as 2 linhas e a contagem 2.
  - O texto da auditoria rodou inteiro, com o `true` do JSON sobre `SUPER`. Na tabela com os
    defeitos plantados, `total_valor` deu 4.500000, sem o `NaN` e o infinito, mas
    `naofinito_valor` contou 1 dos 2: `NOT (valor > '-Infinity'::float8 AND valor <
    'Infinity'::float8)` não contou o `NaN`. `nan_na_tabela` deu 0 e 0: na varredura, nem
    `valor = 'NaN'::float8` nem `valor <> valor` foi verdadeiro para o `NaN`, que numa constante
    era igual a si mesmo.
  - O `ALTER TABLE ... ALTER COLUMN ... TYPE VARCHAR(10)` foi recusado com `0A000 Operation is not
    supported through datashares` na coluna comum e na da chave, as larguras ficaram 5 e 5, e as
    duas inserções de dez caracteres foram recusadas com `22001`.
  - O papel rodou o `EXPLAIN` do join no esquema do datashare: `XN Hash Join DS_DIST_ALL_NONE`
    entre as duas tabelas pequenas.
  - As duas stagings temporárias, cheia dentro da transação e antes do `BEGIN`, confirmaram. Na
    publicação que lê a linha de controle no início e a grava no fim, B leu a versão 1, esperou o
    `COMMIT` de A por 10,1 s e 10,7 s no `DELETE` da partição e recebeu `1023`; a partição e a
    linha de controle ficaram as de A. O `UPDATE` condicionado, as duas publicações da mesma
    tabela, os dois ambientes e o `LOCK` repetiram as leituras das 18:52.
  - Repetiram-se o `COPY` posicional recusado, a lista de colunas e o `FILLRECORD`, o `VARCHAR`
    excedido, o `TRUNCATECOLUMNS` recusado no Parquet, o `34510` depois do `TRUNCATE`, as leituras
    do `SUPER`, o rodapé do `UNLOAD` sem o `NaN` no máximo e o `read_parquet` do DuckDB perdendo a
    linha. Os dois `COPY` em paralelo levaram 4,2 s e 3,3 s, os dois `UNLOAD` 1,4 s, a Data API
    471 ms e 149 ms, e a carga de 10 linhas 0,83 s e 1,04 s pelo `COPY` contra 0,60 s e 0,61 s
    pelo `INSERT`.
- **A migração** (23:05 a 23:19, um processo por tabela na ordem de `SUITE.md`, `register`, com
  a ordem da `sort_key` e a medição): onze das doze tabelas carregaram cada partição com
  contagens, somas e não finitos iguais entre a origem e o Delta, e nenhuma coluna `Double` com
  valor não finito. `cad_lancamentos` rodou entre `cad_contratos`, cujo relatório saiu às 23:06,
  e `cad_operacoes`, que começou às 23:14:58, sozinho na máquina pela sequência de `SUITE.md`, e
  não deixou relatório: o
  probe das threads leu a tabela na versão 2, com as partições 2026-01-31 e 2026-02-28, e o
  processo terminou na 2026-03-31, de 52.654.607 linhas, sem gravar o JSON, morto pelo kernel por
  falta de memória (seção "O que os limites do DuckDB lidos do ambiente mostraram"). A carga no
  processo principal, numa conexão aberta a tabela inteira: `rel_contrato_operacao` levou 2,2 s,
  13,3 s e 11,3 s nas três partições, com o pico do processo em 508 MB, 2.459 MB e 2.712 MB, acima
  do anterior também na terceira, menor que a segunda; `cad_operacoes` de 4,3 s a 5,1 s com o pico
  até 1.801 MB; `cad_contratos` de 3,2 s a 3,6 s até 1.142 MB; as tabelas sem partição de 0,4 s a
  0,6 s, a cerca de 270 MB. As partições medidas das outras três tabelas:

  | Tabela e partição | Linhas | `register` ordenada | `rewrite` ordenada | `register` sem ordem | `rewrite` sem ordem |
  | --- | --- | --- | --- | --- | --- |
  | `cad_contratos` 2026-03-31 | 2.555.232 | 3,6 s, 1.024 MB, 30,6 MB | 5,0 s, 1.414 MB, 32,0 MB | 2,9 s, 1.001 MB, 37,7 MB | 4,0 s, 1.065 MB, 39,3 MB |
  | `cad_operacoes` 2026-03-31 | 4.012.922 | 5,3 s, 1.639 MB, 46,3 MB | 7,4 s, 2.335 MB, 47,9 MB | 4,2 s, 1.519 MB, 62,2 MB | 5,5 s, 1.529 MB, 62,9 MB |
  | `rel_contrato_operacao` 2026-03-31 | 13.637.568 | 13,1 s, 2.442 MB, 201,2 MB | 16,1 s, 2.689 MB, 208,9 MB em 3 arquivos | 8,1 s, 1.849 MB, 243,4 MB | 9,2 s, 1.720 MB, 251,1 MB em 3 arquivos |

  Cada célula dá o tempo, o pico do processo filho e os bytes gravados, sobre uma base de 220 MB a
  226 MB. Nas nove partições, o `rewrite` levou de 1,14 a 1,52 vez o tempo do `register` com a
  ordem e de 1,14 a 1,46 vez sem ela, com o pico até 696 MB maior ordenado; a ordem custou de 1,23
  a 1,63 vez o tempo do `register` e deixou os arquivos com 74% a 92% do tamanho.
- **O probe das threads** (23:21), na partição 2026-02-28, a mais recente comum às quatro tabelas:
  `cad_lancamentos` com 23.789.279 linhas e 393 MB num arquivo, e as quatro juntas com 30.001.596
  linhas. A melhor de três repetições da `materializada` foi de 12,676 s com 4 threads, 13,454 s
  com 8, 13,161 s com 12, 14,857 s com 16 e 18,018 s com 20, com o pico subindo de 1.590 MB a
  3.125 MB; a `agregada`, de 1,162 s a 1,177 s em todos os valores. As várias tabelas levaram
  19,547 s em série e 15,588 s em sessões a mais com 4 threads (1,25x), e mais threads pioraram as
  duas. Nas repetições seguintes à primeira, o DuckDB leu da memória: o cache de arquivos externos
  (`enable_external_file_cache`, ligado por padrão e global à instância) guarda os blocos lidos, e
  no moto a primeira leitura de um arquivo por `delta_scan` fez 3 `GET` dele, a segunda e a
  terceira nenhum, e a leitura com o cache desligado de novo 3 (sonda de 2026-09-24). A leitura do
  S3 é a primeira repetição: a `agregada` levou 4,143 s com 4 threads e 2,013 s, 1,871 s, 1,895 s e
  2,066 s com 8, 12, 16 e 20; a `materializada`, 15,713 s, 14,403 s, 13,973 s, 15,616 s e 18,862 s.

Em 2026-09-24, no contêiner Linux x86_64 com 4 vCPUs e 16.095 MB (DuckDB com `memory_limit` de
10,6 GiB), uma partição sintética de `cad_lancamentos` com as 52.654.607 linhas de 2026-03-31, as
14 colunas do esquema da origem e 37 arquivos SNAPPY de um grupo de linhas cada (937 MB), passou
pela migração local: `register` ordenada em 50,2 s com pico de 11.966 MB, `rewrite` ordenada em
54,8 s com 12.357 MB em 12 arquivos, `register` sem ordem em 14,0 s com 4.517 MB e `rewrite` sem
ordem em 24,2 s com 5.046 MB; a carga ordenada no processo principal levou 35,1 s com RSS máximo
de 12.250 MB e terminou com contagens e somas iguais. Um `COPY` ordenado da mesma partição direto
no DuckDB levou 17,2 s com pico de 10.196 MB sob `memory_limit` de 12,3 GiB e 17,4 s com 7.432 MB
sob 6 GiB.

**Consequência**: a igualdade dos três caminhos do `stream`, o `UNLOAD` vazio, a tabela temporária
e a recusa do `ALTER COLUMN ... TYPE` viram asserções de `test_redshift.py`, e o substituto local
imita a recusa. A [etapa 8](PLAN-STAGE-8.md) fica com a recriação para a largura de `VARCHAR(n)`,
sem o atalho, com a staging temporária pela regra decidida e com o `1023` da publicação que perde a
corrida como `ExecutionConflict`. A contagem dos não finitos da auditoria passa a ser a dos não
nulos menos a dos finitos, sem negar `is_finite` ([etapa 4](PLAN-STAGE-4.md)), e a suíte ganha a
leitura `nan_na_tabela_detalhe`. O `redshift.py` lê a contagem sem linha como leitura. O probe das
threads desliga o cache de arquivos externos em cada configuração, e o script de migração regrava o
relatório depois da medição de cada tabela e de cada partição gravada. A migração de
`cad_lancamentos` no ambiente alvo, com os limites lidos do ambiente e a medição com e sem a ordem
([etapa 7](PLAN-STAGE-7.md)), e as `threads` pedem uma nova execução
([`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md)).

## O que os limites do DuckDB lidos do ambiente mostraram

Em 2026-09-24, depois da bateria de 2026-09-23:

- **A migração de `cad_lancamentos` morreu por falta de memória.** O usuário viu `Killed` no
  terminal algumas vezes, na parte de `cad_lancamentos` da migração de 2026-09-23 às 23:05: o kernel
  matou o processo, numa máquina de 15.786 MB com o `memory_limit` padrão do DuckDB, 12,3 GiB. É o
  segundo tipo de falta de memória do guia do DuckDB 1.4, o processo morto pelo sistema, para o
  qual a documentação pede o limite em 50% a 60% da memória, porque parte das operações foge do
  gerenciador de buffers; o primeiro tipo é a `OutOfMemoryException` do próprio DuckDB
  (`failed to pin block of size ...`). A documentação pede também de 1 a 4 GB por thread, com o
  mínimo de 125 MB.
- **O DuckDB 1.5.5 lê o cgroup v1.** Num contêiner Linux de 4 vCPUs com `MemTotal` de
  16.481.980 kB e limite de cgroup v1 de 14.345.912.320 bytes na pasta do processo, o padrão foi
  `memory_limit` de 10,6 GiB, 80% do limite, e 4 threads. A leitura do cgroup, v1 e v2, com a
  cota de CPU arredondada para cima, entrou na versão 1.3 (duckdb/duckdb#16608); a 1.1.3 lia a
  memória da máquina hospedeira no contêiner (duckdb/duckdb#15080).
- **A leitura do pacote no mesmo contêiner**: `available_cpus()` deu 4 (sem cota de CPU),
  `available_memory()` deu 14.197.641.216 bytes, a folga do cgroup, abaixo do `MemAvailable` de
  16.083.521.536, e `environment_limits()` deu `threads` 4 e `memory_limit` de 6.761 MiB, que o
  DuckDB mostra como 6,6 GiB. O `/proc/self/cgroup` do contêiner tem o `memory` do v1 numa pasta
  própria e a linha `0::/` do v2 sem controlador.
- **O DuckDB só devolve a memória ao fechar a conexão.** Num processo que partiu de 191 MB, um
  `CREATE TABLE ... AS SELECT ... ORDER BY` de 20.000.000 de linhas de um Parquet local deixou o RSS
  em 1.188 MB; depois do `DROP TABLE`, 1.188 MB; depois do `close`, 208 MB. Na leitura desse
  arquivo local, o cache de arquivos externos guardou 1.042 entradas e 271.906 bytes.
- **A migração da base fictícia com os limites novos**: as 12 tabelas terminaram com contagens e
  somas iguais em 70 s, cada conexão da carga com `memory_limit` de 6,6 GiB e 4 threads e cada
  variante da medição, num processo filho, com 6,5 GiB.
- **O probe das threads com a rodada da metade**: sobre as tabelas dessa migração, com uma
  repetição, as 24 configurações (2, 4, 8, 12, 16 e 20 threads nos quatro cenários) leram as linhas
  do log e aplicaram as threads pedidas; a razão compara com as 4 threads do padrão do motor. O
  contêiner tem uma thread por núcleo físico (`thread_siblings_list` da CPU 0 e o `lscpu`).
- **As vCPUs da AWS**: nas instâncias com SMT, cada vCPU é uma thread de um núcleo físico, e o
  `m5.xlarge` tem 2 núcleos e 4 vCPUs; o T2, o C7a, o M7a, o R7a, os Mac e os Graviton não mudam as
  threads por núcleo (documentação do EC2, "CPU options").

**Consequência**: o motor DuckDB e o script de migração abrem cada conexão com os limites lidos do
ambiente (`environment_limits`, sobre `serialize_db.resources`): `threads` nas CPUs que o processo
pode usar e `memory_limit` na metade da memória que ele ainda pode usar, no lugar do padrão do
DuckDB (instrução do usuário de 2026-09-24, que troca a decisão de 2026-09-22). A carga de cada
tabela do script tem a sua conexão, fechada no fim, e o relatório leva os limites de cada carga e
de cada variante. O probe das threads mede também a metade das CPUs. As revisões estão em
[`PLAN-STAGE-4.md`](PLAN-STAGE-4.md), [`PLAN-STAGE-7.md`](PLAN-STAGE-7.md), [`PLAN.md`](PLAN.md) e
[`duckdb.md`](duckdb.md); a execução de `cad_lancamentos` de 2026-09-24 confirmou a metade, com o
pico da carga 20% acima do limite (seção "O que a bateria de 2026-09-24 às 01:41 mostrou no
ambiente alvo").

## O que o secret do DuckDB mostrou

Em 2026-09-24, na releitura da bateria de 2026-09-23: `space.py` e `RS-18` leram às 22:50 a
credencial de quem chama expirando 46 minutos depois, e a carga de uma tabela do script de migração
vive numa conexão só, do fim da medição ao relatório da carga. Uma sonda no contêiner Linux
x86_64 (DuckDB 1.5.5, `httpfs` e `aws` de `.duckdb/`, uma chave de mentira nas variáveis `AWS_*`):

- **O secret `credential_chain` guarda a credencial resolvida no `CREATE SECRET`.**
  `duckdb_secrets()` mostra `key_id`, `secret` e `session_token` dentro do secret; nada no secret
  criado por `storage.duckdb_setup` e pelo script pede a renovação quando ela expira.
- **`REFRESH auto` é aceito.** `CREATE SECRET (TYPE s3, PROVIDER credential_chain, REGION ...,
  REFRESH auto)` passou, com e sem `CHAIN 'env'`, e o secret ganhou
  `refresh_info={'refresh': auto, 'region': ...}`. A documentação da extensão `aws` diz que alguns
  endpoints exigem a renovação periódica da credencial, que `REFRESH auto` a pede, e que
  `CHAIN 'sts'` e `'web_identity'` a ligam sozinhos, por serem credenciais curtas; quando a
  renovação acontece, a página não diz, e uma conexão atravessando a expiração não foi medida.

**Consequência**: os dois secrets levam `REFRESH auto` (decisão do usuário de 2026-09-24,
[etapa 3](PLAN-STAGE-3.md)); a renovação numa conexão que atravessa a rotação da credencial do
contêiner só uma execução longa no alvo mostra ([`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md)).

## O que o arquivo do `COPY` do DuckDB mostrou

Em 2026-09-24, no contêiner Linux x86_64 (DuckDB 1.5.5, deltalake 1.6.4, pyarrow 25.0.1), duas
sondas sobre o arquivo do registro, o que a publicação da [etapa 8](PLAN-STAGE-8.md) carrega no
Redshift desde a decisão de 2026-09-24:

- **Os tipos físicos.** Um `COPY ... (FORMAT parquet, RETURN_STATS)` de uma coluna de cada tipo do
  contrato gravou formato 1.0, SNAPPY e codificação `PLAIN` em toda coluna: `BIGINT` em `INT64`,
  `DECIMAL(18, 2)` em `INT64` e `DECIMAL(38, 6)` em `FIXED_LEN_BYTE_ARRAY`, `DOUBLE` em `DOUBLE`,
  `DATE` em `INT32`, `TIMESTAMP` em `INT64` de microssegundos, `VARCHAR` em `BYTE_ARRAY` com o tipo
  lógico `String`, `BOOLEAN` em `BOOLEAN` e `JSON` em `BYTE_ARRAY` com o tipo lógico `JSON`, que o
  pyarrow lê como `extension<arrow.json>`.
- **O campo JSON pelo registro.** Numa tabela com uma coluna `sa.JSON`, `string` no esquema Delta,
  `export_partition` do motor DuckDB gravou a coluna com o tipo lógico `JSON` (`CAST` para o `JSON`
  do DuckDB), `register_files` aceitou o arquivo, porque confere o tipo físico, e os dois leitores
  leram o texto de cada documento: `delta_scan` como `VARCHAR` e o delta-rs como `string`.

**Consequência**: o `DECIMAL(18, 2)` e o `TIMESTAMP` do registro têm os tipos físicos que o
`COPY ... MANIFEST` do Redshift carregou de arquivos do delta-rs em 2026-09-21, e o `COPY` sobre um
arquivo do DuckDB, com a coluna JSON com o tipo lógico numa staging `VARCHAR(65535)`, espera os
testes `redshift` da etapa 8 ([`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md)). O item do arquivo do
`UNLOAD` com coluna `SUPER` numa tabela Delta fica só com o registro do arquivo do `UNLOAD`: um
arquivo com o mesmo tipo lógico, gravado pelo DuckDB, os dois leitores leram como texto.

## O que a bateria de 2026-09-24 às 01:41 mostrou no ambiente alvo

Em 2026-09-24, entre 01:41 e 02:19 UTC, o usuário rodou de novo os comandos de `SUITE.md` no
ambiente alvo, a partir da `main` com o #69, numa máquina de 16 vCPUs, duas por núcleo físico, e
31.159 MB (Python 3.13.15, DuckDB 1.5.5 com o padrão de 16 threads e `memory_limit` de 24,3 GiB,
deltalake 1.6.4, pyarrow 25.0.1), nas mesmas raízes sob a pasta pessoal; `environment_limits` deu
16 threads e de 13,1 GiB a 13,6 GiB, a metade dos 27 GB a 28 GB disponíveis a cada abertura
([`readings/`](readings/README.md)).

- **Os probes.** `space.py`, `diagnose_aws.py` e `catalog.py` repetiram as leituras, com a máquina
  nova. `redshift.py` leu a seção da sessão inteira: `sys_load_error_detail` respondeu 25 erros de
  carga em 30 dias em 1,5 s (`RS-12`), nenhum esquema externo (`RS-13`), `svv_all_tables` com duas
  tabelas no esquema, nenhuma com o prefixo da biblioteca (`RS-8`), `svv_table_info` negada depois
  do `USE` (42501), `has_schema_privilege` falso para `USAGE` e `CREATE`, `RS-19` pelo `USE`, e a
  credencial de quem chama expirando em 49 minutos (`RS-18`); nenhuma chamada falhou. `bucket.py`
  achou sob a raiz nova 366 versões não correntes (16.345.479 bytes) e 358 marcadores de exclusão,
  o rastro das três sessões de 2026-09-23 (`BK-14`).
- **A sessão `-m "not redshift"`** (01:43): 454 aprovados. As medições de memória repetiram a
  leitura (a tabela inteira 244 MB acima da base, o leitor em lotes 11 MB, o arquivo de transbordo
  24 MB). O cache de arquivos externos, ligado por padrão, guardou 5 entradas e 3.458.366 bytes
  depois da primeira leitura e as mesmas depois da segunda. Com 16 threads, quatro sessões juntas
  levaram 0,094 s contra 0,025 s de uma (3,77x), e com `threads = 1` as quatro levaram o mesmo que
  uma (0,201 s contra 0,197 s), porque a thread que chama cada sessão executa a consulta dela. O
  pipeline de três estágios sobre 3.000.000 de linhas levou 1,276 s por tabela, 1,710 s em série e
  0,949 s encadeado; a leitura antecipada do Arrow puxou 19 lotes antes da falha do `INSERT`, e
  25 meio segundo depois, contra 7 e 12 com 4 vCPUs. As 20 consultas pontuais levaram 4,9 s por
  `delta_scan`, 2,5 s por `ATTACH ... PIN_SNAPSHOT`, 1,1 s por `read_parquet` e 0,014 s na tabela
  materializada; a limpeza apagou 200 objetos.
- **A suíte Redshift** (01:46 e 01:49): 30 aprovados em cada execução, as leituras iguais salvo
  ids e tempos. `naofinito_valor` contou 2 dos 2 pela contagem dos não nulos menos os finitos, e
  cada medida bateu com o esperado; na linha do `NaN`, `valor > '-Infinity'::float8`,
  `valor < 'Infinity'::float8`, a negação da primeira e o `is null` dela deram falso, e o texto do
  valor `NaN`: na varredura o Redshift compara o `NaN` como o IEEE, falso em toda comparação, e a
  negação também falsa casa com a negação reescrita como `<=`, sem sonda que a confirme. As
  limpezas apagaram 74 e 75 objetos.
- **A migração** (01:53 a 02:02, um processo por tabela, `register` ordenado com a medição das
  quatro variantes da época): as doze tabelas carregaram cada partição com contagens, somas e não
  finitos iguais entre a origem e o Delta, e nenhuma coluna `Double` com valor não finito.
  `cad_lancamentos` (01:55 a 01:59) entrou com `memory_limit` de 13,4 GiB e 16 threads: 2026-01-31
  (33.239.719 linhas) em 6,4 s, 2026-02-28 (23.789.279) em 5,4 s, 2026-03-31 (52.654.607) em 10,1 s
  e 2026-06-30 (32.218.190) em 6,4 s, com o pico do processo em 9.678 MB depois das duas primeiras
  e 16.430 MB depois da terceira, 20% acima do limite e 53% da memória da máquina. As partições
  medidas de `cad_lancamentos`, cada variante num processo novo com o limite lido na abertura:

  | Partição | Linhas | `register` ordenada | `rewrite` ordenada | `register` sem ordem | `rewrite` sem ordem |
  | --- | --- | --- | --- | --- | --- |
  | 2026-01-31 | 33.239.719 | 7,0 s, 10.190 MB, 551,1 MB | 15,0 s, 11.430 MB, 568,7 MB em 6 arquivos | 8,8 s, 8.650 MB, 551,3 MB | 13,9 s, 4.981 MB, 568,9 MB em 6 arquivos |
  | 2026-02-28 | 23.789.279 | 5,4 s, 7.413 MB, 392,9 MB | 11,1 s, 8.873 MB, 405,9 MB em 5 arquivos | 6,7 s, 6.539 MB, 393,0 MB | 10,5 s, 4.322 MB, 406,1 MB em 5 arquivos |
  | 2026-03-31 | 52.654.607 | 10,6 s, 15.126 MB, 903,7 MB | 22,4 s, 16.367 MB, 935,3 MB em 10 arquivos | 12,5 s, 7.540 MB, 905,7 MB | 20,2 s, 6.331 MB, 936,6 MB em 10 arquivos |
  | 2026-06-30 | 32.218.190 | 6,6 s, 9.782 MB, 542,2 MB | 14,5 s, 11.048 MB, 561,2 MB em 6 arquivos | 8,9 s, 8.214 MB, 545,2 MB | 13,5 s, 5.258 MB, 564,1 MB em 6 arquivos |

  Cada célula dá o tempo, o pico do processo filho e os bytes gravados, sobre uma base de 226 MB a
  227 MB. Com 16 threads, a ordem pela `sort_key` foi mais rápida que a sua falta nas quatro
  partições de `cad_lancamentos` (de 0,79 a 0,85 do tempo), com arquivos do mesmo tamanho, e mais
  lenta nas outras três tabelas particionadas (de 1,1 a 1,4 vez, com arquivos de 74% a 92%); a
  causa não foi medida. O `rewrite` levou de 1,4 a 2,1 vezes o tempo do `register`, com o pico
  maior ordenado e menor sem ordem. Os picos com 16 threads passaram dos de 4 vCPUs:
  `rel_contrato_operacao` 2026-03-31 em 3.121 MB contra 2.442 MB no `register` ordenado, e a carga
  da tabela em 3.486 MB contra 2.712 MB, em 4,7 s contra 13,3 s; `cad_contratos` e `cad_operacoes`
  carregaram em 2,1 s a 2,9 s por partição.
- **O probe das threads** (02:02 a 02:19), com o cache de arquivos externos desligado, na partição
  2026-06-30 (`cad_lancamentos` com 32.218.190 linhas e 542 MB num arquivo, as quatro tabelas com
  51.238.647 linhas), 8, 16, 32, 48, 64 e 80 threads, a razão sobre as 16 do padrão do motor. A
  `materializada` levou 7,847 s com 8 threads, 7,008 s com 16, 11,133 s com 32, 14,214 s com 48,
  17,066 s com 64 e 15,794 s com 80, com o pico do processo de 1.880 MB a 6.427 MB; a `agregada`,
  a leitura do S3, 2,033 s com 8, 1,194 s com 16, 0,877 s com 32, 0,843 s com 48, 0,874 s com 64
  e 0,892 s com 80. As quatro tabelas levaram 17,516 s em série e 9,292 s em sessões a mais com 16
  threads (1,89x), e mais threads pioraram as duas (27,2 s a 35,7 s em série; 9,9 s a 12,6 s em
  sessões a mais).

**Consequência**: `DuckDBConfig.threads` fica nas CPUs que o processo pode usar, o padrão de
`environment_limits`: a ingestão materializa, e a metade das CPUs e o dobro delas perdem nela; a
leitura agregada do S3 ganha 1,4 vez com o triplo, para quem a pedir na configuração
([etapa 4](PLAN-STAGE-4.md)). A metade da memória disponível no `memory_limit` fica: o `COPY`
ordenado da maior partição passou do limite em 11% no processo filho e a carga em 20%, dentro da
máquina. A carga ordenada da maior partição de `cad_lancamentos` pede uma máquina de 32 GB, e a
sem ordem cabe em 16 GB ([etapa 7](PLAN-STAGE-7.md), [`PLAN.md`](PLAN.md)). A raiz Delta de
2026-09-24 tem as doze tabelas pela regra da issue #59, sem valor não finito, e substitui a
primeira migração. A contagem dos não finitos e a linha do `NaN` viram asserções de
`test_redshift.py`, a linha só no ambiente alvo, porque o DuckDB do substituto lê `NaN > -inf`
como verdadeiro. Os itens da migração de `cad_lancamentos`, da metade da memória, das `threads`,
do `Double` não finito e do texto da auditoria saem de [`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md);
os relatórios estão em [`readings/`](readings/README.md), e os da migração ficam fora do git.

## O que a implementação das etapas 5 e 8 mostrou

As duas etapas foram implementadas em 2026-09-24 sobre o substituto local de `tests/emulator.py`
(o moto e um DuckDB em memória), com os casos sem conexão sobre uma conexão de mentira; nenhum
comando rodou no ambiente alvo ([`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md)). O que a
implementação leu das bibliotecas:

- **`register_files` sem arquivo falha no delta-rs**: `create_write_transaction` com a lista de
  ações vazia, em `overwrite` de uma partição, levanta `IndexError` (`deltalake` 1.6.4). O
  `UNLOAD` de um resultado vazio não grava arquivo (leitura de 2026-09-23), então o motor
  Redshift grava um Parquet sem linha com o esquema do contrato sem a coluna de partição e o
  registra; a partição vazia entra no log como no motor DuckDB, cujo `COPY` grava o arquivo vazio.
- **O PyArrow 25.0.1 não expõe a exatidão do mínimo e do máximo do rodapé**: `Statistics` não tem
  `is_min_value_exact` nem `is_max_value_exact`, que o formato Parquet guarda desde 2.10 para o
  texto truncado. `file_from_footer` leva ao log o mínimo e o máximo das colunas inteiras,
  `Double` e de data, e o `null_count` de todas; o texto fica sem os dois, e o leitor não poda por
  ele. A coluna `pa.json_(pa.string())`, como o PyArrow lê o tipo lógico `JSON`, converte para
  `string` por `RecordBatch.cast` sem passar pelo `storage`.
- **`Table.to_metadata(metadata, name=...)` do SQLAlchemy copia `info` e o comentário**: o motor
  cria a staging `_publicado` pelo `ddl` do modelo com o nome trocado, com a `SORTKEY` e a chave
  do contrato.
- **O DuckDB cria a tabela sem esquema no primeiro esquema de `search_path`**, e uma tabela
  temporária de mesmo nome vence a permanente: o substituto responde ao `ddl` sem esquema como o
  Redshift respondeu no ambiente alvo em 2026-09-23. O conflito entre duas transações do DuckDB
  (`TransactionContext Error: Conflict on tuple deletion!`, na segunda a apagar a mesma linha
  depois do `COMMIT` da primeira) faz as vezes do `1023` do Redshift no substituto, que o mapeia
  para essa mensagem, e a relação inexistente e a que já existe saem com `42P01` e `42P07`.
- **O substituto lista `svv_all_columns` pelo DDL lembrado**, na grafia do Redshift (`character
  varying`, `numeric`, `timestamp without time zone`, `double precision`, `super`), com a largura,
  a precisão e a escala, porque o `information_schema` do DuckDB não guarda o `n` do `VARCHAR`. A
  grafia do ambiente alvo é a leitura de `test_reconcile_published_on_the_target`.
- **O teste da publicação simultânea** pausa toda conexão da segunda publicação depois da leitura
  da linha de controle, pela porta `driver_connect` do motor, até a primeira confirmar: no
  substituto a segunda recebe o conflito no `DELETE` da partição, como no ambiente alvo em
  2026-09-23, e `publish_redshift` o devolve como `ExecutionConflict`.
- **As contagens**: `tests/test_engine_redshift.py` tem 21 casos (15 sem conexão, 6 `redshift`)
  e `tests/test_publication.py` 13 (5 sem conexão, 8 `redshift`); os 14 casos `redshift` passaram
  no substituto local em 2026-09-24, com a publicação simultânea, a primeira publicação sobre os
  arquivos exportados pelo motor DuckDB e o ciclo `ingest`, `stream`, `loader`, auditoria e
  exportação pelo registro e pela troca.

## O que a primeira bateria das etapas 5 e 8 mostrou no ambiente alvo

O usuário rodou em 2026-09-24, a partir da `main` com o #71, os quatro comandos que o relatório
da implementação listou: `tests/test_engine_redshift.py` com `-m redshift` às 05:10 e às 05:12 UTC
e `tests/test_publication.py` com `-m redshift` às 05:13, duas vezes cada, com
`SERIALIZE_DB_TEST_REPORT` (Linux 6.12 do Amazon Linux 2023, Python 3.13.15, deltalake 1.6.4,
duckdb 1.5.5, pyarrow 25.0.1, boto3 1.43.98, sqlalchemy 2.0.54, pandas 3.0.6, pytest 9.1.1,
`sa-east-1`, sem proxy). Os achados estão aqui e os relatórios ficam fora de `plan/readings/`.

- **Cinco dos seis casos do motor passaram nas duas rodadas** (116,5 s e 80,6 s), a primeira vez
  que um comando do motor rodou lá: `test_ingest_stream_loader_export` (o `ingest` de uma partição
  por `COPY ... MANIFEST FILLRECORD` de um Delta gravado pelo delta-rs no bucket; o `stream` por
  `UNLOAD` em lotes de 50, 50 e 20 igual ao `query`, com a coluna JSON, o `DECIMAL(18, 2)` e o
  `timestamp[us]`; o `loader` por `COPY` com o nome ocupado recusado; a auditoria com a staging
  `_publicado` e a chave estrangeira pela versão fixada de `cad_contas`; o `export_partition` pelo
  registro do arquivo do `UNLOAD`, lido pelo delta-rs com a coluna JSON como texto e contado pelo
  `delta_scan`; a troca por `publish_partition` na partição com `NaN`, com o `max.valor` ausente
  nela e 60.0 na outra; o `cleanup` sem tabela `exec_<id>_*` e com o `staging/` vazio);
  `test_loader_creates_the_table_at_close` (a relação inexistente antes do `close`, o `COPY` de um
  arquivo ausente desfazendo o `CREATE TABLE`, o `loader` sem lote criando a tabela vazia);
  `test_new_session_sees_committed_tables` (duas ingestões em duas sessões, em threads, e a tabela
  temporária invisível à outra sessão); `test_stream_literal_values_on_the_target` (os literais
  com `'`, `\` e `%`, o stream vazio com o esquema, a tabela temporária lida pelo `UNLOAD`
  seguinte); e `test_small_load_copy_cost` (1,56 s e 1,46 s o melhor de três `load` de 10 linhas
  pelo `loader`). O `row_desc` dos agregados: `count(*)` em `int64`, `sum` de `DECIMAL(18, 2)` em
  `decimal128(38, 2)`, `sum` de `DOUBLE PRECISION` em `double`, o literal de texto em `string` e
  `1.5` em `decimal128(2, 1)`, como a suíte de estudo leu em 2026-09-23. A limpeza apagou 39
  objetos por sessão.
- **`select current_database()` reprovou `test_connect_uses_share_database` nas duas rodadas**: o
  Redshift descreve a coluna com o tipo `name` (OID 19, `type_size` 128), o tipo dos
  identificadores do catálogo, que o mapa de `schema_from_row_description` não tinha, e `query`
  recusou com `SandboxError: current_database: tipo NAME (OID 19) fora do contrato`. As asserções
  anteriores do caso passaram: o `CREATE TABLE` por nome em duas partes depois do `USE` e
  `name_in_use` com o nome livre, sem dizer se pelo SQLSTATE `42P01` ou pela mensagem.
  Consequências: `NAME` entra no mapa como `string`; o substituto descreve `current_database()`
  com o OID 19, e o caso reprova nele com o motor anterior e passa com o corrigido; o caso
  registra o SQLSTATE e a mensagem da relação inexistente, `current_database()` e a presença da
  tabela de controle, e `test_ingest_stream_loader_export` registra a coluna JSON do arquivo do
  `UNLOAD` lida pelo `delta_scan`. O valor de `current_database()` é leitura e nunca asserção:
  ele continua `dev` depois do `USE`, que muda a resolução dos nomes e não o banco da sessão
  (leitura de 2026-09-21, a regra "A state change is confirmed by the effect the caller depends
  on" de `CLAUDE.md`), e o usuário lembrou isso em 2026-09-24.
- **Nenhum caso da publicação rodou**: os oito erraram na fixture `local_location`,
  "SERIALIZE_DB_TEST_LOCAL_ROOT aponta para uma pasta inexistente:
  /home/<usuário>/serialize-db-local", em 1,2 s e 1,1 s, porque `SUITE.md` exporta a variável e a
  pasta do `mkdir` dele não existia na máquina. Os casos usam a pasta local pelo `temp_directory`
  do motor DuckDB que exporta os arquivos que a publicação lê, mas levavam só os marcadores
  `redshift` e `s3`: sem a variável, `pytest -m redshift` os erraria no lugar de pulá-los. Eles
  levam o marcador `local` agora, e `tests/conftest.py` recusa na coleta um teste que usa a
  fixture de uma suíte sem o marcador dela (`SUITE_FIXTURES`). O `COPY` do Redshift sobre o
  arquivo do `COPY` do DuckDB, a grafia de `svv_all_columns`, o `1023` pela biblioteca e o
  `EXPLAIN` da junção esperam a repetição com a pasta criada
  ([`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md)).

## O que a implementação da etapa 7 mostrou

`serialize_db.load`, `serialize-db load` e o script fino foram implementados em 2026-09-24 sobre a
base fictícia de `tests/source_db_projetado.py`, na pasta local; os 20 casos de `tests/test_load.py`
e os 3 de `tests/test_migrate_parquet_to_delta.py` passam. O que a implementação leu:

- **`get_add_actions` do delta-rs 1.6.4 devolve uma tabela `arro3`**, sem `to_pylist`: o script
  a converte por `pa.table`, como `delta.partition_values` já fazia. O `fetch_arrow_table()` de uma
  conexão do DuckDB 1.5.5 avisa que está obsoleto em favor de `to_arrow_table()`.
- **`delta_scan` sobre a pasta de uma tabela que não existe é `IOException`**
  (`InvalidTableLocationError`): `load_report` só agrega o lado do Delta quando `table_exists`, e a
  tabela ainda fora do Delta sai com `None` nas linhas dele em toda partição.
- **Uma chamada de `initial_load` por partição** custa a abertura de um motor DuckDB por partição
  (o banco e o transbordo numa pasta nova, fechados no fim) e uma leitura do log; sobre a base
  fictícia inteira, 12 tabelas e 24 partições, a linha de comando do script leva cerca de 8 s, e a
  de `serialize-db load`, com uma chamada por tabela, cerca de 5 s.
- **Os casos do script que cobriam a carga foram para `tests/test_load.py`**: a descoberta, a
  consulta, a carga uma vez só com a retomada e o filtro, os tipos, a ordem da `sort_key`, as
  recusas sem commit, o `Double` não finito e o relatório; os da medição saíram com ela, e o da
  conexão por tabela com os limites do ambiente é do motor (`tests/test_engine_duckdb.py`), que a
  carga abre por chamada.
- **A tabela sem partição sob `partitions` criava a tabela Delta vazia** (2026-10-01):
  `initial_load` com `partitions` numa tabela sem partição criava a tabela na versão 0 e abria um
  motor sem nada a gravar, e `load_report` abria outro para um relatório sem partição;
  `serialize-db load --tables cad_aliquotas --partitions 9999-12-31` criava
  `prd/cad_aliquotas/_delta_log` e imprimia `0 partição(ões) conferida(s)`. Desde a decisão do
  usuário de 2026-10-02, a tabela fica inteira de fora, sem criação, motor nem relatório.
- **Três erros de chamada que passavam calados** (2026-10-01): `run.audit(tabela, [])` aprovava
  sem auditar e `run.publish_delta(tabela, partitions=[])` reconciliava a tabela sem exportar
  partição, porque o `IN ()` da lista vazia é falso e toda contagem dá zero; `run.snapshot`
  chamado duas vezes na mesma execução gravava só o último nome, e os commits entre as chamadas
  levavam o primeiro; o `ingest` do motor Redshift carregava duas vezes a partição repetida em
  `partitions`, um `COPY` por valor da lista, e o do DuckDB não. Desde a decisão do usuário de
  2026-10-02, a lista vazia e a segunda chamada de `run.snapshot` são `ContractError`, e a
  partição repetida entra uma vez no Redshift (`test_the_empty_list_of_partitions_is_refused`,
  `test_snapshot_is_marked_once_per_execution` e `test_ingest_counts_a_repeated_partition_once`
  reprovam no código anterior).

## O que a implementação da etapa 9 mostrou

Os subcomandos `snapshot`, `vacuum`, `compact`, `archive`, `export` e `history`, `history` e
`archive_snapshot` em `serialize_db.delta`, o `deep_copy` pela cópia e o registro e o runbook
`docs/operacao.md` foram implementados em 2026-09-24 na pasta local; os 6 casos de
`tests/test_operation.py` e os 70 de `tests/test_delta.py` passam. O que a implementação leu:

- **`get_add_actions(flatten=False)` do delta-rs 1.6.4** traz `path`, `size_bytes`,
  `modification_time`, `num_records` e os structs `null_count`, `min`, `max` (com os valores
  tipados: `Decimal`, `date`, `datetime`) e `partition` (só numa tabela particionada), a sonda de
  2026-09-24; `deep_copy` monta o `RegisteredFile` de cada ação a partir deles e registra as
  estatísticas dos tipos exatos, como `register_files`.
- **`DeltaTable.history()`** devolve os commits do mais recente ao mais antigo, com `version`,
  `operation`, `timestamp` (milissegundos da época) e os metadados de `CommitProperties` como
  chaves de primeiro nível: `history` os lê por nome e converte o instante para UTC.
- **A cópia por registro termina numa versão por partição**, e não na 0 do `write_deltalake`:
  `test_deep_copy_and_relocation` lê a versão 1 na cópia de uma versão com uma partição, com o
  mesmo caminho, tamanho e extremos da origem.
- **A recusa da compactação alcança a tabela sem partição**: o snapshot registra a versão atual de
  toda tabela existente, então `compact --table cad_contas` logo depois de um snapshot é recusado
  como o de uma tabela particionada; o caso do teste publicou uma versão nova antes.

## O que a bateria de 2026-09-24 às 12:38 mostrou no ambiente alvo

Em 2026-09-24, entre 12:38 e 14:25 UTC, o usuário rodou os comandos de `SUITE.md` no ambiente
alvo, a partir da `main` com o #72, numa máquina de 16 vCPUs e 31.383 MB, com 28.061 MB
disponíveis (Python 3.13.15, DuckDB 1.5.5, deltalake 1.6.4, pyarrow 25.0.1, boto3 1.43.98,
sqlalchemy 2.0.54, pandas 3.0.6, pytest 9.1.1, `sa-east-1`, sem proxy), com a pasta local criada,
e em seguida a carga pelo pacote, a auditoria e as rotinas da operação sobre a cópia da base de
produção. Os relatórios das suítes do motor e da publicação estão em
[`readings/`](readings/README.md); os dos probes e o da carga ficam fora de `plan/`, com os achados
aqui. O probe das threads e a publicação Delta para Redshift não rodaram.

- **Os probes** repetiram as leituras de 2026-09-23 e de 01:41: `space.py` com 16 vCPUs, a `.venv`
  completa (`SP-9`) e sem internet (`SP-7`); `diagnose_aws.py` com os três clientes listando o
  prefixo; `redshift.py` sem chamada falhada, com a credencial de quem chama expirando em 36
  minutos (`RS-18`) e a temporária do workgroup em uma hora (`RS-15`); `bucket.py` com 907 versões
  não correntes (32.966.477 bytes) e 859 marcadores de exclusão sob a raiz dos testes (`BK-14`,
  contra 366 e 358 às 01:42), as leituras negadas de sempre e o IAM e o KMS sem conexão TCP;
  `catalog.py` com o Glue de um banco e uma tabela, os três workgroups do Athena com `GetWorkGroup`
  negado em `primary`, e o Lake Formation e o S3 Tables sem rota (`ConnectTimeoutError` em 60,3 s
  e 30,4 s), que resolvem para endereços públicos. O código de saída 1 de `catalog.py`, `bucket.py`
  e `space.py` conta as chamadas que falharam, todas leituras esperadas; nenhuma checagem reprovou.
- **A sessão `-m "not redshift"`** (12:41, 182,8 s): 477 aprovados de 521 coletados, os 44
  `redshift` de fora; as leituras de memória, do cache de arquivos externos, do stream e do
  pipeline como às 01:43 (`engine.stream.first_batch` 0,010 s com a consulta rodando, 27 de 30
  lotes no arquivo, o pipeline de três estágios em 1,222 s por tabela, 1,611 s em série e 0,929 s
  encadeado).
- **A sessão `-m redshift`** (12:44 e 12:53, 549,8 s e 514,6 s): 44 aprovados nas duas rodadas,
  os 30 das suítes de estudo como às 01:46, mais os 6 do motor e os 8 da publicação, que rodaram
  junto pela primeira vez; as leituras das suítes de estudo repetiram, o `1023` da segunda
  transação inclusive.
- **A suíte do motor Redshift** (13:01 e 13:03, 95,5 s e 103,5 s): 6 aprovados nas duas rodadas,
  `current_database()` incluído, pelo tipo `name` no mapa. A relação inexistente responde o
  SQLSTATE `XX000` com a mensagem `Relation <nome> does not exist in the database.`, não o `42P01`
  do PostgreSQL, e `relation_missing` a reconheceu pela mensagem (`redshift.engine.relation_missing`);
  `current_database()` continua `dev` depois do `USE`; a tabela de controle não existia no esquema
  no início da sessão; a coluna JSON do arquivo do `UNLOAD` registrado por `export_partition` sai
  do `delta_scan` como `VARCHAR` com o texto `{"k":121}` (`redshift.engine.delta_scan_meta`), o
  mesmo texto que o delta-rs lê; o `load` de 10 linhas levou 2,06 s, 1,77 s, 1,92 s e 1,66 s nas
  quatro sessões, o melhor de três em cada uma.
- **A suíte da publicação** (13:05 e 13:08, 180,8 s e 175,2 s): 8 aprovados nas duas rodadas, a
  primeira vez que a publicação da biblioteca rodou no Redshift.
  `test_first_publication_loads_every_partition` publicou 80 linhas de 2 partições exportadas pelo
  motor DuckDB (`redshift.publication.first`): o `COPY ... MANIFEST` do Redshift carregou os
  arquivos do `COPY` do DuckDB, com o `DECIMAL(18, 2)` em `INT64`, o `TIMESTAMP` em `INT64` de
  microssegundos, o `DATE` em `INT32` e o campo JSON com o tipo lógico `JSON` numa staging
  `VARCHAR(65535)`; a publicação simultânea saiu como `ExecutionConflict` com o `1023` do servidor
  (`redshift.publication.concurrent`); o `COPY` que falha sai como `ProgrammingError` e nada é
  publicado; `svv_all_columns` descreve a tabela publicada com `bigint`, `date`, `timestamp without
  time zone`, `double precision`, `numeric` com precisão 18 e escala 2, `character varying` com a
  largura e `super`, a grafia que a reconciliação compara, sem diff
  (`redshift.publication.svv_all_columns`); e a junção de duas tabelas publicadas em `AUTO` roda com
  `DS_DIST_ALL_NONE` (`redshift.publication.join_plan`).
- **A carga pelo pacote** (`scripts/migrate_parquet_to_delta.py --environment prod`, 14:16 a
  14:19): as 12 tabelas do modelo entraram na raiz `<raiz>/prod/<tabela>`, com contagens e somas
  iguais em toda partição, e `alembic_version`, `meta_update_status` e `schema.json` ficaram fora
  do modelo; `environment_limits` deu 16 threads e `memory_limit` de 14.030 MiB. As partições de
  `cad_lancamentos` entraram em 22,7 s, 16,8 s, 36,9 s e 22,7 s (33.239.719, 23.789.279, 52.654.607
  e 32.218.190 linhas), com o pico do processo em 11.419 MB depois das duas primeiras e 16.198 MB
  depois da 2026-03-31, 15% acima do limite e 52% da memória da máquina, contra 16.430 MB às 01:53;
  `rel_contrato_operacao` até 11,9 s e 3.691 MB, `cad_operacoes` até 6,5 s e 2.376 MB,
  `cad_contratos` até 5,5 s, e as tabelas sem partição de 2,1 s a 5,5 s, o tempo da abertura do
  motor por partição. O tempo a mais da 2026-03-31, 36,9 s contra 10,1 s às 01:53, cabe nas duas
  leituras da partição que o pacote acrescentou, a conferência na origem antes do `COPY` e a
  releitura da cópia pelos dois leitores; nenhuma medição separou as parcelas.
- **A auditoria** (`serialize-db audit --table cad_lancamentos --partitions 2026-01-31
  --foreign-keys`, o sandbox em `/tmp` com `memory_limit` de 13,7 GiB e 16 threads): `linhas`,
  `chave_id_lancamento`, `chave_id_lancamento_publicada` e cinco chaves estrangeiras aprovadas, e
  `orfao_data_base_sistema_contrato` reprovada com 989.852 chaves distintas sem cadastro, o órfão
  conhecido da base real, porque `cad_contratos` não tem a partição 2026-01-31 (a leitura de
  2026-09-21, [`PLAN-STAGE-7.md`](PLAN-STAGE-7.md)). As medidas da partição: 33.239.719 linhas,
  nenhum nulo nas colunas `NOT NULL`, `total_valor` 117.667.407.519,194421 e nenhum `Double` não
  finito, iguais às do relatório da carga.
- **A operação**: `history` listou os cinco commits de `cad_lancamentos`, o `CREATE TABLE` sem
  metadados e os quatro `WRITE` com `serialize_db_execution_id=carga-<id>` e
  `serialize_db_input_versions={}`; `snapshot --name carga-2026-09-24` gravou as 12 tabelas nas
  versões atuais; `vacuum` listou 0 arquivos em todas, o que cabe a uma raiz recém-carregada;
  `archive --name carga-2026-09-24` copiou `cad_aliquotas`, `cad_contas` e `cad_contratos` e morreu
  em `cad_lancamentos`, no `CopyObject` do arquivo da partição 2026-06-30 (32.218.190 linhas), com
  `OSError: ... AWS Error NETWORK_CONNECTION during CopyObject operation: curlCode: 28, Timeout was
  reached; Details: Operation too slow. Less than 1 bytes/sec transferred the last 3 seconds`. O
  `copy_file` do `S3FileSystem` é um `CopyObject` só, o S3 copia o objeto no servidor antes de
  responder, e o SDK da AWS em C++ abandona a chamada quando 3 segundos passam sem byte de resposta;
  o PyArrow não expõe esse limite. A entrada do snapshot ficou em `snapshots`, e o arquivo, pela
  metade.

**Consequências**: `Storage.copy` no S3 passa a ser a transferência gerenciada do `boto3`
(`CopyObject` até 8 MiB e `UploadPartCopy` em partes de 8 MiB acima, em paralelo e com a repetição
por parte do botocore), e `test_list_copy_delete` copia 9 MiB nas duas raízes, no moto pelo
`UploadPartCopy`; `deep_copy` continua uma cópia interrompida, pulando as partições que o destino
já registra e recusando o destino que registra um arquivo fora da versão; e `serialize-db archive`
deixou de pular a tabela presente em `arquivo/<nome>/`, um defeito que a falha expôs, porque a
repetição teria dado a `cad_lancamentos` pela metade como arquivada e movido a entrada.
`test_deep_copy_and_relocation` e `test_archive_copies_each_table_with_the_same_sums` cobrem a
repetição, a continuação e a recusa, e reprovaram no código anterior. O substituto local responde a
relação inexistente com `XX000` e a mensagem do alvo. [`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md)
perdeu a carga pelo pacote, as etapas 5 e 8, o arquivo do `UNLOAD` com `SUPER` e o `COPY` do
arquivo do DuckDB; a repetição do `archive` no alvo, o `export`, o `compact` e a publicação da base
inteira ficam nele.

## O que a bateria de 2026-09-24 às 16:51 mostrou no ambiente alvo

Em 2026-09-24, a partir de 16:51 UTC, o usuário rodou de novo os comandos de `SUITE.md` no
ambiente alvo, a partir da `main` com o #73 (as linhas `INFO serialize_db.delta: ... copiada` são
do código novo), na mesma máquina de 16 vCPUs e 31.383 MB, com 28.074 MB disponíveis (Python
3.13.15, DuckDB 1.5.5, deltalake 1.6.4, pyarrow 25.0.1, `sa-east-1`), sobre a raiz de `SUITE.md`
sem nada da bateria das 12:38: o `history` de `cad_lancamentos` tem só os cinco commits da carga,
com o `CREATE TABLE` às 16:52:40 UTC, `snapshot` aceitou de novo o nome `carga-2026-09-24`, e
`archive` copiou toda tabela inteira, sem partição pulada nem recusa. A carga, a auditoria,
`history`, `snapshot`, `vacuum`, `archive` e a publicação da base inteira no Redshift terminaram
sem erro; a saída do terminal e o relatório da carga ficam fora de `plan/`, com os achados aqui. O
probe das threads, `export` e `compact` não rodaram.

- **A carga pelo pacote** (`scripts/migrate_parquet_to_delta.py --environment prod`, `started_at`
  16:51:12): as 12 tabelas, 187.340.509 linhas em 21 arquivos, um por partição, com contagens e
  somas iguais em toda partição (`matches` verdadeiro nas 12) e `alembic_version`,
  `meta_update_status` e `schema.json` fora do modelo; `environment_limits` deu 16 threads e
  `memory_limit` de 14.036 MiB. As partições de `cad_lancamentos` entraram em 19,9 s, 15,3 s,
  31,8 s e 19,4 s (33.239.719, 23.789.279, 52.654.607 e 32.218.190 linhas), de 9% a 15% menos que
  às 14:16, na mesma máquina e com os mesmos limites, sem medição que separe a causa; o pico do
  processo foi 10.766 MB depois das duas primeiras e 16.355 MB depois da 2026-03-31, 17% acima do
  limite e 52% da memória da máquina (16.198 MB às 14:16). `rel_contrato_operacao` até 11,0 s e
  3.780 MB, `cad_operacoes` até 6,5 s e 2.420 MB, `cad_contratos` até 5,1 s, e as tabelas sem
  partição de 2,1 s a 2,7 s; as 21 partições somam 162,5 s.
- **A auditoria** (`serialize-db audit --table cad_lancamentos --partitions 2026-01-31
  --foreign-keys`, o sandbox em `/tmp` com `memory_limit` de 13,7 GiB e 16 threads): o mesmo
  resultado das 14:16, leitura a leitura: `linhas`, as duas chaves e cinco chaves estrangeiras
  aprovadas, `orfao_data_base_sistema_contrato` reprovada com os 989.852 órfãos conhecidos, e as
  medidas da partição iguais (33.239.719 linhas, `total_valor` 117.667.407.519,194421, nenhum
  `Double` não finito).
- **`history`, `snapshot` e `vacuum`**: os cinco commits de `cad_lancamentos`, das 16:52:40 às
  16:53:54, os quatro `WRITE` com `serialize_db_execution_id=carga-<id>`; o snapshot
  `carga-2026-09-24` com as 12 tabelas nas versões atuais (`cad_lancamentos` 4, `cad_contratos`,
  `cad_operacoes` e `rel_contrato_operacao` 3, as outras 1); 0 arquivos a apagar em todas.
- **`archive --name carga-2026-09-24`**, a primeira execução inteira no alvo: as 12 tabelas
  copiadas para `<raiz>/prod/arquivo/carga-2026-09-24/<tabela>`, os 21 arquivos pela transferência
  gerenciada do `boto3`, os quatro de `cad_lancamentos` inclusive, que o `CopyObject` único do
  PyArrow não atravessou na bateria das 12:38; um commit por partição, as partições na ordem
  inversa da carga (`2026-06-30`, `2026-03-31`, `2026-02-28`), a versão no arquivo igual à da
  origem em toda tabela, e a entrada movida para `archived` no fim. A continuação de uma cópia
  interrompida, que `test_archive_copies_each_table_with_the_same_sums` prova no substituto, não
  foi exercitada lá, porque o arquivo estava vazio. A rotina não imprime tempo: a duração da cópia
  ficou sem leitura.
- **A publicação da base inteira** (`serialize-db publish`, a primeira da base real pela
  biblioteca): `--init` criou `sbx_aco_decon.serialize_db_publications`; `--tables cad_contas`
  publicou a versão 1 (`partições [None]`); `--max-workers 4` pulou `cad_contas` (`a versão 1 já
  está publicada`) e publicou as outras 11, as particionadas com todas as partições e
  `cad_lancamentos` na versão 4 com as quatro (141.901.795 linhas), o `COPY ... MANIFEST` sobre
  os arquivos que o `COPY` do DuckDB da carga gravou; `--status` leu as 12 com a versão publicada
  igual à atual e nenhuma partição pendente, sob os nomes `prod_<tabela>`. A rotina não imprime
  tempo: a duração da publicação, `cad_lancamentos` inclusive, ficou sem leitura.

**Consequências**: [`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md) perdeu a repetição do `archive` e a
publicação da base inteira; ficam nele `export`, `compact` e a leitura das medidas. Pela decisão
do usuário do mesmo dia, `compact`, `archive` e `export` imprimem por tabela o tempo e o pico de
RSS do processo, a publicação os põe no log de cada tabela e `deep_copy` registra o tempo de cada
partição; a leitura é `serialize_db.resources.peak_rss_mb`, que o script de migração passa a
importar.

## O que a revisão da documentação mostrou

Em 2026-09-24, a revisão das docstrings e de `docs/` rodou sondas na pasta local, com deltalake
1.6.4, DuckDB 1.5.5 e PyArrow 25.0.1:

- Os exemplos do tutorial de `docs/index.md` (o modelo, `check_models`, `arrow_schema`, o DDL dos
  dois motores, `cast`, `render`, `bind`, `create_table`, `publish_partition`, `version_diff` e
  as propriedades de retenção) e a tabela de tipos devolvem o que a página mostra.
- O `cast` recusa com `ContractError` em dois textos: a instrução ao cliente nas perdas que o cast
  seguro do PyArrow não acusa (o `double` fora da escala, a hora numa coluna `Date`, o fuso, o JSON
  aninhado, o texto longo) e o texto do PyArrow nas outras (`Casting field 'id' with null values to
  non-nullable`, `Rescaling Decimal value would cause data loss`, `Decimal value does not fit in
  precision 18`, `would lose data`, `not in range`, `Unsupported cast`). A página dizia a
  instrução em todas, e o parágrafo foi corrigido.
- A carga inicial falha fora do `ContractError` quando um valor não converte para o tipo do
  contrato (`ConversionException`) ou uma coluna do contrato falta nos arquivos
  (`BinderException`), sem commit: a tabela ficou na versão 0 e sem arquivo de dados.
- Só os commits das partições de uma execução levam os metadados da biblioteca: `CREATE TABLE`, o
  `ADD COLUMN` de `reconcile`, o `WRITE` de `rewrite` e os commits da cópia de `deep_copy` vieram
  sem eles, e `history` e o runbook diziam só `vacuum` e `OPTIMIZE`.
- `delta.compact` numa partição de dois arquivos, um com `NaN` em `valor` e sem mínimo e máximo no
  log, gravou um arquivo com `min.valor` 1.0 e `max.valor` 3.0: a compactação devolve a estatística
  que a decisão da issue #59 tira ([`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md)).
- `check_models` levantou `ContractError` na tabela com duas colunas de partição, em vez de listar
  a violação; com `SERIALIZE_DB_ENVIRONMENT` vazia, `serialize-db history` saiu com erro de uso e
  `serialize-db audit` usou `dev`. A revisão de código de 2026-09-24 corrigiu os dois: a violação
  entra na lista ([etapa 1](PLAN-STAGE-1.md)), e a variável vazia conta como ausente em todo subcomando
  ([`docs/operacao.md`](../docs/operacao.md)).
- O `pdoc` 16 lê o nome de um campo `:param` ou `:raises` até o último dois-pontos da primeira
  linha do campo, mesmo o de um `s3://` citado, e ignora `:returns:` e `:raise:`; a primeira linha
  de cada campo fica sem outro dois-pontos.

## O que a revisão do código de 2026-09-24 mostrou

Em 2026-09-24, a revisão de `src/` e dos testes do pacote contra as regras de código de
`CLAUDE.md` rodou as três execuções da suíte depois de cada grupo de mudanças, no Linux x86_64 do
contêiner:

- A base fictícia de `tests/source_db_projetado.py` saiu idêntica byte a byte, 63 arquivos com o
  mesmo SHA-256 do conjunto, gravada pela versão do `HEAD` e pela revisada, que trocou nomes,
  constantes e expressões sem mudar o que o gerador sorteia.
- A leitura de uma versão cujo arquivo o `vacuum` apagou levantou `FileNotFoundError` no leitor do
  PyArrow numa pasta local (deltalake 1.6.4, PyArrow 25.0.1); `tests/test_operation.py`, só
  `local`, confere essa classe, e `tests/test_delta.py`, que roda também no S3, fica com
  `Exception`.
- O pipeline de `--redshift` de `tests/test_execution.py` juntava por `or` uma condição que a
  asserção anterior já tornava verdadeira, e o motor de cada opção ficava sem conferência; um
  pipeline por opção confere o `RedshiftEngine` e o `DuckDBEngine`.
- `read_as_the_probe`, de `tests/test_source_db_projetado.py`, repetia a leitura de
  `parquet_source.footer_columns`, com a mesma saída nos 62 arquivos Parquet da base; o teste lê
  cada arquivo pela função do probe.
- As contagens: sem variável, 209 passam e 316 são pulados; com a raiz local, 431 e 94; com o
  substituto local, 524 e 1 ([`CURRENT_STATE.md`](CURRENT_STATE.md)).
- `scripts/migrate_parquet_to_delta.py`, com `SERIALIZE_DB_ENVIRONMENT` vazia e sem
  `--environment`, saiu com 1 e o traceback de `ContractError: valor '' fora da regra da partição`,
  enquanto `serialize-db load` usava `dsv`; o script lê a variável vazia como ausente, e
  `tests/test_migrate_parquet_to_delta.py` confere o ambiente `dsv`. Com o caso novo, sem
  variável, 209 passam e 317 são pulados; com a raiz local, 432 e 94; com o substituto local, 525
  e 1.

## O que as sondas do acesso de leitura mostraram

Em 2026-09-24, numa pasta local do contêiner de desenvolvimento (Linux x86_64), com DuckDB 1.5.5,
deltalake 1.6.4, PyArrow 25.0.1 e SQLAlchemy 2.0.54, as sondas da
[etapa 10](PLAN-STAGE-10.md) rodaram sobre uma tabela Delta de quatro partições gravada por
`delta.publish_partition`:

- **A view sobre `delta_scan(uri, version := v)` lê o log na criação**: uma abertura de arquivo, em
  8,7 ms, e nenhuma de Parquet. A versão inexistente falha no `CREATE VIEW`, com `IOException`
  (`LogSegment end version 4 not the same as the specified end version 9`).
- **A poda pela view é a do `delta_scan` direto**: o `=` e o `BETWEEN` abriram só as pastas das
  partições pedidas, o `IN` de um valor também, e o `IN` de dois valores abriu as quatro, pela view
  e direto; o `IN` ao lado do `BETWEEN` abriu as três do intervalo.
- **A troca da view por tabela numa transação** (`BEGIN`, `DROP VIEW`, `CREATE TABLE ... AS
  SELECT`) devolveu a view com as 40 linhas no `ROLLBACK` e deixou a tabela com as 10 linhas da
  partição pedida no `COMMIT`. Duas trocas em cursores paralelos da mesma conexão terminaram sem
  erro.
- **A versão anterior a uma coluna nova** expõe as colunas dela: `delta_scan(..., version := 0)`
  deu `a` e `b`, e a coluna `c` da versão 1 foi `BinderException` na consulta que a cita.
- **O statement ORM compila para as tabelas publicadas** por `compiled_for_cursor` e
  `literal_text` do motor Redshift com o prefixo `prod_`, a junção e o `IN` de lista inclusive
  (`FROM "prod_cad_contas" JOIN "prod_cad_lancamentos"`), o mesmo nome de `published_name`.

A leitura do código achou o que o leitor não pode supor: `_write_control` grava o arquivo de
controle com as chaves em ordem alfabética, e a entrada de um snapshot não tem data, então nada nele
diz qual snapshot é o último.

**Consequências**: o [arquivo da etapa 10](PLAN-STAGE-10.md) cria as views pelo `ingest` do motor,
materializa pela troca numa transação e documenta o `IN` de vários valores, que o leitor não
reescreve; o snapshot padrão, que o arquivo de controle não identifica, passou a ser o canal
`default` da etapa, movido por `serialize-db channel` (decisões do usuário do mesmo dia).

No mesmo dia e ambiente, na revisão da interface da etapa, uma sonda leu uma view cuja definição
tem o filtro de partições do `ingest` (o `BETWEEN` do menor ao maior valor ao lado do `IN`), sobre
uma tabela de quatro partições gravada por `write_deltalake` (deltalake 1.6.4). A view poda como o
`ingest`: sem filtro do cliente, abriu as pastas do intervalo; com `data = '2026-08-31'`, só essa
pasta; o valor do intervalo fora do `IN` abriu a pasta dele e deu 0 linhas, e o valor fora do
intervalo não abriu nenhuma. O `CREATE TABLE ... AS SELECT *` sobre a view abriu as pastas do
intervalo.

**Consequência**: nenhuma no plano. A sonda sustentava o recorte de partições na abertura do
leitor, que o usuário não adotou: a materialização parcial continua em
`materialize(..., partitions=...)`.

## O que a bateria de 2026-09-24 às 23:25 mostrou no ambiente alvo

Em 2026-09-24, de 23:25 a 00:02 UTC (já 2026-09-25), o usuário rodou no ambiente alvo os probes, as
suítes e os comandos de `SUITE.md`, a partir da `main`, na mesma máquina de 16 vCPUs e
31.383 MB, com 28.043 MB disponíveis (Python 3.13.15, DuckDB 1.5.5, deltalake 1.6.4, pyarrow
25.0.1, `sa-east-1`), sobre uma raiz nova no layout `<raiz>/prd/<tabela>`. É a primeira bateria com
o ambiente `prd` e com o tempo e o pico de RSS impressos pelo `archive`, pelo `export`, pelo
`compact` e pela publicação. Os relatórios e a saída do terminal ficam fora de `plan/`, com os
achados aqui. Nenhum caso das suítes falhou, e as únicas chamadas que falharam foram as permissões
já conhecidas dos probes.

- **Os probes** (23:25 a 23:27): `redshift.py` sem falha, com a credencial de quem chama expirando
  em 45 minutos (`RS-18`) e a contagem de `sys_load_error_detail` sem linha (`RS-12`); `space.py`
  com o DuckDB em 16 threads e `memory_limit` de 24,5 GiB pelo padrão do DuckDB, e a mesma falha de
  `SP-4` (sem `sagemaker_studio` na `.venv`, sem o perfil `DomainExecutionRoleCreds`);
  `bucket.py` com as mesmas 6 chamadas negadas (versionamento, Object Lock, propriedade, ciclo de
  vida, política e uploads multipart) e `BK-14` com 1.943 versões não correntes (50.394.018 bytes)
  e 1.823 marcadores de exclusão sob a raiz das suítes; `catalog.py` com o Athena negado e o Lake
  Formation e o S3 Tables sem resposta (61,0 s e 30,1 s), como em 2026-09-23.
- **As suítes**: `-m "not redshift"` com 481 aprovados em 185,0 s; `-m redshift` com 44 aprovados
  duas vezes (531,8 s e 569,5 s), as duas leituras iguais salvo nomes e tempos, `naofinito_valor`
  2 igual ao esperado e `nan_na_tabela_detalhe` com as quatro comparações falsas e o texto `NaN`;
  `tests/test_engine_redshift.py -m redshift` com 6 aprovados duas vezes (94,7 s e 83,2 s) e
  `tests/test_publication.py -m redshift` com 8 aprovados duas vezes (187,5 s e 178,0 s), com a
  publicação simultânea como `ExecutionConflict` e a junção em `DS_DIST_ALL_NONE`.
- **A carga pelo pacote** (`scripts/migrate_parquet_to_delta.py --environment prd`, `started_at`
  23:59:36): as 12 tabelas, 187.340.509 linhas, com contagens e somas iguais em toda partição e
  `alembic_version`, `meta_update_status` e `schema.json` fora do modelo; `environment_limits` deu
  16 threads e `memory_limit` de 14.021 MiB. As partições de `cad_lancamentos` entraram em 20,8 s,
  16,1 s, 34,2 s e 19,9 s, e o pico do processo foi 11.011 MB depois das duas primeiras e
  16.248 MB depois da 2026-03-31, acima do limite do DuckDB como às 14:16 (16.198 MB) e às 16:51
  (16.355 MB). As 21 partições somam 164,3 s. As tabelas sem partição aparecem na saída como
  `None:`.
- **A auditoria** (`--table cad_lancamentos --partitions 2026-01-31 --foreign-keys`, `memory_limit`
  de 13,5 GiB): o mesmo resultado das 14:16 e das 16:51, com os 989.852 órfãos de
  `orfao_data_base_sistema_contrato`, 33.239.719 linhas, `total_valor` 117.667.407.519,194421 e
  nenhum `Double` não finito.
- **`history`, `snapshot` e `vacuum`**: os cinco commits de `cad_lancamentos`, de 00:01:01 a
  00:02:19; o snapshot `carga-2026-09-24` com as 12 tabelas; 0 arquivos a apagar em todas.
- **`archive --name carga-2026-09-24`**: os 21 arquivos das 12 tabelas copiados e a entrada movida
  para `archived`. As tabelas sem partição levaram de 1,5 s a 1,6 s cada (0,4 s de cópia);
  `cad_contratos` 3,8 s, `cad_operacoes` 3,9 s, `rel_contrato_operacao` 4,8 s e `cad_lancamentos`
  11,1 s, com as partições dela copiadas em 1,9 s a 3,3 s. O pico do processo ficou em 349 MB.
- **A publicação da base inteira** (`serialize-db publish`): `--init` criou a tabela de controle;
  `--tables cad_contas` publicou a versão 1 em 4,3 s; `--max-workers 4` pulou `cad_contas` e
  publicou as outras 11, as sem partição de 3,8 s a 5,7 s, `cad_contratos` em 21,5 s,
  `cad_operacoes` em 32,5 s, `rel_contrato_operacao` em 40,3 s e `cad_lancamentos` em 153,9 s,
  com o pico do processo em 273 MB; `--status` leu as 12 como `prd_<tabela>`, sem partição
  pendente.
- **`export` de `cad_lancamentos`**, a primeira execução no alvo: por cópia, 4 arquivos em
  8,8 s com pico de 264 MB; por `--mode rewrite`, 4 arquivos em 17,3 s com pico de 5.425 MB.
- **`compact --table cad_lancamentos --partitions 2026-03-31`**: 0 arquivos gravados e 0
  removidos em 0,2 s. A partição tem um arquivo só, e `delta.compact` não commita nesse caso: a
  compactação real e a memória dela continuam sem leitura no alvo.

**Consequências**: [`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md) perdeu o `export` e a duração do
`archive` e da publicação no item da operação no ambiente alvo, que fica com o `compact` de uma
partição de vários arquivos; o item das versões não correntes ganhou a leitura de `BK-14`.
Nenhum arquivo do plano muda.

## O que a bateria de 2026-09-25 às 17:25 mostrou no ambiente alvo

Em 2026-09-25, de 17:25 a 19:37 UTC, o usuário rodou no ambiente alvo os probes, as suítes e os
comandos de `SUITE.md`, a partir da `main` do dia, numa máquina de 8 vCPUs e 15.505 MB (Python
3.13.15, DuckDB 1.5.5, pyarrow 25.0.1, `sa-east-1`), com a pasta preparada de novo por
`prepare_offline.sh`: `SP-9` leu na `.venv` o deltalake 1.6.6, o boto3 1.43.102, o
`redshift_connector` 2.1.17 e o sqlglot 30.19.0. É a primeira bateria com o deltalake 1.6.6 e com
a etapa 10, e a primeira rodada de `probes/credentials.py` no alvo. O relatório dela,
`plan/readings/credentials-2026-09-25-1833.txt`, está no histórico do git; os outros ficam fora de
`plan/`, com os achados aqui, e a saída
do leitor, do `export` e do `compact` veio colada na conversa, sem a hora. Nenhum caso das suítes
falhou.

- **Os probes** (17:25 a 17:27): `redshift.py` sem falha nova, com a credencial de quem chama
  expirando em 52 minutos (`RS-18`) e 45 erros de carga em 30 dias em `sys_load_error_detail`
  (`RS-12`), contra 25 em 2026-09-24 às 01:41; `space.py` com 8 CPUs, 15,1 GiB, o DuckDB em 8
  threads e `memory_limit` de 12,1 GiB pelo padrão do DuckDB, e a mesma falha de `SP-4`;
  `bucket.py` com as mesmas 6 chamadas negadas e `BK-14` com 2.980 versões não correntes
  (86.695.363 bytes) e 2.788 marcadores de exclusão sob a raiz das suítes; `diagnose_aws.py` com o
  boto3, o delta-rs 1.6.6 e o DuckDB listando o prefixo; `catalog.py` com o Athena negado e o Lake
  Formation e o S3 Tables sem resposta (60,7 s e 30,3 s), como em 2026-09-23.
- **As suítes** (17:27 a 18:00): `-m "not redshift"` com 512 aprovados em 225,5 s, com
  `delta.client_version` `delta-rs.py-1.6.6` e as leituras de 2026-09-24 às 01:43 salvo os tempos
  e as 8 threads; `-m redshift` com 45 aprovados duas vezes (620,2 s e 566,1 s), os 44 de
  2026-09-24 mais `test_redshift_reader_matches_the_delta_reader`, em que o leitor Redshift, com o
  `stream` pelo `UNLOAD` no bucket da suíte, leu as mesmas 40 linhas do leitor Delta;
  `tests/test_engine_redshift.py -m redshift` com 6 aprovados duas vezes (71,7 s e 77,9 s) e
  `tests/test_publication.py -m redshift` com 8 aprovados duas vezes (191,7 s e 174,2 s). Entre os
  8 está `test_cli_publishes_by_channel_and_snapshot_and_reverts`: `serialize-db publish` por
  `--channel default`, por `--snapshot`, de volta ao snapshot anterior, com a partição trocada de
  volta às 40 linhas e o mês novo retirado, por `--channel current`, `--status` e `--unpublish`,
  num ambiente `poc<id>`.
- **A carga** (`scripts/migrate_parquet_to_delta.py --environment prd`, `started_at` 18:16:38), na
  raiz de `SUITE.md` carregada de novo: as 12 tabelas, 187.340.509 linhas em 21 partições, com
  contagens e somas iguais em toda partição e `alembic_version`, `meta_update_status` e
  `schema.json` fora do modelo; `environment_limits` deu 8 threads e `memory_limit` de 6.227 MiB,
  a metade dos 12.454 MB disponíveis. As partições de `cad_lancamentos` entraram em 28,2 s,
  22,0 s, 52,3 s e 28,7 s, e o pico do processo foi 7.824 MB depois das duas primeiras e
  8.677 MB depois da 2026-03-31, acima do limite do DuckDB como nas baterias de 2026-09-24. As 21
  partições somam 219,4 s, contra 164,3 s na máquina de 16 vCPUs às 23:25 de 2026-09-24. É a
  primeira carga inteira numa máquina de cerca de 16 GB: em 2026-09-23 às 23:05, numa de
  15.786 MB, o kernel matou o processo na partição 2026-03-31, sob o `memory_limit` padrão do
  DuckDB, 12,3 GiB.
- **`duckdb_threads.py`** (18:21) parou em `DT-1` com `TableNotFoundError`: `SUITE.md` passava a
  raiz, o probe lê as tabelas direto na pasta recebida, em `<raiz>/cad_lancamentos`, e a carga as
  grava em `<raiz>/prd/`. O comando passa `$TARGET_ROOT_PATH/prd` desde então.
- **O leitor Delta e o leitor Redshift** (bloco "Acesso de leitura" de `SUITE.md`):
  `db.open_delta()` abriu as 12 views do snapshot `carga-2026-09-25`, apontado pelo canal
  `default`, em 0,645 s, com `cad_lancamentos` na versão 4 e `cad_contas` na 1; a contagem de
  `cad_contas` deu 97 pelo leitor Delta e 97 por `db.open_redshift()`, só por `query`.
- **`export` de `cad_lancamentos`**: por cópia, 4 arquivos em 10,6 s com pico de 262 MB; por
  `--mode rewrite`, 4 arquivos em 34,0 s com pico de 4.343 MB, contra 8,8 s e 17,3 s com 5.425 MB
  na máquina de 16 vCPUs.
- **`compact --table cad_lancamentos --partitions 2026-03-31`** saiu com 2 e `o snapshot
  carga-2026-09-25 está na versão atual 4 de cad_lancamentos; compacte antes de um snapshot`, a
  recusa que a [etapa 9](PLAN-STAGE-9.md) prevê: `SUITE.md` roda o `compact` depois do snapshot
  da publicação.
- **As credenciais de uma hora** (`probes/credentials.py` sobre `<raiz>/prd/cad_contas`, de
  18:33:51 a 19:36:53, 14 rodadas,
  `plan/readings/credentials-2026-09-25-1833.txt`, no histórico do git):
  - **O contêiner trocou a chave a cada cerca de 30,6 minutos.** A chave do início expirava às
    19:19:10, 45 minutos adiante; a cadeia do `boto3` passou a entregar outra, que expira às
    19:49:57, entre 18:48:54 e 18:53:55, e outra, que expira às 20:20:31, entre 19:18:59 e
    19:24:00. Com a expiração de 18:17:54 que `space.py` leu às 17:25, as quatro ficam a cerca de
    30,6 minutos uma da outra: se cada chave vale uma hora, a que a cadeia entrega tem de 29 a 60
    minutos pela frente.
  - **O delta-rs, o `S3FileSystem`, o `boto3` e a conexão Redshift seguiram lendo.** O
    `DeltaTable` aberto no início leu nas quatro rodadas depois da expiração das 19:19:10
    (`CR-3`), como o rodapé pelo `S3FileSystem` (`CR-6`) e `Storage.read_text` (`CR-7`); a conexão
    Redshift respondeu ao `select 1` duas vezes depois da expiração da senha, às 19:33:52
    (`CR-8`).
  - **O `delta_scan` falhou uma vez depois que a chave do secret expirou.** O secret do DuckDB
    guardou a chave do início até ela expirar, sem seguir a troca do contêiner das 18:53; às
    19:24:00, 5 minutos depois da expiração, o `delta_scan` da conexão aberta no início falhou
    com `IO Error: DeltaKernel ObjectStoreError (8): ... Generic S3 error: Error performing GET
    .../_delta_log/_last_checkpoint`, o `read_parquet` da mesma rodada leu, e o secret passou à
    chave nova (`CR-9`). O `delta_scan` leu nas três rodadas seguintes (`CR-4` reprovada, 1 de 4).
    É o que o substituto mostrou: só o `httpfs` renova o secret de `REFRESH auto`, e a sonda lê
    cada resultado até o fim.
  - **A cláusula do `COPY` e do `UNLOAD` seguiu a chave do contêiner** desde a troca das 18:53:55
    (`CR-10`): `credentials_clause` resolve a cadeia numa sessão nova do `boto3` a cada chamada.
  - **O controle**: os cinco clientes novos leram depois da espera (`CR-11`).

**Consequências**: a primeira execução longa confirma que o delta-rs, o `S3FileSystem` e o `boto3`
renovam a credencial do contêiner e que a conexão Redshift aberta sobrevive à expiração da senha.
O motor DuckDB de uma execução e o leitor Delta leem por `delta_scan` numa conexão cujo secret
guarda a chave da abertura, com menos de uma hora pela frente: o primeiro `delta_scan` depois da
expiração falha, a menos que uma leitura pelo `httpfs` tenha renovado o secret antes, e a
biblioteca precisa renovar o secret por conta própria, na forma que espera o usuário em
[`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md). A cláusula montada uma vez por tabela leva, pela leitura
deste dia, uma chave com cerca de 29 minutos ou mais pela frente, contra os 153,9 s da publicação
de `cad_lancamentos` em 2026-09-24, e a decisão de 2026-09-25 da [etapa 8](PLAN-STAGE-8.md) fica.
O item do deltalake 1.6.6 saiu de [`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md); o do acesso de leitura
ficou com a publicação da base inteira por canal, com o tempo e o pico de RSS por tabela, e com o
`UNLOAD` do cliente só de leitura; o da operação, com o `compact` de uma partição de vários
arquivos, que roda antes do snapshot. Os arquivos das [etapas 3](PLAN-STAGE-3.md) e
[8](PLAN-STAGE-8.md) e [`PLAN.md`](PLAN.md) citam esta leitura.

## O que a implementação da etapa 10 mostrou

`serialize_db.reader`, os canais do arquivo de controle (`delta.set_channel`, `channel_snapshot`,
`snapshot_versions`), `serialize-db channel` e `serialize-db publish` por `--snapshot` ou
`--channel` foram implementados em 2026-09-25 no contêiner de desenvolvimento (Linux x86_64,
Python 3.13.12, DuckDB 1.5.5, deltalake 1.6.4, PyArrow 25.0.1, pandas 3.0.6, SQLAlchemy 2.0.54,
pytest 9.1.1), na pasta local e no substituto local; os 16 casos de `tests/test_reader.py`
passam, 13 `local`, 2 sem conexão e o `redshift` no substituto. O que os casos leram:

- **A poda pela view do leitor é a do `delta_scan`**, lida no log `FileSystem` do DuckDB por
  `opened_partition_folders` sobre `cad_lancamentos` em três meses: `data_base_str = '2026-07-31'`
  abriu só a pasta desse mês, o `BETWEEN` dos dois primeiros abriu as duas e o `IN` do primeiro e
  do terceiro abriu as três, como a sonda de 2026-09-24; o leitor não reescreve o `IN`.
- **Os tipos do `query` são os do contrato**: os tipos Arrow das 30 linhas lidas pela view são os
  de `schema.arrow_schema`, campo a campo, a nulidade à parte; `stream` com `batch_size=7`
  entregou lotes de até 7 linhas, as mesmas 30; `to_pandas(types_mapper=pd.ArrowDtype)` deu
  `decimal128(18, 2)[pyarrow]` ao decimal e `date32[day][pyarrow]` à data.
- **A materialização que falha deixa a view**: com os arquivos Parquet de um mês apagados depois
  da abertura, `materialize` sobe o `duckdb.Error` do `CREATE TABLE ... AS SELECT` pelo pool, o
  `ROLLBACK` devolve a view, `materialized` fica vazio e a view responde pelos meses com arquivo.
- **O finalizador apaga a pasta do motor**: `close` chama `DuckDBEngine.cleanup` uma vez, contado
  por um `monkeypatch`, e a pasta `serialize_db_*` some; o leitor não fechado some com a pasta no
  `del` seguido de `gc.collect()`, pelo `weakref.finalize`.
- **A tabela criada depois do snapshot não tem view**: `cad_lancamentos_projetados`, publicada
  depois do snapshot `2026T3`, fica fora de `versions`; o statement Core que a cita é
  `ContractError` com o nome da tabela e do snapshot em `query`, `stream` e `materialize`, o texto
  pronto que a cita é `CatalogException` do DuckDB, e pelo canal `current` ela entra na versão 1.
- **A volta a um snapshot anterior, no substituto** (`serialize-db publish --snapshot A`, com a
  versão 4 de `cad_lancamentos_projetados` publicada pelo snapshot `B` e `A` na versão 2)
  republicou o mês trocado na versão 3 pelos arquivos da versão 2, as 40 linhas de ids 41 a 80 no
  lugar das 7, apagou o mês `2026-09-30` da versão 4, sem manifesto, e gravou a linha de controle
  `(2, exec-r)`; `--channel current` publicou a 4 de novo e `--status` leu `publicada 4, atual 4,
  pendentes []`. Sobre a conexão de mentira, o `UPDATE` da volta termina em `AND delta_version =
  3` e o manifesto do mês fica em `prd/publicacao/exec-r/cad_lancamentos/<mês>.manifest` com os
  arquivos da versão 2.
- **Os erros de uso saem com 2 e uma linha, sem Traceback**: o snapshot arquivado, a tabela fora
  do snapshot, a chamada sem `--snapshot` nem `--channel` ou com os dois, `--status --channel
  current`, o canal sem snapshot e a tabela fora do modelo; `serialize-db archive` do snapshot do
  canal `default` sai com 2 antes de copiar, sem criar `prd/arquivo`.
- **O leitor Redshift dá as linhas do leitor Delta, no substituto**: as 40 linhas de um mês de
  `cad_lancamentos` publicado, por `query` e por `stream` com `batch_size=25`, são as do leitor
  Delta na versão atual, e o `close` deixa vazias a pasta do leitor sob `unload_to` e a de
  `staging/<reader_id>` do banco em `db.open_redshift`.

As rodadas antes do commit, no mesmo contêiner: `uv run pytest` sem variável, 211 aprovados e 334
pulados em 24,6 s; com `SERIALIZE_DB_TEST_LOCAL_ROOT`, 449 aprovados e 96 pulados em 99,6 s; com a
raiz local e `SERIALIZE_DB_TEST_EMULATOR`, as seis suítes do alvo em 91 aprovados e 1 pulado em
30,9 s, e tudo em 544 aprovados e 1 pulado, o teste da Data API, em 138,8 s.

**Consequências**: o [arquivo da etapa 10](PLAN-STAGE-10.md) trocou a `Interface` pela descrição
da implementação, como as etapas anteriores; nenhuma leitura contradisse o plano, e as leituras que
só o ambiente alvo dá estão em [`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md).

## O que as sondas da compactação mostraram

Em 2026-09-25, numa pasta local do contêiner de desenvolvimento (Linux x86_64, 4 vCPUs e 16 GB),
com deltalake 1.6.4, DuckDB 1.5.5 e PyArrow 25.0.1, as sondas procuraram, pelos escritores da
biblioteca, uma partição com `NaN` em mais de um arquivo, o caso em que `delta.compact` devolve a
estatística que a issue #59 tira; a sonda de 2026-09-24 montou os dois arquivos por fora. As
tabelas tinham `id_lancamento` (`BigInteger`), `valor` (`Double`) e a partição `data_base_str`.

- **As propriedades de escrita tiram a estatística da coluna no `optimize.compact`**: sobre dois
  arquivos de uma partição, um com `NaN` em `valor`, `writer_properties=WriterProperties(
  column_properties={"valor": ColumnProperties(statistics_enabled="NONE")})` deu o arquivo
  compactado sem mínimo, máximo e `nullCount` de `valor` no log e sem estatística dela no rodapé,
  com as de `id_lancamento`, e as três linhas, o `NaN` incluído; sem as propriedades, `min.valor`
  1.0 e `max.valor` 3.0, como em 2026-09-24.
- **O `publish_partition` divide a partição no tamanho alvo do delta-rs, 104.857.600 bytes**:
  20.000.000 linhas por um leitor de lotes de 100 mil, como o da troca do motor Redshift, o único
  chamador dele no pacote, deram três arquivos de 104.884.565, 104.890.931 e 104.884.007 bytes e
  um de 16.285.616, todos sem mínimo e máximo de `valor`. O `compact` da partição não commitou (0
  gravados, 0 removidos): nenhum par de arquivos cabe no tamanho alvo.
- **O motor DuckDB e a carga inicial gravam um arquivo por partição**: um `COPY ... TO` sem
  `PARTITION_BY` por partição (`DuckDBEngine.export_partition`, `load._copy_partition`), num
  commit que substitui a partição; lido no código.
- **O `COPY ... PARTITION_BY` do DuckDB abre outro arquivo para a mesma partição acima de
  `partitioned_write_max_open_files`**, 100 no DuckDB 1.5.5: direto no DuckDB, com as linhas das
  partições intercaladas, 3 partições de 5.000.000 linhas e 40 de 1.000.000 deram um arquivo por
  partição com 4, 16 e 32 threads, e 150 de 20.000 deram até 8, 16 e 25 arquivos por partição.
- **O `rewrite` leva esse limite à tabela Delta**: com as threads fixadas na sonda, 150 partições
  de um arquivo cada deram um arquivo por partição com 4, 16 e 32 threads; 150 partições de três
  arquivos cada (o primeiro por `publish_partition` e os outros por `write_deltalake(mode=
  "append")`, no lugar dos arquivos de um `UNLOAD` sem `PARALLEL OFF`) deram 254 arquivos com 4
  threads e 222 com 16, e 40 partições de três arquivos, um por partição com 16 threads. Com 4
  threads, a partição do `NaN` saiu em três arquivos sem mínimo e máximo de `valor`, e o `compact`
  dela juntou os três num arquivo com `min.valor` 4,5e-06 e `max.valor` 0,99998.
- **`deltalake==1.6.4` está retirado (yanked) do PyPI**, com o motivo "Issue: #4784", pelo aviso
  do `uv` ao instalar a versão para a sonda.

**Consequências**: uma partição com coluna `Double` sem mínimo e máximo chega ao `compact` em
vários arquivos abaixo do tamanho alvo só depois de um `rewrite` de uma tabela com mais de 100
partições e partições de vários arquivos, que hoje só o motor Redshift grava (o `UNLOAD` acima de
5.000.000 linhas e a troca acima do tamanho alvo); a carga no ambiente alvo tem 21 arquivos nas 12
tabelas, um por partição. O usuário decidiu em 2026-09-25 que `delta.compact` passa ao
`optimize.compact` as propriedades de escrita sem estatística nas colunas `Double` que o log da
partição já traz sem mínimo e máximo, e a regra entrou no [arquivo da etapa 9](PLAN-STAGE-9.md);
o item saiu de [`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md), onde entrou a versão retirada do
`deltalake`.

## O que o substituto das credenciais que expiram mostrou

Em 2026-09-25, no contêiner de desenvolvimento (Linux x86_64), com deltalake 1.6.4, DuckDB 1.5.5
(`httpfs`, `aws` e `delta` de uma pasta de extensões), PyArrow 25.0.1 e boto3 1.43.98, a sonda
`probes/credentials.py` e scripts de apoio leram os clientes da biblioteca contra um substituto: o
moto com uma tabela `cad_contas` de duas partições, um IMDS local que troca a chave a cada 40 s,
cada uma válida por 70 s, e um proxy S3 que responde `400 ExpiredToken` à requisição assinada com a
chave vencida, como o S3. O IMDS ficou no lugar do endpoint de credenciais do contêiner, o caminho
do alvo: o delta-rs não leu `AWS_CONTAINER_CREDENTIALS_FULL_URI` e foi ao IMDS de 169.254.169.254,
que o contêiner nega, e o endereço de `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`, 169.254.170.2, não
existe no contêiner.

- **O `delta_scan` não renova o secret.** Depois que a chave guardada no secret expirou, o
  `delta_scan` da conexão aberta no início falhou com `IO Error: DeltaKernel ObjectStoreError (8):
  ... Generic S3 error: Error performing GET .../_delta_log/_last_checkpoint ... 400 Bad Request`:
  a extensão `delta` lê o log pelo object_store do delta-kernel, com a chave do secret. O `glob`,
  a listagem, falhou com `HTTPException ... (HTTP 400 Bad Request)`, também sem renovar.
- **O `read_parquet` renova o secret e repete a requisição.** O `HEAD` e os `GET` com a chave
  vencida receberam 400; o `httpfs` pediu uma credencial nova pela cadeia do SDK da AWS da
  extensão `aws` (1.11.702), repetiu o `HEAD` e o `GET` com ela e leu o arquivo. Depois dele, o
  `delta_scan`, o `glob` e um `COPY ... TO` leram e gravaram com a chave nova.
- **A renovação só fica quando a consulta que a fez termina.** Lido o resultado do `read_parquet`
  com `fetchall()`, `duckdb_secrets()` mostrou a chave nova e o `delta_scan` seguinte leu; com
  `fetchone()`, a consulta ficou aberta, a seguinte a desfez com a renovação, o secret manteve a
  chave vencida e o `delta_scan` falhou nas quatro rodadas depois da expiração, com e sem
  parâmetro na consulta a `duckdb_secrets()`; com `fetchall()`, só na primeira. A sonda lê cada
  contagem com `fetchall()` e, assim, leu o `delta_scan` falhando uma vez a cada chave vencida
  do secret, que o `read_parquet` da mesma rodada renovava.
- **O delta-rs renovou sozinho**: o `DeltaTable` aberto no início pediu credencial nova ao IMDS
  antes da expiração e leu em todas as rodadas.
- **O `S3FileSystem` e o `boto3` falharam com a chave vencida**, pelas regras do IMDS, que o alvo
  não usa: o botocore estende a expiração da credencial do IMDS para 12 a 20 minutos adiante
  quando ela está perto (`ec2_credential_refresh_window` de 10 minutos mais 2 a 10 aleatórios), e
  o SDK da AWS do PyArrow (1.11.800) renovou a cada cerca de cinco minutos, mais que os 70 s da
  chave. A mesma extensão pôs a expiração que a sonda lê pelo `boto3` cerca de 20 minutos
  adiante, e a espera de 2,5 minutos terminou antes dela, com `CR-3` a `CR-7` em `note`.

**Consequências**: a biblioteca lê as tabelas por `delta_scan` e lê várias contagens com
`fetchone()` (`delta.py`, `load.py`, `engine/duckdb.py`); se o alvo repetir o substituto, uma
conexão do DuckDB que atravessa a expiração da chave guardada no secret falha no primeiro
`delta_scan` depois dela, a menos que uma leitura pelo `httpfs` encerrada antes a tenha renovado.
A sonda lê no alvo, em cerca de uma hora, a renovação de cada cliente segurado, com o controle dos
clientes novos, e a pergunta continua em [`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md); o
[arquivo da etapa 3](PLAN-STAGE-3.md) cita esta leitura.

## O que as sondas dos achados da revisão mostraram

Em 2026-09-25, numa pasta local do contêiner de desenvolvimento (Linux x86_64, 4 vCPUs e 16 GB),
com deltalake 1.6.4, DuckDB 1.5.5, PyArrow 25.0.1 e redshift-connector 2.1.17 e sem as variáveis
`AWS_*`, as sondas conferiram os achados da revisão dos comentários e da documentação que dependem
do comportamento, e a do `nullCount` achou a perda de linhas no dataset do delta-rs.

- **Os códigos de saída da CLI.** Por subprocesso, sobre uma raiz com `cad_contas` do modelo de
  `tests/lancamentos_model.py`: `run --engine redshift` sem as variáveis `SERIALIZE_DB_REDSHIFT_*`,
  e com a conexão informada e um `--execution-id` de 60 caracteres, saíram com o traceback do
  `ContractError` e o código 1; `publish --init` e `audit --engine redshift` sem conexão, também;
  `publish --status` sem conexão saiu com 2 e a mensagem, sem traceback; e `load --source
  gs://bucket/origem` saiu com o traceback do `ValueError` e 1.
- **O `nullCount` de uma coluna sem estatística no rodapé.** O PyArrow gravou um timestamp em
  `INT96` (`use_deprecated_int96_timestamps=True`), 5 linhas com 3 nulos, com `statistics` `None`
  na coluna; `file_from_footer` deu `null_count` 0 a ela, e `register_files` commitou a versão 1
  com esse `nullCount`. `quando IS NULL` leu 0 linhas pelo `DeltaTable.to_pyarrow_dataset()` e 3
  pelo `delta_scan`.
- **O `Double` sem estatística.** `publish_partition` com `columns_without_min_max=["valor"]`
  sobre `[1.0, NaN, null, null, null]` gravou o rodapé sem estatística de `valor` e o log sem o
  mínimo, o máximo e o `nullCount` dela. Pelo dataset do delta-rs, `valor IS NULL`, `valor > 0` e
  `valor = 1.0` leram 0 linhas, e `valor IS NOT NULL` leu as 5, os três nulos incluídos; o
  `delta_scan` leu 3 em `valor IS NULL`. Com a estatística gravada, sobre `[1.0, 2.0, null, null,
  null]` e o `nullCount` 3, os dois leitores leram 3.
- **O arquivo registrado pelo motor DuckDB.** `DuckDBEngine.export_partition` de uma tabela com
  `Numeric(18, 2)`, `DateTime`, `Boolean`, `String(10)` e `Double` registrou o log sem o mínimo e o
  máximo das três primeiras. `valor > 1`, `quando > '2025-12-31'` e `legado = true` leram 0, 0 e 0
  linhas pelo dataset do delta-rs, contra 2, 2 e 1 pelo `delta_scan`; `nome = 'a'` e `taxa > 1`
  leram 1 e 1 pelos dois; `valor IS NULL`, com o `nullCount` 1 no log, leu 1 pelos dois. O
  `to_pyarrow_table` e o `to_pandas` com `filters=[("valor", ">", Decimal("1.00"))]` leram 0; o
  DuckDB sobre o dataset registrado por `con.register` leu 2 em `valor > 1` e 0 no filtro de
  `quando`.
- **A garantia do fragmento.** O `partition_expression` do arquivo sem estatística trazia
  `(valor >= null[double])` e `(valor <= null[double])`, e o do arquivo `INT96`, `is_valid(quando)`.
  O `filestats_to_expression_next` de `python/src/lib.rs`, lido no `main` do delta-rs em
  2026-09-25, monta `>=` e `<=` de cada mínimo e máximo sem conferir o nulo, põe `is_valid` com o
  `nullCount` 0 e só acrescenta `or is_null` com o `nullCount` entre zero e as linhas do arquivo;
  as colunas são as de `delta.dataSkippingStatsColumns` ou as primeiras de
  `delta.dataSkippingNumIndexedCols`.
- **O filtro pela partição.** Numa tabela particionada por `particao`, com `valor` gravado sem
  estatística, a garantia de cada fragmento trazia `particao == "a"` ao lado de
  `valor >= null[double]`; `particao == 'a'`, `particao == 'b'` e `id > 3` leram as 2, 3 e 2
  linhas certas pelo dataset do delta-rs, e `valor > 1.5` leu 0 de 2. O `read_back` de
  `delta.py`, que filtra o dataset só pela coluna da partição, fica fora da perda.
- **A perda por arquivo e o `nullCount`.** Numa tabela com a partição `a` gravada com a
  estatística de `valor` e a `b` sem ela, como a regra da issue #59 por partição, a leitura sem
  filtro e `particao = 'b'` trouxeram as mesmas 6 e 3 linhas pelo dataset do delta-rs e pelo
  `delta_scan`; `valor > 1.5` trouxe 1 linha, só da `a`, contra 2; `valor IS NULL`, 1 contra 3; e
  `valor IS NOT NULL`, 5 contra 3, com os dois nulos da `b`. Num arquivo registrado sem mínimo e
  máximo e com o `nullCount` 3 de 5, `IS NULL` e `IS NOT NULL` trouxeram os certos 3 e 2, e
  `valor > 1.5`, 0 de 1. Numa coluna toda nula gravada pelo `write_deltalake`, com o `nullCount`
  igual às linhas, a garantia trazia `is_null(valor)`, e os três filtros leram certo.
- **A propriedade `delta.dataSkippingStatsColumns`.** Com `id,nome,taxa`, posta por
  `alter.set_table_properties` na tabela registrada, a garantia deixou `valor`, `quando` e
  `legado` de fora, e os três filtros leram 2, 2 e 1 pelo dataset do delta-rs.
- **O arquivo órfão do `export_partition`.** `DuckDBEngine.export_partition` com o valor
  `2026-08-31` numa tabela sem partição levantou o `ContractError` de `register_files` depois do
  `COPY`, e o arquivo `sonda_<uuid>.parquet` ficou na pasta da tabela, fora do log.
- **As ações de uma tabela particionada sem arquivos.** `get_add_actions(flatten=True)` de uma
  tabela criada por `create_table` devolveu 0 linhas com a coluna `partition.data_str`, e o
  conjunto dos valores saiu vazio.

**Consequências**: o código se afasta da [etapa 6](PLAN-STAGE-6.md) nos códigos de saída, e
`file_from_footer` supõe zero no `null_count` que falta, o que [`parquet.md`](parquet.md) proíbe
ao leitor. A frase da [etapa 3](PLAN-STAGE-3.md) de que a estatística ausente só deixa de podar
vale para o `delta_scan` e não para o dataset do delta-rs, e foi revista; [`delta.md`](delta.md)
ganhou o comportamento na seção "As estatísticas por tipo", e
`tests/proof_of_concept/test_deltalake.py`, o caso dele. Os achados esperam o usuário em
[`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md).

## O que a pesquisa do filtro do dataset do delta-rs mostrou

Em 2026-09-25, no contêiner da seção anterior, a pesquisa leu o código, as issues, as versões e a
documentação do delta-rs e do PyArrow, e as sondas leram os outros leitores das tabelas e o efeito
de `delta.dataSkippingStatsColumns`, com deltalake 1.6.4 no venv do projeto e, num ambiente à parte
(`uv run --no-project`), deltalake 1.6.6 e Polars 1.44.2, todos com PyArrow 25.0.1.

- **Os leitores que leem certo.** Na tabela da seção anterior com a partição `a` gravada com a
  estatística de `valor` e a `b` sem ela, `valor > 1.5`, `valor IS NULL` e `valor IS NOT NULL`
  leram as certas 2, 3 e 3 linhas pelo `DeltaTable.scan(predicate=...)`, pelo `QueryBuilder`
  (DataFusion) e pelo `scan_delta` do Polars 1.44.2. Com `use_pyarrow=True`, o Polars leu 1 em
  `valor > 1.5` e em `valor IS NULL`, como o dataset, e as 3 certas em `valor IS NOT NULL`, contra
  as 5 do dataset. A 1.6.6 leu os mesmos números numa tabela igual, por todos os leitores.
- **A versão 1.6.6.** O deltalake 1.6.6, publicado em 2026-09-24 e o mais novo no PyPI, montou a
  mesma garantia `(valor >= null[double])` e `(valor <= null[double])` num arquivo de
  `[1.0, 2.0, null, null, null]` gravado com `ColumnProperties(statistics_enabled="NONE")` em
  `valor`: `to_pyarrow_table` e `to_pandas` com `filters=[("valor", ">", 1.5)]` leram 0 linhas, e
  o dataset leu 0 em `IS NULL` e 5 em `IS NOT NULL`, contra os certos 1, 3 e 2; o `QueryBuilder`
  leu 1. A 1.6.4 leu o mesmo. O `filestats_to_expression_next` de `python/src/lib.rs` é idêntico
  nas tags `python-v1.6.4`, `python-v1.6.5` e `python-v1.6.6` e no `main`; nele e nas tags
  `python-v0.24.0` e `python-v0.25.0`, só o valor da partição passa pela conferência do nulo, e o
  mínimo e o máximo não.
- **As issues do delta-rs.** Nenhuma issue aberta trata do limite nulo na garantia do dataset. A
  PR #3210, de 2025-02-12, entrou na 0.25.0: ela limitou a garantia às colunas de
  `delta.dataSkippingStatsColumns` ou às primeiras `delta.dataSkippingNumIndexedCols` e fechou as
  issues #3201 e #3173, das colunas fora das estatísticas. A #3032 ("Filter expressions not being
  applied"), aberta em 2024-11-25, foi fechada em 2025-02-15 com os rótulos `bug` e `mre-needed`.
- **A propriedade e a poda do `delta_scan`.** Numa tabela de dois arquivos, com `valor` em
  `[1, 2]` e em `[10, 20]` e a estatística dele no log, `valor > 5` abriu 1 dos 2 arquivos no
  `delta_scan` antes e depois de `delta.dataSkippingStatsColumns` = `id`, e contou as 2 linhas
  certas: o DuckDB segue podando pelo que o log já guarda. O dataset do delta-rs deixou `valor`
  fora da garantia, `((is_valid(id) and (id >= 3)) and (id <= 4))`, e leu as 2.
- **A escrita depois da propriedade.** O `write_deltalake` seguinte gravou no log o mínimo, o
  máximo e o `nullCount` só de `id`, e o `get_add_actions`, com `flatten=True` e com
  `flatten=False`, passou a mostrar só as estatísticas de `id`, também nos dois arquivos cujo log
  guarda as de `valor`.
- **O protocolo Delta.** O `PROTOCOL.md` de `delta-io/delta`, lido em 2026-09-25, deixa as
  estatísticas opcionais na ação `add`, diz que o mínimo e o máximo de uma coluna toda nula não
  carregam informação e aceita limites largos com `tightBounds` falso: um mínimo menor ou igual a
  todo valor válido do arquivo e um máximo maior ou igual.
- **A versão retirada.** O PyPI marca a 1.6.4 (2026-09-18) e a 1.6.5 (2026-09-21) como retiradas
  com o motivo `Issue: #4784`: o `MERGE` numa tabela com o CDF ligado insere uma linha toda nula
  por linha da origem que o predicado de `when_not_matched_insert` recusa, regressão da 1.6.4 que a
  1.6.5 manteve. A 1.6.6 traz a correção (PR #4785, "fix: cdf merge regression") e não está
  retirada. Nenhum arquivo de `src/`, `tests/`, `scripts/` e `probes/` chama `merge` ou lê o CDF.

**Consequências**: nenhuma versão do delta-rs corrige o defeito, e a proteção fica com o pacote e
com a documentação: [`docs/index.md`](../docs/index.md), seção "Ler a base com o modelo", passou a
listar os leitores que leem certo com filtro e os que perdem linhas. A propriedade
`delta.dataSkippingStatsColumns` protege o dataset sem tirar a poda do `delta_scan` pelas
estatísticas já gravadas, mas o `write_deltalake` deixa de gravar as das colunas de fora (o
`compact`, que escreve pelo delta-rs, não foi medido), e o `get_add_actions` as esconde; pelo código
de `delta.py`, isso tira da releitura (`read_back`) a conferência do log numa chave de fora da
propriedade e da cópia do `archive` (`deep_copy`) as estatísticas das colunas de fora. A escolha
espera o usuário em [`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md), e a troca da versão fixada pela 1.6.6
está na seção seguinte.

## O que a troca do deltalake para a 1.6.6 mostrou

Em 2026-09-25, no contêiner de desenvolvimento (Linux x86_64, 4 vCPUs, Python 3.13.12), com DuckDB
1.5.5 (`httpfs`, `delta` e `aws` em `.duckdb/`), PyArrow 25.0.1 e boto3 1.43.98, sem as variáveis
`AWS_*`, as três sessões do `pytest` sobre `tests/` inteiro rodaram com o deltalake 1.6.4 e depois
com o 1.6.6, a pedido do usuário.

- **As contagens.** Sem variável, 217 passaram e 337 foram pulados; com
  `SERIALIZE_DB_TEST_LOCAL_ROOT`, 458 e 96; com a raiz local e `SERIALIZE_DB_TEST_EMULATOR`, 553 e
  só o teste da Data API pulado. As duas versões deram os mesmos números, sem aviso do pytest, e o
  relatório da terceira sessão leu `delta.client_version` `delta-rs.py-1.6.6`. As suítes de estudo
  de `tests/proof_of_concept/`, `test_deltalake.py` entre elas, rodaram nas três sessões.
- **A interface Python.** Entre os pacotes-fonte das duas versões no PyPI, `deltalake/` muda assim:
  `DeltaTable` ignora `without_files`, `log_buffer_size` e `skip_stats` com `DeprecationWarning`, e
  `table_config` avisa o mesmo; `load_as_version` lê o `datetime` sem fuso como UTC, onde a 1.6.4 o
  lia no fuso local; `is_deltatable` aceita `Path`; `convert_to_deltalake` ganha a estratégia
  `directory` e `collect_stats`; o `encoding` de `default_column_properties` passa a valer no
  `WriterProperties` e desliga o dicionário. Nenhum arquivo de `src/`, `scripts/`, `tests/` e
  `probes/` usa esses argumentos, `load_as_version` com `datetime`, `convert_to_deltalake` ou
  `default_column_properties`.
- **O núcleo em Rust.** A estatística de um `decimal(p, 0)` gravado como `INT64` sai exata no log,
  onde a 1.6.4 passava por `f64` e arredondava acima de 2^53 (PR #4757); nenhum modelo do
  repositório tem coluna `Numeric(p, 0)`. O `vacuum` deixa de apagar arquivos de uma pasta oculta,
  começada por `_` ou `.`, cujo nome só começa pelo de uma coluna de partição: a exceção passa a
  exigir a coluna seguida de `=` (PR #4747), e as colunas de partição do pacote começam por letra. O
  `MERGE` com o change data feed deixa de inserir as linhas nulas da issue #4784 (PR #4785), e o
  pacote não usa nenhum dos dois: a busca por `.merge(`, `enableChangeDataFeed`, `change_data_feed`
  e `load_cdf` em `src/`, `scripts/`, `tests/` e `probes/` não acha nada. O resto reorganiza o
  código (`DeltaTableConfig` removida, o histórico lido em fluxo). `crates/aws` é o mesmo, e
  `object_store` 0.13.2, `arrow` e `parquet` 59.3.0 e `datafusion` 55.1.0 ficaram nas mesmas
  versões, enquanto as crates do SDK da AWS subiram (`aws-runtime` 1.9.4 para 1.10.0, `aws-sigv4`
  1.5.3 para 1.6.0, `aws-smithy-runtime` 1.14.2 para 1.15.0).
- **O PyPI.** A 1.6.6, de 2026-09-24, é a mais nova e não está retirada; traz as mesmas rodas da
  1.6.4, `manylinux_2_17_x86_64` entre elas, e as mesmas dependências (`arro3-core>=0.5.0`,
  `deprecated>=1.2.18` e o extra `pyarrow>=21`).

**Consequências**: `pyproject.toml` fixa `deltalake==1.6.6` nas dependências de execução e no grupo
`dev`, e o item da versão retirada saiu de [`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md). O substituto
dá chaves estáticas ao delta-rs e não passa pela cadeia de credenciais do contêiner, onde as crates
da AWS mudaram: a suíte S3 no ambiente alvo, com a pasta preparada de novo, espera o usuário no
mesmo documento.

## O que a revisão da tabela de tipos mostrou

Em 2026-09-25, no contêiner de desenvolvimento (Linux x86_64, Python 3.13.12, SQLAlchemy 2.0.54,
PyArrow 25.0.1, DuckDB 1.5.5, deltalake 1.6.6), a tabela de mapeamento de tipos de
`docs/index.md` foi lida contra `serialize_db.schema`, os motores e o registro, e sondas na pasta
local leram o que o código faz com cada tipo.

- **As subclasses.** `arrow_type` e `sql_type` buscam o tipo por `isinstance`: `Unicode(20)` e
  `CHAR(2)` saíram `VARCHAR(20)` e `VARCHAR(2)` nos dois motores, `UnicodeText` e `Text(100)`
  saíram `VARCHAR` e `VARCHAR(65535)`, `TIMESTAMP(timezone=True)`, `DATETIME`, `UUID`,
  `DOUBLE_PRECISION`, `NUMERIC` e `DECIMAL` seguiram o tipo de que derivam, e `check_models` não
  listou nenhum. `Float`, `REAL`, `Time`, `Interval`, `LargeBinary`, `ARRAY` e `PickleType` saíram
  como tipo fora do contrato.
- **O `Enum`.** `Enum("a", "bbb")` saiu `string` e `VARCHAR(3)`, sem violação em `check_models`;
  `cast` aceitou `"zzz"`, fora da lista, e recusou `"zzzz"`, acima de `String(3)`. A auditoria
  mede só o comprimento dele.
- **O `Numeric` sem precisão.** `Numeric()` saiu `decimal128(18, 0)` e `DECIMAL(18, 0)` nos dois
  motores, e `Numeric(10)`, `DECIMAL(10, 0)`. `Numeric(39, 2)` levantou o `ValueError` do PyArrow
  ("precision should be between 1 and 38") em `arrow_type` e em `check_models`, que não o listou.
- **O `Uuid`.** Um `uuid.UUID` vira `extension<arrow.uuid>` em `pa.array` e em
  `pa.Table.from_pandas`, e `cast` converte a extensão para `string` pelos 16 bytes: de 200 UUIDs
  pseudoaleatórios, 199 foram recusados com `ContractError: u.c_uuid: Invalid UTF8 payload`, e o
  UUID de bytes nulos entrou como 16 caracteres `\x00`. Um texto de 50 bytes entrou na coluna
  `Uuid`, `VARCHAR(36)` no DDL, porque nem `cast` nem a auditoria medem o `Uuid`.
- **O protocolo.** A tabela com uma coluna `DateTime` nasceu de `delta.create_table` com leitor
  3, escritor 7 e o recurso `timestampNtz`; a tabela com só uma `DateTime(timezone=True)`, com
  leitor 1 e escritor 2.
- **Os tipos no DuckDB.** A view do `ingest` sobre o `delta_scan` leu `String(10)`, `Text`, `Uuid`
  e `JSON` como `VARCHAR` e `DateTime(timezone=True)` como `TIMESTAMP WITH TIME ZONE`; a tabela do
  `loader`, criada pelo DDL do modelo, leu `JSON` como `JSON` e `String(10)` como `VARCHAR`. `->>`,
  `json_extract` e `json_valid` leram o JSON em `VARCHAR`. A saída Arrow das duas trouxe o JSON em
  `string` e o `TIMESTAMPTZ` com o fuso da sessão (`Etc/UTC` no contêiner, `America/Sao_Paulo`
  depois de `SET TimeZone`), que `cast` levou a UTC no mesmo instante.
- **Os tipos físicos.** O delta-rs e o `COPY` do DuckDB gravaram `decimal(9,2)` em `INT32`,
  `decimal(18,2)` em `INT64` e `decimal(20,4)` em `FIXED_LEN_BYTE_ARRAY`; o PyArrow gravou
  `decimal(9,2)` e `decimal(18,2)` em `FIXED_LEN_BYTE_ARRAY`. O `COPY` do DuckDB gravou a coluna
  `JSON` com o tipo lógico `JSON`, e o delta-rs, com `String`.
- **O `Double` no Redshift.** O `redshift_connector` 2.1.17 lê o `FLOAT8` em formato binário
  (`float8_recv`), e o pacote carrega e descarrega em Parquet. O aviso "Loss of floating-point
  precision" da documentação do `UNLOAD` não cita formato, e as suítes do motor, da publicação e do
  leitor gravam `valor` em múltiplos de 0,25, exatos em qualquer formato.

- **A correção do `Uuid`.** Na mesma data, com pandas 3.0.6, o `cast` da `main` aceitou o UUID de
  16 bytes ASCII (`uuid.UUID(bytes=b"abcdefghijklmnop")`) como o texto `abcdefghijklmnop` e um
  texto de 40 bytes na coluna `Uuid`. O PyArrow 25.0.1 não tem função de hexadecimal
  (`pc.list_functions()`), nem `pa.types.is_uuid`; tem `pa.UuidType`, e `cast(pa.binary(16))`
  devolve os 16 bytes de um `Array` e de um `ChunkedArray`. Num milhão de UUIDs, o melhor de três
  deu 3,7 a 4,0 s por `str(uuid.UUID)` e 1,0 a 1,1 s por `bytes.hex` com os hífens, o mesmo texto;
  o `pa.array` do cliente sobre os `uuid.UUID` levou 2,72 s. Uma coluna `UUID` nativa do DuckDB,
  que uma tabela criada por SQL tem (`gen_random_uuid()`), recusou `strlen` e `octet_length`
  ("No function matches the given name and argument types 'strlen(UUID)'") e aceitou
  `strlen(CAST(x AS VARCHAR))`, 36; o `COPY` dela gravou `FIXED_LEN_BYTE_ARRAY` com o tipo lógico
  `UUID`, que o PyArrow leu como `arrow.uuid`, e a saída Arrow de uma consulta a trouxe em
  `string`.
- **A escala do `Numeric`.** `Numeric(10, 12)` e `Numeric(38, -1)` passaram em `check_models`, e o
  `delta_schema` levantou a `Exception` genérica do delta-rs ("Invalid decimal: scale must be in
  range 0..10 inclusive, found: 12." e "Invalid decimal: Negative scales are not supported in
  Delta"); o DuckDB recusou `DECIMAL(10, 12)` ("DECIMAL type scale cannot be greater than width")
  e `DECIMAL(39, 2)` ("DECIMAL type width must be between 1 and 38"). A sonda seguinte, na mesma
  data, leu o `Numeric(-1, 0)` derrubando `check_models` com o `ValueError` do PyArrow
  ("precision should be between 1 and 38"), e o `DECIMAL(38, 38)`, o `DECIMAL(10, 10)` e o
  `DECIMAL(1, 0)` aceitos pelo PyArrow, pelo delta-rs e pelo DuckDB, que recusou o
  `DECIMAL(38, -1)` ("DECIMAL type scale must be between 0 and 38"). A documentação do Redshift
  ("Numeric types") limita a escala à precisão e a 37; o `DECIMAL(38, 38)` fica sem leitura no
  alvo.

**Consequências**: a tabela de `docs/index.md` passou a dizer a regra das subclasses, o padrão do
`Numeric`, os tipos físicos por escritor, o `timestampNtz`, o `INT96` do `UNLOAD`, o `NaN` do
`Double`, o JSON em `VARCHAR` no `delta_scan`, o texto do `Uuid` e o tipo Arrow dos resultados. Por
decisão do usuário de 2026-09-25, o `cast` leva o `arrow.uuid` ao texto canônico por `bytes.hex` e
recusa o texto acima de 36 bytes numa coluna `Uuid`, a auditoria mede o mesmo sobre o `CAST` para
texto, que a coluna `UUID` nativa do DuckDB exige, e `arrow_type` recusa o `Enum` e o `Numeric` de
precisão acima de 38, que `check_models` lista ([`PLAN-STAGE-1.md`](PLAN-STAGE-1.md),
[`PLAN-STAGE-4.md`](PLAN-STAGE-4.md)); `test_schema.py` passou de 51 a 57 casos e `test_audit.py`
de 5 a 6, e os 7 casos novos e o caso mudado de `check_models` falharam no código anterior. Por
outra decisão do usuário da mesma data, `check_models` lista também a escala fora de 0 à precisão;
a regra cobre a precisão de 1 a 38, que tira o `ValueError` do `Numeric(-1, 0)`, e a escala até
37, pela documentação do Redshift. `test_schema.py` passou a 65 casos, e os 5 casos de recusa e o
caso mudado de `check_models` falharam no código anterior.

## O que a troca das versões das dependências mostrou

Em 2026-09-25, no contêiner de desenvolvimento (Linux x86_64, 4 vCPUs e 16 GB, Python 3.13.12), a
API JSON do PyPI tinha versões novas de três dependências fixadas em `pyproject.toml`, fora o
deltalake da seção anterior: boto3 1.43.102 (o pino era 1.43.98), sqlglot 30.19.0 (30.18.0) e
SQLAlchemy 2.1.0 (2.0.54), publicada em 2026-09-24. As outras estavam na última versão: DuckDB 1.5.5
(a 2.0.0 só em pré-lançamentos `dev`), duckdb-engine 0.17.0, PyArrow 25.0.1, sqlalchemy-redshift
1.0.0, redshift-connector 2.1.17, pandas 3.0.6, flask 3.1.3, flask-cors 6.0.5, moto 5.2.3 e pdoc
16.0.0; os limites inferiores resolveram pytest 9.1.1 e ipykernel 7.3.0, e o intervalo do
`uv_build`, a 0.12.19. As extensões do DuckDB 1.5.5 instaladas nesse dia, `aws` efa54a9, `delta`
45c4087 e `httpfs` 827222f, são as que o espaço leu em 2026-09-24 às 01:41. A sessão inteira de
testes, com `SERIALIZE_DB_TEST_EMULATOR` e `SERIALIZE_DB_TEST_LOCAL_ROOT` e sem as variáveis
`AWS_*`, rodou numa cópia do repositório para cada troca, com o deltalake 1.6.4.

- **boto3 e sqlglot.** Com boto3 1.43.102 e sqlglot 30.19.0, 553 casos passaram e 1 foi pulado, o da
  Data API sem `SERIALIZE_DB_REDSHIFT_WORKGROUP`. O botocore, que o boto3 traz sem pino, veio
  1.43.102 também com o pino antigo, porque o `uv.lock` fica fora do git. Com o deltalake 1.6.6 do
  #86 junto, as três sessões no repositório deram os mesmos números: 217 aprovados e 337 pulados sem
  variável, 458 e 96 com a raiz local, e 553 e 1 com o substituto.
- **O `params()` da SQLAlchemy 2.1.** Com a 2.1.0 no lugar da 2.0.54, 8 casos falharam e 545
  passaram. O `params()` dos statements executáveis passou a guardar os valores no statement, em vez
  de copiar cada `bindparam` com o valor: o `required` do `bindparam` segue verdadeiro, e a
  compilação com `literal_binds` escreve o valor como `NULL`, com o `SAWarning` em `=`, `>=` e no
  `IN` expansível e sem ele no `LIKE`. O `stream` do motor Redshift pelo `UNLOAD` (`literal_text`)
  rodou com `IN (NULL)` e leu 0 linhas em `test_stream_literal_values`,
  `test_ingest_stream_loader_export` e `test_stream_literal_values_on_the_target` de
  `tests/test_engine_redshift.py`, e o guarda de `test_stream_by_unload_with_literal_values`, de
  `tests/proof_of_concept/test_redshift.py`, achou o `bindparam` marcado depois do `params()`.
- **O `IN` expansível.** O `construct_params()` de um statement com `bindparam` expansível e
  `params()`, compilado com `render_postcompile`, levantou `InvalidRequestError` ("can't construct
  new parameters when render_postcompile is used") no `query` do motor DuckDB
  (`test_statement_parameters_expand_in_lists`) e em
  `test_in_list_needs_render_postcompile_on_the_engine_path`; numa sonda, o `compiled_for_cursor`
  do motor Redshift levantou o mesmo.
- **O `Double` fora de `Numeric`.** `Float` e `Double` deixaram de derivar de `Numeric`, e o
  `load_report` deixou de somar a coluna `Double` `fator` de `cad_aliquotas`
  (`test_load_report_matches_and_detects_a_difference`), porque `load.py` escolhe as colunas
  somadas por `isinstance(..., sa.Numeric)`.
- **A reflexão do duckdb-engine.** A consulta da reflexão do dialeto PostgreSQL da 2.1 junta
  `pg_catalog.pg_collation`, que o DuckDB não tem, e o `get_columns` do duckdb-engine 0.17.0 falhou
  em `test_create_all_and_reflection`; o pacote não usa reflexão.

**Consequências**: `pyproject.toml` fixa boto3 1.43.102 e sqlglot 30.19.0, e a SQLAlchemy fica em
2.0.54; a adoção da 2.1 espera o usuário em [`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md).

## O que as correções dos achados da revisão mostraram

Em 2026-09-25, na mesma pasta local do contêiner de desenvolvimento (Linux x86_64, 4 vCPUs e
16 GB), com deltalake 1.6.4, DuckDB 1.5.5, PyArrow 25.0.1 e redshift-connector 2.1.17 e sem as
variáveis `AWS_*`, as sondas repetiram os casos dos achados sobre o código corrigido, e cada
asserção nova dos testes reprovou no código de antes.

- **O `nullCount` de uma coluna sem estatística no rodapé, por leitor.** Um timestamp em `INT96`,
  5 linhas com 3 nulos, registrado por `file_from_footer` com o `nullCount` 0 (o código de antes)
  e sem ele (o corrigido). Com o 0, `quando IS NULL` leu 0 linhas pelo dataset do delta-rs, 0 pelo
  `DeltaTable.scan(predicate=...)` e 0 pelo `QueryBuilder`, contra 3 pelo `delta_scan`; sem ele,
  3 pelo `scan`, pelo `QueryBuilder` e pelo `delta_scan`, e 0 pelo dataset, o defeito da
  [issue #85](https://github.com/felipenoris/serialize-db/issues/85). `quando IS NOT NULL` leu 5
  pelo dataset e 2 pelos outros três nos dois casos.
- **Os códigos de saída da CLI.** Os seis casos da sonda anterior, por subprocesso, saíram com 2
  e a mensagem, sem traceback: `run --engine redshift` sem conexão e com o `--execution-id` de 60
  caracteres, `publish --init`, `publish --status` e `audit --engine redshift` sem conexão, e
  `load --source gs://bucket/origem`.
- **As ações de uma tabela particionada sem arquivos** trouxeram 0 linhas e a coluna
  `partition.data_str` também no deltalake 1.6.6, lido num ambiente à parte, como no 1.6.4.
- **As asserções novas no código de antes**: a CLI saiu com o traceback do `ContractError` e do
  `ValueError`; `file_from_footer` deu `null_count` 0 ao `INT96`; o `export_partition` do motor
  DuckDB deixou o arquivo `exec-2026-09-05_<uuid>.parquet` na pasta de `cad_contas`, fora do log, e
  o do motor Redshift rodou três comandos antes da recusa; `channel_snapshot(control, "current")`
  sugeriu `serialize-db channel --name current`; `export_snapshot(mode="rewrite")` gravou fora da
  raiz sem erro; e `publish_redshift` com a tabela fora de `versions` disse que ela não existe
  no ambiente.
- **As decisões de 2026-09-25 no motor Redshift, no código de antes**: `published` devolveu a
  staging `_publicado` sem a versão, e a troca com `expected_rows=999` commitou 1.000 linhas sem
  reprovar.
- **O `unload_to` local do leitor Redshift** serve só à conexão de mentira de
  `tests/test_engine_redshift.py`, que grava o arquivo do `UNLOAD` pelo armazenamento do
  leitor. O substituto de `tests/emulator.py` grava o `UNLOAD` pelo boto3 no moto, e
  `object_uris`, a conferência do destino vazio, levantou com um caminho local
  `ParamValidationError` (`Invalid bucket name ""`) antes de gravar.
- **O caso de estudo do GIL**, `test_gil_reacquisition_waits_the_switch_interval`, reprovou em
  duas sessões da suíte inteira, uma no substituto e outra com a raiz local, e passou nas demais
  do mesmo dia: os 200 `os.stat` levaram 0,011 s e 0,026 s ao lado do laço, e 0,018 s e 0,029 s
  com o intervalo de troca dez vezes menor, e a asserção pede menos da metade do primeiro. Numa
  sessão aprovada leu 0,256 s e 0,016 s, e em dez repetições isoladas, todas aprovadas, de
  0,022 s a 0,482 s e de 0,010 s a 0,024 s, com 0,3 ms a 0,5 ms sozinho.

**Consequências**: `file_from_footer` deixa fora do `nullCount` do log a coluna sem a contagem em
algum grupo de linhas, e as etapas [3](PLAN-STAGE-3.md) e [5](PLAN-STAGE-5.md) dizem isso; o
`scan` e o `QueryBuilder` do delta-rs, que leem certo sem mínimo e máximo, liam errado o `IS NULL`
com o zero. As outras correções estão nas etapas [4](PLAN-STAGE-4.md), [5](PLAN-STAGE-5.md),
[6](PLAN-STAGE-6.md), [7](PLAN-STAGE-7.md), [8](PLAN-STAGE-8.md), [9](PLAN-STAGE-9.md) e
[10](PLAN-STAGE-10.md); a troca relê a partição, e `published` carrega cada versão numa staging
própria ([etapa 5](PLAN-STAGE-5.md)).

## O que a renovação do secret do DuckDB mostrou

Em 2026-09-25, no contêiner de desenvolvimento (Linux x86_64, 4 vCPUs e 16 GB), com DuckDB 1.5.5
(`httpfs` e `delta` de `.duckdb/`), deltalake 1.6.6, PyArrow 25.0.1, boto3 1.43.102 e botocore
1.43.103, as sondas leram o secret do DuckDB para a renovação que o usuário decidiu no mesmo dia, e
um substituto rodou o código anterior e o novo lado a lado.

- **O secret é do banco.** Os cursores de `cursor()` sobre a mesma conexão veem o secret que um
  deles recria, e oito cursores recriando o secret ao mesmo tempo, sem lock, deram `Catalog
  write-write conflict on alter with "serialize_db_s3"` em 1.249 de 1.600 tentativas. O motor
  divide um lock do secret entre as sessões do banco.
- **O `key_id` do secret é legível.** Num secret com a chave explícita (`provider=config`),
  `duckdb_secrets()` mostra o `key_id` sem redação e o `secret` e o `session_token` redigidos, e
  `renew_duckdb_secret` compara esse `key_id` com a chave da credencial do `boto3`.
- **A conferência custa meio milissegundo por sessão.** A entrada de `session()` num motor sobre
  uma raiz S3, com a chave inalterada, levou 485 µs, a leitura de `duckdb_secrets()`, contra
  1,2 µs numa pasta local (melhor de 3 séries de 2.000 entradas).
- **O `CREATE SECRET` aceita parâmetros.** `KEY_ID ?` e `SECRET ?` com os valores passados à parte
  criaram o secret com a chave dada, e um erro de sintaxe no comando repetiu a linha dele com o
  segredo escrito nela (`Parser Error: syntax error at or near "REGION"`): a biblioteca passa a
  chave, o segredo e o token como parâmetros.
- **O botocore troca a chave antes da expiração.** O `RefreshableCredentials` pede uma credencial
  nova quando faltam menos de 900 s e espera por ela quando faltam menos de 600 s: a chave que o
  secret recebe na entrada da sessão tem pelo menos 15 minutos pela frente, ou 10 quando a
  renovação opcional falha.
- **O motor no substituto.** O moto com uma tabela `cad_contas`, um endpoint de credenciais de
  contêiner (`AWS_CONTAINER_CREDENTIALS_FULL_URI`) que troca a chave a cada 40 s, cada uma válida
  por 70 s, e um proxy S3 que responde `400 ExpiredToken` à requisição assinada com a chave
  vencida. Por 200 s, um motor DuckDB com uma view sobre o `delta_scan` e uma conexão crua de
  `Storage.duckdb_connect()`, abertos no início e lidos a cada 10 s: no código anterior, os dois
  falharam a partir de 72 s com `IO Error: DeltaKernel ObjectStoreError (8)`, o secret preso na
  primeira chave, que a cadeia da extensão `aws` leu do endpoint do contêiner; no novo, o motor
  leu em todas as rodadas, com o secret na chave nova em 41 s, 83 s, 123 s e 164 s, e a conexão
  crua, que nada renova, falhou a partir de 72 s. O proxy recusou 39 requisições, todas com a
  primeira chave.
- **A sonda no substituto.** Com um IMDS local servindo as mesmas chaves ao delta-rs, que não lê o
  endpoint do contêiner (`AWS_METADATA_ENDPOINT`), `probes/credentials.py` rodou a cada 12 s até
  45 s depois da expiração da primeira chave, a sonda e o pacote do commit anterior e os de agora
  lado a lado. Na anterior, o `delta_scan` da conexão segurada falhou em 2 das 4 rodadas depois da
  expiração, cada vez que a chave guardada no secret vencia, e o `read_parquet` da mesma rodada
  renovou o secret, como no alvo: `CR-4` reprovou e a saída foi 2. Na de agora, o motor leu nas 10
  rodadas, o secret passou à chave nova na primeira rodada depois da troca, 29 s antes da
  expiração da primeira chave, e a saída foi 0. O delta-rs, o `S3FileSystem` e o `boto3` leram em
  todas as rodadas das duas, e o proxy recusou 12 requisições, com a primeira e a segunda chave.
- **As asserções novas no código mudado.** Sem o lock comum, o caso das quatro sessões concorrentes
  reprovou com o `Catalog write-write conflict`; com a recriação também na entrada reentrante, o
  secret trocou de chave dentro do bloco (`AKIATERCEIRA` no lugar de `AKIASEGUNDA`); sem a
  recriação, os dois casos locais do motor reprovaram; com a entrada da sessão fora do tratamento de
  erro da thread auxiliar, a construção do `stream` esperou sem fim, e o prazo de 10 s do caso a
  pegou; com a chave, o segredo e o token escritos no texto do comando, o caso de
  `renew_duckdb_secret` os achou nele; e sem o fechamento do motor da sonda, a pasta dele ficou na
  pasta temporária do teste.
- **As sessões do pytest**, sem as variáveis `AWS_*`: sem variável, 218 passaram e 343 foram
  pulados; com a raiz local, 463 e 98; com a raiz local e o substituto, 560 passaram e só o teste
  da Data API foi pulado. Os casos novos do secret pedem a extensão `httpfs`, que a esteira do
  GitHub passa a instalar.

**Consequências**: o secret leva a chave da credencial do `boto3`, e o motor DuckDB o recria na
entrada de cada sessão quando ela troca, sob um lock comum às sessões do banco (decisão do usuário
de 2026-09-25). As etapas [3](PLAN-STAGE-3.md), [4](PLAN-STAGE-4.md) e [7](PLAN-STAGE-7.md),
[`PLAN.md`](PLAN.md), [`delta.md`](delta.md) e `docs/index.md` foram revistos; a rodada de
`probes/credentials.py` no alvo fecha o item de [`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md).

## O que a instalação do pacote num projeto cliente mostrou

Em 2026-09-25, no contêiner de desenvolvimento (Linux x86_64, Python 3.13.12, uv 0.8.17), sem as
variáveis `AWS_*`, um projeto cliente criado por `uv init --python 3.13` recebeu o pacote de `main`,
com o driver ainda no extra `redshift`, por `uv add <pasta do repositório>`: o usuário perguntou se
o cliente precisa do grupo `dev` que as instruções de instalação citavam.

- **O que o cliente recebe.** O `uv add` instalou 20 pacotes, o próprio incluído: as dependências
  de execução e as transitivas delas, sem `pandas`, `pytest`, `sqlglot` nem `redshift-connector`.
  Os grupos de dependência (PEP 735) ficam fora dos metadados do pacote, e o `uv` não os instala
  para quem depende dele. `uv add git+https://github.com/felipenoris/serialize-db` instalou os
  mesmos pacotes nas mesmas versões, e um cliente com `requires-python = ">=3.12"` teve a
  resolução recusada, porque o pacote pede `>=3.13`.
- **A interface pública sem o grupo `dev`.** Com o `pytest` acrescentado só ao cliente, os módulos
  de teste que não importam pandas, sqlglot nem o driver rodaram sobre o pacote instalado, com
  `SERIALIZE_DB_TEST_LOCAL_ROOT`: 160 passaram e 40 foram pulados. Os 36 erros vieram do pandas de
  `tests/source_db_projetado.py`, que imita o escritor da base de origem, e as 2 falhas, dos testes
  da linha de comando com `--engine redshift` e `publish --init`, sem o `redshift_connector`. Com
  `serialize-db[redshift]`, 182 passaram e 54 foram pulados, com os mesmos 36 erros e nenhuma
  falha. Em `src/`, `import pandas` e `to_pandas` só aparecem nos exemplos das docstrings.
- **O `uv sync` no repositório.** Numa cópia de `pyproject.toml`, `README.md` e `src/`, `uv sync` e
  `uv sync --group dev` instalaram os mesmos 39 pacotes, porque o `uv` inclui o grupo `dev` por
  padrão, e `uv sync --no-dev` instalou 20.
- **O custo do driver.** Importar `redshift_connector` depois de `serialize_db` levou de 0,034 s a
  0,045 s em três medidas, sem aviso com `-W error`; sozinho, de 0,26 s a 0,31 s, porque o pacote
  já importa o boto3.
- **O driver nas dependências de execução.** Com o `pyproject.toml` alterado, o cliente que
  declara `serialize-db` sem extra recebeu o `redshift-connector` 2.1.17, sem `pandas` e sem
  `sqlglot`, e os mesmos módulos de teste deram 182 aprovados, 54 pulados e os 36 erros do pandas.
  Os passos novos da esteira de testes, `uv sync --no-dev`, a importação de cada módulo e
  `serialize-db --help`, passaram sobre ele e reprovaram sobre o `pyproject.toml` de `main`, com
  `ModuleNotFoundError: No module named 'redshift_connector'`.

**Consequências**: `redshift-connector==2.1.17` passa às dependências de execução e o extra
`redshift` sai, para que o `uv sync` do projeto cliente instale toda a interface pública (decisão do
usuário de 2026-09-25), o que [`PLAN.md`](PLAN.md) e a [etapa 5](PLAN-STAGE-5.md) registram. As
instruções de `docs/index.md` e do `README.md` passam ao `uv sync`, com a instalação no projeto
cliente, e a esteira de testes importa cada módulo depois de `uv sync --no-dev`.

## O que as sondas de consistência de leitura e escrita mostraram

Em 2026-09-25, a pedido do usuário, oito sondas, hoje em `probes/consistencia/`, atravessaram cada
fronteira de leitura e escrita do pacote com valores de borda (o texto vazio, o `NUL`, o emoji, os
50 bytes exatos, `±0.0`, `5e-324`, `±1.7976931348623157e308`, `NaN`, os infinitos, o `Decimal`
extremo de cada escala, as datas `0001-01-01` e `9999-12-31`, o `Numeric(38, 10)`, `Uuid`, JSON e
nulos) e com trabalho paralelo, e compararam valor a valor o que saiu com o que entrou, no contêiner
de 4 CPUs e 16 GB (Linux x86_64, Python 3.13.12, deltalake 1.6.6, duckdb 1.5.5, pyarrow 25.0.1,
pandas 3.0.6), na pasta local e no substituto de `tests/emulator.py`; `src/` não mudou. As sondas
entraram em `probes/consistencia/` no mesmo dia, a pedido do usuário, com os comandos e as variáveis
do ambiente alvo em `SUITE.md` (`probes/README.md` as descreve), e a versão do repositório repetiu
as mesmas leituras na pasta local e no substituto, com o sinal do zero, as estatísticas que
`deep_copy` não registra e as atualizações perdidas do arquivo de controle impressos como leituras
conhecidas, sem reprovar a checagem; a repetição depois do rebase sobre a `main` de 2026-09-25, com
as oito ao mesmo tempo, achou a janela de `publish` entre a conferência da versão fixada e o commit,
que a seção D da sonda da execução reproduz e imprime como leitura conhecida; nenhuma rodou no
ambiente alvo.

- **Os tipos pela fronteira do motor DuckDB** (`probe_types.py`): 2.000 linhas de uma tabela com
  toda coluna do contrato por `load`, `query`, `export_partition` e `publish_partition`, lidas
  pelo dataset do delta-rs, pelo `delta_scan` e pelo `pq.read_table` do arquivo registrado; um
  `stream` de 300 linhas por lote com 10.000 bytes de orçamento (7 lotes no transbordo) num
  `loader` de outra sessão; e o mínimo e o máximo do log iguais aos dados nas colunas de tipo
  exato, com a poda do `delta_scan` e do dataset nos extremos (`valor = hi`, `nome = ''`,
  `nome = 'ÿÿ'`). Tudo igual, fora o sinal do zero: o `COPY ... (FORMAT parquet)` do DuckDB, o
  escritor de `export_partition`, `initial_load`, `rewrite` e `export_snapshot(mode="rewrite")`,
  codifica a coluna `DOUBLE` por dicionário (`PLAIN_DICTIONARY` no rodapé) e trata `-0.0` e
  `0.0` como o mesmo valor: os dois saem com o sinal do primeiro que apareceu (2.000 linhas
  alternadas em ordem de `id` vieram todas `0.0`; em ordem inversa, todas `-0.0`; 100 de 2.000
  na sonda dos tipos e 1.428 por partição na sonda da execução). Com 4 linhas o arquivo sai
  `PLAIN` e guarda o sinal; `COPY ... (DICTIONARY_SIZE_LIMIT 0)` guarda o sinal; o
  `write_deltalake` de `publish_partition` e de `compact`, o `pq.write_table` e o IPC do
  transbordo guardam o sinal. O instante de `DateTime(timezone=True)` volta certo de `query`, de
  `stream`, do `ingest` por `delta_scan` e do leitor Delta, no tipo `timestamp[us, tz=Etc/UTC]`,
  o `TimeZone` da sessão do DuckDB, que `docs/index.md` documenta; numa sessão com
  `SET TimeZone = 'America/Sao_Paulo'` o tipo vem `tz=America/Sao_Paulo` e o valor `09:00-03:00`
  para o instante `12:00+00`, e o arquivo Parquet que o `COPY` grava leva `tz=UTC` nos dois casos.
- **Os streams e loaders em paralelo** (`probe_stream.py`): 1.500.000 linhas de nove colunas; um
  stream lento e ordenado com 30 lotes no transbordo, oito streams ao mesmo tempo na mesma
  conexão (de 0 a 1.500 lotes no transbordo cada), quatro pipelines `stream` para `loader` em
  threads (o conteúdo igual pelo `EXCEPT` nos dois sentidos), um `loader` alimentado por quatro
  threads, um stream depois de outro fechado no meio, um stream ao lado de um que falha
  (`OSError` no que falha, o outro inteiro) e a pasta de transbordo vazia no fim: contagens,
  somas, ordem e esquema dos lotes iguais em todos. A soma de uma coluna `DOUBLE` difere nos
  últimos dígitos entre duas consultas (160714178571.42856 e 160714178571.4286), a ordem da soma
  em ponto flutuante, sem diferença de conteúdo.
- **O ciclo da execução** (`probe_execution.py`): quatro partições de 20.000 linhas com valores
  de borda; na execução `exec-a`, quatro threads, uma por partição, cada uma com o seu `stream`
  da entrada escrevendo em dois `loader` com ids de `next_ids`, `audit` das três tabelas e dois
  `publish` ao mesmo tempo, um das duas tabelas particionadas com `max_workers=2` e outro da
  tabela sem partição, numa thread cada: os ids das duas tabelas saíram exatamente de 1 a
  80.000, sem repetição, e as linhas iguais às sementes pelo delta-rs e pelo leitor Delta (fora o
  sinal do zero), com `serialize_db_execution_id`, `serialize_db_snapshot` e
  `serialize_db_input_versions` (`{"cad_entradas": 4}`, texto JSON) nos quatro commits e a
  entrada do snapshot `t1` com as quatro versões. Um leitor `channel="current"` aberto antes de
  `exec-b`, consultado a cada 50 ms enquanto ela ingeria com `materialize=True`, lia `published`
  (`max` 80.000, `next_ids` a partir de 80.001) e publicava agosto de novo e setembro, leu 80.000
  linhas em toda leitura, antes e depois do commit; `version_diff(4, 6)` deu as duas partições; o
  canal `default` em `t1` seguiu lendo agosto com os ids antigos enquanto `current` lia os
  novos. Duas execuções abertas na mesma versão, `exec-c` e `exec-d`, publicando a mesma partição em
  threads: nas rodadas antes do rebase sobre a `main` de 2026-09-25, uma commitou (versão 7) e a
  outra recebeu `ExecutionConflict` com a mensagem do delta-rs (`a concurrent transaction deleted
  data this operation read`), a partição ficou com um arquivo só, o da vencedora, os ids sem
  repetição, e o arquivo do `COPY` da perdedora ficou na pasta da partição fora do log, o órfão que
  `register_files` documenta e `vacuum(full=True)` lista; nas rodadas depois dele, com as oito
  sondas ao mesmo tempo e sozinha, as duas commitaram em sequência (versões 7 e 8), sem
  `ExecutionConflict`, e o arquivo da segunda substituiu o da primeira, com os dados certos:
  `publish` confere por `version_diff` que nada mudou desde a versão fixada antes do `reconcile` e
  do `export_partition`, e `register_files` abre a tabela de novo, na versão atual, logo antes do
  `create_write_transaction`, de modo que um commit de dados entre a conferência e essa abertura
  escapa aos dois; o resultado depende do intervalo entre o commit de uma e a abertura da tabela
  pela outra, e a sonda imprime a leitura. A seção D reproduz a janela sem depender do tempo:
  `exec-e` com um motor cujo `export_partition` espera um sinal, e `exec-f` publicando setembro
  inteira enquanto ela espera; `exec-e` seguiu e commitou (versão 10 sobre a 9 de `exec-f`), quando
  a docstring de `publish` promete `ExecutionConflict`, com um arquivo só na partição, o dela, os
  dados iguais à projeção e os ids sem repetição. No delta-rs 1.6.6,
  `create_write_transaction(mode="overwrite", partition_filters=...)` sobre um `DeltaTable` aberto
  antes do commit intermediário falhou com `CommitFailedError` depois de um `overwrite` da mesma
  partição (`a concurrent transaction deleted data this operation read`) e de uma mudança de esquema
  por `alter.add_columns` (`Metadata changed since last commit`), e passou depois de uma gravação em
  outra partição, de uma compactação da mesma partição (`optimize.compact`, `numFilesRemoved` 2) e
  de um commit só de metadados (`custom_metadata` sem ações).
- **As rotinas Delta** (`probe_delta_ops.py`): três escritores numa tabela (`publish_partition`,
  o `export_partition` do motor DuckDB e dois `write_deltalake(mode="append")` numa partição de
  dois arquivos), lidos iguais pelo dataset e pelo `delta_scan`; `compact` juntou os dois
  arquivos (`numFilesRemoved` 2, `numFilesAdded` 1), `version_diff` não o contou, o log do
  arquivo novo limita os dados e a coluna `Double` com `NaN` e infinitos saiu sem mínimo e
  máximo pelo escritor do delta-rs (`null_count` 150); `deep_copy` igual à origem pelos dois
  leitores, com o mesmo tamanho, as mesmas linhas e o mesmo `nullCount`, mínimo e máximo por
  arquivo nas colunas de tipo exato, e a repetição sem commit, mas sem o mínimo e o máximo de
  `Boolean` e `DateTime` que o escritor do delta-rs tinha gravado (`flag`, `carimbo`,
  `carimbo_tz`), a regra dos tipos exatos de `register_files`; `export_snapshot` por cópia (cada
  arquivo com o esquema do seu escritor: `doc` em `string` nos do delta-rs e na extensão
  `arrow.json` nos do DuckDB) e por reescrita, iguais; `rewrite` com `texto` renomeada para
  `texto2` igual pelos dois leitores, e a versão anterior legível com o esquema antigo;
  `vacuum_keeping_snapshots` com retenção zero apagou os dois arquivos da compactação e nenhum
  da versão presa pelo snapshot, que seguiu legível; `read_back` devolveu `None` na contagem
  certa e, com 2.999 por 3.000, `RegistrationRefused` com a tabela de volta à versão anterior.
  A escrita condicional do arquivo de controle na pasta local perdeu atualizações entre threads:
  oito threads somando 50 cada por `read_text` e `write_text(if_match=...)`, com nova tentativa
  no `ConflictError`, deixaram 107 de 400 (204 conflitos vistos), porque `_replace_local`
  confere a impressão digital e faz o `os.replace` fora de um lock; a docstring diz que a
  escrita não é atômica entre processos.
- **O leitor Delta sob concorrência** (`probe_reader.py`): 100.000 linhas em quatro partições;
  duas threads de `query`, uma de `stream` e uma de texto com parâmetro leram sem parar enquanto
  `materialize` trocou a view pela tabela inteira, pela parcial de duas partições e de novo,
  três vezes (de 0,32 s a 1,78 s cada): 396 leituras, todas de 100.000 ou de 50.000 linhas com a
  soma dos ids certa, nenhum erro; a materialização parcial igual às sementes; um valor fora da
  regra recusado antes da troca, com a tabela intacta.
- **O motor Redshift no substituto** (`probe_redshift_test.py`, pelo pytest com
  `SERIALIZE_DB_TEST_EMULATOR`): `ingest` e `query`, quatro `stream` ao mesmo tempo com `query`
  ao lado, dois `loader` em threads, `audit` e `export_partition` lidos pelo delta-rs e pelo
  `delta_scan`, duas sessões a mais em threads (uma ingerindo `cad_contas`, outra consultando),
  `publish_redshift` com `max_workers=2` e o leitor publicado consultado e transmitido por duas
  threads: tudo igual, com o documento JSON de volta reserializado (`{"k": 1}` vira `{"k":1}`),
  o `SUPER` do substituto; o alvo também reserializa, por `JSON_SERIALIZE` (a seção "O que a
  segunda execução da suíte Redshift mostrou"), e o espaço em branco lá não foi lido. Pelo motor
  DuckDB o texto do documento volta igual.
- **A carga inicial** (`probe_load.py`): as 12 tabelas da base fictícia de
  `tests/source_db_projetado.py` por `initial_load`, cada uma comparada valor a valor com a
  origem lida por `read_parquet` e levada ao contrato, pelos dois leitores: iguais; a segunda
  passagem sem commit e `load_report` fechando.
- **A fronteira do pandas** (`probe_pandas.py`): um `DataFrame` do pandas 3 (`Int16`, `Int32`,
  `boolean`, `float64`, `Decimal` em `object`, `datetime64[us]`, `datetime64[us, UTC]`, `str`)
  por `pa.Table.from_pandas`, `cast`, `load`, `query` e `to_pandas`: igual, com o `str` em
  `large_string` levado a `string`. O `NaN` de uma coluna `float64` vira nulo em `from_pandas`
  (o `from_pandas=True` do PyArrow), enquanto `pa.array([..., nan], pa.float64())` o guarda;
  `-0.0`, os infinitos e `2.675` voltam iguais. O `to_pandas()` sem `types_mapper` devolve o
  nulo do `Double` como `NaN`, o `Int16` com nulo como `float64` e o `boolean` como `object`;
  com `types_mapper=pd.ArrowDtype`, como a documentação pede, cada nulo é `<NA>` no tipo da
  coluna. As recusas de `cast` responderam como documentadas: nanossegundo não nulo, `Decimal`
  de escala 3 em `Numeric(18, 2)`, `timestamp` sem fuso em coluna com fuso, `1.125` em
  `Numeric(18, 2)` e 51 bytes em `String(50)`; `1.10` em `float` entrou como `1.10`.
- **A soma de controle da auditoria** derruba `audit`, e com ela `publish`, quando uma coluna
  `Double` tem valor finito de magnitude 1e32 ou mais: `CAST(valor AS NUMERIC(38, 6))` deu
  `ConversionException` em `1e32` e em `1.7976931348623157e308` (`9.9999999999999999e31` já
  falha, `1e31` passa), e a soma estourou o `DECIMAL(38, 6)` com `OutOfRangeException` em 20
  linhas de `1e31` e em 20.000 de `1e28` (20.000 de `1e27` passam). A sonda da execução tirou
  esses valores dos dados para seguir. Na base de produção `valor` chega a `±1,18e10`, e a soma
  de 141.901.795 linhas fica em 1e18, longe do teto.

**Consequências**: cinco itens em [`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md), que esperam a decisão do
usuário: o sinal do zero pelo `COPY` do DuckDB, a soma de controle acima de 1e32, a escrita
condicional local entre threads, a nota do `NaN` pelo pandas em `docs/index.md` e a janela de
`publish` entre a conferência da versão fixada e o commit. O fuso da sessão, o órfão da publicação
perdedora e as estatísticas da cópia arquivada já estão documentados; a reserialização do JSON pelo
`SUPER` é uma leitura do substituto.

## O que a comparação do duckdb-sqlalchemy com o duckdb-engine mostrou

Em 2026-09-26, à pergunta do usuário se o `duckdb-sqlalchemy` seria preferível ao `duckdb-engine`,
a leitura do PyPI, dos repositórios e da documentação dos dois pacotes, uma sonda de compilação e
três sessões de teste no contêiner de desenvolvimento (Linux x86_64, 4 vCPUs e 16 GB, Python
3.13.12, DuckDB 1.5.5, deltalake 1.6.6) responderam; `src/` não mudou.

- **Os pacotes.** O `duckdb-sqlalchemy` 1.5.5.9 (2026-09-24) é a bifurcação do `duckdb_engine`
  0.17.0 feita por Leonardo Vida, publicada desde a 0.18.0 de 2025-12-24, com 37 lançamentos, 12
  deles entre 2026-07-30 e 2026-09-24, e a versão acompanha a do DuckDB (a 1.5.5 "Support DuckDB
  and MotherDuck 1.5.5"). O módulo é `duckdb_sqlalchemy`, o dialeto segue registrado como `duckdb`
  (`create_engine("duckdb:///...")`), e a documentação pede para não instalar os dois no mesmo
  ambiente, porque ambos registram esse nome. Exige `sqlalchemy>=2.0.0` e declara `pytz>=2024.2`,
  que nenhum módulo do pacote importa. O repositório tem 17 estrelas, nenhuma bifurcação e 1 issue
  aberta; as notas de lançamento são correções escritas à mão (a reflexão, o `executemany`, as
  tentativas repetidas), a 1.5.4.5 de 2026-07-24 adaptou "SQLAlchemy 2.1 execution-context and
  PostgreSQL reflection API changes", e a matriz de CI declara SQLAlchemy 2.0.0, 2.0.52 e 2.1.0rc2
  e DuckDB 1.3.0 a 1.5.5. O `duckdb-engine` 0.17.0 é de 2025-03-29, o último lançamento; os
  commits seguem até 2025-10-21 e são atualizações de dependência dos bots; o repositório tem 497
  estrelas, 55 issues abertas (a `pg_collation` da reflexão, de 2026-02-23; o `Double`, de
  2026-01-07; o `DuckDBPyType` sem hash com o DuckDB 1.4.0, #1344 de 2025-09-23) e 43 PRs abertos,
  entre eles a correção da `pg_collation` de 2026-03-28, sem resposta do mantenedor. Os downloads
  do mês no pypistats: 1.841.220 do `duckdb-engine` e 8.973 do `duckdb-sqlalchemy`; o guia do
  Jupyter da documentação do DuckDB e a documentação do MotherDuck instalam `duckdb-engine`.
- **A compilação.** Os mesmos statements por `duckdb_engine.Dialect(paramstyle="named")` (no
  `.venv` do projeto) e por `duckdb_sqlalchemy.Dialect(paramstyle="named")` (num venv de rascunho
  com SQLAlchemy 2.0.54): o `CreateTable` com `String`, `DateTime(timezone=True)`,
  `Numeric(18, 2)`, `Double`, `Text`, `Uuid`, `JSON`, `Boolean`, `Date`, `DateTime` e
  `BigInteger`; o `select` com `bindparam`, `LIKE`, `>=` e `IN` expansível, sem `literal_binds`,
  com `literal_binds` e com `render_postcompile` mais `construct_params()`; e o
  `INSERT ... SELECT DISTINCT ... WHERE NOT EXISTS` com `CAST(... AS NUMERIC(18, 2))` saíram
  byte a byte iguais, com `"to"` citado e `timestamp` sem aspas nos dois. O que difere no
  dialeto: `driver` `duckdb_sqlalchemy`, `supports_statement_cache` verdadeiro (falso no
  `duckdb-engine`, que registra "does not support caching" no log do engine) e o
  `DuckDBDDLCompiler`, que escreve `DEFAULT nextval('<tabela>_<coluna>_seq')` na coluna inteira
  de chave primária com `autoincrement` e cria e derruba a sequência por
  `event.listen(sa.Table, "before_create")` e `"after_drop"`, registrados na importação do módulo
  e ativos só numa conexão `duckdb`. O paramstyle padrão segue `pyformat`,
  `supports_native_decimal` falso e o compilador `PGCompiler`.
- **As sessões de teste.** A sessão inteira (`SERIALIZE_DB_TEST_EMULATOR` e
  `SERIALIZE_DB_TEST_LOCAL_ROOT`, sem as variáveis `AWS_*`) rodou na pasta do projeto e em duas
  cópias dele com `duckdb-sqlalchemy==1.5.5.9` no lugar de `duckdb-engine==0.17.0` em
  `pyproject.toml` e `import duckdb_sqlalchemy as duckdb_engine` em `serialize_db.sql`,
  `serialize_db.engine.duckdb` e `tests/proof_of_concept/test_sqlalchemy.py`. Com o
  `duckdb-engine`, 575 aprovados e 1 pulado (sem variáveis, 233 e 343). Com o `duckdb-sqlalchemy`
  e a SQLAlchemy 2.0.54, 574 aprovados, 1 pulado e 1 reprovado, `test_create_all_and_reflection`
  da suíte de estudo, na asserção de que o dialeto não reflete a chave primária: o
  `duckdb-sqlalchemy` devolve `['id_operacao']`; os testes do pacote passaram todos, o `render`,
  o motor DuckDB e os arquivos SQL versionados incluídos. Com o `duckdb-sqlalchemy` e a SQLAlchemy
  2.1.0, 567 aprovados, 1 pulado e 8 reprovados, os mesmos 8 casos de 2026-09-25: 7 pela 2.1 (o
  `params()` que guarda os valores no statement, o `IN` expansível com `render_postcompile` e o
  `Double` fora de `Numeric`) e `test_create_all_and_reflection` na asserção da chave primária, a
  que a reflexão chegou, e não mais na `pg_collation` do `get_columns` do `duckdb-engine` 0.17.0.
  A asserção seguinte desse caso, o `SERIAL` que o DuckDB recusa, não rodou nas duas cópias.

**Consequências**: um item em [`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md), que espera a decisão do
usuário; nada muda em `src/` nem em `pyproject.toml` antes dela.

## O que a bateria de 2026-09-26 às 15:14 mostrou no ambiente alvo

Em 2026-09-26, de 15:14 a 15:47 UTC, o usuário rodou no ambiente alvo o bloco "Probes e Testes -
BN" de `SUITE.md`, os cinco probes e as sete sessões do pytest, numa máquina de 8 vCPUs e
15,3 GiB (Python 3.13.15, DuckDB 1.5.5, deltalake 1.6.6, pyarrow 25.0.1, boto3 1.43.102,
`redshift_connector` 2.1.17, `sa-east-1`). A `main` era a de 2026-09-25 às 22:35 UTC ou
posterior, com a renovação do secret do DuckDB [inferido: as 576 coletas e o `SP-9`, que lista o
`redshift_connector` entre as dependências de execução, só batem com ela]. Os relatórios ficam fora
de `plan/`, com os achados aqui. Nenhum caso falhou, e as leituras repetem as de 2026-09-25 às
17:25, salvo:

- **O secret pela chave do `boto3` passou no alvo.** A sessão `-m "not redshift"` aprovou os 531
  casos em 219,7 s, contra 512 em 225,5 s. Entre os novos está
  `test_delta_scan_reads_after_the_secret_holds_a_stale_key`: com um secret de chave desconhecida
  plantado no motor sobre o bucket da suíte, que tem SSE-KMS, a entrada da sessão seguinte o
  recriou com a chave, o segredo e o token do contêiner como parâmetros do `CREATE SECRET`, e o
  `delta_scan` leu as 3 linhas. Os casos S3 das sessões criaram o secret pela chave do `boto3`,
  sem a extensão `aws`. A troca da chave pelo contêiner durante uma execução fica para
  `probes/credentials.py`.
- **As suítes Redshift, do motor e da publicação** aprovaram 45 casos duas vezes (570,5 s e
  535,5 s), 6 duas vezes (85,3 s e 89,5 s) e 8 duas vezes (183,4 s cada), com as leituras de
  2026-09-25 salvo os tempos, os ids e a slice no nome do arquivo do `UNLOAD`, que muda entre
  execuções ([`redshift.md`](redshift.md)).
- **`RS-12` voltou sem linha.** O `count(*)` de `sys_load_error_detail` dos últimos 30 dias não
  devolveu linha, em 15,1 s, como em 2026-09-23 às 22:49 (12,8 s) e em 2026-09-24 às 23:25; às
  01:41 de 2026-09-24 e em 2026-09-25 contou 25 e 45 erros, o último em 3,3 s. A contagem sem
  linha não quer dizer zero erros: as suítes de 2026-09-25 gravaram outros depois da leitura de 45,
  e a suíte Redshift deste dia leu na visão, por `select ... limit 1`, o erro que o seu caso do
  `VARCHAR` acabara de provocar.
- **`BK-14`** contou 5.269 versões não correntes (159.538.248 bytes) e 4.883 marcadores de exclusão
  sob a raiz das suítes, contra 2.980 (86.695.363 bytes) e 2.788 em 2026-09-25 às 17:26: 2.289
  versões e 72.842.885 bytes entre as duas leituras.

A partir das 15:55 UTC, o usuário rodou o bloco "Migração Parquet -> Delta" de `SUITE.md`, salvo
`probes/duckdb_threads.py`, na raiz de 2026-09-25, carregada de novo: o `history` de
`cad_lancamentos` começa no `CREATE TABLE` das 15:57:46. O relatório JSON da carga veio junto:

- **A base de origem ganhou o mês 2026-07-31.** Desde a carga de 2026-09-25 às 18:16, as quatro
  tabelas particionadas ganharam a partição 2026-07-31: `cad_lancamentos` com 141.933.948 linhas,
  quase as 141.901.795 das quatro partições anteriores juntas, `rel_contrato_operacao` com
  15.209.141, `cad_operacoes` com 5.579.536 e `cad_contratos` com 3.985.447. Das tabelas sem
  partição, `cad_contas` passou de 97 a 101 linhas, `rel_contas_hierarquias` de 89 a 93 e
  `cad_aliquotas` de 15 a 22, com a soma de `fator` de 3,873450 a 3,454950; as outras partições
  repetiram as contagens e as somas de 2026-09-25.
- **A carga** (`started_at` 15:55:29; 8 CPUs, 15.617 MB, e `environment_limits` com 8 threads e
  `memory_limit` de 6.384 MiB, a metade dos 12.768 MB disponíveis) conferiu as 12 tabelas, com
  354.048.596 linhas em 25 partições, contagens e somas iguais em toda partição e os mesmos três
  itens fora do modelo, em 521,1 s somados, contra 219,4 s para 187.340.509 linhas em 21
  partições em 2026-09-25. As partições de antes repetiram os tempos daquele dia (as de
  `cad_lancamentos` em 31,5 s, 20,8 s, 54,9 s e 32,0 s). A 2026-07-31 de `cad_lancamentos` entrou
  em 268,2 s, a 0,53 milhão de linhas por segundo, contra cerca de 1 milhão nas outras quatro:
  com 2,7 vezes as linhas da 2026-03-31, levou 4,9 vezes o tempo. O pico do processo foi a
  9.161 MB, contra 8.647 MB depois da 2026-03-31, acima do limite do DuckDB como nas cargas
  anteriores, e o processo terminou. O `history` mostra o commit dela às 16:03:30, 217 s depois do
  commit da 2026-06-30: o tempo da partição conta também, depois do commit, a releitura de
  `read_back` e a contagem do script [inferido].
- **A auditoria** de `cad_lancamentos` 2026-01-31 com `--foreign-keys`, na versão 5, repetiu os
  989.852 órfãos de `data_base`, `sistema` e `contrato` e deu o total de `valor`
  117.667.407.519,194421, a soma da carga; as outras chaves passaram.
- **`snapshot`, `vacuum` e `archive`**: o snapshot `carga-2026-09-24` gravou as 12 tabelas, com
  `cad_lancamentos` na versão 5; o `vacuum` achou 0 arquivo a apagar em cada tabela; o `archive`
  copiou os 25 arquivos, um por partição, e moveu o snapshot para `archived`, com
  `cad_lancamentos` em 19,1 s e pico de 348 MB, o arquivo da 2026-07-31 em 7,4 s, contra 11,1 s
  para as quatro partições em 2026-09-24 às 23:25.

Em seguida, o usuário rodou os blocos "Publicação Delta -> Redshift", com `probes/credentials.py`
no fim, "Acesso de leitura" e "Exportação e Compact" de `SUITE.md`, na mesma raiz. A saída da
publicação, do leitor, do `export` e do `compact` veio colada na conversa, sem a hora, e o
relatório do probe fica fora de `plan/`:

- **A publicação da base inteira por canal.** `--init` criou a tabela de controle; o snapshot
  `carga-2026-09-25` gravou as 12 tabelas, com `cad_lancamentos` na versão 5, e
  `serialize-db channel` apontou o `default` para ele. `--tables cad_contas --channel default`
  publicou a versão 1 em 3,5 s, com o pico do processo em 247 MB; `--max-workers 4 --channel
  default` pulou `cad_contas` e publicou as outras 11 com todas as partições, as sem partição de
  3,5 s a 4,8 s, `cad_contratos` em 31,2 s, `cad_operacoes` em 53,1 s, `rel_contrato_operacao` em
  56,5 s e `cad_lancamentos`, com as 283.835.743 linhas das cinco partições, em 295,1 s, com o
  pico do processo em 266 MB. Em 2026-09-24 às 23:25, na máquina de 16 vCPUs, as quatro
  partições de então, 141.901.795 linhas, levaram 153,9 s, com o pico em 273 MB: o tempo cresce
  com as linhas, a cerca de 0,96 milhão de linhas por segundo contra 0,92, e o pico não.
  `--status` leu as 12 `prd_<tabela>` com a versão publicada igual à atual e nenhuma partição
  pendente.
- **A volta a um snapshot não trocou nada.** `--channel current` e `--snapshot carga-2026-09-25
  --tables cad_contas` responderam, tabela a tabela, que a versão já estava publicada
  (`cad_contas: a versão 1 já está publicada`): o snapshot do passo 6 de `SUITE.md` está na
  versão atual, e sem um commit depois dele a volta não tem partição para trocar. A volta com
  partições trocadas passou na suíte da publicação, num ambiente `poc<id>`.
- **O leitor Delta e o leitor Redshift**: `db.open_delta()` abriu as 12 views do snapshot
  `carga-2026-09-25` em 0,582 s, com `cad_lancamentos` na versão 5, contra 0,645 s em 2026-09-25
  com a versão 4; a contagem de `cad_contas` deu 101 pelos dois leitores, as linhas da carga do
  dia.
- **`export` de `cad_lancamentos`**, com as cinco partições: por cópia, o padrão, 5 arquivos em
  15,6 s com pico de 258 MB; por `--mode rewrite`, 5 arquivos em 55,0 s com pico de 6.989 MB,
  contra 10,6 s e 34,0 s com 4.343 MB para as quatro partições em 2026-09-25, na máquina de
  8 vCPUs: com o dobro das linhas, a cópia levou 1,5 vez o tempo, e a reescrita 1,6 vez o tempo e
  o pico.
- **`compact --table cad_lancamentos --partitions 2026-03-31`** recusou de novo: `o snapshot
  carga-2026-09-25 está na versão atual 5 de cad_lancamentos; compacte antes de um snapshot`.
- **As credenciais de uma hora** (`probes/credentials.py` sobre `<raiz>/prd/cad_contas`, de
  16:15:59 a 17:19:00, 14 rodadas): nenhuma leitura falhou, e a saída foi 0.
  - **O motor DuckDB leu pelo `delta_scan` depois da expiração.** O secret passou à chave nova às
    16:46:04 e às 17:16:09, 14,3 e 14,6 minutos antes da expiração da chave que guardava
    (`CR-9`), na primeira rodada a menos de 15 minutos dela, a janela em que o botocore renova a
    credencial, e o `delta_scan` da conexão aberta no início leu nas 5 rodadas depois da
    expiração das 17:00:22 (`CR-4`), onde em 2026-09-25 falhou uma vez. É a renovação que o
    usuário decidiu em 2026-09-25, lida no alvo.
  - **O contêiner trocou a chave a cada cerca de 30 minutos**, como em 2026-09-25: a do início
    expirava às 17:00:22, e a cadeia do `boto3` passou a entregar outra, que expira às 17:30:45,
    entre 16:26:01 e 16:31:02, e outra, que expira às 18:00:54, entre 16:56:06 e 17:01:06. Nas
    rodadas, a chave da cadeia teve de 34 a 60 minutos pela frente, dentro dos 29 a 60 minutos da
    leitura anterior.
  - **Os outros clientes seguiram lendo**: a cláusula do `COPY` e do `UNLOAD` seguiu a chave do
    contêiner desde a troca das 16:31:02 (`CR-10`), e a leitura da credencial numa sessão nova do
    `boto3`, o que `credentials_clause` faz a cada chamada, levou menos de 0,05 s (o relatório
    imprime 0,0 s); o delta-rs, o `read_parquet`, o `S3FileSystem` e o `boto3` leram nas 5 rodadas
    depois da expiração (`CR-3`, `CR-5` a `CR-7`); a conexão Redshift respondeu ao `select 1` duas
    vezes depois da expiração da senha, às 17:16:00 (`CR-8`); e os cinco clientes novos leram
    (`CR-11`).

Às 18:30, o usuário rodou o bloco "Sondas de consistência" de `SUITE.md`, a primeira rodada delas
no ambiente alvo: as sete da pasta local, sob `SERIALIZE_DB_TEST_LOCAL_ROOT` na máquina, de
18:30:31 a 18:31:33, e `probe_redshift_test.py` pelo pytest, contra o Redshift e o bucket das
suítes, num ambiente `poc<id>`. Os relatórios ficam fora de `plan/`. Nenhuma checagem reprovou,
e as leituras repetem as da pasta local e do substituto de 2026-09-25 ("O que as sondas de
consistência de leitura e escrita mostraram"), salvo:

- **Os achados conhecidos se repetiram**: o sinal do zero pelo `COPY` do DuckDB (100 de 2.000
  linhas na sonda dos tipos, 1.538 e 1.539 por partição na da execução); a janela de `publish` na
  seção D da sonda da execução, com `exec-e` commitando a versão 9 sobre a 8 de `exec-f`, sem
  `ExecutionConflict`, e o arquivo dela no lugar do de `exec-f`; as estatísticas de `Boolean` e
  `DateTime` que `deep_copy` não registra; e as atualizações perdidas da escrita condicional na
  pasta local, 321 de 400, com 175 conflitos vistos, contra 293 no contêiner de desenvolvimento.
- **A disputa da seção C deu o caminho do conflito**: `exec-c` commitou e `exec-d` recebeu
  `ExecutionConflict` com a mensagem do delta-rs (`a concurrent transaction deleted data this
  operation read`), como nas rodadas locais antes do rebase, e o arquivo do `COPY` da perdedora
  ficou na pasta da partição, fora do log.
- **O leitor Delta sob `materialize`** fez 331 leituras sem erro, com a troca pela tabela inteira
  em 0,41 s a 0,44 s e pela parcial em 0,22 s a 0,25 s, contra 0,32 s a 1,78 s cada no contêiner
  de desenvolvimento.
- **O motor Redshift no alvo** (`probe_redshift_test.py`, 1 caso em 53,6 s): `ingest`, os quatro
  `stream` ao mesmo tempo, os dois `loader`, a auditoria, `export_partition` lido pelo dataset e
  pelo `delta_scan`, com os mesmos tipos nos dois, as sessões a mais e `publish_redshift` com dois
  workers deram os valores das sementes, com o JSON comparado como documento; `cad_contas` e
  `cad_lancamentos_projetados` ficaram publicadas na versão 1 do ambiente `poc<id>`, e a limpeza
  apagou os 26 objetos da sessão no bucket.

**Consequências**: nenhuma leitura contradiz o plano. A máquina de 8 vCPUs e cerca de 16 GB carregou
a partição de 142 milhões de linhas com o pico do processo em 9,2 GB, e o tempo dela, que cresceu
mais que as linhas, entra no dimensionamento da máquina pelas medições que [`PLAN.md`](PLAN.md)
pede. `probes/credentials.py` leu no alvo a renovação do secret do DuckDB, e o item das credenciais
de uma hora em [`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md) fica com o que segue sem medida; a cláusula
montada uma vez por tabela leva uma chave com cerca de 29 minutos ou mais pela frente, contra os
295,1 s da publicação de `cad_lancamentos`, e a leitura da credencial levou menos de 0,05 s. O
usuário, que estima `cad_lancamentos` três vezes maior em poucos meses, decidiu no mesmo dia montar
a cláusula a cada `COPY`, como a [etapa 8](PLAN-STAGE-8.md) passou a fazer. O item do acesso de
leitura fica com a volta a um snapshot anterior sobre a base, que pede um commit depois do snapshot,
e com o `UNLOAD` do cliente só de leitura; o da operação segue esperando uma partição de vários
arquivos e um `compact` antes do snapshot; o das versões não correntes ganha a contagem deste dia.
Os cinco achados das sondas de consistência seguem esperando o usuário, agora lidos também no alvo.
Os arquivos das [etapas 8](PLAN-STAGE-8.md) e [10](PLAN-STAGE-10.md) citam estas leituras.

## O que a sonda do tamanho alvo da compactação mostrou

Em 2026-09-27, numa pasta local do contêiner de desenvolvimento (Linux x86_64, 4 vCPUs e 16 GB),
com deltalake 1.6.6, a sonda gravou numa partição arquivos de 512 a 514 bytes e chamou o
`optimize.compact` com tamanhos alvo em volta da soma deles, para escrever a linha que
`serialize-db compact` imprime quando não há o que juntar.

- **O `optimize.compact` junta numa partição só os arquivos que cabem juntos no tamanho alvo**:
  dois arquivos de 514 e 512 bytes ficaram como estavam com o alvo de 1.025 bytes, dado pelo
  `target_size` e pela propriedade `delta.targetFileSize` da tabela, e viraram um com o alvo de
  1.026; três arquivos de 512, 514 e 514 bytes com o alvo de 1.026 deram um arquivo gravado, dois
  removidos e um pulado (`totalFilesSkipped` 1).
- **Sem nada a juntar, ele não grava nem commita**: a versão da tabela não mudou, e as métricas
  trazem os arquivos lidos em `totalConsideredFiles`, todos pulados: 1 na partição de um arquivo
  só, 2 no par acima do alvo e 0 na partição sem arquivo.

**Consequências**: `serialize-db compact` imprime `nada a juntar em <n> arquivo(s), nenhum commit`
quando o delta-rs não grava nem remove arquivo, com o `totalConsideredFiles` (pedido do usuário de
2026-09-27). A regra do tamanho alvo entrou em `docs/operacao.md`, no
[arquivo da etapa 9](PLAN-STAGE-9.md), em [`delta.md`](delta.md) e na suíte de estudo do delta-rs
(`test_compact_packs_files_up_to_the_target_size`).

## O que a comparação dos probes corrigidos mostrou

Em 2026-09-27, no contêiner de desenvolvimento (Linux x86_64, 4 vCPUs e 16 GB), com boto3
1.43.102, botocore 1.43.103, deltalake 1.6.6, PyArrow 25.0.1, DuckDB 1.5.5 e moto 5.2.3, os probes
de antes da correção e os corrigidos rodaram nos mesmos cenários, sem as variáveis `AWS_*` da
máquina e com uma `HOME` vazia: `diagnose_aws.py` contra o moto, também com um
`AWS_ENDPOINT_URL_STS` inválido (`http://[bad`); `redshift.py` sem região, com
`SERIALIZE_DB_REDSHIFT_HOST` num nome que não resolve (`redshift.example.invalid`); e
`duckdb_threads.py` sobre a base fictícia de `tests/source_db_projetado.py` carregada em Delta, com
`--threads 1 2 --repetitions 1` e com `--metadata nao_existe:Base.metadata`.

- **Os relatórios diferiram só nas linhas corrigidas**, fora o que muda a cada rodada: os tempos,
  com o `threads` mais rápido de `DT-5` e as razões de `DT-6`, os IPs que o DNS devolveu, a
  memória disponível, com o `memory_limit` que o motor tira dela, e o caminho do relatório. No
  `diagnose_aws.py`, a linha do endpoint do resumo passou de
  `a suíte não o passa ao DuckDB (manutenção necessária)` a
  `a suíte o passa ao boto3, ao PyArrow, ao delta-rs e ao secret do DuckDB (sem manutenção)`. Com o
  `AWS_ENDPOINT_URL_STS` inválido, o botocore levanta `ValueError: Invalid IPv6 URL` antes de a
  chamada sair, e o resumo, que dava o STS como `sem resposta` e pedia manutenção, diz
  `erro local, antes de a chamada sair`. No `redshift.py`, o `RS-14`, que sem região julgava o
  host do Redshift como endpoint de API (`sem endpoint VPC: redshift.example.invalid`), diz
  `sem região, os endpoints das APIs não foram resolvidos: defina AWS_DEFAULT_REGION`. No
  `duckdb_threads.py`, o `--metadata` que não importa saía com traceback e código 1 e sai com o
  erro de uso do argparse e código 2
  (`argument --metadata: nao_existe:Base.metadata: No module named 'nao_existe'`), como o mesmo
  argumento de `scripts/migrate_parquet_to_delta.py`, que também saía com traceback.
- **Os helpers do `probelib.py` no `diagnose_aws.py`** mudaram uma leitura: uma credencial em
  `PRESENCE_VARIABLES` definida como texto vazio saía `(ausente)` e sai `(vazia)`.
- **As leituras que pedem conexão ou o alvo não rodaram aqui**: o rótulo de `pg_settings` com
  `wlm_query_slot_count`, uma seção do `duckdb_threads.py` interrompida por erro e a expiração da
  credencial temporária. Os 7 casos novos de `tests/test_probes.py` cobrem, com respostas
  fabricadas, o veredito do STS, a linha do endpoint, a variável vazia, os endpoints que o `RS-14`
  julga, a expiração, o `--metadata` e a seção interrompida, e reprovaram no código de antes.
- **`AWS_ENDPOINT_URL_S3` sozinha**, apontada ao moto, chegou ao `boto3`, que listou o bucket, e
  ao delta-rs, cujo `DeltaTable.is_deltatable` achou a tabela, os dois lendo a variável do
  ambiente; o `S3FileSystem` do PyArrow foi a outro endpoint, e a `HeadObject` voltou
  `ACCESS_DENIED`, que o moto, sem conferir a chave, não devolve. O secret do DuckDB da suíte recebe
  o endpoint só de `AWS_ENDPOINT_URL` (`_endpoint` de `serialize_db.storage`, lido no código).
- **A expiração da credencial do botocore** fica no atributo privado `_expiry_time` da
  `RefreshableCredentials` (botocore 1.43.103), e a issue que pede um campo público está aberta
  desde 2022-06-13 (<https://github.com/boto/botocore/issues/2694>). A credencial das variáveis
  `AWS_*` só vira `RefreshableCredentials` com `AWS_CREDENTIAL_EXPIRATION` (`EnvProvider`).

**Consequências**: a leitura do atributo privado ficou num só lugar, `credential_expiry` de
`probes/probelib.py`, que `space.py`, `redshift.py` e `credentials.py` chamam. O cabeçalho do
`diagnose_aws.py` diz quais clientes leem `AWS_ENDPOINT_URL_S3`. A bateria de 2026-09-27 comparou
no alvo os relatórios com os da bateria de 2026-09-26 (seção seguinte).

## O que a bateria de 2026-09-27 às 15:58 mostrou no ambiente alvo

Em 2026-09-27, de 15:58 a 16:29 UTC, o usuário rodou no ambiente alvo o bloco "Probes e Testes - BN"
de `SUITE.md`, os cinco probes e as sete sessões do pytest, numa máquina de 8 vCPUs e 15.617 MB,
como a de 2026-09-26 (Python 3.13.15, DuckDB 1.5.5, deltalake 1.6.6, pyarrow 25.0.1, boto3 1.43.102,
`redshift_connector` 2.1.17, `sa-east-1`). A `main` era a de 2026-09-27 às 15:47 UTC, com os probes
corrigidos [inferido: as 541 coletas da sessão S3 são as dela]. Na mesma máquina vieram depois os
blocos da migração, da publicação, do acesso de leitura e da exportação, as sondas de consistência,
`probes/credentials.py` e `probes/duckdb_threads.py`. Os relatórios ficam fora de `plan/`, com os
achados aqui. Nenhum caso e nenhuma checagem falharam, e as leituras repetem as de 2026-09-26 ("O
que a bateria de 2026-09-26 às 15:14 mostrou no ambiente alvo"), salvo:

- **Os probes corrigidos leram no alvo o que leram na pasta local.** Cada relatório dos cinco
  diferiu do de 2026-09-26 só no que muda a cada rodada (a hora, os tempos, os IPs e a quantidade de
  endereços que o DNS devolveu, a expiração da credencial, as contagens do bucket) e nas linhas
  corrigidas ("O que a comparação dos probes corrigidos mostrou"); as chamadas que falharam, que dão
  o código de saída 1, são as de 2026-09-26: o módulo `sagemaker_studio` fora do ambiente virtual no
  `space.py`, as 6 negadas no `bucket.py` e, no `catalog.py`, a do Athena, negada, e as do Lake
  Formation e do S3 Tables, sem resposta. No `redshift.py`, o rótulo de `pg_settings` nomeia
  `wlm_query_slot_count`, lido com o valor 1 como antes, e o `RS-14`, com a região que o bloco
  define, repetiu os endpoints VPC de interface. `credential_expiry` leu a expiração da credencial
  do contêiner no `CREDENTIAL_EXPIRY` do `space.py` (16:29:21, 30 minutos à frente), no `RS-18` do
  `redshift.py` e no `CR-1` do `credentials.py`, no formato de antes. O resumo do `diagnose_aws.py`
  repetiu `STS: respondeu (sem manutenção)` e a linha do endpoint sem `AWS_ENDPOINT_URL`, os ramos
  que o alvo exercita; o erro local do STS e o endpoint definido ficam com os casos de
  `tests/test_probes.py`. O `catalog.py` esperou 30,1 s pelo Lake Formation, contra 60,1 s: o DNS
  devolveu 3 endereços, contra mais de 4, e cada um custa os 5 s do `connect_timeout` de
  `short_config()` nas duas tentativas, como os 30,5 s do S3 Tables sobre 3 endereços [inferido].
- **As suítes.** A sessão `-m "not redshift"` aprovou 541 casos em 226,7 s, contra 531 em 219,7 s:
  os casos a mais são os dos PRs #97 e #98, a cláusula de credenciais a cada `COPY`, a
  `tabela inteira` na saída da carga, o tamanho alvo do `compact` e os probes. As suítes Redshift,
  do motor e da publicação aprovaram 45 casos duas vezes (516,9 s e 480,8 s), 6 duas vezes (83,1 s e
  78,6 s) e 8 duas vezes (174,5 s e 177,3 s), com as leituras de 2026-09-26 salvo os tempos, os ids
  e os nomes dos arquivos nos manifestos. A publicação com a cláusula montada a cada `COPY`
  ([etapa 8](PLAN-STAGE-8.md)) passou assim no Redshift do alvo, nas duas rodadas da suíte e na
  publicação da base inteira abaixo.
- **`BK-14`** contou 7.619 versões não correntes (233.165.927 bytes) e 7.030 marcadores de exclusão
  sob a raiz das suítes, contra 5.269 (159.538.248 bytes) e 4.883 em 2026-09-26: 2.350 versões e
  73.627.679 bytes entre as duas leituras.

A partir das 16:34 UTC, o usuário rodou a carga, a auditoria, `history`, `snapshot`, `vacuum` e
`archive` do bloco "Migração Parquet -> Delta" de `SUITE.md`, na raiz de 2026-09-25, carregada de
novo: o `history` de `cad_lancamentos` começa no `CREATE TABLE` das 16:36:35.

- **A base de origem não mudou desde 2026-09-26.** A carga (`started_at` 16:34:17; 8 CPUs,
  15.617 MB, e `environment_limits` com 8 threads e `memory_limit` de 6.273 MiB, a metade dos
  12.547 MB disponíveis) conferiu as 12 tabelas com as mesmas 354.048.596 linhas em 25 partições, as
  mesmas contagens e somas em cada partição e os mesmos três itens fora do modelo, em 496,8 s
  somados, contra 521,1 s. As partições de `cad_lancamentos` levaram 30,6 s, 21,2 s, 54,7 s, 30,7 s
  e 245,0 s, a 2026-07-31 a 0,58 milhão de linhas por segundo, contra 268,2 s; o pico do processo
  chegou a 8.625 MB na 2026-03-31 e não subiu na 2026-07-31, que em 2026-09-26 o levou a 9.161 MB. A
  saída chama as tabelas sem partição de `tabela inteira`, a correção de 2026-09-27.
- **A auditoria** de `cad_lancamentos` 2026-01-31 com `--foreign-keys`, na versão 5, repetiu os
  989.852 órfãos de `data_base`, `sistema` e `contrato` e o total de `valor` 117.667.407.519,194421.
- **`snapshot`, `vacuum` e `archive`** repetiram 2026-09-26: o snapshot `carga-2026-09-24` com as 12
  tabelas, 0 arquivo a apagar em cada uma e os 25 arquivos copiados, com `cad_lancamentos` em 16,8 s
  e pico de 350 MB, o arquivo da 2026-07-31 em 5,9 s, contra 19,1 s e 7,4 s.

Em seguida, o usuário rodou os blocos "Publicação Delta -> Redshift", "Acesso de leitura" e
"Exportação e Compact" de `SUITE.md`, na mesma raiz, com a saída colada na conversa, sem a hora:

- **A publicação da base inteira por canal** repetiu o fluxo de 2026-09-26: `--init` criou a tabela
  de controle; `--tables cad_contas --channel default` publicou a versão 1 em 3,4 s, com o pico do
  processo em 249 MB; `--max-workers 4 --channel default` publicou as outras 11, as sem partição de
  3,4 s a 4,0 s, `cad_contratos` em 37,9 s, `cad_operacoes` em 60,5 s, `rel_contrato_operacao` em
  66,1 s e `cad_lancamentos` em 328,5 s, com o pico em 270 MB, contra 31,2 s, 53,1 s, 56,5 s e
  295,1 s: de 11% a 21% mais por tabela particionada, com a causa não medida. A cláusula a cada
  `COPY` não a explica sozinha: `cad_lancamentos` faz cinco `COPY`, e a leitura da credencial levou
  menos de 0,05 s em 2026-09-26 [inferido]. `--status` leu as 12 `prd_<tabela>` com a versão
  publicada igual à atual e nenhuma partição pendente, e `--channel current` e
  `--snapshot carga-2026-09-25 --tables cad_contas` responderam que cada versão já estava publicada.
- **O leitor Delta** abriu as 12 views do snapshot `carga-2026-09-25` em 0,571 s, e a contagem de
  `cad_contas` deu 101 pelos dois leitores.
- **`export` de `cad_lancamentos`**: por cópia, 5 arquivos em 14,7 s com pico de 261 MB; por
  `--mode rewrite`, 5 arquivos em 56,7 s com pico de 6.938 MB, contra 15,6 s e 55,0 s com 6.989 MB.
  O `compact` da 2026-03-31 recusou de novo pelo snapshot na versão atual 5.

Das 17:33:03 às 17:34:59, o usuário rodou o bloco "Sondas de consistência" de `SUITE.md`. Nenhuma
checagem reprovou, e os achados conhecidos se repetiram, com a variação de uma disputa:

- **O sinal do zero** pelo `COPY` do DuckDB deu 100 de 2.000 linhas na sonda dos tipos e 1.538 e
  1.539 por partição na da execução; **a janela de `publish`** da seção D da sonda da execução
  repetiu `exec-e` commitando a versão 9 sobre a 8 de `exec-f`, sem `ExecutionConflict`; `deep_copy`
  seguiu sem as estatísticas de `Boolean` e `DateTime`; e a escrita condicional na pasta local
  perdeu 312 de 400 atualizações, com 204 conflitos vistos, contra 321 e 175.
- **A disputa da seção C** deu o mesmo caminho com o vencedor trocado: `exec-d` commitou, `exec-c`
  recebeu `ExecutionConflict` com a mensagem do delta-rs, e o arquivo dela ficou na pasta da
  partição, fora do log.
- **O leitor Delta sob `materialize`** fez 352 leituras sem erro, com a troca pela tabela inteira em
  0,40 s a 0,46 s e pela parcial em 0,23 s a 0,25 s; **o motor Redshift** da sonda
  (`probe_redshift_test.py`) passou em 49,9 s, e a limpeza apagou os 26 objetos da sessão.

Das 17:35:23 às 18:38:25, `probes/credentials.py` leu `<raiz>/prd/cad_contas` em 14 rodadas, e
nenhuma leitura falhou:

- **O motor DuckDB leu pelo `delta_scan` depois da expiração.** O secret passou à chave nova às
  18:20:31, 10,2 minutos antes da expiração das 18:30:43 da chave que guardava (`CR-9`), na primeira
  rodada dentro dos 15 minutos em que o botocore renova a credencial (a das 18:15:30 estava a
  15,2 minutos), e o `delta_scan` leu nas 2 rodadas depois da expiração (`CR-4`).
- **O contêiner trocou a chave a cada cerca de 30 minutos**: a cadeia do `boto3` passou a entregar a
  chave que expira às 19:01:13 entre 18:00:28 e 18:05:29, e a que expira às 19:31:50 entre 18:30:33
  e 18:35:34, e nas rodadas a chave da cadeia teve de 30 a 56 minutos pela frente. As quatro trocas
  lidas em 2026-09-26 e neste dia cabem todas numa troca a cerca de 30 minutos da expiração da chave
  anterior [inferido].
- **Os outros clientes seguiram lendo**: a cláusula do `COPY` e do `UNLOAD` seguiu a chave do
  contêiner desde as 18:05:29 (`CR-10`); o delta-rs, o `read_parquet`, o `S3FileSystem` e o `boto3`
  leram depois da expiração (`CR-3`, `CR-5` a `CR-7`); a conexão Redshift respondeu ao `select 1`
  duas vezes depois da expiração da senha, às 18:35:24 (`CR-8`); e os cinco clientes novos leram
  (`CR-11`).

A partir das 17:35:54, ao lado de `probes/credentials.py`, cujas leituras de `cad_contas` a cada
5 minutos são pequenas diante da partição medida [inferido], `probes/duckdb_threads.py` rodou
inteiro pela primeira vez no alvo desde 2026-09-24, sobre a partição 2026-07-31 (`cad_lancamentos`
com 141.933.948 linhas e 2.331 MB num arquivo; as quatro tabelas com 166.708.072 linhas), com 4, 8,
16, 24, 32 e 40 threads, a razão sobre as 8 do padrão do motor, o cache de arquivos externos
desligado e a melhor de três repetições, numa máquina de 8 vCPUs com duas threads por núcleo físico:

- **A materialização** levou 52,660 s com 4 threads, 37,215 s com 8, 34,192 s com 16, 35,446 s com
  24, 37,838 s com 32 e 36,448 s com 40, com o pico do processo de 3.341 MB a 6.362 MB (3.879 MB com
  8 e 4.937 MB com 16). As três repetições com 16 threads foram mais rápidas que as três com 8, a
  mais lenta delas por 0,007 s: a melhor ganhou 9%, e a mediana 5% (35,781 s contra 37,678 s).
- **A leitura agregada do S3** levou 15,271 s com 4 threads, 7,666 s com 8, 4,628 s com 16, 4,020 s
  com 24 (1,91 vez mais rápida que com 8), 4,036 s com 32 e 4,076 s com 40.
- **As quatro tabelas** foram mais rápidas em série com 8 threads (48,998 s, contra 51,417 s com 16
  e até 62,209 s com 40) e em sessões a mais, como `run.ingest`, com 16 (39,143 s, 1,08 vez mais
  rápido que os 42,119 s com 8, com o pico de 5.326 MB contra 4.584 MB; pela repetição mediana,
  40,853 s contra 42,188 s). As sessões a mais ganharam da série de 1,09 vez com 4 threads a
  1,44 vez com 40 (1,16 vez com 8), contra 1,89 vez com 16 em 2026-09-24, quando as quatro tabelas
  da partição 2026-06-30 somavam 51.238.647 linhas; aqui `cad_lancamentos` tem 85% das linhas
  [inferido: a tabela maior limita o ganho das sessões a mais].

**Consequências**: as correções dos probes de 2026-09-27 leram no alvo o que leram na pasta local, e
o item dos probes sai de [`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md), com a resposta nesta seção. A
cláusula de credenciais a cada `COPY` da [etapa 8](PLAN-STAGE-8.md) passou no Redshift do alvo. O
padrão de `threads` segue nas CPUs do processo (instrução do usuário de 2026-09-24): com 16 vCPUs a
metade e o dobro perderam na materialização, e com 8 vCPUs, numa partição quatro vezes maior, o
dobro ganhou de 5% a 9% numa tabela e de 3% a 8% nas sessões a mais, com o pico de 0,7 GB a 1,1 GB
maior, e perdeu 5% com as quatro tabelas em série; [`PLAN.md`](PLAN.md) e a
[etapa 4](PLAN-STAGE-4.md) guardam as duas leituras. O item das versões não correntes ganha a
contagem deste dia; o do acesso de leitura segue com a volta a um snapshot anterior sobre a base e o
`UNLOAD` do cliente só de leitura; o da operação segue esperando uma partição de vários arquivos e
um `compact` antes do snapshot. Os cinco achados das sondas de consistência seguem esperando o
usuário.

## O que a sonda do fim da execução mostrou

Em 2026-09-27, numa pasta local do contêiner de desenvolvimento (Linux x86_64, 4 vCPUs e 16 GB),
com DuckDB 1.5.5, deltalake 1.6.6 e o `TMPDIR` apontado para a pasta da sonda, a sonda abriu
`Execution(db, "duckdb", ...)` sem `with`, com `__enter__` e `__exit__` chamados à mão, para
responder ao usuário o que o fim do bloco libera.

- **A entrada cria o sandbox numa pasta `serialize_db_*` de `tempfile.gettempdir()`**, com
  `<execution_id>.duckdb`, o `.wal` e a pasta `<execution_id>_transbordo`; antes do `__enter__`,
  `run.sandbox` é `None` e `run.versions` está vazio.
- **`__exit__(None, None, None)` apaga a pasta e grava o snapshot marcado**; o `with` que termina
  em erro e o `__exit__` que recebe a exceção apagam a pasta sem gravar o snapshot;
  `run.sandbox.cleanup()` sozinho apaga a pasta sem gravar o snapshot, e o `__exit__` seguinte o
  grava. Depois do fim, `run.versions` guarda as versões finais.
- **Sem `__exit__`, nada libera o sandbox**: depois de `del run` e `gc.collect()`, o
  `exec-b.duckdb` e a pasta de transbordo continuaram na pasta temporária, porque os motores não têm
  finalizador; o leitor tem (`weakref.finalize` em `serialize_db.reader`). No Redshift, o `cleanup`
  apaga as tabelas `exec_<id>_*` que a execução criou e os arquivos do `staging/` dela (lido no
  código, sem rodar).
- **O exemplo novo da seção do DuckDB do tutorial** rodou como está sobre uma raiz local, com
  `run.sandbox` um `DuckDBEngine`, e a pasta temporária sumiu no fim do bloco.

**Consequências**: o tutorial de `docs/index.md` usa os motores por `run.sandbox`, dentro de um
`Execution`, e diz o que o fim da execução libera em cada motor (pedido do usuário de 2026-09-27).

## O que a sonda da tabela de domínio mostrou

Em 2026-09-27, na mesma pasta local do contêiner de desenvolvimento, com DuckDB 1.5.5 e deltalake
1.6.6, a sonda rodou o pipeline que só atualiza uma tabela sem partição, `dom_moedas` (chave
`id_moeda`, `sigla`, `nome`), para responder ao usuário como usar a biblioteca nesse caso, e repetiu
os exemplos da seção nova do tutorial com a partição opcional.

- **Na primeira carga a tabela não existe**: `run.ingest` a recusa com `SandboxError` (`a tabela
  não existe no ambiente prd`), e `run.published` também (`sem versão publicada, a tabela ainda não
  existe`); `run.next_ids` começa em 1, `run.sandbox.load`, `run.audit(table, None)` e
  `run.publish(table)` gravaram a versão 1, depois da criação na versão 0.
- **A atualização pelo sandbox substitui a tabela inteira**: `run.ingest(table, materialize=True)`,
  `sa.update`, `sa.insert` e `sa.delete` por `run.sandbox.query` (o `sa.update` devolveu a
  contagem de linhas numa coluna `Count`) e `run.publish(table)` gravaram a tabela inteira numa
  versão nova; a reescrita por `run.published`, `query` e `load` também. Sem `materialize`, o `UPDATE` na view
  falhou com `BinderException: Binder Error: Can only update base table`.
- **A auditoria da tabela de domínio não vê quem a referencia**: com a moeda 2 apagada no sandbox e
  uma operação que a aponta, `run.audit(dom_moedas, None, foreign_keys=True)` aprovou (`linhas`,
  `chave_id_moeda`), e a auditoria da tabela das operações com `foreign_keys=True` reprovou em
  `orfao_id_moeda`, procurando a moeda na tabela do sandbox.
- **A execução sem partição**: `Execution(db, "duckdb")` abriu com `run.partition` `None`,
  `run.previous_partitions` levantou `ContractError`, uma tabela particionada foi publicada com
  `partitions` explícito, e `partitions=[run.partition]` levantou `ContractError` (`valor None fora
  da regra da partição`) antes de qualquer escrita. A chave repetida reprovou a auditoria em
  `chave_id_moeda`, sem tocar o Delta.

**Consequências**: a partição do `Execution` e o `--partition` de `serialize-db run` ficaram
opcionais (decisão do usuário de 2026-09-27), e o tutorial ganhou a seção "Atualizar uma tabela de
domínio sem partição"; revisados `plan/PLAN.md` e `plan/PLAN-STAGE-6.md`.

## O que o teste do pipeline completo mostrou

Em 2026-09-27, na pasta local do contêiner de desenvolvimento (Linux x86_64, DuckDB 1.5.5 com
`threads` 4 e `memory_limit` 6,4 GiB lidos do ambiente, deltalake 1.6.6, pyarrow 25.0.1, SQLAlchemy
2.0.54, Python 3.13.12), `tests/test_pipeline.py` rodou a execução mensal completa no motor DuckDB
sobre a base fictícia de `tests/source_db_projetado.py` carregada no Delta com o modelo cliente: a
partição 2026-07-31 sobre a última data-base, 2026-06-30, pela API e por `serialize-db run`, e em
seguida a de 2026-08-31 sobre a partição recém-publicada.

- **A carga da base fictícia**: `initial_load` gravou as 12 tabelas, as quatro partições de cada
  particionada, em 4,8 s, com uma conexão DuckDB por tabela.
- **Os tempos da execução**: `abertura` 0,133 s, `ingest` 0,036 s (11 tabelas, as particionadas
  só na última data-base e materializadas), `audit` 0,287 s (as quatro tabelas gravadas, com
  `foreign_keys=True`) e `publish_delta` 0,684 s (uma versão por tabela); a mesma execução por
  `serialize-db run` levou 0,16 s, 0,06 s, 0,211 s e 0,684 s, e a do mês seguinte 0,141 s,
  0,039 s, 0,209 s e 0,697 s. Os dois testes levam 9,9 s.
- **A geração por `INSERT ... SELECT`**: `cad_operacoes`, `cad_contratos` e `rel_contrato_operacao`
  já existem no sandbox, materializadas pela ingestão, e a partição nova entra em Core
  (`insert().from_select`) pela sessão da execução, com a chave `row_number() OVER (ORDER BY chave)
  + (faixa.start - 1)` sobre a faixa de `next_ids`, e a data-base e a coluna de partição como
  `sa.literal`; `run.sandbox.query` devolve a contagem do DuckDB.
- **A geração em pyarrow**: um nome no sandbox tem um só dono, e `run.sandbox.load` recusa o que o
  `ingest` ocupou; por isso `cad_lancamentos` fica fora da ingestão, a última partição vem de
  `run.pinned_delta` como `pa.Table`, o filtro dos meses posteriores e as colunas novas (a chave
  pela faixa de `next_ids`, a data-base, a partição e o carimbo) são pyarrow, e `run.sandbox.load`
  cria a tabela do sandbox só com a partição nova (pedido do usuário de 2026-09-27, a geração dos
  lançamentos em Python com pyarrow). `saldos_por_conta` roda depois da carga, sobre a partição
  nova.
- **A auditoria dispensou a chave sequencial da junção**: em cada tabela, `chave_<chave>_tabela`
  saiu aprovada com o motivo "dispensada", porque o menor id novo passa do maior da versão fixada;
  as chaves estrangeiras compostas `orfao_data_operacao` e `orfao_data_base_sistema_contrato`
  rodaram, e `not_run` ficou vazio nas quatro.
- **O commit**: `version_diff` entre a versão fixada e a nova lista só a partição nova em cada
  tabela, e o commit carrega `serialize_db_execution_id` e `serialize_db_input_versions` com as 12
  versões lidas, sem `serialize_db_snapshot`.
- **O leitor Delta e a execução seguinte**: as quatro partições antigas mantiveram as contagens da
  carga; a execução de 2026-08-31 leu 2026-07-31 como a última data-base e gerou um mês projetado a
  menos nos lançamentos.

**Consequências**: `tests/test_pipeline.py` entra nos testes do pacote, com dois casos `local`;
`plan/PLAN-STAGE-6.md` descreve o teste e `plan/CURRENT_STATE.md` as contagens. A adaptação do
teste ao S3 e ao Redshift fica com o usuário (mensagem de 2026-09-27).

## O que a sonda de dois escritores na mesma tabela mostrou

Em 2026-09-28, a proposta do usuário de trocar `load` e `loader` por `create_table`, `append` e
`appender` ([`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md)) abriu duas perguntas que a regra de um só dono
por nome dispensava: o que acontece com dois escritores na mesma tabela do sandbox, e se um
`CREATE TABLE` pedido a uma sessão a mais espera a consulta de um `stream` aberto na principal, como
o pedido à principal espera (leitura de 2026-09-23, acima).
`probes/consistencia/probe_append_test.py` fazia, nessa rodada, o que o `appender` proposto faria,
sem ele: no DuckDB, o leitor Arrow registrado com nome único e um `INSERT ... BY NAME`; no
Redshift, o Parquet no `staging/` e o `COPY`, pela staging temporária com `JSON_PARSE`, porque a
tabela tem coluna JSON; desde a implementação do mesmo dia ela chama `create_table` e `append` dos
motores. Rodou três vezes no DuckDB deste contêiner
(Linux, 4 vCPUs, 16.095 MB, Python 3.13.12, DuckDB 1.5.5, PyArrow 25.0.1) e uma vez no substituto
de `tests/emulator.py`, com 200.000 linhas por escritor no DuckDB e 20.000 no substituto; a rodada
no Redshift do ambiente alvo é do usuário.

- **Dois `INSERT ... BY NAME` na mesma tabela, em duas sessões a mais, ao mesmo tempo**: nas 15
  rodadas das três execuções os dois entraram, em 0,25 s a 0,48 s cada, e a tabela ficou com as
  400.000 linhas, sem id repetido e com a soma de `valor` das duas escritas. Os dois na sessão
  principal, em duas threads, também entraram (0,32 s a 0,84 s), em série sob o lock.
- **Um `INSERT ... BY NAME` numa sessão a mais ao lado de um `UPDATE` da principal** sobre as
  linhas já gravadas: os dois entraram (o `UPDATE` de 100.000 linhas em 0,017 s a 0,020 s, a escrita
  em 0,26 s a 0,32 s), a soma reflete o `UPDATE` só nas linhas que ele alcançou, e o controle
  otimista do DuckDB não recusou o par.
- **O `CREATE TABLE` durante um `stream`** de 5.000.000 de linhas com um `md5` por linha, cujo
  primeiro lote chegou em 0,025 s: na sessão a mais ele levou 0,002 s; na principal, 1,569 s,
  1,896 s e 1,888 s, a consulta inteira, e o stream esgotou o resto em 0,106 s a 0,142 s depois
  dele. A sessão principal escreveu em seguida na tabela que a sessão a mais criou.
- **No substituto**, as seções passaram com três rodadas de dois escritores; o substituto é DuckDB
  por trás de uma conexão de mentira, e a leitura vale para o código da sonda, não para o Redshift.

**Consequências**: no DuckDB, dois `appender` na mesma tabela em sessões a mais não conflitam
entre si nem com um `UPDATE` da principal, e o `create_table` proposto pode rodar numa sessão a
mais (um cursor da conexão) para não esperar a consulta de um `stream` aberto antes, o que
dispensaria a ordem `create_table` antes do `stream`. A leitura do Redshift, dois `COPY` na mesma
tabela, está na seção da bateria de 2026-09-28 às 23:09; a decisão sobre a API, tomada em
2026-09-28, está na seção seguinte.

## O que a implementação de `create_table`, `append` e `appender` mostrou

Em 2026-09-28, com a aprovação do usuário (a decisão está em `.claude/memory/decisions.md`),
`create_table`, `append` e `appender` entraram nos dois motores no lugar de `load` e `loader`, e o
`ingest` do DuckDB com `materialize=True` passou a criar a tabela por `ddl(table, "duckdb")` e a
carregá-la por `INSERT ... BY NAME SELECT * FROM delta_scan(...)`, numa transação
([`PLAN-STAGE-4.md`](PLAN-STAGE-4.md), [`PLAN-STAGE-5.md`](PLAN-STAGE-5.md)). As leituras deste
contêiner (Linux, 4 vCPUs, 16.095 MB, Python 3.13.12, DuckDB 1.5.5, PyArrow 25.0.1, deltalake
1.6.6):

- **O `INSERT ... BY NAME` do `delta_scan` custa o mesmo que o `CREATE TABLE AS`**: sobre uma
  tabela Delta de 4.000.000 de linhas de `Lancamento` (`tests/lancamentos_model.py`) numa pasta
  local, cada variante num processo novo, o melhor de três foi 2,737 s pelo `CREATE TABLE AS` e
  2,919 s pela DDL mais o `INSERT` com 2 threads, e 2,499 s contra 2,565 s com 4 threads; o pico de
  RSS acima da base do processo ficou entre 339 MB e 347 MB com 2 threads e entre 497 MB e 515 MB
  com 4, igual nos dois caminhos. A diferença é menor que a variação entre rodadas.
- **A tabela ingerida passou a ter o contrato**: `test_materialized_ingest_applies_the_contract`
  lê a coluna JSON como `JSON`, a `Numeric(18, 2)` como `DECIMAL(18,2)` e a `Double` não nulável
  como `DOUBLE NOT NULL` na tabela materializada, e como `VARCHAR` e nuláveis na view; um documento
  JSON malformado gravado no Delta, que `cast` e `publish_partition` aceitam, faz o `ingest`
  materializado falhar com `duckdb.Error` e deixa o nome livre, enquanto a view o aceita.
- **Uma leitura durante um `append` em curso vê a tabela sem as linhas novas**:
  `test_read_during_an_append_in_flight_sees_the_table_without_the_new_rows` lê 10 linhas com o
  `appender` aberto numa thread, na sessão principal e numa sessão a mais, e 1.010 depois do
  `close`, o efeito que o usuário aceitou em 2026-09-28 no lugar da falha com `CatalogException`
  de 2026-09-23.
- **`duckdb_views()` lista as views internas do catálogo**: 47 no DuckDB 1.5.5, todas no banco
  `system` (`information_schema.columns`, `information_schema.tables`, `pg_catalog.pg_class`,
  `main.sqlite_master`), e `duckdb_tables()` nenhuma tabela interna. A leitura do catálogo de
  `name_in_use`, desde a etapa 4, e a de `object_kind`, sem filtro, davam `columns` e `tables` como
  nomes ocupados; a revisão de 2026-09-28 pôs `NOT internal` nas duas consultas, e
  `test_create_table_creates_the_empty_table_of_the_model` cria a tabela `columns` desde então.
- **As suítes**: `tests/test_engine_duckdb.py`, `tests/test_execution.py` e `tests/test_pipeline.py`
  passaram na raiz local (63 casos, 1 pulado), com o pipeline ingerindo as 12 tabelas do modelo
  cliente pela DDL e pelo `INSERT`; as suítes do motor Redshift, da publicação e do leitor, com as
  três de estudo do S3 e do Redshift, passaram no substituto (92 casos, 1 pulado), e o `COPY` de 10
  linhas por `append` levou 0,06 s ali (`redshift.engine.small_append`). As três configurações
  completas estão em [`CURRENT_STATE.md`](CURRENT_STATE.md).

## O que a revisão do repositório de 2026-09-28 reproduziu

Em 2026-09-28, a revisão do repositório pedida pelo usuário, atrás de defeitos no código e de
documentação e dados desatualizados, reproduziu cada defeito antes de corrigi-lo, neste contêiner
(Linux, 4 vCPUs, 16.095 MB, cgroup v1 sem swap, Python 3.13.12, DuckDB 1.5.5, PyArrow 25.0.1,
deltalake 1.6.6, SQLAlchemy 2.0.54, pandas 3.0.6), na pasta local e no substituto de
`tests/emulator.py`. Cada correção tem um teste que reprovou no código anterior e passa no novo. Os
achados que pediam uma decisão vêm primeiro; o usuário os decidiu no mesmo dia, e a seção seguinte
diz o que a implementação das decisões mostrou.

- **O `COPY` posicional do Redshift depois de uma coluna nova no meio do modelo**: no substituto, as
  linhas de `t_meio` gravadas antes de a coluna `novo` entrar entre `id` e `a` saíram do `ingest` e
  da publicação com `novo` igual a `a1`, `a` igual a `b1` e `b` nulo; com `a` e `b` trocados de
  ordem no modelo, `schema_diff` não achou mudança, e o `ingest` e a publicação trocaram os valores
  das duas colunas.
- **As conversões da carga inicial**: `initial_load` de uma tabela com `1.236` numa
  `Numeric(18, 2)`, `2026-08-31 12:00` numa `Date` e `15:00` UTC com o fuso `America/Sao_Paulo` numa
  `DateTime` sem fuso, que o `cast` recusa um a um, gravou `1.24`, `2026-08-31` e `15:00`, a hora em
  UTC, o fuso deste contêiner, sem recusa. `load_report` deu `matches` falso pela soma de `valor`
  (1,236 contra 1,24) e não vê a hora nem o fuso.
- **A auditoria da linha de comando com o contrato no `ingest`**: `serialize-db audit` sobre
  `cad_lancamentos` com o documento `{` numa coluna JSON saiu do `ingest` com `ConversionException`
  (`Malformed JSON at byte 1 of input`), e com um nulo em `preco`, `NOT NULL`, com
  `ConstraintException`; o `ingest` por `CREATE TABLE AS`, o de antes de 2026-09-28, imprimiu o
  relatório reprovado com `json_meta` 1 e saiu com 1.
- **O nome de snapshot repetido em `serialize-db run`**: com `2026T3` já no arquivo de controle, o
  `cli.main` de um pipeline que marca `2026T3` levantou
  `ValueError: prd: o snapshot 2026T3 já existe` na saída da execução, que o script de console
  transforma em saída 1 com traceback.
- **O `ingest` materializado dentro da transação do cliente**: no DuckDB 1.5.5, o
  `BEGIN TRANSACTION` recusado dentro de uma transação a aborta, e o `COMMIT` da transação abortada
  voltou sem erro e sem nada do que ela fez. A transação fixa o seu snapshot no primeiro comando que
  lê ou muda o banco, não no `BEGIN`: a tabela que outro cursor cria depois do `BEGIN` ou de um
  `SELECT 1` é vista. Um `CatalogException` dentro da transação não a aborta.
- **A abertura do motor DuckDB que falha**, por uma extensão ausente ou pelo secret recusado,
  deixava a pasta `serialize_db_*` do `mkdtemp` e o banco; o segundo `close` do `appender` levantava
  `FileNotFoundError`.
- **A memória compartilhada no cgroup**: com 256 MiB de `mmap` compartilhado, `total_cache` e
  `total_shmem` subiram 256 MiB cada; a folga antiga caiu 2 MiB, e a nova, 274 MiB, com o uso
  subindo 274 MiB. O kernel sem swap não devolve essa memória, que o `file` do cgroup v2 também
  conta.
- **O primeiro lote do `stream` do DuckDB**: dez rodadas de
  `test_stream_delivers_each_batch_while_the_query_runs`, cada uma num processo novo, receberam o
  primeiro lote em 0,012 s a 0,024 s com a consulta ainda rodando.
- **O `appender` do Redshift com um lote sem uma coluna anulável do meio**: no substituto, o lote de
  `id_medida` e `largura` igual a 10 entrou como `altura` 10 e `largura` nula, sem erro, porque o
  `COPY` de Parquet é posicional; com a lista das colunas do arquivo, `altura` fica nula e `largura`
  10. A lista com `FILLRECORD` nunca rodou no alvo ([`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md)).
- **O `:` dentro de um literal no caminho de texto do Redshift**: `'a :b'` saía `'a NULL'`, e no
  substituto o `query` devolvia `[1]` e o `stream`, `[]`. O valor `r"ref \:x2"` chegava ao `UNLOAD`
  como `ref :x2`, porque o `BIND_PARAMS_ESC` do compilador também age sobre os literais
  renderizados, e o `stream` não achava a linha que o `query` achava.
- **A conexão do Redshift derrubada no `COMMIT`**: o código anterior reconectava e repetia o
  `COMMIT` numa sessão sem transação, que voltava sem erro. O segundo `close` do `appender` rodava
  outro `COPY` e outro `COMMIT` na conexão de mentira e dava `NoSuchKey` no substituto.
- **A pasta local no comando das suítes**: no substituto, `tests/test_publication.py` e
  `tests/test_reader.py` com as variáveis do comando do `README.md` passaram 4 casos e pularam 26,
  os 9 `redshift` que também são `local` entre eles, e saíram com 0; com a pasta local, passaram os
  30.
- **A versão fixada depois de `publish_delta`**: `next_ids` devolvia, depois de um `publish_delta`
  na mesma execução, ids já publicados, e `previous_partitions` devolvia `[]`, porque os dois liam a
  versão da abertura.
- **O motor fora de `duckdb` e `redshift`**: `SERIALIZE_DB_ENGINE=Redshift` passava pelo `argparse`,
  que aplica o `type=` ao padrão em texto mas não o `choices`, e `serialize-db audit` abria o DuckDB
  calado; com `--sql`, `KeyError`. A `SERIALIZE_DB_REDSHIFT_PORT` que não é número saía de
  `serialize-db run` com `ValueError`, e `--partitions` numa tabela sem partição saía de
  `serialize-db audit` com `ContractError`, os dois com 1 e o traceback.
- **O leitor Delta fora de uma variável**: o finalizador do leitor que nenhuma variável segurava
  fechava o motor debaixo do `stream` e da `session()` que ele tinha devolvido, e o stream terminava
  em `INTERRUPT` e a sessão em `Connection already closed`.
- **As mensagens da tabela sem partição**: a auditoria, `publish_delta`, a carga, o conflito e a
  releitura do Delta e o aviso da troca do motor Redshift imprimiam `partição None`; o estado da
  publicação, `publicada None` e `pendentes [None]`; a diferença da carga, `None linhas`.
- **O `cast` de um dicionário**: a `category` do pandas chega como dicionário, e o cast de
  `dictionary<double>` para `decimal128(18, 2)` arredonda calado (1.236 vira 1.24), e o de
  `dictionary<timestamp>` para `date32` descarta a hora, passando ao largo das recusas que o `cast`
  aplica aos valores.
- **Os casts numéricos do PyArrow 25.0.1**: não há cast de `float16` para `decimal128`
  (`ArrowNotImplementedError`), e o de `float16` para `float64` é exato; o de `int64` para
  `decimal128(38, s)` falha com `s` acima de 19, e o de `int32` com `s` acima de 28
  (`Precision is not great enough ... at least 39`), enquanto `decimal128(38, 0)` guarda até o
  máximo de um `uint64`. `Numeric(0, s)` virava a precisão 18.
- **A ordem das chaves**: `Table.constraints` e `Table.indexes` do SQLAlchemy 2.0.54 são conjuntos,
  cuja ordem de iteração mudou entre processos, e `table_options(...).keys` herdava essa ordem.
- **O `prefixed` sem a `key`**: um `INSERT` com `values(to_=...)` sobre a cópia prefixada de uma
  coluna de `key` diferente do nome falhava com `CompileError: Unconsumed column names: to_`.
- **Os arquivos gerados**: o `check` não acusava a falta da quebra de linha final, que o
  `unified_diff` sobre `splitlines()` escondia, nem o arquivo gerado que a geração não produz mais.
- **O glob Hive do `load_report`**: `read_parquet('<tabela>/*/*.parquet', hive_partitioning = true)`
  falha com `Hive partition mismatch` quando a tabela tem uma subpasta fora do padrão Hive, como
  `backup/`, que `discover_partitions` pula, e uma pasta como `data_str=2026 Q1` entra como
  partição; `load_report` saía com `BinderException` onde a carga passava. A carga media `Uuid`,
  JSON e `Text` sem limite e `Text(n)` pelo `n`.
- **`max_key`**: o arquivo sem linhas de uma partição vazia não tem máximo no log, o que forçava uma
  varredura que devolvia `None` e, ao lado de dados, a varredura da tabela inteira.
- **A barra final e as pastas da exportação por reescrita**: a URI com barra final punha `//` na
  chave do `rewrite` e da exportação; o `COPY ... PARTITION_BY` local do DuckDB falha sem as pastas
  acima do destino (`IOException: Failed to create directory`) e aceita um destino vazio que já
  existe.
- **O esquema do `deep_copy`**: um commit de partição por
  `create_write_transaction(mode="overwrite")` com `schema=` diferente grava um `metaData` com o
  esquema novo e guarda o id, o nome, a descrição, a configuração e as colunas de partição do
  destino (as ações `commitInfo`, `add` e `metaData`); um commit vazio com o mesmo esquema grava só
  `commitInfo`, e um `append` com esquema novo e sem ações não levanta erro e guarda o esquema
  antigo. A cópia de uma versão posterior a um `reconcile` saía sem a coluna nova.
- **Os arquivos locais**: o arquivo novo de `write_text` nascia com o modo 0o600 do
  `NamedTemporaryFile`, e `file:///tmp/meu%20banco/delta` não era decodificado.
- **As conexões do DuckDB de `serialize_db.delta`**: a releitura, o `rewrite`, a exportação por
  reescrita e a contagem da cópia abriam o DuckDB sem `threads` nem `memory_limit`, com o padrão de
  80% da memória da máquina.
- **A saída do interpretador**: uma sonda que roda `deep_copy`, `reconcile`, `publish_partition` e
  `deep_copy` e lê a cópia por `to_pyarrow_dataset().to_table()` ficou presa na saída do
  interpretador (código 124 sob `timeout 60`), no código anterior e no novo; sem a última leitura, e
  com a mesma leitura sem aquelas chamadas, saiu normalmente. A causa não foi procurada.
- **Os probes**: `hide_credentials` deixava o usuário e a senha de um proxy sem esquema
  (`usuario:s3nh4@host:porta`) em `environment_rows` e na leitura "sem host" de `duckdb_proxy`; o
  `BK-3` reprovava a regra `NoncurrentVersionExpiration` que `docs/index.md` recomenda e comparava a
  raiz como texto, com `raiz-logs/` alcançando `raiz/`; o limite inferior do grupo `dev`
  (`boto3>=1.40`, `redshift-connector>=2.1`) apagava a versão fixada nas dependências de execução, e
  o `SP-9` aceitava o boto3 1.40 contra o pin 1.43.102; o `SP-10` reprovava pela falta da extensão
  `aws`, que só `diagnose_aws.py` carrega; e o `failure_message` do `conftest` cortava a mensagem em
  300 caracteres antes de mascarar as credenciais. Os relatórios de antes e de depois diferiram só
  nas linhas corrigidas: no `space.py`, o `SP-10` passou de reprovação a leitura e o resumo de 3
  para 2 checagens reprovadas, com a saída ainda 2 por `SP-1` e `SP-2`, sem credenciais nem região
  neste contêiner; no `parquet_source.py`, sobre a base fictícia, só o horário; no
  `duckdb_threads.py`, sobre um Delta local, a linha da materialização e o rótulo da referência.

**Consequências**: as correções entraram no pacote, nos probes e nos testes, e os arquivos de etapa
descrevem o comportamento novo; os achados que mudavam uma interface ou pediam uma rodada no alvo
foram ao usuário, que os decidiu em 2026-09-28.

## O que a implementação das decisões da revisão de 2026-09-28 mostrou

Em 2026-09-28, neste contêiner, com as versões da seção anterior, na pasta local e no substituto de
`tests/emulator.py`, cada decisão que muda código entrou com testes que reprovaram no código
anterior e passam no novo; a checagem do DuckDB de `probes/diagnose_aws.py`, sem caso no pytest, foi
lida no moto antes e depois.

- **O `COPY` com a lista das colunas de cada arquivo**: os testes novos da coluna anulável `novo`
  entre `id` e `a` e de `a` e `b` trocados de ordem, rodados sobre o `src/` anterior no substituto,
  reprovaram 11 dos 15 selecionados: o `ingest` deu `{'novo': 'a1', 'a': 'b1', 'b': None}` e
  `{'b': 'a1', 'a': 'b1'}`, a publicação deslocou os valores do mesmo jeito, e o agrupamento de
  `copy_manifest` falhou sem `CopyManifest`; no código novo, os 15 passaram.
  `get_add_actions()` do deltalake 1.6.6 lista o commit mais novo primeiro (os commits de `p3`,
  `p1`, `p2` e `p0` voltaram como `p0`, `p2`, `p1` e `p3`), e sem a ordenação pelo caminho o teste
  do agrupamento reprova. O dublê do `COPY` que falha em
  `test_failed_copy_leaves_control_row_untouched` levantava `AttributeError` no código anterior, e
  `pytest.raises(Exception)` o engolia; o teste confere agora que o dublê gravou o manifesto
  inválido.
- **O custo de ler os rodapés**: `copy_manifest` sobre uma tabela de 200 arquivos (20 partições de
  10 arquivos), melhor de três, levou 31,3 ms na pasta local contra 9,6 ms do código anterior, e
  1.255,8 ms no moto contra 91,4 ms; numa partição de 10 arquivos, 12,0 ms contra 10,8 ms e 163,5 ms
  contra 85,7 ms. A leitura dos 200 rodapés, em série, levou 12,4 ms na pasta local e 1.145,3 ms no
  moto; com 4 a 32 threads, de 34,6 ms a 53,2 ms e de 1.057,9 ms a 1.103,3 ms. O moto é limitado
  pelo próprio servidor, e a latência do S3 real, um `HEAD` e um `GET` de intervalo por arquivo, não
  foi medida.
- **A auditoria da linha de comando com a ingestão recusada**: no DuckDB 1.5.5,
  `ConstraintException` deriva de `IntegrityError` e `ConversionException` de `DataError`, as duas
  de `DatabaseError`, e `IOException`, de que `HTTPException` deriva, de `OperationalError`.
  `serialize-db audit` imprimiu `ingestão: reprovada (Constraint Error: NOT NULL constraint failed:
  cad_lancamentos_projetados.valor)` e a linha de `Conversion Error: Malformed JSON` e saiu com 1,
  onde o código anterior saía com `ConstraintException` e o traceback. A primeira versão capturava
  `duckdb.Error` e imprimia como reprovação um `IOException` de acesso negado; a captura ficou nas
  duas classes. No substituto, as duas recusas do Redshift chegam como `ProgrammingError` com o
  SQLSTATE `XX000`, que a classe não separa de outro erro do servidor, e a linha impressa traz o
  erro do driver; o texto do alvo, que `test_cli_audit_on_redshift_prints_the_refused_ingest` guarda
  no relatório da sessão (`redshift.audit.refused_ingest.<mês>`), espera a próxima bateria.
- **O nome de snapshot já usado**: com o nome em `snapshots` ou em `archived`, `run.snapshot`
  levantou `ContractError` antes de qualquer commit, e `serialize-db run` saiu com 2, com a versão
  do Delta igual e `serialize-db channel` na mensagem; o código anterior commitava e saía com 1 e o
  traceback. O dublê do outro escritor em `test_cli_run_parses_and_exits_by_result`, que regravava o
  mesmo conteúdo, passou a sair com 0, porque a impressão digital da pasta local é o `sha256` do
  conteúdo e a leitura nova de `run.snapshot` via o mesmo arquivo que a saída; o dublê grava agora
  um snapshot novo depois de cada leitura.
- **A checagem do DuckDB de `probes/diagnose_aws.py`**: contra o moto, com a mesma pasta de
  extensões e duas rodadas de cada versão, os relatórios diferiram, fora a data, a porta, os
  endereços do DNS e o caminho do relatório, só na linha do DuckDB e no resumo. A checagem anterior,
  com o secret `credential_chain` sem `URL_STYLE 'path'` nem `USE_SSL false`, reprovou com
  `IOException: IO Error: Could not resolve hostname error for HTTP GET to
  'https://emulador.127.0.0.1:<porta>/...'`; a nova, pelo `Storage.duckdb_connect`, listou
  `serialize-db-poc/` e as subpastas (2 objetos) em 1,5 s, e o resumo passou a "os três clientes
  listaram o prefixo", com saída 0 no lugar de 1. Numa pasta de extensões sem a `aws`, a anterior
  reprovou com `Extension ".../aws.duckdb_extension" not found` e a nova listou. Contra um endpoint
  que aceita a conexão e não responde, a nova parou no teto de 60 s do subprocesso (60,1 s), e o
  DuckDB sozinho, com as esperas padrão do `httpfs`, desistiu em 121,8 s, depois de 4 conexões; o
  `http_timeout` do DuckDB 1.5.5 é medido em segundos, e o `SET http_timeout = 10000` da checagem
  anterior eram 10.000 s. `probes/space.py` neste contêiner deixou as duas linhas da `aws` e passou
  o `SP-10` de `note` a `pass`; `test_extensions_check_fails_on_any_extension_that_does_not_load` e
  `test_duckdb_section_neither_loads_nor_lists_aws` reprovaram no `space.py` anterior.
- **A simulação do `RS-11`**: com um IAM fabricado que devolve, por ação, a decisão mais restritiva
  entre os recursos da chamada, como a documentação de `EvaluationResult` descreve, a chamada única
  anterior reprovou uma política de privilégio mínimo
  (`s3:ListBucket, s3:GetObject, s3:PutObject negadas`), e a simulação por recurso, `ListBucket`
  contra o bucket e `GetObject` e `PutObject` contra os objetos sob a raiz, passou em duas chamadas,
  com e sem prefixo; a política sem `PutObject` reprova, e a falha da segunda chamada deixa o
  `RS-11` como nota. `test_copy_access_simulates_each_action_against_its_resource` e
  `test_copy_access_reads_a_failed_object_simulation` reprovaram na chamada única.

**Consequências**: `copy_manifest` lê os rodapés em série, como `register_files`, e a publicação e o
motor Redshift rodam um `COPY` por lista de colunas ([etapa 3](PLAN-STAGE-3.md),
[etapa 5](PLAN-STAGE-5.md), [etapa 8](PLAN-STAGE-8.md)); o tempo por tabela da publicação, que o
log registra, mede o custo no alvo na próxima bateria, junto com a lista e o `FILLRECORD`
([`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md)). A linha do DuckDB do `diagnose_aws.py` e o `SP-10`
esperam a mesma bateria, e o `RS-11` segue como leitura no alvo, onde o IAM não conecta por TCP
([`probes/README.md`](../probes/README.md)).

## O que a bateria de 2026-09-28 às 20:14 mostrou no ambiente alvo

Em 2026-09-28, de 20:14 a 20:54 UTC, o usuário rodou no ambiente alvo o bloco "Probes e Testes - BN"
de `SUITE.md`, os cinco probes e as sete sessões do pytest, numa máquina de 8 vCPUs e 15,1 GiB
(Python 3.13.15, DuckDB 1.5.5, deltalake 1.6.6, pyarrow 25.0.1, boto3 1.43.102, `redshift_connector`
2.1.17, `sa-east-1`), sobre a `main` com o PR #104 [inferido: as 662 coletas são as dela]. Os
relatórios ficam fora de `plan/`, com os achados aqui. As leituras repetem as de 2026-09-27 ("O que
a bateria de 2026-09-27 às 15:58 mostrou no ambiente alvo"), salvo:

- **O `COPY` de Parquet sem manifesto, de um caminho sem objeto, saiu sem erro.**
  `test_appender_copies_the_file_at_close` reprovou nas quatro rodadas, duas da suíte Redshift e
  duas da do motor, com `DID NOT RAISE Exception`: o dublê trocava o arquivo do `appender` por
  `<arquivo>.ausente`, e o `COPY ... FORMAT AS PARQUET FILLRECORD` desse caminho não levantou erro
  [inferido: sem objeto que case com o prefixo, não carregou nada; a contagem seguinte não rodou].
  Até o PR #104, o dublê chamava a si mesmo, e o `RecursionError` satisfazia o
  `pytest.raises(Exception)`: esse `COPY` nunca tinha rodado no alvo. O substituto lia o objeto pelo
  nome e falhava com o `NoSuchKey` do S3. A documentação do `COPY` lê o caminho sem `MANIFEST` como
  prefixo de chave e recomenda o manifesto quando o prefixo pode alcançar arquivos indesejados; com
  `mandatory: true`, o `COPY` termina quando não acha o arquivo da entrada, e ele falha sem o
  manifesto. O `append` do Redshift devolveria as linhas gravadas no arquivo com a tabela sem elas;
  a ingestão e a publicação já liam manifestos de entradas obrigatórias.
- **As leituras que a revisão de 2026-09-28 deixou para o alvo saíram como esperado.** O `COPY` com
  a lista das colunas de cada arquivo e o `FILLRECORD` passou no `appender`
  (`test_appender_loads_a_batch_without_a_middle_column`), no `ingest` e no `pinned_delta` (os casos
  da coluna nova no meio e da reordenação de `tests/test_engine_redshift.py`) e na publicação (os de
  `tests/test_publication.py`), nas duas rodadas. A linha do DuckDB do `diagnose_aws.py`, pelo
  `Storage.duckdb_connect`, listou `serialize-db-poc/` e as subpastas (0 objetos), e o resumo
  repetiu "os três clientes listaram o prefixo"; o `SP-10` carregou `httpfs`, `delta`, `parquet` e
  `json` e passou, e `duckdb_extensions()` não listou a `aws`. A auditoria da linha de comando no
  Redshift imprimiu a ingestão recusada como reprovação: `Cannot insert a NULL value into column
  valor` (código 8007) na partição com o nulo e `Invalid input` (código 8001, `JSON_PARSE() error:
  End-of-input inside object or array: {`) na do JSON truncado, as duas com o SQLSTATE `XX000` e o
  dicionário inteiro do driver, que traz o caminho do fonte do servidor.
- **As suítes.** A sessão `-m "not redshift"` aprovou 610 casos em 274,1 s, contra 541 em 226,7 s:
  os casos a mais vêm dos PRs de 2026-09-27 e 2026-09-28 [inferido]. As suítes Redshift, do motor
  e da publicação levaram 671,8 s e 605,9 s (52 casos), 150,9 s e 147,0 s (10) e 205,6 s e 249,9 s
  (10), contra 45, 6 e 8 casos em 2026-09-27; o `append` de 10 linhas levou 1,23 s e 1,26 s
  (`redshift.engine.small_append`, a leitura que se chamava `small_load`).
- **Os probes** diferiram dos de 2026-09-27 no que muda a cada rodada e em três leituras: o `RS-12`
  contou 85 erros de carga em 30 dias em `sys_load_error_detail`, onde a contagem de 2026-09-26 e a
  de 2026-09-27 não devolveram linha e a de 2026-09-24 contou 25, com a causa não lida; o `BK-14`
  contou 8.822 versões não correntes (270.369.639 bytes) e 8.127 marcadores de exclusão sob a raiz
  das suítes, contra 7.619 (233.165.927 bytes) e 7.030: 1.203 versões e 37.203.712 bytes entre as
  duas leituras; e a credencial do contêiner tinha 55 minutos pela frente, contra 30. As chamadas
  que falharam são as de 2026-09-27: 1 no `space.py`, 6 no `bucket.py` e 3 no `catalog.py`.

A partir das 21:14:56 UTC, o usuário rodou a carga, a auditoria, `history`, `snapshot`, `vacuum` e
`archive` do bloco "Migração Parquet -> Delta" de `SUITE.md`, numa raiz recarregada: o `history` de
`cad_lancamentos` começa no `CREATE TABLE` das 21:16:54.

- **A origem mudou desde 2026-09-27, e o mês 2026-07-31 mudou durante a carga** [inferido: a base de
  origem estava sendo regravada]. `cad_aliquotas` tinha 21 linhas, contra 22; `cad_operacoes` e
  `rel_contrato_operacao` vieram sem a partição 2026-07-31, que tinha 5.579.536 e 15.209.141
  linhas; `cad_contratos` 2026-07-31 seguiu com 3.985.447, e as outras partições repetiram as
  contagens e as somas. Em `cad_lancamentos` 2026-07-31, que tinha 141.933.948 linhas, a
  conferência da partição contou 80.000.000 e o `COPY` gravou 82.000.000, múltiplos dos arquivos de
  1.000.000 linhas da origem, e `register_files` recusou com `RegistrationRefused: 82000000 linhas
  nos arquivos, 80000000 na fonte`, sem commit, como a [etapa 3](PLAN-STAGE-3.md) prevê. As quatro
  partições anteriores ficaram no log (versões 1 a 4), o relatório guardou `cad_lancamentos` em
  `in_progress`, o script saiu pelo traceback, como a decisão de 2026-09-25 quer, e o arquivo
  gravado ficou na pasta da partição, fora do log, até um `serialize-db vacuum --full`.
- **A carga das partições conferidas** (8 CPUs, 12.515 MB disponíveis, 8 threads e
  `memory_limit` de 6.257 MiB): as de `cad_lancamentos` levaram 36,1 s, 24,4 s, 63,5 s e 33,6 s,
  contra 30,6 s, 21,2 s, 54,7 s e 30,7 s em 2026-09-27, com o pico do processo em 8.742 MB, contra
  8.625 MB. A linha `conversões` de cada tabela listou só as chaves `int32 -> int64`.
- **A auditoria** de `cad_lancamentos` 2026-01-31 com `--foreign-keys`, na versão 4, repetiu os
  989.852 órfãos de `data_base`, `sistema` e `contrato` e o total de `valor` 117.667.407.519,194421.
- **`snapshot`, `vacuum` e `archive`**: o snapshot `carga-2026-09-24` com as 12 tabelas; 0 arquivo a
  apagar em cada uma, porque o `vacuum` sem `--full` não lista órfãos; e os 22 arquivos copiados,
  com `cad_lancamentos` em 11,9 s e pico de 329 MB.

**Consequências**: o `COPY` do `appender` do Redshift passa a ler um manifesto com o arquivo como a
única entrada obrigatória (decisão do usuário de 2026-09-28, [etapa 5](PLAN-STAGE-5.md),
[`redshift.md`](redshift.md)), e o substituto lê o caminho sem manifesto como prefixo, com o que o
código anterior reprova como reprovou no alvo (a seção seguinte). Os itens da lista de colunas com
`FILLRECORD` e da linha do DuckDB do `diagnose_aws.py` saem de
[`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md), com a resposta nesta seção; o custo de ler os rodapés na
publicação da base segue lá, porque o bloco da publicação não rodou, e o `COPY` do `appender` pelo
manifesto espera a próxima bateria. A carga seguinte recomeça de `cad_lancamentos` 2026-07-31
quando a origem estiver estável.

## O que o manifesto do `appender` e a sonda das conversões mostraram

Em 2026-09-28, neste contêiner (Linux x86_64, 4 vCPUs), com as versões da seção anterior, na pasta
local e no substituto de `tests/emulator.py`:

- **O substituto reproduziu o alvo.** Com o `COPY` sem `MANIFEST` lido como prefixo e o resultado
  vazio sem erro, o `src/` e o teste anteriores reprovaram `test_appender_copies_the_file_at_close`
  com `DID NOT RAISE Exception`, como no alvo, e os outros 5 casos do `appender` passaram. Com o
  manifesto, os 6 passaram; o teste novo apaga o arquivo depois que o manifesto é gravado, e o
  substituto recusou o `COPY` pela entrada obrigatória ausente, com a nota do comando
  `comando: COPY ... FROM '<manifesto>'` no erro.
- **A entrada precisa ser obrigatória.** Com `mandatory: false` no manifesto, o teste local do
  manifesto reprovou na comparação do manifesto, e o do alvo, no substituto, com `DID NOT RAISE
  Error`: o substituto pula a entrada opcional ausente e o `COPY` sai sem erro.
- **Os nanossegundos do `INT96` somem na carga inicial.** Uma origem com o `timestamp`
  `2026-01-31 15:30:00.123456789` em `INT96` numa coluna `DateTime` foi carregada com as contagens e
  as somas iguais e a linha `conversões: carimbo: INT96 -> timestamp[us]`, e o Delta guardou
  `15:30:00.123456`: o `CAST` do DuckDB trunca a parte abaixo do microssegundo, que o `cast` recusa.
- **A linha `conversões` lê só o primeiro arquivo da primeira partição.** Numa tabela com os três
  tipos com perda na primeira partição (`1.234` e `2.675` em `double` numa `Numeric(18, 2)`, a hora
  numa `Date` e o fuso `America/Sao_Paulo` numa `DateTime`), a linha listou
  `valor: double -> decimal128(18, 2), data: timestamp[us] -> date32[day], carimbo:
  timestamp[us, tz=America/Sao_Paulo] -> timestamp[us]`, e o arredondamento saiu como `DIFERENÇA`
  (3,909 contra 3,910). Com a primeira partição no tipo do contrato e os três tipos na segunda, a
  linha não saiu, a `DIFERENÇA` da soma apontou a segunda partição, e a hora e o fuso se perderam
  sem aviso.

**Consequências**: a [etapa 5](PLAN-STAGE-5.md) e [`redshift.md`](redshift.md) descrevem o `COPY`
do `appender` pelo manifesto, e `docs/index.md` e a [etapa 7](PLAN-STAGE-7.md) listam a quarta
perda da carga inicial e o alcance da linha `conversões`, que fica como está (decisão do usuário de
2026-09-28).

## O erro no meio do stream do motor DuckDB trocado pela interrupção

Em 2026-09-28, neste contêiner (Linux x86_64, 4 vCPUs, `duckdb` 1.5.5, revisão `d8cdaa33fd`), na
pasta local:

- **O caso reprova às vezes.** `test_stream_delivers_each_batch_while_the_query_runs`, marcado
  `local`, reprovou em 3 de 21 execuções: uma na sessão inteira com a raiz local, uma em 12
  repetições do caso isolado e uma em 8 repetições com um plugin do pytest que registrava as
  chamadas de `DuckDBEngine.interrupt`. Nas três, a consulta que falha na linha 2.900.000 entregou
  `OSError: INTERRUPT Error: Interrupted!` no lugar de `Could not convert string 'x' to INT32`; na
  execução registrada, a falha veio no orçamento de 10.000 bytes, e o motor não chamou
  `interrupt()` nenhuma vez.
- **O DuckDB troca o erro pela interrupção [inferido].** No código da revisão `d8cdaa33fd`,
  `Executor::PushError` guarda o erro de uma tarefa e liga `context.interrupted` para parar as
  outras, e `SimpleBufferedData::ExecuteTaskInternal`, no caminho do resultado em stream que o
  `to_arrow_reader` lê, levanta `InterruptException` quando acha essa marca, antes de pedir ao
  executor o erro guardado. O erro que a thread de trabalho acha entre duas leituras chega ao
  leitor como a interrupção.
- **As sondas não reproduziram.** A mesma consulta entregou o erro de conversão em todas as 210
  leituras no DuckDB puro (90 por `read_all` numa thread auxiliar, 60 delas com quatro processos
  ocupando a CPU; 120 lote a lote, com pausas de 0, 1 e 5 ms com duas threads e de 1 ms com uma) e
  em todas as 240 pelo stream do motor (120 nos dois orçamentos, com duas threads e com uma; 120 na
  sequência do fim do teste, com e sem o stream abandonado antes).
- **O caminho do stream pula a troca da interrupção pelo erro.**
  `ClientContext::ExecuteTaskInternal` troca a `InterruptException` pelo erro guardado no
  executor quando uma thread de trabalho o achou; a verificação da marca em
  `SimpleBufferedData::ExecuteTaskInternal` roda antes dessa chamada, fora da troca. Com uma
  thread, quem lê executa as tarefas e acha o erro dentro da chamada que o troca [inferido].
- **O caso do erro num motor de uma thread passou em todas as execuções.** Com o caso do erro num
  motor de `threads=1`, o teste passou em 60 de 60 execuções isoladas: 30 durante o ajuste e 30 da
  versão final, que confere o `OSError` do leitor Arrow.

**Consequência**: o caso do erro roda num motor de uma thread e confere o `OSError` do leitor
Arrow, e as docstrings de `DuckDBEngine.stream` e `DeltaReader.stream` avisam o cliente de que,
com mais de uma thread, o erro da consulta pode chegar como a interrupção, sem a causa (decisão
do usuário de 2026-09-28, [etapa 4](PLAN-STAGE-4.md)); o comportamento do pacote não muda.

## O que a bateria de 2026-09-28 às 23:09 mostrou no ambiente alvo

Em 2026-09-28, das 23:09 às 23:55 UTC, o usuário rodou no ambiente alvo o bloco "Probes e Testes -
BN" de `SUITE.md`, os cinco probes e as sete sessões do pytest, numa máquina de 8 vCPUs e 15.505 MB
(Python 3.13.15, DuckDB 1.5.5, deltalake 1.6.6, pyarrow 25.0.1, boto3 1.43.102, `redshift_connector`
2.1.17, `sa-east-1`), sobre a `main` com o PR #105 [inferido: as suítes Redshift gravaram a leitura
`redshift.engine.copy_missing_mandatory_file`, que só o teste do PR #105 grava]. Em 2026-09-29, a
partir das 00:01 UTC, vieram na mesma máquina os blocos da migração, da publicação, do acesso de
leitura e da exportação, as sondas de consistência com a de dois escritores,
`probes/duckdb_threads.py` e `probes/credentials.py`. Os relatórios ficam fora de `plan/`, com os
achados aqui. Nenhum caso e nenhuma checagem falharam, e as leituras repetem as de 2026-09-27 ("O
que a bateria de 2026-09-27 às 15:58 mostrou no ambiente alvo") e as das 20:14 ("O que a bateria de
2026-09-28 às 20:14 mostrou no ambiente alvo"), salvo:

- **O `COPY` pelo manifesto falha quando o arquivo obrigatório não existe.**
  `test_appender_copies_the_file_at_close` passou nas quatro rodadas, duas da suíte Redshift e duas
  da do motor: o arquivo apagado depois do manifesto fez o `COPY ... MANIFEST` do `appender` falhar
  com `Spectrum Scan Error: File not found` e o SQLSTATE `XX000`
  (`redshift.engine.copy_missing_mandatory_file`), sem deixar linha, e os outros casos do `appender`
  carregaram pelo manifesto. O substituto recusava o mesmo `COPY` com uma mensagem própria, e dá a
  do alvo desde a decisão do usuário de 2026-09-29. O `append` de 10 linhas levou 1,49 s e 1,55 s na
  suíte Redshift e 1,47 s e 2,41 s na do motor, contra 1,23 s, 1,26 s, 1,63 s e 1,22 s às 20:14, sem
  o manifesto (`redshift.engine.small_append`).
- **Dois escritores na mesma tabela entraram nos dois motores.**
  `probes/consistencia/probe_append_test.py` passou com `-m redshift` em 41,4 s, com 20.000 linhas
  por escritor, e com `-m local` no DuckDB da máquina em 11,6 s, com 200.000, e cada seção deixou a
  tabela com as linhas das escritas, sem id repetido e com a soma de `valor` delas. No Redshift, os
  dois `append` em duas sessões a mais, ao mesmo tempo, entraram nas três rodadas, cada um em
  3,060 s a 4,353 s; os dois na sessão principal, em duas threads, em 2,946 s e 4,891 s, em série
  sob o lock; e o `append` ao lado de um `UPDATE` das linhas já gravadas, em 3,345 s, com o `UPDATE`
  em 2,372 s: o Redshift não recusou nenhum par com o `1023`. No DuckDB, os mesmos pares entraram em
  0,526 s a 0,681 s nas cinco rodadas, em 0,573 s e 1,094 s na sessão principal e em 0,493 s ao lado
  do `UPDATE`, que levou 0,031 s.
- **O `create_table` do DuckDB não esperou a consulta de um `stream`.** Na seção D da mesma sonda,
  com o `stream` de 5.000.000 de linhas rodando, cujo primeiro lote chegou em 0,040 s, o
  `create_table` entrou em 0,010 s numa sessão a mais e em 0,006 s na principal, e o stream esgotou
  o resto 1,414 s depois deles; na rodada local de 2026-09-28, antes da implementação, o
  `CREATE TABLE` na principal esperou a consulta inteira (1,569 s a 1,896 s). No Redshift, o da
  sessão a mais entrou em 1,072 s, começando durante o `UNLOAD` do `stream`, que voltou em 1,009 s,
  e o da principal, que roda sob o lock, entrou em 0,693 s depois do `UNLOAD` [inferido pelos
  tempos: a sonda espera 0,2 s antes do primeiro], sem leitura da espera. Nos dois motores, o
  `append` da principal na tabela criada pela sessão a mais entrou (0,318 s e 2,413 s).
- **As suítes.** A sessão `-m "not redshift"` aprovou 610 casos em 271,4 s, contra 274,1 s às 20:14.
  As suítes Redshift, do motor e da publicação aprovaram 52 casos duas vezes (617,4 s e 690,6 s), 10
  duas vezes (214,0 s e 239,0 s) e 10 duas vezes (305,7 s e 286,9 s), contra 671,8 s e 605,9 s,
  150,9 s e 147,0 s e 205,6 s e 249,9 s às 20:14, com a causa da diferença das duas últimas não
  medida.
- **Os probes** diferiram dos das 20:14 só no que muda a cada rodada: o `RS-12` contou 93 erros de
  carga em 30 dias, contra 85; o `BK-14` contou 10.301 versões não correntes (307.133.320 bytes) e
  9.504 marcadores de exclusão sob a raiz das suítes, contra 8.822 (270.369.639 bytes) e 8.127:
  1.479 versões e 36.763.681 bytes entre as duas leituras; e a credencial do contêiner tinha 32
  minutos pela frente, contra 55. As chamadas que falharam são as de 2026-09-27: 1 no `space.py`, 6
  no `bucket.py` e 3 no `catalog.py`, com o Lake Formation e o S3 Tables sem resposta em 60,9 s e
  30,4 s.

Em 2026-09-29, a partir das 00:01:30 UTC, o usuário rodou a carga, a auditoria, `history`,
`snapshot`, `vacuum` e `archive` do bloco "Migração Parquet -> Delta" de `SUITE.md`, numa raiz
recarregada: o `history` de `cad_lancamentos` começa no `CREATE TABLE` das 00:03:54.

- **A origem voltou à de 2026-09-27, e a carga passou inteira.** A carga (8 CPUs, 12.042 MB
  disponíveis, 8 threads e `memory_limit` de 6.021 MiB) conferiu as 12 tabelas com as mesmas
  354.048.596 linhas em 25 partições de 2026-09-27, as mesmas contagens, somas e contagens de não
  finitos em cada partição, as mesmas conversões e os mesmos três itens fora do modelo, em 514,3 s
  somados, contra 496,8 s: `cad_aliquotas` com 22 linhas, e `cad_operacoes`, `rel_contrato_operacao`
  e `cad_lancamentos` com a partição 2026-07-31 inteira (5.579.536, 15.209.141 e 141.933.948
  linhas). As partições de `cad_lancamentos` levaram 33,3 s, 22,8 s, 56,8 s, 33,7 s e 249,9 s,
  contra 30,6 s, 21,2 s, 54,7 s, 30,7 s e 245,0 s, com o pico do processo em 8.734 MB, contra
  8.625 MB. Com a raiz recarregada, a continuação da carga parada em 2026-09-28 e o `vacuum --full`
  do arquivo que ela deixou fora do log não rodaram.
- **A auditoria** de `cad_lancamentos` 2026-01-31 com `--foreign-keys`, na versão 5, repetiu os
  989.852 órfãos de `data_base`, `sistema` e `contrato` e o total de `valor` 117.667.407.519,194421.
- **`snapshot`, `vacuum` e `archive`** repetiram 2026-09-27: o snapshot `carga-2026-09-24` com as 12
  tabelas, 0 arquivo a apagar em cada uma e os 25 arquivos copiados, com `cad_lancamentos` em 17,7 s
  e pico de 349 MB, o arquivo da 2026-07-31 em 6,8 s, contra 16,8 s, 350 MB e 5,9 s.

Em seguida, o usuário rodou os blocos "Publicação Delta -> Redshift", "Acesso de leitura" e
"Exportação e Compact" de `SUITE.md`, na mesma raiz, com a saída colada na conversa, sem a hora:

- **A publicação da base inteira por canal** repetiu o fluxo de 2026-09-27: `--init` criou a tabela
  de controle; `--tables cad_contas --channel default` publicou a versão 1 em 3,3 s, com o pico do
  processo em 251 MB; `--max-workers 4 --channel default` publicou as outras 11, as sem partição de
  3,4 s a 4,5 s, `cad_contratos` em 38,6 s, `cad_operacoes` em 60,8 s, `rel_contrato_operacao` em
  66,8 s e `cad_lancamentos` em 335,2 s, com o pico em 286 MB, contra 37,9 s, 60,5 s, 66,1 s e
  328,5 s com 270 MB em 2026-09-27, antes da leitura dos rodapés da revisão de 2026-09-28: de 0,5% a
  2,0% a mais por tabela particionada, menos que os 11% a 21% entre 2026-09-26 e 2026-09-27, com um
  arquivo por partição. `--status` leu as 12 `prd_<tabela>` com a versão publicada igual à atual e
  nenhuma partição pendente, e `--channel current` e `--snapshot carga-2026-09-25 --tables
  cad_contas` responderam que cada versão já estava publicada.
- **O leitor Delta** abriu as 12 views do snapshot `carga-2026-09-25` em 0,556 s, e a contagem de
  `cad_contas` deu 101 pelos dois leitores.
- **`export` de `cad_lancamentos`**: por cópia, 5 arquivos em 15,6 s com pico de 259 MB; por
  `--mode rewrite`, 5 arquivos em 58,4 s com pico de 6.902 MB, contra 14,7 s e 56,7 s com 6.938 MB.
  O `compact` da 2026-03-31 recusou de novo pelo snapshot na versão atual 5.

Das 00:29:48 às 00:33:03, o usuário rodou o bloco "Sondas de consistência" de `SUITE.md`, com a
sonda de dois escritores acima. Nenhuma checagem reprovou, e os achados conhecidos se repetiram:

- **O sinal do zero** pelo `COPY` do DuckDB deu 100 de 2.000 linhas na sonda dos tipos e 1.538 e
  1.539 por partição na da execução; **a janela de `publish`** da seção D da sonda da execução
  repetiu `exec-e` commitando a versão 9 sobre a 8 de `exec-f`, sem `ExecutionConflict`; a disputa
  da seção C deu o caminho de 2026-09-27, `exec-d` commitando e `exec-c` recebendo
  `ExecutionConflict`; `deep_copy` seguiu sem as estatísticas de `Boolean` e `DateTime`; e a escrita
  condicional na pasta local perdeu 335 de 400 atualizações, com 84 conflitos vistos, contra 312 e
  204.
- **O leitor Delta sob `materialize`** fez 330 leituras sem erro, com a troca pela tabela inteira em
  0,53 s a 0,65 s e pela parcial em 0,35 s a 0,40 s, contra 0,40 s a 0,46 s e 0,23 s a 0,25 s, ao
  lado de `probes/duckdb_threads.py` [inferido pelos horários]; **o motor Redshift** da sonda
  (`probe_redshift_test.py`) passou em 59,6 s, e a limpeza apagou os 27 objetos da sessão.

Das 00:31:00 às 01:34:02, `probes/credentials.py` leu `<raiz>/prd/cad_contas` em 14 rodadas, e
nenhuma leitura falhou:

- **O secret do motor DuckDB trocou de chave antes de cada expiração.** Ele passou à chave nova às
  01:01:07, 12,1 minutos antes da expiração das 01:13:11 da chave que guardava (`CR-9`), e às
  01:31:12, 13,0 minutos antes da das 01:44:11, a cada vez na primeira rodada dentro dos 15 minutos
  em que o botocore renova a credencial (as das 00:56:05 e das 01:26:11 estavam a 17,1 e a 18,0
  minutos), e o `delta_scan` leu nas 5 rodadas depois da primeira expiração (`CR-4`).
- **O contêiner trocou a chave de 27 a 33 minutos antes da expiração da anterior**: a cadeia do
  `boto3` passou a entregar a chave que expira às 01:44:11 entre 00:41:03 e 00:46:04, e a que expira
  às 02:14:49 entre 01:11:08 e 01:16:09, como as trocas a cerca de 30 minutos de 2026-09-26 e de
  2026-09-27.
- **Os outros clientes seguiram lendo**: a cláusula do `COPY` e do `UNLOAD` seguiu a chave do
  contêiner desde as 00:46:04 (`CR-10`); o delta-rs, o `read_parquet`, o `S3FileSystem` e o `boto3`
  leram depois da expiração (`CR-3`, `CR-5` a `CR-7`); a conexão Redshift respondeu ao `select 1`
  duas vezes depois da expiração da senha, às 01:31:01 (`CR-8`); e os cinco clientes novos leram
  (`CR-11`).

A partir das 00:27:53, `probes/duckdb_threads.py` rodou inteiro sobre a partição 2026-07-31, como em
2026-09-27 (`cad_lancamentos` com 141.933.948 linhas e 2.331 MB num arquivo; as quatro tabelas com
166.708.072 linhas), com 4, 8, 16, 24, 32 e 40 threads, a razão sobre as 8 do padrão do motor, o
cache de arquivos externos desligado e a melhor de três repetições. A materialização é, desde a
implementação de 2026-09-28, a DDL do modelo e o `INSERT ... BY NAME` do `delta_scan`, no lugar do
`CREATE TABLE AS`, e as sondas de consistência rodaram na mesma máquina durante as repetições com 4
e com 8 threads [inferido pelos horários: a terceira com 4 threads levou 71,541 s, contra 55,308 s
e 57,061 s]:

- **A materialização** levou 55,308 s com 4 threads, 39,558 s com 8, 35,251 s com 16, 34,934 s com
  24, 36,316 s com 32 e 36,310 s com 40, com o pico do processo de 3.401 MB a 6.338 MB (3.955 MB com
  8 e 4.950 MB com 16), contra 52,660 s, 37,215 s, 34,192 s, 35,446 s, 37,838 s e 36,448 s pelo
  `CREATE TABLE AS` em 2026-09-27: 6% a mais com 8 threads e 3% com 16. Com 16 threads, ela foi
  1,12 vez mais rápida que com 8, e com 24, 1,13 vez.
- **A leitura agregada do S3** levou 14,343 s com 4 threads, 7,690 s com 8, 4,535 s com 16, 4,081 s
  com 24, 4,015 s com 32 (1,92 vez mais rápida que com 8) e 4,042 s com 40, como em 2026-09-27.
- **As quatro tabelas** foram mais rápidas em série com 8 threads (48,796 s, contra 52,709 s com 16
  e até 61,494 s com 40); em sessões a mais, como `run.ingest`, levaram 40,367 s com 8 threads,
  40,446 s com 16 e de 40,014 s a 42,554 s com 24 a 40, contra 42,119 s e 39,143 s com 8 e 16 em
  2026-09-27. As sessões a mais ganharam da série de 1,11 vez com 4 threads a 1,52 vez com 40
  (1,21 vez com 8).

**Consequências**: o `COPY` do `appender` pelo manifesto passou no Redshift do alvo, e a mensagem do
arquivo obrigatório ausente entra na [etapa 5](PLAN-STAGE-5.md) e em [`redshift.md`](redshift.md);
o substituto dá a mesma mensagem, com o mesmo SQLSTATE, e `test_appender_copies_the_file_at_close`
segue registrando a mensagem como leitura, sem conferi-la (decisão do usuário de 2026-09-29). Dois
escritores na mesma tabela entram nos dois motores, e as docstrings de `append` e de `appender`
dizem isso, com um teste por motor (decisão do usuário de 2026-09-29, seção seguinte). O custo de
ler os rodapés na publicação da base ficou de 0,5% a 2,0% com um arquivo por partição, e o item sai
de [`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md), com a resposta nesta seção. O padrão de `threads` segue
nas CPUs do processo (instrução do usuário de 2026-09-24): com a materialização pelo `INSERT`, o
dobro ganhou 12% numa tabela, empatou nas sessões a mais e perdeu 8% com as quatro tabelas em série;
[`PLAN.md`](PLAN.md) e a [etapa 4](PLAN-STAGE-4.md) guardam a leitura. Os itens das versões não
correntes, das credenciais, do acesso de leitura e da operação ganham as leituras deste dia; a carga
de uma origem estável passou inteira, e a continuação de uma carga parada e o `vacuum --full` de um
arquivo fora do log seguem sem leitura no alvo.

## O que os testes de dois escritores na mesma tabela mostraram

Pela decisão do usuário de 2026-09-29, as docstrings de `appender` e de `append`, no protocolo
`Engine` e nos dois motores, dizem que dois escritores na mesma tabela entram, e
`test_two_writers_on_the_same_table_both_enter` o confere em `tests/test_engine_duckdb.py` e em
`tests/test_engine_redshift.py` (`redshift`): dois appenders com os `close` ao mesmo tempo, por
uma barreira depois do `write`, em duas sessões a mais e na sessão principal, e um appender numa
sessão a mais ao lado de um `UPDATE` da principal que soma 1 à metade das linhas já gravadas; cada
seção confere as linhas, os ids distintos e a soma de `valor`. Neste contêiner (Linux, 4 vCPUs,
16.094 MB, Python 3.13.12, DuckDB 1.5.5, PyArrow 25.0.1), em 2026-09-29, o do DuckDB passou dez
vezes, com 20.000 linhas por escritor, e o do Redshift seis vezes no substituto, com 1.000, sem
aviso.

Um plugin do pytest fora do repositório trocou o `close` dos appenders por três mutações, uma
seção de cada vez: recusar com `SandboxError` o `close` de uma tabela com outro `close` em voo,
descartar sem erro as linhas desse `close`, e apagar as linhas da tabela antes de inserir as suas.
Nos dois motores, a recusa e o descarte reprovaram as duas seções de dois appenders, e a troca
reprovou a da sessão principal e a do `UPDATE`; na das sessões a mais, a troca passou, porque os
dois `DELETE` rodaram juntos e cada um viu a tabela vazia. A seção do `UPDATE` tem um appender
só, e a recusa e o descarte não a alcançam. No Redshift, a recusa e o descarte envolvem o `close`
inteiro, porque o `_copy_file` já roda sob o lock da transação.

**Consequências**: os dois testes rodam no alvo na próxima bateria pelos comandos de `SUITE.md`, sem
comando novo: o do DuckDB na sessão `-m "not redshift"`, e o do Redshift nas sessões `-m redshift`.

## O que a bateria de 2026-09-29 às 13:31 mostrou no ambiente alvo

Em 2026-09-29, das 13:31 às 14:13 UTC, o usuário rodou no ambiente alvo o bloco "Probes e Testes -
BN" de `SUITE.md`, os cinco probes e as sete sessões do pytest, numa máquina de 8 vCPUs e 15,1 GiB
(Python 3.13.15, DuckDB 1.5.5, deltalake 1.6.6, pyarrow 25.0.1, boto3 1.43.102, `redshift_connector`
2.1.17, `sa-east-1`), sobre a `main` com o PR #106 [inferido: as sessões coletaram
`test_two_writers_on_the_same_table_both_enter`, que só o PR #106 tem]. Os relatórios ficam fora de
`plan/`, com os achados aqui. Nenhum caso e nenhuma checagem falharam, e as leituras repetem as da
bateria anterior ("O que a bateria de 2026-09-28 às 23:09 mostrou no ambiente alvo"), salvo:

- **Os testes de dois escritores passaram no alvo.**
  `test_two_writers_on_the_same_table_both_enter` passou no DuckDB da máquina, na sessão
  `-m "not redshift"` com a raiz local, e no Redshift nas quatro sessões que o coletam, duas da
  suíte Redshift e duas da do motor. Cada sessão aprovou um caso a mais que às 23:09, fora a da
  publicação: `-m "not redshift"` 611 casos em 281,8 s, a suíte Redshift 53 em 687,1 s e em 613,5 s,
  a do motor 11 em 188,0 s e em 167,7 s, e a da publicação 10 em 214,5 s e em 209,4 s. O
  `COPY ... MANIFEST` do arquivo obrigatório ausente falhou de novo com
  `Spectrum Scan Error: File not found` e o SQLSTATE `XX000` nas quatro sessões Redshift
  (`redshift.engine.copy_missing_mandatory_file`), e o `append` de 10 linhas levou 1,52 s e 1,45 s
  na suíte Redshift e 1,79 s e 1,39 s na do motor (`redshift.engine.small_append`).
- **Uma tabela do sandbox ficou no esquema.** A listagem de `svv_all_tables` que o `RS-8` de
  `probes/redshift.py` imprime trouxe 17 tabelas, entre elas `exec_poc_faa78dd7_cad_append_0`, onde
  as de 2026-09-27 e de 2026-09-28 traziam só `teste` e `teste3`, e o `RS-19` a resolveu por nome
  depois do `USE`. A contagem do `RS-8` fica nos nomes que começam por `serialize_db`
  (`TABLE_PREFIX`) e leu 1 de 17, `serialize_db_publications`; as tabelas `exec_` do sandbox ficam
  fora dela. O nome é o da primeira tabela de `probes/consistencia/probe_append_test.py`. Lido em
  2026-09-29 às 16:43 pela identidade da biblioteca, o `sys_query_history` mostra a execução
  `poc-faa78dd7` de 00:29:01 a 00:29:06 UTC: a sessão principal criou só essa tabela, duas sessões a
  mais criaram e apagaram cada uma a sua staging temporária (os dois `append` da primeira rodada da
  seção A), e às 00:29:06 a sessão principal rodou `DROP TABLE IF EXISTS` na tabela, com status
  `success`. A execução parou antes da segunda rodada, e a saída dela não está nos relatórios; a
  rodada `-m redshift` das 00:32, que passou, é outra execução e não deixou tabela. O
  `sys_transaction_history`, que diz se a transação do `DROP` foi confirmada, recusou a identidade
  da biblioteca com `42501`. Neste contêiner, a sonda no substituto, com os avisos impressos
  (`-o log_cli=true --log-cli-level=WARNING`), apagou todas as tabelas sem aviso.
- **O esquema guarda a publicação da bateria anterior.** As outras 16 tabelas são `teste`,
  `teste3`, `teste_query_editor`, `serialize_db_publications` e as 12 `prd_*` da publicação da base
  por `--channel default` de 2026-09-29, e `redshift.engine.control_table_present` leu `True` nas
  quatro sessões Redshift, onde as das 23:09, antes da publicação, liam `False`.
- **O `BK-14` parou no limite da listagem.** `probes/bucket.py` lista as versões sob a raiz em até
  20 páginas de 1.000 entradas, e parou nas 20.000: 1 versão corrente, 10.434 não correntes
  (283.570.979 bytes) e 9.565 marcadores de exclusão, contra 10.301 não correntes (307.133.320
  bytes) e 9.504 marcadores às 23:10. A contagem de `BK-14` passa a ser um piso do acumulado. O
  `RS-12` contou 88 erros de carga em 30 dias, contra 93 às 23:09.

**Consequências**: a frase das docstrings de `append` e de `appender` sobre dois escritores na mesma
tabela vale no alvo pelo teste do pacote, nos dois motores. O `DROP` da tabela que ficou no esquema
rodou com status `success` e não a apagou ("O `redshift_connector` depois de um comando
interrompido"); a listagem do próximo `RS-8` diz se as sessões desta bateria deixaram alguma tabela.
O item das versões não correntes de [`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md) ganha a leitura, como
piso.

## O que a bateria de 2026-09-29 às 17:04 mostrou no ambiente alvo

Em 2026-09-29, das 17:04 às 17:45 UTC, o usuário rodou de novo no ambiente alvo o bloco "Probes e
Testes - BN" de `SUITE.md`, com as mesmas versões das 13:31 e sobre a `main` com o PR #106
[inferido: as sessões aprovaram os mesmos casos]. Às 17:00, o usuário atribuiu a tabela
`exec_poc_faa78dd7_cad_append_0` a uma rodada da sonda de dois escritores que interrompeu, e a
apagou à mão antes do bloco. Os relatórios ficam fora de `plan/`, com os achados aqui. Nenhum caso e
nenhuma checagem falharam, e as leituras repetem as das 13:31 ("O que a bateria de 2026-09-29 às
13:31 mostrou no ambiente alvo"), salvo:

- **A listagem não traz mais a tabela do sandbox.** O `svv_all_tables` das 17:04 listou 16 tabelas,
  as das 13:31 sem `exec_poc_faa78dd7_cad_append_0`: o `DROP` à mão a apagou, e as sessões das 13:31
  às 14:13 não deixaram tabela `exec_`. O `RS-8` leu de novo 1 de 16, `serialize_db_publications`.
  As sessões das 17:07 às 17:45 rodaram depois do probe, e só a listagem da próxima bateria diz se
  deixaram alguma tabela.
- **Os casos repetiram as contagens das 13:31.** A sessão `-m "not redshift"` aprovou 611 casos em
  275,7 s, a suíte Redshift 53 em 631,5 s e em 625,4 s, a do motor 11 em 165,6 s e em 180,0 s, e a
  da publicação 10 em 210,4 s e em 204,9 s. O `COPY ... MANIFEST` do arquivo obrigatório ausente
  falhou de novo com `Spectrum Scan Error: File not found`, e o `append` de 10 linhas levou 1,46 s e
  1,54 s na suíte Redshift e 1,76 s e 1,47 s na do motor.
- **O `RS-12` contou 100 erros de carga em 30 dias**, contra 88 às 13:31, e o `BK-14` parou de novo
  no limite da listagem, com 10.481 versões não correntes (283.880.162 bytes) e 9.518 marcadores de
  exclusão.

**Consequências**: a sobra de tabela do sandbox não se repetiu nas sessões das 13:31 às 14:13, e a
das 00:29 fica atribuída pelo usuário à interrupção ("O `redshift_connector` depois de um comando
interrompido"). A contagem do `RS-8` não enxerga as tabelas `exec_` do sandbox, que só a listagem
mostra, e fica assim por decisão do usuário de 2026-09-29.

## O `redshift_connector` depois de um comando interrompido

Em 2026-09-29, neste contêiner, um servidor falso do protocolo de linha do PostgreSQL, num script do
scratchpad que não foi guardado, atendeu o `redshift_connector` 2.1.17 conectado como o motor o
conecta (`max_prepared_statements=0`, autocommit ligado, sem SSL). O servidor respondia a cada
`SELECT` com uma linha que repete o texto do comando e demorava 2 s no `SELECT 'lento'`. Um SIGINT
enviado ao processo 0,5 s depois do início desse comando interrompeu o `execute` dele com
`KeyboardInterrupt`, e os comandos seguintes na mesma conexão voltaram sem erro e um passo
atrasados:

- **Um `SELECT` volta sem linhas.** `SELECT 'depois'` esperou os 2 s do lento e voltou `()`, e
  `SELECT 'fim'`, depois do `DROP`, também.
- **Um `DROP` volta sem ler a resposta da execução dele.** `DROP TABLE IF EXISTS t` voltou ao ler a
  resposta do `Parse` dele, com a da execução ainda por ler.
- **O servidor executou todos os comandos, na ordem**, e recebeu o `Terminate` do `close` depois do
  último.

O driver lê cada fase até o primeiro `ReadyForQuery` (`Connection.handle_messages`) e não marca a
conexão quando uma exceção sai no meio da leitura, e o motor reusa a conexão no comando seguinte
(`RedshiftEngine._run`).

**Consequências**: a hipótese do `DROP` das 00:29 ganha o lado do cliente, reproduzido: depois de um
comando interrompido, o `cleanup` roda o `DROP` numa conexão cujas respostas chegam um comando
atrasadas, e a fecha sem ler a resposta da execução do último. Por que o Redshift não confirmou esse
`DROP` fica sem leitura. O usuário decidiu em 2026-09-29 deixar o motor como está: depois de um
Ctrl+C no meio de um comando, o motor Redshift é recriado à mão, e uma rodada interrompida pode
deixar tabela `exec_` no esquema ([etapa 5](PLAN-STAGE-5.md)).

## O que o caso do runbook de refazer um snapshot mostrou

A pedido do usuário, em 2026-09-30, `test_redo_a_snapshot_and_revert_by_the_channel`, em
`tests/test_publication.py` (`redshift`, `s3` e `local`), roda o runbook "Refazer um snapshot" de
[`docs/operacao.md`](../docs/operacao.md) de ponta a ponta: o snapshot `2026T3` publicado pelo canal
`default`; a `Execution` que refaz a partição `2026-08-31`, marcada com o snapshot `2026T3.r2`; o
canal movido para ele e a publicação pelo canal; a volta do canal ao `2026T3` e a publicação de
novo, com as linhas conferidas no Redshift e no leitor Delta sem argumento a cada passo; e, depois
da volta, a partição refeita no leitor do canal `current` e no snapshot seguinte. Neste contêiner
(Linux, 4 vCPUs, 16.094 MB, Python 3.13.12, DuckDB 1.5.5, PyArrow 25.0.1, deltalake 1.6.6), no
substituto, a sessão só com ele passou em 5,0 s.

As publicações do caso citam `cad_lancamentos_projetados` em `--tables`, porque o ambiente do teste
não tem as outras tabelas do modelo, e `serialize-db publish_redshift --channel default` sem ele
saiu com 2: `cad_contas, cad_lancamentos: fora do snapshot 2026T3; --tables deixa de fora a tabela
sem versão`.

Uma mutação de `_versions_to_publish`, em `serialize_db.cli`, que lê qualquer canal como o
`current`, reprovou o caso novo na volta, com a partição publicada nos ids a partir de 1000 no lugar
de 41. `test_cli_publishes_by_channel_and_snapshot_and_reverts` reprovou só no `--channel nada`, que
saiu com 0 no lugar de 2, porque ele volta por `--snapshot`.

**Consequências**: a volta pelo canal e a correção que continua na versão atual do Delta têm um
caso, que entra no alvo na próxima bateria pelo comando da suíte da publicação de `SUITE.md`, sem
comando novo.

## O que a bateria de 2026-09-30 às 14:58 mostrou no ambiente alvo

Em 2026-09-30, das 14:58 às 15:42 UTC, o usuário rodou no ambiente alvo o bloco "Probes e Testes -
BN" de `SUITE.md`, os cinco probes e as sete sessões do pytest, sobre a `main` com os PRs #109, #110
e #111 [inferido: cada sessão coletou os casos que essa `main` coleta, 614, 54, 11 e 11], numa
máquina de 8 vCPUs e 15,3 GiB, com as versões das baterias de 2026-09-29. Os relatórios ficam fora
de `plan/`, com os achados aqui. Nenhum caso e nenhuma checagem falharam, e as leituras repetem as
da bateria de 2026-09-29 às 17:04 ("O que a bateria de 2026-09-29 às 17:04 mostrou no ambiente
alvo"), salvo:

- **O runbook de refazer um snapshot passou no alvo.**
  `test_redo_a_snapshot_and_revert_by_the_channel` passou nas quatro sessões que o coletam, as duas
  da suíte Redshift e as duas da publicação, com o banco Delta sob a raiz S3 da suíte e as tabelas
  publicadas no esquema do alvo ("O que o caso do runbook de refazer um snapshot mostrou"). A suíte
  Redshift aprovou 54 casos em 748,9 s e em 674,3 s, e a da publicação 11 em 248,1 s e em 236,7 s,
  um a mais que em 2026-09-29 em cada.
- **Os casos novos da sessão `-m "not redshift"` rodam sem rede.** A sessão aprovou 614 casos em
  274,9 s, três a mais que em 2026-09-29: os de `tests/test_storage.py` que dão a `ConflictError` ao
  412 e ao 409 da escrita condicional e deixam subir os outros erros, pelo cliente dublê, sem ler o
  S3 do alvo. A suíte do motor aprovou 11 casos em 168,4 s e em 169,1 s; o `COPY ... MANIFEST` do
  arquivo obrigatório ausente falhou de novo com `Spectrum Scan Error: File not found`, e o `append`
  de 10 linhas levou 1,66 s e 1,50 s na suíte Redshift e 1,67 s e 1,39 s na do motor.
- **A listagem repete a de 2026-09-29 às 17:04.** O `svv_all_tables` das 14:58 listou as mesmas 16
  tabelas, sem `exec_`, e o `RS-8` leu 1 de 16: nenhuma sessão entre as duas listagens, as de
  2026-09-29 das 17:07 às 17:45 entre elas, deixou tabela do sandbox. As sessões desta bateria
  rodaram depois do probe, e só a listagem da próxima diz se deixaram alguma.
- **O `RS-12` contou 112 erros de carga em 30 dias**, contra 100 às 17:04, e o `BK-14` parou de novo
  no limite da listagem, com 10.460 versões não correntes (283.526.691 bytes) e 9.539 marcadores de
  exclusão.

**Consequências**: o caso do runbook de refazer um snapshot, com a volta pelo canal, sai dos que
esperam o alvo em [`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md). A sobra de tabela do sandbox não se
repetiu desde a rodada interrompida de 2026-09-29 às 00:29.

## O que as sondas da operação mostraram na pasta local e no substituto

Em 2026-09-30, neste contêiner (Linux x86_64, 4 vCPUs, 13,2 GiB disponíveis, Python 3.13.12,
deltalake 1.6.6, DuckDB 1.5.5, PyArrow 25.0.1), as sondas de `probes/operacao/`, escritas a pedido
do usuário para as leituras que [`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md) espera no alvo, rodaram
sobre `cad_lancamentos` da base fictícia de `tests/source_db_projetado.py`, repetida com os ids
deslocados: quatro partições de 3.008.000, 3.000.000, 3.000.000 e 60 linhas, as três primeiras num
arquivo de cerca de 12 MB cada. As quatro que não pedem o Redshift rodaram na pasta local, e as
cinco no substituto, com o moto no lugar do S3 e `tests/emulator.py` no lugar do Redshift; as da
carga e do `archive` rodaram duas vezes em cada lugar. Todas as checagens passaram.

- **A carga parada deixa na pasta o arquivo da partição interrompida.** Na pasta local, o arquivo
  da segunda partição apareceu 0,2 s depois da linha da primeira no log, o `SIGKILL` que veio em
  seguida chegou antes do commit dela, e o arquivo ficou fora do log nas duas rodadas. No moto, onde
  o objeto só aparece quando o envio termina, 0,8 s e 0,9 s depois da linha, o commit da segunda
  partição chegou ao log antes do sinal nas duas. A repetição do comando gravou só as partições
  ausentes do log, numa execução nova, com um arquivo por partição no log. A pasta temporária do
  motor DuckDB, `serialize_db_*` em `tempfile.gettempdir()` com o `<execution_id>.duckdb`, ficou
  para trás em todas as rodadas, com 0,0 MB: o `cleanup` não roda depois do `SIGKILL`.
- **`serialize-db load --partitions` conferia a origem inteira.** A repetição de três das quatro
  partições imprimiu `DIFERENÇA em 2026-06-30: origem 60 linhas {...}, Delta ausente` e o veredito
  `com diferenças`, e saiu com 1, com as três pedidas iguais nos dois lados: `_load` chamava
  `load_report(db, table, args.source)`, que não recebia as partições pedidas.
- **O `archive` copia as partições na ordem das ações do log, e a repetição continua a cópia.** A
  primeira copiada foi a 2026-03-31, a última gravada, e depois a 2026-02-28 e a 2026-01-31. O
  `SIGKILL` depois da linha da primeira deixou o snapshot em `snapshots`, e na pasta local a cópia
  da seguinte já tinha começado: dois arquivos na pasta da cópia, contra um no moto. A repetição
  imprimiu `partição 2026-03-31 já no destino`, copiou as outras duas, gravou a versão 3 no arquivo
  e moveu o snapshot para `archived`, com os arquivos de cada partição iguais aos da versão e nenhum
  órfão, em 0,2 s e com o pico de RSS de 242 MB e 243 MB na pasta local, e em 0,7 s e com 280 MB no
  moto.
- **O `vacuum --full` só lista o arquivo fora do log mais velho que a retenção.** Com a retenção
  padrão de 9.600 horas, os 400 dias, os dois órfãos recém-gravados, um na pasta da partição e outro
  num prefixo dentro dela, ficaram fora da lista; com `--retention-hours 0` os dois foram listados,
  e só eles; com `--apply` os dois foram apagados, a tabela ganhou os commits `VACUUM START` e
  `VACUUM END`, da versão 1 à 3, e as linhas e a soma de `id_lancamento` pelo `delta_scan` não
  mudaram. Sem `--apply`, a versão não mudou.
- **O `compact` juntou a partição repartida num arquivo em ZSTD.** A partição de 3.008.000 linhas,
  14,5 MB no arquivo da carga, repartida pelo `COPY ... FILE_SIZE_BYTES` do DuckDB em 24 arquivos de
  até 1,2 MB, virou um arquivo de 6,7 MB num commit `OPTIMIZE`, com as mesmas linhas e soma: em
  0,6 s e com o pico de RSS de 295 MB na pasta local, e em 0,8 s e com 312 MB no moto, com 13,0 GiB
  a 13,1 GiB disponíveis antes. O arquivo da carga, do `COPY` do DuckDB, sai em SNAPPY, e o do
  `compact`, `part-00000-<uuid>-c000.zstd.parquet` com o `created_by` `delta-rs version py-1.6.6`,
  em ZSTD, o padrão do `optimize.compact`, que `delta.compact` chama sem `writer_properties`. O
  `COPY` da publicação de um arquivo ZSTD não rodou no alvo; a página dos arquivos do Redshift
  Spectrum, que o `COPY` de Parquet usa, diz que o Redshift lê o zstd no Parquet.
- **O `UNLOAD` do substituto grava um arquivo nos dois modos.** O substituto traduz o `UNLOAD` num
  `COPY` do DuckDB, com um arquivo com `PARALLEL OFF` e sem ele: as checagens de linhas e de arquivo
  único passaram, e os tempos, 0,4 s para 1.000.000 de linhas e de 1,3 s a 1,4 s para as 3.008.000
  nos dois modos, não medem o Redshift.

**Consequências**: as cinco sondas esperam a rodada no alvo, pelos comandos de `SUITE.md`, seção
"Sondas da operação", e o `COPY` de uma partição compactada entra no item da operação no alvo de
[`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md). O usuário decidiu em 2026-10-01 que o relatório de
`serialize-db load --partitions` confere só as partições pedidas: `load_report` recebe
`partitions`, que o subcomando e o script passam, e a sonda da carga exige da repetição a saída 0
e a linha `3 partição(ões) conferida(s), contagens e somas iguais`. Em 2026-10-01 a sonda passou
assim na pasta local e no substituto, e reprovou contra o subcomando anterior, com a
`DIFERENÇA em 2026-06-30` e a saída 1 ([etapa 7](PLAN-STAGE-7.md)).

## O que a suíte do pacote mostrou no Windows

Em 2026-10-01, o usuário relatou o `import serialize_db` falhando no Windows, num projeto cliente,
com `ModuleNotFoundError: No module named 'resource'` em `serialize_db/resources.py`. A esteira
`tests.yml` ganhou um job `windows-latest` (Windows-2025Server-10.0.26100-SP0, Python 3.13.15,
deltalake 1.6.6, DuckDB 1.5.5, PyArrow 25.0.1, com a raiz local sob
`D:\a\serialize-db\serialize-db`), disparado à mão três vezes no ramo da correção; o job Ubuntu
passou nas três.

- **O Windows não tem o módulo `resource` nem o `os.sysconf`.** `resources.py` importava
  `resource` no topo, para o `ru_maxrss` de `peak_rss_mb`, e `available_memory` e o relatório de
  `scripts/migrate_parquet_to_delta.py` liam a memória física por `os.sysconf`; o import de
  `serialize_db` passa por `cli.py`, `delta.py` e `resources.py` e falhava antes de qualquer uso.
  No Windows, `resources.py` lê pelo `ctypes` a memória física e a disponível de
  `GlobalMemoryStatusEx` (`ullTotalPhys` e `ullAvailPhys`, que conta como livre a lista de espera)
  e o pico de `K32GetProcessMemoryInfo` (`PeakWorkingSetSize`). No runner, a física conferiu com o
  `TotalPhysicalMemory` do WMI (`Win32_ComputerSystem`), e a disponível ficou positiva e abaixo
  dela. A importação de cada módulo e o `serialize-db --help` passaram nas três rodadas.
- **A primeira rodada teve 328 casos aprovados, 22 reprovados e 79 pulados.** A raiz local
  guardava o caminho com `\` (`D:\a\...`), e o `LocalFileSystem` do PyArrow lista os arquivos com
  `/`: `list_files` devolvia o caminho absoluto no lugar do relativo, que `join` juntava à raiz de
  novo (`WinError 123` na cópia profunda, na leitura do Delta e na limpeza do `UNLOAD` do
  substituto), e `relative` recusava a pasta de staging do leitor como fora da raiz.
- **O `RETURN_STATS` do `COPY` particionado do DuckDB junta a pasta da partição ao destino com
  `\`** (`.../reescrita\data_str=2026-07-31\data_0.parquet`), e o registro recusava o arquivo da
  reescrita como fora da pasta da tabela (`RegistrationRefused`). O `COPY` sem partição devolve o
  destino como o recebeu.
- **As outras reprovações da primeira rodada.** Os quatro casos de
  `scripts/migrate_parquet_to_delta.py` pararam no `os.sysconf` do relatório.
  `test_engine_config_and_single_session` esperava 1,0 GiB do `/proc` fabricado e leu 6,5 GiB: no
  Windows, `available_memory` devolvia a leitura da API sem ler o `/proc`, e os casos do `/proc`
  fabricado de `tests/test_resources.py` estavam pulados lá. Nos testes, a URI
  `file:///tmp/serialize-db/delta` não é absoluta no Windows (`URI is not absolute`), o arquivo
  novo tem o modo 0o666 contra os 0o644 esperados, e o `os.path.join` monta o cabeçalho do diff dos
  arquivos gerados com `\`.
- **A segunda rodada teve 352 aprovados, 2 reprovados e 75 pulados.** A exportação por reescrita
  devolvia o caminho do `RETURN_STATS` com `\`, e o teste da descoberta das partições esperava a
  raiz com `\`. Os casos do `/proc` fabricado rodaram no Windows.
- **A terceira rodada passou com 354 aprovados e 75 pulados, em 143,8 s.** Os pulados são os
  casos `s3` e `redshift`, sem as variáveis, o do modo dos arquivos e o da memória física sem
  `/proc`, que no Windows é a disponível.
- **A rodada de 2026-10-01 sobre o merge do PR #120 (f167867) passou com 362 aprovados e 76
  pulados no Windows, em 150,8 s, e com 363 aprovados e 75 pulados no Ubuntu, em 129,7 s.** Os
  nove casos a mais sobre a terceira rodada são os do PR: a partição pedida que a origem não tem
  (dois em `test_load.py` e um do script), a coluna fora do contrato no registro (o caso `local`
  aprovado e o `s3` pulado, o pulado a mais de cada plataforma), a troca de `write_text` que
  falha sem deixar o temporário, a coluna do Delta fora do modelo no `ingest` materializado, a
  auditoria com lista numa tabela sem partição e a memória abaixo de 2 MiB.

**Consequências**: no Windows, `resources.py` lê a memória e o pico pela API do sistema, e
`physical_memory`, protegida, dá a memória física ao script de migração; `available_memory` fica
com a menor das leituras, as do `/proc` e do cgroup também, quando existem. A raiz local passa pelo
`as_posix`, e `delta._return_stats_path` troca `\` por `/` no caminho do `RETURN_STATS`, no registro
e na exportação. A esteira roda no Ubuntu e no Windows. No Linux, os caminhos e as leituras não
mudam. O S3, o Redshift, os probes, `prepare_offline.sh` e o projeto cliente não rodaram no
Windows, e a variável `username` do proxy fica pendente em [`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md).

## O que a pasta da execução no teste do pipeline mostrou

Em 2026-10-03, na pasta local do contêiner de desenvolvimento (Linux x86_64, DuckDB 1.5.5,
deltalake 1.6.6, pyarrow 25.0.1, SQLAlchemy 2.0.54, Python 3.13.14), a pergunta do usuário sobre
uma API que guarde, por `execution_id`, a cópia das entradas e os arquivos intermediários de um
pipeline levou a pasta da execução a `tests/test_pipeline.py`, como código cliente:
`<ambiente>/execucoes/<execution_id>/`, com `entradas/`, `relatorios/` e, na falha, `geracao/`. Os
contratos do mês chegam em um CSV por sistema em `<ambiente>/recebidos/<partição>/`.

- **O CSV dos contratos ida e volta pelo pyarrow**: os 41 contratos de 2026-06-30, sem
  `id_contrato` e `data`, gravados por `pyarrow.csv.write_csv` em um arquivo por sistema (15, 43 e
  89, com 14, 14 e 13 linhas) e lidos por `read_csv` com os tipos de `arrow_schema(cad_contratos)`
  em `column_types`, voltaram iguais (`Table.equals`), com os 11 `-0.0` de `taxa_juros_fixos` e
  os nulos de `estagio` (1) e de `data_assinatura` (22).
- **A inferência de tipos do CSV decide pelo conteúdo, com ou sem aspas**: sem `column_types`, a
  coluna `"01","02"` foi lida como `int64`. O `to` dos contratos tem os códigos `01`, `02` e `05`
  ao lado de `RC`, `RI`, `ZB`, `ZD` e `ZT`.
- **O nulo e o texto vazio**: `write_csv` grava o nulo como campo vazio e o texto vazio como `""`.
  Na leitura de uma coluna `string`, o padrão dá texto vazio aos dois, `strings_can_be_null=True`
  dá nulo aos dois, e só com `quoted_strings_can_be_null=False` também os dois voltam separados.
  O teste entrega um `fonte_familia` vazio, que sem a segunda opção chegou nulo ao Delta.
- **A reexecução de um mês publicado**: `run.previous_partitions(tabela, n)` conta a partição da
  execução quando ela já está na versão fixada, e o `last_base_date` do teste, que pedia uma, leu
  2026-07-31 como a data-base anterior a 2026-07-31; a reexecução levou a partição para ela mesma e
  parou em `AuditFailed: cad_operacoes em ['2026-07-31']: reprovada em ['chave_data_operacao']`.
  Com as duas últimas e a anterior à da execução, a reexecução leu 2026-06-30 e publicou uma
  versão a mais de cada tabela, com as mesmas contagens e só a partição dela alterada.
- **A ingestão só do que o pipeline lê**: `cad_contas` como view e a última data-base de
  `cad_operacoes`, `rel_contrato_operacao` e `cad_lancamentos`, materializada, com `cad_contratos`
  criada vazia por `create_table`, no lugar das 11 tabelas de antes. As auditorias com
  `foreign_keys=True` rodaram toda verificação, com `not_run` vazio, e leram `dom_veiculos`,
  `dom_mensuracoes`, `dom_segmentos` e `dom_negocios` na versão fixada.
- **A falha**: um contrato repetido na entrega reprovou `cad_contratos` em
  `chave_data_sistema_contrato`, e nenhuma tabela ganhou versão. A pasta da execução guardou as três
  cópias da entrega, os dois relatórios, com o rateio do contrato repetido somando 2, e a partição
  gerada de cada tabela, lida por `stream` e gravada por `pq.ParquetWriter`, com 42 contratos.
- **Os tempos**: os quatro casos levaram 11,5 s, dos quais 4,2 s na carga da base fictícia, e a
  execução com a pasta, 1,4 s.

**Consequências**: `tests/test_pipeline.py` passa a ter quatro casos, com os comentários das
recomendações de uso do pacote; revisados `plan/PLAN-STAGE-6.md` e `plan/CURRENT_STATE.md`. A
pasta da execução fica no código cliente, sem API no pacote, e a decisão de levá-la ao pacote
espera o usuário em [`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md).

## O que a troca das versões das dependências de 2026-10-03 mostrou

Em 2026-10-03, no contêiner de desenvolvimento (Linux x86_64, 4 vCPUs e 16 GB, Python 3.13.14), a
API JSON do PyPI tinha versões novas de cinco dependências fixadas em `pyproject.toml`: boto3
1.43.108 (o pino era 1.43.102), DuckDB 1.5.6 (1.5.5), de 2026-09-28, SQLAlchemy 2.1.3 (2.0.54), de
2026-10-02, ruff 0.16.10 (0.16.9) e sqlglot 30.21.0 (30.19.0). As outras estavam na última versão:
deltalake 1.6.6, duckdb-engine 0.17.0, PyArrow 25.0.1, redshift-connector 2.1.17,
sqlalchemy-redshift 1.0.0, pandas 3.0.6, flask 3.1.3, flask-cors 6.0.5, moto 5.2.3 e pdoc 16.0.0;
os limites inferiores resolveram pytest 9.1.1 e ipykernel 7.4.0, e o intervalo do `uv_build`, a
0.12.23. O botocore, que o boto3 traz sem pino, veio 1.43.108 também com o pino antigo. Cada
combinação rodou numa cópia do repositório, sem as variáveis `AWS_*`, nas três sessões de testes
(sem variável, com `SERIALIZE_DB_TEST_LOCAL_ROOT` e com ela e `SERIALIZE_DB_TEST_EMULATOR`), depois
do `ruff check` e do `ruff format --check`.

- **Os pinos da `main`.** 255 aprovados e 435 pulados sem variável, 571 e 119 com a raiz local, e
  687 aprovados, 2 pulados e 1 reprovado com o substituto: o caso de estudo do GIL,
  `test_gil_reacquisition_waits_the_switch_interval`, que reprova em algumas sessões e passa nas
  outras, como na seção das correções dos achados da revisão, acima.
- **boto3, ruff e sqlglot.** Com boto3 1.43.108, ruff 0.16.10 e sqlglot 30.21.0, os mesmos números,
  com o caso do GIL reprovado na sessão sem variável e aprovado com o substituto (688 aprovados e 2
  pulados). O `ruff check` e o `ruff format --check` da 0.16.10 passaram nos 89 arquivos.
- **DuckDB 1.5.6.** Com as três trocas acima e o DuckDB 1.5.6, os mesmos números, com o caso do GIL
  reprovado só sem variável. O `INSTALL` trouxe as extensões `delta` 6059958 e `httpfs` 4bc690d para
  `.duckdb/v1.5.6/linux_amd64/`; as da 1.5.5, `45c4087` e `827222f`, ficam em `.duckdb/v1.5.5/`, e a
  pasta preparada para a 1.5.5 não tem extensão para a 1.5.6. O anúncio da 1.5.6 lista correções de
  resultado (o `LIMIT` empurrado por uma projeção volátil, o `UNNEST`, a eliminação de Top-N em
  janelas, a poda de partições Hive, a contagem de valores do Parquet v2) e as aspas em todo
  identificador das mensagens de erro; nenhum teste do pacote mudou de resultado.
- **SQLAlchemy 2.1.3.** Com as três trocas e a 2.1.3: 251 aprovados, 4 reprovados e 435 pulados sem
  variável; 565, 6 e 119 com a raiz local; 679, 9 e 2 com o substituto. A 2.1.3 corrigiu o `NULL`
  dos valores de `params()` sob `literal_binds` (#13635), e os valores saem como literais. Seguem:
  - o `construct_params()` de um statement com valores de `params()`, compilado com
    `render_postcompile`, levanta `InvalidRequestError` ("can't construct new parameters when
    render_postcompile is used"), porque junta os valores guardados no statement e recusa qualquer
    conjunto: `test_statement_parameters_expand_in_lists`, do motor DuckDB, e o estudo
    `test_in_list_needs_render_postcompile_on_the_engine_path`. Numa sonda, o
    `construct_expanded_state()` do statement compilado sem `render_postcompile` deu o mesmo texto e
    os mesmos valores na 2.0.54 e na 2.1.3, pelo `duckdb_engine.Dialect` com `qmark` e pelo
    `RedshiftDialect_redshift_connector` com `named`;
  - o `_backslash_escapes` do dialeto PostgreSQL, que os dialetos do DuckDB e do Redshift herdam,
    passou de verdadeiro a falso, e o literal deixou de dobrar a contrabarra: `'a\:b'` no lugar de
    `'a\\:b'`. No substituto, que lê a contrabarra como escape, como o Redshift (leitura de
    2026-09-23), o `stream` pelo `UNLOAD` leu 0 linhas onde o `query` leu 1, sem erro
    (`test_stream_literal_values_on_the_target` e
    `test_stream_and_query_agree_on_a_colon_inside_a_literal`), e `test_stream_literal_values` e
    `test_literal_text_keeps_the_backslash_before_a_colon_in_a_value` acharam o texto sem a
    contrabarra dobrada;
  - o `Double` fora de `Numeric` tirou de novo a coluna `fator` das somas do `import_report`
    (`test_import_report_matches_and_detects_a_difference`);
  - o `params()` mantém o `required` do `bindparam`: o guarda do estudo
    `test_stream_by_unload_with_literal_values` achou `texto` depois dele, e, numa sonda, um
    statement que o cliente passou por `params(x=1)` foi recusado por `bound_statement` com
    `SqlError`, e o `render` o escreveu com `:x` no lugar de `1`;
  - a reflexão do duckdb-engine 0.17.0 seguiu falhando na `pg_catalog.pg_collation`
    (`test_create_all_and_reflection`).
- **A contrabarra das constantes no DuckDB com a 2.0.54.** O `_backslash_escapes` verdadeiro dobra a
  contrabarra da constante `a\b` no texto de `render(..., "duckdb")`, `'a\\b'`, e o DuckDB, que não
  a trata como escape, lê as duas: o `WHERE "t"."b" = 'a\\b'` não achou a linha `a\b` na 2.0.54 e a
  achou na 2.1.3. O motor DuckDB passa os valores como parâmetros, mas o mesmo `_backslash_escapes`
  vale para as constantes que o texto dele embute: o `like(r"a\%b", escape="\\")` saiu `ESCAPE
  '\\'`, que o DuckDB recusa com `Invalid escape string. Escape string must be empty or one
  character.`, e o `bindparam("v", r"a\b", literal_execute=True)` saiu `= 'a\\b'` e não achou a
  linha. A `Connection` do cliente sobre o duckdb-engine recusa o mesmo `ESCAPE`: o `initialize` do
  duckdb-engine pula o do PostgreSQL, e o `_backslash_escapes` segue verdadeiro depois de conectar.
  Nenhum teste cobria uma constante com contrabarra.
- **O ajuste do pacote.** Em duas cópias a mais com boto3, ruff e sqlglot novos, com
  `construct_expanded_state()` nos dois motores, `(sa.Numeric, sa.Float)` nas somas do
  `import_report` e `_backslash_escapes` explícito, verdadeiro nos dialetos do Redshift de
  `serialize_db.sql` e de `serialize_db.engine.redshift` e falso no do DuckDB de
  `serialize_db.sql`: com a 2.1.3, 253, 569 e 685 aprovados nas três sessões, e só
  reprovaram os estudos da reflexão, do `render_postcompile` e, com o substituto, do `required`
  depois do `params()`; com a 2.0.54, 255, 571 e 688 aprovados, sem reprovação. A sonda do
  `render` deu, nas duas versões, o texto do DuckDB com uma contrabarra e o do Redshift com duas.
- **A correção da contrabarra no DuckDB.** Numa cópia do branch com `_backslash_escapes` falso no
  dialeto do DuckDB de `serialize_db.sql` e no `qmark` de `serialize_db.engine.duckdb`, o `render`
  escreveu `'a\b'`, o motor escreveu `ESCAPE '\'`, e os três casos acharam as linhas; o texto do
  Redshift seguiu com a contrabarra dobrada. Os testes novos,
  `test_render_writes_the_backslash_as_each_engine_reads_it` e
  `test_query_keeps_backslash_literals`, reprovaram no código anterior, um pelo texto
  `= '1\\2'` e o outro pelo `Invalid escape string`, e passaram com a correção: 256 aprovados e
  436 pulados sem variável, 573 e 119 com a raiz local, e 690 e 2 com o substituto.

**Consequências**: `pyproject.toml` fixa boto3 1.43.108, DuckDB 1.5.6, ruff 0.16.10 e sqlglot
30.21.0, e a pasta preparada para o ambiente alvo precisa do `prepare_offline.sh` de novo, pelas
extensões da 1.5.6. A SQLAlchemy fica em 2.0.54, por decisão do usuário do mesmo dia, e o item dela
em [`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md) traz a 2.1.3; a contrabarra das constantes no DuckDB,
que o usuário mandou corrigir no mesmo dia, sai com `_backslash_escapes` falso nos dois dialetos do
DuckDB do pacote, como registram [`PLAN-STAGE-2.md`](PLAN-STAGE-2.md) e
[`PLAN-STAGE-4.md`](PLAN-STAGE-4.md).

## O que os wheels das extensões do DuckDB no PyPI mostraram

Em 2026-10-04, depois do merge do PR #125, o usuário apontou que os clientes do pacote instalam as
extensões do DuckDB pelos wheels `duckdb-extension-delta` e `duckdb-extension-httpfs` do PyPI, que
não tinham a 1.5.6. Na API JSON do PyPI, a última versão de `duckdb-extension-delta`,
`duckdb-extension-httpfs` e `duckdb-extension-aws` era a 1.5.5, de 2026-08-10, e cada versão exige
o `duckdb` da mesma versão (`requires_dist` `duckdb==1.5.5`); o DuckDB 1.5.6 é de 2026-09-28. O
`uv pip compile` de `duckdb==1.5.6` com os dois wheels sem versão resolveu sem erro para
`duckdb-extension-delta` 1.0.3 e `duckdb-extension-httpfs` 1.0.3; com `duckdb==1.5.5`, para a 1.5.5.
No contêiner de desenvolvimento, com o DuckDB 1.5.5 e o resto da `main`, o `INSTALL` trouxe de novo
`delta` 45c4087 e `httpfs` 827222f para `.duckdb/v1.5.5/linux_amd64/`, o `ruff check` e o
`ruff format --check` passaram, e as três sessões, sem as variáveis `AWS_*`, deram 256 aprovados e
436 pulados sem variável, 573 e 119 com a raiz local, e 690 e 2 com o substituto, os números da
`main` com o DuckDB 1.5.6.

**Consequências**: `pyproject.toml` volta a fixar o DuckDB 1.5.5, e a troca para a 1.5.6 espera os
wheels das extensões no PyPI, no item de [`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md). A pasta preparada
para a 1.5.5 no ambiente alvo tem as extensões da versão fixada.

## O que a revisão de bugs de 2026-10-04 mostrou

Em 2026-10-04, a pedido do usuário, a revisão de `src/serialize_db` na `main` (`fec8015`) procurou
bugs na consistência da informação nas leituras, nas escritas e na publicação, com uma sonda no
contêiner de desenvolvimento para cada hipótese (Linux x86_64, DuckDB 1.5.5, deltalake 1.6.6).

- **O `COPY` repetido pela reconexão do motor Redshift.** `RedshiftEngine.execute` reabre a
  conexão derrubada fora de transação e repete o comando, e o `InterfaceError` do driver não diz
  se o servidor aplicou o comando antes da queda. No substituto, uma conexão de mentira que
  derruba o primeiro `COPY`, ou o primeiro `INSERT`, depois de passá-lo ao DuckDB fez o `ingest`
  de uma partição de 120 linhas deixar 240 no sandbox, nos dois casos. Com a carga de cada
  partição numa `transaction()`, do `DELETE` da staging ao `INSERT`, a queda subiu como
  `InterfaceError` depois do `ROLLBACK`, a tabela ficou criada e sem linha, e o `USE` da conexão
  rodou uma vez. O teste novo, `test_ingest_partition_load_raises_on_a_dropped_connection_without_retry`,
  reprovou no código anterior (`DID NOT RAISE`), e a asserção nova de
  `test_ingest_loads_each_partition_through_the_staging`, a sequência `BEGIN`, `DELETE`, `COPY`,
  `INSERT`, `COMMIT`, também. O `appender` já carregava numa transação, e a conexão da publicação
  não reconecta.
- **As estatísticas de texto truncadas.** O `COPY ... RETURN_STATS` do DuckDB 1.5.5 corta o
  mínimo e o máximo de um texto acima de 256 bytes, no `RETURN_STATS` e no rodapé, pela regra
  que fecha com os casos das duas sondas, a desta revisão e a da revisão do pdoc (PR #127): o
  mínimo é o prefixo de até 256 bytes em fronteira de caractere (`a` × 254 seguido de `é` e de
  `z` × 10 dá `a` × 254 e `é`, 256 bytes; `a` × 255 seguido de `é` dá `a` × 255); o máximo é o
  prefixo de 256 bytes sem os bytes fora do ASCII e sem um byte 127 do fim, com o último byte
  incrementado (`x` × 257 dá `x` × 255 e `y`; `~` × 300 dá `~` × 255 e `\x7f`; `a` × 254
  seguido de `é` × 100 dá `a` × 253 e `b`, 254 bytes; `a` × 255 seguido de `\x7f` × 100 dá
  `a` × 254 e `b`; `é` × 100, `a` e `é` × 100 dá `é` × 100 e `b`, 201 bytes; `c` × 300 e `z` dá
  `c` × 255 e `d`); sem byte ASCII no prefixo de 256 bytes do mínimo ou do máximo, os dois saem
  omitidos, com `has_min_max` falso no rodapé (`é` × 300, `é` × 128 seguido de `z`, `\x7f` × 300,
  `\u07ff` × 200, `\U0010ffff` × 100, e `é` × 300 como mínimo com `z` como máximo); `é` × 128,
  256 bytes exatos, sai inteiro. Os limites nunca excluem o valor real. O PyArrow 25 não expõe
  `is_min_value_exact` e `is_max_value_exact` do rodapé, e a leitura de 2026-09-22, de um texto
  de 41 caracteres transcrito exato, vale só abaixo do corte. O `write_deltalake` do delta-rs
  1.6.6 guarda no log e no rodapé um prefixo de 64 bytes em fronteira de caractere, o máximo com
  o último caractere incrementado (`a` × 300 dá `a` × 64 como mínimo; `é` × 300 dá `é` × 31 e
  `ê` como máximo), e o valor inteiro quando o incremento não existe (`\U0010ffff` × 100, 400
  bytes). `_read_back_problems` aceita os limites; as docstrings de `_stat_converter`,
  `_exact_statistic`, `file_from_return_stats`, `_action_stats` e `_read_back_problems`, que
  chamavam o texto de exato, dizem como o DuckDB o trunca desde o PR #127, com esta regra em
  `_stat_converter` e `file_from_return_stats`; o código não muda, porque os limites são
  seguros, e [`PLAN-STAGE-3.md`](PLAN-STAGE-3.md) registra a regra.
- **O texto sem mínimo e máximo no log e o filtro do dataset (issue #85).** O `export_partition`
  do motor DuckDB de uma partição com `texto` de `é` × 300 (600 bytes) registrou o arquivo com o
  `nullCount` de `texto` e sem o mínimo e o máximo, passou na releitura, e o `delta_scan` achou a
  linha pelo filtro `texto = ...`; `to_pyarrow_table`, o dataset e `to_pandas` com o mesmo filtro
  devolveram 0 de 1, também para o valor curto `a` × 10 do mesmo arquivo, porque a perda é por
  arquivo, e `IS NULL` deu 0, certo, pelo `nullCount`; com `é` × 10 o log tem o mínimo
  `aaaaaaaaaa` e o máximo `éééééééééé`, e o filtro acha a linha. É o gatilho da issue #85 numa
  coluna de texto gravada pelo DuckDB, fora da lista da issue (`Numeric`, `DateTime`, `Boolean` e
  o texto do `UNLOAD`); nenhuma leitura do pacote filtra o dataset por uma coluna de texto.
- **A posição da coluna de partição.** Um modelo com a coluna de partição na segunda posição
  passou em `check_models`, em `cast` com as colunas fora de ordem (a saída na ordem do modelo),
  em `create_table`, `append` e `export_partition` do motor DuckDB, com o arquivo sem a coluna de
  partição e a releitura, em `delta_scan` e `to_pyarrow_table` na ordem do modelo e em
  `publish_partition`; `partition_query` escreve a constante na posição da coluna,
  `_ordered_select` a deixa fora do arquivo, e `insert_from_staging` a preenche na posição dela.
  A posição não é regra do contrato, e nenhum caminho depende dela; "no fim da tabela", em
  `docs/index.md`, descrevia a base atual, e o PR #127 a trata assim.
- **As ações do log.** O `remove` do `publish_partition` que substitui uma partição e o do
  `restore` levam `partitionValues` e `dataChange` verdadeiro, e `version_diff` devolve a
  partição certa entre versões vizinhas, pela compactação sem efeito (`numFilesAdded` e
  `numFilesRemoved` zero, sem versão nova, nas partições de um arquivo) e por cima do `restore`.
- **Os literais do `render`.** Texto com aspas, contrabarra, `%`, `:` e Unicode, data, `DateTime`
  com microssegundos, `Double` nos extremos, `Numeric(18, 2)` e os limites do `BigInteger` saem
  transcritos nos dois dialetos (`'d''ag\ua'` no DuckDB e `'d''ag\\ua'` no Redshift,
  `'2026-08-31 23:59:59.999999'`, `1.7976931348623157e+308`, `-0.0`, `123456789012345.67`,
  `-9223372036854775808`), e o `bind` do DuckDB devolve cada valor igual. O `Double` não finito
  como constante sai `nan`, `inf` e `-inf`, que o DuckDB recusa com `BinderException`
  (`Referenced column "nan" was not found`); o Redshift não foi lido.
- **A ordem do `Execution.__exit__`.** `sandbox.cleanup()` roda antes de `_write_snapshot()`, e a
  falha do descarte deixa a execução sem a entrada do snapshot, com as partições já publicadas no
  Delta (lido no código, sem sonda).
- **As sessões.** Com a correção, as três sessões deram 256 aprovados e 437 pulados sem variável,
  574 e 119 com a raiz local e 691 e 2 com o substituto; o `ruff check` e o `ruff format --check`
  passaram.

**Consequências**: `ingest` e `pinned_delta` carregam cada partição numa transação, nas docstrings
do motor e em [`PLAN-STAGE-5.md`](PLAN-STAGE-5.md), e a lição está em `.claude/memory/lessons.md`;
o corte das estatísticas de texto está nas docstrings de `serialize_db.delta` (PR #127, com a
regra das duas sondas) e em [`PLAN-STAGE-3.md`](PLAN-STAGE-3.md), e o texto longo sem mínimo e
máximo entra no item da issue #85 em [`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md),
sem nada no `src/` (decisão do usuário de 2026-09-25); o `Double` não finito no `render` e a
ordem do `__exit__` esperam o usuário em [`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md).

## O que a medição das APIs com threads mostrou

Em 2026-10-04, neste contêiner (Linux x86_64, 4 vCPUs, 16.094 MB, Python 3.13.14, DuckDB 1.5.5,
PyArrow 25.0.1, deltalake 1.6.6, pandas 3.0.6), a pedido do usuário, um script no scratchpad mediu
pela API pública cada API com threads do pacote contra a forma em série que faz o mesmo. O motor
DuckDB abriu um banco em arquivo com uma tabela de 10.000.000 de linhas em seis colunas
(`id_lancamento`, `id_conta`, `data_base`, `valor`, `historico` e `data_base_str`), lida e gravada
em lotes de 100.000; os pools leram quatro tabelas Delta iguais de 5.000.000 de linhas numa pasta
local, publicadas pelo pacote. Cada medida rodou num processo novo, três vezes por variante, com o
pico de memória residente zerado por `/proc/self/clear_refs` depois do preparo; o tempo é o menor
dos três, e o pico, o maior acima da base. O trabalho do cliente em cada lote foi nenhum (a coluna
`valor_projetado` pelo `pyarrow.compute`), pandas (o lote no pandas com `ArrowDtype`,
`valor_projetado` e `historico` em maiúsculas calculados nele, e a volta ao Arrow), Python (um laço
de Python puro calibrado em 5 ms) ou espera (5 ms de `time.sleep`, como uma chamada de rede). O
`stream` rodou em duas rodadas, a segunda com o leitor da conexão crua ao lado dele.

- **O `stream` ganhou do `query` seguido do laço em todo trabalho, com um sexto do pico.** Nas duas
  rodadas, sem trabalho, 0,686 s e 0,697 s contra 0,771 s e 0,795 s (1,12 e 1,14 vez); com o
  pandas, 0,872 s e 0,835 s contra 1,365 s e 1,378 s (1,56 e 1,65 vez); com Python, 0,747 s e
  0,758 s contra 1,270 s e 1,312 s (1,70 e 1,73 vez); com a espera, 0,695 s e 0,693 s contra
  1,319 s e 1,321 s (1,90 e 1,91 vez). O pico ficou entre 118 MB e 134 MB, contra 686 MB a 700 MB
  do `query`, e o primeiro lote chegou em 0,022 s a 0,025 s, contra 0,757 s a 0,822 s.
- **O leitor da conexão crua dentro de `session()` não sobrepôs o trabalho à consulta.**
  `connection.execute("SELECT * FROM cad_lancamentos").to_arrow_reader(100_000)` deu o primeiro lote
  em 0,017 s a 0,019 s, com o pico de 111 MB a 126 MB, e levou 0,682 s sem trabalho, 1,327 s com o
  pandas, 1,162 s com Python e 1,304 s com a espera, de 1,01 a 1,17 vez a série. Com a espera, que
  solta o GIL, o tempo foi o da consulta mais o das esperas: o DuckDB produz o lote seguinte só
  quando o cliente o pede [inferido], e o ganho do `stream` vem da thread auxiliar.
- **O `stream` aberto dentro de `session()`, na mesma thread, foi o mais lento**: 1,400 s e 1,431 s
  sem trabalho, 2,024 s e 1,995 s com o pandas, 1,913 s e 1,903 s com Python e 1,920 s e 1,969 s
  com a espera, de 0,55 a 0,69 vez a série, com o primeiro lote em 1,185 s a 1,238 s e o pico de
  213 MB a 236 MB. A consulta roda inteira na thread do cliente e passa toda pelo arquivo
  intermediário antes do primeiro lote ([`PLAN.md`](PLAN.md)).
- **O `appender` ganhou do trabalho de todos os lotes seguido do `append`**: 2,843 s contra 3,089 s
  sem trabalho (1,09 vez), 3,109 s contra 4,733 s com o pandas (1,52 vez), 2,992 s contra 3,377 s
  com Python (1,13 vez) e 2,979 s contra 3,551 s com a espera (1,19 vez), com o pico de 826 MB a
  876 MB contra 878 MB a 1.173 MB. Com 5.000.000 de linhas, também em processo novo, 1,359 s contra
  1,686 s sem trabalho (1,24 vez) e 1,536 s contra 2,180 s com o pandas (1,42 vez). Num processo só,
  quatro rodadas alternadas com o pandas levaram 2,864 s, 1,792 s, 2,113 s e 2,283 s em série e de
  1,462 s a 1,621 s pelo `appender`: a primeira série do processo fez 88.810 faltas de página, e as
  seguintes de 20.888 a 52.306.
- **`stream` e `appender` no mesmo `with` ganharam do `query`, do trabalho e do `append` em
  série**: 3,471 s contra 3,730 s sem trabalho (1,07 vez), 3,509 s contra 5,162 s com o pandas
  (1,47 vez), 3,525 s contra 4,214 s com Python (1,20 vez) e 3,424 s contra 4,245 s com a espera
  (1,24 vez), com o pico de 943 MB a 1.020 MB contra 1.582 MB a 1.870 MB.
- **As consultas em threads só ganharam com uma sessão a mais por thread, e o statement Core
  limitou o ganho.** 200 consultas de 1.000 linhas por `BETWEEN` em `id_lancamento`, em texto SQL,
  levaram 0,375 s em série, 0,377 s em quatro threads na sessão principal e 0,170 s em quatro
  threads com uma `new_session()` cada (2,20 vezes); quatro agregações por `id_conta` sobre as
  10.000.000 de linhas, 0,270 s, 0,277 s e 0,242 s (1,12 vez). Num processo só, o menor de quatro,
  as 200 consultas levaram 0,288 s, 0,309 s e 0,126 s em texto (2,28 vezes), 0,760 s, 0,826 s e
  0,608 s pelo statement Core que devolve as linhas (1,25 vez) e 0,668 s, 0,717 s e 0,557 s pelo
  que as conta (1,20 vez): o caminho de compilação de cada chamada, `bound_statement` e o `compile`
  com `render_postcompile`, custou cerca de 2 ms em Python, sob o GIL.
- **Os pools ganharam na leitura das tabelas, e a publicação no Delta não.** `run.ingest` das quatro
  tabelas com `materialize=True` levou 4,859 s numa chamada por tabela e 3,735 s numa chamada só
  (1,30 vez), com o pico de 439 MB e 612 MB; o `materialize` das quatro no leitor Delta, 5,888 s e
  3,765 s (1,56 vez), com 435 MB e 635 MB; e `run.publish_delta` das quatro, 1,941 s com
  `max_workers=1` e 1,905 s com 4 (1,02 vez), com 106 MB e 127 MB: o `COPY` de cada partição corre
  em série na sessão principal ([`serialize-db.md`](serialize-db.md), seção "Paralelismo").

A sonda `probes/operacao/probe_parallel_gain.py` repete as medidas no próprio processo, sem o
trabalho em Python, a espera e as variantes dentro de `session()`, sobre quatro tabelas que ela
gera: no motor DuckDB, nos pools, no motor Redshift e na publicação no Redshift. Neste contêiner,
com o padrão de 5.000.000 de linhas e três medidas, as seções `duckdb` e `pools` passaram a checagem
duas vezes. Na primeira, o pico lido depois de outra medida no mesmo processo saiu abaixo do de um
processo novo, de +0 MB a +125 MB no `appender`: uma consulta da tabela inteira seguida do
`to_pandas` leu +418 MB na primeira medida do processo e +205 MB em cada repetição, porque o pool do
Arrow (mimalloc) e o `malloc` guardam a memória solta, e +365 MB com `gc.collect()`, o
`release_unused()` do pool e o `malloc_trim(0)` da glibc antes de zerar o pico, o que a sonda faz
desde então. Na segunda, o `stream` ganhou 1,23 vez sem trabalho e 1,66 com o pandas, com o pico de
+19 MB e +10 MB contra +313 MB e +324 MB; o `appender`, 1,03 e 1,07 vez; os dois juntos, 1,25 e 1,38
vez; as 200 consultas pelo statement Core, 0,90 vez nas threads da sessão principal e 1,22 nas
sessões a mais; `run.ingest`, 1,28 vez; `publish_delta`, 1,08 vez; e o `materialize`, 1,32 vez. As
medidas no mesmo processo variaram mais que em processo novo: a série do `appender` com o pandas
levou 2,573 s, 1,927 s e 1,655 s. No substituto, com 20.000 linhas e duas medidas, as quatro seções
passaram a checagem, a publicação no Redshift e a limpeza das tabelas `poc<id>_*` inclusive, com os
tempos sem valor de medida.

**Consequências**: a página principal do `pdoc` ganhou a seção "Multithreading" (`docs/index.md`),
com as APIs, o ganho medido e as regras de uso, entre elas juntar as consultas pequenas numa só;
[`PLAN.md`](PLAN.md), seção "A troca de dados com o código cliente", ganhou as medidas das
primitivas, e [`serialize-db.md`](serialize-db.md), seção "Paralelismo", aponta para esta seção. O
ganho no Redshift, com as tabelas no S3 e numa máquina com mais CPUs espera a sonda no alvo
([`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md)). A compilação de cada chamada pelo statement Core, cerca
de 2 ms em Python, não tem cache no pacote.
