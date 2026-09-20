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
[etapa 5](PLAN-STAGE-5.md) conecta por senha por padrão e trata a autenticação por IAM e a Data
API como opcionais, porque dependem das APIs do Redshift, sem endpoint VPC no laboratório
(`RS-14`); e o `vacuum` num bucket versionado só libera espaço com a regra
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
região nos dois sentidos, não chama o STS e carrega as extensões só da pasta configurada. Se as APIs
do Redshift também não tiverem endpoint lá, resta a conexão por senha na porta 5439, que não passa
por elas (`RS-14`).

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
