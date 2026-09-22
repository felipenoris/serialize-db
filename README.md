[![Testes do pacote](https://github.com/felipenoris/serialize-db/actions/workflows/tests.yml/badge.svg?branch=main)](https://github.com/felipenoris/serialize-db/actions/workflows/tests.yml)
[![Documentação do pacote](https://github.com/felipenoris/serialize-db/actions/workflows/docs.yml/badge.svg?branch=main)](https://github.com/felipenoris/serialize-db/actions/workflows/docs.yml)

# Inicialização

```
uv init --python 3.13
```

# Ambiente de Desenvolvimento

## Instruções para VS Code windows

- Instalar extensões Python e Jupyter.

- Encontrar caminho do interpretador Python do env:

```
uv sync --group interactive
uv python find
```

- Adicionar interpretador python no VS Code com:

```
Ctrl+Shift+P → Python: Select Interpreter → Enter interpreter path...
```

- Abrir notebook existente (ou criar novo com `Ctrl+Shift+P → Create: New Jupyter Notebook`).

- Selecionar Kernel -> selecionar caminho para interpretador criado no passo anterior.

Obs.: project.toml foi inicializado com `uv add ipykernel --group interactive`

# Dependências

```
UV_PYTHON_DOWNLOADS=automatic uv sync --group dev
```

O `uv sync` instala em `.venv/` o Python 3.13, o pacote com as dependências de execução fixadas em
`pyproject.toml` (`sqlalchemy`, `pyarrow`, `deltalake`, `duckdb` e os dialetos `duckdb-engine` e
`sqlalchemy-redshift`, que compilam o texto SQL de cada motor) e o grupo `dev`: o `pytest` e as
bibliotecas dos testes (`boto3`, `pandas`, `redshift-connector`, `sqlglot`), fixadas nas versões
usadas pelos documentos em `plan/`. O grupo `docs` traz o
`pdoc`. `UV_PYTHON_DOWNLOADS=automatic` só é necessário onde o `uv` está configurado para não baixar
o Python, como no SageMaker Unified Studio.

# Documentação

A documentação do pacote é gerada pelo `pdoc` a partir das docstrings e de [`docs/index.md`](docs/index.md),
a página principal, com o funcionamento geral, o tutorial e a tabela de mapeamento de tipos. Para
gerar o HTML estático, informe a pasta alvo em `-o`:

```
uv sync --group docs
uv run pdoc serialize_db --docformat restructuredtext -o /pasta/da/documentacao
```

A pasta é criada se não existir e recebe `index.html`, a página principal, `serialize_db.html`,
`serialize_db/` com uma página por módulo e `search.js`; abra `index.html` no navegador. `site/`,
a pasta que a esteira usa, fica fora do git. Sem `-o`, o `pdoc` serve a documentação em
`http://localhost:8080` e a regenera a cada mudança. A esteira [`docs.yml`](.github/workflows/docs.yml) publica o mesmo
resultado no GitHub Pages a cada push na `main`, em <https://felipenoris.github.io/serialize-db/>;
a publicação exige o Pages do repositório com a origem "GitHub Actions".

# Testes

O que cada arquivo de teste exercita está no cabeçalho do próprio arquivo; o que cada suíte grava,
as variáveis que a autorizam e o relatório da sessão estão no cabeçalho de
[`tests/conftest.py`](tests/conftest.py). Uma suíte sem a sua variável é pulada, e o fim da sessão
imprime o comando que a autoriza.

## Testes do pacote

Os testes de `tests/`, sem `tests/proof_of_concept/` nem `tests/test_probes.py` (as funções puras
dos probes), sobre o DuckDB em memória e arquivos locais, sem AWS. É o que a esteira
[`tests.yml`](.github/workflows/tests.yml) roda a cada push e pull request:

```
SERIALIZE_DB_TEST_LOCAL_ROOT=/pasta/existente uv run pytest tests --ignore=tests/proof_of_concept --ignore=tests/test_probes.py
```

Os testes da migração leem o Delta pelo `delta_scan` e precisam da extensão `delta` do DuckDB em
`.duckdb/` na raiz do repositório, ou na pasta de `SERIALIZE_DB_DUCKDB_EXTENSIONS`;
`prepare_offline.sh` a instala lá, e com internet basta:

```
uv run python -c "import duckdb; duckdb.connect(config={'extension_directory': '.duckdb'}).execute('INSTALL delta')"
```

## Testes no ambiente AWS

As provas de conceito e as suítes de estudo de `tests/proof_of_concept/` junto com os testes do
pacote. Sem o Redshift, com a pasta local e a raiz S3:

```
export SERIALIZE_DB_TEST_LOCAL_ROOT=/pasta/existente
export SERIALIZE_DB_TEST_S3_ROOT=s3://bucket/prefixo
uv run pytest
```

Com o Redshift, a suíte é ligada por `SERIALIZE_DB_TEST_REDSHIFT_SCHEMA`, o esquema onde ela pode
criar tabelas, mais a conexão:

```
export SERIALIZE_DB_TEST_LOCAL_ROOT=/pasta/existente
export SERIALIZE_DB_TEST_S3_ROOT=s3://bucket/prefixo
export SERIALIZE_DB_TEST_REDSHIFT_SCHEMA=esquema
export SERIALIZE_DB_REDSHIFT_WORKGROUP=workgroup
export SERIALIZE_DB_REDSHIFT_DATABASE=banco
export SERIALIZE_DB_REDSHIFT_SHARE_DATABASE=banco_do_datashare
export AWS_DEFAULT_REGION=regiao
uv run pytest
```

Para desligar o Redshift sem retirar as variáveis, `uv run pytest -m "not redshift"`; `-m local`,
`-m s3` e `-m redshift` rodam uma suíte só. No ambiente alvo, na pasta preparada sem internet
(seção "Ambiente sem internet"), `.venv/bin/python -m pytest` substitui `uv run pytest`, e
`SERIALIZE_DB_TEST_REPORT` grava o relatório da sessão para acompanhar os relatórios dos probes:

```
export AWS_DEFAULT_REGION=sa-east-1
export SERIALIZE_DB_REDSHIFT_WORKGROUP=controladoria-wg
export SERIALIZE_DB_REDSHIFT_DATABASE=dev
export SERIALIZE_DB_REDSHIFT_SHARE_DATABASE=datalake_rw_shared
export SERIALIZE_DB_TEST_REDSHIFT_SCHEMA=sbx_aco_decon
export SERIALIZE_DB_TEST_S3_ROOT=s3://bucket/prefixo
SERIALIZE_DB_TEST_REPORT=probes/output/redshift_suite_1.json .venv/bin/python -m pytest -m redshift
SERIALIZE_DB_TEST_REPORT=probes/output/redshift_suite_2.json .venv/bin/python -m pytest -m redshift
```

São duas execuções: a [etapa 0](plan/PLAN-STAGE-0.md) só escreve a consequência de uma leitura num
arquivo de etapa depois que a segunda a repete.

## Variáveis de ambiente

| Variável | Efeito |
| --- | --- |
| `SERIALIZE_DB_TEST_LOCAL_ROOT` | Pasta existente sob a qual a suíte local cria `serialize-db-poc/<id>/`. Sem ela, os testes `local` são pulados. |
| `SERIALIZE_DB_TEST_S3_ROOT` | Raiz `s3://bucket/prefixo` sob a qual a suíte S3 cria `serialize-db-poc/<id>/`. Sem ela, os testes `s3` são pulados. |
| `SERIALIZE_DB_TEST_REDSHIFT_SCHEMA` | Esquema do Redshift onde a suíte cria as tabelas `serialize_db_poc_<id>_*`. Sem ela, os testes `redshift` são pulados. A conexão vem de `SERIALIZE_DB_REDSHIFT_*` (a tabela está em [`probes/README.md`](probes/README.md)), e `SERIALIZE_DB_REDSHIFT_SHARE_DATABASE` é o banco do datashare em que cada conexão roda `USE`. `SERIALIZE_DB_REDSHIFT_IAM_ROLE` nomeia o papel do `COPY` e do `UNLOAD`, ou a palavra `default`; sem ela, os dois levam as credenciais de quem chama. |
| `SERIALIZE_DB_TEST_KEEP` | Qualquer valor mantém a pasta, os objetos e as tabelas criados pela sessão. |
| `SERIALIZE_DB_TEST_REPORT` | Caminho de um JSON onde o relatório da sessão é gravado, além de impresso. |
| `SERIALIZE_DB_DUCKDB_EXTENSIONS` | Pasta de extensões do DuckDB, a única onde a suíte instala as que faltam. Sem ela, `.duckdb/` na raiz do repositório quando existir, senão a pasta padrão do DuckDB, e nada é instalado. |
| `AWS_REGION`, `AWS_DEFAULT_REGION` | Região do bucket e do workgroup. O botocore lê `AWS_DEFAULT_REGION` ou o perfil, e o delta-rs lê as duas; sem uma delas, a suíte Redshift procura o workgroup na região errada. |

# Probes

Scripts só de leitura, em [`probes/`](probes/README.md), que fotografam o que o ambiente oferece à
biblioteca; o que cada um lê está no cabeçalho do próprio script e em `probes/README.md`. Cada um
imprime o relatório e o grava em `probes/output/`, pasta fora do git, para ser colado na conversa
com o assistente.

```
.venv/bin/python probes/space.py
.venv/bin/python probes/bucket.py s3://bucket/prefixo
.venv/bin/python probes/diagnose_aws.py s3://bucket/prefixo
.venv/bin/python probes/redshift.py s3://bucket/prefixo
.venv/bin/python probes/catalog.py
.venv/bin/python probes/parquet_source.py /caminho/da/base
.venv/bin/python probes/parquet_source.py /caminho/da/base --text-bytes
```

O argumento `s3://bucket/prefixo` é a raiz que o probe fotografa; sem ele valem `SERIALIZE_DB_ROOT`
e `SERIALIZE_DB_TEST_S3_ROOT`, nessa ordem. `redshift.py` conecta pelas variáveis
`SERIALIZE_DB_REDSHIFT_*` ou pela conexão Redshift do projeto. No ambiente alvo, com a raiz S3 do
projeto no lugar do exemplo:

```
export AWS_DEFAULT_REGION=sa-east-1
export SERIALIZE_DB_REDSHIFT_WORKGROUP=controladoria-wg
export SERIALIZE_DB_REDSHIFT_DATABASE=dev
export SERIALIZE_DB_REDSHIFT_SHARE_DATABASE=datalake_rw_shared
export SERIALIZE_DB_REDSHIFT_SCHEMA=sbx_aco_decon
.venv/bin/python probes/redshift.py s3://bucket/prefixo
```

`SERIALIZE_DB_REDSHIFT_SCHEMA` é o esquema do projeto, que o probe lê, e
`SERIALIZE_DB_TEST_REDSHIFT_SCHEMA` é a autorização da suíte, que diz onde ela pode criar tabelas:
apontam para o mesmo esquema, com significados diferentes.

# Migração da base Parquet para o Delta

[`scripts/migrate_parquet_to_delta.py`](scripts/migrate_parquet_to_delta.py) é a migração
adiantada da etapa 7: cada partição da base de origem vira um commit numa tabela Delta, e o
relatório confere contagem e somas por partição. O que ele faz, o que recusa e como se mede a
partição de `cad_lancamentos` estão no cabeçalho do script. Sobre a base fictícia, gravada em
pasta local:

```
PYTHONPATH=tests uv run python -c "from pathlib import Path; import source_db_projetado; source_db_projetado.write_source(Path('/pasta/db_projetado'))"
PYTHONPATH=tests uv run python scripts/migrate_parquet_to_delta.py --metadata client_model:Base.metadata --source /pasta/db_projetado --root /pasta/delta
```

No ambiente alvo, com a pasta preparada, sobre a cópia da base de produção, uma tabela por vez e
o relatório em JSON:

```
export AWS_DEFAULT_REGION=sa-east-1
PYTHONPATH=tests .venv/bin/python scripts/migrate_parquet_to_delta.py --metadata client_model:Base.metadata --source s3://bucket/prefixo/db_projetado --root s3://bucket/prefixo/delta --tables cad_contratos --report relatorio.json
```

`--partitions AAAA-MM-DD` carrega só as partições listadas, `--mode rewrite` grava pelo
`write_deltalake` em vez do `COPY` do DuckDB registrado no log, e `--no-sort` grava na ordem da
origem. A segunda execução não grava nada: a carga recomeça das partições fora do log.

# Exemplos: conectividade com o Redshift

[`examples/`](examples/) guarda os scripts que rodaram no ambiente alvo, como foram executados:
[`redshift_native.py`](examples/redshift_native.py), o protocolo nativo com credencial temporária do
workgroup serverless, que é o caminho da biblioteca; [`redshift_data_api.py`](examples/redshift_data_api.py),
a Data API por HTTPS, assíncrona; e [`redshift_copy_unload.py`](examples/redshift_copy_unload.py), o
`USE` no banco do datashare com `CREATE TABLE`, `COPY` e `UNLOAD` pelas credenciais de quem chama. O
probe, a suíte e a etapa 5 repetem as chamadas que estão lá, e
[`examples/README.md`](examples/README.md) diz o que cada um fixa.

[`redshift_manifest.py`](examples/redshift_manifest.py) rodou em 2026-09-21: ele converte uma
partição de `cad_contratos` de Parquet para Delta e roda os dois comandos com manifesto, o
`COPY ... MANIFEST` e o `UNLOAD ... PARTITION BY ... MANIFEST VERBOSE` numa tabela do datashare, que
são os pré-requisitos do `export_partition`. Os dois são aceitos, e o que o rodapé do `UNLOAD`
respondeu está em [`plan/POC.md`](plan/POC.md).

# Credenciais do delta-rs e proxy

O `deltalake` (delta-rs) tem cliente HTTP próprio, em Rust, e não usa o `boto3`: busca as
credenciais nas variáveis `AWS_*`, no endpoint de credenciais do contêiner
(`AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`), nos metadados da instância ou em `storage_options`, e
respeita `HTTP_PROXY` e `HTTPS_PROXY` nas duas grafias, mas lê `NO_PROXY` e, só quando ela está
ausente, `no_proxy`: uma `NO_PROXY` vazia anula todas as exceções. O espaço do SageMaker Unified
Studio sai para a rede por um proxy e lista o endpoint de credenciais em `no_proxy`; num shell aberto
pela extensão do Claude Code no Code Editor, `NO_PROXY` existe vazia, a chamada de credenciais do
delta-rs vai pelo proxy e falha com `StatusCode(403)` (medido em 2026-09-20: com `NO_PROXY` ausente,
exportada de `no_proxy` ou reduzida a `169.254.170.2` a chamada passa; vazia, falha). Por isso
`tests/conftest.py` exporta `NO_PROXY` a partir de `no_proxy` quando a maiúscula está ausente ou
vazia, e `test_delta_rs_credential_chain` registra no relatório o resultado de cinco variantes
(ambiente como encontrado, `NO_PROXY` exportada, ausente, vazia, proxies retirados). Se a cadeia
padrão falhar em todas, `test_delta_rs_storage_options_fallback` mostra que credenciais congeladas
do `boto3` em `storage_options` funcionam. A biblioteca não as usa: o `storage_options` dela leva
região, endpoint, retry e as chaves de SSE, e credencial alguma (decisão do usuário de 2026-09-22),
porque a cadeia padrão renova as credenciais no `DeltaTable` que a execução segura, enquanto um trio
congelado expiraria em cerca de uma hora e circularia num dicionário que um log ou uma exceção
imprime.

# Ambiente sem internet

O ambiente de destino não instala nada: a pasta do projeto é preparada num computador com internet,
da mesma plataforma (Linux x86_64), por `./prepare_offline.sh`, e copiada inteira. Em outra
plataforma o script roda do mesmo jeito e deixa a pasta pronta para uso local, mas ela não serve ao
destino. O cabeçalho de `prepare_offline.sh` traz o procedimento: como rodá-lo, as variáveis de
ambiente do proxy e os comandos de empacotar com `tar` e extrair no destino.

O script cria na raiz do projeto tudo o que o pacote e os testes precisam em execução: o Python
3.13 em `.python/`, o pacote com as dependências de execução e as de todos os grupos do
`pyproject.toml` em `.venv/`, e as extensões do DuckDB (`httpfs`, `delta`, `aws`) em `.duckdb/`. Ele
recria a `.venv/` a cada execução, resolve as dependências na hora (o `uv.lock` não é versionado) e
troca os links absolutos que o `uv` cria por links relativos, para a pasta funcionar em qualquer
caminho. As três pastas estão no `.gitignore`. Rode o script de novo sempre que uma dependência
mudar; se a dependência nova estiver fora do `pyproject.toml` (extensão do DuckDB, versão do Python),
acrescente-a ao script antes.

Atrás de um proxy com autenticação, o `uv` lê `HTTP_PROXY` com as credenciais embutidas, mas o
DuckDB recusa esse endereço (`Failed to parse http_proxy ... into a host and port`) e precisa do
usuário e da senha à parte: o script os lê das variáveis `username` e `password` e os passa em
`http_proxy_username` e `http_proxy_password`.

No destino, a suíte local roda sem S3 e valida a pasta preparada; com
`SERIALIZE_DB_TEST_S3_ROOT=s3://bucket/prefixo` a suíte S3 roda também, e a do Redshift com as
variáveis da seção Testes. Não é preciso `uv` nem rede além do S3 e do Redshift: o Python, as
bibliotecas e as extensões vêm da pasta. A receita foi verificada extraindo o pacote em outro
caminho e rodando a suíte com os proxies apontados para uma porta fechada.
