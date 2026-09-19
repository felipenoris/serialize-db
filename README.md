# Inicialização

```
uv init --python 3.13
```

# Dependências

```
UV_PYTHON_DOWNLOADS=automatic uv sync --group dev
```

O `uv sync` instala em `.venv/` o Python 3.13 e as dependências do projeto. O grupo `dev` traz o
`pytest` e as bibliotecas dos testes (`deltalake`, `duckdb`, `pyarrow`, `boto3`), fixadas nas versões
usadas pelos documentos em `docs/`. `UV_PYTHON_DOWNLOADS=automatic` só é necessário onde o `uv`
está configurado para não baixar o Python, como no SageMaker Unified Studio.

# Testes

```
SERIALIZE_DB_TEST_S3_ROOT=s3://bucket/prefixo uv run pytest
```

A suíte é `tests/test_s3_proof_of_concept.py`: a prova de conceito da camada Delta no S3 (etapa 0
de `docs/estrategia.md`). Os testes levam o marcador `s3`, criam `serialize-db-poc/<id>/` sob a raiz
informada, apagam essa pasta no fim da sessão e imprimem um relatório com os fatos e as medições do
ambiente. Sem raiz configurada, são pulados. Eles cobrem a origem das credenciais, a escrita e a leitura pelo delta-rs, o put condicional,
o `delta_scan` do DuckDB com poda de partição, os tempos de consulta e o `vacuum`.

Variáveis de ambiente:

| Variável | Efeito |
| --- | --- |
| `SERIALIZE_DB_TEST_S3_ROOT` | Raiz `s3://bucket/prefixo` dos testes. Dentro de um espaço do SageMaker Unified Studio, o padrão é `sagemaker_studio.Project().s3.root`. |
| `SERIALIZE_DB_TEST_KEEP` | Qualquer valor mantém no S3 os objetos criados pela sessão. |
| `SERIALIZE_DB_TEST_REPORT` | Caminho de um JSON onde o relatório da sessão é gravado, além de impresso. |
| `SERIALIZE_DB_DUCKDB_EXTENSIONS` | Pasta de extensões do DuckDB. Sem ela, `.duckdb/` na raiz do repositório quando existir; senão a pasta padrão do DuckDB. |
| `AWS_REGION` | Região do bucket. Sem ela, os testes usam a região que o `boto3` resolve. |

O ambiente precisa de credenciais da AWS que o `boto3` encontre (papel do contêiner ou da instância,
variáveis `AWS_*` ou perfil), das permissões `s3:ListBucket`, `s3:GetObject`, `s3:PutObject` e
`s3:DeleteObject` sob o prefixo (e as de KMS quando o bucket usa SSE-KMS), e das extensões `httpfs`,
`delta` e `aws` do DuckDB, que o DuckDB baixa no primeiro uso quando há internet.

## Credenciais do delta-rs e proxy

O `deltalake` (delta-rs) tem cliente HTTP próprio, em Rust, e não usa o `boto3`: busca as
credenciais nas variáveis `AWS_*`, no endpoint de credenciais do contêiner
(`AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`), nos metadados da instância ou em `storage_options`, e
respeita `HTTP_PROXY`, `HTTPS_PROXY` e `NO_PROXY`. O espaço do SageMaker Unified Studio sai para a
rede por um proxy e define só `no_proxy` em minúsculas. Na primeira verificação, a chamada de
credenciais do delta-rs falhou com 403 e passou com `NO_PROXY` exportada; depois o ambiente como
encontrado passou a funcionar, e a causa não ficou isolada. Por isso `tests/conftest.py` exporta
`NO_PROXY` a partir de `no_proxy` quando só a minúscula existe, e `test_delta_rs_credential_chain`
registra no relatório o resultado de três variantes (ambiente como encontrado, `NO_PROXY` exportada,
proxies retirados). Se a cadeia padrão falhar em todas, `test_delta_rs_storage_options_fallback`
mostra que o caminho de reserva da biblioteca, as credenciais do `boto3` em `storage_options`,
funciona.

## Ambiente sem internet

O ambiente de destino não instala nada: a pasta do projeto é preparada num computador com internet,
da mesma plataforma (Linux x86_64), e copiada inteira.

No computador com internet, dentro da pasta do projeto:

```
tests/prepare_offline.sh
```

O script instala o Python 3.13 em `.python/`, as dependências em `.venv/` e as extensões do DuckDB
em `.duckdb/`, tudo dentro da pasta do projeto, e troca o link `.venv/bin/python` por um link
relativo, para a pasta funcionar em qualquer caminho. As três pastas estão no `.gitignore`.

Empacotar preservando os links simbólicos (copiar arquivo a arquivo por uma pasta montada do S3 os
perde):

```
tar czf serialize-db.tgz --exclude=serialize-db/.git -C .. serialize-db
```

No ambiente de destino, depois de extrair o pacote em qualquer caminho:

```
cd serialize-db
SERIALIZE_DB_TEST_S3_ROOT=s3://bucket/prefixo .venv/bin/python -m pytest
```

Não é preciso `uv` nem rede além do S3: o Python, as bibliotecas e as extensões vêm da pasta. Os
comandos de `.venv/bin/` (como `pytest`) guardam o caminho original no cabeçalho, por isso a chamada
é `python -m pytest`. A receita foi verificada extraindo o pacote em outro caminho e rodando a suíte
com os proxies apontados para uma porta fechada.
