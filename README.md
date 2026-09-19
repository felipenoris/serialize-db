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
SERIALIZE_DB_TEST_LOCAL_ROOT=/pasta/existente uv run pytest
```

Duas suítes provam a camada Delta (etapa 0 de `docs/estrategia.md`), uma por tipo de armazenamento
que a biblioteca suporta. Os testes comuns às duas, em `tests/delta_proof_of_concept.py`, cobrem a
escrita e a leitura pelo delta-rs, o `delta_scan` do DuckDB com os tipos do contrato e a poda de
partição, os tempos de consulta e o `vacuum`; cada suíte acrescenta os testes próprios do seu
armazenamento. As duas criam `serialize-db-poc/<id>/` sob a raiz, apagam essa pasta no fim da sessão
e imprimem um relatório com os fatos e as medições, com as chaves prefixadas por `local.` ou `s3.`.

Cada suíte escreve só sob a raiz que a sua variável de ambiente informa, e a variável é a
autorização: sem ela a suíte é pulada, com o motivo no relatório e em `pytest -rs`, e `uv run pytest`
sem variável alguma não executa nenhum teste que grave arquivos. Com a raiz informada, o que impede
a escrita é falha: pasta local inexistente, ou raiz S3 sem credencial ou sem acesso, reprovada por
uma sondagem com tempos curtos (cerca de 11 s com um proxy que não responde) antes de o delta-rs
tentar. `-m local` e `-m s3` selecionam uma suíte.

| Suíte | Marcador | Raiz | Testes próprios |
| --- | --- | --- | --- |
| `tests/test_local_proof_of_concept.py` | `local` | `SERIALIZE_DB_TEST_LOCAL_ROOT`, uma pasta existente. Roda em qualquer ambiente, sem AWS. | Commit atômico em disco e conflito entre escritores na mesma versão, caminhos relativos do log e realocação da pasta, abertura sem variáveis `AWS_*`. |
| `tests/test_s3_proof_of_concept.py` | `s3` | `SERIALIZE_DB_TEST_S3_ROOT`, `s3://bucket/prefixo`. | Origem das credenciais, cadeia de credenciais do delta-rs e sua reserva, put condicional, criptografia dos arquivos. |

Variáveis de ambiente:

| Variável | Efeito |
| --- | --- |
| `SERIALIZE_DB_TEST_LOCAL_ROOT` | Pasta existente sob a qual a suíte local cria `serialize-db-poc/<id>/`. Sem ela, a suíte local é pulada. |
| `SERIALIZE_DB_TEST_S3_ROOT` | Raiz `s3://bucket/prefixo` sob a qual a suíte S3 cria `serialize-db-poc/<id>/`. Sem ela, a suíte S3 é pulada. |
| `SERIALIZE_DB_TEST_KEEP` | Qualquer valor mantém a pasta e os objetos criados pela sessão. |
| `SERIALIZE_DB_TEST_REPORT` | Caminho de um JSON onde o relatório da sessão é gravado, além de impresso. |
| `SERIALIZE_DB_DUCKDB_EXTENSIONS` | Pasta de extensões do DuckDB, a única onde a suíte instala as que faltam. Sem ela, `.duckdb/` na raiz do repositório quando existir, senão a pasta padrão do DuckDB, e nada é instalado: o teste cuja extensão falta é pulado. A instalação automática do DuckDB, que no `LOAD` baixaria a extensão para `~/.duckdb` sem aviso, fica desligada. |
| `AWS_REGION` | Região do bucket. Sem ela, a suíte S3 usa a região que o `boto3` resolve. |

A suíte S3 precisa de credenciais da AWS que o `boto3` encontre (papel do contêiner ou da instância,
variáveis `AWS_*` ou perfil), das permissões `s3:ListBucket`, `s3:GetObject`, `s3:PutObject` e
`s3:DeleteObject` sob o prefixo (e as de KMS quando o bucket usa SSE-KMS), e das extensões `httpfs`,
`delta` e `aws` do DuckDB. A suíte local precisa só da extensão `delta`. Fora das raízes informadas,
o que uma sessão grava é `.pytest_cache/` na raiz do repositório, do próprio pytest.

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
da mesma plataforma (Linux x86_64), e copiada inteira. Em outra plataforma o script roda do mesmo
jeito e deixa a pasta pronta para uso local, mas ela não serve ao destino.

No computador com internet, dentro da pasta do projeto:

```
./prepare_offline.sh
```

O script cria na raiz do projeto tudo o que o pacote e os testes precisam em execução: o Python
3.13 em `.python/`, o pacote com as dependências de execução e as de todos os grupos do
`pyproject.toml` em `.venv/`, e as extensões do DuckDB (`httpfs`, `delta`, `aws`) em `.duckdb/`. Ele
recria a `.venv/` a cada execução, resolve as dependências na hora (o `uv.lock` não é versionado) e
troca os links absolutos que o `uv` cria por links relativos, para a pasta funcionar em qualquer
caminho. As três pastas estão no `.gitignore`. Rode o script de novo sempre que uma dependência
mudar; se a dependência nova estiver fora do `pyproject.toml` (extensão do DuckDB, versão do Python),
acrescente-a ao script antes.

Empacotar com `tar`, que preserva os links simbólicos, as permissões e os demais metadados do
sistema de arquivos. Copiar arquivo a arquivo por uma pasta montada do S3 perde os links, e sem eles
`.venv/bin/python` só funciona no caminho de origem.

```
tar czf serialize-db.tar.gz --exclude=serialize-db/.git -C .. serialize-db
```

No ambiente de destino, depois de extrair o pacote em qualquer caminho:

```
tar xzf serialize-db.tar.gz
cd serialize-db
SERIALIZE_DB_TEST_LOCAL_ROOT=/pasta/existente .venv/bin/python -m pytest
```

A suíte local roda sem S3 e valida a pasta preparada; com `SERIALIZE_DB_TEST_S3_ROOT=s3://bucket/prefixo`
a suíte S3 roda também. Não é preciso `uv` nem rede além do S3: o Python, as bibliotecas e as extensões vêm da pasta. Os
comandos de `.venv/bin/` (como `pytest`) guardam o caminho original no cabeçalho, por isso a chamada
é `python -m pytest`; o pacote roda do mesmo jeito, com `.venv/bin/python -m serialize_db` ou
`.venv/bin/python -c "import serialize_db"`. A receita foi verificada extraindo o pacote em outro
caminho e rodando a suíte com os proxies apontados para uma porta fechada.
