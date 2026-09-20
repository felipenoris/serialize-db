# Inicialização

```
uv init --python 3.13
```

# Dependências

```
UV_PYTHON_DOWNLOADS=automatic uv sync --group dev
```

O `uv sync` instala em `.venv/` o Python 3.13 e as dependências do projeto. O grupo `dev` traz o
`pytest` e as bibliotecas dos testes (`deltalake`, `duckdb`, `pyarrow`, `boto3`, `sqlalchemy` com os
dialetos `duckdb-engine` e `sqlalchemy-redshift`, `pandas`, `redshift-connector`), fixadas nas
versões usadas pelos documentos em `docs/`. `UV_PYTHON_DOWNLOADS=automatic` só é necessário onde o `uv`
está configurado para não baixar o Python, como no SageMaker Unified Studio.

# Testes

Os testes sem marcador e a suíte `local` rodam sem a AWS:

```
SERIALIZE_DB_TEST_LOCAL_ROOT=/pasta/existente uv run pytest
```

A suíte `s3` precisa de credenciais que o `boto3` encontre:

```
SERIALIZE_DB_TEST_S3_ROOT=s3://bucket/prefixo uv run pytest -m s3
```

A suíte `redshift` também grava no S3 e por isso recebe as duas raízes;
`SERIALIZE_DB_REDSHIFT_WORKGROUP` ou `SERIALIZE_DB_REDSHIFT_CLUSTER` no lugar de host, usuário e
senha autentica por IAM:

```
SERIALIZE_DB_TEST_REDSHIFT_SCHEMA=esquema SERIALIZE_DB_TEST_S3_ROOT=s3://bucket/prefixo \
SERIALIZE_DB_REDSHIFT_DATABASE=banco SERIALIZE_DB_REDSHIFT_HOST=host \
SERIALIZE_DB_REDSHIFT_USER=usuario SERIALIZE_DB_REDSHIFT_PASSWORD=senha \
uv run pytest -m redshift
```

As três variáveis de autorização se somam: informadas juntas, `uv run pytest` sem `-m` roda tudo.

`tests/` na raiz recebe os testes do pacote `serialize_db`, um módulo por módulo do pacote (as etapas
de `docs/PLAN.md`). `tests/proof_of_concept/` recebe as provas de conceito e os testes das bibliotecas
externas: cada módulo exercita a parte da API que a biblioteca usa, com comentários passo a passo, e
é o material de estudo de quem dá manutenção na biblioteca. Os testes que não gravam nada
(SQLAlchemy, DuckDB em memória, PyArrow em memória, a stdlib) rodam sempre; os demais só onde o
usuário autoriza, pelas variáveis abaixo. Uma suíte pulada não é erro: o fim da sessão imprime o
comando que a autoriza e o que ela grava.

| Módulo | O que exercita | Marcador |
| --- | --- | --- |
| `test_sqlalchemy.py` | O modelo declarativo como `Table`, o DDL por dialeto com as opções físicas de `Table.info`, `create_all` no DuckDB em memória, `insert` e `select` do Core, o caminho Arrow na conexão bruta, a reflexão, a precisão do `Numeric`, `pandas.read_sql`, o texto SQL gerado por dialeto com parâmetro e prefixo, o DML compilado para o Redshift. | Nenhum. |
| `test_duckdb.py` | A configuração da conexão, Arrow na entrada e na saída, o leitor esvaziado pelo comando seguinte, o `DECIMAL` inferido do pandas contra o esquema Arrow, JSON, `executemany` contra Arrow, as consultas da auditoria; `COPY ... TO` com `RETURN_STATS`, o `COPY` particionado por mês, o banco em arquivo. | Nenhum; os três últimos, `local`. |
| `test_pyarrow.py` | O esquema com metadados e `field_id`, `from_pylist`, o cast seguro e as perdas que ele não acusa, o ciclo com o pandas por `ArrowDtype` sem cópia, o `RecordBatchReader`; o `ParquetWriter` por lote e o rodapé, o mesmo conteúdo gravado pelo DuckDB, o dataset Hive, a leitura por lotes com `filters` e o pandas com tipos Arrow. | Nenhum; os três últimos, `local`. |
| `test_deltalake.py` | `DeltaTable.create` idempotente, os modos de escrita e o predicado, a evolução de esquema e o `update`, a viagem no tempo e o `restore`, as ações `add` e a `AddAction`, o `vacuum` com `keep_versions`, o dataset Arrow e a cópia profunda, o log; `is_deltatable` e `drop_column_not_null`, a view do DuckDB presa a uma versão e o leitor Arrow no `write_deltalake`, a reescrita pelo `COPY` particionado registrada com estatísticas, a diferença de versões, a compactação e o checkpoint, a exportação por cópia dos arquivos, a carga inicial de pastas Parquet. | `local`. |
| `test_stdlib.py` | A biblioteca padrão nos papéis do plano: aritmética de meses, identificadores de execução e de sandbox, `Protocol` e `dataclass`, o gerenciador de contexto que descarta o sandbox na falha, `importlib` para `modulo:funcao`, `argparse`, `logging`, `os.environ` com `monkeypatch`, `json`, `urllib.parse` e `pathlib`, `itertools` e `collections`, `decimal`, `difflib`; a criação exclusiva, a substituição atômica e a impressão digital de um arquivo. | Nenhum; o último, `local`. |
| `poc_delta.py`, `test_local.py`, `test_s3.py` | A prova de conceito da camada Delta nos dois armazenamentos: a escrita e a leitura pelo delta-rs, o `delta_scan` com os tipos do contrato e a poda de partição, os tempos de consulta, o `vacuum`; em disco, o commit atômico e o conflito entre escritores, a realocação da pasta, a abertura sem variáveis `AWS_*`; no bucket, a origem das credenciais, a cadeia do delta-rs e sua reserva, o put condicional, a criptografia, listar, copiar e apagar pelo `boto3`. | `local` e `s3`. |
| `test_redshift.py` | Os itens da etapa 0 que esperam uma conexão: a sessão e o `paramstyle` nomeado, o DDL do SQLAlchemy, o `COPY ... MANIFEST` de arquivos do delta-rs (`DECIMAL` em `INT64`, `timestamp_ntz`, lista de colunas, `FILLRECORD`), o `VARCHAR` excedido, o `SUPER`, o `UNLOAD ... PARTITION BY` registrado no Delta e lido pelo DuckDB. Escrito antes de haver conexão; ainda não rodou. | `redshift` e `s3`. |
| `test_probes.py` (em `tests/`) | As funções puras dos probes, sem rede: a classificação dos erros do `boto3`, os rótulos de DNS, as tabelas e os segredos mascarados, o código de saída do relatório, o inventário do bucket (tabelas Delta, sessões da suíte, versões não correntes), o versionamento pela amostra, o Object Lock, o ciclo de vida, a montagem de `~/shared`, o formato das tabelas do Glue e os parâmetros da conexão Redshift. | Nenhum. |

Cada suíte escreve só onde a sua variável autoriza, e a variável é a autorização: sem ela a suíte é
pulada, com o motivo no relatório e em `pytest -rs`, e `uv run pytest` sem variável alguma não
executa nenhum teste que grave arquivos ou crie tabelas. Com a autorização dada, o que impede a
escrita é falha: pasta local inexistente, raiz S3 sem credencial ou sem acesso (reprovada por uma
sondagem com tempos curtos, cerca de 11 s com um proxy que não responde, antes de o delta-rs
tentar), Redshift sem conexão. `-m local`, `-m s3` e `-m redshift` selecionam uma suíte. As suítes
criam `serialize-db-poc/<id>/` sob a raiz ou tabelas `serialize_db_poc_<id>_*` no esquema, apagam
tudo no fim da sessão e imprimem um relatório com os fatos e as medições, com as chaves prefixadas
pelo alvo (`local.`, `s3.`, `redshift.`) ou pela biblioteca (`duckdb.`, `sqlalchemy.`, `pyarrow.`).
O relatório abre com a sessão (`session.`: início, plataforma, Python, versões, marcadores e, no
fim, a contagem de testes por resultado e a duração) e registra a limpeza de cada raiz
(`local.cleanup`, `s3.cleanup`), para dizer sozinho se a suíte passou e o que ficou. Num bucket
versionado, cada objeto que a limpeza apaga vira versão não corrente, invisível à listagem e cobrada
até uma regra `NoncurrentVersionExpiration`; `probes/bucket.py` (`BK-14`) conta o acumulado.

Variáveis de ambiente:

| Variável | Efeito |
| --- | --- |
| `SERIALIZE_DB_TEST_LOCAL_ROOT` | Pasta existente sob a qual a suíte local cria `serialize-db-poc/<id>/`. Sem ela, os testes `local` são pulados. |
| `SERIALIZE_DB_TEST_S3_ROOT` | Raiz `s3://bucket/prefixo` sob a qual a suíte S3 cria `serialize-db-poc/<id>/`. Sem ela, os testes `s3` são pulados. |
| `SERIALIZE_DB_TEST_REDSHIFT_SCHEMA` | Esquema do Redshift onde a suíte cria as tabelas `serialize_db_poc_<id>_*`. Sem ela, os testes `redshift` são pulados. A conexão vem de `SERIALIZE_DB_REDSHIFT_*` (as variáveis de `probes/redshift.py`), e o papel do `COPY` e do `UNLOAD` de `SERIALIZE_DB_REDSHIFT_IAM_ROLE`; sem ela, `IAM_ROLE default`. |
| `SERIALIZE_DB_TEST_KEEP` | Qualquer valor mantém a pasta, os objetos e as tabelas criados pela sessão. |
| `SERIALIZE_DB_TEST_REPORT` | Caminho de um JSON onde o relatório da sessão é gravado, além de impresso. |
| `SERIALIZE_DB_DUCKDB_EXTENSIONS` | Pasta de extensões do DuckDB, a única onde a suíte instala as que faltam. Sem ela, `.duckdb/` na raiz do repositório quando existir, senão a pasta padrão do DuckDB, e nada é instalado: o teste cuja extensão falta é pulado. A instalação automática do DuckDB, que no `LOAD` baixaria a extensão para `~/.duckdb` sem aviso, fica desligada. |
| `AWS_REGION` | Região do bucket. Sem ela, a suíte S3 usa a região que o `boto3` resolve. |

A suíte S3 precisa de credenciais da AWS que o `boto3` encontre (papel do contêiner ou da instância,
variáveis `AWS_*` ou perfil), das permissões `s3:ListBucket`, `s3:GetObject`, `s3:PutObject` e
`s3:DeleteObject` sob o prefixo (e as de KMS quando o bucket usa SSE-KMS), e das extensões `httpfs`,
`delta` e `aws` do DuckDB. A suíte local precisa só da extensão `delta`. Fora das raízes informadas,
o que uma sessão grava é `.pytest_cache/` na raiz do repositório, do próprio pytest.

## Probes: leituras do ambiente

```
.venv/bin/python probes/space.py
.venv/bin/python probes/bucket.py s3://bucket/prefixo
.venv/bin/python probes/diagnose_aws.py s3://bucket/prefixo
.venv/bin/python probes/redshift.py s3://bucket/prefixo
.venv/bin/python probes/catalog.py
```

Scripts só de leitura, em [`probes/`](probes/README.md), que fotografam o que o ambiente oferece à
biblioteca: o espaço do SageMaker visto de dentro (credenciais, região, projeto, rede, máquina,
Python e DuckDB), o bucket sob a raiz (configuração, ciclo de vida, inventário, tabelas Delta e
versões não correntes, as permissões do papel pela simulação de política do IAM, a chave KMS, a
política do bucket), o acesso que a suíte S3 exige, o Redshift (conexão, papel IAM do `COPY` e seu
alcance sobre a raiz, Data API, sessão, privilégios, configurações e o diagnóstico de um `COPY`
reprovado) e os serviços de catálogo. Cada um imprime o relatório e o grava em `probes/output/`,
pasta fora do git, para ser colado na conversa com o assistente, com seções numeradas, cada chamada
ecoada acima do resultado ou do erro, a tabela de checagens e a seção final de chamadas que
falharam. O JSON de `SERIALIZE_DB_TEST_REPORT` (seção Testes) acompanha os relatórios dos probes na
conversa.

O argumento `s3://bucket/prefixo` é a raiz que a suíte S3 recebe em `SERIALIZE_DB_TEST_S3_ROOT`,
variável que o substitui quando ele falta: `bucket.py` inventaria o que há sob ela,
`diagnose_aws.py` lista `<raiz>/serialize-db-poc/` como a suíte faz e `redshift.py` simula o papel
do `COPY` sobre ela; nenhum probe cria pasta ou objeto. No espaço do SageMaker, a raiz é a área de
trabalho `dev/` do projeto, que `space.py` imprime como `s3_root` na seção do projeto, ou uma
subpasta dela reservada aos testes. Fora disso os probes precisam só das credenciais e da região que
o `boto3` resolve, presentes no espaço; `redshift.py` conecta pelas variáveis
`SERIALIZE_DB_REDSHIFT_*` ou pela conexão Redshift do projeto, e sem elas registra as seções da
sessão como `note`.

Os fatos que `diagnose_aws.py` usa no seu resumo:

- **Região.** O botocore lê `AWS_DEFAULT_REGION` ou o perfil, não `AWS_REGION`, e sem região usa o
  endpoint global `s3.amazonaws.com`, que um endpoint VPC regional não atende. O delta-rs lê
  `AWS_REGION` e `AWS_DEFAULT_REGION`; sem nenhuma, consulta o IMDS e cai em `us-east-1`. A suíte
  copia a região do `boto3` para `AWS_REGION`, num sentido só: um ambiente com apenas `AWS_REGION` e
  sem `~/.aws/config` precisa de manutenção.
- **STS.** `test_boto3_credential_source` chama `get_caller_identity`; um ambiente só com endpoint
  VPC do S3 não alcança o STS, e o teste falharia depois dos 60 s por tentativa e 5 tentativas do
  botocore. O diagnóstico distingue "o serviço respondeu com erro" de "sem resposta": só o segundo
  pede manutenção.
- **Proxy.** Nada na suíte exige proxy; sem as variáveis, nada a fazer. Com elas, o delta-rs roda
  como encontrado e, quando `NO_PROXY` está ausente ou vazia ao lado de `no_proxy`, de novo com
  `NO_PROXY` exportada de `no_proxy`, como a suíte faz; a segunda linha é a que vale para a suíte.
- **Endpoint.** Com `AWS_ENDPOINT_URL`, o `boto3` e o delta-rs o usam, e a suíte não o passa ao
  secret do DuckDB.

Sem rede, `diagnose_aws.py` leva um minuto e meio: o `boto3` desiste em 11 s, o delta-rs em 10 s
(`max_retries` e `retry_timeout` em `storage_options`) e o DuckDB no teto de 60 s do subprocesso.

## Credenciais do delta-rs e proxy

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
padrão falhar em todas, `test_delta_rs_storage_options_fallback` mostra que o caminho de reserva da
biblioteca, as credenciais do `boto3` em `storage_options`, funciona.

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

A suíte local roda sem S3 e valida a pasta preparada; com
`SERIALIZE_DB_TEST_S3_ROOT=s3://bucket/prefixo` a suíte S3 roda também, e a do Redshift com as
variáveis da seção Testes. Não é preciso `uv` nem rede além do S3 e do Redshift: o Python, as
bibliotecas e as extensões vêm da pasta. Os comandos de `.venv/bin/` (como `pytest`) guardam o
caminho original no cabeçalho, por isso a chamada é `python -m pytest`; o pacote roda do mesmo
jeito, com `.venv/bin/python -m serialize_db` ou `.venv/bin/python -c "import serialize_db"`. A
receita foi verificada extraindo o pacote em outro caminho e rodando a suíte com os proxies
apontados para uma porta fechada.
