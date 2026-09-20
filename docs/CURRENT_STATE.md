# Estado da implementação

Este documento registra onde a implementação do plano está, e cada unidade de trabalho o atualiza. O
plano, com as decisões, as regras e as etapas, está em [`PLAN.md`](PLAN.md); o que as provas de
conceito e as leituras do ambiente mostraram, em [`POC.md`](POC.md); as pendências e as decisões em
aberto, em [`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md). O estado descrito aqui é o de 2026-09-20.

## Situação de cada etapa

Nenhum módulo do pacote existe: `src/serialize_db/` tem só o `main` de exemplo do `uv init`, e
`tests/` ainda não tem teste do pacote. A ordem do trabalho está em [`PLAN.md`](PLAN.md).

| Etapa | Situação |
| --- | --- |
| [0. Prova de conceito na AWS](PLAN-STAGE-0.md) | Os itens de S3 estão verificados; os de Redshift esperam a conexão do projeto. |
| [1. `schema`](PLAN-STAGE-1.md) | Não começou; abre a ordem do trabalho, em pasta local. |
| [2. `sql`](PLAN-STAGE-2.md) | Não começou; roda em pasta local, junto com a etapa 1. |
| [3. `storage` e `delta`](PLAN-STAGE-3.md) | Não começou; os casos `-m s3` exigem o bucket. |
| [4. `audit` e motor DuckDB](PLAN-STAGE-4.md) | Não começou; roda em pasta local. |
| [5. Motor Redshift](PLAN-STAGE-5.md) | Não começou; exige a conexão do projeto. |
| [6. Execução e linha de comando](PLAN-STAGE-6.md) | Não começou; roda em pasta local. |
| [7. Carga inicial](PLAN-STAGE-7.md) | Não começou; exige os Parquet de origem. |
| [8. Publicação para clientes](PLAN-STAGE-8.md) | Não começou; exige a conexão do projeto. |
| [9. Operação](PLAN-STAGE-9.md) | Não começou; fecha a ordem do trabalho. |

## O repositório

| Artefato | Estado |
| --- | --- |
| `docs/` | Completa: `parquet.md`, `duckdb.md`, `redshift.md`, `sqlalchemy.md`, `schema.md`, `delta.md`, `guia.md`, `estrategia.md`, `serialize-db.md`, o plano (`PLAN.md` e `PLAN-STAGE-0.md` a `PLAN-STAGE-9.md`), este documento, `POC.md` e `OPEN_QUESTIONS.md`. Todo bloco Python dos documentos rodou com as versões fixadas em `pyproject.toml`; os comandos do Redshift foram compilados, não executados. |
| `tests/model/` | O modelo de referência: os modelos do pipeline (`model_base_contabil.py`, `model_base_gerencial.py` e `model_db_projetado.py`), movidos do pacote para os testes em 2026-09-20. Ele faz o papel da biblioteca cliente: os testes o entregam à API do pacote como um pipeline entregaria os seus modelos, e o pacote não contém modelo algum. Dois não importam (`from lib_base_contabil import Base` e `from lib_base_gerencial import Base`, módulos que o repositório não tem). Os modelos usam `Double` onde o contrato pede `Numeric(18, 2)`, deixam o `autoincrement` padrão nas chaves inteiras (o `duckdb_engine` emite `SERIAL`, que o DuckDB rejeita), declaram chaves estrangeiras `DEFERRABLE INITIALLY DEFERRED` (o DuckDB descarta a cláusula, o Redshift não a tem) e não têm a coluna `mes`, comentários nem `Table.info["serialize_db"]`. A [etapa 1](PLAN-STAGE-1.md) os corrige. |
| `src/serialize_db/__init__.py` | Só o `main` de exemplo do `uv init`; o pacote não tem outro módulo. `pyproject.toml` não declara dependência de execução; o grupo `dev` fixa pytest, deltalake 1.6.4, DuckDB 1.5.5, PyArrow 25.0.1, boto3, redshift-connector, SQLAlchemy 2.0.54, duckdb-engine 0.17.0, sqlalchemy-redshift 1.0.0 e pandas 3.0.6. |
| `tests/` | `conftest.py` com a regra de autorização e as fixtures dos três alvos (pasta local, bucket, Redshift); nenhum teste do pacote ainda; `test_probes.py` testa as funções puras dos probes com respostas fabricadas, sem rede. `tests/proof_of_concept/` com a prova de conceito da camada Delta nos dois armazenamentos (`poc_delta.py`, `test_local.py`, `test_s3.py`), as suítes de estudo das bibliotecas externas e da stdlib (`test_sqlalchemy.py`, `test_duckdb.py`, `test_pyarrow.py`, `test_deltalake.py`, `test_stdlib.py`, `test_concurrency.py`, `test_parallel.py`), comentadas passo a passo por serem o material de aprendizado de quem dará manutenção, e `test_redshift.py`, os itens Redshift da [etapa 0](PLAN-STAGE-0.md), escrito antes de haver conexão e ainda não executado. Cada arquivo de etapa lista as provas de conceito que exercitam as suas APIs. Sem variável, 71 testes passam e 57 são pulados; com a raiz local, 109 passam e 19 são pulados (2026-09-20, macOS). No espaço, em 2026-09-20, uma sessão com a raiz local e a raiz S3 gravou o JSON de `SERIALIZE_DB_TEST_REPORT` com as medições das duas raízes, sem a contagem por resultado nem o registro da limpeza, que o relatório passou a ter (`session.`, `local.cleanup`, `s3.cleanup`); a execução das 04:52 UTC, após as correções de 2026-09-20, registrou 104 testes passados e 7 pulados em 35 s e a limpeza das duas raízes; sem variável, 63 passam e 48 são pulados. |
| `probes/` | Leituras do ambiente, só de leitura: `space.py`, `bucket.py`, `diagnose_aws.py`, `redshift.py` e `catalog.py` sobre `probelib.py`, com o resultado em `probes/output/` para colar na conversa. Quatro execuções no laboratório em 2026-09-20, sem Redshift, corrigiram a leitura das conexões, as leituras de rede, o rótulo dos IPs públicos, a montagem de `~/shared` e o Object Lock, acrescentaram por tabela Delta os arquivos, commits e último objeto, as sessões da suíte S3 e as versões não correntes sob a raiz, e a quarta isolou o 403 do delta-rs (`NO_PROXY` vazia) e fez `diagnose_aws.py` rodar o delta-rs como encontrado e como a suíte; o ambiente de destino ainda não foi lido. |
| `prepare_offline.sh` | Deixa a pasta autossuficiente para o destino sem internet (`.python/`, `.venv/`, `.duckdb/`); verificado extraindo o pacote em outro caminho e rodando a suíte local com proxies mortos. |
