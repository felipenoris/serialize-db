
# Ambiente Lab

```
export UV_PYTHON_DOWNLOADS=automatic uv sync
export S3_TMP_PATH=s3://awsds-sandbox-smus-projects/dzd-d8yrvx1ko7im6o/avhvbqn37ty7m8/shared/serialize-db-tests
.venv/bin/python probes/space.py
.venv/bin/python probes/bucket.py $S3_TMP_PATH
.venv/bin/python probes/diagnose_aws.py $S3_TMP_PATH
.venv/bin/python probes/catalog.py

export AWS_REGION=us-west-2
export SERIALIZE_DB_TEST_LOCAL_ROOT=$HOME/local-serialize-db-tests
export SERIALIZE_DB_TEST_S3_ROOT=s3://awsds-sandbox-smus-projects/dzd-d8yrvx1ko7im6o/avhvbqn37ty7m8/shared/serialize-db-tests
export SERIALIZE_DB_TEST_REPORT=$HOME/tests-report.json
uv run pytest
```

# Ambiente Alvo

## Lista de variáveis de ambiente

```
SERIALIZE_DB_TEST_LOCAL_ROOT
SERIALIZE_DB_TEST_S3_ROOT
SERIALIZE_DB_REDSHIFT_WORKGROUP
SERIALIZE_DB_REDSHIFT_DATABASE
SERIALIZE_DB_REDSHIFT_SHARE_DATABASE
SERIALIZE_DB_REDSHIFT_SCHEMA
SERIALIZE_DB_TEST_REDSHIFT_SCHEMA
AWS_DEFAULT_REGION
SOURCE_PATH
TARGET_ROOT_PATH
```

## Probes

```
cd ~/work/projects/serialize-db

.venv/bin/python probes/redshift.py $SERIALIZE_DB_TEST_S3_ROOT
.venv/bin/python probes/space.py
.venv/bin/python probes/bucket.py $SERIALIZE_DB_TEST_S3_ROOT
.venv/bin/python probes/diagnose_aws.py $SERIALIZE_DB_TEST_S3_ROOT
.venv/bin/python probes/catalog.py

SERIALIZE_DB_TEST_REPORT=probes/output/suite_s3.json .venv/bin/python -m pytest -m "not redshift"
SERIALIZE_DB_TEST_REPORT=probes/output/redshift_suite_1.json .venv/bin/python -m pytest -m redshift
SERIALIZE_DB_TEST_REPORT=probes/output/redshift_suite_2.json .venv/bin/python -m pytest -m redshift
SERIALIZE_DB_TEST_REPORT=probes/output/engine_redshift_1.json .venv/bin/python -m pytest -m redshift tests/test_engine_redshift.py
SERIALIZE_DB_TEST_REPORT=probes/output/engine_redshift_2.json .venv/bin/python -m pytest -m redshift tests/test_engine_redshift.py
SERIALIZE_DB_TEST_REPORT=probes/output/publication_1.json .venv/bin/python -m pytest -m redshift tests/test_publication.py
SERIALIZE_DB_TEST_REPORT=probes/output/publication_2.json .venv/bin/python -m pytest -m redshift tests/test_publication.py
```

# Migração Parquet -> Delta

```
cd ~/work/projects/serialize-db
mkdir probes/output

export PYTHONPATH=tests

.venv/bin/python scripts/migrate_parquet_to_delta.py \
    --metadata client_model:Base.metadata \
    --environment prd \
    --source $SOURCE_PATH \
    --root $TARGET_ROOT_PATH \
    --report probes/output/carga_inicial_delta.json

.venv/bin/serialize-db audit \
    --root $TARGET_ROOT_PATH \
    --environment prd \
    --metadata client_model:Base.metadata \
    --table cad_lancamentos \
    --partitions 2026-01-31 \
    --foreign-keys

.venv/bin/serialize-db history --root $TARGET_ROOT_PATH --environment prd --metadata client_model:Base.metadata --table cad_lancamentos
.venv/bin/serialize-db snapshot --root $TARGET_ROOT_PATH --environment prd --metadata client_model:Base.metadata --name carga-2026-09-24
.venv/bin/serialize-db vacuum --root $TARGET_ROOT_PATH --environment prd --metadata client_model:Base.metadata
.venv/bin/serialize-db archive --root $TARGET_ROOT_PATH --environment prd --metadata client_model:Base.metadata --name carga-2026-09-24
```

Benchmark threads do duckdb:

```
.venv/bin/python probes/duckdb_threads.py $TARGET_ROOT_PATH/prd
```

# Publicação Delta -> Redshift

```
export PYTHONPATH=tests

# 1. Uma vez por esquema: a tabela de controle serialize_db_publications.
#    Sem ela, publish_redshift recusa com PublicationError; a segunda chamada falha
#    porque a tabela já existe (sem IF NOT EXISTS, por decisão sua).
.venv/bin/serialize-db publish_redshift --init

# 2. O snapshot da carga e o canal default apontado para ele: a publicação e o leitor
#    Delta sem argumento leem esse snapshot. "serialize-db channel" sem opções lista os canais.
.venv/bin/serialize-db snapshot --root $TARGET_ROOT_PATH --environment prd \
    --metadata client_model:Base.metadata --name carga-2026-09-25
.venv/bin/serialize-db channel --root $TARGET_ROOT_PATH --environment prd \
    --metadata client_model:Base.metadata --name default --snapshot carga-2026-09-25
.venv/bin/serialize-db channel --root $TARGET_ROOT_PATH --environment prd \
    --metadata client_model:Base.metadata

# 3. Primeiro uma tabela pequena, para validar o caminho no alvo; publish_redshift exige
#    --snapshot <nome> ou --channel <nome> (default, current).
.venv/bin/serialize-db publish_redshift --root $TARGET_ROOT_PATH --environment prd \
    --metadata client_model:Base.metadata --tables cad_contas --channel default

# 4. A base inteira, uma conexão por tabela em paralelo.
.venv/bin/serialize-db publish_redshift --root $TARGET_ROOT_PATH --environment prd \
    --metadata client_model:Base.metadata --max-workers 4 --channel default

# 5. O estado: versão publicada, versão atual e partições pendentes por tabela.
.venv/bin/serialize-db publish_redshift --root $TARGET_ROOT_PATH --environment prd \
    --metadata client_model:Base.metadata --status

# 6. A versão atual sem snapshot (--channel current) e a volta a um snapshot pelo nome, que
#    troca só as partições alteradas entre as duas versões.
.venv/bin/serialize-db publish_redshift --root $TARGET_ROOT_PATH --environment prd \
    --metadata client_model:Base.metadata --channel current
.venv/bin/serialize-db publish_redshift --root $TARGET_ROOT_PATH --environment prd \
    --metadata client_model:Base.metadata --snapshot carga-2026-09-25 --tables cad_contas
```

Teste credencial expirando (leva 1h):

```
# probe credentials: 1h de leitura
.venv/bin/python probes/credentials.py $TARGET_ROOT_PATH/prd/cad_contas
```

# Acesso de leitura

```
# O leitor Delta sobre o snapshot do canal default e o leitor Redshift sobre as tabelas
# publicadas, com o mesmo statement; o tempo de abertura das 12 views é a leitura pendente.
PYTHONPATH=tests .venv/bin/python - <<'PY'
import os
import time
import sqlalchemy as sa
from client_model import Base
from serialize_db import Database

db = Database(os.environ["TARGET_ROOT_PATH"], "prd", Base.metadata)
contas = Base.metadata.tables["cad_contas"]
statement = sa.select(sa.func.count()).select_from(contas)
started = time.perf_counter()
with db.open_delta() as reader:
    print(f"leitor Delta aberto em {time.perf_counter() - started:.3f} s: {reader.versions}")
    print("delta:", reader.query(statement).to_pylist())
with db.open_redshift() as reader:
    print("redshift:", reader.query(statement).to_pylist())
PY
```

# Exportação e Compact

```
# export por cópia (o padrão): os mesmos bytes dos arquivos da versão atual, sem o log
.venv/bin/serialize-db export --root $TARGET_ROOT_PATH --environment prd --metadata client_model:Base.metadata --table cad_lancamentos --destination $TARGET_ROOT_PATH/prd/exportacao/carga-2026-09-24/cad_lancamentos

# export por reescrita: um arquivo por partição pelo COPY do DuckDB, com os limites do ambiente
.venv/bin/serialize-db export --root $TARGET_ROOT_PATH --environment prd --metadata client_model:Base.metadata --table cad_lancamentos --destination $TARGET_ROOT_PATH/prd/exportacao/carga-2026-09-24-reescrita/cad_lancamentos --mode rewrite

# compact: exige --partitions numa tabela particionada e recusa a tabela com um snapshot na
# versão atual; numa partição de um arquivo só, não grava nada
.venv/bin/serialize-db compact --root $TARGET_ROOT_PATH --environment prd --metadata client_model:Base.metadata --table cad_lancamentos --partitions 2026-03-31
```

# Sondas de consistência

```
cd ~/work/projects/serialize-db
mkdir -p $HOME/serialize-db-local probes/output

export PYTHONPATH=tests

# As sete sondas da pasta local gravam sob $SERIALIZE_DB_TEST_LOCAL_ROOT/consistencia/<sonda>/,
# que cada uma apaga no fim, e imprimem o relatório no terminal e em
# probes/output/consistencia_<sonda>_<data-hora>.txt; código de saída 1 quando alguma checagem
# reprova. O sinal do zero, as estatísticas que deep_copy não registra e as atualizações perdidas
# do arquivo de controle (plan/OPEN_QUESTIONS.md) saem como leituras conhecidas, não como reprovação.
.venv/bin/python probes/consistencia/probe_types.py
.venv/bin/python probes/consistencia/probe_stream.py
.venv/bin/python probes/consistencia/probe_execution.py
.venv/bin/python probes/consistencia/probe_delta_ops.py
.venv/bin/python probes/consistencia/probe_reader.py
.venv/bin/python probes/consistencia/probe_load.py
.venv/bin/python probes/consistencia/probe_pandas.py

# A sonda do motor Redshift roda pelo pytest com as fixtures das suítes: grava sob
# $SERIALIZE_DB_TEST_S3_ROOT/serialize-db-poc/<id>/ e publica no esquema da suíte num ambiente
# poc<id> próprio, cujas tabelas poc<id>_* e linhas de controle saem no fim; -s imprime as
# leituras e os problemas.
SERIALIZE_DB_TEST_REPORT=probes/output/consistencia_redshift.json .venv/bin/python -m pytest -p conftest -m redshift -s probes/consistencia/probe_redshift_test.py 2>&1 | tee probes/output/consistencia_redshift.txt

# A sonda de dois escritores na mesma tabela do sandbox roda pelo pytest nos dois motores: -m local
# no DuckDB, sob $SERIALIZE_DB_TEST_LOCAL_ROOT/serialize-db-poc/<id>/, e -m redshift no alvo, com o
# Delta e o staging sob $SERIALIZE_DB_TEST_S3_ROOT/serialize-db-poc/<id>/ e as tabelas do sandbox da
# execução poc-<id> no esquema da suíte, apagadas no fim. O desfecho de cada escrita (entrou, ou
# conflito) é leitura; a checagem é a consistência do que ficou.
.venv/bin/python -m pytest -p conftest -m local -s probes/consistencia/probe_append_test.py
SERIALIZE_DB_TEST_REPORT=probes/output/consistencia_append_redshift.json .venv/bin/python -m pytest -p conftest -m redshift -s probes/consistencia/probe_append_test.py 2>&1 | tee probes/output/consistencia_append_redshift.txt
```

# Sondas da operação

```
cd ~/work/projects/serialize-db

# As sondas que recebem $SOURCE_PATH leem cad_lancamentos nele. Cada sonda grava sob
# $SERIALIZE_DB_TEST_S3_ROOT/serialize-db-operacao/<sonda>-<id>/, que apaga no fim, e imprime o
# relatório no terminal e em probes/output/operacao_<sonda>_<data-hora>.txt; código de saída 1
# quando alguma checagem reprova.

# A carga das três primeiras partições, encerrada por SIGKILL quando o arquivo da segunda aparece,
# e o mesmo comando de novo.
.venv/bin/python probes/operacao/probe_load_resume.py $SOURCE_PATH

# O archive de um snapshot das três partições, encerrado por SIGKILL depois da primeira partição
# copiada, e o mesmo comando de novo.
.venv/bin/python probes/operacao/probe_archive_resume.py $SOURCE_PATH

# O vacuum --full de dois órfãos com a retenção padrão, com --retention-hours 0 e com --apply.
.venv/bin/python probes/operacao/probe_vacuum_orphans.py $SOURCE_PATH

# O compact da primeira partição repartida em cerca de 32 arquivos, com o tempo e o pico de RSS.
.venv/bin/python probes/operacao/probe_compact_memory.py $SOURCE_PATH

# O UNLOAD da exportação com PARALLEL OFF e em paralelo, de 1, 5, 10 e 20 milhões de linhas e da
# primeira partição inteira, três vezes cada; as tabelas exec_operacao_<id>_* saem no fim.
.venv/bin/python probes/operacao/probe_unload_parallel.py $SOURCE_PATH

# O ganho das APIs com threads sobre a execução em série no motor DuckDB, nos pools de tabelas, no
# motor Redshift e na publicação no Redshift, três medidas de cada forma, sobre quatro tabelas de
# 5 milhões de linhas que a sonda gera; as tabelas exec_* do sandbox e as poc<id>_* publicadas,
# com as linhas de controle delas, saem no fim.
.venv/bin/python probes/operacao/probe_parallel_gain.py
```

# Resultados

```
mkdir ~/output
cd ~/work/projects/serialize-db

mv relatorio_*.json ~/output
mv probes/output/* ~/output
tar -czf ~/output.tar.gz ~/output
mv ~/output.tar.gz ~/volume/shared/fnoro/serialize-db
```
