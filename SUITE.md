
# Probes e Testes - Lab

```
export UV_PYTHON_DOWNLOADS=automatic uv sync --group dev
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

# Probes e Testes - BN

```
cd ~/work/projects/serialize-db

export SERIALIZE_DB_TEST_S3_ROOT=s3://bndes-aco-models-138071776059/dzd-5qqmzj3amjp657/3hpfa7636y4qor/shared/fnoro/serialize-db/serialize-db-tests
export SERIALIZE_DB_REDSHIFT_WORKGROUP=controladoria-wg
export SERIALIZE_DB_REDSHIFT_DATABASE=dev
export SERIALIZE_DB_REDSHIFT_SHARE_DATABASE=datalake_rw_shared
export SERIALIZE_DB_REDSHIFT_SCHEMA=sbx_aco_decon
export AWS_DEFAULT_REGION=sa-east-1
.venv/bin/python probes/redshift.py $SERIALIZE_DB_TEST_S3_ROOT
.venv/bin/python probes/space.py
.venv/bin/python probes/bucket.py $SERIALIZE_DB_TEST_S3_ROOT
.venv/bin/python probes/diagnose_aws.py $SERIALIZE_DB_TEST_S3_ROOT
.venv/bin/python probes/catalog.py

mkdir $HOME/serialize-db-local

export AWS_DEFAULT_REGION=sa-east-1
export SERIALIZE_DB_TEST_LOCAL_ROOT=$HOME/serialize-db-local
export SERIALIZE_DB_TEST_S3_ROOT=s3://bndes-aco-models-138071776059/dzd-5qqmzj3amjp657/3hpfa7636y4qor/shared/fnoro/serialize-db/serialize-db-tests
export SERIALIZE_DB_TEST_REDSHIFT_SCHEMA=sbx_aco_decon
export SERIALIZE_DB_REDSHIFT_WORKGROUP=controladoria-wg
export SERIALIZE_DB_REDSHIFT_DATABASE=dev
export SERIALIZE_DB_REDSHIFT_SHARE_DATABASE=datalake_rw_shared
SERIALIZE_DB_TEST_REPORT=probes/output/suite_s3.json .venv/bin/python -m pytest -m "not redshift"
SERIALIZE_DB_TEST_REPORT=probes/output/redshift_suite_1.json .venv/bin/python -m pytest -m redshift
SERIALIZE_DB_TEST_REPORT=probes/output/redshift_suite_2.json .venv/bin/python -m pytest -m redshift
SERIALIZE_DB_TEST_REPORT=probes/output/engine_redshift_1.json .venv/bin/python -m pytest -m redshift tests/test_engine_redshift.py
SERIALIZE_DB_TEST_REPORT=probes/output/engine_redshift_2.json .venv/bin/python -m pytest -m redshift tests/test_engine_redshift.py
SERIALIZE_DB_TEST_REPORT=probes/output/publication_1.json .venv/bin/python -m pytest -m redshift tests/test_publication.py
SERIALIZE_DB_TEST_REPORT=probes/output/publication_2.json .venv/bin/python -m pytest -m redshift tests/test_publication.py
```

# Migração Parquet -> Delta

Tabelas:

```
cad_aliquotas
cad_contas
cad_contratos
cad_lancamentos
cad_operacoes
dom_hierarquias_contas
dom_mensuracoes
dom_negocios
dom_segmentos
dom_veiculos
rel_contas_hierarquias
rel_contrato_operacao
```

```
cd ~/work/projects/serialize-db
mkdir probes/output

export PYTHONPATH=tests
export SOURCE_PATH=s3://bndes-aco-models-138071776059/dzd-5qqmzj3amjp657/3hpfa7636y4qor/shared/bndes_grupos_bases_analise_financeira/databases/prd/db_projetado
export TARGET_ROOT_PATH=s3://bndes-aco-models-138071776059/dzd-5qqmzj3amjp657/3hpfa7636y4qor/shared/fnoro/serialize-db/delta
export AWS_DEFAULT_REGION=sa-east-1

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

.venv/bin/python probes/duckdb_threads.py $TARGET_ROOT_PATH/prd

.venv/bin/serialize-db history --root $TARGET_ROOT_PATH --environment prd --metadata client_model:Base.metadata --table cad_lancamentos
.venv/bin/serialize-db snapshot --root $TARGET_ROOT_PATH --environment prd --metadata client_model:Base.metadata --name carga-2026-09-24
.venv/bin/serialize-db vacuum --root $TARGET_ROOT_PATH --environment prd --metadata client_model:Base.metadata
.venv/bin/serialize-db archive --root $TARGET_ROOT_PATH --environment prd --metadata client_model:Base.metadata --name carga-2026-09-24
```

# Publicação Delta -> Redshift

```
export AWS_DEFAULT_REGION=sa-east-1
export SERIALIZE_DB_REDSHIFT_WORKGROUP=controladoria-wg
export SERIALIZE_DB_REDSHIFT_DATABASE=dev
export SERIALIZE_DB_REDSHIFT_SHARE_DATABASE=datalake_rw_shared
export SERIALIZE_DB_REDSHIFT_SCHEMA=sbx_aco_decon
export TARGET_ROOT_PATH=s3://bndes-aco-models-138071776059/dzd-5qqmzj3amjp657/3hpfa7636y4qor/shared/fnoro/serialize-db/delta
export PYTHONPATH=tests

# 1. Uma vez por esquema: a tabela de controle serialize_db_publications.
#    Sem ela, publish recusa com PublicationError; a segunda chamada falha
#    porque a tabela já existe (sem IF NOT EXISTS, por decisão sua).
.venv/bin/serialize-db publish --init

# 2. O snapshot da carga e o canal default apontado para ele: a publicação e o leitor
#    Delta sem argumento leem esse snapshot. "serialize-db channel" sem opções lista os canais.
.venv/bin/serialize-db snapshot --root $TARGET_ROOT_PATH --environment prd \
    --metadata client_model:Base.metadata --name carga-2026-09-25
.venv/bin/serialize-db channel --root $TARGET_ROOT_PATH --environment prd \
    --metadata client_model:Base.metadata --name default --snapshot carga-2026-09-25
.venv/bin/serialize-db channel --root $TARGET_ROOT_PATH --environment prd \
    --metadata client_model:Base.metadata

# 3. Primeiro uma tabela pequena, para validar o caminho no alvo; publish exige
#    --snapshot <nome> ou --channel <nome> (default, current).
.venv/bin/serialize-db publish --root $TARGET_ROOT_PATH --environment prd \
    --metadata client_model:Base.metadata --tables cad_contas --channel default

# 4. A base inteira, uma conexão por tabela em paralelo.
.venv/bin/serialize-db publish --root $TARGET_ROOT_PATH --environment prd \
    --metadata client_model:Base.metadata --max-workers 4 --channel default

# 5. O estado: versão publicada, versão atual e partições pendentes por tabela.
.venv/bin/serialize-db publish --root $TARGET_ROOT_PATH --environment prd \
    --metadata client_model:Base.metadata --status

# 6. A versão atual sem snapshot (--channel current) e a volta a um snapshot pelo nome, que
#    troca só as partições alteradas entre as duas versões.
.venv/bin/serialize-db publish --root $TARGET_ROOT_PATH --environment prd \
    --metadata client_model:Base.metadata --channel current
.venv/bin/serialize-db publish --root $TARGET_ROOT_PATH --environment prd \
    --metadata client_model:Base.metadata --snapshot carga-2026-09-25 --tables cad_contas

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

# Resultados

```
mkdir ~/output
cd ~/work/projects/serialize-db

mv relatorio_*.json ~/output
mv probes/output/* ~/output
tar -czf ~/output.tar.gz ~/output
mv ~/output.tar.gz ~/volume/shared/fnoro/serialize-db
```
