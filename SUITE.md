
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

```
cd ~/work/projects/serialize-db

export PYTHONPATH=tests
export SOURCE_PATH=s3://bndes-aco-models-138071776059/dzd-5qqmzj3amjp657/3hpfa7636y4qor/shared/bndes_grupos_bases_analise_financeira/databases/prd/db_projetado
export TARGET_ROOT_PATH=s3://bndes-aco-models-138071776059/dzd-5qqmzj3amjp657/3hpfa7636y4qor/shared/fnoro/serialize-db/delta/db_projetado

export TABELA=cad_aliquotas
.venv/bin/python scripts/migrate_parquet_to_delta.py \
    --metadata client_model:Base.metadata \
    --source $SOURCE_PATH \
    --root   $TARGET_ROOT_PATH \
    --tables "${TABELA}" --report "relatorio_${TABELA}.json"

export TABELA=cad_contas
.venv/bin/python scripts/migrate_parquet_to_delta.py \
    --metadata client_model:Base.metadata \
    --source $SOURCE_PATH \
    --root   $TARGET_ROOT_PATH \
    --tables "${TABELA}" --report "relatorio_${TABELA}.json"

export TABELA=cad_contratos
.venv/bin/python scripts/migrate_parquet_to_delta.py \
    --metadata client_model:Base.metadata \
    --source $SOURCE_PATH \
    --root   $TARGET_ROOT_PATH \
    --tables "${TABELA}" --report "relatorio_${TABELA}.json"

export TABELA=cad_lancamentos
.venv/bin/python scripts/migrate_parquet_to_delta.py \
    --metadata client_model:Base.metadata \
    --source $SOURCE_PATH \
    --root   $TARGET_ROOT_PATH \
    --tables "${TABELA}" --report "relatorio_${TABELA}.json"

export TABELA=cad_operacoes
.venv/bin/python scripts/migrate_parquet_to_delta.py \
    --metadata client_model:Base.metadata \
    --source $SOURCE_PATH \
    --root   $TARGET_ROOT_PATH \
    --tables "${TABELA}" --report "relatorio_${TABELA}.json"

export TABELA=dom_hierarquias_contas
.venv/bin/python scripts/migrate_parquet_to_delta.py \
    --metadata client_model:Base.metadata \
    --source $SOURCE_PATH \
    --root   $TARGET_ROOT_PATH \
    --tables "${TABELA}" --report "relatorio_${TABELA}.json"

export TABELA=dom_mensuracoes
.venv/bin/python scripts/migrate_parquet_to_delta.py \
    --metadata client_model:Base.metadata \
    --source $SOURCE_PATH \
    --root   $TARGET_ROOT_PATH \
    --tables "${TABELA}" --report "relatorio_${TABELA}.json"

export TABELA=dom_negocios
.venv/bin/python scripts/migrate_parquet_to_delta.py \
    --metadata client_model:Base.metadata \
    --source $SOURCE_PATH \
    --root   $TARGET_ROOT_PATH \
    --tables "${TABELA}" --report "relatorio_${TABELA}.json"

export TABELA=dom_segmentos
.venv/bin/python scripts/migrate_parquet_to_delta.py \
    --metadata client_model:Base.metadata \
    --source $SOURCE_PATH \
    --root   $TARGET_ROOT_PATH \
    --tables "${TABELA}" --report "relatorio_${TABELA}.json"

export TABELA=dom_veiculos
.venv/bin/python scripts/migrate_parquet_to_delta.py \
    --metadata client_model:Base.metadata \
    --source $SOURCE_PATH \
    --root   $TARGET_ROOT_PATH \
    --tables "${TABELA}" --report "relatorio_${TABELA}.json"

export TABELA=rel_contas_hierarquias
.venv/bin/python scripts/migrate_parquet_to_delta.py \
    --metadata client_model:Base.metadata \
    --source $SOURCE_PATH \
    --root   $TARGET_ROOT_PATH \
    --tables "${TABELA}" --report "relatorio_${TABELA}.json"

export TABELA=rel_contrato_operacao
.venv/bin/python scripts/migrate_parquet_to_delta.py \
    --metadata client_model:Base.metadata \
    --source $SOURCE_PATH \
    --root   $TARGET_ROOT_PATH \
    --tables "${TABELA}" --report "relatorio_${TABELA}.json"

export AWS_DEFAULT_REGION=sa-east-1
.venv/bin/python probes/duckdb_threads.py $TARGET_ROOT_PATH
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
