
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
export SERIALIZE_DB_TEST_S3_ROOT=s3://bndes-aco-models-138071776059/dzd-5qqmzj3amjp657/3hpfa7636y4qor/shared/serialize-db-tests
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

export AWS_DEFAULT_REGION=sa-east-1
export SERIALIZE_DB_REDSHIFT_WORKGROUP=controladoria-wg
export SERIALIZE_DB_REDSHIFT_DATABASE=dev
export SERIALIZE_DB_REDSHIFT_SHARE_DATABASE=datalake_rw_shared
export SERIALIZE_DB_TEST_REDSHIFT_SCHEMA=sbx_aco_decon
export SERIALIZE_DB_TEST_S3_ROOT=s3://bndes-aco-models-138071776059/dzd-5qqmzj3amjp657/3hpfa7636y4qor/shared/serialize-db-tests
SERIALIZE_DB_TEST_REPORT=probes/output/redshift_suite_1.json .venv/bin/python -m pytest -m redshift
SERIALIZE_DB_TEST_REPORT=probes/output/redshift_suite_2.json .venv/bin/python -m pytest -m redshift
```

# Migração Parquet -> Delta

```
export PYTHONPATH=tests
export TABELA=cad_aliquotas
.venv/bin/python scripts/migrate_parquet_to_delta.py \
    --metadata client_model:Base.metadata \
    --source s3://bndes-aco-models-138071776059/dzd-5qqmzj3amjp657/3hpfa7636y4qor/shared/bndes_grupos_bases_analise_financeira/databases/prd/db_projetado \
    --root   s3://bndes-aco-models-138071776059/dzd-5qqmzj3amjp657/3hpfa7636y4qor/shared/fnoro/delta/db_projetado \
    --tables "${TABELA}" --report "relatorio_${TABELA}.json"

export PYTHONPATH=tests
export TABELA=cad_contas
.venv/bin/python scripts/migrate_parquet_to_delta.py \
    --metadata client_model:Base.metadata \
    --source s3://bndes-aco-models-138071776059/dzd-5qqmzj3amjp657/3hpfa7636y4qor/shared/bndes_grupos_bases_analise_financeira/databases/prd/db_projetado \
    --root   s3://bndes-aco-models-138071776059/dzd-5qqmzj3amjp657/3hpfa7636y4qor/shared/fnoro/delta/db_projetado \
    --tables "${TABELA}" --report "relatorio_${TABELA}.json"

export PYTHONPATH=tests
export TABELA=cad_contratos
.venv/bin/python scripts/migrate_parquet_to_delta.py \
    --metadata client_model:Base.metadata \
    --source s3://bndes-aco-models-138071776059/dzd-5qqmzj3amjp657/3hpfa7636y4qor/shared/bndes_grupos_bases_analise_financeira/databases/prd/db_projetado \
    --root   s3://bndes-aco-models-138071776059/dzd-5qqmzj3amjp657/3hpfa7636y4qor/shared/fnoro/delta/db_projetado \
    --tables "${TABELA}" --report "relatorio_${TABELA}.json"

export PYTHONPATH=tests
export TABELA=cad_lancamentos
.venv/bin/python scripts/migrate_parquet_to_delta.py \
    --metadata client_model:Base.metadata \
    --source s3://bndes-aco-models-138071776059/dzd-5qqmzj3amjp657/3hpfa7636y4qor/shared/bndes_grupos_bases_analise_financeira/databases/prd/db_projetado \
    --root   s3://bndes-aco-models-138071776059/dzd-5qqmzj3amjp657/3hpfa7636y4qor/shared/fnoro/delta/db_projetado \
    --tables "${TABELA}" --report "relatorio_${TABELA}.json"

export PYTHONPATH=tests
export TABELA=cad_operacoes
.venv/bin/python scripts/migrate_parquet_to_delta.py \
    --metadata client_model:Base.metadata \
    --source s3://bndes-aco-models-138071776059/dzd-5qqmzj3amjp657/3hpfa7636y4qor/shared/bndes_grupos_bases_analise_financeira/databases/prd/db_projetado \
    --root   s3://bndes-aco-models-138071776059/dzd-5qqmzj3amjp657/3hpfa7636y4qor/shared/fnoro/delta/db_projetado \
    --tables "${TABELA}" --report "relatorio_${TABELA}.json"

export PYTHONPATH=tests
export TABELA=dom_hierarquias_contas
.venv/bin/python scripts/migrate_parquet_to_delta.py \
    --metadata client_model:Base.metadata \
    --source s3://bndes-aco-models-138071776059/dzd-5qqmzj3amjp657/3hpfa7636y4qor/shared/bndes_grupos_bases_analise_financeira/databases/prd/db_projetado \
    --root   s3://bndes-aco-models-138071776059/dzd-5qqmzj3amjp657/3hpfa7636y4qor/shared/fnoro/delta/db_projetado \
    --tables "${TABELA}" --report "relatorio_${TABELA}.json"

export PYTHONPATH=tests
export TABELA=dom_mensuracoes
.venv/bin/python scripts/migrate_parquet_to_delta.py \
    --metadata client_model:Base.metadata \
    --source s3://bndes-aco-models-138071776059/dzd-5qqmzj3amjp657/3hpfa7636y4qor/shared/bndes_grupos_bases_analise_financeira/databases/prd/db_projetado \
    --root   s3://bndes-aco-models-138071776059/dzd-5qqmzj3amjp657/3hpfa7636y4qor/shared/fnoro/delta/db_projetado \
    --tables "${TABELA}" --report "relatorio_${TABELA}.json"

export PYTHONPATH=tests
export TABELA=dom_negocios
.venv/bin/python scripts/migrate_parquet_to_delta.py \
    --metadata client_model:Base.metadata \
    --source s3://bndes-aco-models-138071776059/dzd-5qqmzj3amjp657/3hpfa7636y4qor/shared/bndes_grupos_bases_analise_financeira/databases/prd/db_projetado \
    --root   s3://bndes-aco-models-138071776059/dzd-5qqmzj3amjp657/3hpfa7636y4qor/shared/fnoro/delta/db_projetado \
    --tables "${TABELA}" --report "relatorio_${TABELA}.json"

export PYTHONPATH=tests
export TABELA=dom_segmentos
.venv/bin/python scripts/migrate_parquet_to_delta.py \
    --metadata client_model:Base.metadata \
    --source s3://bndes-aco-models-138071776059/dzd-5qqmzj3amjp657/3hpfa7636y4qor/shared/bndes_grupos_bases_analise_financeira/databases/prd/db_projetado \
    --root   s3://bndes-aco-models-138071776059/dzd-5qqmzj3amjp657/3hpfa7636y4qor/shared/fnoro/delta/db_projetado \
    --tables "${TABELA}" --report "relatorio_${TABELA}.json"

export PYTHONPATH=tests
export TABELA=dom_veiculos
.venv/bin/python scripts/migrate_parquet_to_delta.py \
    --metadata client_model:Base.metadata \
    --source s3://bndes-aco-models-138071776059/dzd-5qqmzj3amjp657/3hpfa7636y4qor/shared/bndes_grupos_bases_analise_financeira/databases/prd/db_projetado \
    --root   s3://bndes-aco-models-138071776059/dzd-5qqmzj3amjp657/3hpfa7636y4qor/shared/fnoro/delta/db_projetado \
    --tables "${TABELA}" --report "relatorio_${TABELA}.json"

export PYTHONPATH=tests
export TABELA=rel_contas_hierarquias
.venv/bin/python scripts/migrate_parquet_to_delta.py \
    --metadata client_model:Base.metadata \
    --source s3://bndes-aco-models-138071776059/dzd-5qqmzj3amjp657/3hpfa7636y4qor/shared/bndes_grupos_bases_analise_financeira/databases/prd/db_projetado \
    --root   s3://bndes-aco-models-138071776059/dzd-5qqmzj3amjp657/3hpfa7636y4qor/shared/fnoro/delta/db_projetado \
    --tables "${TABELA}" --report "relatorio_${TABELA}.json"

export PYTHONPATH=tests
export TABELA=rel_contrato_operacao
.venv/bin/python scripts/migrate_parquet_to_delta.py \
    --metadata client_model:Base.metadata \
    --source s3://bndes-aco-models-138071776059/dzd-5qqmzj3amjp657/3hpfa7636y4qor/shared/bndes_grupos_bases_analise_financeira/databases/prd/db_projetado \
    --root   s3://bndes-aco-models-138071776059/dzd-5qqmzj3amjp657/3hpfa7636y4qor/shared/fnoro/delta/db_projetado \
    --tables "${TABELA}" --report "relatorio_${TABELA}.json"
```