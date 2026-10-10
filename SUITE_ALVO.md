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

## Bateria

Os passos da bateria estão em `suite_alvo.sh`, na ordem em que rodam: os probes e as suítes, a
migração Parquet -> Delta, a publicação Delta -> Redshift, o acesso de leitura, a exportação e o
compact, a sonda da base publicada, as sondas de consistência e as sondas da operação, cada passo
com o seu comentário. O script confere as variáveis acima, roda cada passo mesmo quando o anterior
falhou (Ctrl-C encerra o passo em curso e o script), grava tudo o que vai ao terminal em
`probes/output/suite_alvo_<data-hora>.txt`, ao lado dos relatórios das sondas, e termina com a
tabela dos passos e o código de saída de cada um.

```
cd ~/work/projects/serialize-db
./suite_alvo.sh
```

## Sonda das threads do DuckDB

Fora da bateria desde 2026-10-10, por decisão do usuário: as seis rodadas no alvo, de 2026-09-24 a
2026-10-09, terminaram com o `threads` nas CPUs do processo, por instrução do usuário de
2026-09-24, e o passo levou de 46 minutos, com 8 vCPUs, a 93, com 4. Roda à mão, com as variáveis
acima exportadas e depois da migração, nunca junto com ela, se a máquina ou a versão do DuckDB
mudar; o relatório sai em `probes/output/duckdb_threads_<data-hora>.txt`.

```
cd ~/work/projects/serialize-db
PYTHONPATH=tests .venv/bin/python probes/duckdb_threads.py "$TARGET_ROOT_PATH/prd"
```

## Sonda da credencial expirando

Fora da bateria desde 2026-10-10, por decisão do usuário: as cinco rodadas no alvo com o código
atual, de 2026-09-26 a 2026-10-09, leram todos os clientes renovando a credencial, sem leitura
falhando, e o passo leva cerca de 63 minutos. Roda à mão, com as variáveis acima exportadas e
depois da migração, que grava a tabela que ela lê, se a credencial do contêiner, o driver do
Redshift ou um cliente do pacote mudar; o relatório sai em
`probes/output/credentials_<data-hora>.txt`.

```
cd ~/work/projects/serialize-db
.venv/bin/python probes/credentials.py "$TARGET_ROOT_PATH/prd/cad_contas"
```

## Sonda do UNLOAD em paralelo

Fora da bateria desde 2026-10-10: cinco rodadas, de 2026-10-05 a 2026-10-10, leram um arquivo nos
dois modos e o mesmo tempo. Roda à mão, com as variáveis acima exportadas, se o workgroup mudar ou
um cluster provisionado entrar; o relatório sai em `probes/output/operacao_unload_<data-hora>.txt`.

```
cd ~/work/projects/serialize-db
.venv/bin/python probes/operacao/probe_unload_parallel.py "$SOURCE_PATH" --ignore-partitions 2025-09-30
```

# Resultados

```
mkdir ~/output
cd ~/work/projects/serialize-db

mv probes/output/* ~/output
tar -czf ~/output.tar.gz ~/output
mv ~/output.tar.gz ~/volume/shared/fnoro/serialize-db
```
