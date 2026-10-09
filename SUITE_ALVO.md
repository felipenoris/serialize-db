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
migração Parquet -> Delta, a publicação Delta -> Redshift, o teste da credencial expirando (1h), o
acesso de leitura, a exportação e o compact, a sonda da base publicada, as sondas de consistência
e as sondas da operação, cada passo com o seu comentário. O script confere as variáveis acima,
roda cada passo mesmo quando o anterior falhou (Ctrl-C encerra o passo em curso e o script), grava
tudo o que vai ao terminal em `probes/output/suite_alvo_<data-hora>.txt`, ao lado dos relatórios
das sondas, e termina com a tabela dos passos e o código de saída de cada um.

```
cd ~/work/projects/serialize-db
./suite_alvo.sh
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
