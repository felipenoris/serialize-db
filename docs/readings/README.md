# `docs/readings/`: os relatórios dos probes que viraram registro

Cada arquivo aqui é a saída de uma execução de `probes/<script>.py` num ambiente real, ou o JSON
de `SERIALIZE_DB_TEST_REPORT` de uma sessão de suíte, guardados como saíram. A pasta `probes/output/`
fica fora do git e recebe toda execução; esta guarda as que sustentam uma afirmação de
[`../POC.md`](../POC.md), que é onde a leitura é interpretada. O nome é
`<script>-<AAAA-MM-DD>[-<HHMM>].txt` para um probe e `<suíte>-suite-<AAAA-MM-DD>-<HHMM>.json` para
uma suíte, com a hora em UTC. As execuções reprovadas da suíte Redshift de 2026-09-21 (10:50,
11:28, 12:08 e 12:10 UTC) não estão aqui: cada leitura do ambiente que elas trouxeram se repete nas
duas execuções limpas, e os defeitos da suíte que elas expuseram estão descritos em
[`../POC.md`](../POC.md). A leitura da base de desenvolvimento de 2026-09-20 (`probes/parquet_source.py`
sobre `.../databases/dsv/db_projetado`, no espaço) também não está aqui: o relatório foi colado na
conversa, e [`../POC.md`](../POC.md), `tests/source_db_projetado.py` e
`tests/test_source_db_projetado.py` o transcrevem.

| Arquivo | Ambiente e hora | O que mostrou |
| --- | --- | --- |
| [`redshift-2026-09-20-2037.txt`](redshift-2026-09-20-2037.txt) | Ambiente alvo, 20:37 UTC | A primeira leitura do Redshift: workgroup `controladoria-wg`, endpoints VPC para as três APIs, credencial temporária, `sbx_aco_decon` no banco de datashare, nenhum papel IAM no namespace, `CREATE` negado no banco da conexão. Expôs quatro defeitos do probe, corrigidos no mesmo dia. |
| [`redshift-2026-09-20-2043.txt`](redshift-2026-09-20-2043.txt) | Ambiente alvo, 20:43 UTC, com `SERIALIZE_DB_TEST_S3_ROOT` | A repetição com a raiz S3, transcrita a partir da seção 1. `sys_load_error_detail` respondeu `0` em 1,5 s e `svv_external_schemas` também: o tempo limite da leitura anterior era o defeito do probe, não visão negada. O resto repetiu. |
| [`redshift-suite-2026-09-21-1335.json`](redshift-suite-2026-09-21-1335.json) | Ambiente alvo, 13:35 UTC, `pytest -m redshift` com `max_prepared_statements=0` | Os doze testes passaram, a primeira execução limpa: o comando repetido depois do `TRUNCATE` passou sem o cache e recebeu `34510` com ele, também na repetição, e passou depois de um `ALTER` e numa tabela temporária; `FILLRECORD` carregou 100 linhas com a coluna nova nula; `TRUNCATECOLUMNS` recusado com Parquet (`0A000`); `SERIALIZETOJSON` recusou a string de 80.901 bytes e `FORMAT JSON 'auto'` carregou o objeto; 5 linhas na fila do cursor antes do primeiro `fetchmany`. |
| [`redshift-suite-2026-09-21-1339.json`](redshift-suite-2026-09-21-1339.json) | Ambiente alvo, 13:39 UTC, a repetição | O mesmo resultado, leitura a leitura: a segunda execução limpa que a etapa 0 exigia. |
| [`parquet_source-2026-09-21-1354.txt`](parquet_source-2026-09-21-1354.txt) | Ambiente alvo, 13:54 UTC, `parquet_source.py --sample 5000` sobre a base de produção `s3://.../shared/bndes_grupos_bases_analise_financeira/databases/prd/db_projetado` | A base de produção lida pelo S3: 14 tabelas, 205 arquivos, 3.771.538.655 bytes, 187.340.531 linhas, `schema.json` na raiz, nenhuma checagem reprovada; a seção 3 idêntica à da base de desenvolvimento de 2026-09-20, as mesmas partições, o mesmo layout físico e as mesmas sete colunas sem estatística; 113 linhas a menos (`cad_contas`, `rel_contas_hierarquias`, `cad_lancamentos`), ids máximos menores, o extremo de `valor` com cinco casas, e a chave `pandas` em parte dos arquivos (5 de 8, 111 de 144, 9 de 13 e 16 de 30; nenhuma em `alembic_version` e `meta_update_status`). |
