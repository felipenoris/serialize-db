# `plan/readings/`: os relatórios dos probes que viraram registro

Cada arquivo aqui é a saída de uma execução de `probes/<script>.py` num ambiente real, ou o JSON
de `SERIALIZE_DB_TEST_REPORT` de uma sessão de suíte. A pasta `probes/output/` fica fora do git e
recebe toda execução; esta guarda as que uma etapa pendente ainda consulta, e
[`../POC.md`](../POC.md) é onde a leitura é interpretada. Um relatório sai daqui quando `POC.md` e o
arquivo da etapa já guardam o que ele mostrou (decisão do usuário de 2026-09-23), e o histórico do
git o guarda. O nome é `<script>-<AAAA-MM-DD>[-<HHMM>].txt` para um probe e
`<suíte>-suite-<AAAA-MM-DD>-<HHMM>.json` para uma suíte, com a hora em UTC. A leitura da base de
desenvolvimento de 2026-09-20 (`probes/parquet_source.py` sobre `.../databases/dsv/db_projetado`,
no espaço) não está aqui: o relatório foi colado na conversa, e [`../POC.md`](../POC.md),
`tests/source_db_projetado.py` e `tests/test_source_db_projetado.py` o transcrevem.

Os relatórios estão como saíram, com os identificadores sensíveis do ambiente mascarados (decisão
do usuário de 2026-09-23): as contas (`<conta>`, `<conta do produtor>` do datashare), o usuário do
banco e a pasta pessoal (`<usuário>`), o id do papel (`<id do papel>`) e o sufixo do papel SSO, os
ids do DataZone (`dzd-<domínio>`, `<projeto>`, `<ambiente>`) e o do banco do Glue
(`glue_db_<id>`), a chave KMS (`<chave>`), o namespace do produtor (`<namespace>`) e os endereços
privados (`10.x.x.x`). O byte nulo que o Redshift devolve no fim da versão aparece como `\0`.

| Arquivo | Ambiente e hora | O que mostrou |
| --- | --- | --- |
| [`s3-suite-2026-09-23-1848.json`](s3-suite-2026-09-23-1848.json) | Ambiente alvo, 18:48 UTC, `pytest -m "not redshift"` com as raízes local e S3, a partir da `main` | 430 casos aprovados e as duas medições de memória de `test_duckdb.py` reprovadas com 1.120 MB em todo cenário: o `ru_maxrss` do subprocesso herdava o pico do pytest. Os casos `s3` do pacote e a suíte S3 passaram no bucket com SSE-KMS, e a cadeia de credenciais do delta-rs respondeu nas cinco variantes. |
| [`redshift-suite-2026-09-23-1852.json`](redshift-suite-2026-09-23-1852.json) | Ambiente alvo, 18:52 UTC, `pytest -m redshift` | 24 casos aprovados e `test_stream_by_unload_with_literal_values` reprovado na contrabarra: o `UNLOAD` passou sem gravar nada. As leituras da etapa 5 (o prefixo com `=`, o `LIMIT` recusado, o resultado vazio sem manifesto nem arquivo, a tabela temporária, o `SUPER`, os OIDs e o `type_modifier`), o rodapé do `UNLOAD` sem o `NaN` no máximo, o texto da auditoria e as transações simultâneas. |
| [`redshift-suite-2026-09-23-1855.json`](redshift-suite-2026-09-23-1855.json) | Ambiente alvo, 18:55 UTC, a repetição | O mesmo resultado, leitura a leitura. |
| [`space-2026-09-23-1918.txt`](space-2026-09-23-1918.txt) | Ambiente alvo, 19:18 UTC | 4 vCPUs, 15,4 GiB e 29,7 GiB livres; o DuckDB com 4 threads e `memory_limit` de 12,3 GiB; a `.venv` com o grupo `dev`; sem proxy e sem internet. |
| [`redshift-2026-09-23-1918.txt`](redshift-2026-09-23-1918.txt) | Ambiente alvo, 19:18 UTC | `RS-19` pelo critério novo; `RS-8`: `svv_table_info` negada ao papel depois do `USE` (42501); o teste de alcance do IAM dispensou a simulação. |
| [`bucket-2026-09-23-1918.txt`](bucket-2026-09-23-1918.txt) | Ambiente alvo, 19:18 UTC, raiz `.../shared/serialize-db-tests` | SSE-KMS com bucket key; 219 versões não correntes e 219 marcadores de exclusão; o teste de alcance do IAM e do KMS dispensou as chamadas. |
| [`diagnose_aws-2026-09-23-1918.txt`](diagnose_aws-2026-09-23-1918.txt) | Ambiente alvo, 19:18 UTC | O boto3, o delta-rs e o DuckDB listaram o prefixo; a região e o STS sem manutenção. |
| [`catalog-2026-09-23-1918.txt`](catalog-2026-09-23-1918.txt) | Ambiente alvo, 19:18 UTC | O Glue com um banco e uma tabela Parquet, o Athena com três workgroups, o Lake Formation e o S3 Tables sem rota (60,6 s e 30,2 s). |
| [`parquet_source-2026-09-21-1354.txt`](parquet_source-2026-09-21-1354.txt) | Ambiente alvo, 13:54 UTC, `parquet_source.py --sample 5000` sobre a cópia da base de produção no sandbox, `s3://.../shared/bndes_grupos_bases_analise_financeira/databases/prd/db_projetado` | A cópia da base de produção lida pelo S3: 14 tabelas, 205 arquivos, 3.771.538.655 bytes, 187.340.531 linhas, `schema.json` na raiz, nenhuma checagem reprovada; a seção 3 idêntica à da base de desenvolvimento de 2026-09-20, as mesmas partições, o mesmo layout físico e as mesmas sete colunas sem estatística; 113 linhas a menos (`cad_contas`, `rel_contas_hierarquias`, `cad_lancamentos`), ids máximos menores, o extremo de `valor` com cinco casas, e a chave `pandas` em parte dos arquivos (5 de 8, 111 de 144, 9 de 13 e 16 de 30; nenhuma em `alembic_version` e `meta_update_status`). A carga inicial da [etapa 7](../PLAN-STAGE-7.md) e a base fictícia de `tests/source_db_projetado.py` a têm como referência. |
