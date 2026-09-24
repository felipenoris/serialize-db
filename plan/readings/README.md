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
| [`s3-suite-2026-09-24-0143.json`](s3-suite-2026-09-24-0143.json) | Ambiente alvo, 01:43 UTC, `pytest -m "not redshift"` com as raízes local e S3, a partir da `main` com o #69, numa máquina de 16 vCPUs e 31.159 MB | 454 casos aprovados, entre eles as duas medições de memória de `test_duckdb.py` pelo `VmHWM` (a tabela inteira 244 MB acima da base, o leitor em lotes 11 MB, o arquivo de transbordo 24 MB), o cache de arquivos externos com as mesmas 5 entradas depois da primeira e da segunda leitura, quatro sessões juntas em 0,094 s contra 0,025 s de uma com 16 threads e o pipeline de três estágios em 1,276 s por tabela, 1,710 s em série e 0,949 s encadeado. |
| [`redshift-suite-2026-09-24-0146.json`](redshift-suite-2026-09-24-0146.json) | Ambiente alvo, 01:46 UTC, `pytest -m redshift` | 30 casos aprovados: `naofinito_valor` 2 dos 2 pela contagem dos não nulos menos os finitos, cada medida igual ao esperado, e a linha do `NaN` com as quatro comparações falsas e o texto `NaN`; o resto como em 2026-09-23. |
| [`redshift-suite-2026-09-24-0149.json`](redshift-suite-2026-09-24-0149.json) | Ambiente alvo, 01:49 UTC, a repetição | O mesmo resultado, leitura a leitura. |
| [`duckdb_threads-2026-09-24-0202.txt`](duckdb_threads-2026-09-24-0202.txt) | Ambiente alvo, 02:02 UTC, `duckdb_threads.py` sobre a raiz Delta da migração de 01:53, com o cache de arquivos externos desligado, numa máquina de 16 vCPUs de 8 núcleos físicos | A partição 2026-06-30 de `cad_lancamentos` (542 MB, 32.218.190 linhas) materializada em 7,0 s com 16 threads, 7,8 s com 8 e 11,1 s a 17,1 s com 32 a 80; a leitura agregada em 2,03 s com 8, 1,19 s com 16 e 0,84 s a 0,89 s com 48 a 80; as quatro tabelas em 17,5 s em série e 9,3 s em sessões a mais com 16 threads. |
| [`space-2026-09-24-0141.txt`](space-2026-09-24-0141.txt) | Ambiente alvo, 01:41 UTC | 16 vCPUs, 30,4 GiB e 29,7 GiB livres; o DuckDB com 16 threads e `memory_limit` de 24,3 GiB; a `.venv` com o grupo `dev`; sem proxy e sem internet. |
| [`redshift-2026-09-24-0141.txt`](redshift-2026-09-24-0141.txt) | Ambiente alvo, 01:41 UTC | A seção da sessão inteira: `sys_load_error_detail` com 25 erros de carga em 30 dias (`RS-12`), nenhum esquema externo (`RS-13`), `RS-19` pelo `USE`, `svv_table_info` negada ao papel depois do `USE` (42501), `has_schema_privilege` falso para `USAGE` e `CREATE`, e a credencial de quem chama expirando em 49 minutos (`RS-18`); nenhuma chamada falhou. |
| [`bucket-2026-09-24-0142.txt`](bucket-2026-09-24-0142.txt) | Ambiente alvo, 01:42 UTC, raiz `.../shared/<usuário>/serialize-db/serialize-db-tests` | SSE-KMS com bucket key; 366 versões não correntes (16.345.479 bytes) e 358 marcadores de exclusão, o rastro das sessões de 2026-09-23; o ciclo de vida, a política, os uploads multipart, o versionamento e o Object Lock negados. |
| [`diagnose_aws-2026-09-23-1918.txt`](diagnose_aws-2026-09-23-1918.txt) | Ambiente alvo, 19:18 UTC | O boto3, o delta-rs e o DuckDB listaram o prefixo; a região e o STS sem manutenção. |
| [`catalog-2026-09-23-1918.txt`](catalog-2026-09-23-1918.txt) | Ambiente alvo, 19:18 UTC | O Glue com um banco e uma tabela Parquet, o Athena com três workgroups, o Lake Formation e o S3 Tables sem rota (60,6 s e 30,2 s). |
| [`parquet_source-2026-09-21-1354.txt`](parquet_source-2026-09-21-1354.txt) | Ambiente alvo, 13:54 UTC, `parquet_source.py --sample 5000` sobre a cópia da base de produção no sandbox, `s3://.../shared/bndes_grupos_bases_analise_financeira/databases/prd/db_projetado` | A cópia da base de produção lida pelo S3: 14 tabelas, 205 arquivos, 3.771.538.655 bytes, 187.340.531 linhas, `schema.json` na raiz, nenhuma checagem reprovada; a seção 3 idêntica à da base de desenvolvimento de 2026-09-20, as mesmas partições, o mesmo layout físico e as mesmas sete colunas sem estatística; 113 linhas a menos (`cad_contas`, `rel_contas_hierarquias`, `cad_lancamentos`), ids máximos menores, o extremo de `valor` com cinco casas, e a chave `pandas` em parte dos arquivos (5 de 8, 111 de 144, 9 de 13 e 16 de 30; nenhuma em `alembic_version` e `meta_update_status`). A carga inicial da [etapa 7](../PLAN-STAGE-7.md) e a base fictícia de `tests/source_db_projetado.py` a têm como referência. |
