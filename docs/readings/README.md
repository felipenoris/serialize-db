# `docs/readings/`: os relatórios dos probes que viraram registro

Cada arquivo aqui é a saída de uma execução de `probes/<script>.py` num ambiente real, ou o JSON
de `SERIALIZE_DB_TEST_REPORT` de uma sessão de suíte, guardados como saíram. A pasta `probes/output/`
fica fora do git e recebe toda execução; esta guarda as que sustentam uma afirmação de
[`../POC.md`](../POC.md), que é onde a leitura é interpretada. O nome é
`<script>-<AAAA-MM-DD>[-<HHMM>].txt` para um probe e `<suíte>-suite-<AAAA-MM-DD>-<HHMM>.json` para
uma suíte, com a hora em UTC.

| Arquivo | Ambiente e hora | O que mostrou |
| --- | --- | --- |
| [`redshift-2026-09-20-2037.txt`](redshift-2026-09-20-2037.txt) | Ambiente alvo, 20:37 UTC | A primeira leitura do Redshift: workgroup `controladoria-wg`, endpoints VPC para as três APIs, credencial temporária, `sbx_aco_decon` no banco de datashare, nenhum papel IAM no namespace, `CREATE` negado no banco da conexão. Expôs quatro defeitos do probe, corrigidos no mesmo dia. |
| [`redshift-2026-09-20-2043.txt`](redshift-2026-09-20-2043.txt) | Ambiente alvo, 20:43 UTC, com `SERIALIZE_DB_TEST_S3_ROOT` | A repetição com a raiz S3, transcrita a partir da seção 1. `sys_load_error_detail` respondeu `0` em 1,5 s e `svv_external_schemas` também: o tempo limite da leitura anterior era o defeito do probe, não visão negada. O resto repetiu. |
| [`redshift-suite-2026-09-21-1050.json`](redshift-suite-2026-09-21-1050.json) | Ambiente alvo, 10:50 UTC, `pytest -m redshift` | A primeira execução da suíte Redshift: um teste passou e dez reprovaram, com os onze `DROP` da limpeza em `25P02`, porque a sessão correu numa transação aberta antes do `USE` e abortada pela visão `stv_slices` negada. O JSON ainda não levava a mensagem das falhas. |
