# `plan/readings/`: os relatórios dos probes que viraram registro

Cada arquivo aqui é a saída de uma execução de `probes/<script>.py` num ambiente real, ou o JSON
de `SERIALIZE_DB_TEST_REPORT` de uma sessão de suíte, guardados como saíram. A pasta `probes/output/`
fica fora do git e recebe toda execução; esta guarda as que uma etapa pendente ainda consulta, e
[`../POC.md`](../POC.md) é onde a leitura é interpretada. Um relatório sai daqui quando `POC.md` e o
arquivo da etapa já guardam o que ele mostrou (decisão do usuário de 2026-09-23), e o histórico do
git o guarda. O nome é `<script>-<AAAA-MM-DD>[-<HHMM>].txt` para um probe e
`<suíte>-suite-<AAAA-MM-DD>-<HHMM>.json` para uma suíte, com a hora em UTC. A leitura da base de
desenvolvimento de 2026-09-20 (`probes/parquet_source.py` sobre `.../databases/dsv/db_projetado`,
no espaço) não está aqui: o relatório foi colado na conversa, e [`../POC.md`](../POC.md),
`tests/source_db_projetado.py` e `tests/test_source_db_projetado.py` o transcrevem.

| Arquivo | Ambiente e hora | O que mostrou |
| --- | --- | --- |
| [`parquet_source-2026-09-21-1354.txt`](parquet_source-2026-09-21-1354.txt) | Ambiente alvo, 13:54 UTC, `parquet_source.py --sample 5000` sobre a base de produção `s3://.../shared/bndes_grupos_bases_analise_financeira/databases/prd/db_projetado` | A base de produção lida pelo S3: 14 tabelas, 205 arquivos, 3.771.538.655 bytes, 187.340.531 linhas, `schema.json` na raiz, nenhuma checagem reprovada; a seção 3 idêntica à da base de desenvolvimento de 2026-09-20, as mesmas partições, o mesmo layout físico e as mesmas sete colunas sem estatística; 113 linhas a menos (`cad_contas`, `rel_contas_hierarquias`, `cad_lancamentos`), ids máximos menores, o extremo de `valor` com cinco casas, e a chave `pandas` em parte dos arquivos (5 de 8, 111 de 144, 9 de 13 e 16 de 30; nenhuma em `alembic_version` e `meta_update_status`). A carga inicial da [etapa 7](../PLAN-STAGE-7.md) e a base fictícia de `tests/source_db_projetado.py` a têm como referência. |
