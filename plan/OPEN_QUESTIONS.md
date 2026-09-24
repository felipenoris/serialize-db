# Questões em aberto

Este documento registra o que ainda não tem resposta: as pendências do projeto, as decisões que
esperam o usuário e as perguntas que uma leitura ou um teste vai fechar. Um item sai daqui quando a
resposta entra no documento que a guarda, e a saída nomeia esse documento. O plano está em
[`PLAN.md`](PLAN.md), o estado da implementação em [`CURRENT_STATE.md`](CURRENT_STATE.md) e o que já
foi medido em [`POC.md`](POC.md).

- **Versões não correntes.** O bucket é versionado e o papel não lê o ciclo de vida: cada exclusão
  (o `vacuum`, a limpeza da suíte S3) deixa uma versão não corrente invisível à listagem. `BK-14`
  conta o acumulado (219 versões não correntes, 1.388.530 bytes, e 219 marcadores de exclusão
  sob a raiz dos probes em 2026-09-23, e 366 versões, 16.345.479 bytes, com 358 marcadores sob a
  raiz nova em 2026-09-24, depois das três sessões de 2026-09-23, [`POC.md`](POC.md)), e a regra
  `NoncurrentVersionExpiration` sob a raiz, junto com `AbortIncompleteMultipartUpload`, é pergunta
  para quem administra o bucket. Sem ela, o `vacuum` da retenção de 400 dias não libera espaço;
  `docs/index.md`, seção "Retenção dos arquivos removidos", traz a regra de exemplo e como mudar a
  retenção.
- **Credenciais de uma hora.** Nenhuma execução mais longa que uma emissão rodou ainda; a
  [etapa 3](PLAN-STAGE-3.md) resolve `storage_options` a cada chamada e não põe credencial nele
  (decisão do usuário de 2026-09-22), e a primeira execução longa no espaço confirma que o delta-rs
  renova pela cadeia padrão o `DeltaTable` que a execução segura. O secret `credential_chain` do
  DuckDB, de `storage.duckdb_setup` e do script de migração, guarda a chave e o token resolvidos no
  `CREATE SECRET`, e a documentação da extensão `aws` pede `REFRESH auto` para a credencial que
  expira ([`POC.md`](POC.md), sonda de 2026-09-24): a proposta, à espera do usuário, é criar o
  secret com `REFRESH auto`. A conexão da carga de uma tabela do script vive do fim da medição ao
  relatório da carga, e a de `cad_lancamentos`, a mais longa da bateria, pode atravessar a rotação
  da credencial de quem chama, que às 22:50 de 2026-09-23 expirava em 46 minutos (`RS-18`). O
  `S3FileSystem` do `Storage`, que o `stream` e o `loader` do motor Redshift e a leitura dos
  rodapés usam, guarda a cadeia de credenciais do SDK da AWS, que renova a credencial do contêiner
  por conta própria; nenhuma execução mediu essa renovação. A credencial do Redshift
  tem o mesmo teto (`GetCredentials`, 3600 segundos), e o serverless encerra a sessão ociosa há
  3.600 s e a transação inativa há 21.600 s ([`redshift.md`](redshift.md)): o que acontece com uma
  conexão aberta quando a senha expira, e se ela cai no meio de um `COPY`, ainda não foi medido; o
  motor da [etapa 5](PLAN-STAGE-5.md) reconecta uma vez por comando e perde só a tabela temporária
  que o pipeline tenha criado na sessão. As credenciais que o `COPY`
  e o `UNLOAD` levam no texto do comando expiram com as do espaço, e `RS-18` imprime quando; um
  `COPY` mais longo que isso também não foi medido.
- **O arquivo do `UNLOAD` com uma coluna `SUPER` numa tabela Delta.** As execuções de 2026-09-23
  leram o `SUPER` no Parquet do `UNLOAD` como `extension<arrow.json>`, com o texto de cada valor,
  que o `cast` do contrato converte em `string` ([etapa 5](PLAN-STAGE-5.md)). Nenhum caso da suíte
  registra esse arquivo numa tabela Delta e o lê pelo delta-rs e pelo `delta_scan`, o caminho do
  `export_partition` de uma tabela com coluna JSON. Um arquivo do `COPY` do DuckDB com o mesmo tipo
  lógico `JSON`, registrado pelo motor DuckDB, foi lido como texto pelos dois leitores (sonda de
  2026-09-24, [`POC.md`](POC.md)); falta o arquivo do `UNLOAD`.
- **O `COPY` do Redshift sobre o arquivo do `COPY` do DuckDB.** A publicação da
  [etapa 8](PLAN-STAGE-8.md) lê os arquivos do registro, gravados pelo `COPY` do DuckDB desde a
  decisão de 2026-09-24: `DECIMAL` até 18 dígitos em `INT64`, `TIMESTAMP` em `INT64` de
  microssegundos, `DATE` em `INT32` e o campo JSON em `BYTE_ARRAY` com o tipo lógico `JSON`, com
  `PLAIN` e SNAPPY (sonda de 2026-09-24, [`POC.md`](POC.md)). O `COPY ... MANIFEST` do ambiente
  alvo carregou em 2026-09-21 arquivos do delta-rs, com o `DECIMAL(18, 2)` e o `timestamp_ntz` em
  `INT64`; um arquivo do DuckDB, e a coluna JSON com o tipo lógico numa staging `VARCHAR(65535)`,
  esperam os testes `redshift` da etapa 8 sobre arquivos exportados pelo motor DuckDB.
- **A memória do `archive` e da compactação.** O `deep_copy` da [etapa 3](PLAN-STAGE-3.md)
  reescreve a tabela inteira pelo `write_deltalake`, cuja memória cresce com a tabela fora do
  `memory_limit` do DuckDB (1.140 MB para 12.000.000 de linhas e 1.960 MB para 24.000.000,
  [`delta.md`](delta.md)); `cad_lancamentos` tem 141.901.795 linhas. A proposta da
  [etapa 9](PLAN-STAGE-9.md), à espera do usuário, é o `archive` partição a partição pelo `COPY`
  do DuckDB sob `environment_limits` e `register_files`, o caminho que `delta.md` mediu com memória
  constante. A compactação pelo `optimize.compact` do delta-rs também roda fora do `memory_limit`,
  e a memória dela numa partição de `cad_lancamentos` não foi medida.

## Decisões de API pendentes por etapa

Cada arquivo de etapa fecha com a seção "Decisões pendentes"; a lista abaixo as reúne, e uma decisão
tomada sai daqui e do arquivo da etapa no mesmo commit.

- [Etapa 9](PLAN-STAGE-9.md): o `archive` partição a partição pelo caminho do registro, no lugar
  do `deep_copy` da tabela inteira pelo `write_deltalake`.
