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
  expira ([`POC.md`](POC.md), sonda de 2026-09-24): os dois secrets levam `REFRESH auto` desde a
  decisão do usuário de 2026-09-24, e a renovação numa conexão que atravessa a rotação da
  credencial do contêiner, que às 22:50 de 2026-09-23 expirava em 46 minutos (`RS-18`), só uma
  execução longa no alvo mostra. O
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
  2026-09-24, [`POC.md`](POC.md)). A primeira bateria no alvo, de 2026-09-24 às 05:10, registrou
  pelo `export_partition` o arquivo do `UNLOAD` de `cad_lancamentos_projetados`, com a coluna
  serializada por `JSON_SERIALIZE`, e o delta-rs a leu como texto; o `delta_scan` só contou as
  linhas, e a repetição registra a coluna por ele (`redshift.engine.delta_scan_meta`).
- **O `COPY` do Redshift sobre o arquivo do `COPY` do DuckDB.** A publicação da
  [etapa 8](PLAN-STAGE-8.md) lê os arquivos do registro, gravados pelo `COPY` do DuckDB desde a
  decisão de 2026-09-24: `DECIMAL` até 18 dígitos em `INT64`, `TIMESTAMP` em `INT64` de
  microssegundos, `DATE` em `INT32` e o campo JSON em `BYTE_ARRAY` com o tipo lógico `JSON`, com
  `PLAIN` e SNAPPY (sonda de 2026-09-24, [`POC.md`](POC.md)). O `COPY ... MANIFEST` do ambiente
  alvo carregou em 2026-09-21 arquivos do delta-rs, com o `DECIMAL(18, 2)` e o `timestamp_ntz` em
  `INT64`; um arquivo do DuckDB, e a coluna JSON com o tipo lógico numa staging `VARCHAR(65535)`,
  esperam os testes `redshift` da etapa 8 sobre arquivos exportados pelo motor DuckDB, que a
  bateria de 2026-09-24 às 05:13 não alcançou (a pasta local ausente, [`POC.md`](POC.md)).
- **As etapas 5 e 8 no ambiente alvo.** O motor Redshift rodou lá pela primeira vez em 2026-09-24
  às 05:10: cinco dos seis casos de `tests/test_engine_redshift.py` passaram nas duas rodadas, e
  `current_database()` reprovou o sexto pelo tipo `name` (OID 19), corrigido no mapa de tipos
  ([`POC.md`](POC.md)). A repetição lê o SQLSTATE da relação inexistente, `42P01`, que
  `name_in_use` e a conferência da tabela de controle esperam ao lado da mensagem `does not exist`
  (`redshift.engine.relation_missing`: o nome livre passou sem dizer qual dos dois), e a coluna
  JSON do arquivo do `UNLOAD` pelo `delta_scan` (o item acima). A primeira execução de
  `tests/test_publication.py` com `-m redshift`, que às 05:13 errou na pasta local ausente, lê: o
  `COPY` do Redshift sobre os arquivos do `COPY` do DuckDB
  (`test_first_publication_loads_every_partition`, o item acima); a grafia de `svv_all_columns`
  na reconciliação (`test_reconcile_published_on_the_target`, que exige diff nenhum na tabela igual
  ao modelo); o `1023` da publicação simultânea pela publicação da biblioteca
  (`test_concurrent_publication_raises_execution_conflict`); e o `EXPLAIN` da junção entre tabelas
  publicadas (`test_published_join_redistribution_is_read`). Ficam sem medida o `PARALLEL OFF` até
  5.000.000 linhas na exportação e a reconexão depois de uma queda do servidor, que nenhum teste
  provoca lá. A suíte publica num ambiente `poc<id>` e cria a tabela de controle quando ela não
  existe, apagando-a só nesse caso.
- **A memória da compactação.** O `optimize.compact` do delta-rs roda fora do `memory_limit` do
  DuckDB, com as tarefas paralelas do padrão do delta-rs, e a memória dele numa partição de
  `cad_lancamentos` não foi medida ([etapa 9](PLAN-STAGE-9.md)); o `archive` saiu desse risco pela
  cópia dos arquivos de cada partição e o registro deles (decisão do usuário de 2026-09-24).

- **A carga inicial pelo pacote no ambiente alvo.** `serialize_db.load` e o script fino sobre ele
  rodaram só sobre a base fictícia, na pasta local ([`POC.md`](POC.md)); as execuções do script no
  alvo, em 2026-09-23 e 2026-09-24, foram da versão anterior, sobre o `deltalake` e o DuckDB diretos,
  numa raiz `<raiz>/<tabela>`. A primeira execução do script sobre a cópia da base de produção lê:
  a raiz `<raiz>/<ambiente>/<tabela>` de `Database`, com `--environment`; o custo das conferências
  do rodapé e da releitura de `register_files` na partição 2026-03-31 de `cad_lancamentos`
  (52.654.607 linhas), que o script anterior não fazia; a abertura de um motor DuckDB por partição;
  e os órfãos das bases reais por `serialize-db audit --foreign-keys` sobre o Delta carregado (o
  `data_base` 2026-01-31 sem `cad_contratos`, o contrato `desemb-999` sem cadastro).

- **A operação no ambiente alvo.** Os subcomandos da [etapa 9](PLAN-STAGE-9.md) rodaram só na
  pasta local ([`POC.md`](POC.md)). No bucket, o `archive` copia cada arquivo por `CopyObject`
  (`Storage.copy`) e confere a contagem da cópia pelos dois leitores, o `vacuum` lista e apaga
  versões não correntes que só a regra `NoncurrentVersionExpiration` libera (o item acima), e a
  compactação roda no escritor do delta-rs; a primeira execução das rotinas lá lê o tempo do
  `archive` de `cad_lancamentos` e a memória da compactação de uma partição dela.

## Decisões de API pendentes por etapa

Cada arquivo de etapa fecha com a seção "Decisões pendentes"; a lista abaixo as reúne, e uma decisão
tomada sai daqui e do arquivo da etapa no mesmo commit. Nenhuma etapa tem decisão pendente.
