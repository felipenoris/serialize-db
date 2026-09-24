# Questões em aberto

Este documento registra o que ainda não tem resposta: as pendências do projeto, as decisões que
esperam o usuário e as perguntas que uma leitura ou um teste vai fechar. Um item sai daqui quando a
resposta entra no documento que a guarda, e a saída nomeia esse documento. O plano está em
[`PLAN.md`](PLAN.md), o estado da implementação em [`CURRENT_STATE.md`](CURRENT_STATE.md) e o que já
foi medido em [`POC.md`](POC.md).

- **Versões não correntes.** O bucket é versionado e o papel não lê o ciclo de vida: cada exclusão
  (o `vacuum`, a limpeza da suíte S3) deixa uma versão não corrente invisível à listagem. `BK-14`
  conta o acumulado (219 versões não correntes, 1.388.530 bytes, e 219 marcadores de exclusão
  sob a raiz dos probes em 2026-09-23; 366 versões, 16.345.479 bytes, com 358 marcadores sob a
  raiz nova em 2026-09-24 às 01:42, depois das três sessões de 2026-09-23; e 907 versões,
  32.966.477 bytes, com 859 marcadores às 12:39 do mesmo dia, [`POC.md`](POC.md)), e a regra
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
- **O `PARALLEL OFF` e a reconexão do motor Redshift.** As suítes do motor e da publicação rodaram
  no ambiente alvo em 2026-09-24, duas vezes cada, e leram o que esperavam ([`POC.md`](POC.md)):
  ficam sem medida o `PARALLEL OFF` até 5.000.000 linhas na exportação e a reconexão depois de uma
  queda do servidor, que nenhum teste provoca lá ([etapa 5](PLAN-STAGE-5.md)).
- **O `compact` e as colunas `Double` não finitas.** Numa sonda de 2026-09-24 na pasta local,
  `serialize_db.delta.compact` juntou dois arquivos de uma partição, um deles com `NaN` em `valor`
  e sem mínimo e máximo no log, num arquivo com `min.valor` 1.0 e `max.valor` 3.0
  ([`POC.md`](POC.md)): o `optimize.compact` do delta-rs grava estatística em toda coluna, e a
  decisão da issue #59 (sem mínimo e máximo na coluna `Double` com valor não finito) não vale na
  partição compactada. Espera o usuário: passar ao `compact` as propriedades de escrita sem
  estatística nessas colunas, recusar a compactação da partição com valor não finito, ou aceitar
  ([etapa 9](PLAN-STAGE-9.md)).
- **A contagem na troca do motor Redshift.** Com `columns_without_min_max`,
  `RedshiftEngine.export_partition` leva a partição de volta por `publish_partition`, que não
  confere `expected_rows`, e a partição sai sem a conferência de contagem que o registro faz; com
  `Execution.publish(audit=False)` toda coluna `Double` entra na lista, e a tabela com `Double`
  segue esse caminho ([etapa 5](PLAN-STAGE-5.md)). Lido no código em 2026-09-24; espera o
  usuário.
- **A versão da staging `_publicado` do motor Redshift.** `RedshiftEngine.published` carrega a
  staging uma vez por execução pelo nome, sem a versão: chamada de novo com outra versão, depois de
  `Execution.publish` avançar `versions`, devolve a staging da primeira, enquanto o motor DuckDB lê
  a versão nova. Lido no código em 2026-09-24, sem sonda; espera o usuário.
- **`check_models` e as colunas de partição.** Na tabela com mais de uma coluna em `partition_by`,
  `check_models` levanta o `ContractError` de `table_options` em vez de listar a violação (sonda de
  2026-09-24); a docstring registra o comportamento, e a correção espera o usuário.
- **`SERIALIZE_DB_ENVIRONMENT` vazia na linha de comando.** `load` e as rotinas de operação leem
  `os.environ.get("SERIALIZE_DB_ENVIRONMENT", "dev")`, e a variável vazia vira erro de uso;
  `run`, `audit` e `publish` usam `dev` (sonda de 2026-09-24). Alinhar os dois espera o usuário.
- **A memória da compactação.** O `optimize.compact` do delta-rs roda fora do `memory_limit` do
  DuckDB, com as tarefas paralelas do padrão do delta-rs, e a memória dele numa partição de
  `cad_lancamentos` não foi medida ([etapa 9](PLAN-STAGE-9.md)); o `archive` saiu desse risco pela
  cópia dos arquivos de cada partição e o registro deles (decisão do usuário de 2026-09-24).

- **A operação no ambiente alvo.** Em 2026-09-24 às 16:51, sobre a raiz recarregada, a carga, a
  auditoria, `history`, `snapshot`, `vacuum`, `archive` (os 21 arquivos das 12 tabelas pela
  transferência gerenciada do `boto3`) e a publicação da base inteira no Redshift rodaram sem erro
  ([`POC.md`](POC.md)). `export` e `compact` (a memória da compactação, o item acima) ainda não
  rodaram lá, e a duração do `archive` e a da publicação de `cad_lancamentos` ficaram sem
  leitura, porque a linha de comando não imprimia tempo: desde a decisão do usuário do mesmo dia,
  `compact`, `archive` e `export` imprimem por tabela o tempo e o pico de RSS do processo, a
  publicação os põe no log de cada tabela e `deep_copy` registra o tempo de cada partição, e a
  próxima execução lá os lê; a continuação de uma cópia interrompida só o substituto exercitou.

## Decisões de API pendentes por etapa

Cada arquivo de etapa fecha com a seção "Decisões pendentes"; a lista abaixo as reúne, e uma decisão
tomada sai daqui e do arquivo da etapa no mesmo commit. Nenhuma etapa tem decisão pendente.
