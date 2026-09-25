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
  32.966.477 bytes, com 859 marcadores às 12:39 do mesmo dia; e 1.943 versões, 50.394.018 bytes,
  com 1.823 marcadores às 23:26, [`POC.md`](POC.md)), e a regra
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
  `COPY` mais longo que isso também não foi medido. `probes/credentials.py` segura esses
  clientes, menos o `COPY`, até passar a expiração da credencial do contêiner e a da senha do
  Redshift, em cerca de uma hora, e lê cada um a cada cinco minutos. No substituto de
  2026-09-25, com chaves de 70 s, o `delta_scan` falhou com a chave vencida do secret, e só o
  `read_parquet` a renovou, numa renovação que a consulta seguinte desfaz quando o resultado
  fica aberto ([`POC.md`](POC.md)); se o alvo repetir isso, a biblioteca precisa renovar o
  secret do DuckDB por conta própria, e a forma de renovar espera o usuário.
- **O `PARALLEL OFF` e a reconexão do motor Redshift.** As suítes do motor e da publicação rodaram
  no ambiente alvo em 2026-09-24, duas vezes cada, e leram o que esperavam ([`POC.md`](POC.md)):
  ficam sem medida o `PARALLEL OFF` até 5.000.000 linhas na exportação e a reconexão depois de uma
  queda do servidor, que nenhum teste provoca lá ([etapa 5](PLAN-STAGE-5.md)).
- **A versão retirada do `deltalake`.** O `uv` avisou em 2026-09-25, ao instalar numa sonda a
  versão fixada em `pyproject.toml`, que `deltalake==1.6.4` está retirada (yanked) do PyPI, com o
  motivo "Issue: #4784", e a instalou assim mesmo ([`POC.md`](POC.md)). A issue #4784 do delta-rs,
  lida em 2026-09-25, é um `MERGE` numa tabela com o change data feed ligado que insere uma linha
  toda nula para cada linha que o predicado de `when_not_matched_insert` recusa, e afeta a 1.6.4
  e a 1.6.5; o pacote não usa `MERGE` nem o change data feed (busca por `.merge(`,
  `enableChangeDataFeed`, `change_data_feed` e `load_cdf` em `src/`, `scripts/`, `tests/` e
  `probes/`). A versão que corrige não foi lida, e a troca da versão fixada espera o usuário.
- **A memória da compactação.** O `optimize.compact` do delta-rs roda fora do `memory_limit` do
  DuckDB, com as tarefas paralelas do padrão do delta-rs, e a memória dele numa partição de
  `cad_lancamentos` não foi medida ([etapa 9](PLAN-STAGE-9.md)); o `archive` saiu desse risco pela
  cópia dos arquivos de cada partição e o registro deles (decisão do usuário de 2026-09-24).
- **O filtro do dataset do delta-rs nas colunas sem mínimo e máximo.** O
  `DeltaTable.to_pyarrow_dataset()` do delta-rs, e com ele o `to_pyarrow_table` e o `to_pandas`
  com `filters`, perde as linhas de um filtro sobre uma coluna que o log deixa sem mínimo e máximo:
  o `filestats_to_expression_next` do delta-rs põe `coluna >= null` e `coluna <= null` na garantia
  de cada arquivo, e o PyArrow pula o arquivo. Na sonda de 2026-09-25 ([`POC.md`](POC.md)), um
  arquivo registrado pelo motor DuckDB leu 0 linhas em `valor > 1` (`Numeric(18, 2)`),
  `quando > '2025-12-31'` (`DateTime`) e `legado = true` (`Boolean`), contra 2, 2 e 1 pelo
  `delta_scan`. Ficam sem os dois no log os tipos que o registro omite ([etapa 3](PLAN-STAGE-3.md)),
  entre eles as colunas `DateTime` e `Boolean` do modelo cliente; o texto dos arquivos do
  `UNLOAD`; e as `Double` com valor não finito da issue #59, na troca do motor Redshift e no
  `compact` da [etapa 9](PLAN-STAGE-9.md). O `delta_scan` dos motores e do leitor Delta e o `COPY`
  do Redshift leem certo, e o pacote filtra o dataset do delta-rs só pela coluna da partição
  (`read_back`), fora da perda. Com `delta.dataSkippingStatsColumns` sem as três colunas, a mesma
  sonda leu 2, 2 e 1; a propriedade também limita as estatísticas que o `write_deltalake` grava.
  A issue #3032 do delta-rs, aberta em 2024-11-25 e fechada com o rótulo `mre-needed`, relata o
  mesmo sintoma num filtro fora da partição, sem a causa. Espera o usuário: pôr essa propriedade
  nas tabelas, gravar no log mínimo e máximo que não percam linha nesses tipos, documentar o
  `delta_scan` como o leitor das tabelas, ou levar o defeito ao delta-rs.

- **A operação no ambiente alvo.** Em 2026-09-24, nas baterias das 16:51 e das 23:25, a carga, a
  auditoria, `history`, `snapshot`, `vacuum`, `archive`, a publicação da base inteira e `export`
  rodaram sem erro, com o tempo e o pico de RSS de `archive`, `export` e da publicação lidos às
  23:25 ([`POC.md`](POC.md)). O `compact` rodou só sobre a partição 2026-03-31 de
  `cad_lancamentos`, que tem um arquivo só e não commita: a compactação de uma partição de vários
  arquivos e a memória dela (o item acima) esperam uma partição com mais de um arquivo; a
  continuação de uma cópia interrompida do `archive` só o substituto exercitou.

- **O acesso de leitura no ambiente alvo.** A [etapa 10](PLAN-STAGE-10.md), implementada em
  2026-09-25 na pasta local e no substituto, espera as leituras que só o alvo dá, com os comandos
  em `SUITE.md`: o tempo de abertura do leitor Delta sobre as 12 tabelas da raiz carregada, com
  uma view por tabela (8,7 ms por view na pasta local, [`POC.md`](POC.md)); a publicação por
  `--channel default` e a volta a um snapshot anterior ao publicado, com o tempo e o pico de RSS
  por tabela; e o `UNLOAD` de um cliente com usuário só de leitura para um bucket próprio, com o
  caminho de credencial que serve a ele, que precisa de um papel de cliente no alvo.

## Decisões de API pendentes por etapa

Cada arquivo de etapa fecha com a seção "Decisões pendentes"; a lista abaixo as reúne, e uma decisão
tomada sai daqui e do arquivo da etapa no mesmo commit. Nenhuma etapa tem decisão pendente; os
itens que esperam o usuário fora dos arquivos de etapa estão na lista acima.
