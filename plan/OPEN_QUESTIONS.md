# Questões em aberto

Este documento registra o que ainda não tem resposta: as pendências do projeto, as decisões que
esperam o usuário e as perguntas que uma leitura ou um teste vai fechar. Um item sai daqui quando a
resposta entra no documento que a guarda, e a saída nomeia esse documento. O plano está em
[`PLAN.md`](PLAN.md), o estado da implementação em [`CURRENT_STATE.md`](CURRENT_STATE.md) e o que já
foi medido em [`POC.md`](POC.md).

- **Versões não correntes.** O bucket é versionado e o papel não lê o ciclo de vida: cada exclusão
  (o `vacuum`, a limpeza da suíte S3) deixa uma versão não corrente invisível à listagem. `BK-14`
  conta o acumulado (219 versões não correntes, 1.388.530 bytes, e 219 marcadores de exclusão
  sob a raiz dos probes em 2026-09-23, [`POC.md`](POC.md)), e a regra
  `NoncurrentVersionExpiration` sob a raiz, junto com `AbortIncompleteMultipartUpload`, é pergunta
  para quem administra o bucket. Sem ela, o `vacuum` da retenção de 400 dias não libera espaço;
  `docs/index.md`, seção "Retenção dos arquivos removidos", traz a regra de exemplo e como mudar a
  retenção.
- **Credenciais de uma hora.** Nenhuma execução mais longa que uma emissão rodou ainda; a
  [etapa 3](PLAN-STAGE-3.md) resolve `storage_options` a cada chamada e não põe credencial nele
  (decisão do usuário de 2026-09-22), e a primeira execução longa no espaço confirma que o delta-rs
  renova pela cadeia padrão o `DeltaTable` que a execução segura. A credencial do Redshift
  tem o mesmo teto (`GetCredentials`, 3600 segundos), e o serverless encerra a sessão ociosa há
  3.600 s e a transação inativa há 21.600 s ([`redshift.md`](redshift.md)): o que acontece com uma
  conexão aberta quando a senha expira, e se ela cai no meio de um `COPY`, ainda não foi medido; o
  motor da [etapa 5](PLAN-STAGE-5.md) reconecta uma vez por comando e perde só a tabela temporária
  que o pipeline tenha criado na sessão. As credenciais que o `COPY`
  e o `UNLOAD` levam no texto do comando expiram com as do espaço, e `RS-18` imprime quando; um
  `COPY` mais longo que isso também não foi medido.
- **A memória da partição de `cad_lancamentos`.** Cerca de 700 MB de Parquet e 35 milhões de linhas
  por partição; a primeira carga real mede o `write_deltalake` de um leitor e o `COPY ...
  RETURN_STATS` mais `register_files` antes de fixar o padrão ([etapa 7](PLAN-STAGE-7.md)); a
  migração adiantada (`scripts/migrate_parquet_to_delta.py`, logo depois da etapa 1) é essa carga. O
  script mede por padrão, antes da carga de cada tabela particionada, cada partição nas quatro
  variantes de gravação (`register` e `rewrite`, com e sem a ordem da `sort_key`), cada uma num
  processo novo, e a próxima execução dos comandos de todas as tabelas no ambiente alvo, com os
  mesmos parâmetros, traz a medição (decisão do usuário de 2026-09-23). `export_mode="rewrite"` e
  `"register"` medem os dois caminhos em cada motor e na carga inicial (etapas [4](PLAN-STAGE-4.md),
  [5](PLAN-STAGE-5.md) e [7](PLAN-STAGE-7.md)). O relatório da migração com essa medição é o gatilho
  de revisão de [`PLAN.md`](PLAN.md): ele decide o padrão da flag e se o outro modo sai, em cada
  motor e na carga inicial, e se a carga ordena pela `sort_key`.
- **O `threads` do DuckDB na leitura do S3.** O DuckDB lê arquivos remotos com E/S síncrona, uma
  requisição HTTP por thread, e a documentação recomenda `threads` de 2 a 5 vezes os núcleos para
  essa leitura ([`duckdb.md`](duckdb.md)); o padrão é um por núcleo, 4 no ambiente alvo em
  2026-09-23 (2 em 2026-09-21), e a sessão a mais de cada tabela de `run.ingest` só acrescenta a
  thread que a chama. `probes/duckdb_threads.py` mede no ambiente alvo, sobre as tabelas Delta que
  a migração gravou, a ingestão pelo motor DuckDB com o padrão e com `threads` até 5 vezes os
  núcleos, e a medição decide o padrão de `DuckDBConfig.threads` para uma raiz no S3
  ([etapa 4](PLAN-STAGE-4.md)). O mesmo probe mede a ingestão de `cad_lancamentos`,
  `cad_contratos`, `cad_operacoes` e `rel_contrato_operacao` em série e numa sessão a mais por
  tabela: em disco local, num macOS de 11 núcleos, quatro tabelas de 8.000.000 de linhas entraram
  em 1,629 s contra 3,498 s em série com `threads = 2`, e parte do ganho veio das threads que
  chamam cada sessão, que o ambiente alvo, com 4 vCPUs, não tem de sobra (2026-09-23,
  [`POC.md`](POC.md)). O probe roda depois da migração dos comandos de `SUITE.md`.
- **O `Double` não finito nas estatísticas do Delta**, a
  [issue #59](https://github.com/felipenoris/serialize-db/issues/59). O `cast` aceita `NaN` e
  infinito numa coluna `Double`, e a biblioteca grava sem mínimo e máximo, no rodapé Parquet e no
  log Delta, as colunas `Double` com valor não finito em cada partição, pela contagem da auditoria
  (decisões do usuário de 2026-09-23, [etapa 3](PLAN-STAGE-3.md)). Continuam abertos:
  - As tabelas que a migração adiantada gravou no ambiente alvo, com o mínimo e o máximo do
    `Double` registrados. O relatório da versão que rodou lá somava cada coluna `Double` por `CAST`
    para `DECIMAL(38, 6)`, que falha com `NaN` e infinito, e só `ContractError` era tratado: uma
    execução completa sem erro indica tabelas sem valor não finito. O script já segue a regra. Os relatórios da execução, ainda não
    disponíveis, dizem quais tabelas rodaram; uma tabela fora deles pede a contagem de `isnan` e
    `isinf`.
- **O texto da auditoria no Redshift.** A suíte leu no ambiente alvo em 2026-09-23 o texto de
  `serialize_db.audit.audit_sql(..., "redshift")` pelo caminho do motor da [etapa 5](PLAN-STAGE-5.md)
  ([`POC.md`](POC.md)): o `search_path`, o `count(CASE WHEN ...)`, o `to_char`, o `octet_length` e o
  `~` passaram, e o `is_valid_json` sobre `SUPER` (`42883`) e o `is_finite` por `NOT IN`, que deixou
  passar o `NaN` da varredura da tabela, reprovaram. O texto novo, `true` no JSON e a comparação
  estrita com os infinitos ([etapa 4](PLAN-STAGE-4.md)), espera duas execuções da suíte:
  `test_audit_sql_under_search_path_and_nan_comparison` compara cada medida da tabela com os
  defeitos plantados com o esperado, e `nan_na_tabela` lê a comparação do `NaN` na varredura. O
  `json_size` do teto de 65.535 bytes do documento JSON (`texto_<coluna>`), que entrou depois
  dessas execuções, espera as mesmas duas.
- **As leituras da etapa 5 na próxima execução da suíte Redshift.** As execuções de 2026-09-23
  responderam o prefixo com `=`, o `LIMIT` externo, o resultado vazio, a tabela temporária, o
  `SUPER` no Parquet, o `row_desc` e o custo da carga pequena ([`POC.md`](POC.md),
  [etapa 5](PLAN-STAGE-5.md)). Faltam:
  - `test_stream_by_unload_with_literal_values`, que parou na contrabarra: o `UNLOAD` agora dobra a
    contrabarra além da aspa, e os seis casos, a aspa, a contrabarra, o `%`, o `LIKE`, a data com o
    número e o `IN` de lista, e o timestamp com o `Float`, esperam duas execuções pelos três
    caminhos;
  - `pg_last_unload_count()`, que separa o resultado vazio do manifesto que falta: a suíte o lê
    depois do `UNLOAD` vazio e do da tabela temporária;
  - o arquivo do `UNLOAD` com uma coluna `SUPER` registrado numa tabela Delta e lido pelo delta-rs
    e pelo `delta_scan`, que nenhum caso da suíte grava ainda.
- **O `ALTER COLUMN TYPE` no datashare.** A [etapa 8](PLAN-STAGE-8.md) trata a largura de
  `String(n)` que cresce como diff destrutivo, com recriação e recarga (decisão do usuário de
  2026-09-23), porque o comando não está na lista do que a escrita por datashare aceita e recusa
  coluna com chave. `test_redshift.py::test_alter_column_type_on_the_share` o lê numa coluna comum
  e numa da chave primária informativa, com a largura em `svv_all_columns` e a inserção de dez
  caracteres depois; o substituto local só confere o código, porque o DuckDB ignora a largura. Se
  o datashare aceitar o aumento numa coluna comum, ele entra na etapa 8 como atalho da recriação.
- **O `EXPLAIN` no datashare.** A leitura da distribuição da [etapa 8](PLAN-STAGE-8.md) é o
  `EXPLAIN` de um join típico entre as tabelas publicadas, e a distribuição fica `AUTO` até o
  plano mostrar `DS_BCAST_INNER` ou `DS_DIST_BOTH` (decisão do usuário de 2026-09-23). Ninguém
  rodou `EXPLAIN` no esquema do datashare com o papel do projeto;
  `test_redshift.py::test_explain_of_a_join_on_the_share` lê o plano, ou a recusa, e os rótulos
  `DS_*` dele. O substituto local só confere o código.

## Decisões de API pendentes por etapa

Cada arquivo de etapa fecha com a seção "Decisões pendentes"; a lista abaixo as reúne, e uma decisão
tomada sai daqui e do arquivo da etapa no mesmo commit.

- [Etapa 5](PLAN-STAGE-5.md): a partição com `Double` não finito exportada por `rewrite` qualquer
  que seja o `mode` (proposto: o rodapé do `UNLOAD` deixa o `NaN` fora do máximo, e o leitor
  Parquet do DuckDB perdeu a linha, leitura de 2026-09-23); o esquema do `stream` vazio de um
  texto pelo `row_desc` de `select * from (<texto>) as t limit 0` (proposto).
- [Etapa 7](PLAN-STAGE-7.md): a `sort_key` na consulta da carga; o padrão de `export_mode` na carga;
  antes da migração adiantada, o `COPY ... TO 's3://...' (RETURN_STATS)` do DuckDB no ambiente alvo
  (ou gravar em disco e subir pelo `boto3`) e a medição da partição de `cad_lancamentos`.
- [Etapa 8](PLAN-STAGE-8.md): a linha de controle da primeira publicação de uma tabela (proposto:
  a tabela publicada e a linha com `delta_version` -1 numa transação própria, antes da primeira
  transação de dados). A staging da publicação como tabela comum no datashare ou temporária.
  A regra de que a escrita de uma transação vai para um banco só vem da página "Considerations for
  data sharing reads and writes" da AWS e nunca foi medida no ambiente alvo, e a página não diz em
  que banco fica a tabela temporária criada depois do `USE` ([`redshift.md`](redshift.md));
  `test_redshift_transactions.py` lê a temporária cheia dentro da transação e antes do `BEGIN`, e
  passou no substituto local em 2026-09-23, que só confere o código.
