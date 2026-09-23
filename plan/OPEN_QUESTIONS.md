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
  thread que a chama. A primeira execução no ambiente alvo mede a ingestão por `delta_scan` do S3
  com o padrão e com `threads` acima dos núcleos, e a medição decide o padrão de
  `DuckDBConfig.threads` para uma raiz no S3 ([etapa 4](PLAN-STAGE-4.md)). A mesma execução mede a
  ingestão de várias tabelas em sessões a mais: em disco local, num macOS de 11 núcleos, quatro
  tabelas de 8.000.000 de linhas entraram em 1,629 s contra 3,498 s em série com `threads = 2`, e
  parte do ganho veio das threads que chamam cada sessão, que o ambiente alvo, com 4 vCPUs, não tem
  de sobra (2026-09-23, [`POC.md`](POC.md)).
- **Duas transações simultâneas no esquema do datashare.** A publicação da
  [etapa 8](PLAN-STAGE-8.md) grava a linha de controle em `serialize_db_publications`, a única
  tabela que dois ambientes escrevem, e usa uma staging de nome fixo por ambiente e tabela. A
  documentação prevê que o segundo `DELETE` espere o primeiro terminar e que, sob isolamento de
  snapshot, linhas distintas confirmem as duas transações ([`redshift.md`](redshift.md)); o banco do
  datashare informa isolamento `UNKNOWN`, e o `LOCK` não está na lista de comandos da escrita por
  datashare. `tests/proof_of_concept/test_redshift_transactions.py` (`-m redshift`) mede os cenários
  e espera uma execução no ambiente alvo: o substituto local de 2026-09-23 conferiu só o código
  deles, porque o DuckDB não faz uma transação esperar a outra ([`POC.md`](POC.md)). O resultado
  no ambiente alvo decide se a transação da etapa 8 fica como está ou ganha o `LOCK`, a nova
  tentativa, o `UPDATE` condicionado à versão lida ou a staging por execução.
- **O `Double` não finito nas estatísticas do Delta**, a
  [issue #59](https://github.com/felipenoris/serialize-db/issues/59). O `cast` aceita `NaN` e
  infinito numa coluna `Double`, e a biblioteca grava sem mínimo e máximo, no rodapé Parquet e no
  log Delta, as colunas `Double` com valor não finito em cada partição, pela contagem da auditoria
  (decisões do usuário de 2026-09-23, [etapa 3](PLAN-STAGE-3.md)). Continuam abertos:
  - O rodapé que o `UNLOAD` do Redshift grava num grupo de linhas com `NaN`, que só o ambiente alvo
    mede. O Redshift aceita `NaN` em `DOUBLE PRECISION`, e um rodapé com o máximo sem o `NaN`, como
    o do pyarrow, faz o leitor Parquet do DuckDB perder a linha mesmo com o log sem estatística
    ([duckdb/duckdb#25521](https://github.com/duckdb/duckdb/issues/25521), leituras de 2026-09-23,
    [`POC.md`](POC.md)). `test_redshift.py::test_unload_footer_statistics_with_nan` lê o rodapé
    com o `NaN` no início, no meio e no fim do grupo de linhas e com os infinitos, e o que o
    `read_parquet` do DuckDB devolve para `valor > 3` sobre cada arquivo.
  - As tabelas que a migração adiantada gravou no ambiente alvo, com o mínimo e o máximo do
    `Double` registrados. O relatório da versão que rodou lá somava cada coluna `Double` por `CAST`
    para `DECIMAL(38, 6)`, que falha com `NaN` e infinito, e só `ContractError` era tratado: uma
    execução completa sem erro indica tabelas sem valor não finito. O script já segue a regra. Os relatórios da execução, ainda não
    disponíveis, dizem quais tabelas rodaram; uma tabela fora deles pede a contagem de `isnan` e
    `isinf`.
- **O texto da auditoria no Redshift.** O texto de `serialize_db.audit.audit_sql(..., "redshift")`
  nunca rodou no Redshift: a contagem por `count(CASE WHEN ... THEN 1 END)`, que a documentação do
  `COUNT` sustenta, o `to_char(x, 'YYYY-MM-DD')`, o `is_valid_json`, o `octet_length`, o
  `json_size` do teto de 65.535 bytes do documento JSON, o operador
  POSIX `~` da regra da partição, e o `is_finite` como `x NOT IN ('NaN'::float8, 'Infinity'::float8,
  '-Infinity'::float8)`, que supõe o `NaN` igual a si mesmo, como no PostgreSQL
  ([etapa 4](PLAN-STAGE-4.md)). A [etapa 5](PLAN-STAGE-5.md) roda esse texto, e
  `test_redshift.py::test_audit_sql_under_search_path_and_nan_comparison` o confere antes dela, pelo
  caminho do motor: o `ddl` da etapa 1 e o `audit_sql` citam as tabelas sem esquema, e o caso roda
  numa conexão com `SET search_path` no esquema do datashare depois do `USE`, que também nunca
  rodou lá. Ele lê a comparação do `NaN` que separa o PostgreSQL do IEEE (`'NaN'::float8 =
  'NaN'::float8`, o `is_finite` do `NaN`, dos infinitos, de um número e do nulo, e o `CAST` do `NaN`
  para `NUMERIC(38, 6)`), uma tabela com uma linha de cada defeito, um `NaN` e um infinito, com o
  esperado de cada contador ao lado, cada medida da verificação de linhas isolada (uma medida
  recusada derruba a consulta inteira, e a coluna JSON é `SUPER` no DDL do Redshift, que o
  `is_valid_json` talvez recuse [uncertain]), e os textos do modelo cliente sobre as tabelas vazias.
  Passou pelo emulador local em 2026-09-23 ([`POC.md`](POC.md)).
- **As leituras da etapa 5 na próxima execução da suíte Redshift.** As decisões do usuário de
  2026-09-23 ([etapa 5](PLAN-STAGE-5.md)) supõem comportamentos que ninguém executou no ambiente
  alvo. Os casos estão em `tests/proof_of_concept/test_redshift.py` e passaram, em 2026-09-23, por
  um emulador local com o DuckDB no lugar do Redshift, que confere só o código dos testes
  ([`POC.md`](POC.md)):
  - `test_unload_to_a_hive_prefix_and_register`: o `UNLOAD` sem `PARTITION BY` para um prefixo com
    `=`, `<uri>/<coluna>=<valor>/<execution_id>_<uuid>/`, as colunas do `schema.elements` do
    manifesto dele, o registro dos arquivos, a releitura pelo delta-rs e pelo `delta_scan`, e o
    segundo `UNLOAD` no mesmo destino e num `uuid` novo;
  - `test_stream_by_unload_with_literal_values`: a aspa, a contrabarra, o `%`, o `LIKE`, uma data,
    um número, um `IN` de lista, um timestamp e um `Float`, cada caso pelos parâmetros do driver,
    pelo texto com os literais direto no cursor e pelo mesmo texto dentro do `UNLOAD`; a
    contrabarra, que o dialeto dobra, é a leitura que decide o caminho;
  - `test_unload_limit_empty_result_temp_table_and_super`: a mensagem que recusa o `LIMIT` externo,
    o que o `UNLOAD` grava para um resultado vazio, de que depende o esquema do lote vazio, a tabela
    temporária da sessão lida pelo `UNLOAD` e a coluna `SUPER` no Parquet;
  - `test_row_description_oids_and_type_modifier`: o `row_desc` de um `select` com uma coluna de
    cada tipo do contrato e `SUPER`, e de outro com `count(*)`, `sum` e `avg` de `DECIMAL(18, 2)`,
    `sum` de `DOUBLE PRECISION`, um `DECIMAL(38, 6)` e os literais de texto e de número, que fecha
    a tabela de OIDs de `schema_from_row_description`;
  - `test_small_load_copy_cost`: o melhor de três de um `load` de 10 linhas pelo `COPY` e pelo
    `INSERT` de várias linhas, como leitura: é o que traria de volta o `INSERT` multilinha.
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

- [Etapa 7](PLAN-STAGE-7.md): a `sort_key` na consulta da carga; o padrão de `export_mode` na carga;
  antes da migração adiantada, o `COPY ... TO 's3://...' (RETURN_STATS)` do DuckDB no ambiente alvo
  (ou gravar em disco e subir pelo `boto3`) e a medição da partição de `cad_lancamentos`.
- [Etapa 8](PLAN-STAGE-8.md): a staging da publicação como tabela comum no datashare ou temporária.
  A regra de que a escrita de uma transação vai para um banco só vem da página "Considerations for
  data sharing reads and writes" da AWS e nunca foi medida no ambiente alvo, e a página não diz em
  que banco fica a tabela temporária criada depois do `USE` ([`redshift.md`](redshift.md));
  `test_redshift_transactions.py` lê a temporária cheia dentro da transação e antes do `BEGIN`, e
  passou no substituto local em 2026-09-23, que só confere o código.
