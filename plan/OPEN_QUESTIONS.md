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
- **A migração de `cad_lancamentos` 2026-03-31 no ambiente alvo.** Na migração de 2026-09-23 às
  23:05, numa máquina de 4 vCPUs e 15.786 MB, o kernel matou o processo na partição 2026-03-31, de
  52.654.607 linhas, depois de gravar 2026-01-31 e 2026-02-28: o usuário viu `Killed` no terminal
  algumas vezes, sob o `memory_limit` padrão do DuckDB, 12,3 GiB ([`POC.md`](POC.md)). O script
  abre agora cada conexão com metade da memória que o processo ainda pode usar e com as CPUs dele,
  e a carga de cada tabela na sua conexão, fechada no fim ([etapa 7](PLAN-STAGE-7.md)). A próxima
  execução de `cad_lancamentos` confirma que a partição cabe e traz a medição das quatro variantes
  dela; o script regrava o relatório depois de cada passo.
- **A metade da memória disponível no `memory_limit`.** O motor DuckDB e o script tiram o
  `memory_limit` da memória que o processo ainda pode usar na abertura, pela instrução do usuário
  de 2026-09-24; a metade vem da documentação do DuckDB, que pede de 50% a 60% quando o sistema
  mata o processo, e do RSS que passou do limite em 13% a 21% no `COPY` ordenado
  ([etapa 4](PLAN-STAGE-4.md)). O pico da execução de `cad_lancamentos` no ambiente alvo, que o
  relatório dá ao lado do limite de cada carga e de cada variante, confirma a fração ou pede outra.
- **O `threads` do DuckDB na leitura do S3.** O DuckDB lê arquivos remotos com E/S síncrona, uma
  requisição HTTP por thread, e a documentação recomenda `threads` de 2 a 5 vezes os núcleos para
  essa leitura ([`duckdb.md`](duckdb.md)); o padrão do motor são as CPUs que o processo pode usar. A
  execução de `probes/duckdb_threads.py` no ambiente alvo em 2026-09-23 às 23:21
  ([`POC.md`](POC.md)) mediu a materialização limitada pela CPU, mais lenta com mais threads, e a
  sessão a mais por tabela 1,25 vez mais rápida que a série com 4 threads. A leitura do S3 só a
  primeira repetição de cada configuração fez, porque o cache de arquivos externos do DuckDB serviu
  as outras da memória: nela, a leitura agregada de 393 MB levou 4,1 s com 4 threads e de 1,9 s a
  2,1 s com 8 a 20. O probe agora desliga o cache em cada configuração e mede também a metade das
  CPUs, uma thread por núcleo físico nas instâncias x86 da AWS com SMT (pedido do usuário de
  2026-09-24). A próxima execução, também numa máquina com mais núcleos, decide o padrão de
  `DuckDBConfig.threads` para uma raiz no S3 ([etapa 4](PLAN-STAGE-4.md)) e mostra como a
  materialização escala com as vCPUs, que o usuário escolhe (instrução de 2026-09-23: otimizar para
  o processamento paralelo). O probe e a migração não rodam ao mesmo tempo, porque disputariam as
  mesmas vCPUs.
- **O `Double` não finito nas estatísticas do Delta**, a
  [issue #59](https://github.com/felipenoris/serialize-db/issues/59). O `cast` aceita `NaN` e
  infinito numa coluna `Double`, e a biblioteca grava sem mínimo e máximo, no rodapé Parquet e no
  log Delta, as colunas `Double` com valor não finito em cada partição, pela contagem da auditoria
  (decisões do usuário de 2026-09-23, [etapa 3](PLAN-STAGE-3.md)). Continua aberto:
  - As tabelas da primeira migração, gravadas antes da regra, com o mínimo e o máximo do `Double`
    registrados. A raiz nova de 2026-09-23 às 23:05, gravada pela regra, as substitui: onze das
    doze tabelas terminaram nela sem valor não finito, e `cad_lancamentos` termina na próxima
    execução.
- **O texto da auditoria no Redshift.** As execuções de 2026-09-23 às 22:56 e às 23:01 rodaram o
  texto de `serialize_db.audit.audit_sql(..., "redshift")` inteiro, com o `true` do JSON sobre
  `SUPER`, e a soma de controle deixou o `NaN` e o infinito de fora; mas a negação da comparação
  estrita com os infinitos não contou o `NaN` da varredura da tabela, e `naofinito_valor` deu 1 dos
  2 ([`POC.md`](POC.md)). A contagem dos não finitos passou a ser a dos não nulos menos a dos
  finitos, sem negação ([etapa 4](PLAN-STAGE-4.md)), e espera duas execuções da suíte:
  `test_audit_sql_under_search_path_and_nan_comparison` compara cada medida com o esperado, e
  `nan_na_tabela_detalhe` lê, na linha do `NaN`, cada comparação, a negação e o texto do valor. O
  `json_size` do teto do documento JSON (`texto_<coluna>`) deu 0 na coluna `SUPER` da tabela com
  os defeitos plantados, sem documento acima do teto.
- **O arquivo do `UNLOAD` com uma coluna `SUPER` numa tabela Delta.** As execuções de 2026-09-23
  leram o `SUPER` no Parquet do `UNLOAD` como `extension<arrow.json>`, com o texto de cada valor,
  que o `cast` do contrato converte em `string` ([etapa 5](PLAN-STAGE-5.md)). Nenhum caso da suíte
  registra esse arquivo numa tabela Delta e o lê pelo delta-rs e pelo `delta_scan`, o caminho do
  `export_partition` em `register` de uma tabela com coluna JSON.

## Decisões de API pendentes por etapa

Cada arquivo de etapa fecha com a seção "Decisões pendentes"; a lista abaixo as reúne, e uma decisão
tomada sai daqui e do arquivo da etapa no mesmo commit.

- [Etapa 7](PLAN-STAGE-7.md): se o `rewrite` sai das etapas 4 e 7, com a flag `export_mode` de
  `Execution`, de `serialize-db run` e de `SERIALIZE_DB_EXPORT_MODE` e os testes dele, depois da
  aprovação de 2026-09-24 do `register` como padrão nas etapas 4, 5 e 7 e do `rewrite` só na troca
  da etapa 5 para a partição com `Double` não finito. Proposto: tirá-lo, com `publish_partition` na
  troca da etapa 5 e as variantes da medição no script até a etapa 7 absorvê-lo.
