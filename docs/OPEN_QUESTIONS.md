# Questões em aberto

Este documento registra o que ainda não tem resposta: as pendências do projeto, as decisões que
esperam o usuário e as perguntas que uma leitura ou um teste vai fechar. Um item sai daqui quando a
resposta entra no documento que a guarda, e a saída nomeia esse documento. O plano está em
[`PLAN.md`](PLAN.md), o estado da implementação em [`CURRENT_STATE.md`](CURRENT_STATE.md) e o que já
foi medido em [`POC.md`](POC.md).

- **Onde ficam as tabelas de execução.** O sandbox `exec_<id>_*` da [etapa 4](PLAN-STAGE-4.md) e as
  stagings do `COPY` nascem no banco do datashare, onde o `CREATE TABLE` passou, e herdam as
  restrições dele: escrita num banco por transação, sem `VIEW`. A alternativa da tabela temporária
  (`TEMP` é verdadeiro no banco da conexão, e `CREATE` não) custa o sandbox morrer com a sessão. A
  [etapa 5](PLAN-STAGE-5.md) decide quando o primeiro pipeline rodar lá.
- **O que `has_schema_privilege` e `svv_table_info` respondem depois do `USE`.** Antes dele as duas
  enxergam só o banco local; se passam a responder pelo esquema do datashare depois dele ninguém
  leu. A execução de 2026-09-21 não as leu porque `RS-19` reprovou pelo critério errado:
  `current_database()` continuou `dev` depois do `USE`, que vale mesmo assim (confirmação do usuário
  no mesmo dia, e os exemplos que rodaram). O critério de `RS-19` passou a ser a resolução de um
  nome em duas partes, e a próxima execução no ambiente alvo lê `RS-5` e `RS-8`.
- **A coluna de partição no `schema` do manifesto verboso.** O `schema.elements` do manifesto do
  `UNLOAD` traz nome e tipo de cada coluna, e é a conferência que `register_files` faz antes do
  commit ([`redshift.md`](redshift.md)). Se ele lista a coluna de partição, que o `PARTITION BY`
  tira dos arquivos, ninguém leu: a execução de 2026-09-21 não imprimiu o bloco. A próxima execução
  de [`../examples/redshift_manifest.py`](../examples/redshift_manifest.py) responde, e por isso a
  lista esperada é parâmetro da conferência.
- **O destino do `UNLOAD` dentro da pasta da tabela.** A referência diz que sem `ALLOWOVERWRITE` nem
  `CLEANPATH` o comando falha quando o destino tem arquivos, e a pasta de uma partição já tem os das
  versões anteriores; por isso a [etapa 5](PLAN-STAGE-5.md) grava em `<uri>/<execution_id>/`, vazio
  por construção, e registra `<execution_id>/<coluna>=<valor>/<arquivo>`. Se "destino com arquivos"
  é a pasta exata ou o prefixo, e como o `UNLOAD` nomeia os arquivos (a execução de 2026-09-21 não
  registrou os nomes), a próxima execução do exemplo responde pelas URLs do manifesto.
- **Versões não correntes.** O bucket é versionado e o papel não lê o ciclo de vida: cada exclusão
  (o `vacuum`, a limpeza da suíte S3) deixa uma versão não corrente invisível à listagem. `BK-14`
  conta o acumulado, e a regra `NoncurrentVersionExpiration` sob a raiz, junto com
  `AbortIncompleteMultipartUpload`, é pergunta para quem administra o bucket.
- **Credenciais de uma hora.** Nenhuma execução mais longa que uma emissão rodou ainda; a
  [etapa 3](PLAN-STAGE-3.md) renova `storage_options` a cada chamada, e a primeira execução longa
  no espaço confirma que o delta-rs e o `boto3` renovam pela cadeia padrão. A credencial do Redshift
  tem o mesmo teto (`GetCredentials`, 3600 segundos): o que acontece com uma conexão aberta quando a
  senha expira, e se ela cai no meio de um `COPY`, ainda não foi medido. As credenciais que o `COPY`
  e o `UNLOAD` levam no texto do comando expiram com as do espaço, e `RS-18` imprime quando; um
  `COPY` mais longo que isso também não foi medido.
- **Os relatórios dos probes de 2026-09-21.** O usuário os guardou em `secrets/probes-aws-bn/`, fora
  do git; [`POC.md`](POC.md) os interpreta, e `docs/readings/` não os tem. Copiá-los para
  `docs/readings/`, como os de 2026-09-20, é decisão do usuário: eles trazem os mesmos
  identificadores (conta, papel, usuário do banco) que os relatórios já versionados.
- **Quanto o teste de alcance poupa no ambiente alvo.** O IAM (`iam.amazonaws.com`) e o KMS não têm
  endpoint VPC lá, e `simulate_principal_policy` esperou 10 s e `describe_key` 80 s por nada em
  2026-09-21. As duas passaram a `short_config(2, 5, 1)` atrás de um teste TCP de 2 s num endereço
  (`probelib.endpoint_reachable`), que no macOS no mesmo dia baixou de 10,0 s para 2,0 s a espera por
  um endereço sem rota ([`POC.md`](POC.md)). A próxima execução dos probes no alvo diz o que sobra;
  a permissão sobre a raiz fica provada pela primeira escrita.
- **A Data API em `PICKED`.** Em 2026-09-21 o `select 1` ficou 30 s em `PICKED` sem terminar, e em
  2026-09-20 respondeu em 23 ms. A repetição diz se é transitório; a Data API está fora da
  biblioteca, e `RS-10` a mantém como leitura.
- **`Text` no Redshift.** O `sqlalchemy-redshift` compila `Text` como `TEXT`, que o Redshift guarda
  como `VARCHAR(256)`. A [etapa 1](PLAN-STAGE-1.md) emite `VARCHAR(65535)` por uma regra
  `@compiles(Text, "redshift")` em `ddl`, em vez de exigir `String(65535)` nos modelos; a escolha
  ainda não foi confirmada pelo usuário.
- **Barreira por tabela.** Um cliente que dispara `load` numa thread e esquece o `result()` lê o
  estado anterior em silêncio, porque o DuckDB não espera. A guarda: `load` marca a tabela em voo,
  e `query` e `execute` esperam as tabelas em voo que o statement referencia, tiradas por
  `find_tables` do statement Core ou do sentinela `{prefix}` do texto gerado
  (`test_parallel.py::test_table_barrier_delays_the_read_until_the_load_lands`). Fica fora das
  etapas até existir um pipeline paralelo real.
- **`fetchmany` do `redshift_connector`.** O `stream` do motor Redshift monta cada lote de
  `cursor.fetchmany(batch_size)`; se o driver lê as linhas do socket a cada chamada ou materializa o
  resultado inteiro no `execute` decide se `stream` limita a memória sem `UNLOAD`.
  `test_redshift.py::test_cursor_fetchmany_feeds_record_batches` exercita o caminho, e a primeira
  execução com uma consulta grande mede a memória ([etapa 5](PLAN-STAGE-5.md)).
- **A memória da partição de `cad_lancamentos`.** Cerca de 700 MB de Parquet e 35 milhões de
  linhas por partição; a primeira carga real mede o `write_deltalake` de um leitor e o `COPY ...
  RETURN_STATS` mais `register_files` antes de fixar o padrão ([etapa 7](PLAN-STAGE-7.md)). `export_mode="rewrite"` e `"register"` medem os dois caminhos em cada motor e na carga inicial
  (etapas [4](PLAN-STAGE-4.md), [5](PLAN-STAGE-5.md) e [7](PLAN-STAGE-7.md)), e a medição decide o
  padrão da flag.

## O que a documentação oficial do Redshift não responde

Todas dependem de uma execução da suíte contra o Redshift do ambiente alvo, que ainda não houve; o
caminho de conexão está fixado desde 2026-09-20 ([`../examples/`](../examples/)), e
`tests/proof_of_concept/test_redshift.py` tem um teste por pergunta. As perguntas de S3 foram
respondidas em 2026-09-19 ([`POC.md`](POC.md)). A [etapa 0](PLAN-STAGE-0.md) as agrupa por comando.

- Se o `COPY` de Parquet aceita lista de colunas: o `awswrangler` emite
  `COPY tabela (colunas) ... FORMAT AS PARQUET`, e a referência descreve a lista só para arquivos
  planos.
- A correspondência entre os tipos físicos do Parquet e as colunas do Redshift no `COPY`, em
  especial `TIMESTAMP` como `INT64` em microssegundos e o tipo físico de `DECIMAL`.
- Se o `COPY` trunca ou aborta numa string maior que o `VARCHAR` de destino.
- Se o Spectrum mapeia colunas Parquet soltas por nome ou por posição.
- Se o `FILLRECORD` deixa o `COPY` carregar arquivos antigos, sem as colunas acrescentadas depois,
  que a evolução do Delta e do DuckLake produz.

## Decisões de API pendentes por etapa

Cada arquivo de etapa fecha com a seção "Decisões pendentes"; a lista abaixo as reúne, e uma decisão
tomada sai daqui e do arquivo da etapa no mesmo commit.

- [Etapa 0](PLAN-STAGE-0.md): rodar a suíte Redshift antes da etapa 1, para fixar a tabela de tipos
  antes de `cast` existir.
- [Etapa 1](PLAN-STAGE-1.md): `Text` como `VARCHAR(65535)` por `@compiles`; `String(n)` medido em
  bytes; a coluna sem comentário como violação de `check_models`; `duckdb-engine` e
  `sqlalchemy-redshift` como dependências de execução enquanto `ddl` compilar pelo dialeto.
- [Etapa 2](PLAN-STAGE-2.md): identificadores entre aspas duplas em `bind`; o `sqlglot` no grupo
  `dev`.
- [Etapa 3](PLAN-STAGE-3.md): a reserva de credenciais do `boto3` em `storage_options`; as colunas
  com estatística registrada; `version_diff` quando o log foi limpo.
- [Etapa 4](PLAN-STAGE-4.md): `loader` numa tabela que já existe; o padrão de `memory_limit`; o
  banco em arquivo como padrão; a amostra do `AuditReport`.
- [Etapa 5](PLAN-STAGE-5.md): onde as tabelas `exec_<id>_*` nascem; a confirmação do `USE` pela
  criação da tabela de controle; os limites entre `fetchmany` e `UNLOAD` e entre `INSERT` e `COPY`;
  a tabela de OIDs de `schema_from_description`.
- [Etapa 6](PLAN-STAGE-6.md): `--metadata` na linha de comando; a chave de `next_ids` numa chave
  composta; a barreira por tabela.
- [Etapa 7](PLAN-STAGE-7.md): a `sort_key` na consulta da carga; o padrão de `export_mode` na carga.
- [Etapa 8](PLAN-STAGE-8.md): a staging da publicação no datashare ou temporária; `FILLRECORD` ou
  lista de colunas.
- [Etapa 9](PLAN-STAGE-9.md): o nome do runbook; a marca de arquivamento no controle; a retenção do
  `vacuum` mensal.
