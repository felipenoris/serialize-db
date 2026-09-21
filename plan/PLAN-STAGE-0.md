# Etapa 0: prova de conceito na AWS

A entrega e o critério de aceite desta etapa estão na tabela de etapas de [`PLAN.md`](PLAN.md), que
também fixa as decisões, as regras que toda etapa obedece e a ordem do trabalho.

Os itens de S3 estão verificados ([`POC.md`](POC.md)). Os de Redshift estão em
`tests/proof_of_concept/test_redshift.py`, marcador `redshift`, executado seis vezes no ambiente
alvo em 2026-09-21 (às 10:50, uma transação só, abortada; às 11:28, sete passaram e o
`COPY ... MANIFEST` falhou pela barra dobrada na URL do manifesto; às 12:08 e às 12:10, dez passaram
e a contagem repetida depois de um `TRUNCATE` recebeu o `34510` do cache de prepared statements do
driver; às 13:35 e às 13:39, com o cache desligado, os doze passaram, [`POC.md`](POC.md)): as duas
execuções limpas que a etapa exige, e a etapa está concluída. Ele roda com os
arquivos sob `SERIALIZE_DB_TEST_S3_ROOT` e as tabelas no esquema de
`SERIALIZE_DB_TEST_REDSHIFT_SCHEMA`, no banco de `SERIALIZE_DB_REDSHIFT_SHARE_DATABASE`. O caminho
de conexão já está fixado: a credencial temporária do workgroup serverless, de
[`../examples/redshift_native.py`](../examples/redshift_native.py), executado no ambiente alvo em
2026-09-20.

- `COPY ... FORMAT AS PARQUET MANIFEST` de arquivos gravados pelo delta-rs: o comando passou no
  datashare em 2026-09-21, com 500.000 linhas em `INT64`, `INT32` de data, `BYTE_ARRAY` e `DOUBLE`
  ([`../examples/redshift_manifest.py`](../examples/redshift_manifest.py)). As colunas que a base
  de origem não tem nem gera passaram pela suíte no mesmo dia, às 12:08 e às 12:10: `DECIMAL(18, 2)`
  em `INT64` e `timestamp_ntz` em `INT64` de microssegundos carregam; uma string acima do `VARCHAR`
  de destino aborta o `COPY` (`Spectrum Scan Error` 15007, o motivo em `sys_load_error_detail`); a
  lista de colunas carrega um arquivo anterior a uma coluna nova, com a coluna nova nula;
  `FILLRECORD` carrega o mesmo arquivo com o mesmo resultado; `SUPER` direto de um Parquet com o
  documento em texto exige `SERIALIZETOJSON` e, com ela, recusa a string acima de 65.535 bytes,
  enquanto `COPY ... FORMAT JSON 'auto'` e `INSERT ... JSON_PARSE` carregam um documento de 80.901
  bytes (o teto do campo JSON é decisão da [etapa 8](PLAN-STAGE-8.md)).
- `UNLOAD ... PARTITION BY (<coluna de partição>) MANIFEST VERBOSE`: **verificado** em 2026-09-21
  por [`../examples/redshift_manifest.py`](../examples/redshift_manifest.py), que exercita este
  item e o `COPY ... MANIFEST` do item anterior num script só, com `cast` para `DECIMAL` e
  `TIMESTAMP` no `select` porque a base de origem não tem coluna de nenhum dos dois tipos. O
  comando é aceito a partir de uma tabela do datashare, grava na convenção Hive com a coluna de
  partição fora dos arquivos, e `create_write_transaction` registrou os arquivos numa tabela Delta
  que devolveu as linhas. Os tipos físicos, a obrigatoriedade das colunas e as estatísticas estão em
  [`POC.md`](POC.md) e [`redshift.md`](redshift.md); o `TIMESTAMP` sai em `INT96`, o
  `DECIMAL(18, 2)` em `FIXED_LEN_BYTE_ARRAY(8)`, toda coluna sai `optional` e há mínimo e máximo.
- O `COPY` associa as colunas por posição e recusa um arquivo com colunas a menos
  (`Unmatched number of columns`, 2026-09-21). Se o Redshift Spectrum mapeia por nome ou por posição
  fica sem leitura, só para registro; o projeto não cria esquemas externos.
- O banco do esquema do projeto: a sessão enxerga `datalake_rw_shared.sbx_aco_decon` (`RS-16`,
  2026-09-20), e depois de `USE datalake_rw_shared` o `CREATE TABLE`, o `COPY` de uma pasta, o
  `SELECT` e o `UNLOAD` passaram por `sbx_aco_decon.<tabela>`
  ([`../examples/redshift_copy_unload.py`](../examples/redshift_copy_unload.py)); o `SELECT` em três
  partes passou de `dev`. Em 2026-09-21 passaram também o `COPY ... MANIFEST`, o `INSERT ... SELECT`
  e o `UNLOAD ... PARTITION BY ... MANIFEST VERBOSE`
  ([`../examples/redshift_manifest.py`](../examples/redshift_manifest.py)). O `DELETE` e o `MERGE`
  da publicação, que a documentação lista entre os comandos aceitos no datashare, ficam para os
  testes `redshift` da [etapa 8](PLAN-STAGE-8.md); a suíte exercitou o `TRUNCATE`, o
  `INSERT ... SELECT`, o `ALTER TABLE ADD COLUMN` e a tabela temporária depois do `USE`. Os
  requisitos da escrita num datashare que a sessão não lê (isolamento do produtor, slices) não
  impediram a escrita.
- O ciclo da Data API com `select`, que devolve `DECIMAL` como texto: a prova de que existe caminho
  sem a porta 5439, e a razão de ela ficar fora da biblioteca.

Antes de qualquer etapa na AWS, os probes rodam no ambiente e o resultado é colado na conversa:
`space.py` e `diagnose_aws.py` para a suíte S3, `bucket.py` para a raiz escolhida, `redshift.py`
para a [etapa 5](PLAN-STAGE-5.md) e `parquet_source.py` para a base de origem da
[etapa 7](PLAN-STAGE-7.md). O `redshift.py` rodou no ambiente alvo em 2026-09-20 e respondeu o papel
IAM do `COPY` (`RS-6`: nenhum, e as credenciais de quem chama o substituem) e o que a sessão lê dos
requisitos do datashare (`RS-17`); a execução seguinte, com o probe revisto, confere o `USE`
(`RS-19`), o que `has_schema_privilege` e `svv_table_info` respondem depois dele (`RS-5`, `RS-8`) e
onde as tabelas de execução podem nascer (`RS-9`), a questão de
[`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md) que a suíte não alcança sem escrever.

## Interface

A etapa não entrega módulo: as suas APIs são as dos pacotes externos, exercitadas por
`tests/proof_of_concept/` e por `examples/`. O que ela fixa para as etapas seguintes são os
parâmetros do ambiente alvo (`RedshiftConfig` da [etapa 5](PLAN-STAGE-5.md), lidos de
`SERIALIZE_DB_REDSHIFT_*`) e os comandos que passaram lá, repetidos verbatim pelos motores.

## Pré-requisitos e pós-condições

| Item | Pré-requisitos | Pós-condições |
| --- | --- | --- |
| Probes no ambiente alvo | `space.py`, `diagnose_aws.py`, `bucket.py` sobre a raiz escolhida, `redshift.py` e `parquet_source.py` executados no ambiente, cada relatório colado na conversa e o do Redshift guardado em `plan/readings/`. | Cada leitura que contraria um documento dispara a revisão dele na mesma unidade de trabalho; as leituras `RS-5`, `RS-8` e `RS-19` fecham o item de [`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md) sobre o `USE`. |
| Suíte `-m redshift` | As variáveis de `README.md` (`SERIALIZE_DB_TEST_REDSHIFT_SCHEMA`, `SERIALIZE_DB_TEST_S3_ROOT`, `SERIALIZE_DB_REDSHIFT_WORKGROUP`, `_DATABASE`, `_SHARE_DATABASE`), a identidade da sessão com `s3:GetObject`, `PutObject` e `DeleteObject` sob a raiz, e `SERIALIZE_DB_TEST_REPORT` apontando para um arquivo, porque o relatório é a resposta. | O relatório JSON entra em `plan/readings/`, cada `redshift.*` do relatório responde uma linha da tabela de testes desta etapa e entra em [`redshift.md`](redshift.md), e [`POC.md`](POC.md) ganha a seção da execução com a data; a suíte é reexecutada uma segunda vez antes de qualquer consequência entrar num arquivo de etapa; cumprido em 2026-09-21, às 13:35 e às 13:39. |
| Exemplos | Um script novo só entra em `examples/` depois de rodar; até lá ele é o próximo experimento, dito no docstring. | O script fica como rodou, o probe e a suíte repetem as suas chamadas. |

## Testes por caso

Cada pergunta pendente da etapa tem um teste na suíte, e a coluna da direita diz onde a resposta
entra.

| Pergunta | Teste em `test_redshift.py` | Documento que recebe a resposta |
| --- | --- | --- |
| `COPY` de Parquet aceita lista de colunas; `FILLRECORD` completa um arquivo anterior a uma coluna nova. | `test_copy_column_list_and_fillrecord`: a lista de colunas e o `FILLRECORD` carregaram 100 linhas com a coluna nova nula, e o posicional reprovou por contagem de colunas (2026-09-21; a lista em quatro execuções, o `FILLRECORD` em duas; asserção desde as 13:35) | [`redshift.md`](redshift.md), "Regras do COPY para Parquet"; a ingestão da [etapa 5](PLAN-STAGE-5.md) e a publicação da [etapa 8](PLAN-STAGE-8.md) escolhem entre lista de colunas e `ALTER TABLE ADD COLUMN`. |
| `DECIMAL(18, 2)` em `INT64` e `timestamp_ntz` em `INT64` de microssegundos carregam pelo `COPY`. | `test_copy_manifest_from_delta_files`: passou, com a soma e o menor `timestamp` conferidos (2026-09-21) | [`redshift.md`](redshift.md) e a tabela de tipos de [`schema.md`](schema.md). |
| Uma string acima do `VARCHAR` de destino trunca ou aborta. | `test_copy_varchar_overflow`: aborta, `Spectrum Scan Error` 15007 com o motivo em `sys_load_error_detail` (2026-09-21); `TRUNCATECOLUMNS` não é aceito com Parquet (`0A000`) | A auditoria de tamanho da [etapa 4](PLAN-STAGE-4.md) é a barreira. |
| `SUPER` recebe um documento acima de 65.535 bytes pelo `COPY` direto. | `test_super_and_json_parse`: o `INSERT ... JSON_PARSE` de 80.901 bytes passou; o `COPY` de Parquet exige `SERIALIZETOJSON` e, com ela, recusa a string acima de 65.535 bytes; `COPY ... FORMAT JSON 'auto'` carregou o objeto (2026-09-21) | O caminho `VARCHAR(65535)` mais `JSON_PARSE` da staging fica, com o teto do campo JSON ou o caminho por JSON como decisão da [etapa 8](PLAN-STAGE-8.md). |
| Dois `COPY` e dois `UNLOAD` em conexões distintas correm em paralelo dentro das slots do WLM. | `test_parallel_copy_and_unload_on_two_connections`: 4,5 s e 3,6 s os `COPY`, 1,9 s e 1,5 s os `UNLOAD` (2026-09-21) | `max_workers` de `publish_redshift` na [etapa 6](PLAN-STAGE-6.md). |
| `fetchmany` lê do socket ou o `execute` materializa o resultado. | O código do driver: `execute` lê cada linha para `cursor._cached_rows` antes de devolver (2026-09-21); `test_cursor_fetchmany_feeds_record_batches` leu 5 linhas na fila antes do primeiro `fetchmany` (13:35 e 13:39), asserção desde então | O `stream` do motor Redshift precisa do `UNLOAD` acima de um limite de linhas ([etapa 5](PLAN-STAGE-5.md)). |
| Um comando repetido depois de um `TRUNCATE` na mesma conexão passa. | `test_repeated_statement_after_truncate_and_the_driver_cache`: com o cache de prepared statements do driver, o datashare respondeu `34510` nas execuções das 12:08 e das 12:10; sem o cache (`max_prepared_statements=0`), passou às 13:35 e às 13:39; com o cache, a repetição e a segunda repetição recebem `34510`, um `ALTER` limpa a entrada, e uma tabela temporária não sofre | O `connect` da [etapa 5](PLAN-STAGE-5.md) e [`redshift.md`](redshift.md). |
| `has_schema_privilege` e `svv_table_info` respondem pelo esquema do datashare depois do `USE`. | `probes/redshift.py` (`RS-5`, `RS-8`, `RS-19`) e `test_session_and_named_parameters` (`redshift.has_schema_privilege_create`): a suíte leu `false` em 2026-09-21, seis vezes | Onde as tabelas de execução nascem ([etapa 5](PLAN-STAGE-5.md)); [`redshift.md`](redshift.md) diz que a função não prova o privilégio, e o `CREATE` sim. |
| `schema.elements` do manifesto verboso lista a coluna de partição. | `test_unload_partition_by_and_register` (`redshift.unload.manifest_schema`): lista, nas três execuções que o leram | A conferência de `register_files` recebe a lista esperada ([etapa 3](PLAN-STAGE-3.md)). |
| O `UNLOAD` nomeia os arquivos e recusa um destino que já tem objetos no prefixo. | `test_unload_partition_by_and_register`: `<coluna>=<valor>/<slice>_part_<nn>.parquet`, com a slice variando entre execuções; o mesmo prefixo e o prefixo pai reprovados, um subprefixo novo aceito (2026-09-21) | O destino `<uri>/<execution_id>/<valor>/` da [etapa 5](PLAN-STAGE-5.md). |

## Decisões pendentes

A suíte Redshift rodou antes da etapa 1, em 2026-09-21, e fixou a tabela de tipos de
[`schema.md`](schema.md) antes de `cast` ser escrito; a etapa não tem decisão pendente. A leitura
`RS-8` (`svv_table_info` depois do `USE`) fica com o probe, sem etapa que dependa dela
([`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md)).
