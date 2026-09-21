# Etapa 0: prova de conceito na AWS

A entrega e o critério de aceite desta etapa estão na tabela de etapas de [`PLAN.md`](PLAN.md), que
também fixa as decisões, as regras que toda etapa obedece e a ordem do trabalho.

Os itens de S3 estão verificados ([`POC.md`](POC.md)). Os de Redshift estão em
`tests/proof_of_concept/test_redshift.py`, marcador `redshift`, executado uma vez no ambiente alvo em
2026-09-21 sem resposta, por uma transação aberta antes do `USE` ([`POC.md`](POC.md)); ele roda com os
arquivos sob `SERIALIZE_DB_TEST_S3_ROOT` e as tabelas no esquema de
`SERIALIZE_DB_TEST_REDSHIFT_SCHEMA`, no banco de `SERIALIZE_DB_REDSHIFT_SHARE_DATABASE`. O caminho
de conexão já está fixado: a credencial temporária do workgroup serverless, de
[`../examples/redshift_native.py`](../examples/redshift_native.py), executado no ambiente alvo em
2026-09-20.

- `COPY ... FORMAT AS PARQUET MANIFEST` de arquivos gravados pelo delta-rs: o comando passou no
  datashare em 2026-09-21, com 500.000 linhas em `INT64`, `INT32` de data, `BYTE_ARRAY` e `DOUBLE`
  ([`../examples/redshift_manifest.py`](../examples/redshift_manifest.py)). Faltam as colunas que a
  base de origem não tem nem gera: `DECIMAL(18, 2)` em `INT64`, `timestamp_ntz` em `INT64` de
  microssegundos, o que acontece com uma string acima do `VARCHAR` de destino (truncar ou abortar),
  a lista de colunas no `COPY`, `FILLRECORD` para arquivos anteriores a uma coluna nova, e `SUPER`
  direto do `COPY` para documentos acima de 65.535 bytes.
- `UNLOAD ... PARTITION BY (<coluna de partição>) MANIFEST VERBOSE`: **verificado** em 2026-09-21
  por [`../examples/redshift_manifest.py`](../examples/redshift_manifest.py), que exercita este
  item e o `COPY ... MANIFEST` do item anterior num script só, com `cast` para `DECIMAL` e
  `TIMESTAMP` no `select` porque a base de origem não tem coluna de nenhum dos dois tipos. O
  comando é aceito a partir de uma tabela do datashare, grava na convenção Hive com a coluna de
  partição fora dos arquivos, e `create_write_transaction` registrou os arquivos numa tabela Delta
  que devolveu as linhas. Os tipos físicos, a obrigatoriedade das colunas e as estatísticas estão em
  [`POC.md`](POC.md) e [`redshift.md`](redshift.md); o `TIMESTAMP` sai em `INT96`, o
  `DECIMAL(18, 2)` em `FIXED_LEN_BYTE_ARRAY(8)`, toda coluna sai `optional` e há mínimo e máximo.
- Se o Redshift Spectrum mapeia colunas Parquet por nome ou por posição, só para registro; o
  projeto não cria esquemas externos.
- O banco do esquema do projeto: a sessão enxerga `datalake_rw_shared.sbx_aco_decon` (`RS-16`,
  2026-09-20), e depois de `USE datalake_rw_shared` o `CREATE TABLE`, o `COPY` de uma pasta, o
  `SELECT` e o `UNLOAD` passaram por `sbx_aco_decon.<tabela>`
  ([`../examples/redshift_copy_unload.py`](../examples/redshift_copy_unload.py)); o `SELECT` em três
  partes passou de `dev`. Em 2026-09-21 passaram também o `COPY ... MANIFEST`, o `INSERT ... SELECT`
  e o `UNLOAD ... PARTITION BY ... MANIFEST VERBOSE`
  ([`../examples/redshift_manifest.py`](../examples/redshift_manifest.py)). Faltam o `DELETE` e o
  `MERGE` da publicação. Os requisitos da escrita num datashare que a sessão não lê (isolamento do
  produtor, slices) não impediram a escrita.
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
| Probes no ambiente alvo | `space.py`, `diagnose_aws.py`, `bucket.py` sobre a raiz escolhida, `redshift.py` e `parquet_source.py` executados no ambiente, cada relatório colado na conversa e o do Redshift guardado em `docs/readings/`. | Cada leitura que contraria um documento dispara a revisão dele na mesma unidade de trabalho; as leituras `RS-5`, `RS-8` e `RS-19` fecham o item de [`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md) sobre o `USE`. |
| Suíte `-m redshift` | As variáveis de `README.md` (`SERIALIZE_DB_TEST_REDSHIFT_SCHEMA`, `SERIALIZE_DB_TEST_S3_ROOT`, `SERIALIZE_DB_REDSHIFT_WORKGROUP`, `_DATABASE`, `_SHARE_DATABASE`), a identidade da sessão com `s3:GetObject`, `PutObject` e `DeleteObject` sob a raiz, e `SERIALIZE_DB_TEST_REPORT` apontando para um arquivo, porque o relatório é a resposta. | O relatório JSON entra em `docs/readings/`, cada `redshift.*` do relatório responde uma linha da tabela de testes desta etapa e entra em [`redshift.md`](redshift.md), e [`POC.md`](POC.md) ganha a seção da execução com a data; a suíte é reexecutada uma segunda vez antes de qualquer consequência entrar num arquivo de etapa. |
| Exemplos | Um script novo só entra em `examples/` depois de rodar; até lá ele é o próximo experimento, dito no docstring. | O script fica como rodou, o probe e a suíte repetem as suas chamadas. |

## Testes por caso

Cada pergunta pendente da etapa tem um teste na suíte, e a coluna da direita diz onde a resposta
entra.

| Pergunta | Teste em `test_redshift.py` | Documento que recebe a resposta |
| --- | --- | --- |
| `COPY` de Parquet aceita lista de colunas; `FILLRECORD` completa um arquivo anterior a uma coluna nova. | `test_copy_column_list_and_fillrecord` | [`redshift.md`](redshift.md), "Regras do COPY para Parquet"; a ingestão da [etapa 5](PLAN-STAGE-5.md) e a publicação da [etapa 8](PLAN-STAGE-8.md) escolhem entre lista de colunas e `ALTER TABLE ADD COLUMN`. |
| `DECIMAL(18, 2)` em `INT64` e `timestamp_ntz` em `INT64` de microssegundos carregam pelo `COPY`. | `test_copy_manifest_from_delta_files` | [`redshift.md`](redshift.md) e a tabela de tipos de [`schema.md`](schema.md). |
| Uma string acima do `VARCHAR` de destino trunca ou aborta. | `test_copy_varchar_overflow` | A auditoria de tamanho da [etapa 4](PLAN-STAGE-4.md) passa a barreira ou a aviso. |
| `SUPER` recebe um documento acima de 65.535 bytes pelo `COPY` direto. | `test_super_and_json_parse` | O caminho `VARCHAR(65535)` mais `JSON_PARSE` da staging fica ou cede ao `COPY` em `SUPER`. |
| Dois `COPY` e dois `UNLOAD` em conexões distintas correm em paralelo dentro das slots do WLM. | `test_parallel_copy_and_unload_on_two_connections` | `max_workers` de `publish_redshift` na [etapa 6](PLAN-STAGE-6.md). |
| `fetchmany` lê do socket ou o `execute` materializa o resultado. | `test_cursor_fetchmany_feeds_record_batches`, com uma consulta grande e a memória medida | O `stream` do motor Redshift decide se precisa do `UNLOAD` acima de um limite de linhas ([etapa 5](PLAN-STAGE-5.md)). |
| `has_schema_privilege` e `svv_table_info` respondem pelo esquema do datashare depois do `USE`. | `probes/redshift.py` (`RS-5`, `RS-8`, `RS-19`) | Onde as tabelas de execução nascem ([etapa 5](PLAN-STAGE-5.md)). |
| `schema.elements` do manifesto verboso lista a coluna de partição. | `test_unload_partition_by_and_register` (`redshift.unload.manifest_schema`) | A conferência de `register_files` recebe a lista esperada ([etapa 3](PLAN-STAGE-3.md)). |
| O `UNLOAD` nomeia os arquivos e recusa um destino que já tem objetos no prefixo. | `test_unload_partition_by_and_register` (`ALLOWOVERWRITE` fora, um segundo `UNLOAD` no mesmo destino) | O destino `<uri>/<execution_id>/` da [etapa 5](PLAN-STAGE-5.md). |

## Decisões pendentes

- **[decisão] A suíte Redshift roda antes ou depois da etapa 1.** A ordem do trabalho põe as
  etapas 1 e 2 em pasta local; a suíte depende só do ambiente alvo e pode rodar a qualquer momento.
  Rodar antes fixa a tabela de tipos de [`schema.md`](schema.md) antes de `cast` ser escrito.
