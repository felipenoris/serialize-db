# Etapa 8: publicação para clientes

A entrega e o critério de aceite desta etapa estão na tabela de etapas de [`PLAN.md`](PLAN.md), que
também fixa as decisões, as regras que toda etapa obedece e a ordem do trabalho.

As tabelas publicadas, `<ambiente>_<tabela>` no esquema único, são derivadas do Delta; nada é
escrito nelas por outro caminho. O esquema único é `sbx_aco_decon` no banco de datashare
`datalake_rw_shared` (decisão do usuário de 2026-09-20), então todo comando cita a tabela por nome
em duas partes depois do `USE <banco>` que a conexão roda, e a escrita obedece ao que um datashare
aceita ([`redshift.md`](redshift.md)): `COPY` sem cláusula `COMPUPDATE`, a escrita de uma transação
num banco só, e um comando múltiplo apenas dentro de um bloco de transação. A tabela de controle
mora no mesmo banco das tabelas publicadas, e a transação da publicação abre com `BEGIN` explícito.
O `COPY` e o `UNLOAD` levam a cláusula de credenciais da [etapa 5](PLAN-STAGE-5.md).

O `COPY ... MANIFEST` numa tabela de datashare ainda não foi exercitado
([`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md)); o `COPY` de um prefixo de pasta passou. Recusado o
manifesto, a alternativa é copiar para `staging/<execution_id>/`, por `storage.copy`, os arquivos
que o log da versão lista, e carregar esse prefixo: a cópia no S3 não lê os dados, e o prefixo da
partição no Delta não serve direto, porque guarda também os arquivos das versões anteriores até o
`vacuum` ([`delta.md`](delta.md)).

| Primitiva | O que faz |
| --- | --- |
| `serialize_db_publications` | `CREATE TABLE IF NOT EXISTS serialize_db_publications (table_name VARCHAR(127), delta_version BIGINT, execution_id VARCHAR(127), published_at TIMESTAMP)`. |
| `run.publish_redshift(*tables)` | Para cada tabela, `version_diff` entre a versão em `serialize_db_publications` e a atual (na primeira publicação, todas as partições); a reconciliação da tabela publicada (`ALTER TABLE ADD COLUMN` no fim, porque o `COPY` é posicional; recriação e recarga no diff destrutivo); numa única transação aberta por `BEGIN`, por tabela e partição: `DELETE` da partição, `COPY ... MANIFEST` na staging, `INSERT ... SELECT *, '<valor>'` e a linha de controle. |
| `publication_status(db)` | A versão publicada contra a atual de cada tabela, para o operador. |
| `serialize-db publish` | A publicação fora de uma execução, por exemplo depois de uma correção. |

Testes: o SQL da transação comparado com texto esperado, sem conexão, com o nome em duas partes e
a cláusula de credenciais mascarada; integração marcada `redshift`. Provas de conceito:
`test_deltalake.py::test_version_diff`, `test_stdlib.py::test_group_log_actions_by_month` e
`test_redshift.py::test_copy_manifest_from_delta_files` (a transação da publicação repete o `COPY`
na staging e o `INSERT` com a partição).
