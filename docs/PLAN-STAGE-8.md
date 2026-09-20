# Etapa 8: publicação para clientes

A entrega e o critério de aceite desta etapa estão na tabela de etapas de [`PLAN.md`](PLAN.md), que
também fixa as decisões, as regras que toda etapa obedece e a ordem do trabalho.

As tabelas publicadas, `<ambiente>_<tabela>` no esquema único, são derivadas do Delta; nada é
escrito nelas por outro caminho.

| Primitiva | O que faz |
| --- | --- |
| `serialize_db_publications` | `CREATE TABLE IF NOT EXISTS serialize_db_publications (table_name VARCHAR(127), delta_version BIGINT, execution_id VARCHAR(127), published_at TIMESTAMP)`. |
| `run.publish_redshift(*tables)` | Para cada tabela, `version_diff` entre a versão em `serialize_db_publications` e a atual (na primeira publicação, todos os meses); a reconciliação da tabela publicada (`ALTER TABLE ADD COLUMN` no fim, porque o `COPY` é posicional; recriação e recarga no diff destrutivo); numa única transação, por tabela e mês: `DELETE` do mês, `COPY ... MANIFEST` na staging, `INSERT ... SELECT *, '<mes>'` e a linha de controle. |
| `publication_status(db)` | A versão publicada contra a atual de cada tabela, para o operador. |
| `serialize-db publish` | A publicação fora de uma execução, por exemplo depois de uma correção. |

Testes: o SQL da transação comparado com texto esperado, sem cluster; integração marcada
`redshift`. Provas de conceito: `test_deltalake.py::test_version_diff`,
`test_stdlib.py::test_group_log_actions_by_month` e `test_redshift.py::test_copy_manifest_from_delta_files`
(a transação da publicação repete o `COPY` na staging e o `INSERT` com o mês).
