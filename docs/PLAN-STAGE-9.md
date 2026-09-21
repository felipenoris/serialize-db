# Etapa 9: operação

A entrega e o critério de aceite desta etapa estão na tabela de etapas de [`PLAN.md`](PLAN.md), que
também fixa as decisões, as regras que toda etapa obedece e a ordem do trabalho.

As primitivas são as da [etapa 3](PLAN-STAGE-3.md); a etapa entrega a rotina e a documentação.

| Rotina | Quando | Comando |
| --- | --- | --- |
| Snapshot do banco | Na periodicidade do processo, por exemplo o fim do trimestre. | `run.snapshot("2026T3")` na execução marcada. |
| Compactação | Antes de um snapshot, nunca depois. Também normaliza os arquivos que o `UNLOAD` gravou: `INT64` no lugar de `INT96` e de `FIXED_LEN_BYTE_ARRAY`, estatística em toda coluna ([etapa 3](PLAN-STAGE-3.md)). | `serialize-db compact --partitions ...`. |
| `vacuum` | Mensal: lista com `keep_versions` do arquivo de controle, revisada, depois aplicada; `--full` de tempos em tempos para os órfãos. Num bucket versionado o espaço só é liberado pela regra `NoncurrentVersionExpiration`; `probes/bucket.py` (`BK-14`) mostra o acumulado. | `serialize-db vacuum [--apply] [--full]`. |
| Arquivo | Anual: `deep_copy` dos snapshots mais velhos que o prazo da tabela viva para `arquivo/<nome>/<tabela>/`, a entrada sai de `snapshots.json`, a pasta recebe a regra de ciclo de vida. | `serialize-db archive <nome>`. |
| Exportação | Sob demanda: pastas Parquet por partição de um snapshot, `copy` ou `rewrite`. | `serialize-db export`. |
| Auditoria avulsa | Depois de uma correção, e quando o SQL de uma verificação precisa ser lido. | `serialize-db audit --table ... [--sql]`. |
| Monitoração | `history()` de cada tabela com os metadados da biblioteca. | `serialize-db history`. |

A documentação da API sai do `pdoc`; o runbook lista cada rotina com o comando, o que conferir
antes e o que esperar depois. Testes: `tests/test_operation.py` sob a raiz local: `vacuum` com
`keep_versions` preserva a versão do snapshot e remove a intermediária; `compact` antes do snapshot;
`deep_copy` com as mesmas somas. Provas de conceito: `test_deltalake.py`
(`test_vacuum_with_keep_versions`, `test_compact_and_checkpoint`, `test_dataset_reader_and_deep_copy`,
`test_export_snapshot_by_copying_files`, `test_log_files`) e
`test_stdlib.py::test_exclusive_create_atomic_replace_and_fingerprint` (o arquivo de controle).
