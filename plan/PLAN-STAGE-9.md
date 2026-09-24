# Etapa 9: operação

A entrega e o critério de aceite desta etapa estão na tabela de etapas de [`PLAN.md`](PLAN.md), que
também fixa as decisões, as regras que toda etapa obedece e a ordem do trabalho.

As primitivas são as da [etapa 3](PLAN-STAGE-3.md), mais a inicialização da tabela de controle da
[etapa 8](PLAN-STAGE-8.md); a etapa entrega a rotina e a documentação.

| Rotina | Quando | Comando |
| --- | --- | --- |
| Tabela de controle da publicação | Uma vez no esquema do Redshift, antes da primeira publicação de qualquer ambiente: `publish_redshift` recusa publicar sem ela ([etapa 8](PLAN-STAGE-8.md), decisão do usuário de 2026-09-23). | `serialize-db publish --init`. |
| Snapshot do banco | Na periodicidade do processo, por exemplo o fim do trimestre. | `run.snapshot("2026T3")` na execução marcada. |
| Compactação | Antes de um snapshot, nunca depois. Também normaliza os arquivos que o `UNLOAD` gravou: `INT64` no lugar de `INT96` e de `FIXED_LEN_BYTE_ARRAY`, estatística em toda coluna ([etapa 3](PLAN-STAGE-3.md)). | `serialize-db compact --partitions ...`. |
| `vacuum` | Mensal: lista com `keep_versions` do arquivo de controle, revisada, depois aplicada; `--full` de tempos em tempos para os órfãos. A retenção é de 400 dias (decisão do usuário de 2026-09-23), e `docs/index.md`, seção "Retenção dos arquivos removidos", diz como mudá-la. Num bucket versionado o espaço só é liberado pela regra `NoncurrentVersionExpiration`; `probes/bucket.py` (`BK-14`) mostra o acumulado. | `serialize-db vacuum [--apply] [--full]`. |
| Arquivo | Anual: `deep_copy` dos snapshots mais velhos que o prazo da tabela viva para `arquivo/<nome>/<tabela>/`, a entrada passa de `snapshots` para `archived` no mesmo arquivo, a pasta recebe a regra de ciclo de vida. | `serialize-db archive <nome>`. |
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

## Interface

A etapa não acrescenta módulo: as primitivas são as de `serialize_db.delta`
([etapa 3](PLAN-STAGE-3.md)), e a entrega é o despacho de `serialize_db.cli` para cada rotina, o
runbook e a documentação do `pdoc`. O despacho reaproveita o que a [etapa 6](PLAN-STAGE-6.md) pôs
na linha de comando: `--metadata`, `--root` e `--environment` com os padrões `SERIALIZE_DB_*`, o
`Database` que monta a URI de cada tabela, a regra da partição nos nomes e o `logging` em `INFO`.

```python
"""Os subcomandos de serialize_db.cli entregues pela etapa 9, com os argumentos de cada um."""
COMMANDS = {
    "snapshot": ["--name"],                                  # snapshot(storage, environment, name, versions atuais de todas as tabelas)
    "vacuum": ["--apply", "--full", "--retention-hours"],    # vacuum_keeping_snapshots por tabela; lista sem --apply
    "compact": ["--table", "--partitions"],                  # compact; recusa depois de um snapshot sem confirmação
    "archive": ["--name"],                                   # deep_copy de cada tabela do snapshot para arquivo/<nome>/<tabela>/
    "export": ["--table", "--destination", "--version", "--mode"],   # export_snapshot
    "history": ["--table"],                                  # history() com os metadados da biblioteca
}
```

## Estratégia de implementação

- **`snapshot`** fora de uma execução lê a versão atual de cada tabela do ambiente e grava a
  entrada; dentro da execução, `run.snapshot(name)` a grava no encerramento.
- **`vacuum`** roda `vacuum_keeping_snapshots(uri, control, table.name, storage, retention_hours,
  apply, full)` da [etapa 3](PLAN-STAGE-3.md) para cada tabela, com `keep_versions` do arquivo de
  controle; sem `--apply` imprime a lista por tabela; `--full` inclui os órfãos das escritas
  interrompidas e dos registros recusados. Dentro da retenção de 400 dias nada é listado, porque a
  retenção é a janela em que toda versão continua legível; a lista aparece quando um arquivo
  removido passa dela.
- **`compact`** roda `optimize.compact` das partições pedidas e imprime `numFilesAdded` e
  `numFilesRemoved`; uma partição com um só arquivo não commita. O comando confere o arquivo de
  controle e recusa compactar uma tabela cujo snapshot mais recente é a versão atual, porque a
  compactação depois do snapshot dobra os arquivos que ele referencia. A compactação roda no
  escritor do delta-rs, fora do `memory_limit` do DuckDB, com as tarefas paralelas do padrão do
  delta-rs; a memória dela numa partição de `cad_lancamentos` não foi medida
  ([`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md)).
- **`archive`** lê a entrada do snapshot, roda `deep_copy(uri, versão, storage.uri_of(<ambiente>/arquivo/<nome>/<tabela>), storage)`
  de cada tabela na versão registrada e move a entrada de `snapshots` para a chave irmã `archived`
  do mesmo arquivo, na escrita condicional (`read_snapshots` e `storage.write_text(if_match=...)`),
  como a tabela de rotinas diz (decisão do usuário de 2026-09-23): `vacuum_keeping_snapshots` lê
  só `snapshots`, então a entrada arquivada deixa de prender as versões e o registro do snapshot
  fica no controle. `snapshot` passa a recusar um nome presente em `snapshots` ou em `archived`,
  porque o nome dá a pasta `arquivo/<nome>/`; a recusa e o movimento entram em
  `serialize_db.delta` com a rotina. O `deep_copy` da [etapa 3](PLAN-STAGE-3.md) é um
  `write_deltalake` do leitor da tabela inteira, cuja memória cresce com a tabela, fora do
  `memory_limit` (1.140 MB para 12.000.000 de linhas e 1.960 MB para 24.000.000,
  [`delta.md`](delta.md)): para `cad_lancamentos`, de 141.901.795 linhas, a leitura linear dá
  cerca de 11 GB, sem medição, acima da metade da memória da máquina de 2026-09-23. A proposta, à
  espera do usuário, é copiar partição a partição pelo caminho do registro, o
  `COPY ... (RETURN_STATS)` do DuckDB sob `environment_limits` e `register_files` na tabela criada
  por `create_table`, que [`delta.md`](delta.md) mediu com memória constante
  ([`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md)).
- **`export`** chama `export_snapshot` com `--mode copy` ou `rewrite`; `--version` exporta uma
  versão antiga, com o DDL tirado do esquema daquela versão.
- **`history`** imprime, por tabela, versão, operação, carimbo e os metadados
  `serialize_db_execution_id`, `serialize_db_input_versions` e `serialize_db_snapshot`; os commits
  de `vacuum` (`VACUUM START`, `VACUUM END`) e de `optimize` aparecem sem metadados.
- **O runbook** entra em `docs/operacao.md`, publicado pelo `pdoc` com a docstring de
  `serialize_db.cli` que o inclui (decisão do usuário de 2026-09-23), ao lado da seção "Retenção
  dos arquivos removidos" de `docs/index.md`: uma seção por rotina com o comando, o que conferir
  antes (o arquivo de controle, o espaço em disco, a memória da máquina contra a medida da rotina
  na maior tabela, a última publicação) e o que esperar depois (a versão, a lista do `vacuum`, o
  `history`).

## Pré-requisitos e pós-condições

| Rotina | Pré-requisitos | Pós-condições |
| --- | --- | --- |
| `snapshot` | Nome inédito em `snapshots` e em `archived`; nenhuma execução aberta no ambiente. | A entrada com todas as tabelas; `ConflictError` se outro escritor mudou o controle. |
| `vacuum` | Controle legível. | Sem `--apply`, nenhuma exclusão; com ele, a versão de cada snapshot continua legível e as intermediárias fora da retenção somem. |
| `compact` | Nenhum snapshot na versão atual da tabela. | Menos arquivos na partição; o mesmo conteúdo; um commit `OPTIMIZE` com `dataChange` falso. |
| `archive` | Snapshot registrado. | Uma tabela nova na versão 0 por tabela do snapshot, com as mesmas somas; a entrada movida de `snapshots` para `archived`. |
| `export` | Destino vazio. | Pastas `<coluna>=<valor>/` sem `_delta_log`; em `copy`, os mesmos bytes; em `rewrite`, o esquema atual em todos os arquivos. |
| `history` | Nenhum. | Uma linha por commit, sem credencial. |

## Testes por caso

`tests/test_operation.py` sob a raiz local.

| Caso | Teste | O que confere |
| --- | --- | --- |
| Snapshot preso | `test_vacuum_keeps_the_snapshot_version` | Com retenção zero e o controle, a versão do snapshot lê e a intermediária falha; dentro da retenção nada é listado. |
| Compactação antes | `test_compact_refuses_after_a_snapshot_on_the_current_version` | Recusa quando o snapshot é a versão atual; compacta antes; `dataChange` falso no commit. |
| Cópia profunda | `test_archive_copies_each_table_with_the_same_sums` | Versão 0 no arquivo, somas iguais, a entrada em `archived` e fora de `snapshots`, o `vacuum` sem a versão arquivada em `keep_versions`, e `snapshot` recusando o mesmo nome. |
| Exportação | `test_export_by_copy_and_by_rewrite` | Os dois modos e uma versão antiga. |
| Histórico | `test_history_shows_library_metadata` | Os três metadados nos commits da biblioteca; nenhum nos de manutenção. |
| Linha de comando | `test_cli_operation_commands` | Cada subcomando com os argumentos; `vacuum` sem `--apply` não apaga. |

## Rascunhos executados

O rascunho rodou em 2026-09-21 com as versões fixadas: três arquivos pequenos compactados antes do
snapshot, a entrada gravada, duas correções posteriores, o `vacuum` dentro e fora da retenção, a
cópia profunda e o histórico.

```python
"""Etapa 9: o snapshot do banco no arquivo de controle, o vacuum que o preserva, a compactação antes dele, a cópia profunda e o histórico."""
import datetime as dt
import decimal
import json
import os
import tempfile

import pyarrow as pa
from deltalake import CommitProperties, DeltaTable, write_deltalake

PARTITION = "data_str"


def rows(value: str, start: int, n: int) -> pa.Table:
    return pa.table({"id_operacao": pa.array(range(start, start + n), pa.int64()), "valor": pa.array([decimal.Decimal(k) / 100 for k in range(start, start + n)], pa.decimal128(18, 2)),
                     PARTITION: pa.array([value] * n)})


def snapshot(control_path: str, name: str, versions: dict[str, int]) -> dict:
    """A entrada {nome: versões} em snapshots.json; na biblioteca a escrita é write_text(if_match=...) do storage."""
    control = json.loads(open(control_path).read()) if os.path.exists(control_path) else {"snapshots": {}}
    if name in control["snapshots"]:
        raise ValueError(f"snapshot {name} já existe: {control['snapshots'][name]}")
    control["snapshots"][name] = dict(sorted(versions.items()))
    with open(control_path, "w") as handle:
        json.dump(control, handle, indent=2, sort_keys=True)
    return control


def vacuum_keeping_snapshots(uri: str, table: str, control: dict, retention_hours: int = 24 * 400, apply: bool = False) -> list[str]:
    keep = sorted({v[table] for v in control["snapshots"].values() if table in v})
    return DeltaTable(uri).vacuum(retention_hours=retention_hours, enforce_retention_duration=False, dry_run=not apply, keep_versions=keep)


def compact(uri: str, partitions: list[str]) -> dict:
    return DeltaTable(uri).optimize.compact(partition_filters=[(PARTITION, "in", partitions)])


def deep_copy(uri: str, version: int, destination: str) -> int:
    reader = DeltaTable(uri, version=version).to_pyarrow_dataset().scanner().to_reader()          # nunca to_pyarrow_table antes de encerrar
    write_deltalake(destination, reader, mode="overwrite", partition_by=[PARTITION])
    return DeltaTable(destination).version()


def history(uri: str) -> list[dict]:
    """Os commits da biblioteca: versão, operação e os metadados próprios."""
    keys = ("serialize_db_execution_id", "serialize_db_input_versions", "serialize_db_snapshot")
    return [{"version": h["version"], "operation": h["operation"], **{k: h[k] for k in keys if k in h}} for h in DeltaTable(uri).history()]


with tempfile.TemporaryDirectory() as folder:
    uri, control_path = os.path.join(folder, "prod", "cad_operacoes"), os.path.join(folder, "prod", "_serialize_db", "snapshots.json")
    os.makedirs(os.path.dirname(control_path))
    props = lambda execution, snap=None: CommitProperties(custom_metadata={"serialize_db_execution_id": execution} | ({"serialize_db_snapshot": snap} if snap else {}))
    write_deltalake(uri, rows("2026-07-31", 1, 100), partition_by=[PARTITION], commit_properties=props("exec-1"))                    # 0
    for k in range(3):
        write_deltalake(uri, rows("2026-08-31", 101 + k, 10), mode="append", commit_properties=props("exec-2"))                       # 1..3, três arquivos pequenos
    before = len(DeltaTable(uri).file_uris())
    metrics = compact(uri, ["2026-08-31"])                                                                                            # 4
    print("compactação antes do snapshot:", {k: metrics[k] for k in ("numFilesAdded", "numFilesRemoved")}, "| arquivos vivos:", before, "->", len(DeltaTable(uri).file_uris()))
    marked = DeltaTable(uri).version()
    write_deltalake(uri, rows("2026-08-31", 200, 5), mode="overwrite", predicate=f"{PARTITION} = '2026-08-31'", commit_properties=props("exec-3", "2026T3"))  # 5, a execução marcada
    control = snapshot(control_path, "2026T3", {"cad_operacoes": DeltaTable(uri).version(), "cad_contratos": 88})
    write_deltalake(uri, rows("2026-08-31", 300, 5), mode="overwrite", predicate=f"{PARTITION} = '2026-08-31'", commit_properties=props("exec-4"))  # 6, uma correção posterior
    write_deltalake(uri, rows("2026-08-31", 400, 5), mode="overwrite", predicate=f"{PARTITION} = '2026-08-31'", commit_properties=props("exec-5"))  # 7
    print("controle:", control["snapshots"])
    try:
        snapshot(control_path, "2026T3", {})
    except ValueError as error:
        print("snapshot repetido:", error)

    print("vacuum com a retenção de 400 dias lista", len(vacuum_keeping_snapshots(uri, "cad_operacoes", control)), "arquivo(s): os arquivos são de hoje")
    unprotected = DeltaTable(uri).vacuum(retention_hours=0, enforce_retention_duration=False, dry_run=True)
    listed = vacuum_keeping_snapshots(uri, "cad_operacoes", control, retention_hours=0)
    print("com retenção zero: sem keep_versions seriam", len(unprotected), "arquivo(s); com o snapshot preso,", len(listed))
    removed = vacuum_keeping_snapshots(uri, "cad_operacoes", control, retention_hours=0, apply=True)
    print("versão do snapshot legível depois do vacuum:", DeltaTable(uri, version=control["snapshots"]["2026T3"]["cad_operacoes"]).to_pyarrow_dataset().count_rows(),
          "| atual:", DeltaTable(uri).to_pyarrow_dataset().count_rows())
    try:
        DeltaTable(uri, version=6).to_pyarrow_dataset().count_rows()
    except Exception as error:
        print("versão intermediária:", type(error).__name__)

    archive = os.path.join(folder, "prod", "arquivo", "2026T3", "cad_operacoes")
    print("deep_copy: versão", deep_copy(uri, control["snapshots"]["2026T3"]["cad_operacoes"], archive), "| linhas:", DeltaTable(archive).to_pyarrow_dataset().count_rows())
    for entry in history(uri)[:4]:
        print("history:", entry)
```

Saída:

```
compactação antes do snapshot: {'numFilesAdded': 1, 'numFilesRemoved': 3} | arquivos vivos: 4 -> 2
controle: {'2026T3': {'cad_contratos': 88, 'cad_operacoes': 5}}
snapshot repetido: snapshot 2026T3 já existe: {'cad_contratos': 88, 'cad_operacoes': 5}
vacuum com a retenção de 400 dias lista 0 arquivo(s): os arquivos são de hoje
com retenção zero: sem keep_versions seriam 6 arquivo(s); com o snapshot preso, 5
versão do snapshot legível depois do vacuum: 105 | atual: 105
versão intermediária: FileNotFoundError
deep_copy: versão 0 | linhas: 105
history: {'version': 9, 'operation': 'VACUUM END'}
history: {'version': 8, 'operation': 'VACUUM START'}
history: {'version': 7, 'operation': 'WRITE', 'serialize_db_execution_id': 'exec-5'}
history: {'version': 6, 'operation': 'WRITE', 'serialize_db_execution_id': 'exec-4'}
```

## Decisões pendentes

- **O `archive` partição a partição pelo caminho do registro.** O `deep_copy` da
  [etapa 3](PLAN-STAGE-3.md) reescreve a tabela inteira pelo `write_deltalake`, cuja memória cresce
  com a tabela fora do `memory_limit`; a proposta é a cópia por partição pelo `COPY` do DuckDB sob
  `environment_limits` e `register_files`, na seção "Estratégia de implementação"
  ([`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md)).

As decisões do usuário de 2026-09-23 sobre o lugar do runbook, `docs/operacao.md`, a retenção de
400 dias do `vacuum` mensal e a entrada do snapshot arquivado, movida para a chave irmã `archived`,
estão escritas nas seções que as descrevem.
