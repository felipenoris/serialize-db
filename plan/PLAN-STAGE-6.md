# Etapa 6: execução e linha de comando

A entrega e o critério de aceite desta etapa estão na tabela de etapas de [`PLAN.md`](PLAN.md), que
também fixa as decisões, as regras que toda etapa obedece e a ordem do trabalho.

`serialize_db.execution` é o ciclo de uma execução; `serialize_db.cli` o expõe.

| Primitiva | O que faz |
| --- | --- |
| `Database(root, environment, metadata)` | A raiz do banco, o ambiente (`prod`, `dev`) e o `MetaData` dos modelos; `uri(table)` é `<root>/<ambiente>/<tabela>`, sem barra final, porque os caminhos das etapas 3 a 5 acrescentam `/<coluna>=<valor>/`, mais o arquivo de controle e os prefixos `staging/`, `publicacao/` e `arquivo/`; chama `prepare_environment`, e `storage` é o `Storage.for_uri(root)`, criado no primeiro uso. As opções do delta-rs saem de `storage.storage_options()` a cada chamada, sem credencial ([etapa 3](PLAN-STAGE-3.md)). |
| `Execution(db, engine, partition, execution_id=None, export_mode=None)` | Gerenciador de contexto: na entrada abre as tabelas de entrada, fixa `versions` e cria o sandbox; na saída descarta o sandbox e grava o resumo no log. As primitivas podem ser chamadas de qualquer thread, cada uma na sessão única do motor, sob o lock dele, e o estado mutável (`versions`, auditorias aprovadas, o alocador) fica sob lock. `export_mode` é o modo dos `publish` da execução que não informam o seu, e o `--export-mode` do `serialize-db run` o preenche. |
| `run.previous_partitions(table, n)` | Os `n` últimos valores de partição da tabela na versão fixada até `run.partition`, inclusive, lidos das ações `add`, na ordem de texto dos valores, que nos valores `AAAA-MM-DD` é a do calendário; o calendário é do cliente, não da biblioteca. |
| `run.ingest(*tables, partitions=None, materialize=False)` | `engine.ingest` de cada tabela na versão fixada; sem `partitions`, a tabela inteira. Uma tabela entra na sessão principal; mais de uma entram todas em paralelo, cada uma numa sessão a mais do motor (`new_session`), e a chamada volta quando todas terminam. Cada comando também usa o paralelismo do motor (as `threads` do DuckDB, as slices do Redshift). Em disco local, quatro tabelas de 150.000 linhas entraram em 0,017 s em quatro sessões e em 0,066 s em série (`test_parallel.py`, 2026-09-23). |
| `run.published(table)` | A versão fixada da tabela como origem de consulta, por `engine.published(table, db.uri(table), versions[table])`: `delta_scan('<uri>', version := <v>)` no DuckDB, a staging `exec_<id>_<tabela>_publicado` no Redshift ([etapa 4](PLAN-STAGE-4.md), [etapa 5](PLAN-STAGE-5.md)). É por ela que o pipeline lê as partições já publicadas da tabela que ele mesmo grava, cujo nome no sandbox pertence ao `loader` (decisão do usuário de 2026-09-22); numa tabela que ainda não existe, levanta `SandboxError`. |
| `run.sandbox` | O motor, onde o pipeline chama `stream` e `loader`, os lotes na saída e na entrada, e `query` e `load`, as formas por `pa.Table`, de qualquer thread; nos dois motores todo comando passa pela sessão única da execução, sob o lock que as primitivas tomam e soltam ([etapa 4](PLAN-STAGE-4.md), [etapa 5](PLAN-STAGE-5.md)); `with run.sandbox.session() as connection:` dá a conexão crua para o que as primitivas não cobrem, com o lock tomado pelo bloco e reentrante na mesma thread, e `with run.sandbox.new_session() as other:` abre uma sessão a mais para o que roda em paralelo, sem as tabelas temporárias da principal. |
| `run.next_ids(table, n)` | Um `range` de `n` inteiros contíguos da chave sequencial, a chave primária inteira de uma coluna, sob lock, a partir de `max_key(chave) + 1` na versão fixada da tabela, lido uma vez por tabela; as faixas de threads paralelas não se sobrepõem, e os ids de uma reexecução diferem. Numa tabela de chave primária composta o cliente decide os ids e não chama `next_ids` (decisão do usuário de 2026-09-23). |
| `run.audit(table, partitions, foreign_keys=False, key_scope=None)` | `engine.audit`; a reprovação levanta `AuditFailed` e encerra sem tocar o Delta, e o relatório, com o SQL de cada verificação e até 20 linhas de amostra por verificação reprovada, vai para o log. |
| `run.publish(*tables, partitions=None, audit=True, max_workers=1, export_mode=None)` | Exige a auditoria aprovada de cada tabela nessas partições na própria execução, e `audit=False` dispensa a exigência e fica no log; confere por `version_diff` que nenhuma alteração de dados entrou em cada tabela desde a versão fixada e aborta com `ExecutionConflict` quando entrou (um avanço só de metadados ou de manutenção passa e atualiza a versão fixada); depois `create_table` se não existir, `reconcile`, `export_partition` e `publish_partition` por partição com `commit_metadata`, tabela a tabela num `ThreadPoolExecutor(max_workers)`: na primeira falha as tarefas em curso terminam, as não iniciadas são canceladas, e a exceção lista o resultado de cada tabela, porque os commits feitos ficam; avança `versions[table]` sob lock. O padrão 1 vem da memória por escrita (`delta.md`). `export_mode` (`"register"` ou `"rewrite"`) é resolvido aqui, nesta ordem: o argumento de `publish`, o de `Execution`, `SERIALIZE_DB_EXPORT_MODE` e `"register"`, sem leitura da variável no motor (decisão do usuário de 2026-09-23), e vai ao `export_partition` dos dois motores ([etapa 4](PLAN-STAGE-4.md), [etapa 5](PLAN-STAGE-5.md)): `register` registra por `register_files` o arquivo que o motor gravou, depois das conferências da [etapa 3](PLAN-STAGE-3.md); `rewrite` grava por `publish_partition` a partir do leitor. A cada partição, `run.publish` passa a `export_partition`, em `columns_without_min_max`, as colunas `Double` com valor não finito que a auditoria da execução contou (`AuditReport.nonfinite_columns`), e todas as colunas `Double` da tabela com `audit=False`; a lista supõe a tabela sem mudança entre a auditoria e a publicação, como a própria aprovação (decisão do usuário de 2026-09-23, [issue #59](https://github.com/felipenoris/serialize-db/issues/59)). |
| `run.publish_redshift(*tables, max_workers=1)` | A publicação da [etapa 8](PLAN-STAGE-8.md), uma conexão por tabela em paralelo, limitada pelas slots do WLM; dois `COPY` e dois `UNLOAD` em conexões distintas passaram em paralelo no ambiente alvo em 2026-09-21 ([`POC.md`](POC.md)). |
| `run.snapshot(name)` | Marca a execução: `serialize_db_snapshot` nos commits e `delta.snapshot(storage, environment, name, versions)` no encerramento ([etapa 3](PLAN-STAGE-3.md)). |
| `serialize-db run` | `--root`, `--environment`, `--engine`, `--export-mode`, `--partition`, `--execution-id`, `--metadata modulo:atributo` (o `MetaData` dos modelos, como em `schema` e `sql`) e `modulo:funcao` do pipeline, que recebe `run`; código de saída 0, 1 na reprovação da auditoria, 2 no conflito. |
| `serialize-db audit` | `--metadata`, `--table`, `--partitions`, `--foreign-keys`, `--key-scope` e `--engine`, que escolhe o dialeto; com `--sql` imprime, para depuração, o texto das verificações desse dialeto e não abre conexão nem armazenamento. Sem `--sql`, com `--root` e `--environment`, roda a auditoria sobre a versão publicada e imprime o relatório. Os padrões vêm das variáveis `SERIALIZE_DB_*`, como no `run`. |

O log é o `logging` padrão com um resumo por execução: identificador, partição, versões lidas, versões
gravadas e tempo por passo. Testes: `tests/test_execution.py` sob a raiz local com o motor DuckDB:
a reexecução com o mesmo `execution_id` produz as mesmas linhas, com ids que podem diferir;
`next_ids` de duas threads devolve faixas disjuntas; `ingest` de três tabelas as carrega em três
sessões a mais, a sessão principal lê as três, e a falha de uma lista o resultado das outras; a auditoria reprovada deixa a versão da tabela
como estava; de duas execuções publicando a mesma partição, a segunda aborta com `ExecutionConflict`, e
também a que publica uma tabela em que outra execução gravou dados desde a abertura, e não a que só foi compactada; `publish` com
`max_workers=2` dá o mesmo resultado que com 1. Provas de conceito: `test_stdlib.py` (`test_partition_values_in_text_order`,
`test_execution_identifiers`, `test_frozen_dataclass_derives_an_attribute_by_cached_property`,
`test_context_manager_cleans_up_on_failure`,
`test_entry_point_by_import_string`, `test_command_line_parsing`, `test_execution_log`,
`test_prepare_environment`, `test_json_control_file_and_commit_metadata`) e
`test_deltalake.py::test_time_travel_and_restore` (os metadados de commit no histórico) e
`test_concurrency.py` (os leitores Delta presos à versão carregada durante um `append`, as faixas de
identificadores de um contador sob `Lock`), `test_parallel.py` (o pool que termina o que está em
curso e cancela o resto, `max_key` pelas estatísticas, a leitura ao lado de um `load` esquecido numa
thread, que falha em vez de ler dado velho) e
`test_deltalake.py::test_partition_value_is_percent_encoded_in_the_folder_and_the_log` (a
codificação do valor de partição que a regra da partição evita).

## Interface

```python
"""Assinaturas de serialize_db.execution e serialize_db.cli; os corpos estão no rascunho abaixo."""
import dataclasses
import functools
from collections.abc import Mapping
from typing import Literal

import sqlalchemy as sa

ExportMode = Literal["register", "rewrite"]


class AuditFailed(Exception):       # o módulo importa o de serialize_db.errors
    """A auditoria reprovou, ou publish foi chamado sem ela; a execução encerra sem tocar o Delta."""


@dataclasses.dataclass(frozen=True)
class Database:
    root: str
    environment: str                       # prod, dev
    metadata: sa.MetaData                  # os modelos do cliente

    @functools.cached_property
    def storage(self) -> object: ...                              # Storage.for_uri(root), criado no primeiro uso
    def uri(self, table: sa.Table) -> str: ...                    # <root>/<ambiente>/<tabela>, sem barra final
    def control_path(self) -> str: ...                            # <ambiente>/_serialize_db/snapshots.json
    def staging_prefix(self, execution_id: str) -> str: ...       # <ambiente>/staging/<execution_id>/
    def publication_prefix(self, execution_id: str) -> str: ...   # <ambiente>/publicacao/<execution_id>/
    def archive_prefix(self, name: str) -> str: ...               # <ambiente>/arquivo/<nome>/
    def tables(self) -> list[sa.Table]: ...


class Execution:
    def __init__(self, db: Database, engine: str | object, partition: str, execution_id: str | None = None, export_mode: ExportMode | None = None) -> None: ...
    def __enter__(self) -> "Execution": ...
    def __exit__(self, *exc: object) -> None: ...
    versions: dict[str, int | None]        # a versão fixada de cada tabela do ambiente; None quando ainda não existe
    sandbox: object                        # o motor
    partition: str
    execution_id: str
    def previous_partitions(self, table: sa.Table, n: int) -> list[str]: ...
    def ingest(self, *tables: sa.Table, partitions: list[str] | None = None, materialize: bool = False) -> None: ...
    def published(self, table: sa.Table) -> sa.FromClause: ...
    def next_ids(self, table: sa.Table, n: int) -> range: ...
    def audit(self, table: sa.Table, partitions: list[str] | None, foreign_keys: bool = False, key_scope: str | None = None) -> object: ...
    def publish(self, *tables: sa.Table, partitions: list[str] | None = None, audit: bool = True, max_workers: int = 1, export_mode: ExportMode | None = None) -> dict[str, int]: ...
    def publish_redshift(self, *tables: sa.Table, max_workers: int = 1) -> dict[str, int]: ...
    def snapshot(self, name: str) -> None: ...


def main(argv: list[str] | None = None) -> int: ...   # serialize-db run|schema|sql|audit|load|publish|snapshot|vacuum|compact|archive|export|history
```

`Database` exige o `MetaData`: sem ele a execução não sabe que tabelas abrir nem que esquema
reconciliar, e o exemplo de [`PLAN.md`](PLAN.md) passou a informá-lo. `Execution` aceita o nome do
motor (`"duckdb"`, `"redshift"`), que resolve com a configuração das variáveis `SERIALIZE_DB_*`, ou
um motor já construído, para os testes.

## Estratégia de implementação

- **`Database`** chama `prepare_environment` no `__post_init__` e monta os caminhos; não toca o
  armazenamento. `uri(table)` é `storage.uri_of("<ambiente>/<tabela>")`, a partir da raiz que
  `Storage.for_uri` normalizou: uma pasta local relativa vira absoluta, sem barra final, e é essa a
  URI que o delta-rs, o DuckDB e `storage.relative` recebem; os prefixos (`control_path`,
  `staging_prefix`, `publication_prefix`, `archive_prefix`) são caminhos relativos à raiz, os que os
  métodos de `Storage` recebem ([etapa 3](PLAN-STAGE-3.md)). `storage` é um
  `functools.cached_property` que cria `Storage.for_uri(root)` no primeiro uso: um campo `init=False` de um dataclass congelado não aceita a atribuição do
  `__post_init__` (`FrozenInstanceError`, leitura de 2026-09-23), e o `object.__setattr__` que a
  contornaria é a atribuição dinâmica que o estilo do projeto evita.
- **`Execution.__enter__`** abre toda tabela do `metadata` que existe no ambiente
  (`delta.table_exists`, e depois `delta.open_table`) e guarda a `DeltaTable` e a versão; a tabela ausente fica com `None`
  e nasce em `publish`. Toda leitura da execução usa esses objetos: `previous_partitions`, `ingest`,
  `next_ids`, `audit`. O `execution_id` ausente vira `exec-<AAAA-MM-DD>-<uuid8>`. A partição e o
  `execution_id` passam pela regra da partição, `re.fullmatch(r"[0-9A-Za-z][0-9A-Za-z_.-]*",
  valor)`: letra ou dígito no início, depois letras, dígitos, `_`, `.` e `-` (decisão do usuário de
  2026-09-23). Os dois viram nome de pasta e literal SQL: a aspa simples fecha o literal
  `'<valor>'` das etapas 3, 4, 5 e 8, e o delta-rs grava `:`, `%` ou um acento codificados no nome
  da pasta, que o `COPY` do `register` gravaria sem codificar ([`POC.md`](POC.md)); com a regra, o
  valor codificado é o próprio valor. A partição também cabe no `String(n)` da coluna de partição
  de cada tabela particionada do modelo, e cada valor de `partitions` que `ingest`, `audit` e
  `publish` recebem passa pela mesma regra. A data `AAAA-MM-DD`, o caso da base atual, e `2026-Q1`
  passam. O motor é construído aqui, com o `execution_id` e o `Storage`: `"duckdb"` é
  `DuckDBEngine(DuckDBConfig(), execution_id, db.storage)` da [etapa 4](PLAN-STAGE-4.md), que também
  confere o `execution_id` pela regra da partição e nasce numa pasta de `tempfile.mkdtemp`.
- **`__exit__`** chama `sandbox.cleanup()` aconteça o que acontecer e grava o resumo no log
  (`serialize_db.execution`): identificador, partição, versões lidas, versões gravadas, tempo por
  passo.
- **`previous_partitions`** lê `partition.<coluna>` de `get_add_actions(flatten=True)` da versão
  fixada, convertido para PyArrow por `pa.table(...)` (o delta-rs devolve uma tabela `arro3`),
  filtra `<= run.partition` e devolve os `n` maiores; o calendário é do cliente.
- **`ingest`** chama `sandbox.ingest(table, uri, versions[table], partitions, materialize)` por
  tabela. Com uma tabela, na sessão principal; com mais, um `ThreadPoolExecutor` com uma thread por
  tabela, e cada thread roda `with sandbox.new_session() as session: session.ingest(...)`. As
  tabelas do sandbox são tabelas comuns, e a sessão principal as vê no comando seguinte. Uma falha
  não cancela as ingestões que já rodam: todas terminam, e a exceção lista o resultado de cada
  tabela; o que entrou fica para o `cleanup`.
- **`published`** devolve `sandbox.published(table, db.uri(table), versions[table])` e não cria
  objeto com o nome do modelo, que fica para o `loader` da tabela que a execução grava.
- **`next_ids`** guarda um contador por tabela sob `threading.Lock`, iniciado em
  `delta.max_key(dt, chave) + 1` na versão fixada, ou 1 numa tabela nova, e devolve `range(inicio,
  inicio + n)`. A chave é a chave primária inteira de uma coluna, a sequencial; numa chave composta
  o cliente decide os ids e não chama `next_ids` (decisão do usuário de 2026-09-23), e a chamada
  numa chave composta ou não inteira levanta `ContractError`.
- **`audit`** chama `sandbox.audit(table, partitions, uri, versions[table], foreign_keys,
  key_scope, referenced)`, com `referenced` montado de `versions` para cada tabela que uma chave
  estrangeira aponta, guarda sob lock o relatório aprovado por `(tabela, partições)`, escreve
  `report.sql()` no log e levanta `AuditFailed` na reprovação. Do relatório saem, por partição, a
  contagem (`report.rows(valor)`), que `publish` passa como `expected_rows`, e o
  `nonfinite_columns` ([etapa 4](PLAN-STAGE-4.md)): uma linha escrita no sandbox entre a auditoria e
  a publicação faz a exportação recusar com `RegistrationRefused`.
- **`publish`** exige o par aprovado (ou `audit=False`, que fica no log); para cada tabela, cria
  (`create_table`) quando `versions[table]` é `None`, senão confere que nenhuma **alteração de
  dados** entrou desde a versão fixada: `delta.version_diff(uri, versions[table], atual)` vazio.
  Um avanço só de metadados ou de manutenção (`reconcile` de outra execução, `compact`, `vacuum`,
  que gravam commits) não é conflito, e `versions[table]` passa para a versão atual; um avanço com
  dados é `ExecutionConflict`, porque duas execuções abertas na mesma versão publicariam a mesma
  faixa de `next_ids`. Depois `reconcile`, e `sandbox.export_partition(table, uri, valor,
  commit_metadata(...), mode, expected_rows, columns_without_min_max)` por partição, com o
  `export_mode` resolvido em `mode` (`partitions=None` numa tabela sem partição é `[None]`) e, em
  `columns_without_min_max`, o `nonfinite_columns[valor]` da auditoria aprovada, ou todas as colunas
  `Double` da tabela com `audit=False`, e em `expected_rows` a contagem da auditoria, ou `None` com
  `audit=False`; as tabelas correm num
  `ThreadPoolExecutor(max_workers)` com a política de `publish_all` de `test_parallel.py`: na
  primeira falha nada novo começa, e a exceção lista o resultado por tabela. `versions[table]`
  avança sob lock a cada commit, e o dicionário devolvido é `{tabela: versão}`.
- **`publish_redshift`** é a [etapa 8](PLAN-STAGE-8.md), uma conexão por tabela no pool.
- **`snapshot`** marca o nome para `commit_metadata` dos commits seguintes e, no `__exit__`, grava
  `delta.snapshot(storage, environment, name, versions)` com todas as tabelas do ambiente.
- **`cli.main`** usa `argparse` com um subcomando por primitiva; `run` recebe `--root`,
  `--environment`, `--engine`, `--export-mode`, `--partition`, `--execution-id`, `--metadata
  modulo:atributo` (o `MetaData` dos modelos do pipeline) e o `modulo:funcao` do pipeline, com as
  variáveis `SERIALIZE_DB_*` como padrão, e `--export-mode` vai a `Execution(export_mode=...)`;
  `--partition` e `--execution-id` passam pela regra da partição como `type` do `argparse`, e o
  `String(n)` fica para `Execution`, que tem o modelo. O código de saída é 0, 1 em `AuditFailed`,
  2 em `ExecutionConflict` e no erro de uso do `argparse`.
  O `run` importa os módulos antes de abrir a execução, para a importação preguiçosa não correr ao
  lado das threads da biblioteca.

## Pré-requisitos e pós-condições

| Primitiva | Pré-requisitos | Pós-condições |
| --- | --- | --- |
| `Database` | Raiz alcançável; `metadata` aprovado por `check_models`. | Caminhos montados; ambiente normalizado. |
| `Execution.__enter__` | Partição e `execution_id` pela regra da partição, a partição dentro do `String(n)`; motor configurável. | `versions` com toda tabela do ambiente; o sandbox aberto; o log com as versões lidas. |
| `previous_partitions` | Tabela existente e particionada. | Os `n` últimos valores até a partição da execução, inclusive, em ordem crescente. |
| `next_ids` | Tabela com chave primária inteira de uma coluna; outra chave é `ContractError`. | Faixas contíguas e disjuntas entre threads, a primeira acima do máximo da versão fixada. |
| `audit` | Sandbox com a tabela. | O par aprovado registrado, ou `AuditFailed` com o relatório no log; o Delta intocado. |
| `publish` | Auditoria aprovada; nenhuma alteração de dados na tabela desde a versão fixada. | Uma versão por partição com os metadados, sem mínimo e máximo nas colunas `Double` com valor não finito; `versions` avançado; `ExecutionConflict` sem commit no conflito; na falha de uma tabela, as demais terminam ou são canceladas e a exceção lista cada resultado. |
| `__exit__` | Nenhum. | Sandbox descartado, staging apagado, resumo no log, snapshot gravado quando marcado. |
| `serialize-db run` | `--metadata` e o pipeline importáveis. | Código de saída pelo resultado; nenhum traceback no erro de uso. |

## Testes por caso

`tests/test_execution.py` sob a raiz local com o motor DuckDB e com um motor de mentira que registra
as chamadas.

| Caso | Teste | O que confere |
| --- | --- | --- |
| Abertura | `test_execution_opens_every_table_and_fixes_versions` | `versions` com as tabelas existentes e `None` nas ausentes; o log com as versões. |
| Partição | `test_execution_refuses_an_invalid_partition` | O valor vazio, com `/`, `=`, espaço, `'`, `:`, `%` ou acento, começado por `.`, `_` ou `-`, ou acima do `String(n)` da coluna de partição é recusado antes de o sandbox abrir, e o `execution_id` fora da regra também; `2026-08-31` e `2026-Q1` passam. |
| Partições anteriores | `test_previous_partitions_up_to_the_execution_partition` | Só valores até a partição da execução, os `n` últimos, em ordem. |
| Identificadores | `test_next_ids_are_disjoint_across_threads` | Duas threads, faixas disjuntas, o início acima do máximo lido das estatísticas; a tabela nova começa em 1; a chave composta é `ContractError`. |
| Auditoria exigida | `test_publish_requires_the_audit` | `publish` sem `audit` é `AuditFailed`; com `audit=False` passa e o log registra. |
| Reprovação | `test_failed_audit_leaves_the_delta_untouched` | A versão da tabela não muda; `__exit__` descarta o sandbox. |
| Reexecução | `test_rerun_with_the_same_execution_id_produces_the_same_rows` | As mesmas linhas, ids que podem diferir, uma versão a mais. |
| Conflito | `test_publish_aborts_when_data_changed_since_open` | Um `append` de outra execução depois da abertura é `ExecutionConflict`; um `compact` não é. |
| Mesma partição | `test_two_executions_on_the_same_partition_conflict` | A segunda aborta no `CommitFailedError`. |
| Paralelismo | `test_publish_with_two_workers_matches_one` | O mesmo resultado com `max_workers=1` e `2`; a falha de uma tabela cancela as não iniciadas e lista cada resultado. |
| `Double` não finito | `test_publish_passes_the_nonfinite_columns_to_the_export` | O motor de mentira recebe, por partição, as colunas `Double` com valor não finito que a auditoria contou, a partição sem elas recebe a lista vazia, e com `audit=False` recebe todas as colunas `Double`; no motor DuckDB, a partição com `NaN` sai sem o mínimo e o máximo da coluna. |
| Modo de exportação | `test_export_mode_resolution` | O argumento de `publish` vence o de `Execution`, que vence `SERIALIZE_DB_EXPORT_MODE`, que vence `"register"`; o motor de mentira recebe o modo resolvido, e o `--export-mode` do `run` chega a `Execution`. |
| Metadados | `test_commit_metadata_in_history` | `serialize_db_execution_id` e `serialize_db_input_versions` no `history`; `serialize_db_snapshot` só na execução marcada. |
| Snapshot | `test_snapshot_writes_the_control_file_at_exit` | Todas as tabelas do ambiente na entrada, gravada uma vez. |
| Linha de comando | `test_cli_run_parses_and_exits_by_result` | `--partition` e `--execution-id` fora da regra saem com 2; `--metadata` obrigatório; `AuditFailed` sai com 1; `ExecutionConflict` com 2. |

## Rascunhos executados

O rascunho rodou em 2026-09-21 com as versões fixadas, com um motor de mentira que grava uma linha
por partição e um Delta local com três partições, e de novo em 2026-09-22, com a partição validada
como texto: a linha de comando recusa o valor vazio ou com `/`, `=` ou espaço, e `Execution` recusa
também o que passa do `String(n)` da coluna de partição, lida por `table_options` da
[etapa 1](PLAN-STAGE-1.md). O `ingest` dele despacha as tabelas num pool de
`max_workers` sobre a mesma sessão; a implementação dá a cada tabela uma sessão a mais e dispensa o
argumento.

```python
"""Etapa 6: Database e Execution sobre um motor de mentira: versões fixadas, partições anteriores, next_ids sob lock, o conflito de versão e a linha de comando."""
import argparse
import dataclasses
import json
import logging
import os
import re
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor

import pyarrow as pa
import pyarrow.compute as pc
import sqlalchemy as sa
from deltalake import CommitProperties, DeltaTable, Schema as DeltaSchema, write_deltalake

from serialize_db.schema import table_options

log = logging.getLogger("serialize_db.execution")
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")


class ExecutionConflict(Exception):
    pass


class AuditFailed(Exception):
    pass


@dataclasses.dataclass(frozen=True)
class Database:
    root: str
    environment: str
    metadata: sa.MetaData

    def uri(self, table: sa.Table) -> str:
        return f"{self.root.rstrip('/')}/{self.environment}/{table.name}"

    def tables(self) -> list[sa.Table]:
        return list(self.metadata.sorted_tables)


def partition_problem(value: str) -> str | None:
    """O motivo de recusa do valor de partição, ou None: texto não vazio e sem /, = nem espaço, os caracteres que o nome da pasta e o manifesto não aceitam."""
    if not value:
        return "vazia"
    if re.search(r"[/=\s]", value):
        return "com /, = ou espaço"
    return None


def partition_length_problem(value: str, db: Database) -> str | None:
    """O String(n) de uma coluna de partição do modelo que o valor excede, medido em bytes, ou None."""
    for table in db.tables():
        column = table_options(table).partition_by
        if column and len(value.encode()) > table.c[column].type.length:
            return f"acima de {table.c[column].type} em {table.name}.{column}"
    return None


class FakeEngine:
    def __init__(self):
        self.calls, self.audited = [], set()

    def ingest(self, table, uri, version, partitions=None, materialize=False):
        self.calls.append(("ingest", table.name, version, partitions))

    def audit(self, table, partitions, **options):
        self.audited.add((table.name, tuple(partitions)))
        return True

    def export_partition(self, table, uri, value, metadata, mode):
        self.calls.append(("export", table.name, value, mode))
        write_deltalake(uri, pa.table({"id_lancamento": pa.array([1], pa.int64()), "data_base_str": [value]}), mode="overwrite", predicate=f"data_base_str = '{value}'",
                        commit_properties=CommitProperties(custom_metadata=metadata))
        return DeltaTable(uri).version()

    def cleanup(self):
        self.calls.append(("cleanup",))


class Execution:
    def __init__(self, db: Database, engine, partition: str, execution_id: str) -> None:
        problem = partition_problem(partition) or partition_length_problem(partition, db)
        if problem:
            raise ValueError(f"partição {partition!r} recusada: {problem}")
        self.db, self.sandbox, self.partition, self.execution_id = db, engine, partition, execution_id
        self.versions: dict[str, int | None] = {}
        self._tables: dict[str, DeltaTable] = {}
        self._lock = threading.Lock()
        self._next: dict[str, int] = {}

    def __enter__(self):
        for table in self.db.tables():                                   # toda tabela do ambiente, aberta uma vez e presa à versão
            uri = self.db.uri(table)
            if DeltaTable.is_deltatable(uri):
                self._tables[table.name] = DeltaTable(uri)
                self.versions[table.name] = self._tables[table.name].version()
            else:
                self.versions[table.name] = None                         # nasce em publish
        log.info("execução %s aberta: partição %s, versões %s", self.execution_id, self.partition, self.versions)
        return self

    def __exit__(self, *exc):
        self.sandbox.cleanup()
        log.info("execução %s encerrada: versões %s", self.execution_id, self.versions)

    def previous_partitions(self, table: sa.Table, n: int) -> list[str]:
        column = pa.table(self._tables[table.name].get_add_actions(flatten=True)).column("partition.data_base_str")   # arro3 -> pyarrow pelo PyCapsule
        values = sorted({v for v in column.to_pylist() if v <= self.partition})
        return values[-n:]

    def ingest(self, *tables, partitions=None, materialize=False, max_workers=1):
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            list(pool.map(lambda t: self.sandbox.ingest(t, self.db.uri(t), self.versions[t.name], partitions, materialize), tables))

    def next_ids(self, table: sa.Table, n: int) -> range:
        with self._lock:
            if table.name not in self._next:
                actions = pa.table(self._tables[table.name].get_add_actions(flatten=True)) if table.name in self._tables else None
                self._next[table.name] = (pc.max(actions.column("max.id_lancamento")).as_py() if actions is not None and actions.num_rows else 0) + 1
            start = self._next[table.name]
            self._next[table.name] += n
            return range(start, start + n)

    def audit(self, table, partitions, **options):
        if not self.sandbox.audit(table, partitions, **options):
            raise AuditFailed(table.name)

    def publish(self, *tables, partitions, audit=True, export_mode="register"):
        metadata = {"serialize_db_execution_id": self.execution_id, "serialize_db_input_versions": json.dumps({k: v for k, v in self.versions.items() if v is not None}, sort_keys=True)}
        for table in tables:
            if audit and any((table.name, tuple(partitions)) not in self.sandbox.audited for _ in [0]):
                raise AuditFailed(f"{table.name}: publish exige a auditoria aprovada em {partitions}")
            uri = self.db.uri(table)
            if self.versions[table.name] is None:
                DeltaTable.create(uri, DeltaSchema.from_arrow(pa.schema([pa.field("id_lancamento", pa.int64()), pa.field("data_base_str", pa.string())])), partition_by=["data_base_str"])
                self.versions[table.name] = 0
            elif DeltaTable(uri).version() != self.versions[table.name]:
                raise ExecutionConflict(f"{table.name} está na versão {DeltaTable(uri).version()}, a execução fixou {self.versions[table.name]}")
            for value in partitions:
                with self._lock:
                    self.versions[table.name] = self.sandbox.export_partition(table, uri, value, metadata, export_mode)


metadata = sa.MetaData()
partitioned = {"serialize_db": {"partition_by": ["data_base_str"]}}
entries = sa.Table("cad_lancamentos", metadata, sa.Column("id_lancamento", sa.BigInteger), sa.Column("data_base_str", sa.String(10)), info=partitioned)
projected = sa.Table("cad_lancamentos_projetados", metadata, sa.Column("id_lancamento", sa.BigInteger), sa.Column("data_base_str", sa.String(10)), info=partitioned)

with tempfile.TemporaryDirectory() as folder:
    db = Database(folder, "prod", metadata)
    write_deltalake(db.uri(entries), pa.table({"id_lancamento": pa.array([10, 20, 30], pa.int64()), "data_base_str": ["2026-06-30", "2026-07-31", "2026-09-30"]}), partition_by=["data_base_str"])
    with Execution(db, FakeEngine(), "2026-08-31", "exec-2026-09-05") as run:
        print("previous_partitions:", run.previous_partitions(entries, 12))
        run.ingest(entries, projected, partitions=run.previous_partitions(entries, 2), max_workers=2)
        ranges = []
        with ThreadPoolExecutor(2) as pool:
            ranges = list(pool.map(lambda k: run.next_ids(entries, 5), range(4)))
        print("next_ids:", [(r.start, r.stop) for r in ranges], "| disjuntas:", len(set().union(*ranges)) == 20, "| começa depois do máximo lido:", min(r.start for r in ranges) == 31)
        try:
            run.publish(projected, partitions=["2026-08-31"])
        except AuditFailed as error:
            print("publish sem auditoria:", error)
        run.audit(projected, ["2026-08-31"])
        run.publish(projected, partitions=["2026-08-31"])
        print("versões após publish:", run.versions, "| metadados do commit:", DeltaTable(db.uri(projected)).history(1)[0]["serialize_db_input_versions"])
        write_deltalake(db.uri(entries), pa.table({"id_lancamento": pa.array([40], pa.int64()), "data_base_str": ["2026-08-31"]}), mode="append")  # outra execução avança
        try:
            run.audit(entries, ["2026-08-31"]); run.publish(entries, partitions=["2026-08-31"])
        except ExecutionConflict as error:
            print("conflito:", error)
    print("chamadas ao motor:", run.sandbox.calls[-3:])
    try:
        Execution(db, FakeEngine(), "2026-08-31-extra", "exec-2026-09-06")
    except ValueError as error:
        print("execução recusada:", error)


def partition_argument(text: str) -> str:
    problem = partition_problem(text)                                    # o String(n) fica para Execution, que tem o modelo
    if problem:
        raise argparse.ArgumentTypeError(f"partição inválida: {text!r} ({problem})")
    return text


parser = argparse.ArgumentParser(prog="serialize-db")
commands = parser.add_subparsers(dest="command", required=True)
run_command = commands.add_parser("run")
run_command.add_argument("--root", default=os.environ.get("SERIALIZE_DB_ROOT"), required="SERIALIZE_DB_ROOT" not in os.environ)
run_command.add_argument("--environment", default=os.environ.get("SERIALIZE_DB_ENVIRONMENT", "dev"))
run_command.add_argument("--engine", choices=["duckdb", "redshift"], default=os.environ.get("SERIALIZE_DB_ENGINE", "duckdb"))
run_command.add_argument("--export-mode", choices=["register", "rewrite"], default=os.environ.get("SERIALIZE_DB_EXPORT_MODE", "register"))
run_command.add_argument("--partition", type=partition_argument, required=True)
run_command.add_argument("--execution-id", default=None)
run_command.add_argument("--metadata", required=True, help="modulo:atributo com o MetaData dos modelos do pipeline")
run_command.add_argument("pipeline", help="modulo:funcao que recebe a execução aberta")
args = parser.parse_args(["run", "--root", "s3://bucket/projeto/delta", "--partition", "2026-08-31", "--metadata", "pipeline.models:Base.metadata", "pipeline:main"])
print(vars(args))
print("partição de texto aceita:", parser.parse_args(["run", "--root", "x", "--partition", "2026-Q1", "--metadata", "m:a", "p:f"]).partition)
try:
    parser.parse_args(["run", "--root", "x", "--partition", "2026/08/31", "--metadata", "m:a", "p:f"])
except SystemExit as exit_code:
    print("partição inválida sai com código", exit_code.code)
```

Saída:

```
INFO serialize_db.execution: execução exec-2026-09-05 aberta: partição 2026-08-31, versões {'cad_lancamentos': 0, 'cad_lancamentos_projetados': None}
INFO serialize_db.execution: execução exec-2026-09-05 encerrada: versões {'cad_lancamentos': 0, 'cad_lancamentos_projetados': 1}
usage: serialize-db run [-h] --root ROOT [--environment ENVIRONMENT]
                        [--engine {duckdb,redshift}]
                        [--export-mode {register,rewrite}]
                        --partition PARTITION [--execution-id EXECUTION_ID]
                        --metadata METADATA
                        pipeline
serialize-db run: error: argument --partition: partição inválida: '2026/08/31' (com /, = ou espaço)
previous_partitions: ['2026-06-30', '2026-07-31']
next_ids: [(31, 36), (46, 51), (36, 41), (41, 46)] | disjuntas: True | começa depois do máximo lido: True
publish sem auditoria: cad_lancamentos_projetados: publish exige a auditoria aprovada em ['2026-08-31']
versões após publish: {'cad_lancamentos': 0, 'cad_lancamentos_projetados': 1} | metadados do commit: {"cad_lancamentos": 0}
conflito: cad_lancamentos está na versão 1, a execução fixou 0
chamadas ao motor: [('ingest', 'cad_lancamentos_projetados', None, ['2026-06-30', '2026-07-31']), ('export', 'cad_lancamentos_projetados', '2026-08-31', 'register'), ('cleanup',)]
execução recusada: partição '2026-08-31-extra' recusada: acima de VARCHAR(10) em cad_lancamentos.data_base_str
{'command': 'run', 'root': 's3://bucket/projeto/delta', 'environment': 'dev', 'engine': 'duckdb', 'export_mode': 'register', 'partition': '2026-08-31', 'execution_id': None, 'metadata': 'pipeline.models:Base.metadata', 'pipeline': 'pipeline:main'}
partição de texto aceita: 2026-Q1
partição inválida sai com código 2
```

O rascunho confere a versão por igualdade e por isso o conflito dispara em qualquer avanço; a
implementação usa `version_diff`, pela regra da estratégia. O rascunho também recusa a partição por
uma lista de caracteres (`/`, `=`, espaço e o vazio); a implementação aceita só a regra da
partição, `[0-9A-Za-z][0-9A-Za-z_.-]*`, no valor e no `execution_id`.

## Decisões pendentes

Nenhuma. As decisões que o usuário tomou em 2026-09-23 estão escritas nas seções que as descrevem:
`--metadata modulo:atributo` no `serialize-db run`, como em `schema` e `sql`; `next_ids` só na
chave sequencial, a chave primária inteira de uma coluna, porque numa chave composta o cliente
decide os ids; e a regra da partição, `[0-9A-Za-z][0-9A-Za-z_.-]*`, no valor da partição e no
`execution_id`. A barreira por tabela não entra nas etapas: um `load` esquecido numa thread faz a
leitura concorrente falhar, porque o `loader` cria a tabela no `close` e recusa o nome ocupado nos
dois motores ([etapa 4](PLAN-STAGE-4.md), [etapa 5](PLAN-STAGE-5.md)).
