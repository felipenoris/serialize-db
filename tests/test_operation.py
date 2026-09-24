"""A operação da etapa 9: ``serialize-db snapshot``, ``vacuum``, ``compact``, ``archive``,
``export`` e ``history`` sobre as primitivas de ``serialize_db.delta``.

Os testes escrevem sob ``SERIALIZE_DB_TEST_LOCAL_ROOT`` (marcador ``local``): o banco de
``tests/lancamentos_model.py`` numa raiz por teste, com a pasta temporária do processo apontada
para a pasta do teste. Eles conferem o snapshot com a versão atual de cada tabela e o histórico
com os metadados da biblioteca; o ``vacuum`` que preserva a versão do snapshot e nada lista dentro
da retenção; a compactação recusada depois de um snapshot na versão atual e feita antes, com o
commit sem alteração de dados; o arquivo que copia cada tabela do snapshot com os mesmos arquivos e
as mesmas somas, move a entrada para ``archived``, solta a versão no ``vacuum`` e ocupa o nome; a
exportação por cópia e por reescrita, de uma versão antiga inclusive; e os erros de uso da linha
de comando.
"""

from __future__ import annotations

import re
import tempfile
import uuid
from pathlib import Path

import pyarrow as pa
import pytest
import sqlalchemy as sa
from deltalake import write_deltalake

from conftest import LocalLocation
from lancamentos_model import (
    ACCOUNTS,
    ENTRIES,
    MONTHS,
    PROJECTED,
    Base,
    account_rows,
    entry_rows,
)
from serialize_db import cli, delta
from serialize_db.execution import Database
from serialize_db.storage import Storage

pytestmark = pytest.mark.local

METADATA = "lancamentos_model:Base.metadata"


@pytest.fixture
def folder(local_location: LocalLocation, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Uma pasta nova por teste sob a raiz da sessão, que também recebe a pasta temporária do
    processo."""
    path = Path(local_location.child(f"operacao/{uuid.uuid4().hex[:8]}"))
    path.mkdir(parents=True)
    monkeypatch.setattr(tempfile, "tempdir", str(path))
    return path


@pytest.fixture
def db(folder: Path) -> Database:
    """O banco do teste: os lançamentos nas duas partições, versões 1 e 2, e as contas, versão 1,
    cada commit com o ``execution_id`` da biblioteca."""
    database = Database(str(folder / "delta"), "prd", Base.metadata)
    delta.create_table(database.uri(ENTRIES), ENTRIES, database.storage)
    for index, month in enumerate(MONTHS, start=1):
        publish(database, ENTRIES, month, entry_rows(month, 1 + index * 10, 10), f"exec-{index}")
    delta.create_table(database.uri(ACCOUNTS), ACCOUNTS, database.storage)
    publish(database, ACCOUNTS, None, account_rows(["A", "B", "C"]), "exec-0")
    return database


def publish(db: Database, table: sa.Table, value: str | None, data: pa.Table,
            execution_id: str) -> None:
    """Publica ``data`` na partição ``value`` num commit com o ``execution_id`` informado."""
    metadata = delta.commit_metadata(execution_id, {})
    delta.publish_partition(db.uri(table), table, value, data, metadata, db.storage)


def common_arguments(db: Database) -> list[str]:
    """Os argumentos comuns dos subcomandos da operação."""
    return ["--root", db.root, "--environment", "prd", "--metadata", METADATA]


def exit_code(arguments: list[str]) -> int | str | None:
    """O código de saída de ``main``, também quando o ``argparse`` encerra o processo."""
    try:
        return cli.main(arguments)
    except SystemExit as error:
        return error.code


def count_and_sum(db: Database, uri: str) -> tuple:
    """As linhas e a soma de ``valor`` de uma tabela pelo ``delta_scan``."""
    with db.storage.duckdb_connect() as connection:
        query = f"SELECT count(*), sum(valor) FROM delta_scan('{uri}')"
        return connection.execute(query).fetchone()


def files_of_version(db: Database, uri: str,
                     version: int | None = None) -> list[tuple[str, int]]:
    """O caminho relativo e o tamanho de cada arquivo de uma versão, em ordem."""
    table = delta.open_table(uri, db.storage, version=version)
    actions = pa.table(table.get_add_actions(flatten=True))
    paths = actions.column("path").to_pylist()
    sizes = actions.column("size_bytes").to_pylist()
    return sorted(zip(paths, sizes))


def test_snapshot_records_every_table_and_history_shows_the_metadata(
    db: Database, capsys: pytest.CaptureFixture
) -> None:
    """``snapshot`` grava a versão atual de cada tabela existente do ambiente e recusa o nome
    repetido; ``history`` lista os commits com os metadados da biblioteca, do mais recente ao mais
    antigo, e o ``CREATE TABLE`` sem eles."""
    assert cli.main(["snapshot", *common_arguments(db), "--name", "2026T3"]) == 0
    printed = capsys.readouterr().out
    assert "cad_lancamentos: versão 2" in printed
    assert "cad_contas: versão 1" in printed
    assert "snapshot 2026T3 gravado com 2 tabela(s)" in printed
    control, _ = delta.read_snapshots(db.storage, "prd")
    assert control["snapshots"] == {"2026T3": {"cad_contas": 1, "cad_lancamentos": 2}}
    assert cli.main(["snapshot", *common_arguments(db), "--name", "2026T3"]) == 2
    assert "já existe" in capsys.readouterr().err

    # O histórico, do commit mais recente ao CREATE TABLE.
    assert cli.main(["history", *common_arguments(db), "--table", "cad_lancamentos"]) == 0
    lines = capsys.readouterr().out.splitlines()
    assert lines[0].startswith("2 WRITE 20")
    assert "serialize_db_execution_id=exec-2" in lines[0]
    assert lines[-1].startswith("0 CREATE TABLE 20")
    assert "serialize_db" not in lines[-1]
    history = delta.history(db.uri(ENTRIES), db.storage)
    assert [entry["version"] for entry in history] == [2, 1, 0]
    assert history[0]["serialize_db_execution_id"] == "exec-2"
    assert history[0]["serialize_db_input_versions"] == "{}"
    assert history[0]["timestamp"].tzinfo is not None
    assert "serialize_db_execution_id" not in history[-1]


def test_vacuum_keeps_the_snapshot_version(db: Database, capsys: pytest.CaptureFixture) -> None:
    """Dentro da retenção nada é listado; com retenção zero, o arquivo que só a versão anterior ao
    snapshot usava é listado e, com ``--apply``, apagado, e a versão do snapshot continua legível
    enquanto a anterior perde o arquivo."""
    uri = db.uri(ENTRIES)
    storage = db.storage
    # Uma versão antes do snapshot e outra depois.
    publish(db, ENTRIES, MONTHS[0], entry_rows(MONTHS[0], 100, 10), "exec-3")
    assert cli.main(["snapshot", *common_arguments(db), "--name", "2026T3"]) == 0
    publish(db, ENTRIES, MONTHS[0], entry_rows(MONTHS[0], 200, 10), "exec-4")
    capsys.readouterr()  # descarta a saída do snapshot

    # Na retenção padrão, com retenção zero e com --apply.
    assert cli.main(["vacuum", *common_arguments(db)]) == 0
    assert "cad_lancamentos: 0 arquivo(s) a apagar" in capsys.readouterr().out
    assert cli.main(["vacuum", *common_arguments(db), "--retention-hours", "0"]) == 0
    printed = capsys.readouterr().out
    assert "cad_lancamentos: 1 arquivo(s) a apagar" in printed
    assert "cad_contas: 0 arquivo(s)" in printed
    assert delta.open_table(uri, storage, version=1).to_pyarrow_dataset().count_rows() == 10
    assert cli.main(["vacuum", *common_arguments(db), "--retention-hours", "0", "--apply"]) == 0
    assert "cad_lancamentos: 1 arquivo(s) apagado(s)" in capsys.readouterr().out
    assert delta.open_table(uri, storage, version=3).to_pyarrow_dataset().count_rows() == 20
    with pytest.raises(FileNotFoundError):
        delta.open_table(uri, storage, version=1).to_pyarrow_dataset().to_table()


def test_compact_refuses_after_a_snapshot_on_the_current_version(
    db: Database, capsys: pytest.CaptureFixture
) -> None:
    """Três arquivos pequenos numa partição: a compactação é recusada enquanto o snapshot está na
    versão atual, e feita depois de uma versão nova, num commit sem alteração de dados; a tabela
    particionada sem ``--partitions`` e a tabela ausente são erros de uso."""
    uri = db.uri(PROJECTED)
    storage = db.storage
    delta.create_table(uri, PROJECTED, storage)
    publish(db, PROJECTED, MONTHS[0], entry_rows(MONTHS[0], 1, 10, PROJECTED), "exec-1")
    for start in (100, 200, 300):
        small_file = entry_rows(MONTHS[1], start, 5, PROJECTED)
        write_deltalake(delta.open_table(uri, storage), small_file, mode="append")
    assert cli.main(["snapshot", *common_arguments(db), "--name", "2026T3"]) == 0
    capsys.readouterr()  # descarta a saída do snapshot
    compact = ["compact", *common_arguments(db), "--table", "cad_lancamentos_projetados"]
    assert cli.main([*compact, "--partitions", MONTHS[1]]) == 2
    assert "o snapshot 2026T3 está na versão atual 4" in capsys.readouterr().err

    # Depois de uma versão nova, a compactação num commit sem alteração de dados.
    publish(db, PROJECTED, MONTHS[0], entry_rows(MONTHS[0], 50, 10, PROJECTED), "exec-2")
    assert cli.main([*compact, "--partitions", MONTHS[1]]) == 0
    assert re.search(r"1 arquivo\(s\) gravado\(s\), 3 removido\(s\), em \d+\.\d s; "
                     r"RSS máximo do processo \d+ MB", capsys.readouterr().out)
    current = delta.open_table(uri, storage).version()
    assert current == 6
    assert delta.version_diff(uri, 5, 6, PROJECTED, storage) == set()
    assert delta.history(uri, storage)[0]["operation"] == "OPTIMIZE"
    row_count, _ = count_and_sum(db, uri)
    assert row_count == 25

    # A tabela particionada sem --partitions.
    assert cli.main(compact) == 2
    assert "informe --partitions" in capsys.readouterr().err

    # A tabela sem partição: recusada enquanto o snapshot está na versão atual dela, e sem commit
    # depois, porque tem um arquivo só.
    accounts_compact = ["compact", *common_arguments(db), "--table", "cad_contas"]
    assert cli.main(accounts_compact) == 2
    assert "de cad_contas" in capsys.readouterr().err
    publish(db, ACCOUNTS, None, account_rows(["A", "B", "C", "D"]), "exec-5")
    assert cli.main(accounts_compact) == 0
    assert "cad_contas: 0 arquivo(s) gravado(s), 0 removido(s)" in capsys.readouterr().out
    assert delta.open_table(db.uri(ACCOUNTS), storage).version() == 2

    # A tabela ausente.
    assert cli.main(["compact", *common_arguments(db), "--table", "nada"]) == 2


def test_archive_copies_each_table_with_the_same_sums(db: Database, capsys: pytest.CaptureFixture,
                                                      monkeypatch: pytest.MonkeyPatch) -> None:
    """``archive`` copia cada tabela do snapshot na versão registrada, com os mesmos arquivos e as
    mesmas somas e uma versão por partição, move a entrada para ``archived``, solta a versão no
    ``vacuum``, ocupa o nome em ``snapshot``, recusa o snapshot com uma tabela que já não existe e,
    repetido depois de uma interrupção, continua a cópia de onde ela parou."""
    storage = db.storage
    entries_uri = db.uri(ENTRIES)
    # O snapshot 2026T3 e uma versão depois dele.
    assert cli.main(["snapshot", *common_arguments(db), "--name", "2026T3"]) == 0
    before = count_and_sum(db, entries_uri)
    publish(db, ENTRIES, MONTHS[0], entry_rows(MONTHS[0], 100, 10), "exec-3")
    capsys.readouterr()  # descarta a saída do snapshot

    # O arquivamento: a saída, as cópias e a entrada movida.
    assert cli.main(["archive", *common_arguments(db), "--name", "2026T3"]) == 0
    printed = capsys.readouterr().out
    assert re.search(r"cad_lancamentos: versão 2 copiada para .*, versão 2 no arquivo, "
                     r"em \d+\.\d s; RSS máximo do processo \d+ MB", printed)
    assert "cad_contas: versão 1 copiada para" in printed
    assert "versão 1 no arquivo" in printed
    assert "snapshot 2026T3 movido para archived" in printed

    archived_entries_uri = storage.uri_of("prd/arquivo/2026T3/cad_lancamentos")
    archived_accounts_uri = storage.uri_of("prd/arquivo/2026T3/cad_contas")
    archived_entries = delta.open_table(archived_entries_uri, storage)
    assert archived_entries.version() == 2
    assert delta.open_table(archived_accounts_uri, storage).version() == 1
    assert count_and_sum(db, archived_entries_uri) == before
    archived_files = files_of_version(db, archived_entries_uri)
    assert archived_files == files_of_version(db, entries_uri, version=2)
    accounts_files = files_of_version(db, db.uri(ACCOUNTS))
    assert files_of_version(db, archived_accounts_uri) == accounts_files
    copied = pa.schema(archived_entries.schema())
    assert not copied.field("id_lancamento").nullable
    assert archived_entries.metadata().name == "cad_lancamentos"
    control, _ = delta.read_snapshots(storage, "prd")
    assert control["snapshots"] == {}
    assert control["archived"] == {"2026T3": {"cad_contas": 1, "cad_lancamentos": 2}}

    # A versão arquivada não prende mais o vacuum, e o nome fica ocupado.
    assert cli.main(["vacuum", *common_arguments(db), "--retention-hours", "0"]) == 0
    assert "cad_lancamentos: 1 arquivo(s) a apagar" in capsys.readouterr().out
    assert cli.main(["snapshot", *common_arguments(db), "--name", "2026T3"]) == 2
    assert "já existe" in capsys.readouterr().err
    assert cli.main(["archive", *common_arguments(db), "--name", "2026T3"]) == 2
    assert "não está em snapshots" in capsys.readouterr().err

    # Um snapshot com uma tabela que já não existe na raiz é recusado antes de qualquer cópia.
    delta.snapshot(storage, "prd", "2026T0", {"cad_contas": 1, "sumida": 4})
    assert cli.main(["archive", *common_arguments(db), "--name", "2026T0"]) == 2
    assert "sumida não existe em" in capsys.readouterr().err
    control, _ = delta.read_snapshots(storage, "prd")
    assert control["snapshots"] == {"2026T0": {"cad_contas": 1, "sumida": 4}}
    assert not Path(storage.uri_of("prd/arquivo/2026T0")).exists()

    # Um arquivamento interrompido na terceira cópia, o segundo arquivo de cad_lancamentos, deixa
    # cad_contas inteira e uma partição registrada; a repetição pula as duas, copia a que falta e
    # move a entrada.
    delta.snapshot(storage, "prd", "2026T4", {"cad_contas": 1, "cad_lancamentos": 3})
    copies = []
    original_copy = Storage.copy

    def copy_until_the_third(self: Storage, source: str, destination: str) -> None:
        copies.append(destination)
        if len(copies) == 3:
            raise OSError("cópia interrompida")
        original_copy(self, source, destination)

    # O cli.main abre o próprio Storage: a falha da terceira cópia entra pela classe.
    with monkeypatch.context() as patch:
        patch.setattr(Storage, "copy", copy_until_the_third)
        with pytest.raises(OSError, match="cópia interrompida"):
            cli.main(["archive", *common_arguments(db), "--name", "2026T4"])
    resumed_entries_uri = storage.uri_of("prd/arquivo/2026T4/cad_lancamentos")
    assert delta.open_table(resumed_entries_uri, storage).version() == 1
    control, _ = delta.read_snapshots(storage, "prd")
    assert "2026T4" in control["snapshots"]
    capsys.readouterr()  # descarta a saída do arquivamento interrompido

    # A repetição.
    assert cli.main(["archive", *common_arguments(db), "--name", "2026T4"]) == 0
    printed = capsys.readouterr().out
    assert "cad_contas: versão 1 copiada" in printed
    assert "cad_lancamentos: versão 3 copiada" in printed
    assert count_and_sum(db, resumed_entries_uri) == count_and_sum(db, entries_uri)
    resumed_files = files_of_version(db, resumed_entries_uri)
    assert resumed_files == files_of_version(db, entries_uri, version=3)
    resumed_accounts_uri = storage.uri_of("prd/arquivo/2026T4/cad_contas")
    assert files_of_version(db, resumed_accounts_uri) == accounts_files
    control, _ = delta.read_snapshots(storage, "prd")
    assert control["archived"]["2026T4"] == {"cad_contas": 1, "cad_lancamentos": 3}


def test_export_by_copy_and_by_rewrite(db: Database, capsys: pytest.CaptureFixture) -> None:
    """``export`` grava as pastas ``<coluna>=<valor>/`` por cópia e por reescrita, de uma versão
    antiga inclusive, e recusa o destino não vazio e o destino fora da raiz."""
    storage = db.storage
    # Por cópia, por reescrita e de uma versão antiga.
    export = ["export", *common_arguments(db), "--table", "cad_lancamentos", "--destination"]
    copy_uri = storage.uri_of("prd/exportacao/copia")
    assert cli.main([*export, copy_uri]) == 0
    assert re.search(r"cad_lancamentos: 2 arquivo\(s\) em .*, em \d+\.\d s; "
                     r"RSS máximo do processo \d+ MB", capsys.readouterr().out)
    rewrite_uri = storage.uri_of("prd/exportacao/reescrita")
    assert cli.main([*export, rewrite_uri, "--mode", "rewrite"]) == 0
    assert cli.main([*export, storage.uri_of("prd/exportacao/antiga"), "--version", "1"]) == 0
    assert "1 arquivo(s) em" in capsys.readouterr().out.splitlines()[-1]
    for folder_name in ("copia", "reescrita"):
        files = storage.list_files(f"prd/exportacao/{folder_name}", ".parquet")
        assert len(files) == 2
        assert all("/data_base_str=2026-0" in path for path in files)
    with storage.duckdb_connect() as connection:
        query = (f"SELECT count(*), sum(valor) FROM read_parquet('{copy_uri}/*/*.parquet', "
                 "hive_partitioning = true, hive_types_autocast = false)")
        assert connection.execute(query).fetchone() == count_and_sum(db, db.uri(ENTRIES))

    # O destino não vazio e o destino fora da raiz.
    assert cli.main([*export, copy_uri]) == 2
    assert "destino não vazio" in capsys.readouterr().err
    assert cli.main([*export, "/fora/da/raiz"]) == 2
    assert "fora da raiz" in capsys.readouterr().err


def test_empty_environment_variable_counts_as_absent(db: Database, capsys: pytest.CaptureFixture,
                                                     monkeypatch: pytest.MonkeyPatch) -> None:
    """``SERIALIZE_DB_ENVIRONMENT`` vazia conta como ausente, como em ``run``, ``audit`` e
    ``publish``: o ambiente é ``dsv``, e não um erro de uso."""
    monkeypatch.setenv("SERIALIZE_DB_ENVIRONMENT", "")
    arguments = ["snapshot", "--root", db.root, "--metadata", METADATA, "--name", "2026T3"]
    assert cli.main(arguments) == 0
    assert "snapshot 2026T3 gravado com 0 tabela(s)" in capsys.readouterr().out
    control, _ = delta.read_snapshots(db.storage, "dsv")
    assert control["snapshots"] == {"2026T3": {}}


def test_cli_operation_usage_errors(db: Database, capsys: pytest.CaptureFixture) -> None:
    """Os erros de uso: o nome ausente, o modo desconhecido, a tabela fora do modelo e a tabela
    do modelo sem Delta, sem traceback."""
    assert exit_code(["snapshot", *common_arguments(db)]) == 2
    assert exit_code(["export", *common_arguments(db), "--table", "cad_contas",
                      "--destination", "x", "--mode", "outro"]) == 2
    assert cli.main(["history", *common_arguments(db), "--table", "nada"]) == 2
    projected_history = ["history", *common_arguments(db), "--table", "cad_lancamentos_projetados"]
    assert cli.main(projected_history) == 2
    assert cli.main(["export", *common_arguments(db), "--table", "cad_lancamentos_projetados",
                     "--destination", db.storage.uri_of("prd/exportacao/x")]) == 2
    printed_errors = capsys.readouterr().err
    assert "não existe" in printed_errors
    assert "Traceback" not in printed_errors
