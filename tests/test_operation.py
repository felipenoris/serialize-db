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

import tempfile
import uuid
from pathlib import Path

import pyarrow as pa
import pytest
from deltalake import write_deltalake

from conftest import LocalLocation
from lancamentos_model import ACCOUNTS, ENTRIES, MONTHS, PROJECTED, Base, accounts, entries
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
    monkeypatch.delenv("SERIALIZE_DB_ROOT", raising=False)
    return path


@pytest.fixture
def db(folder: Path) -> Database:
    """O banco do teste: os lançamentos nas duas partições, versões 1 e 2, e as contas, versão 1,
    cada commit com o ``execution_id`` da biblioteca."""
    database = Database(str(folder / "delta"), "prod", Base.metadata)
    storage = database.storage
    entries_uri = database.uri(ENTRIES)
    delta.create_table(entries_uri, ENTRIES, storage)
    for index, month in enumerate(MONTHS, 1):
        delta.publish_partition(entries_uri, ENTRIES, month, entries(month, 1 + index * 10, 10),
                                delta.commit_metadata(f"exec-{index}", {}), storage)
    accounts_uri = database.uri(ACCOUNTS)
    delta.create_table(accounts_uri, ACCOUNTS, storage)
    delta.publish_partition(accounts_uri, ACCOUNTS, None, accounts(["A", "B", "C"]),
                            delta.commit_metadata("exec-0", {}), storage)
    return database


def common(db: Database) -> list[str]:
    """Os argumentos comuns dos subcomandos da operação."""
    return ["--root", db.root, "--environment", "prod", "--metadata", METADATA]


def exit_code(arguments: list[str]) -> int | str | None:
    """O código de saída de ``main``, também quando o ``argparse`` encerra o processo."""
    try:
        return cli.main(arguments)
    except SystemExit as error:
        return error.code


def measures(db: Database, uri: str) -> tuple:
    """As linhas e a soma de ``valor`` de uma tabela pelo ``delta_scan``."""
    with db.storage.duckdb_connect() as connection:
        query = f"SELECT count(*), sum(valor) FROM delta_scan('{uri}')"
        return connection.execute(query).fetchone()


def add_paths(db: Database, uri: str, version: int | None = None) -> list[tuple[str, int]]:
    """O caminho relativo e o tamanho de cada arquivo de uma versão, em ordem."""
    actions = pa.table(delta.open_table(uri, db.storage, version).get_add_actions(flatten=True))
    paths = actions.column("path").to_pylist()
    return sorted(zip(paths, actions.column("size_bytes").to_pylist()))


def test_snapshot_records_every_table_and_history_shows_the_metadata(
    db: Database, capsys: pytest.CaptureFixture
) -> None:
    """``snapshot`` grava a versão atual de cada tabela existente do ambiente e recusa o nome
    repetido; ``history`` lista os commits com os metadados da biblioteca, do mais recente ao mais
    antigo, e o ``CREATE TABLE`` sem eles."""
    assert cli.main(["snapshot", *common(db), "--name", "2026T3"]) == 0
    out = capsys.readouterr().out
    assert "cad_lancamentos: versão 2" in out and "cad_contas: versão 1" in out
    assert "snapshot 2026T3 gravado com 2 tabela(s)" in out
    control, _ = delta.read_snapshots(db.storage, "prod")
    assert control["snapshots"] == {"2026T3": {"cad_contas": 1, "cad_lancamentos": 2}}
    assert cli.main(["snapshot", *common(db), "--name", "2026T3"]) == 2
    assert "já existe" in capsys.readouterr().err

    assert cli.main(["history", *common(db), "--table", "cad_lancamentos"]) == 0
    lines = capsys.readouterr().out.splitlines()
    assert lines[0].startswith("2 WRITE 20") and "serialize_db_execution_id=exec-2" in lines[0]
    assert lines[-1].startswith("0 CREATE TABLE 20") and "serialize_db" not in lines[-1]
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
    delta.publish_partition(uri, ENTRIES, MONTHS[0], entries(MONTHS[0], 100, 10),
                            delta.commit_metadata("exec-3", {}), storage)
    assert cli.main(["snapshot", *common(db), "--name", "2026T3"]) == 0
    delta.publish_partition(uri, ENTRIES, MONTHS[0], entries(MONTHS[0], 200, 10),
                            delta.commit_metadata("exec-4", {}), storage)
    capsys.readouterr()

    assert cli.main(["vacuum", *common(db)]) == 0
    assert "cad_lancamentos: 0 arquivo(s) a apagar" in capsys.readouterr().out
    assert cli.main(["vacuum", *common(db), "--retention-hours", "0"]) == 0
    out = capsys.readouterr().out
    assert "cad_lancamentos: 1 arquivo(s) a apagar" in out and "cad_contas: 0 arquivo(s)" in out
    assert delta.open_table(uri, storage, 1).to_pyarrow_dataset().count_rows() == 10
    assert cli.main(["vacuum", *common(db), "--retention-hours", "0", "--apply"]) == 0
    assert "cad_lancamentos: 1 arquivo(s) apagado(s)" in capsys.readouterr().out
    assert delta.open_table(uri, storage, 3).to_pyarrow_dataset().count_rows() == 20
    with pytest.raises(Exception):  # noqa: B017 - o erro vem do leitor de arquivos
        delta.open_table(uri, storage, 1).to_pyarrow_dataset().to_table()


def test_compact_refuses_after_a_snapshot_on_the_current_version(
    db: Database, capsys: pytest.CaptureFixture
) -> None:
    """Três arquivos pequenos numa partição: a compactação é recusada enquanto o snapshot está na
    versão atual, e feita depois de uma versão nova, num commit sem alteração de dados; a tabela
    particionada sem ``--partitions`` e a tabela ausente são erros de uso."""
    uri = db.uri(PROJECTED)
    storage = db.storage
    delta.create_table(uri, PROJECTED, storage)
    delta.publish_partition(uri, PROJECTED, MONTHS[0], entries(MONTHS[0], 1, 10, PROJECTED),
                            delta.commit_metadata("exec-1", {}), storage)
    for start in (100, 200, 300):
        write_deltalake(delta.open_table(uri, storage), entries(MONTHS[1], start, 5, PROJECTED),
                        mode="append")
    assert cli.main(["snapshot", *common(db), "--name", "2026T3"]) == 0
    capsys.readouterr()
    compact = ["compact", *common(db), "--table", "cad_lancamentos_projetados"]
    assert cli.main([*compact, "--partitions", MONTHS[1]]) == 2
    assert "o snapshot 2026T3 está na versão atual 4" in capsys.readouterr().err

    delta.publish_partition(uri, PROJECTED, MONTHS[0], entries(MONTHS[0], 50, 10, PROJECTED),
                            delta.commit_metadata("exec-2", {}), storage)
    assert cli.main([*compact, "--partitions", MONTHS[1]]) == 0
    assert "1 arquivo(s) gravado(s), 3 removido(s)" in capsys.readouterr().out
    current = delta.open_table(uri, storage).version()
    assert current == 6
    assert delta.version_diff(uri, 5, 6, PROJECTED, storage) == set()
    assert delta.history(uri, storage)[0]["operation"] == "OPTIMIZE"
    assert measures(db, uri)[0] == 25

    assert cli.main(compact) == 2
    assert "informe --partitions" in capsys.readouterr().err
    # A tabela sem partição: recusada enquanto o snapshot está na versão atual dela, e sem commit
    # depois, porque tem um arquivo só.
    assert cli.main(["compact", *common(db), "--table", "cad_contas"]) == 2
    assert "de cad_contas" in capsys.readouterr().err
    accounts_uri = db.uri(ACCOUNTS)
    delta.publish_partition(accounts_uri, ACCOUNTS, None, accounts(["A", "B", "C", "D"]),
                            delta.commit_metadata("exec-5", {}), storage)
    assert cli.main(["compact", *common(db), "--table", "cad_contas"]) == 0
    assert "cad_contas: 0 arquivo(s) gravado(s), 0 removido(s)" in capsys.readouterr().out
    assert delta.open_table(accounts_uri, storage).version() == 2
    assert cli.main(["compact", *common(db), "--table", "nada"]) == 2


def test_archive_copies_each_table_with_the_same_sums(db: Database, capsys: pytest.CaptureFixture,
                                                      monkeypatch: pytest.MonkeyPatch) -> None:
    """``archive`` copia cada tabela do snapshot na versão registrada, com os mesmos arquivos e as
    mesmas somas e uma versão por partição, move a entrada para ``archived``, solta a versão no
    ``vacuum``, ocupa o nome em ``snapshot``, recusa o snapshot com uma tabela que já não existe e,
    repetido depois de uma interrupção, continua a cópia de onde ela parou."""
    storage = db.storage
    entries_uri = db.uri(ENTRIES)
    assert cli.main(["snapshot", *common(db), "--name", "2026T3"]) == 0
    before = measures(db, entries_uri)
    delta.publish_partition(entries_uri, ENTRIES, MONTHS[0], entries(MONTHS[0], 100, 10),
                            delta.commit_metadata("exec-3", {}), storage)
    capsys.readouterr()

    assert cli.main(["archive", *common(db), "--name", "2026T3"]) == 0
    out = capsys.readouterr().out
    assert "cad_lancamentos: versão 2 copiada para" in out and "versão 2 no arquivo" in out
    assert "cad_contas: versão 1 copiada para" in out and "versão 1 no arquivo" in out
    assert "snapshot 2026T3 movido para archived" in out

    archived_entries = storage.uri_of("prod/arquivo/2026T3/cad_lancamentos")
    archived_accounts = storage.uri_of("prod/arquivo/2026T3/cad_contas")
    assert delta.open_table(archived_entries, storage).version() == 2
    assert delta.open_table(archived_accounts, storage).version() == 1
    assert measures(db, archived_entries) == before
    assert add_paths(db, archived_entries) == add_paths(db, entries_uri, 2)
    assert add_paths(db, archived_accounts) == add_paths(db, db.uri(ACCOUNTS))
    copied = pa.schema(delta.open_table(archived_entries, storage).schema())
    assert not copied.field("id_lancamento").nullable
    assert delta.open_table(archived_entries, storage).metadata().name == "cad_lancamentos"
    control, _ = delta.read_snapshots(storage, "prod")
    assert control["snapshots"] == {}
    assert control["archived"] == {"2026T3": {"cad_contas": 1, "cad_lancamentos": 2}}

    # A versão arquivada não prende mais o vacuum, e o nome fica ocupado.
    assert cli.main(["vacuum", *common(db), "--retention-hours", "0"]) == 0
    assert "cad_lancamentos: 1 arquivo(s) a apagar" in capsys.readouterr().out
    assert cli.main(["snapshot", *common(db), "--name", "2026T3"]) == 2
    assert "já existe" in capsys.readouterr().err
    assert cli.main(["archive", *common(db), "--name", "2026T3"]) == 2
    assert "não está em snapshots" in capsys.readouterr().err

    # Um snapshot com uma tabela que já não existe na raiz é recusado antes de qualquer cópia.
    delta.snapshot(storage, "prod", "2026T0", {"cad_contas": 1, "sumida": 4})
    assert cli.main(["archive", *common(db), "--name", "2026T0"]) == 2
    assert "sumida não existe em" in capsys.readouterr().err
    assert delta.read_snapshots(storage, "prod")[0]["snapshots"] == {"2026T0": {"cad_contas": 1,
                                                                               "sumida": 4}}
    assert not Path(storage.uri_of("prod/arquivo/2026T0")).exists()

    # Um arquivamento interrompido na terceira cópia, o segundo arquivo de cad_lancamentos, deixa
    # cad_contas inteira e uma partição registrada; a repetição pula as duas, copia a que falta e
    # move a entrada.
    delta.snapshot(storage, "prod", "2026T4", {"cad_contas": 1, "cad_lancamentos": 3})
    copies = []
    original_copy = Storage.copy

    def copy_until_the_third(self: Storage, source: str, destination: str) -> None:
        copies.append(destination)
        if len(copies) == 3:
            raise OSError("cópia interrompida")
        original_copy(self, source, destination)

    with monkeypatch.context() as patch:
        patch.setattr(Storage, "copy", copy_until_the_third)
        with pytest.raises(OSError, match="cópia interrompida"):
            cli.main(["archive", *common(db), "--name", "2026T4"])
    later = storage.uri_of("prod/arquivo/2026T4/cad_lancamentos")
    assert delta.open_table(later, storage).version() == 1
    assert "2026T4" in delta.read_snapshots(storage, "prod")[0]["snapshots"]
    capsys.readouterr()
    assert cli.main(["archive", *common(db), "--name", "2026T4"]) == 0
    out = capsys.readouterr().out
    assert "cad_contas: versão 1 copiada" in out and "cad_lancamentos: versão 3 copiada" in out
    assert measures(db, later) == measures(db, entries_uri)
    assert add_paths(db, later) == add_paths(db, entries_uri, 3)
    assert add_paths(db, storage.uri_of("prod/arquivo/2026T4/cad_contas")) == \
        add_paths(db, db.uri(ACCOUNTS))
    assert delta.read_snapshots(storage, "prod")[0]["archived"]["2026T4"] == \
        {"cad_contas": 1, "cad_lancamentos": 3}


def test_export_by_copy_and_by_rewrite(db: Database, capsys: pytest.CaptureFixture) -> None:
    """``export`` grava as pastas ``<coluna>=<valor>/`` por cópia e por reescrita, de uma versão
    antiga inclusive, e recusa o destino não vazio e o destino fora da raiz."""
    storage = db.storage
    export = ["export", *common(db), "--table", "cad_lancamentos", "--destination"]
    copy_uri = storage.uri_of("prod/exportacao/copia")
    assert cli.main([*export, copy_uri]) == 0
    assert "cad_lancamentos: 2 arquivo(s) em" in capsys.readouterr().out
    rewritten = storage.uri_of("prod/exportacao/reescrita")
    assert cli.main([*export, rewritten, "--mode", "rewrite"]) == 0
    assert cli.main([*export, storage.uri_of("prod/exportacao/antiga"), "--version", "1"]) == 0
    assert "1 arquivo(s) em" in capsys.readouterr().out.splitlines()[-1]
    for folder_name in ("copia", "reescrita"):
        files = storage.list_files(f"prod/exportacao/{folder_name}", ".parquet")
        assert len(files) == 2
        assert all("/data_base_str=2026-0" in f"/{path}" for path in files)
    with storage.duckdb_connect() as connection:
        query = (f"SELECT count(*), sum(valor) FROM read_parquet('{copy_uri}/*/*.parquet', "
                 "hive_partitioning = true, hive_types_autocast = false)")
        assert connection.execute(query).fetchone() == measures(db, db.uri(ENTRIES))

    assert cli.main([*export, copy_uri]) == 2
    assert "destino não vazio" in capsys.readouterr().err
    assert cli.main([*export, "/fora/da/raiz"]) == 2
    assert "fora da raiz" in capsys.readouterr().err


def test_cli_operation_usage_errors(db: Database, capsys: pytest.CaptureFixture) -> None:
    """Os erros de uso: o nome ausente, o modo desconhecido, a tabela fora do modelo e a tabela
    do modelo sem Delta, sem traceback."""
    assert exit_code(["snapshot", *common(db)]) == 2
    assert exit_code(["export", *common(db), "--table", "cad_contas", "--destination", "x",
                      "--mode", "outro"]) == 2
    assert cli.main(["history", *common(db), "--table", "nada"]) == 2
    assert cli.main(["history", *common(db), "--table", "cad_lancamentos_projetados"]) == 2
    assert cli.main(["export", *common(db), "--table", "cad_lancamentos_projetados",
                     "--destination", db.storage.uri_of("prod/exportacao/x")]) == 2
    err = capsys.readouterr().err
    assert "não existe" in err and "Traceback" not in err
