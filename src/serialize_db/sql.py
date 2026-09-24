"""O texto SQL de cada motor a partir de um statement Core: prefixo, renderização, bind, arquivos.

O módulo gera o texto SQL do DuckDB e do Redshift a partir de um statement Core do SQLAlchemy
(``render``): as constantes ficam embutidas, cada ``bindparam`` sem valor sai como ``:nome``, o
parâmetro de execução, e cada tabela do contrato sai com o sentinela ``{prefix}`` no nome
(``prefixed``), que quem executa troca pelo prefixo do sandbox (``read_sql``). ``bind``
reescreve os marcadores para o estilo do motor e confere o dicionário de parâmetros;
``referenced_tables`` lista as tabelas do contrato que um statement ou um texto cita;
``sql_files``, ``write_sql_files`` e ``check_sql_files`` geram, gravam e conferem os arquivos que
o pipeline versiona, um por statement e por motor.

O caminho padrão do pipeline é o statement Core submetido ao motor, que o compila pela cópia de
``prefixed`` com os parâmetros do cliente; o texto gerado é a opção para um pipeline que queira
sair do SQLAlchemy: ele entra no repositório do pipeline, revisado no diff, e a chamada que
compilava o statement passa a executar o texto. Toda tabela e toda coluna do contrato saem entre
aspas duplas, como no DDL de ``serialize_db.schema``, com o sentinela dentro das aspas
(``"{prefix}cad_contas"."numero"``). O statement é o mesmo que roda num ``sqlalchemy.Connection``
criado fora da biblioteca e nos motores: nada nele é próprio deste módulo.

Exemplo, com duas tabelas do contrato:

.. code-block:: python

    import duckdb
    import sqlalchemy as sa

    from serialize_db import sql

    metadata = sa.MetaData()
    entries = sa.Table("cad_lancamentos", metadata,
                       sa.Column("id_conta", sa.BigInteger), sa.Column("valor", sa.Double),
                       sa.Column("data_base_str", sa.String(10)))
    accounts = sa.Table("cad_contas", metadata,
                        sa.Column("id_conta", sa.BigInteger), sa.Column("numero", sa.String(20)))
    statement = (
        sa.select(accounts.c.numero, sa.func.sum(entries.c.valor).label("total"))
        .join_from(entries, accounts, entries.c.id_conta == accounts.c.id_conta)
        .where(entries.c.data_base_str == sa.bindparam("data_base_str", type_=sa.String(10)))
        .group_by(accounts.c.numero)
    )
    print(sql.render(statement, "duckdb", metadata))
    # SELECT "{prefix}cad_contas"."numero", sum("{prefix}cad_lancamentos"."valor") AS total
    # FROM "{prefix}cad_lancamentos" JOIN "{prefix}cad_contas" ON ...
    # WHERE "{prefix}cad_lancamentos"."data_base_str" = :data_base_str GROUP BY ...

    text, values = sql.bind(sql.render(statement, "duckdb", metadata, prefix=""),
                            {"data_base_str": "2026-08-31"}, "duckdb")
    duckdb.connect().execute(text, values)   # o texto com $data_base_str e o dicionário
"""

from __future__ import annotations

import os
import re

import duckdb_engine
import sqlalchemy as sa
from sqlalchemy.sql import quoted_name
from sqlalchemy.sql.util import find_tables
from sqlalchemy.sql.visitors import iterate, replacement_traverse
from sqlalchemy_redshift.dialect import RedshiftDialect_redshift_connector

from serialize_db._files import diff_files, write_files
from serialize_db.errors import SqlError
from serialize_db.schema import Dialect

__all__ = [
    "bind",
    "check_sql_files",
    "prefixed",
    "read_sql",
    "referenced_tables",
    "render",
    "sql_files",
    "write_sql_files",
]

SENTINEL = "{prefix}"
"""O sentinela do prefixo do sandbox no texto gerado; os motores o leem, e ``read_sql`` o troca."""

# O compilador de cada motor, com paramstyle "named" para o % dos literais não sair dobrado. São os
# dialetos de terceiros, e não o postgresql do SQLAlchemy.
_DIALECTS = {
    "duckdb": duckdb_engine.Dialect(paramstyle="named"),
    "redshift": RedshiftDialect_redshift_connector(paramstyle="named"),
}
# O marcador de parâmetro de cada motor: $nome no DuckDB, :nome no redshift_connector com
# cursor.paramstyle = "named".
_MARKERS = {"duckdb": "$", "redshift": ":"}
_PARAMETER_NAME = re.compile(r"[a-z_][a-z0-9_]*")
# Uma região citada ('...' ou "...", com a aspa dobrada como escape), ou :nome fora de ::cast. As
# regiões vêm primeiro para um : dentro delas nunca ser lido como marcador.
_QUOTED_OR_PLACEHOLDER = re.compile(
    r"'(?:[^']|'')*'|\"(?:[^\"]|\"\")*\"|(?<![:\w]):(?P<name>[a-z_][a-z0-9_]*)")
_SENTINEL_TABLE = re.compile(r"\{prefix\}(\w+)")


# ---------------------------------------------------------------- o prefixo


def _prefixed_copy(table: sa.Table, prefix: str) -> sa.Table:
    """A cópia da tabela com o prefixo no nome e só nomes e tipos, o que um DML compilado usa.

    Todo nome vai citado, como no DDL da etapa 1: o texto não depende da lista de palavras
    reservadas do dialeto, e o sentinela fica dentro das aspas.
    """
    columns = []
    for column in table.columns:
        columns.append(sa.Column(quoted_name(column.name, quote=True), column.type))
    name = quoted_name(f"{prefix}{table.name}", quote=True)
    return sa.Table(name, sa.MetaData(), *columns)


def prefixed(statement: sa.sql.ClauseElement, metadata: sa.MetaData,
             prefix: str = SENTINEL) -> sa.sql.ClauseElement:
    """O statement com cada tabela do contrato trocada pela cópia prefixada; o original não muda.

    A cópia de cada tabela de ``metadata`` leva o prefixo no nome e cada coluna com nome e tipo,
    tudo entre aspas; chaves e índices ficam de fora, porque um ``SELECT`` ou um
    ``INSERT ... SELECT`` não os compila. ``replacement_traverse`` troca cada ``Table`` do
    contrato pela cópia e cada ``Column`` pela coluna de mesmo nome na cópia, o alvo de um
    ``INSERT`` e as tabelas de uma subconsulta inclusive.

    Exemplo:

    .. code-block:: python

        str(prefixed(sa.select(accounts.c.numero), metadata, "exec_42_"))
        # SELECT "exec_42_cad_contas"."numero" FROM "exec_42_cad_contas"
    """
    copies = {}
    for table in metadata.tables.values():
        copies[table] = _prefixed_copy(table, prefix)

    def replace(element: sa.sql.ClauseElement) -> sa.sql.ClauseElement | None:
        # replacement_traverse chama replace em cada nó do statement; None deixa o nó como está.
        if isinstance(element, sa.Table):
            return copies.get(element)
        if isinstance(element, sa.Column) and element.table in copies:
            return copies[element.table].c[element.name]
        return None

    return replacement_traverse(statement, {}, replace)


# ---------------------------------------------------------------- os parâmetros sem valor


def required_parameters(statement: sa.sql.ClauseElement) -> set[str]:
    """Os nomes dos ``bindparam`` sem valor do statement, pelo percurso de todos os nós; protegida,
    o guarda dos motores antes de compilar.

    Sob ``literal_binds``, ``compiled.binds`` sai vazio e o ``bindparam`` sem valor vira ``NULL``
    calado, até num ``IN`` de lista; sem ``literal_binds``, ``compiled.binds`` o marca, mas
    ``statement.params`` ignora um nome a mais. O ``required`` de cada ``BindParameter`` é o estado
    que marca a falta (leituras de 2026-09-22 e 2026-09-23).
    """
    names = set()
    for element in iterate(statement):
        if isinstance(element, sa.BindParameter) and element.required:
            names.add(element.key)
    return names


# ---------------------------------------------------------------- o texto por motor


def _parameters_as_placeholders(statement: sa.sql.ClauseElement) -> sa.sql.ClauseElement:
    """A cópia do statement com cada ``bindparam`` sem valor trocado por ``:nome``; o original não
    muda.

    ``literal_binds`` renderizaria o ``bindparam`` sem valor como ``NULL``; um
    ``literal_column(":nome")`` atravessa a compilação como texto. O ``bindparam`` com valor fica,
    e sai como constante. Um nome fora de ``[a-z_][a-z0-9_]*`` é ``SqlError``, porque ``bind`` não
    o leria no texto.
    """
    def replace(element: sa.sql.ClauseElement) -> sa.sql.ClauseElement | None:
        # replacement_traverse chama replace em cada nó; None deixa o nó como está.
        if not isinstance(element, sa.BindParameter) or not element.required:
            return None
        if not _PARAMETER_NAME.fullmatch(element.key):
            raise SqlError(f"nome de parâmetro inválido: {element.key!r}")
        return sa.literal_column(f":{element.key}", type_=element.type)

    return replacement_traverse(statement, {}, replace)


def render(statement: sa.sql.ClauseElement, dialect: Dialect, metadata: sa.MetaData,
           prefix: str = SENTINEL) -> str:
    """O texto do motor com as constantes embutidas e cada ``bindparam`` sem valor como ``:nome``.

    O statement é compilado sobre a cópia prefixada (``prefixed``) pelo dialeto do motor com
    ``paramstyle="named"``, que não dobra o ``%`` dos literais. O ``bindparam`` sem valor é o
    parâmetro de execução, que ``bind`` reescreve para o motor; o ``bindparam`` com valor sai como
    constante, e um nome de parâmetro fora de ``[a-z_][a-z0-9_]*`` é ``SqlError``. O texto sai com
    o sentinela ``{prefix}`` dentro das aspas de cada tabela do contrato; ``prefix=""`` dá o texto
    sobre as tabelas do contrato, e ``prefix="exec_42_"`` o texto sobre o sandbox dessa execução.
    Nenhuma linha termina em espaço.

    Exemplo:

    .. code-block:: python

        render(statement, "redshift", metadata, prefix="exec_42_")
        # SELECT "exec_42_cad_contas"."numero", ... WHERE ... = :data_base_str ...
    """
    copy = _parameters_as_placeholders(prefixed(statement, metadata, prefix))
    compiled = copy.compile(dialect=_DIALECTS[dialect], compile_kwargs={"literal_binds": True})
    # O compilador deixa um espaço antes de cada quebra de linha; sem ele o arquivo versionado
    # sobrevive a um editor que apara o fim das linhas.
    lines = []
    for line in str(compiled).splitlines():
        lines.append(line.rstrip())
    return "\n".join(lines)


def _placeholders(sql: str) -> set[str]:
    """Os nomes dos marcadores ``:nome`` do texto, fora das regiões citadas."""
    names = set()
    for match in _QUOTED_OR_PLACEHOLDER.finditer(sql):
        if match.group("name") is not None:
            names.add(match.group("name"))
    return names


def bind(sql: str, params: dict[str, object], style: Dialect) -> tuple[str, dict[str, object]]:
    """O texto com ``:nome`` reescrito para o marcador do motor e o dicionário conferido.

    O marcador vira ``$nome`` no estilo ``duckdb`` e fica ``:nome`` no ``redshift``, que o
    ``redshift_connector`` lê com ``cursor.paramstyle = "named"``. Toda região citada passa
    intacta, entre aspas simples ou duplas: ``'12:30'``, ``valor::DECIMAL(18, 2)`` e a coluna
    ``"taxa :base"`` não mudam. Um texto que ainda traz o sentinela ``{prefix}`` é ``SqlError``,
    e um dicionário com parâmetro faltante ou sobrando também. Nenhum valor entra no texto: o
    dicionário vai ao driver.

    Exemplo:

    .. code-block:: python

        bind('SELECT 1 WHERE "data_str" = :data_str', {"data_str": "2026-08-31"}, "duckdb")
        # ('SELECT 1 WHERE "data_str" = $data_str', {'data_str': '2026-08-31'})
    """
    if SENTINEL in sql:
        raise SqlError(
            f"o texto ainda traz o sentinela {SENTINEL}; leia-o por read_sql(..., prefix=...)")
    names = _placeholders(sql)
    if names != set(params):
        raise SqlError(
            f"parâmetros do texto {sorted(names)} e do dicionário {sorted(params)} não fecham")
    marker = _MARKERS[style]

    def rewrite(match: re.Match) -> str:
        if match.group("name") is None:
            return match.group(0)                # região citada, intacta
        return marker + match.group("name")

    return _QUOTED_OR_PLACEHOLDER.sub(rewrite, sql), dict(params)


def referenced_tables(statement_or_sql: sa.sql.ClauseElement | str) -> set[str]:
    """As tabelas de um statement Core (``find_tables``) ou de um texto gerado (o sentinela).

    Num statement Core entram as tabelas lidas e o alvo de um ``INSERT``, ``UPDATE`` ou
    ``DELETE`` (``include_crud=True``), só as ``sa.Table``; num texto gerado, cada nome que segue
    o sentinela ``{prefix}``. O log da execução as registra por comando.

    Exemplo:

    .. code-block:: python

        referenced_tables(statement)                            # {"cad_contas", "cad_lancamentos"}
        referenced_tables(render(statement, "duckdb", metadata))   # o mesmo conjunto
    """
    if isinstance(statement_or_sql, str):
        return set(_SENTINEL_TABLE.findall(statement_or_sql))
    tables = set()
    for table in find_tables(statement_or_sql, include_crud=True):
        if isinstance(table, sa.Table):
            tables.add(table.name)
    return tables


# ---------------------------------------------------------------- os arquivos gerados


def sql_files(statements: dict[str, sa.sql.ClauseElement], metadata: sa.MetaData) -> dict[str, str]:
    """Os arquivos de texto SQL de cada statement, em memória: o conteúdo por nome de arquivo.

    ``<nome>.duckdb.sql`` e ``<nome>.redshift.sql`` são o texto de cada motor, sempre com o
    sentinela ``{prefix}`` e com ``\\n`` final, porque o arquivo versionado serve a qualquer alvo.
    O pipeline os versiona no seu repositório, e o diff contra a geração nova mostra o que uma
    mudança de modelo ou de statement altera em cada motor.

    Exemplo:

    .. code-block:: python

        sorted(sql_files({"total_por_conta": statement}, metadata))
        # ["total_por_conta.duckdb.sql", "total_por_conta.redshift.sql"]
    """
    files = {}
    for name, statement in statements.items():
        files[f"{name}.duckdb.sql"] = render(statement, "duckdb", metadata) + "\n"
        files[f"{name}.redshift.sql"] = render(statement, "redshift", metadata) + "\n"
    return files


def write_sql_files(statements: dict[str, sa.sql.ClauseElement], metadata: sa.MetaData,
                    directory: str) -> list[str]:
    """Grava ``sql_files`` em ``directory``, criada se preciso, e devolve os caminhos gravados.

    Exemplo:

    .. code-block:: python

        write_sql_files({"total_por_conta": statement}, metadata, "sql")
        # ["sql/total_por_conta.duckdb.sql", "sql/total_por_conta.redshift.sql"]
    """
    return write_files(sql_files(statements, metadata), directory)


def check_sql_files(statements: dict[str, sa.sql.ClauseElement], metadata: sa.MetaData,
                    directory: str) -> list[str]:
    """O diff unificado dos arquivos versionados em ``directory`` contra a geração nova.

    Vazio quando nada mudou; um arquivo ausente aparece inteiro como acrescentado. Nada é gravado.

    Exemplo:

    .. code-block:: python

        check_sql_files({"total_por_conta": statement}, metadata, "sql")   # [] quando atualizados
    """
    return diff_files(sql_files(statements, metadata), directory)


def read_sql(directory: str, name: str, dialect: Dialect, prefix: str) -> str:
    """O texto versionado com o sentinela trocado pelo prefixo informado, pronto para ``bind``.

    ``prefix`` é obrigatório, porque quem chama sabe o alvo: a string vazia para as tabelas do
    contrato, ``exec_<id>_`` para o sandbox da execução. Nenhuma outra primitiva preenche o
    sentinela.

    Exemplo:

    .. code-block:: python

        read_sql("sql", "total_por_conta", "duckdb", prefix="exec_42_")
        # SELECT "exec_42_cad_contas"."numero", ...
    """
    path = os.path.join(directory, f"{name}.{dialect}.sql")
    with open(path, encoding="utf-8") as handle:
        return handle.read().replace(SENTINEL, prefix)
