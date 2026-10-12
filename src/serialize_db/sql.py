"""[public] O texto SQL de cada motor a partir de um statement Core: prefixo, renderização, bind,
arquivos.

Um statement Core é a consulta ou o comando que o cliente escreve com a camada Core do
SQLAlchemy. O módulo gera dele o texto SQL do DuckDB e do Redshift (``render``). No texto gerado:

- as constantes ficam embutidas;
- cada ``bindparam`` sem valor sai como ``:nome``, o parâmetro de execução;
- cada tabela do contrato, o modelo SQLAlchemy do cliente, sai com o sentinela ``{prefix}`` no
  nome (``prefixed``). O sentinela é o marcador que quem executa troca pelo prefixo do sandbox
  (``read_sql``).

As outras funções completam o caminho:

- ``bind`` reescreve os marcadores para o estilo do motor e confere o dicionário de parâmetros;
- ``referenced_tables`` lista as tabelas do contrato que um statement ou um texto cita;
- ``sql_files``, ``write_sql_files`` e ``check_sql_files`` geram, gravam e conferem os arquivos
  que o pipeline versiona, um por statement e por motor.

O caminho padrão do pipeline é o statement Core submetido ao motor, que o compila pela cópia de
``prefixed`` com os parâmetros do cliente. O texto gerado é a opção para um pipeline que queira
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

import math
import os
import re
from collections.abc import Callable, Mapping

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
"""[protected] O sentinela do prefixo do sandbox no texto gerado.

``read_sql`` o troca pelo prefixo informado, e o motor e o leitor Redshift pelo prefixo deles.
``bind`` recusa um texto que ainda o traz."""


class _FloatLiteral(sa.Float):
    """[private] O ``Float`` e o ``Double`` que os dialetos de ``with_float_literals`` embutem como
    constante.

    ``NaN`` e os infinitos saem como texto convertido para ``float8``, que o DuckDB e o Redshift
    leem. O SQLAlchemy os escreveria ``nan``, ``inf`` e ``-inf``, que os dois motores leem como
    nome de coluna e recusam. O número finito sai como no SQLAlchemy.
    """

    def literal_processor(
        self,
        dialect: sa.engine.Dialect,
    ) -> Callable[[object], str]:
        """[private] O conversor que escreve cada valor como literal no texto."""
        number_literal = super().literal_processor(dialect)

        def process(
            value: object,
        ) -> str:
            # O float do Python e o numpy.float64, subclasse dele; o resto sai pelo SQLAlchemy.
            if not isinstance(value, float) or math.isfinite(value):
                return number_literal(value)
            if math.isnan(value):
                return "'NaN'::float8"
            if value > 0:
                return "'Infinity'::float8"
            return "'-Infinity'::float8"

        return process


def with_float_literals(
    dialect: sa.engine.Dialect,
) -> sa.engine.Dialect:
    """[protected] O dialeto com o ``Float`` e o ``Double`` embutidos por ``_FloatLiteral``. Também
    serve aos compiladores dos motores DuckDB e Redshift.

    A função altera o ``colspecs`` da instância, que diz o tipo que o dialeto adota para cada
    ``sa.Float`` e cada subclasse dele, ``sa.Double`` inclusive. A classe do dialeto fica como
    está, e o cliente pode usá-la no ``sqlalchemy.Engine`` dele.
    """
    dialect.colspecs = {**dialect.colspecs, sa.Float: _FloatLiteral}
    return dialect


# O compilador de cada motor, com paramstyle "named" para o % dos literais não sair dobrado. São os
# dialetos de terceiros, e não o postgresql do SQLAlchemy.
_DIALECTS = {
    "duckdb": with_float_literals(duckdb_engine.Dialect(paramstyle="named")),
    "redshift": with_float_literals(RedshiftDialect_redshift_connector(paramstyle="named")),
}
# O DuckDB lê a contrabarra de uma constante como caractere, e o dialeto, herdado do PostgreSQL, a
# dobraria; o Redshift a lê como escape, e o dialeto dele a dobra.
_DIALECTS["duckdb"]._backslash_escapes = False
# O marcador de parâmetro de cada motor: $nome no DuckDB, :nome no redshift_connector com
# cursor.paramstyle = "named".
_MARKERS = {"duckdb": "$", "redshift": ":"}
_PARAMETER_NAME = re.compile(r"[a-z_][a-z0-9_]*")
# Uma região citada ('...' ou "...", com a aspa dobrada como escape), ou :nome fora de ::cast. As
# regiões vêm primeiro para um : dentro delas nunca ser lido como marcador.
_QUOTED_OR_PLACEHOLDER = re.compile(
    r"'(?:[^']|'')*'|\"(?:[^\"]|\"\")*\"|(?<![:\w]):(?P<name>[a-z_][a-z0-9_]*)"
)
_SENTINEL_TABLE = re.compile(r"\{prefix\}(\w+)")


# ---------------------------------------------------------------- o prefixo


def _prefixed_copy(
    table: sa.Table,
    prefix: str,
) -> sa.Table:
    """[private] A cópia da tabela com o prefixo no nome. De cada coluna, a cópia leva só o nome, a
    ``key`` e o tipo, o que um DML compilado usa.

    Todo nome vai citado, como no DDL de ``serialize_db.schema``. Assim o texto não depende da
    lista de palavras reservadas do dialeto, e o sentinela fica dentro das aspas. A ``key`` de cada
    coluna é a do original: é por ela que o ``values(...)`` de um ``INSERT`` ou de um ``UPDATE``
    acha a coluna.
    """
    columns = []
    for column in table.columns:
        name = quoted_name(column.name, quote=True)
        columns.append(sa.Column(name, column.type, key=column.key))
    name = quoted_name(f"{prefix}{table.name}", quote=True)
    return sa.Table(name, sa.MetaData(), *columns)


def prefixed(
    statement: sa.sql.ClauseElement,
    metadata: sa.MetaData,
    prefix: str = SENTINEL,
) -> sa.sql.ClauseElement:
    """[public] O statement com cada tabela do contrato trocada pela cópia prefixada.

    A cópia de cada tabela de ``metadata`` leva o prefixo no nome. Cada coluna da cópia leva o
    nome, a ``key`` e o tipo, com os nomes entre aspas. Chaves e índices ficam de fora, porque um
    ``SELECT`` ou um ``INSERT ... SELECT`` não os compila. ``replacement_traverse`` percorre o
    statement e troca cada ``Table`` do contrato pela cópia, e cada ``Column`` pela coluna de mesma
    ``key`` na cópia. A troca alcança o alvo de um ``INSERT`` e as tabelas de uma subconsulta.

    Exemplo:

    .. code-block:: python

        str(prefixed(sa.select(accounts.c.numero), metadata, "exec_42_"))
        # SELECT "exec_42_cad_contas"."numero" FROM "exec_42_cad_contas"

    :param statement: o statement Core. O original não muda.
    :param metadata: os modelos do cliente, como ``Base.metadata``. Uma tabela fora deles fica
        como está.
    :param prefix: o prefixo do nome de cada tabela do contrato. O padrão é o sentinela
        ``{prefix}``.
    :return: o statement novo.
    """
    copies = {}
    for table in metadata.tables.values():
        copies[table] = _prefixed_copy(table, prefix)

    def replace(
        element: sa.sql.ClauseElement,
    ) -> sa.sql.ClauseElement | None:
        # replacement_traverse chama replace em cada nó do statement; None deixa o nó como está.
        if isinstance(element, sa.Table):
            return copies.get(element)
        if isinstance(element, sa.Column) and element.table in copies:
            return copies[element.table].c[element.key]
        return None

    return replacement_traverse(statement, {}, replace)


# ---------------------------------------------------------------- os parâmetros sem valor


def _required_parameters(
    statement: sa.sql.ClauseElement,
) -> set[str]:
    """[private] Os nomes dos ``bindparam`` sem valor do statement, pelo percurso de todos os nós.
    É o guarda que ``bound_statement`` aplica antes de compilar, no caminho dos motores.

    O estado lido é o ``required`` de cada ``BindParameter``, que marca a falta de valor
    (leituras de 2026-09-22 e 2026-09-23). A compilação não serve de guarda. Sob
    ``literal_binds``, ``compiled.binds`` sai vazio, e o ``bindparam`` sem valor vira ``NULL``
    calado, até num ``IN`` de lista. Sem ``literal_binds``, ``compiled.binds`` o marca, mas
    ``statement.params`` ignora um nome a mais.
    """
    names = set()
    for element in iterate(statement):
        if isinstance(element, sa.BindParameter) and element.required:
            names.add(element.key)
    return names


def _statement_metadata(
    statement: sa.sql.ClauseElement,
) -> sa.MetaData | None:
    """[private] O ``MetaData`` das tabelas do contrato que o statement cita, o que ``prefixed``
    recebe. ``None`` num statement sem ``sa.Table``."""
    for table in find_tables(statement, include_crud=True):
        if isinstance(table, sa.Table):
            return table.metadata
    return None


def bound_statement(
    statement: sa.sql.ClauseElement,
    params: Mapping[str, object] | None,
    prefix: str,
) -> sa.sql.ClauseElement:
    """[protected] A cópia prefixada do statement com os valores do cliente. É o começo do caminho
    de compilação dos dois motores.

    Os nomes de ``params`` têm de fechar com os ``bindparam`` sem valor; a diferença é
    ``SqlError``. A conferência é da biblioteca porque ``params`` ignora o nome a mais, e o valor
    que falta seria ``InvalidRequestError`` na compilação. As tabelas do contrato são as do
    ``MetaData`` da primeira ``sa.Table`` que o statement cita.
    """
    values = dict(params or {})
    required = _required_parameters(statement)
    if required != set(values):
        raise SqlError(
            f"parâmetros do statement {sorted(required)} e do dicionário "
            f"{sorted(values)} não fecham"
        )
    bound = statement
    if values:
        bound = statement.params(**values)
    metadata = _statement_metadata(bound)
    if metadata is None:
        return bound
    return prefixed(bound, metadata, prefix=prefix)


# ---------------------------------------------------------------- o texto por motor


def _parameters_as_placeholders(
    statement: sa.sql.ClauseElement,
) -> sa.sql.ClauseElement:
    """[private] A cópia do statement com cada ``bindparam`` sem valor trocado por ``:nome``. O
    original não muda.

    A troca é necessária porque ``literal_binds`` renderizaria o ``bindparam`` sem valor como
    ``NULL``, e um ``literal_column(":nome")`` atravessa a compilação como texto. O ``bindparam``
    com valor fica, e sai como constante. Um nome fora de ``[a-z_][a-z0-9_]*`` é ``SqlError``,
    porque ``bind`` não o leria no texto.
    """

    def replace(
        element: sa.sql.ClauseElement,
    ) -> sa.sql.ClauseElement | None:
        # replacement_traverse chama replace em cada nó; None deixa o nó como está.
        if not isinstance(element, sa.BindParameter) or not element.required:
            return None
        if not _PARAMETER_NAME.fullmatch(element.key):
            raise SqlError(f"nome de parâmetro inválido: {element.key!r}")
        return sa.literal_column(f":{element.key}", type_=element.type)

    return replacement_traverse(statement, {}, replace)


def render(
    statement: sa.sql.ClauseElement,
    dialect: Dialect,
    metadata: sa.MetaData,
    prefix: str = SENTINEL,
) -> str:
    """[public] O texto do motor com as constantes embutidas e cada ``bindparam`` sem valor como
    ``:nome``.

    O statement é compilado sobre a cópia prefixada (``prefixed``) pelo dialeto do motor com
    ``paramstyle="named"``, que não dobra o ``%`` dos literais. O ``bindparam`` sem valor é o
    parâmetro de execução, que ``bind`` reescreve depois para o marcador do motor. O ``bindparam``
    com valor sai como constante. O ``NaN`` e os infinitos de um ``Float`` ou ``Double`` saem como
    ``'NaN'::float8``, ``'Infinity'::float8`` e ``'-Infinity'::float8``, que os dois motores leem.

    Exemplo:

    .. code-block:: python

        render(statement, "redshift", metadata, prefix="exec_42_")
        # SELECT "exec_42_cad_contas"."numero", ... WHERE ... = :data_base_str ...

    :param statement: o statement Core. O original não muda.
    :param dialect: o motor, ``"duckdb"`` ou ``"redshift"``.
    :param metadata: os modelos do cliente, como ``Base.metadata``. Uma tabela fora deles fica
        como está.
    :param prefix: o prefixo do nome de cada tabela do contrato, dentro das aspas. O padrão deixa
        no texto o sentinela ``{prefix}``. ``prefix=""`` dá o texto sobre as tabelas do contrato,
        e ``prefix="exec_42_"`` o texto sobre o sandbox dessa execução.
    :return: o texto. Nenhuma linha termina em espaço.
    :raises serialize_db.errors.SqlError: um nome de parâmetro fora de ``[a-z_][a-z0-9_]*``, que
        ``bind`` não leria no texto.
    """
    prefixed_statement = prefixed(statement, metadata, prefix)
    with_placeholders = _parameters_as_placeholders(prefixed_statement)
    compiled = with_placeholders.compile(
        dialect=_DIALECTS[dialect], compile_kwargs={"literal_binds": True}
    )
    # O compilador deixa um espaço antes de cada quebra de linha; sem ele o arquivo versionado
    # sobrevive a um editor que apara o fim das linhas.
    lines = []
    for line in str(compiled).splitlines():
        lines.append(line.rstrip())
    return "\n".join(lines)


def _placeholders(
    sql: str,
) -> set[str]:
    """[private] Os nomes dos marcadores ``:nome`` do texto, fora das regiões citadas."""
    names = set()
    for match in _QUOTED_OR_PLACEHOLDER.finditer(sql):
        if match.group("name") is not None:
            names.add(match.group("name"))
    return names


def bind(
    sql: str,
    params: dict[str, object],
    dialect: Dialect,
) -> tuple[str, dict[str, object]]:
    """[public] O texto com ``:nome`` reescrito para o marcador do motor e o dicionário conferido.

    Toda região citada, entre aspas simples ou duplas, passa intacta, e o ``::`` de um cast não é
    marcador: ``'12:30'``, ``valor::DECIMAL(18, 2)`` e a coluna ``"taxa :base"`` não mudam. Nenhum
    valor entra no texto: o dicionário vai ao driver.

    Exemplo:

    .. code-block:: python

        bind('SELECT 1 WHERE "data_str" = :data_str', {"data_str": "2026-08-31"}, "duckdb")
        # ('SELECT 1 WHERE "data_str" = $data_str', {'data_str': '2026-08-31'})

    :param sql: o texto com os parâmetros por nome, como ``read_sql`` o devolve.
    :param params: o valor de cada parâmetro do texto, por nome.
    :param dialect: o motor, ``"duckdb"`` ou ``"redshift"``. O marcador vira ``$nome`` no
        ``duckdb``. No ``redshift`` ele fica como está, porque o ``redshift_connector`` o lê com
        ``cursor.paramstyle = "named"``.
    :return: o texto reescrito e uma cópia de ``params``.
    :raises serialize_db.errors.SqlError: um texto que ainda traz o sentinela ``{prefix}``, ou um
        dicionário com parâmetro faltante ou sobrando.
    """
    if SENTINEL in sql:
        raise SqlError(
            f"o texto ainda traz o sentinela {SENTINEL}; leia-o por read_sql(..., prefix=...)"
        )
    names = _placeholders(sql)
    if names != set(params):
        raise SqlError(
            f"parâmetros do texto {sorted(names)} e do dicionário {sorted(params)} não fecham"
        )
    marker = _MARKERS[dialect]

    def rewrite(
        match: re.Match,
    ) -> str:
        if match.group("name") is None:
            return match.group(0)  # região citada, intacta
        return marker + match.group("name")

    return _QUOTED_OR_PLACEHOLDER.sub(rewrite, sql), dict(params)


def referenced_tables(
    statement_or_sql: sa.sql.ClauseElement | str,
) -> set[str]:
    """[public] As tabelas de um statement Core (``find_tables``) ou de um texto gerado (o
    sentinela).

    Num statement Core entram as tabelas lidas e o alvo de um ``INSERT``, ``UPDATE`` ou
    ``DELETE`` (``include_crud=True``), e só as ``sa.Table``. Num texto gerado entra cada nome que
    segue o sentinela ``{prefix}``. O leitor Delta (``serialize_db.reader.DeltaReader``) confere
    por esta função as tabelas do modelo que um statement Core cita, e recusa com
    ``ContractError`` a tabela que não tem view.

    Exemplo:

    .. code-block:: python

        referenced_tables(statement)                            # {"cad_contas", "cad_lancamentos"}
        referenced_tables(render(statement, "duckdb", metadata))   # o mesmo conjunto

    :param statement_or_sql: o statement Core, ou o texto gerado com o sentinela.
    :return: os nomes das tabelas.
    """
    if isinstance(statement_or_sql, str):
        return set(_SENTINEL_TABLE.findall(statement_or_sql))
    tables = set()
    for table in find_tables(statement_or_sql, include_crud=True):
        if isinstance(table, sa.Table):
            tables.add(table.name)
    return tables


# ---------------------------------------------------------------- os arquivos gerados

# O fim do nome de cada arquivo de sql_files, pelo qual check_sql_files acha na pasta o arquivo que
# a geração não produz mais.
_SQL_SUFFIXES = (".duckdb.sql", ".redshift.sql")


def sql_files(
    statements: dict[str, sa.sql.ClauseElement],
    metadata: sa.MetaData,
) -> dict[str, str]:
    """[public] Os arquivos de texto SQL de cada statement, em memória.

    O pipeline versiona esses arquivos no seu repositório. O diff contra a geração nova mostra o
    que uma mudança de modelo ou de statement altera em cada motor.

    Exemplo:

    .. code-block:: python

        sorted(sql_files({"total_por_conta": statement}, metadata))
        # ["total_por_conta.duckdb.sql", "total_por_conta.redshift.sql"]

    :param statements: os statements Core por nome. O nome dá nome aos arquivos.
    :param metadata: os modelos do cliente, como ``Base.metadata``. Uma tabela fora deles fica
        como está.
    :return: o conteúdo de cada arquivo, pelo nome dele. ``<nome>.duckdb.sql`` e
        ``<nome>.redshift.sql`` são o texto de cada motor, sempre com o sentinela ``{prefix}`` e
        com ``\\n`` final, porque o arquivo versionado serve a qualquer alvo.
    :raises serialize_db.errors.SqlError: um statement com nome de parâmetro fora de
        ``[a-z_][a-z0-9_]*``.
    """
    files = {}
    for name, statement in statements.items():
        files[f"{name}.duckdb.sql"] = render(statement, "duckdb", metadata) + "\n"
        files[f"{name}.redshift.sql"] = render(statement, "redshift", metadata) + "\n"
    return files


def write_sql_files(
    statements: dict[str, sa.sql.ClauseElement],
    metadata: sa.MetaData,
    directory: str,
) -> list[str]:
    """[public] Grava em ``directory`` os arquivos de ``sql_files``.

    Nada é apagado. Os arquivos de um statement que saiu do dicionário ficam na pasta, e
    ``check_sql_files`` os acusa até que o cliente os apague.

    Exemplo:

    .. code-block:: python

        write_sql_files({"total_por_conta": statement}, metadata, "sql")
        # ["sql/total_por_conta.duckdb.sql", "sql/total_por_conta.redshift.sql"]

    :param statements: os statements Core por nome. O nome dá nome aos arquivos.
    :param metadata: os modelos do cliente, como ``Base.metadata``. Uma tabela fora deles fica
        como está.
    :param directory: a pasta local dos arquivos versionados, criada se preciso.
    :return: os caminhos gravados, em ordem de nome.
    :raises serialize_db.errors.SqlError: um statement com nome de parâmetro fora de
        ``[a-z_][a-z0-9_]*``.
    """
    return write_files(sql_files(statements, metadata), directory)


def check_sql_files(
    statements: dict[str, sa.sql.ClauseElement],
    metadata: sa.MetaData,
    directory: str,
) -> list[str]:
    """[public] O diff unificado dos arquivos versionados em ``directory`` contra a geração nova.

    A comparação é do texto exato. O arquivo sem o ``\\n`` final difere, e o diff traz o aviso
    ``\\ Sem quebra de linha no fim do arquivo`` depois da última linha. Nada é gravado.

    Exemplo:

    .. code-block:: python

        check_sql_files({"total_por_conta": statement}, metadata, "sql")   # [] quando atualizados

    :param statements: os statements Core por nome. O nome dá nome aos arquivos.
    :param metadata: os modelos do cliente, como ``Base.metadata``. Uma tabela fora deles fica
        como está.
    :param directory: a pasta local dos arquivos versionados. Ela é separada da pasta dos
        arquivos de ``serialize_db.schema``, que também terminam em ``.duckdb.sql`` e
        ``.redshift.sql``.
    :return: as linhas do diff, vazias quando nada mudou. Um arquivo ausente aparece inteiro como
        acrescentado. Um arquivo da pasta terminado em ``.duckdb.sql`` ou ``.redshift.sql`` que a
        geração não produz, como o de um statement que saiu do dicionário, aparece inteiro como
        removido.
    :raises serialize_db.errors.SqlError: um statement com nome de parâmetro fora de
        ``[a-z_][a-z0-9_]*``.
    """
    return diff_files(sql_files(statements, metadata), directory, _SQL_SUFFIXES)


def read_sql(
    directory: str,
    name: str,
    dialect: Dialect,
    prefix: str,
) -> str:
    """[public] O texto versionado com o sentinela trocado pelo prefixo informado, pronto para
    ``bind``.

    O motor e o leitor Redshift também trocam o sentinela de um texto pronto pelo prefixo deles,
    ``exec_<id>_`` e ``<ambiente>_``. O motor DuckDB e o leitor Delta entregam o texto a ``bind``
    como está, e ``bind`` recusa um texto que ainda traz o sentinela (``SqlError``).

    Exemplo:

    .. code-block:: python

        read_sql("sql", "total_por_conta", "duckdb", prefix="exec_42_")
        # SELECT "exec_42_cad_contas"."numero", ...

    :param directory: a pasta local dos arquivos versionados.
    :param name: o nome do statement, que dá nome ao arquivo.
    :param dialect: o motor, ``"duckdb"`` ou ``"redshift"``.
    :param prefix: o prefixo no lugar do sentinela. A string vazia dá o texto sobre as tabelas do
        contrato, e ``exec_<id>_`` o texto sobre o sandbox da execução. O argumento é obrigatório,
        porque quem chama sabe o alvo.
    :return: o texto do arquivo ``<name>.duckdb.sql`` ou ``<name>.redshift.sql`` de
        ``directory``.
    :raises FileNotFoundError: o arquivo ausente de ``directory``.
    """
    path = os.path.join(directory, f"{name}.{dialect}.sql")
    with open(path, encoding="utf-8") as handle:
        return handle.read().replace(SENTINEL, prefix)
