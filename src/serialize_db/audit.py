"""[public] As verificações que a auditoria roda antes de publicar no Delta, derivadas do contrato.

A auditoria confere uma tabela do sandbox, o banco de trabalho da execução no DuckDB ou no
Redshift, contra o contrato, o modelo SQLAlchemy do cliente. Chave primária, unicidade e chave
estrangeira ficam fora do DDL dos dois sandboxes. A auditoria é onde elas são aplicadas: cada
verificação é uma consulta montada a partir do ``Table`` do modelo, sem declaração adicional.

O módulo divide o trabalho com o motor da execução:

- ``checks`` devolve a lista de ``Check``. Cada ``Check`` traz o statement Core sobre a tabela do
  contrato e não sabe qual motor o roda.
- ``audit_sql`` renderiza o texto de cada verificação por ``sql.render``, para depuração.
- O motor, ``DuckDBEngine`` ou ``RedshiftEngine``, roda as verificações e monta o ``AuditReport``.

As verificações, pelo nome que cada uma tem no relatório:

- ``linhas``: uma consulta só, agrupada pela coluna de partição numa tabela particionada. Ela conta
  por coluna os defeitos:

  - o nulo em ``NOT NULL``;
  - o texto acima de ``String(n)`` em bytes;
  - o texto de uma coluna ``Uuid`` acima de 36 bytes;
  - o texto de uma coluna ``Text`` e o documento JSON acima de 65.535 bytes (o teto do Redshift);
  - o JSON inválido;
  - a coluna de partição diferente da derivação de ``partition_source``;
  - o valor de partição fora de ``schema.PARTITION_VALUE``.

  A mesma consulta soma cada coluna ``Numeric`` e ``Double`` como ``DECIMAL(38, 6)``, a ``Double``
  só nos valores finitos, e conta à parte os não finitos. As somas e as contagens de não finitos
  são leituras: elas não reprovam.
- ``chave_<colunas>``: a chave repetida nas partições da execução, uma consulta por chave.
- ``chave_<colunas>_tabela``: a chave repetida entre as partições da execução e as demais
  partições da versão fixada, a versão da tabela Delta que a execução leu na abertura. Ela roda
  numa tabela particionada, com as partições da execução, e numa chave sem a coluna de partição.
  A chave com a coluna de ``partition_source`` só entra com ``key_scope="table"``, e nenhuma
  chave entra com ``key_scope="partition"``.
- ``orfao_<colunas>``: a chave estrangeira sem a linha referenciada, só com
  ``foreign_keys=True``.

As funções SQL que mudam de nome entre os motores são subclasses de ``FunctionElement`` com uma
regra ``@compiles`` por dialeto. Elas não registram nada em ``sa.func``.

Exemplo:

.. code-block:: python

    from serialize_db import audit

    for name, text in audit.audit_sql(Operacao.__table__, "redshift", partitions=["2026-08-31"],
                                      prefix="exec_42_").items():
        print(f"-- {name}\\n{text}")
"""

from __future__ import annotations

import dataclasses
from collections.abc import Mapping, Sequence
from typing import Literal

import pyarrow as pa
import sqlalchemy as sa
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.functions import FunctionElement

from serialize_db import sql
from serialize_db.schema import (
    PARTITION_VALUE,
    TEXT_LIMIT,
    UUID_LENGTH,
    Dialect,
    TableOptions,
    check_partition_value,
    double_columns,
    foreign_keys_by_columns,
    sequential_key,
    table_options,
)

__all__ = ["AuditReport", "Check", "CheckResult", "KeyScope", "audit_sql", "checks"]

KeyScope = Literal["partition", "table"]
"""[public] O escopo da unicidade na verificação ``chave_<colunas>_tabela``.

- ``"partition"`` confere a chave só nas partições da execução.
- ``"table"`` confere a chave contra as demais partições da versão fixada também na chave com a
  coluna de ``partition_source``.
- ``None`` confere contra a versão fixada só a chave sem a coluna de partição e sem a de
  ``partition_source``."""

# O teto de linhas de amostra de cada verificação reprovada, no relatório e no log.
SAMPLE_ROWS = 20


# ---------------------------------------------------------------- as funções por dialeto


def _by_name(
    element: FunctionElement,
    compiler: object,
    **kw: object,
) -> str:
    """[private] A regra de compilação padrão das funções deste módulo: o nome da função com os
    argumentos. Ela vale para os dialetos sem regra própria e para o ``str`` de um statement. Sem
    uma regra padrão, uma subclasse de ``FunctionElement`` não compila fora dos dialetos que a
    declaram, e o dialeto do DuckDB é um compilador do PostgreSQL."""
    return f"{element.name}({compiler.process(element.clauses, **kw)})"


class _partition_text(FunctionElement):  # noqa: N801 - o nome segue o da função SQL
    """[private] O texto ``AAAA-MM-DD`` de uma data: ``strftime`` no DuckDB e ``to_char`` no
    Redshift. Serve aos contadores de defeito da verificação de linhas."""

    name = "partition_text"
    type = sa.String()
    inherit_cache = True


compiles(_partition_text)(_by_name)


@compiles(_partition_text, "duckdb")
def _duckdb_partition_text(
    element: _partition_text,
    compiler: object,
    **kw: object,
) -> str:
    return f"strftime({compiler.process(element.clauses, **kw)}, '%Y-%m-%d')"


@compiles(_partition_text, "redshift")
def _redshift_partition_text(
    element: _partition_text,
    compiler: object,
    **kw: object,
) -> str:
    return f"to_char({compiler.process(element.clauses, **kw)}, 'YYYY-MM-DD')"


class _json_valid(FunctionElement):  # noqa: N801 - o nome segue o da função SQL
    """[private] Se o texto é um documento JSON: ``json_valid`` no DuckDB e ``true`` no Redshift.
    No Redshift a coluna JSON é ``SUPER``, cujo valor sempre se serializa como documento JSON, e o
    ``is_valid_json`` recusa ``SUPER`` (42883, leitura de 2026-09-23). Serve aos contadores de
    defeito da verificação de linhas."""

    name = "json_valid"
    type = sa.Boolean()
    inherit_cache = True


compiles(_json_valid)(_by_name)


@compiles(_json_valid, "redshift")
def _redshift_json_valid(
    element: _json_valid,
    compiler: object,
    **kw: object,
) -> str:
    return "true"


class _text_bytes(FunctionElement):  # noqa: N801 - o nome segue o da função SQL
    """[private] O comprimento do texto em bytes, a medida do ``VARCHAR(n)`` do Redshift:
    ``strlen`` no DuckDB e ``octet_length`` no Redshift. O ``length`` dos dois motores conta
    caracteres, não bytes. Serve aos contadores de defeito da verificação de linhas."""

    name = "text_bytes"
    type = sa.Integer()
    inherit_cache = True


compiles(_text_bytes)(_by_name)


@compiles(_text_bytes, "duckdb")
def _duckdb_text_bytes(
    element: _text_bytes,
    compiler: object,
    **kw: object,
) -> str:
    return f"strlen({compiler.process(element.clauses, **kw)})"


@compiles(_text_bytes, "redshift")
def _redshift_text_bytes(
    element: _text_bytes,
    compiler: object,
    **kw: object,
) -> str:
    return f"octet_length({compiler.process(element.clauses, **kw)})"


class _json_bytes(FunctionElement):  # noqa: N801 - o nome segue o da função SQL
    """[private] O tamanho do documento JSON em bytes: ``strlen`` do texto no DuckDB, onde a
    coluna é ``JSON`` ou ``VARCHAR``, e ``json_size`` no Redshift, onde ela é ``SUPER``. Serve aos
    contadores de defeito da verificação de linhas."""

    name = "json_bytes"
    type = sa.Integer()
    inherit_cache = True


compiles(_json_bytes)(_by_name)


@compiles(_json_bytes, "duckdb")
def _duckdb_json_bytes(
    element: _json_bytes,
    compiler: object,
    **kw: object,
) -> str:
    return f"strlen(CAST({compiler.process(element.clauses, **kw)} AS VARCHAR))"


@compiles(_json_bytes, "redshift")
def _redshift_json_bytes(
    element: _json_bytes,
    compiler: object,
    **kw: object,
) -> str:
    return f"json_size({compiler.process(element.clauses, **kw)})"


class _partition_value_valid(FunctionElement):  # noqa: N801 - o nome segue o da função SQL
    """[private] Se o valor de partição segue ``schema.PARTITION_VALUE`` por inteiro:
    ``regexp_full_match`` no DuckDB e o operador POSIX ``~`` com as âncoras no Redshift. Serve aos
    contadores de defeito da verificação de linhas."""

    name = "partition_value_valid"
    type = sa.Boolean()
    inherit_cache = True


compiles(_partition_value_valid)(_by_name)


@compiles(_partition_value_valid, "duckdb")
def _duckdb_partition_value_valid(
    element: _partition_value_valid,
    compiler: object,
    **kw: object,
) -> str:
    return f"regexp_full_match({compiler.process(element.clauses, **kw)}, '{PARTITION_VALUE}')"


@compiles(_partition_value_valid, "redshift")
def _redshift_partition_value_valid(
    element: _partition_value_valid,
    compiler: object,
    **kw: object,
) -> str:
    return f"({compiler.process(element.clauses, **kw)} ~ '^{PARTITION_VALUE}$')"


class _is_finite(FunctionElement):  # noqa: N801 - o nome segue o da função SQL
    """[private] Se o ``Double`` é finito: ``isfinite`` no DuckDB e, no Redshift, o valor
    estritamente entre os infinitos. Serve às somas de controle da verificação de linhas.

    A condição só entra afirmada, nunca negada, pelas leituras de 2026-09-23 no Redshift. Numa
    constante, o Redshift comparou o ``NaN`` igual a si mesmo, como o PostgreSQL, e a comparação
    estrita com os infinitos deu falso a ele. Na varredura de uma tabela, nem essa comparação nem a
    negação dela deram verdadeiro ao ``NaN``, e ``NOT IN ('NaN'::float8, ...)`` o deu por finito.
    Por isso a soma de controle corre onde a condição é verdadeira, e os não finitos são os não
    nulos menos os finitos (``_totals``).
    """

    name = "isfinite"
    type = sa.Boolean()
    inherit_cache = True


compiles(_is_finite)(_by_name)


@compiles(_is_finite, "redshift")
def _redshift_is_finite(
    element: _is_finite,
    compiler: object,
    **kw: object,
) -> str:
    value = compiler.process(element.clauses, **kw)
    return f"({value} > '-Infinity'::float8 AND {value} < 'Infinity'::float8)"


# ---------------------------------------------------------------- os tipos


@dataclasses.dataclass(frozen=True)
class Check:
    """[public] Uma verificação da auditoria: o statement Core sobre a tabela do contrato e o que
    a reprova.

    Exemplo:

    .. code-block:: python

        check = checks(Operacao.__table__, ["2026-08-31"])[0]
        check.name, check.fails_when   # ("linhas", "algum contador acima de zero")
    """

    name: str
    """[public] O nome da verificação: ``linhas``, ``chave_<colunas>``, ``chave_<colunas>_tabela``
    ou ``orfao_<colunas>``."""
    statement: sa.Select
    """[public] A consulta da verificação. Na verificação de linhas, ela devolve uma linha por
    partição; nas demais, uma linha por defeito."""
    fails_when: str
    """[public] O que reprova a verificação: ``"algum contador acima de zero"`` na de linhas e
    ``"alguma linha"`` nas demais."""
    skip_when: sa.Select | None = None
    """[public] Uma consulta de uma linha booleana. Quando ela devolve verdadeiro, a verificação é
    aprovada sem rodar."""
    counters: Mapping[str, sa.ColumnElement] = dataclasses.field(default_factory=dict)
    """[public] Na verificação de linhas, a condição de cada contador de defeito, pelo rótulo do
    contador. A amostra da reprovação é ``SELECT * ... WHERE <condição> LIMIT 20``."""


@dataclasses.dataclass(frozen=True)
class CheckResult:
    """[public] O resultado de uma verificação, uma entrada de ``AuditReport.results``.

    Exemplo:

    .. code-block:: python

        result = engine.audit(Operacao.__table__, ["2026-08-31"]).results[0]
        result.name, result.passed, result.defects   # ("linhas", True, 0)
    """

    name: str
    """[public] O nome da verificação, o mesmo de ``Check.name``."""
    sql: str
    """[public] O texto SQL que o motor rodou."""
    defects: int
    """[public] O número de defeitos. Na verificação de linhas, a soma dos contadores de defeito;
    nas demais, as linhas devolvidas."""
    sample: pa.Table
    """[public] Até 20 linhas reprovadas; vazia quando a verificação passou."""
    passed: bool
    """[public] Se a verificação passou: sem defeito, ou dispensada pelo ``skip_when``."""
    reason: str = ""
    """[public] O motivo da aprovação sem rodar, quando o ``skip_when`` dispensou a verificação."""


@dataclasses.dataclass(frozen=True)
class AuditReport:
    """[public] O relatório de uma auditoria: o resultado de cada verificação, as verificações que
    não rodaram e as leituras da verificação de linhas.

    Exemplo:

    .. code-block:: python

        report = engine.audit(Operacao.__table__, ["2026-08-31"], uri, version)
        report.passed                              # True quando nenhuma verificação reprovou
        report.nonfinite_columns["2026-08-31"]     # ("valor",) com NaN na coluna
        print(report.sql())                        # o texto de todas, para o log
    """

    table: str
    """[public] O nome da tabela auditada."""
    partitions: tuple[str, ...] | None
    """[public] As partições auditadas; ``None`` quando a auditoria cobriu a tabela inteira do
    sandbox."""
    results: tuple[CheckResult, ...]
    """[public] O resultado de cada verificação da lista de ``checks``, na ordem dela."""
    not_run: tuple[str, ...]
    """[public] As verificações que não rodaram, cada uma com o motivo. ``orfao_*`` não roda sem
    ``foreign_keys=True``, nem com a tabela referenciada fora do sandbox e sem versão fixada.
    ``chave_<colunas>_tabela`` não roda com ``key_scope="partition"`` nem sem versão fixada."""
    nonfinite_columns: Mapping[str | None, tuple[str, ...]]
    """[public] Por valor de partição, as colunas ``Double`` com ``NaN`` ou infinito. É a lista que
    ``publish_delta`` passa a ``export_partition`` como ``columns_without_min_max``."""
    totals: Mapping[str | None, Mapping[str, object]]
    """[public] Por valor de partição, a linha da verificação de linhas: a contagem de linhas, os
    contadores de defeito, as somas de controle e as contagens de não finitos."""

    @property
    def passed(self) -> bool:
        """[public] Se todas as verificações que rodaram passaram."""
        return all(result.passed for result in self.results)

    def sql(self) -> str:
        """[public] O texto SQL de todas as verificações.

        Exemplo:

        .. code-block:: python

            print(report.sql())   # cada verificação com o nome num comentário e o texto dela

        :return: um bloco por verificação, com o nome num comentário SQL e o texto dela, e uma
            linha em branco entre os blocos.
        """
        blocks = []
        for result in self.results:
            blocks.append(f"-- {result.name}\n{result.sql}")
        return "\n\n".join(blocks)

    def rows(
        self,
        value: str | None,
    ) -> int:
        """[public] As linhas que a auditoria contou numa partição.

        Exemplo:

        .. code-block:: python

            report.rows("2026-08-31")   # 1000

        :param value: o valor de partição; ``None`` numa tabela sem partição.
        :return: a contagem da verificação de linhas; 0 quando a partição não tem linha.
        """
        return int(self.totals.get(value, {}).get("linhas", 0))


# ---------------------------------------------------------------- as verificações


def _scope(
    table: sa.Table,
    partitions: Sequence[str] | None,
) -> sa.ColumnElement:
    """[private] A condição que restringe uma consulta às partições da execução. Numa tabela sem
    partição, ou sem a lista de partições, a condição é sempre verdadeira."""
    partition_by = table_options(table).partition_by
    if partition_by is None or partitions is None:
        return sa.true()
    return table.c[partition_by].in_(list(partitions))


def _defect_counters(
    table: sa.Table,
) -> dict[str, sa.ColumnElement]:
    """[private] A condição de cada contador de defeito da verificação de linhas, pelo rótulo do
    contador."""
    options = table_options(table)
    counters = {}
    # Os contadores de cada coluna: o nulo em NOT NULL, o JSON inválido e o texto acima do limite.
    for column in table.columns:
        if not column.nullable:
            counters[f"nulo_{column.name}"] = column.is_(None)
        # Text é subclasse de String e vem antes: o limite dela é o teto do VARCHAR do Redshift.
        if isinstance(column.type, sa.JSON):
            invalid = sa.not_(_json_valid(column))
            counters[f"json_{column.name}"] = sa.and_(column.isnot(None), invalid)
            counters[f"texto_{column.name}"] = _json_bytes(column) > TEXT_LIMIT
        elif isinstance(column.type, sa.Text):
            counters[f"texto_{column.name}"] = _text_bytes(column) > TEXT_LIMIT
        elif isinstance(column.type, sa.String) and column.type.length:
            counters[f"texto_{column.name}"] = _text_bytes(column) > column.type.length
        elif isinstance(column.type, sa.Uuid):
            # O CAST leva a texto a coluna UUID nativa que o pipeline cria no DuckDB por SQL
            # (gen_random_uuid()), que o strlen não aceita e a exportação converte em VARCHAR(36).
            as_text = sa.cast(column, sa.String())
            counters[f"texto_{column.name}"] = _text_bytes(as_text) > UUID_LENGTH
    # Os contadores da partição: o valor fora da regra e o que difere da data de partition_source.
    if options.partition_by is not None:
        partition = table.c[options.partition_by]
        counters[f"valor_{options.partition_by}"] = sa.not_(_partition_value_valid(partition))
        if options.partition_source is not None:
            derived = _partition_text(table.c[options.partition_source])
            counters[f"particao_{options.partition_by}"] = partition != derived
    return counters


def _totals(
    table: sa.Table,
) -> list[sa.ColumnElement]:
    """[private] As somas de controle de cada coluna ``Numeric`` e ``Double`` como
    ``DECIMAL(38, 6)``, e a contagem dos não finitos de cada ``Double``.

    A soma de uma coluna ``Double`` corre só nos valores finitos, porque o ``CAST`` de um ``NaN``
    ou de um infinito para ``DECIMAL`` falha, e o ``FILTER`` do agregado não o evita. Os não
    finitos são os não nulos menos os finitos, sem negar ``_is_finite``: na varredura de uma
    tabela, o Redshift não deu verdadeiro à negação para o ``NaN``.
    """
    sums = []
    nonfinite = []
    for column in table.columns:
        as_decimal = sa.cast(column, sa.Numeric(38, 6))
        # Double deriva de Numeric e por isso vem antes do ramo das Numeric.
        if isinstance(column.type, sa.Double):
            finite = sa.case((_is_finite(column), as_decimal))
            sums.append(sa.func.sum(finite).label(f"total_{column.name}"))
            not_finite = sa.func.count(column) - _count_where(_is_finite(column))
            nonfinite.append(not_finite.label(f"naofinito_{column.name}"))
        elif isinstance(column.type, sa.Numeric):
            sums.append(sa.func.sum(as_decimal).label(f"total_{column.name}"))
    return sums + nonfinite


def _count_where(
    condition: sa.ColumnElement,
) -> sa.ColumnElement:
    """[private] A contagem das linhas em que a condição vale, por
    ``count(CASE WHEN <condição> THEN 1 END)``. O Redshift não tem a cláusula ``FILTER`` nos
    agregados; o ``CASE`` sem ``ELSE`` dá nulo nas outras linhas, e o ``count`` não conta o nulo."""
    return sa.func.count(sa.case((condition, 1)))


def _rows_check(
    table: sa.Table,
    partitions: Sequence[str] | None,
) -> Check:
    """[private] A verificação de linhas: os contadores de defeito, as somas de controle e os não
    finitos, por partição."""
    partition_by = table_options(table).partition_by
    counters = _defect_counters(table)
    measures = [sa.func.count().label("linhas")]
    for label, condition in counters.items():
        measures.append(_count_where(condition).label(label))
    measures.extend(_totals(table))
    # Uma linha só numa tabela sem partição, e uma por partição numa particionada.
    if partition_by is None:
        statement = sa.select(*measures).where(_scope(table, partitions))
    else:
        partition = table.c[partition_by]
        statement = (
            sa.select(partition, *measures)
            .where(_scope(table, partitions))
            .group_by(partition)
            .order_by(partition)
        )
    return Check("linhas", statement, "algum contador acima de zero", counters=counters)


def _key_label(
    key: Sequence[str],
) -> str:
    """[private] A parte ``<colunas>`` do nome de uma verificação de chave ou de órfão: as colunas
    juntadas por ``_``."""
    return "_".join(key)


def _key_within(
    table: sa.Table,
    key: Sequence[str],
    partitions: Sequence[str] | None,
) -> Check:
    """[private] A verificação ``chave_<colunas>``: a chave repetida nas partições da execução."""
    columns = [table.c[name] for name in key]
    statement = (
        sa.select(*columns, sa.func.count().label("n"))
        .where(_scope(table, partitions))
        .group_by(*columns)
        .having(sa.func.count() > 1)
        .order_by(*columns)
    )
    return Check(f"chave_{_key_label(key)}", statement, "alguma linha")


def _single_integer_key(
    table: sa.Table,
    key: Sequence[str],
) -> sa.Column | None:
    """[private] A coluna de ``key`` quando ``key`` é a chave sequencial da tabela, a chave primária
    inteira de uma só coluna, que ``next_ids`` preenche. ``None`` nas outras chaves."""
    column = sequential_key(table)
    if column is None or list(key) != [column.name]:
        return None
    return column


def _key_against_pinned(
    table: sa.Table,
    key: Sequence[str],
    partitions: Sequence[str],
    pinned: sa.FromClause,
    pinned_max_key: int | None,
) -> Check:
    """[private] A verificação ``chave_<colunas>_tabela``: a chave das partições da execução
    repetida nas demais partições da versão fixada.

    Na chave primária inteira de uma coluna, com ``pinned_max_key``, a verificação ganha um
    ``skip_when``. Ele a aprova sem a junção quando o menor valor da chave na execução passa do
    maior valor da versão fixada.
    """
    partition_by = table_options(table).partition_by
    columns = [table.c[name] for name in key]
    joined = sa.and_(*[table.c[name] == pinned.c[name] for name in key])
    outside = pinned.c[partition_by].notin_(list(partitions))
    statement = (
        sa.select(*columns)
        .select_from(table.join(pinned, joined))
        .where(_scope(table, partitions), outside)
        .distinct()
    )
    skip_when = None
    integer_key = _single_integer_key(table, key)
    if integer_key is not None and pinned_max_key is not None:
        above = sa.func.min(integer_key) > pinned_max_key
        skip_when = sa.select(above.label("dispensa")).where(_scope(table, partitions))
    return Check(f"chave_{_key_label(key)}_tabela", statement, "alguma linha", skip_when)


def _orphans(
    table: sa.Table,
    constraint: sa.ForeignKeyConstraint,
    referenced: sa.FromClause,
    partitions: Sequence[str] | None,
) -> Check:
    """[private] A verificação ``orfao_<colunas>``: as linhas da execução cuja chave estrangeira
    não acha a linha referenciada."""
    local = [table.c[column.name] for column in constraint.columns]
    remote = [referenced.c[element.column.name] for element in constraint.elements]
    pairs = zip(local, remote, strict=True)
    matches = sa.and_(*[remote_column == local_column for local_column, remote_column in pairs])
    # Uma chave com coluna nula não aponta linha alguma e fica fora da verificação.
    present = sa.and_(*[column.isnot(None) for column in local])
    missing = ~sa.exists(sa.select(sa.literal(1)).select_from(referenced).where(matches))
    statement = sa.select(*local).where(_scope(table, partitions), present, missing).distinct()
    label = _key_label([column.name for column in constraint.columns])
    return Check(f"orfao_{label}", statement, "alguma linha")


def _needs_pinned_check(
    options: TableOptions,
    key: Sequence[str],
    partitions: Sequence[str] | None,
    key_scope: KeyScope | None,
) -> bool:
    """[private] Se a chave pede a verificação contra as demais partições da versão fixada. Ela
    pede só numa tabela particionada, com as partições da execução, e numa chave sem a coluna de
    partição. A chave com a coluna de ``partition_source`` só pede com ``key_scope="table"``."""
    if options.partition_by is None or partitions is None:
        return False
    if options.partition_by in key:
        return False
    if options.partition_source in key and key_scope != "table":
        return False
    return True


def _key_checks(
    table: sa.Table,
    partitions: Sequence[str] | None,
    key_scope: KeyScope | None,
    pinned: sa.FromClause | None,
    pinned_max_key: int | None,
) -> tuple[list[Check], list[str]]:
    """[private] As verificações de chave de uma tabela, e as que não rodam, cada uma com o
    motivo."""
    options = table_options(table)
    found = []
    not_run = []
    for key in options.keys:
        found.append(_key_within(table, key, partitions))
        if not _needs_pinned_check(options, key, partitions, key_scope):
            continue
        name = f"chave_{_key_label(key)}_tabela"
        if key_scope == "partition":
            not_run.append(f"{name} (key_scope=partition)")
        elif pinned is None:
            not_run.append(f"{name} (sem versão fixada)")
        else:
            found.append(_key_against_pinned(table, key, partitions, pinned, pinned_max_key))
    return found, not_run


def _foreign_key_checks(
    table: sa.Table,
    partitions: Sequence[str] | None,
    foreign_keys: bool,
    referenced: Mapping[str, sa.FromClause] | None,
) -> tuple[list[Check], list[str]]:
    """[private] As verificações de órfão, o anti-join de cada chave estrangeira, e as que não
    rodam, cada uma com o motivo."""
    found = []
    not_run = []
    for constraint in foreign_keys_by_columns(table):
        label = _key_label([column.name for column in constraint.columns])
        target = constraint.referred_table.name
        if not foreign_keys:
            not_run.append(f"orfao_{label} (sem foreign_keys=True)")
        elif referenced is None or target not in referenced:
            not_run.append(f"orfao_{label} ({target} fora do sandbox e sem versão fixada)")
        else:
            found.append(_orphans(table, constraint, referenced[target], partitions))
    return found, not_run


def checks(
    table: sa.Table,
    partitions: Sequence[str] | None = None,
    foreign_keys: bool = False,
    key_scope: KeyScope | None = None,
    pinned: sa.FromClause | None = None,
    referenced: Mapping[str, sa.FromClause] | None = None,
    pinned_max_key: int | None = None,
) -> list[Check]:
    """[public] As verificações do contrato sobre uma tabela do modelo, para as partições da
    execução.

    Cada verificação é um ``Check`` com o statement Core sobre a tabela do contrato; o motor o
    roda, e ``audit_sql`` o renderiza. A lista depende dos argumentos: a verificação de linhas
    sempre entra, as de chave entram uma por chave da tabela, e as de órfão só com
    ``foreign_keys=True``.

    Exemplo:

    .. code-block:: python

        names = [check.name for check in checks(Operacao.__table__, ["2026-08-31"],
                                                pinned=pinned)]
        # ["linhas", "chave_id_operacao", "chave_id_operacao_tabela"]

    :param table: a tabela do modelo.
    :param partitions: as partições da execução, cada uma pela regra da partição;
        ``partitions=None`` audita a tabela inteira do sandbox.
    :param foreign_keys: ``foreign_keys=True`` roda a verificação ``orfao_<colunas>`` de cada
        chave estrangeira, a chave sem a linha referenciada.
    :param key_scope: o escopo da unicidade na verificação ``chave_<colunas>_tabela``, a da
        chave contra as demais partições da versão fixada. O padrão faz a verificação na chave
        sem a coluna de partição e sem a de ``partition_source``. ``key_scope="partition"`` a
        suprime, e ``key_scope="table"`` a faz também na chave com a coluna de
        ``partition_source``.
    :param pinned: a versão fixada da tabela, a origem das demais partições na verificação
        ``chave_<colunas>_tabela``; sem ela, essa verificação não roda.
    :param referenced: por nome de tabela, a origem da linha referenciada de cada chave
        estrangeira; só é conferida com ``foreign_keys=True``.
    :param pinned_max_key: o ``max_key`` da versão fixada, que dá o ``skip_when`` à chave
        primária inteira de uma coluna.
    :return: a verificação de linhas, as de chave e as de órfão, nessa ordem. As verificações que
        não rodam ficam fora da lista, e o relatório do motor as registra em ``not_run``.
    :raises serialize_db.errors.ContractError: um valor de ``partitions`` fora da regra da partição.
    """
    found, _ = checks_and_not_run(
        table, partitions, foreign_keys, key_scope, pinned, referenced, pinned_max_key
    )
    return found


def checks_and_not_run(
    table: sa.Table,
    partitions: Sequence[str] | None,
    foreign_keys: bool,
    key_scope: KeyScope | None,
    pinned: sa.FromClause | None,
    referenced: Mapping[str, sa.FromClause] | None,
    pinned_max_key: int | None,
) -> tuple[list[Check], list[str]]:
    """[protected] As verificações de ``checks`` e as que ela deixa de fora, cada uma com o motivo.
    O ``audit_report`` de ``serialize_db.engine`` a chama para montar o relatório dos motores."""
    if partitions is not None:
        for value in partitions:
            check_partition_value(value)
    found = [_rows_check(table, partitions)]
    keys, keys_not_run = _key_checks(table, partitions, key_scope, pinned, pinned_max_key)
    orphans, orphans_not_run = _foreign_key_checks(table, partitions, foreign_keys, referenced)
    return found + keys + orphans, keys_not_run + orphans_not_run


def readings_by_partition(
    rows: Sequence[Mapping[str, object]],
    table: sa.Table,
) -> tuple[dict[str | None, dict[str, object]], dict[str | None, tuple[str, ...]]]:
    """[protected] As leituras da verificação de linhas por valor de partição, e as colunas
    ``Double`` com valor não finito em cada partição. O ``audit_report`` de ``serialize_db.engine``
    a usa para montar o relatório dos motores.

    Cada linha do resultado da verificação é uma partição. O valor da coluna de partição é a chave
    dos dois dicionários, ``None`` numa tabela sem partição. As demais colunas da linha são as
    leituras: a contagem, os contadores, as somas e os não finitos.
    """
    partition_by = table_options(table).partition_by
    doubles = double_columns(table)
    totals = {}
    nonfinite = {}
    for row in rows:
        value = row[partition_by] if partition_by else None
        readings = {name: reading for name, reading in row.items() if name != partition_by}
        totals[value] = readings
        nonfinite[value] = tuple(name for name in doubles if readings[f"naofinito_{name}"])
    return totals, nonfinite


def failing_counters(
    counters: Mapping[str, sa.ColumnElement],
    totals: Mapping[str | None, Mapping[str, object]],
) -> tuple[int, list[str]]:
    """[protected] A soma dos contadores de defeito em todas as partições, e os rótulos dos
    contadores acima de zero. O ``audit_report`` de ``serialize_db.engine`` a usa no resultado da
    verificação de linhas."""
    defects = 0
    failing = []
    for label in counters:
        counted = sum(readings[label] for readings in totals.values())
        defects += counted
        if counted:
            failing.append(label)
    return defects, failing


def sample_statement(
    table: sa.Table,
    partitions: Sequence[str] | None,
    condition: sa.ColumnElement,
) -> sa.Select:
    """[protected] A consulta de até 20 linhas inteiras em que a condição de um contador vale, nas
    partições da execução. Os motores a rodam para a amostra da verificação de linhas reprovada."""
    return sa.select(table).where(_scope(table, partitions), condition).limit(SAMPLE_ROWS)


def audit_sql(
    table: sa.Table,
    dialect: Dialect,
    partitions: Sequence[str] | None = None,
    foreign_keys: bool = False,
    key_scope: KeyScope | None = None,
    pinned: sa.FromClause | None = None,
    referenced: Mapping[str, sa.FromClause] | None = None,
    prefix: str = sql.SENTINEL,
) -> dict[str, str]:
    """[public] O texto SQL de cada verificação no dialeto de um motor, para depuração.

    A função renderiza por ``sql.render`` os statements de ``checks``, com o prefixo pedido, sem
    abrir conexão e sem construir um motor. O texto é o SQL que a auditoria roda nesse motor.

    Exemplo:

    .. code-block:: python

        audit_sql(Operacao.__table__, "duckdb", ["2026-08-31"], prefix="")["linhas"]
        # 'SELECT "cad_operacoes"."data_str", count(*) AS linhas, ...'

    :param table: a tabela do modelo.
    :param dialect: o motor, ``"duckdb"`` ou ``"redshift"``.
    :param partitions: as partições da execução, cada uma pela regra da partição;
        ``partitions=None`` audita a tabela inteira do sandbox.
    :param foreign_keys: ``foreign_keys=True`` roda a verificação ``orfao_<colunas>`` de cada
        chave estrangeira, a chave sem a linha referenciada.
    :param key_scope: o escopo da unicidade na verificação ``chave_<colunas>_tabela``, a da
        chave contra as demais partições da versão fixada. O padrão faz a verificação na chave
        sem a coluna de partição e sem a de ``partition_source``. ``key_scope="partition"`` a
        suprime, e ``key_scope="table"`` a faz também na chave com a coluna de
        ``partition_source``.
    :param pinned: a versão fixada da tabela, a origem das demais partições na verificação
        ``chave_<colunas>_tabela``; sem ela, essa verificação não roda.
    :param referenced: por nome de tabela, a origem da linha referenciada de cada chave
        estrangeira; só é conferida com ``foreign_keys=True``.
    :param prefix: o prefixo das tabelas no texto. O padrão deixa no texto o sentinela
        ``{prefix}``, o marcador que a execução troca pelo prefixo do sandbox. ``prefix=""`` dá o
        texto sobre as tabelas do contrato, o que o motor DuckDB roda.
    :return: ``{nome: texto}``, na ordem de ``checks``.
    :raises serialize_db.errors.ContractError: um valor de ``partitions`` fora da regra da partição.
    """
    texts = {}
    for check in checks(table, partitions, foreign_keys, key_scope, pinned, referenced):
        texts[check.name] = sql.render(check.statement, dialect, table.metadata, prefix)
    return texts
