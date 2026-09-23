"""As verificações que a auditoria roda antes de publicar, derivadas do contrato.

Chave primária, unicidade e chave estrangeira ficam fora do DDL dos dois sandboxes; a auditoria é
onde elas são aplicadas, com consultas montadas a partir do ``Table`` do modelo, sem declaração
adicional. ``checks`` devolve a lista de ``Check``, cada um com o statement Core sobre a tabela do
contrato, e não sabe qual motor o roda; ``audit_sql`` renderiza o texto de cada verificação por
``sql.render``, para depuração. O motor roda as verificações e monta o ``AuditReport``.

As verificações:

- **linhas**: uma consulta, agrupada pela coluna de partição numa tabela particionada, que conta
  por coluna o nulo em ``NOT NULL``, o texto acima de ``String(n)`` em bytes, o texto de uma coluna
  ``Text`` e o documento JSON acima de 65.535 bytes, o teto do Redshift, o JSON inválido, a
  coluna de partição diferente da derivação de ``partition_source`` e o valor de partição fora de
  ``schema.PARTITION_VALUE``; e que soma cada coluna ``Numeric`` e ``Double`` como
  ``DECIMAL(38, 6)`` (a ``Double`` só nos valores finitos) e conta à parte os não finitos, uma
  leitura que não reprova;
- **chave_<colunas>**: a chave repetida nas partições da execução, uma consulta por chave;
- **chave_<colunas>_publicada**: a chave repetida entre as partições da execução e as demais da
  versão publicada, quando a chave não inclui a coluna de partição nem a de ``partition_source``;
- **orfao_<colunas>**: a chave estrangeira sem a linha referenciada, só com ``foreign_keys=True``.

As funções que mudam de nome entre os motores são subclasses de ``FunctionElement`` com uma regra
``@compiles`` por dialeto, e não registram nada em ``sa.func``.

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
    Dialect,
    check_partition_value,
    foreign_keys_by_columns,
    sequential_key,
    table_options,
)

__all__ = ["AuditReport", "Check", "CheckResult", "KeyScope", "audit_sql", "checks"]

KeyScope = Literal["partition", "table"]
"""O escopo da unicidade: ``"partition"`` confere a chave só nas partições da execução."""

# O teto de linhas de amostra de cada verificação reprovada, no relatório e no log.
SAMPLE_ROWS = 20


# ---------------------------------------------------------------- as funções por dialeto


def _by_name(element: FunctionElement, compiler: object, **kw: object) -> str:
    """A regra padrão: o nome da função com os argumentos, para os dialetos sem regra própria e para
    o ``str`` de um statement. Uma subclasse de ``FunctionElement`` sem regra padrão não compila
    fora dos dialetos que a declaram, e o dialeto do DuckDB é um compilador do PostgreSQL."""
    return f"{element.name}({compiler.process(element.clauses, **kw)})"


class partition_text(FunctionElement):  # noqa: N801 - o nome da classe é o da função SQL
    """O texto ``AAAA-MM-DD`` de uma data: ``strftime`` no DuckDB e ``to_char`` no Redshift."""

    name = "partition_text"
    type = sa.String()
    inherit_cache = True


compiles(partition_text)(_by_name)


@compiles(partition_text, "duckdb")
def _duckdb_partition_text(element: partition_text, compiler: object, **kw: object) -> str:
    return f"strftime({compiler.process(element.clauses, **kw)}, '%Y-%m-%d')"


@compiles(partition_text, "redshift")
def _redshift_partition_text(element: partition_text, compiler: object, **kw: object) -> str:
    return f"to_char({compiler.process(element.clauses, **kw)}, 'YYYY-MM-DD')"


class json_valid(FunctionElement):  # noqa: N801 - o nome da classe é o da função SQL
    """Se o texto é um documento JSON: ``json_valid`` no DuckDB; no Redshift, ``true``, porque a
    coluna JSON é ``SUPER``, cujo valor sempre se serializa como documento JSON, e o
    ``is_valid_json`` recusa ``SUPER`` (42883, leitura de 2026-09-23)."""

    name = "json_valid"
    type = sa.Boolean()
    inherit_cache = True


compiles(json_valid)(_by_name)


@compiles(json_valid, "redshift")
def _redshift_json_valid(element: json_valid, compiler: object, **kw: object) -> str:
    return "true"


class text_bytes(FunctionElement):  # noqa: N801 - o nome da classe é o da função SQL
    """O comprimento do texto em bytes, a medida do ``VARCHAR(n)`` do Redshift: ``strlen`` no DuckDB
    e ``octet_length`` no Redshift, porque o ``length`` dos dois conta caracteres."""

    name = "text_bytes"
    type = sa.Integer()
    inherit_cache = True


compiles(text_bytes)(_by_name)


@compiles(text_bytes, "duckdb")
def _duckdb_text_bytes(element: text_bytes, compiler: object, **kw: object) -> str:
    return f"strlen({compiler.process(element.clauses, **kw)})"


@compiles(text_bytes, "redshift")
def _redshift_text_bytes(element: text_bytes, compiler: object, **kw: object) -> str:
    return f"octet_length({compiler.process(element.clauses, **kw)})"


class json_bytes(FunctionElement):  # noqa: N801 - o nome da classe é o da função SQL
    """O tamanho do documento JSON em bytes: ``strlen`` do texto no DuckDB, onde a coluna é ``JSON``
    ou ``VARCHAR``, e ``json_size`` no Redshift, onde ela é ``SUPER``."""

    name = "json_bytes"
    type = sa.Integer()
    inherit_cache = True


compiles(json_bytes)(_by_name)


@compiles(json_bytes, "duckdb")
def _duckdb_json_bytes(element: json_bytes, compiler: object, **kw: object) -> str:
    return f"strlen(CAST({compiler.process(element.clauses, **kw)} AS VARCHAR))"


@compiles(json_bytes, "redshift")
def _redshift_json_bytes(element: json_bytes, compiler: object, **kw: object) -> str:
    return f"json_size({compiler.process(element.clauses, **kw)})"


class partition_value_valid(FunctionElement):  # noqa: N801 - o nome da classe é o da função SQL
    """Se o valor de partição segue ``schema.PARTITION_VALUE`` por inteiro: ``regexp_full_match`` no
    DuckDB e o operador POSIX ``~`` com as âncoras no Redshift."""

    name = "partition_value_valid"
    type = sa.Boolean()
    inherit_cache = True


compiles(partition_value_valid)(_by_name)


@compiles(partition_value_valid, "duckdb")
def _duckdb_partition_value_valid(element: partition_value_valid, compiler: object,
                                  **kw: object) -> str:
    return f"regexp_full_match({compiler.process(element.clauses, **kw)}, '{PARTITION_VALUE}')"


@compiles(partition_value_valid, "redshift")
def _redshift_partition_value_valid(element: partition_value_valid, compiler: object,
                                    **kw: object) -> str:
    return f"({compiler.process(element.clauses, **kw)} ~ '^{PARTITION_VALUE}$')"


class is_finite(FunctionElement):  # noqa: N801 - o nome da classe é o da função SQL
    """Se o ``Double`` é finito: ``isfinite`` no DuckDB; no Redshift, estritamente entre os
    infinitos.

    O Redshift comparou o ``NaN`` igual a si mesmo numa constante, como o PostgreSQL, e diferente de
    tudo na varredura de uma tabela, como o IEEE (leituras de 2026-09-23). A comparação estrita com
    os infinitos dá falso ao ``NaN`` pelas duas regras: o PostgreSQL o põe acima de todo número, e
    no IEEE toda comparação com ele é falsa. ``NOT IN ('NaN'::float8, ...)`` o dava por finito na
    tabela.
    """

    name = "isfinite"
    type = sa.Boolean()
    inherit_cache = True


compiles(is_finite)(_by_name)


@compiles(is_finite, "redshift")
def _redshift_is_finite(element: is_finite, compiler: object, **kw: object) -> str:
    value = compiler.process(element.clauses, **kw)
    return f"({value} > '-Infinity'::float8 AND {value} < 'Infinity'::float8)"


# ---------------------------------------------------------------- os tipos


@dataclasses.dataclass(frozen=True)
class Check:
    """Uma verificação: o statement Core sobre a tabela do contrato e o que o reprova.

    Exemplo:

    .. code-block:: python

        check = checks(Operacao.__table__, ["2026-08-31"])[0]
        check.name, check.fails_when   # ("linhas", "algum contador acima de zero")
    """

    name: str
    """``linhas``, ``chave_<colunas>``, ``chave_<colunas>_publicada`` ou ``orfao_<colunas>``."""
    statement: sa.Select
    """A consulta; na de linhas, uma linha por partição, e nas demais uma linha por defeito."""
    fails_when: str
    """``"algum contador acima de zero"`` na de linhas, ``"alguma linha"`` nas demais."""
    skip_when: sa.Select | None = None
    """Uma consulta de uma linha booleana que, verdadeira, aprova a verificação sem rodá-la."""
    counters: Mapping[str, sa.ColumnElement] = dataclasses.field(default_factory=dict)
    """Na de linhas, a condição de cada contador de defeito, pelo rótulo dele: a amostra da
    reprovação é ``SELECT * ... WHERE <condição> LIMIT 20``."""


@dataclasses.dataclass(frozen=True)
class CheckResult:
    """O resultado de uma verificação no relatório."""

    name: str
    sql: str
    """O texto que o motor rodou."""
    defects: int
    """Na de linhas, a soma dos contadores de defeito; nas demais, as linhas devolvidas."""
    sample: pa.Table
    """Até 20 linhas reprovadas; vazia na aprovação."""
    passed: bool
    reason: str = ""
    """Por que a verificação foi aprovada sem rodar, quando o ``skip_when`` a dispensou."""


@dataclasses.dataclass(frozen=True)
class AuditReport:
    """O relatório de uma auditoria: o resultado de cada verificação, o que não rodou e as leituras.

    Exemplo:

    .. code-block:: python

        report = engine.audit(Operacao.__table__, ["2026-08-31"], uri, version)
        report.passed                              # True quando nenhuma verificação reprovou
        report.nonfinite_columns["2026-08-31"]     # ("valor",) com NaN na coluna
        print(report.sql())                        # o texto de todas, para o log
    """

    table: str
    partitions: tuple[str, ...] | None
    results: tuple[CheckResult, ...]
    not_run: tuple[str, ...]
    """As verificações que não rodaram, com o motivo: ``orfao_*`` sem ``foreign_keys=True``, a chave
    publicada com ``key_scope="partition"`` ou sem versão publicada."""
    nonfinite_columns: Mapping[str | None, tuple[str, ...]]
    """Por valor de partição, as colunas ``Double`` com ``NaN`` ou infinito: a lista que a
    publicação passa a ``export_partition`` como ``columns_without_min_max``."""
    totals: Mapping[str | None, Mapping[str, object]]
    """Por valor de partição, a linha da verificação de linhas: a contagem, os contadores, as somas
    de controle e os não finitos."""

    @property
    def passed(self) -> bool:
        """Se todas as verificações que rodaram passaram."""
        return all(result.passed for result in self.results)

    def sql(self) -> str:
        """O texto de todas as verificações, uma por bloco com o nome em comentário."""
        blocks = []
        for result in self.results:
            blocks.append(f"-- {result.name}\n{result.sql}")
        return "\n\n".join(blocks)

    def rows(self, value: str | None) -> int:
        """As linhas da partição na auditoria, 0 quando ela não tem linha."""
        return int(self.totals.get(value, {}).get("linhas", 0))


# ---------------------------------------------------------------- as verificações


def _scope(table: sa.Table, partitions: Sequence[str] | None) -> sa.ColumnElement:
    """A condição das partições da execução; verdadeira numa tabela sem partição ou sem lista."""
    partition_by = table_options(table).partition_by
    if partition_by is None or partitions is None:
        return sa.true()
    return table.c[partition_by].in_(list(partitions))


def _defect_counters(table: sa.Table) -> dict[str, sa.ColumnElement]:
    """A condição de cada contador de defeito da verificação de linhas, pelo rótulo."""
    options = table_options(table)
    counters = {}
    for column in table.columns:
        if not column.nullable:
            counters[f"nulo_{column.name}"] = column.is_(None)
        if isinstance(column.type, sa.JSON):
            invalid = sa.not_(json_valid(column))
            counters[f"json_{column.name}"] = sa.and_(column.isnot(None), invalid)
            counters[f"texto_{column.name}"] = json_bytes(column) > TEXT_LIMIT
        elif isinstance(column.type, sa.Text):
            counters[f"texto_{column.name}"] = text_bytes(column) > TEXT_LIMIT
        elif isinstance(column.type, sa.String) and column.type.length:
            counters[f"texto_{column.name}"] = text_bytes(column) > column.type.length
    if options.partition_by is not None:
        partition = table.c[options.partition_by]
        counters[f"valor_{options.partition_by}"] = sa.not_(partition_value_valid(partition))
        if options.partition_source is not None:
            derived = partition_text(table.c[options.partition_source])
            counters[f"particao_{options.partition_by}"] = partition != derived
    return counters


def _totals(table: sa.Table) -> list[sa.ColumnElement]:
    """As somas de controle como ``DECIMAL(38, 6)`` e a contagem dos não finitos de cada ``Double``.

    A soma de uma coluna ``Double`` corre só nos valores finitos: o ``CAST`` de um ``NaN`` ou de um
    infinito para ``DECIMAL`` falha, e o ``FILTER`` do agregado não o evita.
    """
    sums = []
    nonfinite = []
    for column in table.columns:
        as_decimal = sa.cast(column, sa.Numeric(38, 6))
        if isinstance(column.type, sa.Double):
            finite = sa.case((is_finite(column), as_decimal))
            sums.append(sa.func.sum(finite).label(f"total_{column.name}"))
            not_finite = _count_where(sa.not_(is_finite(column)))
            nonfinite.append(not_finite.label(f"naofinito_{column.name}"))
        elif isinstance(column.type, sa.Numeric):
            sums.append(sa.func.sum(as_decimal).label(f"total_{column.name}"))
    return sums + nonfinite


def _count_where(condition: sa.ColumnElement) -> sa.ColumnElement:
    """As linhas em que a condição vale, por ``count(CASE WHEN <condição> THEN 1 END)``: o Redshift
    não tem a cláusula ``FILTER`` nos agregados, e o ``CASE`` sem ``ELSE`` dá nulo, que o ``count``
    não conta."""
    return sa.func.count(sa.case((condition, 1)))


def _rows_check(table: sa.Table, partitions: Sequence[str] | None) -> Check:
    """A verificação de linhas: os contadores de defeito, as somas e os não finitos, por
    partição."""
    partition_by = table_options(table).partition_by
    counters = _defect_counters(table)
    measures = [sa.func.count().label("linhas")]
    for label, condition in counters.items():
        measures.append(_count_where(condition).label(label))
    measures.extend(_totals(table))
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


def _key_label(key: Sequence[str]) -> str:
    """O nome de uma verificação de chave: as colunas juntadas por ``_``."""
    return "_".join(key)


def _key_within(table: sa.Table, key: Sequence[str], partitions: Sequence[str] | None) -> Check:
    """A chave repetida nas partições da execução."""
    columns = [table.c[name] for name in key]
    statement = (
        sa.select(*columns, sa.func.count().label("n"))
        .where(_scope(table, partitions))
        .group_by(*columns)
        .having(sa.func.count() > 1)
        .order_by(*columns)
    )
    return Check(f"chave_{_key_label(key)}", statement, "alguma linha")


def _single_integer_key(table: sa.Table, key: Sequence[str]) -> sa.Column | None:
    """A coluna de ``key`` quando ``key`` é a chave sequencial da tabela, a chave primária inteira
    de uma coluna que ``next_ids`` preenche; ``None`` nas outras chaves."""
    column = sequential_key(table)
    if column is None or list(key) != [column.name]:
        return None
    return column


def _key_against_published(table: sa.Table, key: Sequence[str], partitions: Sequence[str],
                           published: sa.FromClause, published_max_key: int | None) -> Check:
    """A chave das partições da execução repetida nas demais partições da versão publicada.

    Na chave primária inteira de uma coluna, com ``published_max_key``, o ``skip_when`` aprova a
    verificação sem a junção quando o menor valor da execução passa do maior publicado.
    """
    partition_by = table_options(table).partition_by
    columns = [table.c[name] for name in key]
    joined = sa.and_(*[table.c[name] == published.c[name] for name in key])
    outside = published.c[partition_by].notin_(list(partitions))
    statement = (
        sa.select(*columns)
        .select_from(table.join(published, joined))
        .where(_scope(table, partitions), outside)
        .distinct()
    )
    skip_when = None
    integer_key = _single_integer_key(table, key)
    if integer_key is not None and published_max_key is not None:
        above = sa.func.min(integer_key) > published_max_key
        skip_when = sa.select(above.label("dispensa")).where(_scope(table, partitions))
    return Check(f"chave_{_key_label(key)}_publicada", statement, "alguma linha", skip_when)


def _orphans(table: sa.Table, constraint: sa.ForeignKeyConstraint, referenced: sa.FromClause,
             partitions: Sequence[str] | None) -> Check:
    """As linhas da execução cuja chave estrangeira não acha a linha referenciada."""
    local = [table.c[column.name] for column in constraint.columns]
    remote = [referenced.c[element.column.name] for element in constraint.elements]
    matches = sa.and_(*[there == here for here, there in zip(local, remote)])
    present = sa.and_(*[column.isnot(None) for column in local])
    missing = ~sa.exists(sa.select(sa.literal(1)).select_from(referenced).where(matches))
    statement = sa.select(*local).where(_scope(table, partitions), present, missing).distinct()
    label = _key_label([column.name for column in constraint.columns])
    return Check(f"orfao_{label}", statement, "alguma linha")


def _key_checks(table: sa.Table, partitions: Sequence[str] | None, key_scope: KeyScope | None,
                published: sa.FromClause | None,
                published_max_key: int | None) -> tuple[list[Check], list[str]]:
    """As verificações de chave e as que não rodam, com o motivo."""
    options = table_options(table)
    found = []
    not_run = []
    for key in options.keys:
        found.append(_key_within(table, key, partitions))
        name = f"chave_{_key_label(key)}_publicada"
        in_partition = options.partition_by in key
        in_source = options.partition_source in key and key_scope != "table"
        if options.partition_by is None or partitions is None or in_partition or in_source:
            continue
        if key_scope == "partition":
            not_run.append(f"{name} (key_scope=partition)")
        elif published is None:
            not_run.append(f"{name} (sem versão publicada)")
        else:
            found.append(_key_against_published(table, key, partitions, published,
                                                published_max_key))
    return found, not_run


def _foreign_key_checks(
    table: sa.Table, partitions: Sequence[str] | None, foreign_keys: bool,
    referenced: Mapping[str, sa.FromClause] | None,
) -> tuple[list[Check], list[str]]:
    """Os anti-joins das chaves estrangeiras e as que não rodam, com o motivo."""
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


def checks(table: sa.Table, partitions: Sequence[str] | None = None, foreign_keys: bool = False,
           key_scope: KeyScope | None = None, published: sa.FromClause | None = None,
           referenced: Mapping[str, sa.FromClause] | None = None,
           published_max_key: int | None = None) -> list[Check]:
    """As verificações do contrato para as partições da execução, sobre a tabela do modelo.

    ``partitions=None`` audita a tabela inteira do sandbox. ``published`` é a versão publicada da
    tabela, a origem das demais partições na verificação de chave que não inclui a coluna de
    partição nem a de ``partition_source``; ``key_scope="partition"`` a suprime, e
    ``key_scope="table"`` a faz também na chave com a coluna de ``partition_source``.
    ``referenced`` dá, por nome de tabela, a origem da linha referenciada de cada chave
    estrangeira, que só é conferida com ``foreign_keys=True``. ``published_max_key``, o
    ``max_key`` da versão publicada, dá à chave primária inteira de uma coluna o ``skip_when``.
    As verificações que não rodam ficam fora da lista, e o relatório do motor as registra.

    Exemplo:

    .. code-block:: python

        names = [check.name for check in checks(Operacao.__table__, ["2026-08-31"],
                                                published=published)]
        # ["linhas", "chave_id_operacao", "chave_id_operacao_publicada"]
    """
    return checks_and_not_run(table, partitions, foreign_keys, key_scope, published, referenced,
                              published_max_key)[0]


def checks_and_not_run(table: sa.Table, partitions: Sequence[str] | None, foreign_keys: bool,
                       key_scope: KeyScope | None, published: sa.FromClause | None,
                       referenced: Mapping[str, sa.FromClause] | None,
                       published_max_key: int | None) -> tuple[list[Check], list[str]]:
    """As verificações de ``checks`` e as que ela deixa de fora, com o motivo; protegida, para o
    relatório dos motores."""
    if partitions is not None:
        for value in partitions:
            check_partition_value(value)
    found = [_rows_check(table, partitions)]
    keys, keys_not_run = _key_checks(table, partitions, key_scope, published, published_max_key)
    orphans, orphans_not_run = _foreign_key_checks(table, partitions, foreign_keys, referenced)
    return found + keys + orphans, keys_not_run + orphans_not_run


def sample_statement(table: sa.Table, partitions: Sequence[str] | None,
                     condition: sa.ColumnElement) -> sa.Select:
    """Até 20 linhas inteiras em que a condição de um contador vale, nas partições da execução;
    protegida, para a amostra que os motores buscam na reprovação da verificação de linhas."""
    return sa.select(table).where(_scope(table, partitions), condition).limit(SAMPLE_ROWS)


def audit_sql(table: sa.Table, dialect: Dialect, partitions: Sequence[str] | None = None,
              foreign_keys: bool = False, key_scope: KeyScope | None = None,
              published: sa.FromClause | None = None,
              referenced: Mapping[str, sa.FromClause] | None = None,
              prefix: str = sql.SENTINEL) -> dict[str, str]:
    """``{nome: texto}`` de cada verificação no dialeto, por ``sql.render`` com o prefixo pedido,
    sem conexão e sem motor: o SQL que a auditoria roda, para depuração.

    Com o prefixo padrão o texto sai com o sentinela ``{prefix}``; ``prefix=""`` dá o texto sobre as
    tabelas do contrato, o que o motor DuckDB roda.

    Exemplo:

    .. code-block:: python

        audit_sql(Operacao.__table__, "duckdb", ["2026-08-31"], prefix="")["linhas"]
        # 'SELECT "cad_operacoes"."data_str", count(*) AS linhas, ...'
    """
    texts = {}
    for check in checks(table, partitions, foreign_keys, key_scope, published, referenced):
        texts[check.name] = sql.render(check.statement, dialect, table.metadata, prefix)
    return texts
