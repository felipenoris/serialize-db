"""Biblioteca comum das sondas de consistência: o modelo com toda coluna do contrato, os valores
de borda, a pasta de trabalho, a comparação valor a valor e o relatório.

Uma sonda de consistência atravessa uma fronteira de leitura e escrita do pacote com valores de
borda e trabalho paralelo, e compara valor a valor o que saiu com o que entrou. Ao contrário dos
probes de ``probes/``, ela grava: em ``<SERIALIZE_DB_TEST_LOCAL_ROOT>/consistencia/<sonda>/``,
pasta que ``probe_folder`` recria no início e ``finish`` apaga no fim
(``SERIALIZE_DB_TEST_KEEP`` a mantém). Cada leitura sai no terminal e em
``probes/output/consistencia_<sonda>_<data-hora>.txt``. Código de saída: 0 quando toda checagem
passou, 1 quando alguma reprovou, 2 sem a pasta de trabalho.

Cada checagem é um ``report(título, problemas)``: ``OK`` sem problema, ``PROBLEMAS`` com a lista.
A diferença conhecida do sinal do zero pelo ``COPY`` do DuckDB (``plan/OPEN_QUESTIONS.md``) sai
das checagens por ``known_zero_sign`` e é impressa como leitura, até a decisão do usuário.
"""
from __future__ import annotations

import datetime
import decimal
import importlib.metadata
import json
import math
import os
import platform
import shutil
import sys
import time
import uuid as uuidlib
from pathlib import Path

import pyarrow as pa
import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from serialize_db import schema

OUTPUT_DIR = Path(__file__).resolve().parents[1] / "output"
ROOT_VARIABLE = "SERIALIZE_DB_TEST_LOCAL_ROOT"
KEEP_VARIABLE = "SERIALIZE_DB_TEST_KEEP"
PACKAGES = ("deltalake", "duckdb", "pyarrow", "pandas", "sqlalchemy")


class Base(DeclarativeBase):
    pass


TUDO_INFO = {"serialize_db": {"partition_by": ["data_str"], "partition_source": "data",
                              "sort_key": ["data", "id"]}}


class Tudo(Base):
    """A tabela com toda coluna do contrato, particionada por ``data_str`` e única em ``texto``."""

    __tablename__ = "cad_tudo"
    __table_args__ = (sa.UniqueConstraint("texto"), {"info": TUDO_INFO})
    id: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=False)
    pequeno: Mapped[int | None] = mapped_column(sa.SmallInteger)
    inteiro: Mapped[int | None] = mapped_column(sa.Integer)
    flag: Mapped[bool | None] = mapped_column(sa.Boolean)
    valor: Mapped[float | None] = mapped_column(sa.Double)
    preco: Mapped[decimal.Decimal | None] = mapped_column(sa.Numeric(18, 2))
    grande: Mapped[decimal.Decimal | None] = mapped_column(sa.Numeric(38, 10))
    data: Mapped[datetime.date] = mapped_column(sa.Date)
    carimbo: Mapped[datetime.datetime | None] = mapped_column(sa.DateTime)
    carimbo_tz: Mapped[datetime.datetime | None] = mapped_column(sa.DateTime(timezone=True))
    texto: Mapped[str | None] = mapped_column(sa.String(50))
    longo: Mapped[str | None] = mapped_column(sa.Text)
    ident: Mapped[str | None] = mapped_column(sa.Uuid)
    doc: Mapped[dict | None] = mapped_column(sa.JSON)
    data_str: Mapped[str] = mapped_column(sa.String(10))


class Simples(Base):
    """A tabela sem partição de três colunas, para as estatísticas exatas do log."""

    __tablename__ = "cad_simples"
    id: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=False)
    nome: Mapped[str | None] = mapped_column(sa.String(20))
    valor: Mapped[float | None] = mapped_column(sa.Double)


TUDO = Tudo.__table__
SIMPLES = Simples.__table__
UTC = datetime.timezone.utc

# Os valores de borda de cada tipo: o texto vazio, o NUL, o emoji, os 50 bytes exatos, o zero
# com os dois sinais, o menor subnormal, os extremos do double, NaN, os infinitos, o Decimal
# extremo de cada escala, as datas 0001-01-01 e 9999-12-31, o instante máximo do pandas.
TEXTS = ["", "a", "ção", "😀 emoji", "quote'single", "back\\slash", "tab\there", "new\nline",
         "nul\x00char", "z" * 50, "Z", "ÿ", "é́ combining", "﷽ wide", "~tilde", " space",
         "\x7f del", "a" * 50, "ﬀ ligature", "𝔘 math"]
DOUBLES = [0.0, -0.0, 5e-324, 1.7976931348623157e308, -1.7976931348623157e308, 0.1, 1 / 3,
           2.675, 1e-300, 123456789.123456789, 1e16 + 1, 9007199254740993.0, -2.5, 1e308, 1.0,
           float("nan"), float("inf"), float("-inf"), None, 3.141592653589793]
PRICES = [decimal.Decimal("9999999999999999.99"), decimal.Decimal("-9999999999999999.99"),
          decimal.Decimal("0.00"), decimal.Decimal("0.01"), decimal.Decimal("-0.01"), None]
BIG = [decimal.Decimal("9999999999999999999999999999.9999999999"),
       decimal.Decimal("-9999999999999999999999999999.9999999999"),
       decimal.Decimal("0.0000000001"), decimal.Decimal("1234567890123456789.0123456789"), None]
DATES = [datetime.date(1, 1, 1), datetime.date(9999, 12, 31), datetime.date(1970, 1, 1),
         datetime.date(1969, 12, 31), datetime.date(2000, 2, 29), datetime.date(1582, 10, 4)]
STAMPS = [datetime.datetime(1970, 1, 1), datetime.datetime(2262, 4, 11, 23, 47, 16, 854775),
          datetime.datetime(1000, 1, 1, 0, 0, 0, 1),
          datetime.datetime(9999, 12, 31, 23, 59, 59, 999999),
          datetime.datetime(1969, 12, 31, 23, 59, 59, 999999),
          datetime.datetime(2026, 9, 25, 12, 0, 0, 123456), None,
          datetime.datetime(1900, 1, 1), datetime.datetime(1, 1, 1, 0, 0, 0, 0)]
DOCS = ['{"a": 1}', '[1, 2, 3]', 'null', '"str"', '{"n": {"m": [1, {"k": "v"}]}}',
        '{"e": "ção 😀"}', '1.5', 'true', '{"s": "quote\\"d"}', None]
SMALL_INTS = [-32768, 32767, 0, -1, 1, None]
INTS = [-2147483648, 2147483647, 0, -1, 1, None]
FLAGS = [True, False, None]


def pick(values: list, index: int) -> object:
    """O valor de ``values`` na posição ``index``, dando a volta na lista."""
    return values[index % len(values)]


def edge_rows(value: str, start: int, count: int) -> pa.Table:
    """``count`` linhas de ``cad_tudo`` na partição ``value``, com ids de ``start`` em diante e
    cada coluna percorrendo os seus valores de borda, no contrato por ``schema.cast``."""
    ids = list(range(start, start + count))
    day = datetime.date.fromisoformat(value)
    stamps_tz = []
    for index in ids:
        stamp = pick(STAMPS, index + 1)
        stamps_tz.append(stamp.replace(tzinfo=UTC) if stamp is not None else None)
    # A restrição única em texto: o id no valor evita a colisão, e o nulo a cada sete linhas.
    texts = [f"{pick(TEXTS, i)}|{i}"[:50] if i % 7 else None for i in ids]
    table = pa.table({
        "id": pa.array(ids, pa.int64()),
        "pequeno": pa.array([pick(SMALL_INTS, i) for i in ids], pa.int16()),
        "inteiro": pa.array([pick(INTS, i) for i in ids], pa.int32()),
        "flag": pa.array([pick(FLAGS, i) for i in ids], pa.bool_()),
        "valor": pa.array([pick(DOUBLES, i) for i in ids], pa.float64()),
        "preco": pa.array([pick(PRICES, i) for i in ids], pa.decimal128(18, 2)),
        "grande": pa.array([pick(BIG, i) for i in ids], pa.decimal128(38, 10)),
        "data": pa.array([day] * count, pa.date32()),
        "carimbo": pa.array([pick(STAMPS, i) for i in ids], pa.timestamp("us")),
        "carimbo_tz": pa.array(stamps_tz, pa.timestamp("us", "UTC")),
        "texto": pa.array(texts, pa.string()),
        "longo": pa.array([("L" * (i % 3000)) + f"|{i}" if i % 5 else None for i in ids],
                          pa.string()),
        "ident": pa.array([str(uuidlib.UUID(int=i)) if i % 4 else None for i in ids],
                          pa.string()),
        "doc": pa.array([pick(DOCS, i) for i in ids], pa.string()),
        "data_str": pa.array([value] * count, pa.string()),
    })
    return schema.cast(table, TUDO)


def same(a: object, b: object) -> bool:
    """Se dois valores lidos são o mesmo: ``NaN`` igual a ``NaN``, e o zero com o seu sinal."""
    if isinstance(a, float) and isinstance(b, float):
        if math.isnan(a) and math.isnan(b):
            return True
        return a == b and math.copysign(1, a) == math.copysign(1, b)
    return a == b


def compare(expected: pa.Table, found: pa.Table, key: str = "id", label: str = "",
            json_columns: tuple[str, ...] = ()) -> list[str]:
    """As diferenças entre duas tabelas ordenadas por ``key``: a contagem de linhas, e por
    coluna o tipo e o primeiro valor diferente, com quantos há; uma coluna em ``json_columns``
    compara os documentos, porque o Redshift reserializa o texto JSON."""
    problems = []
    if found.num_rows != expected.num_rows:
        problems.append(f"{label}: {found.num_rows} linhas, esperadas {expected.num_rows}")
    sorted_expected = expected.sort_by(key)
    sorted_found = found.sort_by(key)
    for name in sorted_expected.column_names:
        if name not in sorted_found.column_names:
            problems.append(f"{label}: coluna {name} ausente")
            continue
        expected_column = sorted_expected.column(name)
        found_column = sorted_found.column(name)
        if expected_column.type != found_column.type:
            problems.append(f"{label}: {name} no tipo {found_column.type}, "
                            f"esperado {expected_column.type}")
        expected_values = expected_column.to_pylist()
        found_values = found_column.to_pylist()
        if name in json_columns:
            expected_values = [json.loads(v) if v is not None else None for v in expected_values]
            found_values = [json.loads(v) if v is not None else None for v in found_values]
        differences = []
        for position, (a, b) in enumerate(zip(expected_values, found_values)):
            if not same(a, b):
                differences.append((position, a, b))
        if differences:
            position, a, b = differences[0]
            problems.append(f"{label}: {name}: {len(differences)} diferenças, a primeira na "
                            f"linha {position}: esperado {a!r}, encontrado {b!r}")
    return problems


def to_contract(found: pa.Table, table: sa.Table, notes: set[str]) -> pa.Table:
    """As colunas do contrato de ``found`` levadas aos tipos dele por ``schema.cast``, anotando em
    ``notes`` cada tipo cru diferente do contrato (o fuso da sessão, ``large_string``, a extensão
    JSON), que é leitura e não diferença de valor."""
    contract = schema.arrow_schema(table)
    for field in contract:
        raw_type = found.schema.field(field.name).type
        if raw_type != field.type:
            notes.add(f"{field.name}: {raw_type} contra {field.type} do contrato")
    return schema.cast(found.select(contract.names), table)


def print_notes(notes: set[str]) -> None:
    """Imprime os tipos crus anotados por ``to_contract``."""
    print("tipos crus dos leitores contra o contrato:")
    for note in sorted(notes):
        print("   ", note)


def known_zero_sign(problems: list[str]) -> tuple[list[str], list[str]]:
    """Separa a diferença conhecida do sinal do zero em ``valor`` (o ``COPY`` do DuckDB) das
    demais: devolve as demais e as conhecidas."""
    known = []
    for problem in problems:
        zero_swapped = ("esperado -0.0, encontrado 0.0" in problem
                        or "esperado 0.0, encontrado -0.0" in problem)
        if "valor: " in problem and zero_swapped:
            known.append(problem)
    rest = [problem for problem in problems if problem not in known]
    return rest, known


# --------------------------------------------------------------------------------------------------
# A pasta de trabalho, a saída duplicada no arquivo e o relatório

class Tee:
    """Escreve ao mesmo tempo no terminal e no arquivo de saída."""

    def __init__(self, path: Path) -> None:
        self.file = path.open("w", encoding="utf-8")
        self.terminal = sys.stdout

    def write(self, text: str) -> int:
        self.terminal.write(text)
        self.file.write(text)
        self.file.flush()
        return len(text)

    def flush(self) -> None:
        self.terminal.flush()
        self.file.flush()


FAILED: list[str] = []
PASSED: list[str] = []


def probe_folder(name: str) -> Path:
    """A pasta de trabalho da sonda ``name``, vazia, sob ``SERIALIZE_DB_TEST_LOCAL_ROOT``; sem a
    variável, ou com uma pasta inexistente, a sonda para com o código 2. Também abre o arquivo de
    saída em ``probes/output/`` e imprime o cabeçalho com a data, a plataforma e as versões."""
    root = os.environ.get(ROOT_VARIABLE)
    if not root or not Path(root).is_dir():
        print(f"{ROOT_VARIABLE} ausente ou apontando para uma pasta inexistente: {root!r}; "
              f"a sonda grava só sob essa pasta.", file=sys.stderr)
        sys.exit(2)
    folder = Path(root) / "consistencia" / name
    shutil.rmtree(folder, ignore_errors=True)
    folder.mkdir(parents=True)
    OUTPUT_DIR.mkdir(exist_ok=True)
    output = OUTPUT_DIR / f"consistencia_{name}_{time.strftime('%Y%m%d-%H%M%S')}.txt"
    sys.stdout = Tee(output)
    versions = ", ".join(f"{p} {importlib.metadata.version(p)}" for p in PACKAGES)
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S %z')}; {platform.platform()}; "
          f"python {platform.python_version()}; {versions}")
    print(f"pasta de trabalho: {folder}; saída: {output}")
    return folder


def report(title: str, problems: list[str]) -> None:
    """Imprime a checagem ``title`` como ``OK`` ou ``PROBLEMAS`` com a lista, e a registra para o
    código de saída."""
    print(f"== {title}: {'OK' if not problems else 'PROBLEMAS'}")
    for problem in problems:
        print("   -", problem)
    (FAILED if problems else PASSED).append(title)


def finish(folder: Path) -> None:
    """Imprime o resumo das checagens, apaga a pasta de trabalho (``SERIALIZE_DB_TEST_KEEP`` a
    mantém) e encerra com o código 1 quando alguma checagem reprovou."""
    print(f"checagens: {len(PASSED) + len(FAILED)}, reprovadas: {len(FAILED)}")
    for title in FAILED:
        print("   reprovada:", title)
    if os.environ.get(KEEP_VARIABLE):
        print(f"pasta mantida por {KEEP_VARIABLE}: {folder}")
    else:
        shutil.rmtree(folder, ignore_errors=True)
    sys.stdout.flush()
    sys.exit(1 if FAILED else 0)
