"""A estrutura da base atual em Parquet particionado: uma pasta por tabela, lida arquivo por arquivo.

A base de origem da carga inicial (etapa 7) é um conjunto de pastas Parquet. Este probe fotografa a
estrutura dela para duas perguntas: quais são os campos, os tipos e as faixas de valores de cada
tabela, de modo que um script de teste possa gerar uma base fictícia com a mesma forma; e se todos
os arquivos de todas as partições de uma tabela têm o mesmo esquema, que é o que quebra uma leitura
posicional.

Uso:

    .venv/bin/python probes/parquet_source.py <raiz> [--sample N] [--files N] [--text-bytes]

``<raiz>`` é a pasta que contém uma subpasta por tabela, ou uma URI (``s3://bucket/prefixo``); cada
subpasta tem um arquivo Parquet ou uma árvore de partições com vários. ``--sample N`` lê as N
primeiras linhas de um arquivo por tabela para medir cardinalidade e comprimento de texto, em
caracteres e em bytes, que o rodapé não guarda; sem ele nenhuma página de dados é lida. ``--files N`` limita quantos arquivos de
cada tabela aparecem na listagem por arquivo, sem limitar quantos são lidos. ``--text-bytes`` lê
as colunas de texto de todos os arquivos, linha por linha, e mede o maior valor de cada coluna em
bytes e em caracteres: é a medida do ``String(n)`` do contrato, e a varredura é longa.

Só leitura. Todo arquivo da base é aberto por ``open_input_file``, e o relatório sai no terminal e
em ``probes/output/parquet_source_<data-hora>.txt``, dentro do repositório. Nada é criado, alterado
ou apagado sob a raiz.

Seções:

1. A raiz: o sistema de arquivos, as pastas de tabela e o que não é Parquet.
2. As tabelas: arquivos, bytes, linhas, partições e esquemas distintos de cada uma.
3. O esquema de cada tabela: coluna, tipo Arrow, nulidade, tipo físico e tipo lógico do Parquet.
4. Divergências de esquema: os arquivos cujo esquema difere do majoritário, coluna a coluna.
5. As partições: as colunas de partição, os seus valores e se elas também estão dentro dos arquivos.
6. Estatísticas por coluna: nulos, mínimo, máximo e distintos, somados do rodapé de cada arquivo.
7. Layout físico: row groups, compressão, codificação, escritor e metadados do rodapé.
8. Amostra de valores: só com ``--sample``.

Cada seção é uma função, na ordem acima, que documenta as checagens que emite (``PQ-1`` a ``PQ-9``);
``main`` as chama uma a uma, e uma seção que quebra não cala as outras. Códigos de saída: 0 quando
toda checagem passou, 1 quando alguma leitura falhou, 2 quando alguma checagem reprovou.
"""

from __future__ import annotations

import dataclasses
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
from probelib import Report, describe_error  # noqa: E402

# Um segmento de caminho no layout Hive: mes=2026-08. Um segmento que não casa é um nível sem nome.
HIVE_SEGMENT = re.compile(r"^([^=/]+)=(.*)$")

# Quantos arquivos de cada tabela a listagem por arquivo mostra, e quantos valores distintos a amostra lista.
DEFAULT_FILE_ROWS = 20
MAX_LISTED_VALUES = 25
TEXT_BATCH_ROWS = 100_000


@dataclasses.dataclass
class Column:
    """Uma coluna de um arquivo, pelo esquema Arrow e pela folha correspondente do esquema Parquet."""

    name: str
    arrow_type: str
    nullable: bool
    physical: str
    logical: str
    converted: str
    field_id: str


@dataclasses.dataclass
class FileReading:
    """O rodapé de um arquivo Parquet: o esquema, o layout e as estatísticas de cada coluna."""

    path: str  # relativo à pasta da tabela
    size: int
    rows: int
    row_groups: int
    created_by: str
    format_version: str
    columns: list[Column]
    footer: dict[str, str]
    partition: tuple[tuple[str, str], ...]
    partition_kind: str
    compression: tuple[str, ...]
    encodings: tuple[str, ...]
    statistics: dict[str, dict[str, Any]]


# ---------------------------------------------------------------------------------------------------------------
# Funções puras: o caminho, o esquema como chave, a soma das estatísticas e a formatação


def partition_of(relative: str) -> tuple[tuple[tuple[str, str], ...], str]:
    """As colunas de partição de um caminho relativo à pasta da tabela, e como elas foram escritas.

    ``mes=2026-08/parte.parquet`` dá ``(("mes", "2026-08"),)`` e ``hive``; ``2026/08/parte.parquet``
    dá níveis numerados e ``pastas sem nome``, porque o nome da coluna não está no caminho; um
    arquivo na raiz da tabela dá ``()`` e ``sem partição``.
    """
    segments = [segment for segment in relative.split("/")[:-1] if segment]
    if not segments:
        return (), "sem partição"
    matches = [HIVE_SEGMENT.match(segment) for segment in segments]
    if all(matches):
        return tuple((match.group(1), match.group(2)) for match in matches if match), "hive"
    return tuple((f"nível {index}", segment) for index, segment in enumerate(segments, 1)), "pastas sem nome"


def logical_label(text: str) -> str:
    """O tipo lógico do Parquet em forma curta, com os parâmetros que distinguem um tipo do outro.

    ``Timestamp(isAdjustedToUTC=false, timeUnit=microseconds, is_from_converted_type=false,
    force_set_converted_type=false)`` vira ``Timestamp(us, utc=false)``, que é o que decide entre
    ``timestamp_ntz`` e ``timestamp`` no Delta; ``Decimal(precision=18, scale=2)`` vira
    ``Decimal(18,2)``. Um tipo sem parâmetro sai como veio.
    """
    units = {"milliseconds": "ms", "microseconds": "us", "nanoseconds": "ns", "seconds": "s"}
    match = re.match(r"^(Timestamp|Time)\(isAdjustedToUTC=(\w+), timeUnit=(\w+)", text)
    if match:
        return f"{match.group(1)}({units.get(match.group(3), match.group(3))}, utc={match.group(2)})"
    match = re.match(r"^Decimal\(precision=(\d+), scale=(\d+)\)", text)
    if match:
        return f"Decimal({match.group(1)},{match.group(2)})"
    match = re.match(r"^Int\(bitWidth=(\d+), isSigned=(\w+)\)", text)
    if match:
        return f"Int({match.group(1)}, com sinal={match.group(2)})"
    return text


def schema_key(columns: list[Column]) -> tuple[tuple[str, str, bool, str, str], ...]:
    """O esquema como chave comparável: nome, tipo Arrow, nulidade, tipo físico e tipo lógico, na ordem."""
    return tuple((column.name, column.arrow_type, column.nullable, column.physical, column.logical) for column in columns)


def schema_difference(reference: list[Column], other: list[Column]) -> list[list[str]]:
    """As linhas que explicam por que dois esquemas diferem: coluna ausente, a mais, trocada de tipo ou de posição."""
    by_name = {column.name: column for column in reference}
    other_by_name = {column.name: column for column in other}
    rows: list[list[str]] = []

    for name, column in by_name.items():
        found = other_by_name.get(name)
        if found is None:
            rows.append([name, "ausente", f"{column.arrow_type} no majoritário", "-"])
            continue
        if found.arrow_type != column.arrow_type:
            rows.append([name, "tipo", column.arrow_type, found.arrow_type])
        if found.nullable != column.nullable:
            rows.append([name, "nulidade", "nulo" if column.nullable else "não nulo", "nulo" if found.nullable else "não nulo"])
        if found.physical != column.physical or found.logical != column.logical:
            rows.append([name, "tipo físico", f"{column.physical}/{column.logical}", f"{found.physical}/{found.logical}"])

    for name in other_by_name:
        if name not in by_name:
            rows.append([name, "a mais", "-", other_by_name[name].arrow_type])

    # Mesmas colunas e mesmos tipos, mas em outra ordem: quebra toda leitura posicional, como o COPY do Redshift.
    if not rows and [column.name for column in reference] != [column.name for column in other]:
        rows.append(["(todas)", "ordem", ", ".join(column.name for column in reference), ", ".join(column.name for column in other)])
    return rows


def merge_statistics(readings: list[FileReading]) -> dict[str, dict[str, Any]]:
    """Soma as estatísticas de rodapé de vários arquivos numa entrada por coluna.

    ``rows`` e ``nulls`` são somas; ``min`` e ``max`` são o extremo de todos os arquivos que os
    trazem; ``distinct`` é o maior visto num arquivo, um piso da cardinalidade da tabela, nunca a
    soma, porque um valor pode repetir entre arquivos; ``without`` conta os arquivos sem estatística
    de mínimo e máximo naquela coluna.
    """
    merged: dict[str, dict[str, Any]] = {}
    for reading in readings:
        for name, statistic in reading.statistics.items():
            entry = merged.setdefault(
                name, {"rows": 0, "nulls": 0, "nulls_known": False, "min": None, "max": None, "distinct": None, "without": 0}
            )
            entry["rows"] += statistic.get("rows", 0)
            if statistic.get("nulls") is not None:
                entry["nulls"] += statistic["nulls"]
                entry["nulls_known"] = True
            if statistic.get("distinct") is not None:
                entry["distinct"] = max(entry["distinct"] or 0, statistic["distinct"])
            if statistic.get("min") is None and statistic.get("max") is None:
                entry["without"] += 1
                continue
            for edge, better in (("min", lambda a, b: b < a), ("max", lambda a, b: b > a)):
                value = statistic.get(edge)
                if value is None:
                    continue
                current = entry[edge]
                try:
                    if current is None or better(current, value):
                        entry[edge] = value
                except TypeError:
                    # Tipos que não se comparam entre arquivos (um bytes ao lado de um str): fica o primeiro lido.
                    pass
    return merged


def format_value(value: Any, limit: int = 44) -> str:
    """Um valor de estatística legível numa célula: bytes decodificados, texto sem quebra, cortado em ``limit``."""
    if value is None:
        return "-"
    if isinstance(value, (bytes, bytearray)):
        try:
            text = bytes(value).decode("utf-8")
        except UnicodeDecodeError:
            return f"0x{bytes(value).hex()}"[: limit + 2]
    else:
        text = str(value)
    text = text.replace("\n", "\\n").replace("\t", "\\t")
    return text if len(text) <= limit else text[: limit - 1] + "…"


def partition_values(readings: list[FileReading]) -> dict[str, list[str]]:
    """Os valores distintos de cada coluna de partição, na ordem em que aparecem."""
    values: dict[str, list[str]] = {}
    for reading in readings:
        for name, value in reading.partition:
            seen = values.setdefault(name, [])
            if value not in seen:
                seen.append(value)
    return values


# ---------------------------------------------------------------------------------------------------------------
# A leitura: o sistema de arquivos, a listagem e o rodapé de cada arquivo


def open_root(uri: str) -> tuple[Any, str, str]:
    """O sistema de arquivos e o caminho da raiz; aceita um caminho local ou uma URI como ``s3://``."""
    import pyarrow.fs as pafs

    if "://" in uri:
        filesystem, path = pafs.FileSystem.from_uri(uri)
        return filesystem, path.rstrip("/"), type(filesystem).__name__
    filesystem = pafs.LocalFileSystem()
    return filesystem, str(Path(uri).expanduser().resolve()), "LocalFileSystem"


def list_root(filesystem: Any, root: str) -> tuple[dict[str, list[tuple[str, int]]], list[str]]:
    """Uma entrada por pasta de tabela, com ``(caminho relativo, bytes)`` de cada Parquet, e o que não é Parquet.

    A listagem é recursiva e só lê metadados de diretório. Um arquivo solto na raiz, fora de uma
    pasta de tabela, entra em ``outros``, porque a base é uma pasta por tabela.
    """
    import pyarrow.fs as pafs

    tables: dict[str, list[tuple[str, int]]] = {}
    others: list[str] = []
    selector = pafs.FileSelector(root, recursive=True, allow_not_found=False)
    for info in filesystem.get_file_info(selector):
        if info.type != pafs.FileType.File:
            # Uma pasta de primeiro nível é uma tabela, mesmo que ainda não tenha arquivo algum.
            relative = info.path[len(root) + 1 :]
            if relative and "/" not in relative:
                tables.setdefault(relative, [])
            continue
        relative = info.path[len(root) + 1 :]
        table, _, inside = relative.partition("/")
        if not inside:
            others.append(relative)
            continue
        if inside.rsplit(".", 1)[-1].lower() in ("parquet", "parq", "pq"):
            tables.setdefault(table, []).append((inside, info.size))
        else:
            others.append(relative)
    return tables, others


def read_footer(filesystem: Any, root: str, table: str, relative: str, size: int) -> FileReading:
    """O rodapé de um arquivo: esquema, layout e estatísticas, sem ler página de dados alguma."""
    import pyarrow.parquet as pq

    with filesystem.open_input_file(f"{root}/{table}/{relative}") as handle:
        parquet = pq.ParquetFile(handle)
        metadata = parquet.metadata
        arrow_schema = parquet.schema_arrow

        # O esquema Parquet traz o tipo físico de cada folha; o Arrow traz o tipo que um leitor devolve.
        leaves = {}
        for index in range(len(parquet.schema)):
            leaf = parquet.schema.column(index)
            logical = logical_label(str(leaf.logical_type)) if leaf.logical_type is not None else "None"
            leaves[leaf.path] = (leaf.physical_type, logical, str(leaf.converted_type))

        columns = []
        for field in arrow_schema:
            physical, logical, converted = leaves.get(field.name, ("(aninhado)", "(aninhado)", "(aninhado)"))
            field_id = field.metadata.get(b"PARQUET:field_id", b"").decode() if field.metadata else ""
            columns.append(
                Column(
                    name=field.name,
                    arrow_type=str(field.type),
                    nullable=field.nullable,
                    physical=physical,
                    logical=logical,
                    converted=converted,
                    field_id=field_id or "-",
                )
            )

        # As estatísticas vivem por row group; a entrada do arquivo é a soma delas, e o extremo dos extremos.
        statistics: dict[str, dict[str, Any]] = {}
        compression: set[str] = set()
        encodings: set[str] = set()
        for group in range(metadata.num_row_groups):
            row_group = metadata.row_group(group)
            for index in range(row_group.num_columns):
                chunk = row_group.column(index)
                name = chunk.path_in_schema
                entry = statistics.setdefault(name, {"rows": 0, "nulls": None, "min": None, "max": None, "distinct": None})
                entry["rows"] += row_group.num_rows
                compression.add(str(chunk.compression))
                encodings.update(str(encoding) for encoding in (chunk.encodings or ()))
                statistic = chunk.statistics
                if statistic is None:
                    continue
                if statistic.null_count is not None:
                    entry["nulls"] = (entry["nulls"] or 0) + statistic.null_count
                if statistic.has_distinct_count:
                    entry["distinct"] = max(entry["distinct"] or 0, statistic.distinct_count)
                if statistic.has_min_max:
                    for edge, value in (("min", statistic.min), ("max", statistic.max)):
                        current = entry[edge]
                        try:
                            if current is None or (value < current if edge == "min" else value > current):
                                entry[edge] = value
                        except TypeError:
                            pass

        footer = {}
        for key, value in (arrow_schema.metadata or {}).items():
            footer[key.decode(errors="replace")] = value.decode(errors="replace")

        keys, kind = partition_of(relative)
        return FileReading(
            path=relative,
            size=size,
            rows=metadata.num_rows,
            row_groups=metadata.num_row_groups,
            created_by=str(metadata.created_by),
            format_version=str(metadata.format_version),
            columns=columns,
            footer=footer,
            partition=keys,
            partition_kind=kind,
            compression=tuple(sorted(compression)),
            encodings=tuple(sorted(encodings)),
            statistics=statistics,
        )


def sample_table(filesystem: Any, root: str, table: str, relative: str, rows: int) -> dict[str, dict[str, Any]]:
    """Mede num lote de até ``rows`` linhas o que o rodapé não guarda: cardinalidade e comprimento de texto."""
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    with filesystem.open_input_file(f"{root}/{table}/{relative}") as handle:
        parquet = pq.ParquetFile(handle)
        batches = parquet.iter_batches(batch_size=rows)
        batch = next(batches, None)
        if batch is None:
            return {}

        measured: dict[str, dict[str, Any]] = {}
        for name, column in zip(batch.schema.names, batch.columns):
            entry: dict[str, Any] = {"rows": len(column), "nulls": column.null_count}
            distinct = pc.unique(column)
            entry["distinct"] = len(distinct)
            if len(distinct) <= MAX_LISTED_VALUES:
                entry["values"] = [format_value(value.as_py(), 24) for value in distinct]
            if pa.types.is_string(column.type) or pa.types.is_large_string(column.type):
                lengths = pc.utf8_length(column)
                edges = pc.min_max(lengths).as_py()
                mean = pc.mean(lengths).as_py()
                entry["length"] = (edges["min"], round(mean) if mean is not None else None, edges["max"])
                # O contrato mede o String(n) em bytes, como o VARCHAR(n) do Redshift.
                entry["bytes"] = pc.max(pc.binary_length(column)).as_py()
            measured[name] = entry
        return measured


def text_columns(collected: list[FileReading]) -> list[str]:
    """Os nomes das colunas de texto da tabela, na ordem do primeiro arquivo que as traz."""
    names: list[str] = []
    for reading in collected:
        for column in reading.columns:
            if column.arrow_type.endswith("string") and column.name not in names:
                names.append(column.name)
    return names


def text_lengths(filesystem: Any, root: str, table: str, collected: list[FileReading], names: list[str]) -> dict[str, dict[str, int]]:
    """Mede em todas as linhas de todos os arquivos o maior texto de cada coluna, em bytes e em caracteres."""
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    measured = {name: {"rows": 0, "nulls": 0, "bytes": 0, "chars": 0} for name in names}
    for reading in collected:
        with filesystem.open_input_file(f"{root}/{table}/{reading.path}") as handle:
            parquet = pq.ParquetFile(handle)
            # Um arquivo pode não ter todas as colunas: o esquema divergente é outra seção.
            present = [name for name in names if name in parquet.schema_arrow.names]
            if not present:
                continue
            for batch in parquet.iter_batches(batch_size=TEXT_BATCH_ROWS, columns=present):
                for name, column in zip(batch.schema.names, batch.columns):
                    entry = measured[name]
                    entry["rows"] += len(column)
                    entry["nulls"] += column.null_count
                    longest = pc.max(pc.binary_length(column)).as_py()
                    if longest is not None:
                        entry["bytes"] = max(entry["bytes"], longest)
                        entry["chars"] = max(entry["chars"], pc.max(pc.utf8_length(column)).as_py())
    return measured


# ---------------------------------------------------------------------------------------------------------------
# As seções do relatório


def root_section(report: Report, filesystem_name: str, root: str, tables: dict[str, list[tuple[str, int]]], others: list[str]) -> None:
    """Seção 1, a raiz: ``PQ-1``, as pastas de tabela; ``PQ-2``, as pastas sem Parquet; e o que não é Parquet."""
    report.h1("A raiz")
    report.value("FILESYSTEM", filesystem_name)
    report.value("ROOT", root)

    files = sum(len(found) for found in tables.values())
    total = sum(size for found in tables.values() for _, size in found)
    report.table(
        [
            ["o que", "quanto"],
            ["pastas de tabela", len(tables)],
            ["arquivos Parquet", files],
            ["bytes", f"{total:,}".replace(",", ".")],
            ["arquivos que não são Parquet", len(others)],
        ]
    )
    report.ok("PQ-1", "raiz lida", f"{len(tables)} pasta(s) de tabela, {files} arquivo(s) Parquet, {total} bytes")

    empty = sorted(name for name, found in tables.items() if not found)
    if empty:
        report.fail("PQ-2", "pasta sem Parquet", f"{len(empty)}: {', '.join(empty[:10])}")
    else:
        report.ok("PQ-2", "pasta sem Parquet", "nenhuma: toda pasta de tabela tem arquivo Parquet")

    if others:
        report.h2("Arquivos que não são Parquet")
        report.line("Um arquivo solto na raiz, ou de outra extensão dentro de uma tabela; nenhum foi lido.")
        report.table([["caminho"], *[[path] for path in sorted(others)[:40]]])
        if len(others) > 40:
            report.line(f"... ({len(others) - 40} omitidos)\n")


def tables_section(report: Report, readings: dict[str, list[FileReading]], file_rows: int) -> dict[str, list[list[FileReading]]]:
    """Seção 2, as tabelas: uma linha por tabela, a listagem por arquivo e ``PQ-3``, o esquema uniforme.

    Devolve, por tabela, os grupos de arquivos que compartilham esquema, do maior para o menor; o
    primeiro grupo é o majoritário, contra o qual a seção 4 compara os demais.
    """
    report.h1("As tabelas")

    groups: dict[str, list[list[FileReading]]] = {}
    rows: list[list[Any]] = [["tabela", "arquivos", "bytes", "linhas", "colunas", "partição", "partições", "esquemas"]]
    for table, found in sorted(readings.items()):
        by_schema: dict[tuple, list[FileReading]] = {}
        for reading in found:
            by_schema.setdefault(schema_key(reading.columns), []).append(reading)
        ordered = sorted(by_schema.values(), key=len, reverse=True)
        groups[table] = ordered

        keys = sorted({name for reading in found for name, _ in reading.partition})
        kinds = sorted({reading.partition_kind for reading in found})
        rows.append(
            [
                table,
                len(found),
                f"{sum(reading.size for reading in found):,}".replace(",", "."),
                f"{sum(reading.rows for reading in found):,}".replace(",", "."),
                len(ordered[0][0].columns) if ordered else 0,
                ", ".join(keys) or "/".join(kinds),
                len({reading.partition for reading in found}),
                len(ordered),
            ]
        )
    report.table(rows)

    divergent = sorted(table for table, ordered in groups.items() if len(ordered) > 1)
    if divergent:
        report.fail("PQ-3", "esquema uniforme por tabela", f"{len(divergent)} tabela(s) com esquemas diferentes: {', '.join(divergent)}")
    else:
        report.ok("PQ-3", "esquema uniforme por tabela", "todo arquivo de cada tabela tem o mesmo esquema")

    report.h2("Os arquivos de cada tabela")
    report.line("`grupo` numera os esquemas distintos da tabela, 1 é o majoritário; arquivos de grupos diferentes estão na seção de divergências.\n")
    for table, ordered in sorted(groups.items()):
        number = {id(reading): index for index, group in enumerate(ordered, 1) for reading in group}
        found = sorted(readings[table], key=lambda reading: reading.path)
        listed = found[:file_rows]
        report.line(f"{table} ({len(found)} arquivo(s))")
        report.table(
            [
                ["arquivo", "grupo", "linhas", "row groups", "bytes"],
                *[
                    [reading.path, number[id(reading)], reading.rows, reading.row_groups, f"{reading.size:,}".replace(",", ".")]
                    for reading in listed
                ],
            ]
        )
        if len(found) > len(listed):
            report.line(f"... ({len(found) - len(listed)} arquivo(s) omitidos da listagem; todos foram lidos e contam nas seções seguintes)\n")
    return groups


def schema_section(report: Report, groups: dict[str, list[list[FileReading]]]) -> None:
    """Seção 3, o esquema de cada tabela: ``PQ-7``, os tipos encontrados na base, e ``PQ-9``, as tabelas vazias."""
    report.h1("O esquema de cada tabela")
    report.line("O esquema do grupo majoritário: o tipo Arrow é o que um leitor devolve, o físico e o lógico são o que está gravado.\n")

    types: Counter[str] = Counter()
    empty: list[str] = []
    for table, ordered in sorted(groups.items()):
        if not ordered:
            continue
        reference = ordered[0][0]
        if sum(reading.rows for group in ordered for reading in group) == 0:
            empty.append(table)
        report.line(f"{table}")
        report.table(
            [
                ["coluna", "tipo Arrow", "nulidade", "físico", "lógico", "convertido", "field_id"],
                *[
                    [
                        column.name,
                        column.arrow_type,
                        "nulo" if column.nullable else "NÃO NULO",
                        column.physical,
                        column.logical,
                        column.converted,
                        column.field_id,
                    ]
                    for column in reference.columns
                ],
            ]
        )
        for column in reference.columns:
            types[f"{column.arrow_type} ({column.physical}/{column.logical})"] += 1

    report.h2("Os tipos encontrados na base")
    report.table([["tipo Arrow (físico/lógico)", "colunas"], *[[name, count] for name, count in types.most_common()]])
    report.note("PQ-7", "tipos da base", f"{len(types)} tipo(s) distinto(s) em {sum(types.values())} coluna(s)")

    if empty:
        report.note("PQ-9", "tabela com arquivo e sem linha", f"{len(empty)}: {', '.join(empty)}")
    else:
        report.ok("PQ-9", "tabela com arquivo e sem linha", "nenhuma: toda tabela com arquivo tem linha")


def divergence_section(report: Report, groups: dict[str, list[list[FileReading]]]) -> None:
    """Seção 4, as divergências: para cada tabela com mais de um esquema, o que muda e em quais arquivos."""
    report.h1("Divergências de esquema entre os arquivos")
    divergent = {table: ordered for table, ordered in sorted(groups.items()) if len(ordered) > 1}
    if not divergent:
        report.line("Nenhuma. Em cada tabela, todo arquivo de toda partição tem o mesmo esquema.")
        return

    report.line("Cada bloco compara um grupo com o majoritário da sua tabela e lista todos os arquivos do grupo.\n")
    for table, ordered in divergent.items():
        reference = ordered[0][0].columns
        report.h2(f"{table}: {len(ordered)} esquemas")
        report.line(f"grupo 1 (majoritário): {len(ordered[0])} arquivo(s), {len(reference)} coluna(s)\n")
        for index, group in enumerate(ordered[1:], 2):
            report.line(f"grupo {index}: {len(group)} arquivo(s)")
            report.table(
                [
                    ["coluna", "diferença", "grupo 1", f"grupo {index}"],
                    *schema_difference(reference, group[0].columns),
                ]
            )
            report.table([["arquivo do grupo"], *[[reading.path] for reading in sorted(group, key=lambda item: item.path)]])


def partition_section(report: Report, readings: dict[str, list[FileReading]], groups: dict[str, list[list[FileReading]]]) -> None:
    """Seção 5, as partições: ``PQ-4``, o layout uniforme por tabela, e ``PQ-5``, a coluna de partição dentro do arquivo."""
    report.h1("As partições")
    report.line("A coluna de partição vive no caminho; se ela também está dentro do arquivo, um leitor que junta os dois a vê duas vezes.\n")

    mixed: list[str] = []
    inside: list[str] = []
    for table, found in sorted(readings.items()):
        if not found:
            report.line(f"{table}: sem arquivo Parquet\n")
            continue
        kinds = {reading.partition_kind for reading in found}
        depths = {len(reading.partition) for reading in found}
        if len(kinds) > 1 or len(depths) > 1:
            mixed.append(table)

        values = partition_values(found)
        columns = {column.name for group in groups.get(table, []) for column in group[0].columns}
        style = "/".join(sorted(kinds))
        if not values:
            report.line(f"{table}: {style}, o arquivo está na raiz da tabela\n")
            continue
        report.line(f"{table}: {style}, {len({reading.partition for reading in found})} partição(ões)")
        rows: list[list[Any]] = [["coluna de partição", "valores", "dentro do arquivo", "amostra dos valores"]]
        for name, found_values in values.items():
            in_file = name in columns
            if in_file:
                inside.append(f"{table}.{name}")
            sample = ", ".join(sorted(found_values)[:MAX_LISTED_VALUES])
            if len(found_values) > MAX_LISTED_VALUES:
                sample += f", ... (+{len(found_values) - MAX_LISTED_VALUES})"
            rows.append([name, len(found_values), "sim" if in_file else "não", sample])
        report.table(rows)

    if mixed:
        report.fail("PQ-4", "layout de partição uniforme", f"{len(mixed)} tabela(s) misturam profundidade ou estilo: {', '.join(mixed)}")
    else:
        report.ok("PQ-4", "layout de partição uniforme", "cada tabela usa um só estilo e uma só profundidade de partição")

    if inside:
        report.note("PQ-5", "coluna de partição dentro do arquivo", f"{len(inside)}: {', '.join(inside[:10])}")
    else:
        report.note("PQ-5", "coluna de partição dentro do arquivo", "nenhuma: a coluna só existe no caminho, como o delta-rs grava")


def statistics_section(report: Report, readings: dict[str, list[FileReading]]) -> None:
    """Seção 6, as estatísticas por coluna: ``PQ-6``, se o rodapé traz mínimo e máximo de toda coluna."""
    report.h1("Estatísticas por coluna")
    report.line("Somadas do rodapé de todos os arquivos da tabela. `distintos` é o maior valor visto num arquivo, um piso da cardinalidade; `sem min/max` conta os arquivos sem a estatística naquela coluna.\n")

    without = 0
    total = 0
    for table, found in sorted(readings.items()):
        if not found:
            continue
        merged = merge_statistics(found)
        report.line(f"{table}")
        rows: list[list[Any]] = [["coluna", "linhas", "nulos", "% nulos", "mínimo", "máximo", "distintos", "sem min/max"]]
        for name, entry in merged.items():
            total += 1
            without += 1 if entry["without"] else 0
            nulls = entry["nulls"] if entry["nulls_known"] else None
            fraction = f"{100 * nulls / entry['rows']:.1f}" if nulls is not None and entry["rows"] else "-"
            rows.append(
                [
                    name,
                    f"{entry['rows']:,}".replace(",", "."),
                    "-" if nulls is None else f"{nulls:,}".replace(",", "."),
                    fraction,
                    format_value(entry["min"]),
                    format_value(entry["max"]),
                    entry["distinct"] if entry["distinct"] is not None else "-",
                    entry["without"] or "-",
                ]
            )
        report.table(rows)

    if without:
        report.note("PQ-6", "mínimo e máximo no rodapé", f"{without} de {total} coluna(s) da base têm arquivo sem a estatística")
    else:
        report.ok("PQ-6", "mínimo e máximo no rodapé", f"as {total} coluna(s) da base trazem mínimo e máximo em todo arquivo")


def layout_section(report: Report, readings: dict[str, list[FileReading]]) -> None:
    """Seção 7, o layout físico: row groups, compressão, codificação, escritor e metadados do rodapé."""
    report.h1("Layout físico")

    rows: list[list[Any]] = [["tabela", "row groups", "linhas por row group", "compressão", "codificação", "escritor", "versão"]]
    for table, found in sorted(readings.items()):
        groups_total = sum(reading.row_groups for reading in found)
        per_group = [reading.rows / reading.row_groups for reading in found if reading.row_groups]
        rows.append(
            [
                table,
                groups_total,
                f"{min(per_group):,.0f} a {max(per_group):,.0f}".replace(",", ".") if per_group else "-",
                ", ".join(sorted({value for reading in found for value in reading.compression})),
                ", ".join(sorted({value for reading in found for value in reading.encodings})),
                ", ".join(sorted({reading.created_by.split(" version ")[0] for reading in found})),
                ", ".join(sorted({reading.format_version for reading in found})),
            ]
        )
    report.table(rows)

    report.h2("Metadados do rodapé")
    report.line("As chaves que o escritor gravou no rodapé; `pandas` indica origem pandas, `serialize_db_*` seria da biblioteca.\n")
    footer_rows: list[list[Any]] = [["tabela", "chave", "arquivos", "valor de um arquivo"]]
    for table, found in sorted(readings.items()):
        keys: Counter[str] = Counter()
        example: dict[str, str] = {}
        for reading in found:
            for key, value in reading.footer.items():
                keys[key] += 1
                example.setdefault(key, value)
        for key, count in keys.most_common():
            footer_rows.append([table, key, f"{count}/{len(found)}", format_value(example[key], 60)])
    report.table(footer_rows if len(footer_rows) > 1 else [["tabela", "chave"], ["(nenhuma)", "nenhum arquivo traz metadado no rodapé"]])


def sample_section(report: Report, measured: dict[str, dict[str, dict[str, Any]]], rows: int) -> None:
    """Seção 8, a amostra: cardinalidade, nulos e comprimento de texto nas primeiras linhas de um arquivo por tabela."""
    report.h1("Amostra de valores")
    if not measured:
        report.line("Não pedida. `--sample N` lê as N primeiras linhas de um arquivo por tabela para medir o que o rodapé não guarda.")
        return

    report.line(f"As {rows} primeiras linhas de um arquivo de cada tabela; `distintos` e os comprimentos são dessa amostra, não da tabela; `comprimento` conta caracteres e `máx bytes` conta bytes, a medida do `VARCHAR(n)` do Redshift.\n")
    for table, columns in sorted(measured.items()):
        report.line(f"{table}")
        report.table(
            [
                ["coluna", "linhas", "nulos", "distintos", "comprimento mín/méd/máx", "máx bytes", "valores quando poucos"],
                *[
                    [
                        name,
                        entry["rows"],
                        entry["nulls"],
                        entry["distinct"],
                        "/".join(str(part) for part in entry["length"]) if "length" in entry else "-",
                        entry.get("bytes", "-"),
                        ", ".join(entry.get("values", [])) or "-",
                    ]
                    for name, entry in columns.items()
                ],
            ]
        )


def text_length_section(report: Report, lengths: dict[str, dict[str, dict[str, int]]]) -> None:
    """Seção 9, o comprimento de texto: o maior valor de cada coluna de texto em todas as linhas."""
    report.h1("Comprimento de texto")
    if not lengths:
        report.line("Não pedido. `--text-bytes` lê as colunas de texto de todo arquivo e mede o maior valor de cada uma.")
        return

    report.line("Todas as linhas de todos os arquivos, só as colunas de texto; `máx bytes` é a medida do `VARCHAR(n)` do Redshift e do `String(n)` do contrato.\n")
    for table, columns in sorted(lengths.items()):
        report.line(f"{table}")
        report.table(
            [
                ["coluna", "linhas", "nulos", "máx bytes", "máx caracteres"],
                *[
                    [
                        name,
                        f"{entry['rows']:,}".replace(",", "."),
                        f"{entry['nulls']:,}".replace(",", "."),
                        entry["bytes"],
                        entry["chars"],
                    ]
                    for name, entry in columns.items()
                ],
            ]
        )


# ---------------------------------------------------------------------------------------------------------------


def parse(argv: list[str]) -> tuple[str, int, int, bool] | None:
    """A raiz, a amostra, o limite da listagem e se mede o texto; ``None`` quando o uso está errado."""
    root = ""
    sample = 0
    files = DEFAULT_FILE_ROWS
    text_bytes = False
    rest = argv[1:]
    while rest:
        argument = rest.pop(0)
        if argument == "--text-bytes":
            text_bytes = True
        elif argument in ("--sample", "--files"):
            if not rest or not rest[0].isdigit():
                return None
            value = int(rest.pop(0))
            sample, files = (value, files) if argument == "--sample" else (sample, value)
        elif argument.startswith("-"):
            return None
        elif root:
            return None
        else:
            root = argument
    return (root, sample, files, text_bytes) if root else None


def main(argv: list[str]) -> int:
    parsed = parse(argv)
    if parsed is None:
        print("uso: .venv/bin/python probes/parquet_source.py <raiz> [--sample N] [--files N] [--text-bytes]", file=sys.stderr)
        return 2
    uri, sample, file_rows, text_bytes = parsed

    filesystem, root, filesystem_name = open_root(uri)
    report = Report("parquet_source", f"a estrutura da base Parquet em {root}")
    report.line("Só leitura: cada arquivo é aberto por open_input_file, e nada sob a raiz é criado, alterado ou apagado.")

    listing = report.call(f"listar {root} recursivamente", lambda: list_root(filesystem, root), render=lambda result: f"{len(result[0])} pasta(s) de tabela, {sum(len(found) for found in result[0].values())} arquivo(s) Parquet")
    if listing is None:
        report.fail("PQ-1", "raiz lida", f"não listada: {report.last_reason}")
        return report.finish()
    tables, others = listing

    # O rodapé de todo arquivo de toda partição: é a leitura que responde se os esquemas batem.
    readings: dict[str, list[FileReading]] = {}
    unreadable: list[str] = []
    for table, found in sorted(tables.items()):
        collected: list[FileReading] = []
        for relative, size in sorted(found):
            try:
                collected.append(read_footer(filesystem, root, table, relative, size))
            except Exception as error:  # noqa: BLE001 - um arquivo ilegível é diagnóstico, não interrompe a leitura
                unreadable.append(f"{table}/{relative}: {describe_error(error)}")
                report.failures.append((f"rodapé de {table}/{relative}", describe_error(error)))
        readings[table] = collected
    report.line(f"rodapés lidos: {sum(len(found) for found in readings.values())} arquivo(s), {len(unreadable)} ilegível(is)\n")

    measured: dict[str, dict[str, dict[str, Any]]] = {}
    if sample:
        for table, collected in sorted(readings.items()):
            if not collected:
                continue
            try:
                measured[table] = sample_table(filesystem, root, table, collected[0].path, sample)
            except Exception as error:  # noqa: BLE001
                report.failures.append((f"amostra de {table}", describe_error(error)))

    lengths: dict[str, dict[str, dict[str, int]]] = {}
    if text_bytes:
        for table, collected in sorted(readings.items()):
            names = text_columns(collected)
            if not names:
                continue
            # A varredura é longa: o andamento sai no terminal, fora do relatório.
            print(f"medindo o texto de {table}: {len(collected)} arquivo(s), {len(names)} coluna(s)", file=sys.stderr)
            try:
                lengths[table] = text_lengths(filesystem, root, table, collected, names)
            except Exception as error:  # noqa: BLE001
                report.failures.append((f"comprimento de texto de {table}", describe_error(error)))

    def guarded(section, *arguments):
        # Uma seção interrompida não cala as outras.
        try:
            return section(report, *arguments)
        except Exception as error:  # noqa: BLE001 - toda falha é diagnóstico
            report.line(f"!! seção {section.__name__} interrompida: {describe_error(error)}")
            report.failures.append((f"seção {section.__name__}", describe_error(error)))
            return None

    guarded(root_section, filesystem_name, root, tables, others)
    groups = guarded(tables_section, readings, file_rows) or {}
    guarded(schema_section, groups)
    guarded(divergence_section, groups)
    guarded(partition_section, readings, groups)
    guarded(statistics_section, readings)
    guarded(layout_section, readings)
    guarded(sample_section, measured, sample)
    guarded(text_length_section, lengths)

    if unreadable:
        report.fail("PQ-8", "arquivo ilegível", f"{len(unreadable)}: {unreadable[0]}")
    else:
        report.ok("PQ-8", "arquivo ilegível", "nenhum: o rodapé de todo arquivo foi lido")
    return report.finish()


if __name__ == "__main__":
    sys.exit(main(sys.argv))
