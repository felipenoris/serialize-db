"""A camada de tabela: as tabelas Delta do banco, gravadas e lidas pelo delta-rs.

As primitivas que abrem uma tabela recebem ``uri``, a pasta da tabela sob a raiz do banco, e o
``Storage`` da raiz, que dá as opções do delta-rs a cada chamada e o sistema de arquivos que lê os
rodapés e o log. A coluna de partição sai de ``table_options(table)``, e ``value`` é o valor de uma
partição, ``None`` numa tabela sem partição; o valor segue ``schema.PARTITION_VALUE``, porque entra
no predicado como literal e vira nome de pasta.

O ciclo de uma tabela: ``create_table`` a cria do modelo, ``reconcile`` aplica o diff aditivo do
modelo e recusa o destrutivo, que só ``rewrite`` resolve, num commit. Uma partição entra por um de
dois caminhos: ``register_files`` registra no log os arquivos que outro escritor gravou na pasta da
tabela, depois das conferências do rodapé de cada arquivo e com a releitura pelos dois leitores
depois do commit, e é o caminho dos motores; ``publish_partition`` grava os dados pelo escritor do
delta-rs, que confere tudo e paga a memória, e é o do motor Redshift para a partição com ``Double``
não finito, cujo rodapé do ``UNLOAD`` deixa o ``NaN`` fora do máximo. ``version_diff`` lê no log as
partições alteradas entre duas versões, ``copy_manifest`` monta o manifesto do ``COPY`` do Redshift,
e ``snapshot`` marca no arquivo de controle do ambiente as versões de um snapshot do banco, que
``vacuum_keeping_snapshots`` preserva; ``set_channel`` aponta um canal do ambiente para um
snapshot, e ``channel_snapshot`` e ``snapshot_versions`` leem o canal e as versões para o leitor
e a publicação. ``compact``, ``deep_copy`` e ``export_snapshot`` são a operação.

Exemplo, numa pasta local:

.. code-block:: python

    import pyarrow as pa

    from serialize_db import delta, schema
    from serialize_db.storage import Storage

    storage = Storage.for_uri("/dados/delta")
    uri = storage.uri_of("prd/cad_operacoes")
    delta.create_table(uri, Operacao.__table__, storage)
    data = schema.cast(pa.table({...}), Operacao.__table__)
    metadata = delta.commit_metadata("exec-2026-09-05", {"cad_contratos": 88})
    version = delta.publish_partition(uri, Operacao.__table__, "2026-08-31", data, metadata,
                                      storage)
    delta.version_diff(uri, version - 1, version, Operacao.__table__, storage)   # {"2026-08-31"}
"""

from __future__ import annotations

import dataclasses
import datetime
import json
import logging
import math
import time
import uuid
from collections.abc import Callable, Collection, Mapping
from typing import TYPE_CHECKING, Literal

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import sqlalchemy as sa
from deltalake import (
    ColumnProperties,
    CommitProperties,
    DeltaTable,
    WriterProperties,
    write_deltalake,
)
from deltalake.exceptions import CommitFailedError
from deltalake.transaction import AddAction

from serialize_db.errors import (
    ContractError,
    ExecutionConflict,
    LogUnavailable,
    RegistrationRefused,
    SchemaDiffRefused,
)
from serialize_db.schema import (
    arrow_schema,
    check_partition_value,
    delta_schema,
    double_columns,
    literal,
    quoted,
    sql_type,
    table_options,
)
from serialize_db.storage import Storage

if TYPE_CHECKING:
    import duckdb

log = logging.getLogger(__name__)

__all__ = [
    "RegisteredFile",
    "SchemaDiff",
    "archive_snapshot",
    "channel_snapshot",
    "commit_metadata",
    "compact",
    "copy_manifest",
    "create_table",
    "deep_copy",
    "export_snapshot",
    "history",
    "max_key",
    "open_table",
    "publish_partition",
    "read_back",
    "read_snapshots",
    "reconcile",
    "register_files",
    "rewrite",
    "schema_diff",
    "set_channel",
    "snapshot",
    "snapshot_versions",
    "table_exists",
    "vacuum_keeping_snapshots",
    "version_diff",
]

# As retenções de toda tabela: o log legível por dez anos, porque version_diff e os snapshots do
# banco o leem, e os arquivos removidos guardados por 400 dias, a janela em que toda versão
# continua legível.
RETENTION = {
    "delta.logRetentionDuration": "interval 3650 days",
    "delta.deletedFileRetentionDuration": "interval 400 days",
}

# O arquivo de controle dos snapshots do banco, na raiz de cada ambiente.
CONTROL_FILE = "_serialize_db/snapshots.json"

# O canal que o leitor Delta lê sem argumento, e o canal reservado que resolve para a versão atual
# de cada tabela e não fica no arquivo de controle.
DEFAULT_CHANNEL = "default"
CURRENT_CHANNEL = "current"


# ---------------------------------------------------------------- os tipos


@dataclasses.dataclass(frozen=True)
class RegisteredFile:
    """Um arquivo que outro escritor gravou dentro da pasta da tabela, como o ``COPY ...
    RETURN_STATS`` do DuckDB ou o manifesto do ``UNLOAD`` o descrevem.

    Exemplo:

    .. code-block:: python

        RegisteredFile(path="data_str=2026-08-31/exec-42_ab12.parquet", size=4096, rows=1000,
                       stats={"min": {"id_operacao": 1}, "max": {"id_operacao": 1000},
                              "null_count": {"id_operacao": 0}})
    """

    path: str
    """Relativo à pasta da tabela, como o log o guarda."""
    size: int
    """O tamanho em bytes."""
    rows: int
    """As linhas que o escritor declarou, conferidas contra o rodapé."""
    stats: Mapping[str, Mapping[str, object]]
    """``{"min": {coluna: valor}, "max": {...}, "null_count": {...}}``, com os valores tipados; uma
    coluna sem estatística fica de fora."""


@dataclasses.dataclass(frozen=True)
class SchemaDiff:
    """O diff entre o modelo e o esquema de uma tabela Delta, separado em aditivo e destrutivo.

    Exemplo:

    .. code-block:: python

        diff = schema_diff(Operacao.__table__, open_table(uri, storage))
        diff.destructive   # () quando reconcile pode aplicar o resto
    """

    add: tuple[pa.Field, ...]
    """As colunas novas: anuláveis, ou ``NOT NULL`` numa tabela ainda sem dados."""
    relax: tuple[str, ...]
    """As colunas cujo ``NOT NULL`` o modelo relaxou."""
    checks: tuple[str, ...]
    """Os nomes das ``CheckConstraint`` do modelo que a tabela não tem."""
    description: str | None
    """O comentário da tabela quando difere da ``description`` do Delta; ``None`` quando igual."""
    comments: tuple[str, ...]
    """As colunas cujo comentário difere do do esquema Delta."""
    destructive: tuple[str, ...]
    """Uma frase por diferença que só ``rewrite`` resolve: coluna ``NOT NULL`` nova numa tabela
    com dados, ``NOT NULL`` numa coluna anulável, remoção (a renomeação aparece como remoção e
    coluna nova) e mudança de tipo."""

    @property
    def changes(self) -> bool:
        """Se o diff aditivo tem alguma coisa a aplicar."""
        additive = (self.add, self.relax, self.checks, self.comments)
        return any(additive) or self.description is not None


# ---------------------------------------------------------------- abertura e metadados


def _options(storage: Storage) -> dict[str, str] | None:
    """As opções do delta-rs do armazenamento, ou ``None`` na pasta local."""
    return storage.storage_options() or None


def checked_value(table: sa.Table, value: str | None) -> str | None:
    """O valor de partição conferido: obrigatório e na regra da partição numa tabela particionada,
    ``None`` numa tabela sem partição; protegida, para o ``export_partition`` dos motores, que o
    confere antes de gravar o arquivo."""
    partition_by = table_options(table).partition_by
    if partition_by is None:
        if value is not None:
            raise ContractError(f"{table.name}: tabela sem partição recebeu o valor {value!r}")
        return None
    if value is None:
        raise ContractError(f"{table.name}: particionada por {partition_by}, sem valor de partição")
    return check_partition_value(value)


def create_table(uri: str, table: sa.Table, storage: Storage) -> DeltaTable:
    """A tabela Delta do modelo, criada na versão 0 se ainda não existe.

    ``DeltaTable.create(mode="ignore")`` com ``delta_schema(table)``, a coluna de partição, o nome
    da tabela, o comentário da tabela em ``description`` e as retenções: o log por 3.650 dias e os
    arquivos removidos por 400. Sem vetores de exclusão nem column mapping, que o ``COPY`` do
    Redshift não lê.

    Exemplo:

    .. code-block:: python

        create_table(uri, Operacao.__table__, storage).version()   # 0

    :param uri: a URI da pasta da tabela, sob a raiz do banco.
    :param table: a tabela do modelo.
    :param storage: o armazenamento da raiz do banco.
    :return: a tabela; a chamada repetida a devolve como está.
    """
    partition_by = table_options(table).partition_by
    return DeltaTable.create(
        uri,
        delta_schema(table),
        mode="ignore",
        partition_by=[partition_by] if partition_by else None,
        name=table.name,
        description=table.comment,
        configuration=RETENTION,
        storage_options=_options(storage),
    )


def open_table(uri: str, storage: Storage, version: int | None = None) -> DeltaTable:
    """A tabela numa versão; a execução abre cada tabela uma vez e guarda o objeto e a versão.

    Exemplo:

    .. code-block:: python

        open_table(uri, storage, version=3).version()   # 3

    :param uri: a URI da pasta da tabela, sob a raiz do banco.
    :param storage: o armazenamento da raiz do banco.
    :param version: a versão carregada; ``None`` é a última.
    :return: a tabela carregada na versão.
    """
    return DeltaTable(uri, version=version, storage_options=_options(storage))


def table_exists(uri: str, storage: Storage) -> bool:
    """Se há uma tabela Delta na pasta.

    Exemplo:

    .. code-block:: python

        table_exists(storage.uri_of("prd/cad_operacoes"), storage)   # True depois de create_table

    :param uri: a URI da pasta, sob a raiz do banco.
    :param storage: o armazenamento da raiz do banco.
    :return: ``True`` quando há; a pasta sem ``_delta_log/`` não é tabela.
    """
    return DeltaTable.is_deltatable(uri, storage_options=_options(storage))


def _logged_statistic(actions: pa.Table, name: str,
                      aggregate: Callable[[pa.ChunkedArray], pa.Scalar]) -> object | None:
    """O agregado de uma estatística das ações ``add``, como o ``pc.max`` de ``max.<coluna>``;
    ``None`` quando não há ação ou quando algum arquivo não tem a estatística."""
    if actions.num_rows == 0 or name not in actions.column_names:
        return None
    column = actions.column(name)
    if column.null_count > 0:
        return None
    return aggregate(column).as_py()


def max_key(dt: DeltaTable, column: str) -> int:
    """O maior valor de ``column`` na versão carregada, o início de ``next_ids``.

    O máximo de ``max.<coluna>`` das ações ``add``, sem ler dados; a varredura da coluna quando um
    arquivo não tem a estatística, porque a estatística registrada é verdadeira ou omitida, nunca
    falsa.

    Exemplo:

    .. code-block:: python

        max_key(open_table(uri, storage), "id_operacao")   # 1000

    :param dt: a tabela aberta por ``open_table``.
    :param column: o nome da coluna inteira, como a chave sequencial.
    :return: o maior valor; 0 na tabela vazia.
    """
    # get_add_actions devolve uma tabela arro3; pa.table a converte sem cópia.
    actions = pa.table(dt.get_add_actions(flatten=True))
    if actions.num_rows == 0:
        return 0
    logged = _logged_statistic(actions, f"max.{column}", pc.max)
    if logged is not None:
        return logged
    scanned = dt.to_pyarrow_dataset().to_table(columns=[column]).column(column)
    return pc.max(scanned).as_py()


def commit_metadata(execution_id: str, input_versions: Mapping[str, int],
                    snapshot: str | None = None) -> dict[str, str]:
    """Os metadados que a execução grava em cada commit, todos texto.

    Exemplo:

    .. code-block:: python

        commit_metadata("exec-2026-09-05", {"cad_contratos": 88})
        # {"serialize_db_execution_id": "exec-2026-09-05",
        #  "serialize_db_input_versions": '{"cad_contratos": 88}'}

    :param execution_id: o identificador da execução.
    :param input_versions: a versão lida de cada tabela de entrada, pelo nome da tabela.
    :param snapshot: o nome do snapshot da execução marcada; ``None`` nas outras.
    :return: ``serialize_db_execution_id``, ``serialize_db_input_versions`` (o JSON das versões
        lidas, com as chaves ordenadas) e, só na execução marcada, ``serialize_db_snapshot``;
        eles aparecem no ``history`` da tabela.
    """
    metadata = {
        "serialize_db_execution_id": execution_id,
        "serialize_db_input_versions": json.dumps(dict(input_versions), sort_keys=True),
    }
    if snapshot:
        metadata["serialize_db_snapshot"] = snapshot
    return metadata


# ---------------------------------------------------------------- a publicação pelo delta-rs


def _as_arrow(data: object) -> pa.Table | pa.RecordBatchReader:
    """Os dados como ``pa.Table`` ou leitor: um lote vira tabela, e um objeto com
    ``__arrow_c_stream__``, como o ``BatchStream`` de um motor, vira leitor."""
    if isinstance(data, (pa.Table, pa.RecordBatchReader)):
        return data
    if isinstance(data, pa.RecordBatch):
        return pa.Table.from_batches([data])
    return pa.RecordBatchReader.from_stream(data)


def _writer_properties(columns_without_min_max: Collection[str]) -> WriterProperties | None:
    """As propriedades do escritor que tiram o mínimo e o máximo das colunas, no rodapé e no log,
    que o delta-rs copia do rodapé; ``None`` mantém o padrão."""
    if not columns_without_min_max:
        return None
    no_statistics = ColumnProperties(statistics_enabled="NONE")
    properties = {}
    for name in columns_without_min_max:
        properties[name] = no_statistics
    return WriterProperties(column_properties=properties)


def _partition_predicate(partition_by: str | None, value: str | None) -> str | None:
    """O predicado da substituição da partição, ``"<coluna>" = '<valor>'``; ``None`` sem
    partição."""
    if partition_by is None:
        return None
    return f"{quoted(partition_by)} = {literal(value)}"


def publish_partition(uri: str, table: sa.Table, value: str | None, data: object,
                      metadata: Mapping[str, str], storage: Storage,
                      columns_without_min_max: Collection[str] = ()) -> int:
    """Substitui a partição pelos dados num commit, pelo escritor do delta-rs.

    ``write_deltalake(mode="overwrite", predicate=...)`` sobre o objeto ``DeltaTable``, que depois
    da escrita está na versão do próprio commit mesmo com o commit de outro escritor no meio.

    Exemplo:

    .. code-block:: python

        version = publish_partition(uri, Operacao.__table__, "2026-08-31", data,
                                    commit_metadata("exec-42", {}), storage,
                                    columns_without_min_max=["valor"])

    :param uri: a URI da pasta da tabela, sob a raiz do banco.
    :param table: a tabela do modelo.
    :param value: o valor da partição, na regra de ``schema.PARTITION_VALUE``; ``value=None`` numa
        tabela sem partição substitui a tabela.
    :param data: uma ``pa.Table``, um lote, um ``RecordBatchReader`` ou um objeto com
        ``__arrow_c_stream__``, já passado por ``cast`` e com a coluna de partição.
    :param metadata: os metadados do commit, de ``commit_metadata``.
    :param storage: o armazenamento da raiz do banco.
    :param columns_without_min_max: as colunas que saem sem mínimo e máximo no rodapé e no log, as
        ``Double`` com valor não finito na partição (issue #59).
    :return: a versão do próprio commit.
    :raises ContractError: ``value`` ausente numa tabela particionada, presente numa tabela sem
        partição ou fora da regra; os dados sem a coluna de partição, antes de gravar.
    :raises ExecutionConflict: o commit falhou no delta-rs com ``CommitFailedError``.
    """
    value = checked_value(table, value)
    arrow = _as_arrow(data)
    partition_by = table_options(table).partition_by
    if partition_by and partition_by not in arrow.schema.names:
        raise ContractError(f"{table.name}: os dados não têm a coluna de partição {partition_by}")
    dt = open_table(uri, storage)
    try:
        write_deltalake(
            dt,
            arrow,
            mode="overwrite",
            predicate=_partition_predicate(partition_by, value),
            writer_properties=_writer_properties(columns_without_min_max),
            commit_properties=CommitProperties(custom_metadata=dict(metadata)),
        )
    except CommitFailedError as error:
        raise ExecutionConflict(f"{table.name} partição {value}: {error}") from None
    return dt.version()


# ---------------------------------------------------------------- as estatísticas do registro


def _stat_converter(field_type: pa.DataType) -> Callable[[str], object] | None:
    """A conversão do texto do ``RETURN_STATS`` para o valor que o log guarda, ou ``None`` quando o
    tipo fica sem mínimo e máximo.

    Inteiro, data, ``Double`` e texto transcrevem exato. ``decimal`` fica de fora porque o log
    guarda o mínimo e o máximo como número JSON, e um máximo abaixo do valor real poda o arquivo que
    tem a linha, sem erro, nos dois leitores; ``timestamp``, porque o log o guarda truncado em
    milissegundos.
    """
    if pa.types.is_integer(field_type):
        return int
    if pa.types.is_floating(field_type):
        return float
    if pa.types.is_date(field_type) or pa.types.is_string(field_type):
        return str
    return None


def _relative_file(filename: str, uri: str) -> str:
    """O caminho de um arquivo relativo à pasta da tabela, como o log o guarda."""
    prefix = uri.rstrip("/") + "/"
    if not filename.startswith(prefix):
        raise RegistrationRefused(f"{filename}: fora da pasta da tabela {uri}")
    return filename.removeprefix(prefix)


def file_from_return_stats(row: Mapping[str, object], table: sa.Table, uri: str) -> RegisteredFile:
    """O arquivo que o ``COPY ... (RETURN_STATS)`` do DuckDB gravou dentro da pasta da tabela.

    ``row`` é a linha do ``RETURN_STATS`` como dicionário (``filename``, ``count``,
    ``file_size_bytes``, ``column_statistics``). O ``null_count`` entra de toda coluna do contrato,
    e o mínimo e o máximo dos tipos que transcrevem exato, convertidos do texto; uma coluna só de
    nulos chega sem os dois.
    """
    contract = arrow_schema(table)
    minimum: dict[str, object] = {}
    maximum: dict[str, object] = {}
    nulls: dict[str, int] = {}
    for quoted_column, statistics in row["column_statistics"].items():
        name = quoted_column.strip('"')
        if name not in contract.names:
            continue
        nulls[name] = int(statistics["null_count"])
        convert = _stat_converter(contract.field(name).type)
        if convert is None or "min" not in statistics or "max" not in statistics:
            continue
        minimum[name] = convert(statistics["min"])
        maximum[name] = convert(statistics["max"])
    return RegisteredFile(
        path=_relative_file(str(row["filename"]), uri),
        size=int(row["file_size_bytes"]),
        rows=int(row["count"]),
        stats={"min": minimum, "max": maximum, "null_count": nulls},
    )


def _footer_statistics(footer: pq.ParquetFile, index: int) -> tuple[int | None, object, object]:
    """A soma dos nulos e o mínimo e o máximo da coluna ``index`` nos grupos de linhas do rodapé;
    a soma fica ``None`` quando algum grupo não tem a contagem de nulos, porque o leitor não pode
    supor zero, e o mínimo e o máximo ficam ``None`` quando algum grupo não os tem."""
    nulls = 0
    counted = True
    minimum = None
    maximum = None
    complete = True
    for group in range(footer.metadata.num_row_groups):
        statistics = footer.metadata.row_group(group).column(index).statistics
        if statistics is None:
            counted = False
            complete = False
            continue
        if statistics.has_null_count:
            nulls += statistics.null_count
        else:
            counted = False
        if not statistics.has_min_max:
            complete = False
            continue
        minimum = statistics.min if minimum is None else min(minimum, statistics.min)
        maximum = statistics.max if maximum is None else max(maximum, statistics.max)
    total_nulls = nulls if counted else None
    if not complete:
        return total_nulls, None, None
    return total_nulls, minimum, maximum


def file_from_footer(footer: pq.ParquetFile, path: str, size: int, rows: int,
                     table: sa.Table) -> RegisteredFile:
    """O arquivo que outro escritor gravou dentro da pasta da tabela, como o ``UNLOAD`` do
    Redshift, descrito pelo rodapé Parquet dele; protegida, para o motor Redshift.

    ``path`` é relativo à pasta da tabela, ``size`` e ``rows`` são os que o manifesto declara. O
    ``null_count`` entra de toda coluna do contrato com a contagem de nulos em todos os grupos de
    linhas, e o mínimo e o máximo só das colunas inteiras, ``Double`` e de data: o rodapé pode
    guardar o mínimo e o máximo de um texto truncados, e o PyArrow 25 não expõe a marca de exatidão
    do Parquet, então o texto fica sem os dois, e o leitor não poda por ele.
    """
    contract = arrow_schema(table)
    names = footer.schema_arrow.names
    minimum: dict[str, object] = {}
    maximum: dict[str, object] = {}
    nulls: dict[str, int] = {}
    for field in contract:
        if field.name not in names:
            continue
        null_count, low, high = _footer_statistics(footer, names.index(field.name))
        # A coluna sem a contagem de nulos em algum grupo fica fora do nullCount do log.
        if null_count is not None:
            nulls[field.name] = null_count
        convert = _stat_converter(field.type)
        if convert is None or pa.types.is_string(field.type) or low is None or high is None:
            continue
        minimum[field.name] = convert(low)
        maximum[field.name] = convert(high)
    return RegisteredFile(path=path, size=int(size), rows=int(rows),
                          stats={"min": minimum, "max": maximum, "null_count": nulls})


def _exact_statistic(field_type: pa.DataType) -> bool:
    """Se o mínimo e o máximo do tipo transcrevem exato no log: inteiro, data, ``Double`` e
    texto."""
    return _stat_converter(field_type) is not None


def _json_value(value: object) -> object:
    """O valor como o log o guarda: a data em ``AAAA-MM-DD``; os demais como chegam."""
    if isinstance(value, datetime.date):
        return value.isoformat()
    return value


def _finite(value: object) -> bool:
    """Se o valor pode entrar no JSON do log: um ``float`` infinito ou ``NaN`` não pode."""
    return not isinstance(value, float) or math.isfinite(value)


def _action_stats(file: RegisteredFile, contract: pa.Schema,
                  columns_without_min_max: Collection[str]) -> str:
    """O JSON de estatísticas da ação: ``numRecords``, o ``nullCount`` que o arquivo declara e o
    mínimo e o máximo dos tipos exatos, sem as colunas de ``columns_without_min_max`` e sem um
    extremo não finito, que o JSON não representa."""
    minimum: dict[str, object] = {}
    maximum: dict[str, object] = {}
    nulls: dict[str, int] = {}
    for field in contract:
        null_count = file.stats.get("null_count", {}).get(field.name)
        if null_count is not None:
            nulls[field.name] = int(null_count)
        if field.name in columns_without_min_max or not _exact_statistic(field.type):
            continue
        low = file.stats.get("min", {}).get(field.name)
        high = file.stats.get("max", {}).get(field.name)
        if low is None or high is None or not (_finite(low) and _finite(high)):
            continue
        minimum[field.name] = _json_value(low)
        maximum[field.name] = _json_value(high)
    stats = {"numRecords": file.rows, "minValues": minimum, "maxValues": maximum,
             "nullCount": nulls}
    return json.dumps(stats, allow_nan=False)


def _add_action(file: RegisteredFile, contract: pa.Schema, partition_by: str | None,
                value: str | None, columns_without_min_max: Collection[str]) -> AddAction:
    """A ação ``add`` de um arquivo conferido: caminho relativo, tamanho, partição e
    estatísticas."""
    partition_values = {partition_by: value} if partition_by else {}
    return AddAction(
        path=file.path,
        size=file.size,
        partition_values=partition_values,
        modification_time=int(time.time() * 1000),
        data_change=True,
        stats=_action_stats(file, contract, columns_without_min_max),
    )


# ---------------------------------------------------------------- as conferências do registro


def _physical_types(field_type: pa.DataType) -> frozenset[str]:
    """Os tipos físicos do Parquet que os leitores leem como o tipo lógico da coluna.

    O ``UNLOAD`` do Redshift grava o timestamp em ``INT96`` e o decimal em
    ``FIXED_LEN_BYTE_ARRAY``; o DuckDB e o delta-rs gravam os dois em ``INT64``, e o decimal de
    até 9 dígitos em ``INT32``; uma coluna ``long`` lê um arquivo em ``INT32``.
    """
    if pa.types.is_timestamp(field_type):
        return frozenset({"INT64", "INT96"})
    if pa.types.is_decimal(field_type):
        return frozenset({"INT32", "INT64", "FIXED_LEN_BYTE_ARRAY"})
    if pa.types.is_int64(field_type):
        return frozenset({"INT64", "INT32"})
    if pa.types.is_integer(field_type) or pa.types.is_date(field_type):
        return frozenset({"INT32"})
    if pa.types.is_floating(field_type):
        return frozenset({"DOUBLE"})
    if pa.types.is_boolean(field_type):
        return frozenset({"BOOLEAN"})
    return frozenset({"BYTE_ARRAY"})


def _check_relative_path(file: RegisteredFile) -> None:
    """O caminho relativo à pasta da tabela: o log nunca guarda um caminho absoluto, uma URI nem
    um ``..``, e uma pasta copiada abre na mesma versão."""
    parts = file.path.split("/")
    absolute = file.path.startswith("/") or "://" in file.path
    if not file.path or absolute or ".." in parts:
        raise RegistrationRefused(f"{file.path}: o caminho não é relativo à pasta da tabela")


def _check_file_size(storage: Storage, table_path: str, file: RegisteredFile) -> None:
    """O arquivo existe em ``<pasta da tabela>/<path>``, o caminho que o leitor resolve, com o
    tamanho que a ação declara."""
    found = storage.size(storage.join(table_path, file.path))
    if found != file.size:
        raise RegistrationRefused(
            f"{file.path}: ausente ou com {found} bytes, e a ação declara {file.size}")


def _check_footer_schema(footer: pq.ParquetFile, file: RegisteredFile, contract: pa.Schema,
                         partition_by: str | None) -> None:
    """O esquema do rodapé contra o do contrato, nome a nome.

    Nenhuma coluna do contrato ausente, porque o leitor a leria nula sem erro; o tipo físico entre
    os que os leitores leem como o lógico; a coluna de partição fora do arquivo, porque ela vive na
    ação e o ``COPY`` posicional do Redshift a leria como a coluna seguinte; e as colunas do
    contrato na ordem dele, pelo mesmo ``COPY``. Uma coluna fora do contrato passa, porque os
    leitores a ignoram.
    """
    physical = {}
    order = []
    for index in range(len(footer.schema)):
        column = footer.schema.column(index)
        physical[column.name] = column.physical_type
        if column.name in contract.names:
            order.append(column.name)
    if partition_by in physical:
        raise RegistrationRefused(
            f"{file.path}: a coluna de partição {partition_by} está dentro do arquivo")
    expected = []
    for field in contract:
        if field.name == partition_by:
            continue
        if field.name not in physical:
            raise RegistrationRefused(f"{file.path}: coluna {field.name} do contrato ausente")
        allowed = _physical_types(field.type)
        if physical[field.name] not in allowed:
            raise RegistrationRefused(
                f"{file.path}: {field.name} em {physical[field.name]}, "
                f"fora de {sorted(allowed)} para {field.type}")
        expected.append(field.name)
    if order != expected:
        raise RegistrationRefused(f"{file.path}: colunas na ordem {order}, e o contrato {expected}")


def _check_partition_path(file: RegisteredFile, partition_by: str | None,
                          value: str | None) -> None:
    """O arquivo dentro da pasta ``<coluna>=<valor>/`` da partição registrada."""
    if partition_by is None:
        return
    parts = file.path.split("/")
    if len(parts) < 2 or parts[0] != f"{partition_by}={value}":
        raise RegistrationRefused(f"{file.path}: fora da pasta da partição {partition_by}={value}")


def _footer_null_count(footer: pq.ParquetFile, index: int) -> int:
    """A soma dos nulos da coluna ``index`` nos grupos de linhas do rodapé; um grupo sem a
    estatística conta zero."""
    nulls = 0
    for group in range(footer.metadata.num_row_groups):
        statistics = footer.metadata.row_group(group).column(index).statistics
        if statistics is not None and statistics.has_null_count:
            nulls += statistics.null_count
    return nulls


def _check_not_null(footer: pq.ParquetFile, file: RegisteredFile, contract: pa.Schema) -> None:
    """Nenhum nulo nas colunas ``NOT NULL`` do contrato, pela contagem de nulos de cada grupo de
    linhas do rodapé: o DuckDB grava toda coluna como ``optional``, e o leitor devolveria o nulo
    que o ``write_deltalake`` recusaria. Um grupo sem estatística da coluna não é conferido."""
    names = footer.schema_arrow.names
    for field in contract:
        if field.nullable or field.name not in names:
            continue
        nulls = _footer_null_count(footer, names.index(field.name))
        if nulls:
            raise RegistrationRefused(f"{file.path}: {nulls} nulos na coluna NOT NULL {field.name}")


def _check_file_rows(footer: pq.ParquetFile, file: RegisteredFile) -> None:
    """As linhas do rodapé iguais às que o arquivo declara, que viram o ``numRecords`` da ação."""
    if footer.metadata.num_rows != file.rows:
        raise RegistrationRefused(
            f"{file.path}: {footer.metadata.num_rows} linhas no rodapé, {file.rows} declaradas")


def _check_row_counts(total: int, expected_rows: int | None) -> None:
    """A soma das linhas dos arquivos igual à contagem da fonte, quando o chamador a tem."""
    if expected_rows is not None and total != expected_rows:
        raise RegistrationRefused(f"{total} linhas nos arquivos, {expected_rows} na fonte")


def _check_file(storage: Storage, table_path: str, file: RegisteredFile, contract: pa.Schema,
                partition_by: str | None, value: str | None) -> None:
    """As conferências de um arquivo, com um GET do rodapé no S3."""
    _check_relative_path(file)
    _check_file_size(storage, table_path, file)
    footer = pq.ParquetFile(storage.open_input_file(storage.join(table_path, file.path)))
    _check_footer_schema(footer, file, contract, partition_by)
    _check_not_null(footer, file, contract)
    _check_partition_path(file, partition_by, value)
    _check_file_rows(footer, file)


def _commit_actions(dt: DeltaTable, table_name: str, partition_by: str | None,
                    actions: list[AddAction], value: str | None, metadata: Mapping[str, str],
                    schema: object | None = None) -> None:
    """Um commit ``overwrite`` das ações: da partição de ``value``, ou da tabela inteira sem ele.

    ``schema`` é o esquema Delta do commit; ``None`` mantém o da tabela. ``CommitFailedError``, o
    de outro registro da mesma partição a partir da mesma versão, sobe como ``ExecutionConflict``,
    com ``table_name`` na mensagem.
    """
    filters = None
    if partition_by and value is not None:
        filters = [(partition_by, "=", value)]
    if schema is None:
        schema = dt.schema()
    try:
        dt.create_write_transaction(
            actions,
            mode="overwrite",
            schema=schema,
            partition_by=[partition_by] if partition_by else None,
            partition_filters=filters,
            commit_properties=CommitProperties(custom_metadata=dict(metadata)),
        )
    except CommitFailedError as error:
        raise ExecutionConflict(f"{table_name} partição {value}: {error}") from None


def register_files(uri: str, table: sa.Table, files: list[RegisteredFile], value: str | None,
                   metadata: Mapping[str, str], storage: Storage, expected_rows: int | None = None,
                   columns_without_min_max: Collection[str] = ()) -> int:
    """Registra no log, num commit ``overwrite`` da partição, arquivos que outro escritor gravou
    dentro da pasta da tabela.

    ``create_write_transaction`` grava a ação como a recebe, e os leitores obedecem à ação, não ao
    arquivo; por isso cada arquivo passa antes pelas conferências do rodapé, um GET por arquivo: o
    arquivo existe com o tamanho declarado; o esquema do rodapé tem cada coluna do contrato, na
    ordem dele, num tipo físico que os leitores leem como o lógico, e não tem a coluna de partição;
    as colunas ``NOT NULL`` não têm nulo na contagem do rodapé; o caminho está na pasta da
    partição; as linhas do rodapé são as declaradas.

    A ação leva ``numRecords``, o ``nullCount`` e o mínimo e o máximo das colunas inteiras, de data,
    ``Double`` e texto. Depois do commit, ``read_back`` relê a versão pelos dois leitores e a
    desfaz na diferença.

    Exemplo:

    .. code-block:: python

        file = RegisteredFile("data_str=2026-08-31/exec-42_ab12.parquet", 4096, 1000, stats)
        register_files(uri, Operacao.__table__, [file], "2026-08-31",
                       commit_metadata("exec-42", {}), storage, expected_rows=1000)

    :param uri: a URI da pasta da tabela, sob a raiz do banco.
    :param table: a tabela do modelo.
    :param files: os arquivos que o escritor declarou, com o caminho relativo à pasta da tabela.
    :param value: o valor da partição, na regra de ``schema.PARTITION_VALUE``; ``None`` numa
        tabela sem partição.
    :param metadata: os metadados do commit, de ``commit_metadata``.
    :param storage: o armazenamento da raiz do banco.
    :param expected_rows: a contagem da fonte, quando o chamador a tem, que a soma das linhas
        dos arquivos tem de igualar; ``None`` não confere a soma.
    :param columns_without_min_max: as colunas cujo mínimo e máximo ficam fora da ação, as
        ``Double`` com valor não finito na partição (issue #59).
    :return: a versão do commit.
    :raises ContractError: ``value`` ausente numa tabela particionada, presente numa tabela sem
        partição ou fora da regra.
    :raises ValueError: ``uri`` fora da raiz de ``storage``.
    :raises RegistrationRefused: uma conferência reprovou, sem commit, e o arquivo fica órfão até
        ``vacuum(full=True)``; ou a releitura reprovou e desfez o commit.
    :raises ExecutionConflict: um segundo registro da mesma partição a partir da mesma versão.
    """
    value = checked_value(table, value)
    partition_by = table_options(table).partition_by
    contract = arrow_schema(table)
    table_path = storage.relative(uri)
    total = 0
    for file in files:
        _check_file(storage, table_path, file, contract, partition_by, value)
        total += file.rows
    _check_row_counts(total, expected_rows)
    actions = []
    for file in files:
        actions.append(_add_action(file, contract, partition_by, value, columns_without_min_max))
    dt = open_table(uri, storage)
    _commit_actions(dt, table.name, partition_by, actions, value=value, metadata=metadata)
    # create_write_transaction não atualiza o objeto: a versão vem de uma leitura nova do log, que
    # com uma execução por ambiente é a do próprio commit.
    version = open_table(uri, storage).version()
    read_back(uri, table, value, total, storage)
    return version


# ---------------------------------------------------------------- a releitura


@dataclasses.dataclass(frozen=True)
class _Reading:
    """O que um leitor viu na partição: as linhas e o menor e o maior valor de cada coluna da
    chave, ``None`` quando não há o que ler."""

    rows: int
    minimum: tuple[object, ...]
    maximum: tuple[object, ...]


def _partition_filter(partition_by: str | None, value: str | None) -> str:
    """O ``WHERE`` da partição no texto do DuckDB; vazio sem partição ou na tabela inteira."""
    if partition_by is None or value is None:
        return ""
    return f" WHERE {quoted(partition_by)} = {literal(value)}"


def _duckdb_reading(uri: str, version: int, table: sa.Table, value: str | None,
                    keys: tuple[str, ...], storage: Storage) -> _Reading:
    """A leitura pelo ``delta_scan`` do DuckDB, numa conexão própria."""
    measures = ["count(*)"]
    for name in keys:
        measures.append(f"min({quoted(name)})")
        measures.append(f"max({quoted(name)})")
    where = _partition_filter(table_options(table).partition_by, value)
    text = (f"SELECT {', '.join(measures)} FROM delta_scan({literal(uri)}, "
            f"version := {version}){where}")
    connection = storage.duckdb_connect()
    try:
        row = connection.execute(text).fetchone()
    finally:
        connection.close()
    # A linha é a contagem seguida de min e max alternados, um par por coluna da chave.
    rows = row[0]
    minimums = tuple(row[1::2])
    maximums = tuple(row[2::2])
    return _Reading(rows, minimums, maximums)


def _arrow_reading(dt: DeltaTable, table: sa.Table, value: str | None,
                   keys: tuple[str, ...]) -> _Reading:
    """A leitura pelo dataset Arrow do delta-rs, lote a lote, com a memória de um lote."""
    partition_by = table_options(table).partition_by
    condition = None
    if partition_by is not None and value is not None:
        condition = ds.field(partition_by) == value
    scanner = dt.to_pyarrow_dataset().scanner(columns=list(keys), filter=condition)
    rows = 0
    lows: dict[str, list[object]] = {name: [] for name in keys}
    highs: dict[str, list[object]] = {name: [] for name in keys}
    for batch in scanner.to_batches():
        rows += batch.num_rows
        _collect_extremes(batch, lows, highs)
    minimum = tuple(min(lows[name], default=None) for name in keys)
    maximum = tuple(max(highs[name], default=None) for name in keys)
    return _Reading(rows, minimum, maximum)


def _collect_extremes(batch: pa.RecordBatch, lows: dict[str, list[object]],
                      highs: dict[str, list[object]]) -> None:
    """Acrescenta a ``lows`` e a ``highs`` o menor e o maior valor de cada coluna do lote; uma
    coluna só de nulos no lote não acrescenta nada."""
    for name in lows:
        extremes = pc.min_max(batch.column(name))
        if extremes["min"].is_valid:
            lows[name].append(extremes["min"].as_py())
            highs[name].append(extremes["max"].as_py())


def _log_reading(dt: DeltaTable, table: sa.Table, value: str | None,
                 keys: tuple[str, ...]) -> _Reading:
    """O que o log declara na partição: a soma de ``numRecords`` e o menor mínimo e o maior máximo
    registrados de cada coluna da chave, ``None`` quando algum arquivo não tem a estatística."""
    actions = pa.table(dt.get_add_actions(flatten=True))
    partition_by = table_options(table).partition_by
    if partition_by is not None and value is not None and actions.num_rows:
        actions = actions.filter(pc.equal(actions.column(f"partition.{partition_by}"), value))
    rows = pc.sum(actions.column("num_records")).as_py() or 0
    minimum = []
    maximum = []
    for name in keys:
        minimum.append(_logged_statistic(actions, f"min.{name}", pc.min))
        maximum.append(_logged_statistic(actions, f"max.{name}", pc.max))
    return _Reading(rows, tuple(minimum), tuple(maximum))


def _read_back_problems(table: sa.Table, keys: tuple[str, ...], expected_rows: int,
                        log_reading: _Reading, arrow_reading: _Reading,
                        duckdb_reading: _Reading) -> list[str]:
    """As diferenças entre os leitores, o log e a contagem esperada.

    As linhas iguais nos quatro; o menor e o maior valor de cada coluna da chave iguais nos dois
    leitores; e o mínimo e o máximo registrados no log, das colunas de tipo exato, como limites dos
    lidos: um máximo abaixo do lido podaria o arquivo que tem a linha.
    """
    problems = []
    counts = {"esperadas": expected_rows, "log": log_reading.rows,
              "delta-rs": arrow_reading.rows, "delta_scan": duckdb_reading.rows}
    if len(set(counts.values())) > 1:
        problems.append(f"linhas {counts}")
    arrow_extremes = (arrow_reading.minimum, arrow_reading.maximum)
    duckdb_extremes = (duckdb_reading.minimum, duckdb_reading.maximum)
    if arrow_extremes != duckdb_extremes:
        problems.append(f"chave {keys}: delta-rs {arrow_reading.minimum}..{arrow_reading.maximum}"
                        f", delta_scan {duckdb_reading.minimum}..{duckdb_reading.maximum}")
    contract = arrow_schema(table)
    for index, name in enumerate(keys):
        low, high = log_reading.minimum[index], log_reading.maximum[index]
        read_low, read_high = arrow_reading.minimum[index], arrow_reading.maximum[index]
        exact = _exact_statistic(contract.field(name).type)
        if not exact or low is None or high is None or read_low is None:
            continue
        if low > read_low or high < read_high:
            problems.append(f"{name}: o log registra {low}..{high}, e os dados têm "
                            f"{read_low}..{read_high}")
    return problems


def read_back(uri: str, table: sa.Table, value: str | None, expected_rows: int,
              storage: Storage) -> None:
    """Relê a versão recém-commitada pelos dois leitores e a desfaz quando eles discordam.

    O delta-rs, pelo dataset Arrow, e o ``delta_scan`` do DuckDB, numa conexão própria, contam as
    linhas e leem o menor e o maior valor de cada coluna da primeira chave do modelo na partição.
    A releitura pega o que as conferências do rodapé não veem, um leitor que não lê o arquivo como
    o outro (o ``parquet.field.id`` no esquema Delta fez o ``delta_scan`` ler toda coluna como nula
    com a contagem certa), e uma estatística do log que podaria o arquivo certo.

    Exemplo:

    .. code-block:: python

        read_back(uri, Operacao.__table__, "2026-08-31", 1000, storage)   # None quando concordam

    :param uri: a URI da pasta da tabela, sob a raiz do banco.
    :param table: a tabela do modelo.
    :param value: o valor da partição relida; ``value=None`` relê a tabela inteira.
    :param expected_rows: as linhas que o log e os dois leitores têm de contar.
    :param storage: o armazenamento da raiz do banco.
    :raises RegistrationRefused: uma diferença entre os leitores, o log e ``expected_rows``,
        depois de ``restore(version - 1)``; a mensagem traz as leituras.
    """
    dt = open_table(uri, storage)
    version = dt.version()
    keys = table_options(table).keys[0]
    log_reading = _log_reading(dt, table, value, keys)
    arrow_reading = _arrow_reading(dt, table, value, keys)
    duckdb_reading = _duckdb_reading(uri, version, table, value, keys, storage)
    problems = _read_back_problems(table, keys, expected_rows, log_reading, arrow_reading,
                                   duckdb_reading)
    if not problems:
        return
    dt.restore(version - 1)
    raise RegistrationRefused(
        f"{table.name} partição {value}: a releitura da versão {version} reprovou e a tabela "
        f"voltou à versão {version - 1}: {'; '.join(problems)}")


# ---------------------------------------------------------------- a evolução do esquema


def _comment(field: pa.Field) -> str:
    """O comentário de um campo Arrow, vazio quando não há."""
    return (field.metadata or {}).get(b"comment", b"").decode("utf-8")


def _has_data(dt: DeltaTable) -> bool:
    """Se a versão carregada tem algum arquivo de dados."""
    return pa.table(dt.get_add_actions(flatten=True)).num_rows > 0


def _new_checks(table: sa.Table, dt: DeltaTable) -> list[str]:
    """As ``CheckConstraint`` do modelo que a tabela Delta não tem, pelo nome; sem nome é
    ``ContractError``, porque o Delta guarda o ``CHECK`` pelo nome."""
    configuration = dt.metadata().configuration
    names = []
    for constraint in table.constraints:
        if not isinstance(constraint, sa.CheckConstraint):
            continue
        if not isinstance(constraint.name, str):
            raise ContractError(f"{table.name}: CHECK sem nome ({constraint.sqltext}); dê um nome")
        if f"delta.constraints.{constraint.name}" not in configuration:
            names.append(constraint.name)
    return names


def schema_diff(table: sa.Table, dt: DeltaTable) -> SchemaDiff:
    """O diff entre o esquema do contrato e o da versão carregada, sem alterar nada.

    Aditivos: coluna nova anulável (ou ``NOT NULL`` numa tabela ainda sem dados), ``NOT NULL``
    relaxado, ``CheckConstraint`` nova, comentário de tabela ou de coluna divergente. Destrutivos:
    coluna ``NOT NULL`` nova numa tabela com dados, que o delta-rs aceitaria e deixaria nula,
    ``NOT NULL`` numa coluna anulável, remoção e mudança de tipo; uma renomeação aparece como uma
    remoção e uma coluna nova.

    Exemplo:

    .. code-block:: python

        schema_diff(Operacao.__table__, open_table(uri, storage)).changes   # False

    :param table: a tabela do modelo.
    :param dt: a tabela aberta por ``open_table``.
    :return: o diff, separado em aditivo e destrutivo.
    :raises ContractError: uma ``CheckConstraint`` do modelo sem nome, porque o Delta guarda o
        ``CHECK`` pelo nome.
    """
    contract = arrow_schema(table)
    current = pa.schema(dt.schema())
    with_data = _has_data(dt)
    add, relax, comments, destructive = [], [], [], []
    # As colunas novas: anuláveis, ou NOT NULL numa tabela ainda sem dados.
    for field in contract:
        if field.name in current.names:
            continue
        if field.nullable or not with_data:
            add.append(field)
        else:
            destructive.append(f"{field.name}: coluna NOT NULL nova numa tabela com dados")
    # As colunas do modelo que a tabela já tem: o tipo, a nulidade e o comentário.
    for field in contract:
        if field.name not in current.names:
            continue
        existing = current.field(field.name)
        if existing.type != field.type:
            destructive.append(f"{field.name}: tipo {existing.type} para {field.type}")
            continue
        if field.nullable and not existing.nullable:
            relax.append(field.name)
        if existing.nullable and not field.nullable:
            destructive.append(f"{field.name}: NOT NULL numa coluna anulável")
        if _comment(field) != _comment(existing):
            comments.append(field.name)
    # As colunas da tabela que o modelo não tem mais.
    for name in current.names:
        if name not in contract.names:
            destructive.append(f"{name}: removida do modelo")
    description = None
    if (table.comment or "") != (dt.metadata().description or ""):
        description = table.comment or ""
    return SchemaDiff(
        add=tuple(add),
        relax=tuple(relax),
        checks=tuple(_new_checks(table, dt)),
        description=description,
        comments=tuple(comments),
        destructive=tuple(destructive),
    )


def _check_constraint_texts(table: sa.Table, names: Collection[str]) -> dict[str, str]:
    """O texto SQL de cada ``CheckConstraint`` do modelo cujo nome está em ``names``."""
    texts = {}
    for constraint in table.constraints:
        if isinstance(constraint, sa.CheckConstraint) and constraint.name in names:
            texts[constraint.name] = str(constraint.sqltext)
    return texts


def reconcile(uri: str, table: sa.Table, storage: Storage) -> SchemaDiff:
    """Aplica o diff aditivo entre o modelo e a tabela.

    Cada parte é um commit só de metadados: ``add_columns`` com o campo do esquema Delta do
    contrato, comentário incluído; ``drop_column_not_null``; ``add_constraint``;
    ``set_table_description``; ``set_column_metadata`` com o comentário. A versão anterior continua
    legível com o esquema antigo, e a segunda chamada não commita.

    Exemplo:

    .. code-block:: python

        reconcile(uri, Operacao.__table__, storage).add   # os campos acrescentados

    :param uri: a URI da pasta da tabela, sob a raiz do banco.
    :param table: a tabela do modelo.
    :param storage: o armazenamento da raiz do banco.
    :return: o diff aplicado.
    :raises SchemaDiffRefused: o diff destrutivo, sem alteração; a mensagem traz a lista e a
        instrução de ``rewrite``.
    :raises ContractError: uma ``CheckConstraint`` do modelo sem nome, como em ``schema_diff``.
    """
    dt = open_table(uri, storage)
    diff = schema_diff(table, dt)
    if diff.destructive:
        raise SchemaDiffRefused(
            f"{table.name}: diff destrutivo, só por rewrite(uri, table, storage, expressions): "
            f"{'; '.join(diff.destructive)}")
    fields = {}
    for field in delta_schema(table).fields:
        fields[field.name] = field
    if diff.add:
        dt.alter.add_columns([fields[field.name] for field in diff.add])
    for name in diff.relax:
        dt.alter.drop_column_not_null(name)
    if diff.checks:
        dt.alter.add_constraint(_check_constraint_texts(table, diff.checks))
    if diff.description is not None:
        dt.alter.set_table_description(diff.description)
    contract = arrow_schema(table)
    for name in diff.comments:
        dt.alter.set_column_metadata(name, {"comment": _comment(contract.field(name))})
    return diff


def _check_expressions(table: sa.Table, expressions: Mapping[str, str]) -> None:
    """As chaves de ``expressions`` são colunas do contrato: um nome errado seria ignorado."""
    unknown = sorted(set(expressions) - set(table.c.keys()))
    if unknown:
        raise ContractError(f"{table.name}: expressions para colunas fora do modelo: {unknown}")


def _rewrite_select(table: sa.Table, expressions: Mapping[str, str], source: str) -> str:
    """O ``SELECT`` do contrato sobre a versão atual: cada coluna em ``CAST`` para o tipo do DuckDB,
    com a expressão de ``expressions`` ou o nome da coluna."""
    columns = []
    for column in table.columns:
        expression = expressions.get(column.name, quoted(column.name))
        converted = f"CAST({expression} AS {sql_type(column, 'duckdb')})"
        columns.append(f"{converted} AS {quoted(column.name)}")
    return f"SELECT {', '.join(columns)} FROM {source}"


def _nonfinite_by_partition(connection: duckdb.DuckDBPyConnection, table: sa.Table,
                            select: str) -> dict[str | None, tuple[str, ...]]:
    """As colunas ``Double`` com valor não finito em cada partição do ``SELECT``, que ficam sem
    mínimo e máximo no log (issue #59)."""
    doubles = double_columns(table)
    if not doubles:
        return {}
    partition_by = table_options(table).partition_by
    measures = []
    for name in doubles:
        measures.append(f"count(*) FILTER (WHERE NOT isfinite({quoted(name)}))")
    counts = ", ".join(measures)
    # Uma linha por partição, com o valor dela na frente das contagens; None numa tabela sem
    # partição.
    if partition_by is None:
        counted = connection.execute(f"SELECT {counts} FROM ({select})").fetchone()
        rows = [(None, *counted)]
    else:
        rows = connection.execute(
            f"SELECT {quoted(partition_by)}, {counts} FROM ({select}) GROUP BY 1").fetchall()
    found = {}
    for value, *counts in rows:
        found[value] = tuple(name for name, count in zip(doubles, counts) if count)
    return found


def _copy_rewrite(connection: duckdb.DuckDBPyConnection, uri: str, table: sa.Table,
                  select: str) -> list[dict]:
    """O ``COPY ... RETURN_STATS`` da tabela inteira para arquivos novos na pasta dela: particionado
    por ``PARTITION_BY``, que tira a coluna de partição dos arquivos, ou um arquivo só."""
    partition_by = table_options(table).partition_by
    if partition_by is None:
        target = f"{uri}/rewrite_{uuid.uuid4().hex}.parquet"
        options = "FORMAT parquet, RETURN_STATS"
    else:
        target = uri
        options = (f"FORMAT parquet, PARTITION_BY ({quoted(partition_by)}), APPEND true, "
                   "FILENAME_PATTERN 'rewrite_{uuid}', RETURN_STATS")
    cursor = connection.execute(f"COPY ({select}) TO {literal(target)} ({options})")
    names = [column[0] for column in cursor.description]
    rows = []
    for row in cursor.fetchall():
        rows.append(dict(zip(names, row)))
    return rows


def _written_partition(row: Mapping[str, object], partition_by: str | None) -> str | None:
    """O valor de partição de um arquivo do ``COPY ... PARTITION_BY``, de ``partition_keys``;
    ``None`` numa tabela sem partição."""
    if partition_by is None:
        return None
    partition_keys = dict(row["partition_keys"] or {})
    return partition_keys.get(partition_by)


def _rewritten_actions(
    written: list[dict], table: sa.Table, uri: str, storage: Storage,
    nonfinite: Mapping[str | None, tuple[str, ...]],
) -> tuple[list[AddAction], int]:
    """As ações dos arquivos que o ``COPY`` da reescrita gravou, cada arquivo depois das
    conferências de ``register_files``, e a soma das linhas deles."""
    partition_by = table_options(table).partition_by
    contract = arrow_schema(table)
    table_path = storage.relative(uri)
    actions = []
    total = 0
    for row in written:
        file = file_from_return_stats(row, table, uri)
        value = _written_partition(row, partition_by)
        _check_file(storage, table_path, file, contract, partition_by, value)
        actions.append(_add_action(file, contract, partition_by, value, nonfinite.get(value, ())))
        total += file.rows
    return actions, total


def rewrite(uri: str, table: sa.Table, storage: Storage,
            expressions: Mapping[str, str] | None = None) -> int:
    """Reescreve a tabela inteira com o esquema do contrato num único commit, sem predicado, com
    memória constante.

    O ``COPY ... PARTITION_BY ... RETURN_STATS`` do DuckDB lê a versão atual por ``delta_scan`` e
    grava arquivos novos na pasta da tabela, que entram num ``create_write_transaction(mode=
    "overwrite", schema=delta_schema(table))``, depois das conferências de ``register_files``; a
    versão anterior continua legível com o esquema antigo, e ``read_back`` roda depois do commit.
    Cada coluna sai em ``CAST`` para o tipo do contrato, e a remoção é a coluna que o contrato não
    tem mais. As colunas ``Double`` com valor não finito em cada partição ficam sem mínimo e
    máximo no log.

    Exemplo:

    .. code-block:: python

        rewrite(uri, Operacao.__table__, storage, expressions={"valor": '"valor_bruto"'})

    :param uri: a URI da pasta da tabela, sob a raiz do banco.
    :param table: a tabela do modelo.
    :param storage: o armazenamento da raiz do banco.
    :param expressions: por coluna do contrato, a expressão SQL do DuckDB sobre a versão atual que
        a preenche: o nome antigo numa renomeação, o valor de uma coluna ``NOT NULL`` nova. Uma
        coluna do contrato ausente da versão atual e fora de ``expressions`` é ``BinderException``
        do DuckDB, antes de qualquer commit. ``None`` preenche cada coluna pelo próprio nome.
    :return: a versão do commit.
    :raises ContractError: uma chave de ``expressions`` fora das colunas do modelo, que seria
        ignorada.
    :raises ValueError: ``uri`` fora da raiz de ``storage``.
    :raises RegistrationRefused: uma conferência de ``register_files`` reprovou um arquivo novo,
        sem commit; ou a releitura reprovou e desfez o commit.
    :raises ExecutionConflict: o commit falhou no delta-rs com ``CommitFailedError``.
    """
    expressions = dict(expressions or {})
    _check_expressions(table, expressions)
    dt = open_table(uri, storage)
    source = f"delta_scan({literal(uri)}, version := {dt.version()})"
    select = _rewrite_select(table, expressions, source)
    connection = storage.duckdb_connect()
    try:
        nonfinite = _nonfinite_by_partition(connection, table, select)
        written = _copy_rewrite(connection, uri, table, select)
    finally:
        connection.close()
    actions, total = _rewritten_actions(written, table, uri, storage, nonfinite)
    partition_by = table_options(table).partition_by
    _commit_actions(dt, table.name, partition_by, actions, value=None, metadata={},
                    schema=delta_schema(table))
    version = open_table(uri, storage).version()
    read_back(uri, table, None, total, storage)
    return version


# ---------------------------------------------------------------- o log e a publicação


def _partition_of(file_action: Mapping[str, object], partition_by: str | None) -> str | None:
    """O valor de partição de uma ação ``add`` ou ``remove`` do log; ``None`` sem partição."""
    if partition_by is None:
        return None
    return file_action.get("partitionValues", {}).get(partition_by)


def _changed_partitions(log_text: str, partition_by: str | None) -> set[str | None]:
    """As partições das ações ``add`` e ``remove`` com ``dataChange`` verdadeiro num arquivo do
    log; a compactação grava ``dataChange`` falso e não conta."""
    changed: set[str | None] = set()
    for line in log_text.splitlines():
        if not line.strip():
            continue
        action = json.loads(line)
        file_action = action.get("add") or action.get("remove")
        if file_action is not None and file_action.get("dataChange", True):
            changed.add(_partition_of(file_action, partition_by))
    return changed


def version_diff(uri: str, published: int, current: int, table: sa.Table,
                 storage: Storage) -> set[str | None]:
    """As partições com dados alterados nos commits depois de ``published`` até ``current``.

    Lê os arquivos ``_delta_log/<versão>.json`` pelo ``Storage`` e recolhe a partição das ações
    ``add`` e ``remove`` com ``dataChange`` verdadeiro; a compactação grava ``dataChange`` falso e
    não conta.

    Exemplo:

    .. code-block:: python

        version_diff(uri, 57, 58, Operacao.__table__, storage)   # {"2026-08-31"}

    :param uri: a URI da pasta da tabela, sob a raiz do banco.
    :param published: a versão de partida, como a publicada.
    :param current: a versão de chegada, como a atual.
    :param table: a tabela do modelo.
    :param storage: o armazenamento da raiz do banco.
    :return: os valores das partições alteradas, vazio com ``published`` igual a ``current``;
        numa tabela sem partição, ``{None}`` quando houve alteração.
    :raises ValueError: ``published`` depois de ``current``, ou ``uri`` fora da raiz de
        ``storage``.
    :raises LogUnavailable: um arquivo do log ausente, com a instrução de publicar a tabela
        inteira: a limpeza que o apagaria também torna ilegível a versão publicada.
    """
    if published > current:
        raise ValueError(f"versão publicada {published} depois da atual {current}")
    partition_by = table_options(table).partition_by
    table_path = storage.relative(uri)
    changed: set[str | None] = set()
    for version in range(published + 1, current + 1):
        path = storage.join(table_path, "_delta_log", f"{version:020d}.json")
        try:
            text, _ = storage.read_text(path)
        except FileNotFoundError:
            raise LogUnavailable(
                f"{table.name}: o log da versão {version} não existe; as partições alteradas entre "
                f"{published} e {current} não podem ser lidas, publique a tabela inteira") from None
        changed.update(_changed_partitions(text, partition_by))
    return changed


def partition_values(dt: DeltaTable, partition_by: str | None) -> list[str | None]:
    """Os valores de partição com algum arquivo na versão carregada, em ordem de texto; numa tabela
    sem partição, ``[None]`` quando ela tem arquivo e ``[]`` quando não tem. Protegida, para o
    motor Redshift, a publicação e a carga inicial, que carregam uma partição por vez."""
    actions = pa.table(dt.get_add_actions(flatten=True))
    if actions.num_rows == 0:
        return []
    if partition_by is None:
        return [None]
    values = set(actions.column(f"partition.{partition_by}").to_pylist())
    return sorted(values)


def _in_partitions(action: Mapping[str, object], partition_columns: list[str],
                   partitions: list[str] | None) -> bool:
    """Se a ação ``add`` está numa das partições pedidas; toda ação está, com ``partitions=None``
    ou numa tabela sem partição."""
    if partitions is None or not partition_columns:
        return True
    return action[f"partition.{partition_columns[0]}"] in partitions


def copy_manifest(uri: str, version: int, partitions: list[str] | None, destination: str,
                  storage: Storage) -> str:
    """Grava o manifesto do ``COPY ... MANIFEST`` do Redshift com os arquivos da versão nas
    partições pedidas.

    Cada entrada é ``{"url": "<pasta da tabela>/<path>", "mandatory": true, "meta":
    {"content_length": <size_bytes>}}``, das ações ``add`` da versão.

    Exemplo:

    .. code-block:: python

        copy_manifest(uri, 58, ["2026-08-31"],
                      storage.uri_of("prd/publicacao/exec-42/cad_operacoes/2026-08-31.manifest"),
                      storage)

    :param uri: a URI da pasta da tabela, sob a raiz do banco.
    :param version: a versão cujos arquivos entram.
    :param partitions: os valores das partições pedidas, todas com ``None``; numa tabela sem
        partição, todo arquivo entra.
    :param destination: a URI do manifesto sob a raiz, em ``publicacao/`` ou ``staging/``; um
        manifesto existente é substituído.
    :param storage: o armazenamento da raiz do banco.
    :return: a URI do manifesto, ``destination``.
    :raises ValueError: ``destination`` fora da raiz de ``storage``.
    """
    dt = open_table(uri, storage, version)
    partition_columns = dt.metadata().partition_columns
    entries = []
    for action in pa.table(dt.get_add_actions(flatten=True)).to_pylist():
        if not _in_partitions(action, partition_columns, partitions):
            continue
        entries.append({
            "url": f"{uri.rstrip('/')}/{action['path']}",
            "mandatory": True,
            "meta": {"content_length": action["size_bytes"]},
        })
    manifest = json.dumps({"entries": entries}, indent=2)
    storage.write_text(storage.relative(destination), manifest)
    return destination


# ---------------------------------------------------------------- os snapshots do banco


def read_snapshots(storage: Storage, environment: str) -> tuple[dict, str | None]:
    """O arquivo de controle dos snapshots do ambiente e a impressão digital dele.

    Exemplo:

    .. code-block:: python

        control, fingerprint = read_snapshots(storage, "prd")
        control["snapshots"]   # {"2026T3": {"cad_lancamentos": 143, ...}}

    :param storage: o armazenamento da raiz do banco.
    :param environment: o ambiente, a pasta sob a raiz do banco com o arquivo de controle.
    :return: o controle e a impressão digital, para a escrita condicional seguinte;
        ``({"snapshots": {}}, None)`` quando ele ainda não existe.
    """
    path = storage.join(environment, CONTROL_FILE)
    try:
        text, fingerprint = storage.read_text(path)
    except FileNotFoundError:
        return {"snapshots": {}}, None
    return json.loads(text), fingerprint


def snapshot(storage: Storage, environment: str, name: str, versions: Mapping[str, int]) -> dict:
    """Grava no arquivo de controle do ambiente a entrada ``{name: versions}`` de um snapshot do
    banco.

    A escrita é condicional: ``Storage.create_text`` no primeiro snapshot, e ``if_match`` com a
    impressão da leitura nos seguintes.

    Exemplo:

    .. code-block:: python

        snapshot(storage, "prd", "2026T3", {"cad_lancamentos": 143, "cad_contratos": 88})

    :param storage: o armazenamento da raiz do banco.
    :param environment: o ambiente, a pasta sob a raiz do banco com o arquivo de controle.
    :param name: o nome do snapshot, pela regra da partição (``schema.PARTITION_VALUE``).
    :param versions: a versão de cada tabela, pelo nome da tabela; as versões marcadas são as que
        ``vacuum_keeping_snapshots`` preserva.
    :return: o controle novo.
    :raises ContractError: o nome fora da regra da partição, antes de ler o arquivo de controle.
    :raises ValueError: um nome presente em ``snapshots`` ou em ``archived``, porque o nome dá a
        pasta ``arquivo/<nome>/``.
    :raises ConflictError: outro escritor entre a leitura e a escrita.
    """
    check_partition_value(name)
    control, fingerprint = read_snapshots(storage, environment)
    if name in control["snapshots"] or name in control.get("archived", {}):
        raise ValueError(f"{environment}: o snapshot {name} já existe")
    control["snapshots"][name] = dict(sorted(versions.items()))
    _write_control(storage, environment, control, fingerprint)
    return control


def _write_control(storage: Storage, environment: str, control: Mapping,
                   fingerprint: str | None) -> None:
    """Grava o arquivo de controle na escrita condicional: ``create_text`` quando ele ainda não
    existe, ``write_text`` com ``if_match`` e a impressão da leitura depois."""
    text = json.dumps(control, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    path = storage.join(environment, CONTROL_FILE)
    if fingerprint is None:
        storage.create_text(path, text)
    else:
        storage.write_text(path, text, if_match=fingerprint)


def archive_snapshot(storage: Storage, environment: str, name: str) -> dict:
    """Move a entrada do snapshot de ``snapshots`` para a chave irmã ``archived`` do arquivo de
    controle, na escrita condicional.

    A entrada arquivada deixa de prender as versões no ``vacuum``, que lê só ``snapshots``, e
    continua a ocupar o nome: ``snapshot`` o recusa, porque ele dá a pasta ``arquivo/<nome>/``.

    Exemplo:

    .. code-block:: python

        archive_snapshot(storage, "prd", "2026T3")["archived"]   # {"2026T3": {...}}

    :param storage: o armazenamento da raiz do banco.
    :param environment: o ambiente, a pasta sob a raiz do banco com o arquivo de controle.
    :param name: o nome do snapshot.
    :return: o controle novo.
    :raises ValueError: o nome ausente de ``snapshots``, ou o snapshot que um canal aponta, que
        ``set_channel`` move antes.
    :raises ConflictError: outro escritor entre a leitura e a escrita.
    """
    control, fingerprint = read_snapshots(storage, environment)
    if name not in control["snapshots"]:
        raise ValueError(f"{environment}: o snapshot {name} não está em snapshots")
    pointing = channels_pointing(control, name)
    if pointing:
        raise ValueError(f"{environment}: o snapshot {name} é o do canal {', '.join(pointing)}; "
                         "mova o canal antes (serialize-db channel)")
    control.setdefault("archived", {})[name] = control["snapshots"].pop(name)
    _write_control(storage, environment, control, fingerprint)
    return control


def channels_pointing(control: Mapping, snapshot: str) -> list[str]:
    """Os canais que apontam o snapshot, em ordem de nome; protegida, para ``archive_snapshot`` e
    ``serialize-db archive``, que recusam o snapshot de um canal."""
    pointing = []
    for name, target in control.get("channels", {}).items():
        if target == snapshot:
            pointing.append(name)
    return sorted(pointing)


def set_channel(storage: Storage, environment: str, name: str, snapshot: str) -> dict:
    """Aponta o canal ``name`` do ambiente para o snapshot, na escrita condicional do arquivo de
    controle, sob a chave irmã ``channels``.

    O canal ``default`` é o snapshot que o leitor Delta lê sem argumento e que ``serialize-db
    publish --channel default`` publica; só esta função, por ``serialize-db channel``, o move.

    Exemplo:

    .. code-block:: python

        set_channel(storage, "prd", "default", "2026T3")["channels"]   # {"default": "2026T3"}

    :param storage: o armazenamento da raiz do banco.
    :param environment: o ambiente, a pasta sob a raiz do banco com o arquivo de controle.
    :param name: o nome do canal, pela regra da partição (``schema.PARTITION_VALUE``); o canal
        ``current`` é reservado, a versão atual de cada tabela, e não fica no arquivo.
    :param snapshot: o nome do snapshot, presente em ``snapshots``.
    :return: o controle novo.
    :raises ContractError: o nome do canal fora da regra da partição, ou o nome ``current``,
        antes de ler o arquivo de controle.
    :raises ValueError: o snapshot ausente de ``snapshots``, o arquivado inclusive, porque o
        ``vacuum`` deixa de preservar as versões dele.
    :raises ConflictError: outro escritor entre a leitura e a escrita.
    """
    check_partition_value(name)
    if name == CURRENT_CHANNEL:
        raise ContractError(f"o canal {CURRENT_CHANNEL} é reservado: ele é a versão atual de cada "
                            "tabela, e nada o move")
    control, fingerprint = read_snapshots(storage, environment)
    if snapshot in control.get("archived", {}):
        raise ValueError(f"{environment}: o snapshot {snapshot} está arquivado, e o vacuum não "
                         "preserva as versões dele")
    if snapshot not in control["snapshots"]:
        raise ValueError(f"{environment}: o snapshot {snapshot} não está em snapshots")
    control.setdefault("channels", {})[name] = snapshot
    _write_control(storage, environment, control, fingerprint)
    return control


def channel_snapshot(control: Mapping, name: str) -> str:
    """O snapshot que o canal aponta no arquivo de controle.

    Exemplo:

    .. code-block:: python

        control, _ = read_snapshots(storage, "prd")
        channel_snapshot(control, "default")   # "2026T3"

    :param control: o arquivo de controle, como ``read_snapshots`` o devolve.
    :param name: o nome do canal.
    :return: o nome do snapshot.
    :raises ContractError: o canal ausente; a mensagem traz o ``serialize-db channel`` que o cria
        e o canal ``current``, que lê a versão atual sem canal no arquivo. O próprio ``current``
        também, porque não aponta snapshot.
    """
    # O canal reservado não fica no arquivo, e set_channel recusa criá-lo.
    if name == CURRENT_CHANNEL:
        raise ContractError(f"o canal {CURRENT_CHANNEL} não fica no arquivo de controle: ele é a "
                            "versão atual de cada tabela, sem snapshot")
    snapshot = control.get("channels", {}).get(name)
    if snapshot is None:
        raise ContractError(f"o canal {name} não existe no arquivo de controle; aponte-o com "
                            f"serialize-db channel --name {name} --snapshot <nome>, ou leia a "
                            f"versão atual pelo canal {CURRENT_CHANNEL}")
    return snapshot


def snapshot_versions(control: Mapping, name: str) -> dict[str, int]:
    """As versões de um snapshot presente em ``snapshots`` do arquivo de controle.

    Exemplo:

    .. code-block:: python

        snapshot_versions(control, "2026T3")   # {"cad_contratos": 88, "cad_lancamentos": 143}

    :param control: o arquivo de controle, como ``read_snapshots`` o devolve.
    :param name: o nome do snapshot.
    :return: a versão de cada tabela, pelo nome da tabela, numa cópia da entrada.
    :raises ContractError: o nome ausente de ``snapshots``; a mensagem diz quando ele está em
        ``archived``, que só o leitor Delta lê, pela cópia em ``arquivo/<nome>/``.
    """
    versions = control.get("snapshots", {}).get(name)
    if versions is None:
        if name in control.get("archived", {}):
            raise ContractError(f"o snapshot {name} está arquivado: só o leitor Delta o lê, pela "
                                f"cópia em arquivo/{name}/")
        raise ContractError(f"o snapshot {name} não existe no arquivo de controle")
    return dict(versions)


def vacuum_keeping_snapshots(uri: str, control: Mapping, table_name: str, storage: Storage,
                             retention_hours: int = 9600, apply: bool = False,
                             full: bool = False) -> list[str]:
    """O ``vacuum`` da tabela que preserva os arquivos das versões dos snapshots do banco.

    Exemplo:

    .. code-block:: python

        control, _ = read_snapshots(storage, "prd")
        vacuum_keeping_snapshots(uri, control, "cad_lancamentos", storage)   # a lista, sem apagar

    :param uri: a URI da pasta da tabela, sob a raiz do banco.
    :param control: o arquivo de controle, como ``read_snapshots`` o devolve; ``keep_versions``
        sai das versões da tabela em ``control["snapshots"]``.
    :param table_name: o nome da tabela nas entradas do controle.
    :param storage: o armazenamento da raiz do banco.
    :param retention_hours: a retenção em horas, aceita abaixo da
        ``delta.deletedFileRetentionDuration`` da tabela; o padrão são 400 dias. Dentro da
        retenção nada é listado, mesmo com versões intermediárias: é a janela em que toda versão
        continua legível.
    :param apply: ``True`` apaga os arquivos; o padrão só os lista.
    :param full: ``full=True`` inclui os arquivos órfãos.
    :return: os arquivos listados, ou os apagados com ``apply=True``, relativos à pasta da tabela.
    """
    kept = set()
    for versions in control.get("snapshots", {}).values():
        if table_name in versions:
            kept.add(versions[table_name])
    dt = open_table(uri, storage)
    return dt.vacuum(
        retention_hours=retention_hours,
        dry_run=not apply,
        enforce_retention_duration=False,
        keep_versions=sorted(kept) or None,
        full=full,
    )


# ---------------------------------------------------------------- a operação


def compact(uri: str, table: sa.Table, partitions: list[str], storage: Storage) -> dict:
    """Junta os arquivos pequenos das partições pelo ``optimize.compact`` do delta-rs; uma
    partição com um arquivo só não commita.

    A reescrita sai pelo escritor do delta-rs: os arquivos de outro escritor que ela junta perdem o
    ``INT96`` e o ``FIXED_LEN_BYTE_ARRAY`` e ganham estatística em toda coluna. O commit grava
    ``dataChange`` falso, e ``version_diff`` não o conta.

    Exemplo:

    .. code-block:: python

        compact(uri, Operacao.__table__, ["2026-08-31"], storage)["numFilesRemoved"]

    :param uri: a URI da pasta da tabela, sob a raiz do banco.
    :param table: a tabela do modelo.
    :param partitions: os valores das partições, na regra de ``schema.PARTITION_VALUE``; numa
        tabela sem partição, ignorados, e a tabela inteira é compactada.
    :param storage: o armazenamento da raiz do banco.
    :return: as métricas do ``optimize.compact``, como ``numFilesAdded`` e ``numFilesRemoved``.
    :raises ContractError: um valor de ``partitions`` fora da regra da partição.
    """
    partition_by = table_options(table).partition_by
    filters = None
    if partition_by is not None:
        for value in partitions:
            check_partition_value(value)
        filters = [(partition_by, "in", list(partitions))]
    return open_table(uri, storage).optimize.compact(partition_filters=filters)


# Os metadados da biblioteca que history lê de cada commit.
_METADATA_KEYS = ("serialize_db_execution_id", "serialize_db_input_versions",
                  "serialize_db_snapshot")


def history(uri: str, storage: Storage) -> list[dict]:
    """Os commits da tabela, do mais recente ao mais antigo.

    Exemplo:

    .. code-block:: python

        history(uri, storage)[0]
        # {"version": 143, "operation": "WRITE", "timestamp": datetime(..., tzinfo=UTC),
        #  "serialize_db_execution_id": "exec-2026-08-31", "serialize_db_input_versions": "{}"}

    :param uri: a URI da pasta da tabela, sob a raiz do banco.
    :param storage: o armazenamento da raiz do banco.
    :return: um dicionário por commit, com ``version``, ``operation``, ``timestamp`` (o instante,
        em UTC) e os metadados da biblioteca que o commit tem (``serialize_db_execution_id``,
        ``serialize_db_input_versions``, ``serialize_db_snapshot``), que só os commits das
        partições de uma execução levam; os de ``create_table``, ``reconcile``, ``rewrite`` e
        ``deep_copy``, o ``restore`` de ``read_back``, os de ``vacuum`` e os de ``OPTIMIZE`` vêm
        sem eles.
    """
    commits = []
    for entry in open_table(uri, storage).history():
        commits.append(_commit_record(entry))
    return commits


def _commit_record(entry: Mapping[str, object]) -> dict:
    """Um commit do ``history`` do delta-rs: a versão, a operação, o instante em UTC e os
    metadados da biblioteca que ele tem."""
    instant = datetime.datetime.fromtimestamp(entry["timestamp"] / 1000, datetime.timezone.utc)
    record = {"version": entry["version"], "operation": entry["operation"], "timestamp": instant}
    for key in _METADATA_KEYS:
        if key in entry:
            record[key] = entry[key]
    return record


def _present(values: Mapping[str, object] | None) -> dict[str, object]:
    """As entradas com valor de um dicionário de estatísticas da ação, ou vazio."""
    return {name: value for name, value in (values or {}).items() if value is not None}


def _copied_file(action: Mapping[str, object]) -> RegisteredFile:
    """O arquivo de uma ação ``add`` da origem, como ``register_files`` o descreve: o caminho
    relativo, o tamanho, as linhas e as estatísticas da própria ação."""
    return RegisteredFile(
        path=str(action["path"]),
        size=int(action["size_bytes"]),
        rows=int(action["num_records"]),
        stats={"min": _present(action.get("min")), "max": _present(action.get("max")),
               "null_count": _present(action.get("null_count"))},
    )


def _count_rows(uri: str, storage: Storage) -> tuple[int, int]:
    """As linhas da tabela pelos dois leitores: o dataset do delta-rs e o ``delta_scan``."""
    by_delta = open_table(uri, storage).to_pyarrow_dataset().count_rows()
    connection = storage.duckdb_connect()
    try:
        row = connection.execute(f"SELECT count(*) FROM delta_scan({literal(uri)})").fetchone()
    finally:
        connection.close()
    return by_delta, int(row[0])


def _actions_by_partition(dt: DeltaTable, partition_by: str | None) -> dict[str | None, list]:
    """As ações ``add`` da versão, com os structs de estatísticas, agrupadas pelo valor da
    partição; uma tabela sem partição fica toda sob ``None``."""
    by_partition: dict[str | None, list] = {}
    for action in pa.table(dt.get_add_actions(flatten=False)).to_pylist():
        value = None
        if partition_by is not None:
            value = (action.get("partition") or {}).get(partition_by)
        by_partition.setdefault(value, []).append(action)
    return by_partition


def _copy_destination(destination: str, source: DeltaTable, storage: Storage) -> set[str]:
    """A tabela do destino da cópia e os caminhos que ela já registra: criada com o esquema, a
    partição, o nome, a descrição e as propriedades da origem quando não existe; aberta quando
    existe, para continuar uma cópia interrompida, e recusada quando registra um arquivo que a
    versão de origem não lista, porque guarda outra tabela."""
    metadata = source.metadata()
    if not table_exists(destination, storage):
        DeltaTable.create(
            destination,
            source.schema(),
            mode="error",
            partition_by=metadata.partition_columns or None,
            name=metadata.name,
            description=metadata.description,
            configuration=metadata.configuration,
            storage_options=_options(storage),
        )
        return set()
    existing = open_table(destination, storage)
    registered = set(pa.table(existing.get_add_actions(flatten=True)).column("path").to_pylist())
    listed = set(pa.table(source.get_add_actions(flatten=True)).column("path").to_pylist())
    foreign = sorted(registered - listed)
    if foreign:
        raise RegistrationRefused(f"{destination}: o destino registra {len(foreign)} arquivo(s) "
                                  f"fora da versão {source.version()} da origem: {foreign[0]}")
    return registered


def deep_copy(uri: str, version: int, destination: str, storage: Storage) -> int:
    """Uma tabela nova em ``destination`` com os arquivos, o esquema, a partição, o nome, a
    descrição e as propriedades de uma versão, pela cópia dos arquivos de cada partição e o
    registro deles.

    Cada arquivo que o log da versão lista é copiado por ``Storage.copy`` para o mesmo caminho
    relativo, sem os dados passarem pela máquina no S3, e entra no log novo com o tamanho, as linhas
    e as estatísticas da ação de origem, as dos tipos exatos, num commit ``overwrite`` por partição,
    como ``register_files``; no fim, a contagem da cópia pelos dois leitores é conferida contra a
    soma das ações. A memória é a do log. Cada partição copiada vai ao log com o número de
    arquivos e o tempo da cópia.

    A repetição continua uma cópia interrompida: com tabela em ``destination``, a partição cujos
    arquivos ela já registra é pulada, sem commit, e as outras são copiadas.

    Exemplo:

    .. code-block:: python

        deep_copy(uri, 143, storage.uri_of("prd/arquivo/2026T3/cad_lancamentos"), storage)   # 4

    :param uri: a URI da pasta da tabela de origem, sob a raiz do banco.
    :param version: a versão copiada.
    :param destination: a URI da pasta da cópia, sob a raiz do banco.
    :param storage: o armazenamento da raiz do banco.
    :return: a versão da cópia, que ganha uma versão por partição copiada; a repetição sobre a
        cópia completa devolve a mesma versão.
    :raises RegistrationRefused: um destino que registra um arquivo que a versão não lista, que
        guarda outra tabela; ou, no fim, a contagem da cópia pelos dois leitores diferente da
        soma das ações.
    :raises ValueError: ``uri`` ou ``destination`` fora da raiz de ``storage``.
    :raises ExecutionConflict: o commit de uma partição falhou no delta-rs com
        ``CommitFailedError``.
    """
    source = open_table(uri, storage, version)
    metadata = source.metadata()
    partition_columns = metadata.partition_columns
    partition_by = partition_columns[0] if partition_columns else None
    contract = pa.schema(source.schema())
    registered = _copy_destination(destination, source, storage)
    source_path = storage.relative(uri)
    target_path = storage.relative(destination)
    total = 0
    for value, group in _actions_by_partition(source, partition_by).items():
        # A soma conta toda partição, a pulada inclusive: a contagem final lê a cópia inteira.
        total += sum(int(action["num_records"]) for action in group)
        label = "tabela inteira" if value is None else f"partição {value}"
        if all(action["path"] in registered for action in group):
            log.info("%s: %s já no destino", metadata.name, label)
            continue
        started = time.perf_counter()
        actions = []
        for action in group:
            storage.copy(storage.join(source_path, action["path"]),
                         storage.join(target_path, action["path"]))
            actions.append(_add_action(_copied_file(action), contract, partition_by, value, ()))
        destination_table = open_table(destination, storage)
        _commit_actions(destination_table, str(metadata.name), partition_by, actions, value=value,
                        metadata={})
        log.info("%s: %s copiada, %d arquivo(s) em %.1f s", metadata.name, label, len(actions),
                 time.perf_counter() - started)
    by_delta, by_duckdb = _count_rows(destination, storage)
    if by_delta != total or by_duckdb != total:
        raise RegistrationRefused(f"{destination}: a cópia tem {by_delta} linhas pelo delta-rs e "
                                  f"{by_duckdb} pelo DuckDB, esperadas {total}")
    return open_table(destination, storage).version()


def _export_by_copy(dt: DeltaTable, uri: str, destination: str, storage: Storage) -> list[str]:
    """Os arquivos que o log lista, copiados sem ler dados, no mesmo layout
    ``<coluna>=<valor>/``."""
    source_path = storage.relative(uri)
    target_path = storage.relative(destination)
    copied = []
    for path in sorted(pa.table(dt.get_add_actions(flatten=True)).column("path").to_pylist()):
        storage.copy(storage.join(source_path, path), storage.join(target_path, path))
        copied.append(storage.uri_of(storage.join(target_path, path)))
    return copied


def _export_by_rewrite(dt: DeltaTable, uri: str, table: sa.Table, destination: str,
                       storage: Storage) -> list[str]:
    """A versão reescrita pelo ``COPY`` particionado do DuckDB: um arquivo por partição, sem a
    coluna de partição dentro dele."""
    partition_by = table_options(table).partition_by
    select = f"SELECT * FROM delta_scan({literal(uri)}, version := {dt.version()})"
    if partition_by is None:
        storage.ensure_folder(storage.relative(destination))
        target = f"{destination}/data.parquet"
        options = "FORMAT parquet, RETURN_STATS"
    else:
        target = destination
        options = f"FORMAT parquet, PARTITION_BY ({quoted(partition_by)}), RETURN_STATS"
    connection = storage.duckdb_connect()
    try:
        rows = connection.execute(f"COPY ({select}) TO {literal(target)} ({options})").fetchall()
    finally:
        connection.close()
    return sorted(str(row[0]) for row in rows)


def export_snapshot(uri: str, table: sa.Table, destination: str, storage: Storage,
                    version: int | None = None,
                    mode: Literal["copy", "rewrite"] = "copy") -> list[str]:
    """Exporta uma versão da tabela como arquivos Parquet, sem o log, nas pastas
    ``<coluna>=<valor>/`` numa tabela particionada.

    Exemplo:

    .. code-block:: python

        export_snapshot(uri, Operacao.__table__, storage.uri_of("prd/exportacao/2026T3"), storage,
                        version=143)

    :param uri: a URI da pasta da tabela, sob a raiz do banco.
    :param table: a tabela do modelo, lida só no modo ``rewrite``.
    :param destination: a URI da pasta da exportação, sob a raiz do banco.
    :param storage: o armazenamento da raiz do banco.
    :param version: a versão exportada; ``version=None`` é a atual.
    :param mode: ``copy`` copia os arquivos que o log lista, sem ler dados (no S3, o
        ``CopyObject`` ou o ``UploadPartCopy`` de ``Storage.copy``); cada arquivo guarda o esquema
        da sua escrita. ``rewrite`` reescreve pelo ``COPY`` particionado do DuckDB, com o esquema
        da versão em todos.
    :return: as URIs dos arquivos gravados, em ordem.
    :raises ValueError: ``uri`` ou ``destination`` fora da raiz de ``storage``, nos dois modos,
        antes de qualquer escrita.
    """
    # Os dois caminhos conferidos antes de gravar: o COPY particionado do DuckDB grava onde
    # recebe, fora da raiz também.
    storage.relative(uri)
    storage.relative(destination)
    dt = open_table(uri, storage, version)
    if mode == "copy":
        return _export_by_copy(dt, uri, destination, storage)
    return _export_by_rewrite(dt, uri, table, destination, storage)
