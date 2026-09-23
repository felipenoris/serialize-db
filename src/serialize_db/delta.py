"""A camada de tabela: as tabelas Delta do banco, gravadas e lidas pelo delta-rs.

Cada primitiva recebe ``uri``, a pasta da tabela sob a raiz do banco, e o ``Storage`` da raiz,
que dá as opções do delta-rs a cada chamada e o sistema de arquivos que lê os rodapés e o log. A
coluna de partição sai de ``table_options(table)``, e ``value`` é o valor de uma partição, ``None``
numa tabela sem partição; o valor segue ``schema.PARTITION_VALUE``, porque entra no predicado como
literal e vira nome de pasta.

O ciclo de uma tabela: ``create_table`` a cria do modelo, ``reconcile`` aplica o diff aditivo do
modelo e recusa o destrutivo, que só ``rewrite`` resolve, num commit. Uma partição entra por um de
dois caminhos, escolhidos pelo ``export_mode`` da execução: ``publish_partition`` grava os dados
pelo escritor do delta-rs, que confere tudo e paga a memória; ``register_files`` registra no log os
arquivos que outro escritor gravou na pasta da tabela, depois das conferências do rodapé de cada
arquivo e com a releitura pelos dois leitores depois do commit. ``version_diff`` lê no log as
partições alteradas entre duas versões, ``copy_manifest`` monta o manifesto do ``COPY`` do Redshift,
e ``snapshot`` marca no arquivo de controle do ambiente as versões de um snapshot do banco, que
``vacuum_keeping_snapshots`` preserva. ``compact``, ``deep_copy`` e ``export_snapshot`` são a
operação.

Exemplo, numa pasta local:

.. code-block:: python

    import pyarrow as pa

    from serialize_db import delta, schema
    from serialize_db.storage import Storage

    storage = Storage.for_uri("/dados/delta")
    uri = storage.uri_of("prod/cad_operacoes")
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
import math
import time
import uuid
from collections.abc import Callable, Collection, Mapping
from typing import Literal

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

__all__ = [
    "RegisteredFile",
    "SchemaDiff",
    "commit_metadata",
    "compact",
    "copy_manifest",
    "create_table",
    "deep_copy",
    "export_snapshot",
    "max_key",
    "open_table",
    "publish_partition",
    "read_back",
    "read_snapshots",
    "reconcile",
    "register_files",
    "rewrite",
    "schema_diff",
    "snapshot",
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


def _checked_value(table: sa.Table, value: str | None) -> str | None:
    """O valor de partição conferido: obrigatório e na regra da partição numa tabela particionada,
    ``None`` numa tabela sem partição."""
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
    Redshift não lê. A chamada repetida devolve a tabela como está.

    Exemplo:

    .. code-block:: python

        create_table(uri, Operacao.__table__, storage).version()   # 0
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
    """A tabela numa versão, a última sem ``version``; a execução abre cada tabela uma vez e guarda
    o objeto e a versão.

    Exemplo:

    .. code-block:: python

        open_table(uri, storage, version=3).version()   # 3
    """
    return DeltaTable(uri, version=version, storage_options=_options(storage))


def table_exists(uri: str, storage: Storage) -> bool:
    """Se há uma tabela Delta na pasta: a pasta sem ``_delta_log/`` não é tabela."""
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
    falsa; 0 na tabela vazia.

    Exemplo:

    .. code-block:: python

        max_key(open_table(uri, storage), "id_operacao")   # 1000
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

    ``serialize_db_execution_id``, ``serialize_db_input_versions`` (o JSON das versões lidas, com as
    chaves ordenadas) e, só na execução marcada, ``serialize_db_snapshot``. Eles aparecem no
    ``history`` da tabela.

    Exemplo:

    .. code-block:: python

        commit_metadata("exec-2026-09-05", {"cad_contratos": 88})
        # {"serialize_db_execution_id": "exec-2026-09-05",
        #  "serialize_db_input_versions": '{"cad_contratos": 88}'}
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
    """Substitui a partição pelos dados num commit, pelo escritor do delta-rs, e devolve a versão
    do próprio commit.

    ``data`` é uma ``pa.Table``, um lote, um ``RecordBatchReader`` ou um objeto com
    ``__arrow_c_stream__``, já passado por ``cast`` e com a coluna de partição; sem ela é
    ``ContractError``, antes de gravar. ``write_deltalake(mode="overwrite", predicate=...)`` sobre o
    objeto ``DeltaTable``, que depois da escrita está na versão do próprio commit mesmo com o commit
    de outro escritor no meio. ``value=None`` numa tabela sem partição substitui a tabela. As
    colunas de ``columns_without_min_max``, as ``Double`` com valor não finito na partição, saem sem
    mínimo e máximo no rodapé e no log (issue #59). ``CommitFailedError`` sobe como
    ``ExecutionConflict``.

    Exemplo:

    .. code-block:: python

        version = publish_partition(uri, Operacao.__table__, "2026-08-31", data,
                                    commit_metadata("exec-42", {}), storage,
                                    columns_without_min_max=["valor"])
    """
    value = _checked_value(table, value)
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

    Inteiro, data, ``Double`` e texto transcrevem exato. ``decimal`` e ``timestamp`` ficam de fora:
    o log guarda o mínimo e o máximo como número JSON, e um máximo abaixo do valor real poda o
    arquivo que tem a linha, sem erro, nos dois leitores.
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
    for quoted_name, values in row["column_statistics"].items():
        name = quoted_name.strip('"')
        if name not in contract.names:
            continue
        nulls[name] = int(values["null_count"])
        convert = _stat_converter(contract.field(name).type)
        if convert is None or "min" not in values or "max" not in values:
            continue
        minimum[name] = convert(values["min"])
        maximum[name] = convert(values["max"])
    return RegisteredFile(
        path=_relative_file(str(row["filename"]), uri),
        size=int(row["file_size_bytes"]),
        rows=int(row["count"]),
        stats={"min": minimum, "max": maximum, "null_count": nulls},
    )


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


def _commit_actions(dt: DeltaTable, table: sa.Table, actions: list[AddAction],
                    value: str | None, metadata: Mapping[str, str],
                    schema: object | None = None) -> None:
    """Um commit ``overwrite`` das ações: da partição de ``value``, ou da tabela inteira sem ele.

    ``CommitFailedError``, o de outro registro da mesma partição a partir da mesma versão, sobe
    como ``ExecutionConflict``.
    """
    partition_by = table_options(table).partition_by
    filters = None
    if partition_by and value is not None:
        filters = [(partition_by, "=", value)]
    try:
        dt.create_write_transaction(
            actions,
            mode="overwrite",
            schema=schema if schema is not None else dt.schema(),
            partition_by=[partition_by] if partition_by else None,
            partition_filters=filters,
            commit_properties=CommitProperties(custom_metadata=dict(metadata)),
        )
    except CommitFailedError as error:
        raise ExecutionConflict(f"{table.name} partição {value}: {error}") from None


def register_files(uri: str, table: sa.Table, files: list[RegisteredFile], value: str | None,
                   metadata: Mapping[str, str], storage: Storage, expected_rows: int | None = None,
                   columns_without_min_max: Collection[str] = ()) -> int:
    """Registra no log, num commit ``overwrite`` da partição, arquivos que outro escritor gravou
    dentro da pasta da tabela, e devolve a versão do commit.

    ``create_write_transaction`` grava a ação como a recebe, e os leitores obedecem à ação, não ao
    arquivo; por isso cada arquivo passa antes pelas conferências do rodapé, um GET por arquivo: o
    arquivo existe com o tamanho declarado; o esquema do rodapé tem cada coluna do contrato, na
    ordem dele, num tipo físico que os leitores leem como o lógico, e não tem a coluna de partição;
    as colunas ``NOT NULL`` não têm nulo na contagem do rodapé; o caminho está na pasta da
    partição; as linhas do rodapé são as declaradas, e a soma é ``expected_rows`` quando o chamador
    tem a contagem da fonte. A reprovação é ``RegistrationRefused``, sem commit, e o arquivo fica
    órfão até ``vacuum(full=True)``.

    A ação leva ``numRecords``, o ``nullCount`` e o mínimo e o máximo das colunas inteiras, de data,
    ``Double`` e texto, menos as de ``columns_without_min_max``, as ``Double`` com valor não finito
    na partição (issue #59). Depois do commit, ``read_back`` relê a versão pelos dois leitores e a
    desfaz na diferença. Um segundo registro da mesma partição a partir da mesma versão é
    ``ExecutionConflict``.

    Exemplo:

    .. code-block:: python

        file = RegisteredFile("data_str=2026-08-31/exec-42_ab12.parquet", 4096, 1000, stats)
        register_files(uri, Operacao.__table__, [file], "2026-08-31",
                       commit_metadata("exec-42", {}), storage, expected_rows=1000)
    """
    value = _checked_value(table, value)
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
    _commit_actions(open_table(uri, storage), table, actions, value, metadata)
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
    return _Reading(row[0], tuple(row[1::2]), tuple(row[2::2]))


def _arrow_reading(dt: DeltaTable, table: sa.Table, value: str | None,
                   keys: tuple[str, ...]) -> _Reading:
    """A leitura pelo dataset Arrow do delta-rs, lote a lote, com a memória de um lote."""
    partition_by = table_options(table).partition_by
    condition = None
    if partition_by is not None and value is not None:
        condition = ds.field(partition_by) == value
    scanner = dt.to_pyarrow_dataset().scanner(columns=list(keys), filter=condition)
    rows = 0
    lows: list[list[object]] = [[] for _ in keys]
    highs: list[list[object]] = [[] for _ in keys]
    for batch in scanner.to_batches():
        rows += batch.num_rows
        for index, name in enumerate(keys):
            extremes = pc.min_max(batch.column(name))
            if extremes["min"].is_valid:
                lows[index].append(extremes["min"].as_py())
                highs[index].append(extremes["max"].as_py())
    minimum = tuple(min(values) if values else None for values in lows)
    maximum = tuple(max(values) if values else None for values in highs)
    return _Reading(rows, minimum, maximum)


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
                        log: _Reading, arrow: _Reading, engine: _Reading) -> list[str]:
    """As diferenças entre os leitores, o log e a contagem esperada.

    As linhas iguais nos quatro; o menor e o maior valor de cada coluna da chave iguais nos dois
    leitores; e o mínimo e o máximo registrados no log, das colunas de tipo exato, como limites dos
    lidos: um máximo abaixo do lido podaria o arquivo que tem a linha.
    """
    problems = []
    counts = {"esperadas": expected_rows, "log": log.rows, "delta-rs": arrow.rows,
              "delta_scan": engine.rows}
    if len(set(counts.values())) > 1:
        problems.append(f"linhas {counts}")
    if (arrow.minimum, arrow.maximum) != (engine.minimum, engine.maximum):
        problems.append(f"chave {keys}: delta-rs {arrow.minimum}..{arrow.maximum}, "
                        f"delta_scan {engine.minimum}..{engine.maximum}")
    contract = arrow_schema(table)
    for index, name in enumerate(keys):
        low, high = log.minimum[index], log.maximum[index]
        read_low, read_high = arrow.minimum[index], arrow.maximum[index]
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
    linhas e leem o menor e o maior valor de cada coluna da primeira chave do modelo na partição
    (na tabela inteira com ``value=None``). A releitura pega o que as conferências do rodapé não
    veem, um leitor que não lê o arquivo como o outro (o ``parquet.field.id`` no esquema Delta fez o
    ``delta_scan`` ler toda coluna como nula com a contagem certa), e uma estatística do log que
    podaria o arquivo certo. Uma diferença chama ``restore(version - 1)`` e levanta
    ``RegistrationRefused`` com as leituras.
    """
    dt = open_table(uri, storage)
    version = dt.version()
    keys = table_options(table).keys[0]
    log = _log_reading(dt, table, value, keys)
    arrow = _arrow_reading(dt, table, value, keys)
    engine = _duckdb_reading(uri, version, table, value, keys, storage)
    problems = _read_back_problems(table, keys, expected_rows, log, arrow, engine)
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
    """Aplica o diff aditivo entre o modelo e a tabela e devolve o diff; o destrutivo é
    ``SchemaDiffRefused``, sem alteração, com a lista e a instrução de ``rewrite``.

    Cada parte é um commit só de metadados: ``add_columns`` com o campo do esquema Delta do
    contrato, comentário incluído; ``drop_column_not_null``; ``add_constraint``;
    ``set_table_description``; ``set_column_metadata`` com o comentário. A versão anterior continua
    legível com o esquema antigo, e a segunda chamada não commita.

    Exemplo:

    .. code-block:: python

        reconcile(uri, Operacao.__table__, storage).add   # os campos acrescentados
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


def _nonfinite_by_partition(connection: object, table: sa.Table,
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
    for row in rows:
        found[row[0]] = tuple(name for name, count in zip(doubles, row[1:]) if count)
    return found


def _copy_rewrite(connection: object, uri: str, table: sa.Table, select: str) -> list[dict]:
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


def rewrite(uri: str, table: sa.Table, storage: Storage,
            expressions: Mapping[str, str] | None = None) -> int:
    """Reescreve a tabela inteira com o esquema do contrato num único commit, sem predicado, com
    memória constante; devolve a versão do commit.

    O ``COPY ... PARTITION_BY ... RETURN_STATS`` do DuckDB lê a versão atual por ``delta_scan`` e
    grava arquivos novos na pasta da tabela, que entram num ``create_write_transaction(mode=
    "overwrite", schema=delta_schema(table))``, depois das conferências de ``register_files``; a
    versão anterior continua legível com o esquema antigo, e ``read_back`` roda depois do commit.
    ``expressions`` dá, por coluna do contrato, a expressão SQL do DuckDB sobre a versão atual que a
    preenche: o nome antigo numa renomeação, o valor de uma coluna ``NOT NULL`` nova. Cada coluna
    sai em ``CAST`` para o tipo do contrato, e a remoção é a coluna que o contrato não tem mais.
    Uma coluna do contrato ausente da versão atual e fora de ``expressions`` falha no ``COPY``,
    antes de qualquer commit. As colunas ``Double`` com valor não finito em cada partição ficam sem
    mínimo e máximo no log. ``CommitFailedError`` sobe como ``ExecutionConflict``.

    Exemplo:

    .. code-block:: python

        rewrite(uri, Operacao.__table__, storage, expressions={"valor": '"valor_bruto"'})
    """
    expressions = dict(expressions or {})
    _check_expressions(table, expressions)
    partition_by = table_options(table).partition_by
    dt = open_table(uri, storage)
    source = f"delta_scan({literal(uri)}, version := {dt.version()})"
    select = _rewrite_select(table, expressions, source)
    connection = storage.duckdb_connect()
    try:
        nonfinite = _nonfinite_by_partition(connection, table, select)
        written = _copy_rewrite(connection, uri, table, select)
    finally:
        connection.close()
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
    _commit_actions(dt, table, actions, None, {}, schema=delta_schema(table))
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
    não conta. Numa tabela sem partição, ``{None}`` quando houve alteração. Um arquivo do log
    ausente é ``LogUnavailable``, com a instrução de publicar a tabela inteira: a limpeza que o
    apagaria também torna ilegível a versão publicada.

    Exemplo:

    .. code-block:: python

        version_diff(uri, 57, 58, Operacao.__table__, storage)   # {"2026-08-31"}
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
    partições pedidas (todas com ``None``) e devolve a URI dele.

    Cada entrada é ``{"url": "<pasta da tabela>/<path>", "mandatory": true, "meta":
    {"content_length": <size_bytes>}}``, das ações ``add`` da versão; ``destination`` é a URI do
    manifesto sob a raiz, em ``publicacao/`` ou ``staging/``.

    Exemplo:

    .. code-block:: python

        copy_manifest(uri, 58, ["2026-08-31"],
                      storage.uri_of("prod/publicacao/exec-42/cad_operacoes/2026-08-31.manifest"),
                      storage)
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
    storage.write_text(storage.relative(destination), json.dumps({"entries": entries}, indent=2))
    return destination


# ---------------------------------------------------------------- os snapshots do banco


def read_snapshots(storage: Storage, environment: str) -> tuple[dict, str | None]:
    """O arquivo de controle dos snapshots do ambiente e a impressão digital dele, para a escrita
    condicional seguinte; ``({"snapshots": {}}, None)`` quando ele ainda não existe.

    Exemplo:

    .. code-block:: python

        control, fingerprint = read_snapshots(storage, "prod")
        control["snapshots"]   # {"2026T3": {"cad_lancamentos": 143, ...}}
    """
    path = storage.join(environment, CONTROL_FILE)
    try:
        text, fingerprint = storage.read_text(path)
    except FileNotFoundError:
        return {"snapshots": {}}, None
    return json.loads(text), fingerprint


def snapshot(storage: Storage, environment: str, name: str, versions: Mapping[str, int]) -> dict:
    """Grava no arquivo de controle do ambiente a entrada ``{name: versions}`` de um snapshot do
    banco e devolve o controle novo.

    Um nome repetido é ``ValueError``. A escrita é condicional: ``if_match`` com a impressão da
    leitura, ou ``if_none_match`` no primeiro snapshot, e outro escritor entre a leitura e a escrita
    faz subir ``ConflictError``. As versões marcadas são as que ``vacuum_keeping_snapshots``
    preserva.

    Exemplo:

    .. code-block:: python

        snapshot(storage, "prod", "2026T3", {"cad_lancamentos": 143, "cad_contratos": 88})
    """
    control, fingerprint = read_snapshots(storage, environment)
    if name in control["snapshots"]:
        raise ValueError(f"{environment}: o snapshot {name} já existe")
    ordered = {}
    for table_name in sorted(versions):
        ordered[table_name] = versions[table_name]
    control["snapshots"][name] = ordered
    text = json.dumps(control, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    path = storage.join(environment, CONTROL_FILE)
    storage.write_text(path, text, if_match=fingerprint, if_none_match=fingerprint is None)
    return control


def vacuum_keeping_snapshots(uri: str, control: Mapping, table_name: str, storage: Storage,
                             retention_hours: int = 9600, apply: bool = False,
                             full: bool = False) -> list[str]:
    """O ``vacuum`` da tabela que preserva os arquivos das versões dos snapshots do banco; lista
    por padrão e apaga com ``apply=True``.

    ``keep_versions`` sai das versões da tabela em ``control["snapshots"]``; ``full=True`` inclui
    os arquivos órfãos. Dentro da retenção, 400 dias por padrão, nada é listado, mesmo com versões
    intermediárias: é a janela em que toda versão continua legível.

    Exemplo:

    .. code-block:: python

        control, _ = read_snapshots(storage, "prod")
        vacuum_keeping_snapshots(uri, control, "cad_lancamentos", storage)   # a lista, sem apagar
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
    """Junta os arquivos pequenos das partições pelo ``optimize.compact`` do delta-rs e devolve as
    métricas; uma partição com um arquivo só não commita.

    A reescrita sai pelo escritor do delta-rs: os arquivos de outro escritor que ela junta perdem o
    ``INT96`` e o ``FIXED_LEN_BYTE_ARRAY`` e ganham estatística em toda coluna. O commit grava
    ``dataChange`` falso, e ``version_diff`` não o conta.

    Exemplo:

    .. code-block:: python

        compact(uri, Operacao.__table__, ["2026-08-31"], storage)["numFilesRemoved"]
    """
    partition_by = table_options(table).partition_by
    filters = None
    if partition_by is not None:
        for value in partitions:
            check_partition_value(value)
        filters = [(partition_by, "in", list(partitions))]
    return open_table(uri, storage).optimize.compact(partition_filters=filters)


def deep_copy(uri: str, version: int, destination: str, storage: Storage) -> int:
    """Uma tabela nova em ``destination``, na versão 0, com os dados, o esquema, a partição, o nome,
    a descrição e as propriedades de uma versão; devolve a versão da cópia.

    A leitura vai em lotes pelo dataset Arrow, nunca por ``to_pyarrow_table``, que deixa uma tarefa
    do Acero em voo num programa que termina logo depois. Um ``destination`` que já tem tabela é
    erro.

    Exemplo:

    .. code-block:: python

        deep_copy(uri, 143, storage.uri_of("prod/arquivo/2026T3/cad_lancamentos"), storage)   # 0
    """
    source = open_table(uri, storage, version)
    metadata = source.metadata()
    reader = source.to_pyarrow_dataset().scanner().to_reader()
    write_deltalake(
        destination,
        reader,
        mode="error",
        partition_by=metadata.partition_columns or None,
        name=metadata.name,
        description=metadata.description,
        configuration=metadata.configuration,
        storage_options=_options(storage),
    )
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
    source = f"SELECT * FROM delta_scan({literal(uri)}, version := {dt.version()})"
    if partition_by is None:
        storage.ensure_folder(storage.relative(destination))
        target = f"{destination}/data.parquet"
        options = "FORMAT parquet, RETURN_STATS"
    else:
        target = destination
        options = f"FORMAT parquet, PARTITION_BY ({quoted(partition_by)}), RETURN_STATS"
    connection = storage.duckdb_connect()
    try:
        rows = connection.execute(f"COPY ({source}) TO {literal(target)} ({options})").fetchall()
    finally:
        connection.close()
    return sorted(str(row[0]) for row in rows)


def export_snapshot(uri: str, table: sa.Table, destination: str, storage: Storage,
                    version: int | None = None,
                    mode: Literal["copy", "rewrite"] = "copy") -> list[str]:
    """Exporta uma versão da tabela como pastas ``<coluna>=<valor>/`` de Parquet, sem o log, e
    devolve as URIs dos arquivos gravados.

    ``copy`` copia os arquivos que o log lista, sem ler dados (o ``CopyObject`` no S3); cada arquivo
    guarda o esquema da sua escrita. ``rewrite`` reescreve pelo ``COPY`` particionado do DuckDB, com
    o esquema da versão em todos. ``version=None`` é a atual.

    Exemplo:

    .. code-block:: python

        export_snapshot(uri, Operacao.__table__, storage.uri_of("prod/exportacao/2026T3"), storage,
                        version=143)
    """
    dt = open_table(uri, storage, version)
    if mode == "copy":
        return _export_by_copy(dt, uri, destination, storage)
    return _export_by_rewrite(dt, uri, table, destination, storage)
