"""A base Parquet de origem ``db_projetado`` fabricada com a estrutura que o probe leu nas duas bases reais.

``probes/parquet_source.py`` leu a base de desenvolvimento em 2026-09-20 e a de produção em
2026-09-21 (as duas com 14 pastas de tabela, 205 arquivos, 3,76 GB e 187 milhões de linhas, e a
mesma seção 3), e ``write_source`` reproduz o que as leituras fixaram, com poucas linhas por tabela: as 14 tabelas com as mesmas colunas, na mesma ordem, com os mesmos tipos Arrow e
a mesma nulidade declarada; a partição Hive por ``data_str`` (``cad_contratos``, ``cad_operacoes``,
``rel_contrato_operacao``) e por ``data_base_str`` (``cad_lancamentos``), com o valor no caminho e
nunca dentro do arquivo, igual a ``data`` ou ``data_base`` em toda linha da partição, fins de mês
não contíguos; vários arquivos ``chunk_<n>.parquet`` por partição, numerados de 0 sem zeros à
esquerda, o último com o resto das linhas; um row group por arquivo, SNAPPY, sem dicionário, formato
1.0, timestamps em ``INT96`` sem estatísticas, nenhum ``field_id``, e a chave ``pandas`` no rodapé de
parte dos arquivos (em todos na base de desenvolvimento; na de produção, em 5 de 8, 111 de 144, 9 de
13 e 16 de 30 arquivos das tabelas particionadas e em nenhum de ``alembic_version`` e
``meta_update_status``; aqui, fora da última partição de cada tabela particionada e dessas duas
tabelas);
``alembic_version`` e ``meta_update_status`` fora do modelo, e na raiz o ``schema.json`` da
biblioteca anterior, o arquivo real (``source_db_projetado_schema.json``): o controle de esquema
no formato da reflexão do SQLAlchemy, com colunas, nulidade, chaves estrangeiras, índices e
restrições de unicidade de cada tabela.

Os valores são fictícios, determinísticos e consistentes com o modelo de referência: toda chave
estrangeira do modelo tem a linha referenciada, toda chave é única, e as quatro tabelas
particionadas têm as mesmas quatro datas, para que cada ``data_base`` de
``cad_lancamentos`` tenha os seus ``cad_contratos`` (a leitura mostrou 2026-01-31 só em
``cad_lancamentos``). ``rel_contrato_operacao`` é a relação N×N entre contratos e operações da mesma
data: toda operação tem contratos, todo contrato está em uma ou duas operações, e ``fator_rateio``
reparte cada contrato entre as suas operações e soma 1 por contrato (leitura do usuário na base
de produção, 2026-09-21). Os valores reproduzem o que a carga inicial tem de
tratar: ``valor`` com três casas (o par extremo ``±11846195394.628``), ``fator`` com cinco,
``data_assinatura`` nula em mais da metade das linhas, ``meta`` sempre nula, ``id_lancamento`` até
1.113.599.996 em ``int32``, e as sete colunas de ``cad_contratos`` declaradas anuláveis nos arquivos
sem nulo algum. Os valores que diferem entre as duas bases são os da leitura de desenvolvimento: a
de produção tem 113 linhas a menos, ids máximos menores (``id_lancamento`` até 952.517.158) e o
extremo de ``valor`` em ``±11846195394.62801``. O ``timestamp`` tem precisão de microssegundo; a parte sub-microssegundo da origem é
desconhecida, porque o ``INT96`` não tem estatística.

A partição de ``cad_lancamentos`` é por ``data_base``, não por ``data``: ``data`` é o mês projetado,
sempre posterior a ``data_base``, até 2026-12-31.
"""

from __future__ import annotations

import calendar
import datetime as dt
import json
import random
import shutil
from dataclasses import dataclass, field
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

# A origem corta cada partição a cada 1.000.000 de linhas; 20 dá a mesma estrutura de arquivos em poucas linhas.
CHUNK_ROWS = 20
SEED = 20260920
INT96_TIMESTAMP = pa.timestamp("ns")
SCHEMA_CONTROL = Path(__file__).with_name("source_db_projetado_schema.json")


@dataclass(frozen=True)
class Partition:
    """A partição Hive de uma tabela: o nome no caminho, a coluna do arquivo que tem o mesmo valor e os valores."""

    column: str
    source: str
    values: tuple[str, ...]


PARTITION_VALUES = ("2026-01-31", "2026-02-28", "2026-03-31", "2026-06-30")
PARTITIONS: dict[str, Partition] = {
    "cad_contratos": Partition("data_str", "data", PARTITION_VALUES),
    "cad_lancamentos": Partition("data_base_str", "data_base", PARTITION_VALUES),
    "cad_operacoes": Partition("data_str", "data", PARTITION_VALUES),
    "rel_contrato_operacao": Partition("data_str", "data", PARTITION_VALUES),
}

# Linhas por partição. 2026-01-31 de cad_lancamentos passa de dez chunks (chunk_10 e chunk_11 existem, e a ordem
# alfabética os põe antes de chunk_2). rel_contrato_operacao deriva dos contratos: cada contrato numa operação e um em
# quatro também na seguinte, 42 linhas em 2026-02-28, com um chunk_2 de duas linhas. Cada data tem mais contratos que
# operações, para toda operação ter contrato.
ROWS_PER_PARTITION: dict[str, dict[str, int]] = {
    "cad_contratos": {"2026-01-31": 38, "2026-02-28": 33, "2026-03-31": 50, "2026-06-30": 41},
    "cad_lancamentos": {"2026-01-31": 235, "2026-02-28": 60, "2026-03-31": 60, "2026-06-30": 60},
    "cad_operacoes": {"2026-01-31": 28, "2026-02-28": 25, "2026-03-31": 40, "2026-06-30": 30},
}

# O esquema de cada tabela como a leitura o mostrou: nome, tipo Arrow e nulidade, na ordem das colunas.
SCHEMAS: dict[str, pa.Schema] = {
    "alembic_version": pa.schema([pa.field("version_num", pa.string(), nullable=False)]),
    "cad_aliquotas": pa.schema(
        [
            pa.field("id", pa.int32(), nullable=False),
            pa.field("id_conta_origem", pa.int32(), nullable=False),
            pa.field("id_conta_destino", pa.int32(), nullable=False),
            pa.field("data_fim_validade", pa.date32()),
            pa.field("data_inicio_validade", pa.date32(), nullable=False),
            pa.field("fator", pa.float64(), nullable=False),
        ]
    ),
    "cad_contas": pa.schema(
        [
            pa.field("id_conta", pa.int32(), nullable=False),
            pa.field("nome", pa.string(), nullable=False),
            pa.field("numero", pa.string(), nullable=False),
            pa.field("permite_lancamentos", pa.bool_(), nullable=False),
        ]
    ),
    "cad_contratos": pa.schema(
        [
            pa.field("id_contrato", pa.int32(), nullable=False),
            pa.field("data", pa.date32(), nullable=False),
            pa.field("sistema", pa.int32()),
            pa.field("contrato", pa.string(), nullable=False),
            pa.field("legado", pa.bool_(), nullable=False),
            pa.field("um", pa.int32()),
            pa.field("to", pa.string()),
            pa.field("fonte", pa.int32()),
            pa.field("fonte_familia", pa.string()),
            pa.field("estagio", pa.int32()),
            pa.field("taxa_juros_fixos", pa.float64()),
            pa.field("data_assinatura", pa.date32()),
            pa.field("data_primeira_amortizacao", pa.date32()),
            pa.field("data_ultima_amortizacao", pa.date32()),
        ]
    ),
    "cad_lancamentos": pa.schema(
        [
            pa.field("id_lancamento", pa.int32(), nullable=False),
            pa.field("id_veiculo", pa.int32(), nullable=False),
            pa.field("id_conta", pa.int32(), nullable=False),
            pa.field("data", pa.date32(), nullable=False),
            pa.field("valor", pa.float64(), nullable=False),
            pa.field("meta", pa.string()),
            pa.field("timestamp", INT96_TIMESTAMP, nullable=False),
            pa.field("id_mensuracao", pa.int32(), nullable=False),
            pa.field("id_segmento", pa.int32()),
            pa.field("id_negocio", pa.int32()),
            pa.field("data_base", pa.date32(), nullable=False),
            pa.field("sistema", pa.int32()),
            pa.field("contrato", pa.string()),
            pa.field("area", pa.string()),
        ]
    ),
    "cad_operacoes": pa.schema(
        [
            pa.field("id_operacao", pa.int32(), nullable=False),
            pa.field("data", pa.date32(), nullable=False),
            pa.field("operacao", pa.string(), nullable=False),
            pa.field("legado", pa.bool_(), nullable=False),
            pa.field("area", pa.string()),
            pa.field("departamento", pa.string()),
            pa.field("spread_basico", pa.float64()),
            pa.field("spread_risco", pa.float64()),
            pa.field("spread_total", pa.float64()),
            pa.field("taxa_total", pa.float64()),
            pa.field("taxa_bndes", pa.float64()),
            pa.field("custo_adicional", pa.float64()),
            pa.field("instrumento_financeiro", pa.string()),
        ]
    ),
    "dom_hierarquias_contas": pa.schema(
        [
            pa.field("id_hierarquia", pa.int32(), nullable=False),
            pa.field("nome", pa.string(), nullable=False),
            pa.field("descricao", pa.string()),
        ]
    ),
    "dom_mensuracoes": pa.schema([pa.field("id_mensuracao", pa.int32(), nullable=False), pa.field("nome", pa.string(), nullable=False)]),
    "dom_negocios": pa.schema(
        [
            pa.field("id_negocio", pa.int32(), nullable=False),
            pa.field("id_segmento", pa.int32(), nullable=False),
            pa.field("nome", pa.string(), nullable=False),
        ]
    ),
    "dom_segmentos": pa.schema([pa.field("id_segmento", pa.int32(), nullable=False), pa.field("nome", pa.string(), nullable=False)]),
    "dom_veiculos": pa.schema([pa.field("id_veiculo", pa.int32(), nullable=False), pa.field("nome", pa.string(), nullable=False)]),
    "meta_update_status": pa.schema(
        [
            pa.field("id_update_status", pa.int32(), nullable=False),
            pa.field("table_name", pa.string(), nullable=False),
            pa.field("partition", pa.string()),
            pa.field("timestamp", INT96_TIMESTAMP, nullable=False),
        ]
    ),
    "rel_contas_hierarquias": pa.schema(
        [
            pa.field("id_rel_conta_hierarquia", pa.int32(), nullable=False),
            pa.field("id_hierarquia", pa.int32(), nullable=False),
            pa.field("id_parent", pa.int32(), nullable=False),
            pa.field("id_child", pa.int32(), nullable=False),
        ]
    ),
    "rel_contrato_operacao": pa.schema(
        [
            pa.field("id_rel_contrato_operacao", pa.int32(), nullable=False),
            pa.field("data", pa.date32(), nullable=False),
            pa.field("operacao", pa.string(), nullable=False),
            pa.field("sistema", pa.int32(), nullable=False),
            pa.field("contrato", pa.string(), nullable=False),
            pa.field("fator_rateio", pa.float64(), nullable=False),
        ]
    ),
}

# As tabelas da origem que o modelo de referência não tem: a de controle do Alembic e o registro das cargas.
OUTSIDE_MODEL = ("alembic_version", "meta_update_status")
ALEMBIC_REVISION = "131d90070de7"

# As chaves do modelo de referência (tests/reference_model; test_reference_model.py confere a transcrição): a chave
# primária e as restrições de unicidade de cada tabela, e as chaves estrangeiras com a tabela e as colunas referenciadas.
# A base fictícia satisfaz todas.
UNIQUE_KEYS: dict[str, list[list[str]]] = {
    "cad_aliquotas": [["id"], ["id_conta_origem", "id_conta_destino"]],
    "cad_contas": [["id_conta"], ["numero"]],
    "cad_contratos": [["id_contrato"], ["data", "sistema", "contrato"]],
    "cad_lancamentos": [["id_lancamento"]],
    "cad_operacoes": [["id_operacao"], ["data", "operacao"]],
    "dom_hierarquias_contas": [["id_hierarquia"], ["nome"]],
    "dom_mensuracoes": [["id_mensuracao"], ["nome"]],
    "dom_negocios": [["id_negocio"], ["nome"]],
    "dom_segmentos": [["id_segmento"], ["nome"]],
    "dom_veiculos": [["id_veiculo"], ["nome"]],
    "rel_contas_hierarquias": [["id_rel_conta_hierarquia"], ["id_hierarquia", "id_child"]],
    "rel_contrato_operacao": [["id_rel_contrato_operacao"]],
}
FOREIGN_KEYS: list[tuple[str, list[str], str, list[str]]] = [
    ("cad_aliquotas", ["id_conta_origem"], "cad_contas", ["id_conta"]),
    ("cad_aliquotas", ["id_conta_destino"], "cad_contas", ["id_conta"]),
    ("rel_contas_hierarquias", ["id_hierarquia"], "dom_hierarquias_contas", ["id_hierarquia"]),
    ("rel_contas_hierarquias", ["id_parent"], "cad_contas", ["id_conta"]),
    ("rel_contas_hierarquias", ["id_child"], "cad_contas", ["id_conta"]),
    ("dom_negocios", ["id_segmento"], "dom_segmentos", ["id_segmento"]),
    ("cad_lancamentos", ["id_veiculo"], "dom_veiculos", ["id_veiculo"]),
    ("cad_lancamentos", ["id_conta"], "cad_contas", ["id_conta"]),
    ("cad_lancamentos", ["id_mensuracao"], "dom_mensuracoes", ["id_mensuracao"]),
    ("cad_lancamentos", ["id_segmento"], "dom_segmentos", ["id_segmento"]),
    ("cad_lancamentos", ["id_negocio"], "dom_negocios", ["id_negocio"]),
    ("cad_lancamentos", ["data_base", "sistema", "contrato"], "cad_contratos", ["data", "sistema", "contrato"]),
    ("rel_contrato_operacao", ["data", "operacao"], "cad_operacoes", ["data", "operacao"]),
    ("cad_contratos", ["data", "sistema", "contrato"], "rel_contrato_operacao", ["data", "sistema", "contrato"]),
]
# As colunas NOT NULL do modelo que os arquivos declaram anuláveis; o cast do contrato as recusa com nulo.
MODEL_NOT_NULL_DECLARED_NULLABLE: dict[str, list[str]] = {
    "cad_contratos": ["sistema", "um", "to", "fonte", "taxa_juros_fixos", "data_primeira_amortizacao", "data_ultima_amortizacao"],
}

SOURCE_SYSTEMS = (15, 43, 89)
MAX_ENTRY_ID = 1_113_599_996
EXTREME_AMOUNT = 11846195394.628
WRITE_TIMESTAMP = dt.datetime(2026, 3, 18, 16, 53, 22, 296000)
PROJECTION_HORIZON = dt.date(2026, 12, 31)

SEGMENT_NAMES = ("Crédito e Serviços", "Renda Variável", "Tesouraria e ALM", "Corporativo", "Remuneração do Acionista")
BUSINESSES = (
    ("Crédito e Serviços", 1),
    ("Estruturação de Projetos", 1),
    ("Estruturação de Ofertas Públicas", 2),
    ("Renda Variável", 2),
    ("Tesouraria e ALM", 3),
    ("Corporativo", 4),
    ("Remuneração do Acionista", 5),
)
MEASUREMENT_NAMES = ("Realizado", "Projetado", "Orçado")
ACCOUNT_NAMES = (
    "Ajuste Despesas Tributárias",
    "Receitas de Operações de Crédito",
    "Despesas de Captação",
    "Resultado de Participações",
    "Provisão para Risco de Crédito",
    "Receitas de Prestação de Serviços",
    "Despesas Administrativas",
    "Despesas de Pessoal",
    "Resultado com Títulos e Valores Mobiliários",
    "Variação Cambial de Outras Despesas",
    "Juros sobre Capital Próprio",
    "Lucro",
)
OPERATION_AREAS = ("ADIG", "AI", "AC", "GP")
ENTRY_AREAS = ("ADIG", "AI", "GP", "Sem Área")
DEPARTMENTS = ("DCRED2", "DEPRI", "JUCRE")
FINANCIAL_INSTRUMENTS = ("AEROPORTOS DE PRIMEIRO CICLO", "TURISMO, COMÉRCIO E SERVIÇOS")
FUNDING_SOURCE_FAMILIES = ("FAT", "FMM", "FMC", "BND")
FUNDING_SOURCES = (2277, 3964, 2300, 4199, 1112, 8014, 9023)
CURRENCY_UNITS = (185, 604, 202, 777, 19, 145, 143)
TO_CODES = ("01", "ZB", "ZD", "RC", "02", "RI", "05", "ZT")
RATE_ORIGIN_ACCOUNTS = (10, 106, 31, 8, 34, 9, 4, 47, 103, 20, 5, 19, 43)
RATE_DESTINATION_ACCOUNTS = (22, 25, 99, 24, 98, 50, 26, 21)
RATE_FACTORS = (0.66, 0.55, -0.15, 1.0, 0.59895, -0.0465, -1.0, -0.0925, 0.0465)
UPDATE_STATUS_IDS = (1, 2, 5, 6, 7, 49, 50, 51, 52, 53, 127, 128, 129, 130, 151, 152, 178, 179, 180, 181, 182)


@dataclass
class SourceBase:
    """A base gravada: a raiz, os arquivos de cada tabela na ordem de gravação, as linhas por tabela e por partição."""

    root: Path
    files: dict[str, list[Path]] = field(default_factory=dict)
    rows: dict[str, int] = field(default_factory=dict)
    partition_rows: dict[str, dict[str, int]] = field(default_factory=dict)


def month_ends_after(date: dt.date) -> list[dt.date]:
    """Os fins de mês depois de ``date`` até ``PROJECTION_HORIZON``: os meses que um lançamento de ``data_base = date``
    projeta."""
    ends = []
    year, month = date.year, date.month
    while True:
        month += 1
        if month == 13:
            year, month = year + 1, 1
        last_day = calendar.monthrange(year, month)[1]
        month_end = dt.date(year, month, last_day)
        if month_end > PROJECTION_HORIZON:
            return ends
        ends.append(month_end)


def apportionment(count: int) -> list[float]:
    """``count`` fatores de rateio que somam exatamente 1: metade para o primeiro e o resto repartido do mesmo modo.

    As frações são diádicas (1, 1/2, 1/4, ...), exatas em ponto flutuante, e a soma por contrato dá 1.0 sem erro.
    """
    if count == 1:
        return [1.0]
    return [0.5] + [factor / 2 for factor in apportionment(count - 1)]


def as_pandas_wrote(table: pa.Table) -> pa.Table:
    """A tabela como a origem a gravou: pelo pandas, sem índice, com o esquema explícito, e a chave ``pandas`` no rodapé."""
    return pa.Table.from_pandas(table.to_pandas(), schema=table.schema, preserve_index=False)


def written_by_pandas(table: str, value: str | None = None) -> bool:
    """Se o arquivo da tabela, ou da partição ``value``, leva a chave ``pandas`` no rodapé.

    A base de produção tem a chave em parte dos arquivos das tabelas particionadas e em nenhum de ``alembic_version`` e
    ``meta_update_status``; aqui a última partição de cada tabela particionada e essas duas tabelas saem sem ela.
    """
    if table in OUTSIDE_MODEL:
        return False
    return table not in PARTITIONS or value != PARTITIONS[table].values[-1]


def write_chunks(folder: Path, table: pa.Table, chunk_rows: int = CHUNK_ROWS, pandas_key: bool = True) -> list[Path]:
    """Grava ``table`` em ``folder`` como ``chunk_0.parquet``, ``chunk_1.parquet``, ..., ``chunk_rows`` linhas por arquivo."""
    folder.mkdir(parents=True, exist_ok=True)
    paths = []
    for index, offset in enumerate(range(0, table.num_rows, chunk_rows)):
        path = folder / f"chunk_{index}.parquet"
        chunk = table.slice(offset, chunk_rows)
        # Um row group por arquivo, SNAPPY, sem dicionário, formato 1.0 e INT96: o layout físico que a leitura mostrou.
        pq.write_table(
            as_pandas_wrote(chunk) if pandas_key else chunk,
            path,
            version="1.0",
            compression="snappy",
            use_dictionary=False,
            use_deprecated_int96_timestamps=True,
        )
        paths.append(path)
    return paths


# ---------------------------------------------------------------------------------------------------------------
# As tabelas sem partição


def build_cad_contas() -> pa.Table:
    """102 contas com ids entre 1 e 106, ``numero`` único de 3 a 13 caracteres e nomes que se repetem."""
    ids = [i for i in range(1, 107) if i not in (2, 3, 7, 105)]
    rows: dict[str, list] = {"id_conta": [], "nome": [], "numero": [], "permite_lancamentos": []}
    for k, account_id in enumerate(ids):
        # Uma letra a cada seis contas, e o número cresce com a profundidade: A.1, A.2, A.3.2, ..., A.6.5.1.2.3.
        letter = "ABCDEFGHIJKLMNOPQRST"[k // 6]
        depth = k % 6
        number = f"{letter}.{depth + 1}"
        if depth >= 2:
            number += f".{k}"
        if depth == 5:
            number += ".1.2.3"
        name = ACCOUNT_NAMES[k % len(ACCOUNT_NAMES)]
        rows["id_conta"].append(account_id)
        rows["nome"].append(name if k < 40 else f"{name} {k // len(ACCOUNT_NAMES)}")
        rows["numero"].append(number)
        rows["permite_lancamentos"].append(k % 7 != 0)
    return pa.table(rows, schema=SCHEMAS["cad_contas"])


def build_rel_contas_hierarquias(accounts: pa.Table) -> pa.Table:
    """93 relações na hierarquia 1: 93 filhos distintos, 32 pais distintos."""
    ids = accounts.column("id_conta").to_pylist()
    children = ids[:93]
    parents = [i for i in ids if i <= 100][:32]
    return pa.table(
        {
            "id_rel_conta_hierarquia": list(range(1, 94)),
            "id_hierarquia": [1] * 93,
            "id_parent": [parents[k % len(parents)] for k in range(93)],
            "id_child": children,
        },
        schema=SCHEMAS["rel_contas_hierarquias"],
    )


def build_cad_aliquotas() -> pa.Table:
    """15 alíquotas, ids 2 a 16, pares (origem, destino) únicos, vigência desde 2024-01-01 e sem fim."""
    return pa.table(
        {
            "id": list(range(2, 17)),
            "id_conta_origem": [RATE_ORIGIN_ACCOUNTS[k % len(RATE_ORIGIN_ACCOUNTS)] for k in range(15)],
            "id_conta_destino": [RATE_DESTINATION_ACCOUNTS[k % len(RATE_DESTINATION_ACCOUNTS)] for k in range(15)],
            "data_fim_validade": [None] * 15,
            "data_inicio_validade": [dt.date(2024, 1, 1)] * 15,
            "fator": [RATE_FACTORS[k % len(RATE_FACTORS)] for k in range(15)],
        },
        schema=SCHEMAS["cad_aliquotas"],
    )


def build_dimensions() -> dict[str, pa.Table]:
    """As tabelas ``dom_*``: um veículo, uma hierarquia, três mensurações, cinco segmentos e sete negócios."""
    return {
        "dom_hierarquias_contas": pa.table(
            {"id_hierarquia": [1], "nome": ["Orçamento"], "descricao": ["Hierarquia de contas utilizada no Orçamento e na projeção"]},
            schema=SCHEMAS["dom_hierarquias_contas"],
        ),
        "dom_mensuracoes": pa.table({"id_mensuracao": [1, 2, 3], "nome": list(MEASUREMENT_NAMES)}, schema=SCHEMAS["dom_mensuracoes"]),
        "dom_negocios": pa.table(
            {"id_negocio": list(range(1, 8)), "id_segmento": [segment for _, segment in BUSINESSES], "nome": [name for name, _ in BUSINESSES]},
            schema=SCHEMAS["dom_negocios"],
        ),
        "dom_segmentos": pa.table({"id_segmento": list(range(1, 6)), "nome": list(SEGMENT_NAMES)}, schema=SCHEMAS["dom_segmentos"]),
        "dom_veiculos": pa.table({"id_veiculo": [1], "nome": ["BNDES"]}, schema=SCHEMAS["dom_veiculos"]),
    }


def build_meta_update_status() -> pa.Table:
    """O registro das cargas: uma linha por tabela sem partição, uma por partição das demais, com a partição em JSON.

    Os ids seguem os 21 da leitura de desenvolvimento e continuam do último, porque a base fictícia tem 16 partições.
    """
    dimensions = ("dom_hierarquias_contas", "dom_veiculos", "dom_mensuracoes", "dom_segmentos", "dom_negocios")
    entries: list[tuple[str, str | None]] = [(table, None) for table in dimensions]
    for table in ("cad_operacoes", "rel_contrato_operacao", "cad_contratos", "cad_lancamentos"):
        partition = PARTITIONS[table]
        for value in partition.values:
            partition_json = json.dumps({partition.source: {"__type__": "date", "value": value}})
            entries.append((table, partition_json))
    entries.extend((table, None) for table in ("cad_contas", "rel_contas_hierarquias", "cad_aliquotas"))

    # Os ids da leitura, e depois do último um id novo para cada entrada que a leitura não tinha.
    first_new_id = UPDATE_STATUS_IDS[-1] + 1
    new_ids = range(first_new_id, first_new_id + len(entries) - len(UPDATE_STATUS_IDS))
    ids = list(UPDATE_STATUS_IDS) + list(new_ids)
    started = dt.datetime(2026, 4, 9, 20, 0, 23, 978000)
    return pa.table(
        {
            "id_update_status": ids,
            "table_name": [table for table, _ in entries],
            "partition": [partition for _, partition in entries],
            "timestamp": [started + dt.timedelta(days=k * 7, seconds=k * 61) for k in range(len(entries))],
        },
        schema=SCHEMAS["meta_update_status"],
    )


# ---------------------------------------------------------------------------------------------------------------
# As tabelas particionadas: uma tabela Arrow por partição


def operation_rates(j: int) -> dict[str, float | None]:
    """As taxas da operação ``j`` de uma data, por coluna.

    Uma operação em 40 sai sem taxa alguma, e com ``custo_adicional`` só quando ``j`` é múltiplo de 3. Nas outras,
    ``spread_total`` é o ``spread_basico`` mais 0,1, e uma em 20 grava ``spread_basico`` como zero negativo sem mudar o
    ``spread_total``.
    """
    additional_cost = (14.4, 14.3, 15.3, 0.0, -0.25)[j % 5]
    if j % 40 == 7:
        cost_without_rates = additional_cost if j % 3 == 0 else None
        return {
            "spread_basico": None,
            "spread_risco": None,
            "spread_total": None,
            "taxa_total": None,
            "taxa_bndes": None,
            "custo_adicional": cost_without_rates,
        }

    basic_spread = (1.2, 0.5, 0.8)[j % 3]
    total_spread = round(basic_spread + 0.1, 2)
    bndes_rate = (1.4, 0.6, 1.0, 1.2, 16.1)[j % 5]
    return {
        "spread_basico": -0.0 if j % 20 == 5 else basic_spread,
        "spread_risco": 0.1,
        "spread_total": total_spread,
        "taxa_total": round(bndes_rate + total_spread + (j % 7) * 0.37, 2),
        "taxa_bndes": bndes_rate,
        "custo_adicional": additional_cost,
    }


def build_cad_operacoes() -> dict[str, pa.Table]:
    """As operações de cada data: numéricas de 11 dígitos e ``desemb-<data>-<n>``, com as taxas nulas em bloco."""
    tables = {}
    next_id = 10027979
    for month_index, value in enumerate(PARTITION_VALUES):
        month = dt.date.fromisoformat(value)
        count = ROWS_PER_PARTITION["cad_operacoes"][value]
        rows: dict[str, list] = {name: [] for name in SCHEMAS["cad_operacoes"].names}
        for j in range(count):
            rates = operation_rates(j)
            rows["id_operacao"].append(next_id)
            rows["data"].append(month)
            rows["operacao"].append(f"desemb-{value}-{j:06d}" if j % 4 == 3 else str(10000000001 + month_index * 1000 + j * 97))
            rows["legado"].append(j % 10 == 0)
            rows["area"].append(OPERATION_AREAS[j % 4])
            rows["departamento"].append(None if j % 3 == 0 else DEPARTMENTS[j % 3])
            rows["spread_basico"].append(rates["spread_basico"])
            rows["spread_risco"].append(rates["spread_risco"])
            rows["spread_total"].append(rates["spread_total"])
            rows["taxa_total"].append(rates["taxa_total"])
            rows["taxa_bndes"].append(rates["taxa_bndes"])
            rows["custo_adicional"].append(rates["custo_adicional"])
            rows["instrumento_financeiro"].append(None if j % 3 == 0 else FINANCIAL_INSTRUMENTS[j % 2])
            next_id += 13
        tables[value] = pa.table(rows, schema=SCHEMAS["cad_operacoes"])
    return tables


def build_cad_contratos(rng: random.Random) -> dict[str, pa.Table]:
    """Os contratos de cada data: ``data`` igual à partição, ``data_assinatura`` nula em 7 de cada 13 linhas."""
    tables = {}
    next_id = 5786566
    for month_index, value in enumerate(PARTITION_VALUES):
        month = dt.date.fromisoformat(value)
        count = ROWS_PER_PARTITION["cad_contratos"][value]
        rows: dict[str, list] = {name: [] for name in SCHEMAS["cad_contratos"].names}
        for j in range(count):
            first = month + dt.timedelta(days=30 * (j % 12 + 1))
            # Um sorteio por linha, mesmo nas três em quatro que não o usam: a sequência do gerador fixa os valores de
            # cad_lancamentos, que sorteia depois.
            drawn_rate = round(rng.uniform(0.5, 27.5), 4)
            fixed_rate = (-0.0, 2.5, drawn_rate, 27.5)[j % 4]
            rows["id_contrato"].append(next_id)
            rows["data"].append(month)
            rows["sistema"].append(SOURCE_SYSTEMS[j % 3])
            rows["contrato"].append(f"desemb-{value}-{j:06d}" if j % 5 == 4 else str(10000495031 + month_index * 10000 + j * 53))
            rows["legado"].append(j % 8 == 0)
            rows["um"].append(CURRENCY_UNITS[j % len(CURRENCY_UNITS)])
            rows["to"].append(TO_CODES[j % len(TO_CODES)])
            rows["fonte"].append(FUNDING_SOURCES[j % len(FUNDING_SOURCES)])
            rows["fonte_familia"].append(None if j % 45 == 44 else FUNDING_SOURCE_FAMILIES[j % 4])
            rows["estagio"].append(None if j % 30 == 29 else (1, 2, 3)[j % 3])
            rows["taxa_juros_fixos"].append(fixed_rate)
            rows["data_assinatura"].append(None if j % 13 not in (1, 4, 6, 8, 11, 12) else month - dt.timedelta(days=(j * 37) % 20000))
            rows["data_primeira_amortizacao"].append(first)
            rows["data_ultima_amortizacao"].append(dt.date(2099, 12, 15) if j % 50 == 3 else first + dt.timedelta(days=365 * (j % 20 + 1)))
            next_id += 11
        tables[value] = pa.table(rows, schema=SCHEMAS["cad_contratos"])
    return tables


def build_rel_contrato_operacao(contracts: dict[str, pa.Table], operations: dict[str, pa.Table]) -> dict[str, pa.Table]:
    """A relação N×N de cada data: todo contrato numa operação, um em quatro também na seguinte, e os fatores somando 1.

    As linhas saem agrupadas por contrato, e ``apportionment`` reparte o contrato entre as suas operações.
    """
    tables = {}
    next_id = 2951753
    for value in PARTITION_VALUES:
        month = dt.date.fromisoformat(value)
        contract_rows = contracts[value].to_pylist()
        operation_names = operations[value].column("operacao").to_pylist()
        rows: dict[str, list] = {name: [] for name in SCHEMAS["rel_contrato_operacao"].names}
        for j, contract in enumerate(contract_rows):
            # Todo contrato entra numa operação, e um em cada quatro também na seguinte; o rateio
            # reparte o contrato entre as operações dele e soma 1 por contrato.
            contract_operations = [operation_names[j % len(operation_names)]]
            if j % 4 == 0:
                contract_operations.append(operation_names[(j + 1) % len(operation_names)])
            for operation, factor in zip(contract_operations, apportionment(len(contract_operations)), strict=True):
                rows["id_rel_contrato_operacao"].append(next_id)
                rows["data"].append(month)
                rows["operacao"].append(operation)
                rows["sistema"].append(contract["sistema"])
                rows["contrato"].append(contract["contrato"])
                rows["fator_rateio"].append(factor)
                next_id += 17
        tables[value] = pa.table(rows, schema=SCHEMAS["rel_contrato_operacao"])
    return tables


def evenly_spaced_ids(count: int, largest: int) -> list[int]:
    """``count`` ids crescentes de 1 a ``largest``, espaçados por igual."""
    ids = []
    for i in range(count):
        ids.append(1 + round(i * (largest - 1) / (count - 1)))
    return ids


def postable_account_ids(accounts: pa.Table) -> list[int]:
    """Os ids das contas que aceitam lançamento, a partir da conta 8."""
    ids = []
    for row in accounts.to_pylist():
        if row["permite_lancamentos"] and row["id_conta"] >= 8:
            ids.append(row["id_conta"])
    return ids


def build_cad_lancamentos(rng: random.Random, contracts: dict[str, pa.Table], accounts: pa.Table) -> dict[str, pa.Table]:
    """Os lançamentos projetados de cada ``data_base``: ``data`` posterior à base, ``valor`` com três casas, ids esparsos.

    O último id é ``MAX_ENTRY_ID``. Cada lançamento cita um contrato de ``cad_contratos`` da mesma data, ou nenhum,
    com ``sistema`` e ``contrato`` nulos juntos (o lançamento associado a uma área).
    """
    total = sum(ROWS_PER_PARTITION["cad_lancamentos"].values())
    entry_ids = evenly_spaced_ids(total, MAX_ENTRY_ID)
    entry_index = 0
    postable = postable_account_ids(accounts)
    tables = {}
    for partition_index, value in enumerate(PARTITION_VALUES):
        base = dt.date.fromisoformat(value)
        horizon = month_ends_after(base)
        contract_rows = contracts[value].to_pylist()
        count = ROWS_PER_PARTITION["cad_lancamentos"][value]
        rows: dict[str, list] = {name: [] for name in SCHEMAS["cad_lancamentos"].names}
        for j in range(count):
            contract = contract_rows[j % len(contract_rows)]
            no_contract = j % 50 == 21
            if partition_index == 0 and j < 2:
                amount = EXTREME_AMOUNT if j == 0 else -EXTREME_AMOUNT
            else:
                amount = round(rng.uniform(-5e9, 5e9), 3)
            rows["id_lancamento"].append(entry_ids[entry_index])
            entry_index += 1
            rows["id_veiculo"].append(1)
            rows["id_conta"].append(postable[j % len(postable)])
            rows["data"].append(horizon[j % len(horizon)])
            rows["valor"].append(amount)
            rows["meta"].append(None)
            rows["timestamp"].append(WRITE_TIMESTAMP + dt.timedelta(days=partition_index, seconds=partition_index * 37))
            rows["id_mensuracao"].append(2)
            rows["id_segmento"].append(None if j % 500 == 137 else 1 + j % 5)
            rows["id_negocio"].append(None if j % 5 < 3 else 1 + j % 7)
            rows["data_base"].append(base)
            rows["sistema"].append(None if no_contract else contract["sistema"])
            rows["contrato"].append(None if no_contract else contract["contrato"])
            rows["area"].append(None if j % 400 == 186 else ENTRY_AREAS[j % 4])
        tables[value] = pa.table(rows, schema=SCHEMAS["cad_lancamentos"])
    return tables


def build_tables() -> dict[str, pa.Table | dict[str, pa.Table]]:
    """Toda a base em memória: uma ``pa.Table`` por tabela sem partição, um dicionário por partição nas demais."""
    rng = random.Random(SEED)
    accounts = build_cad_contas()
    operations = build_cad_operacoes()
    contracts = build_cad_contratos(rng)
    tables: dict[str, pa.Table | dict[str, pa.Table]] = {
        "alembic_version": pa.table({"version_num": [ALEMBIC_REVISION]}, schema=SCHEMAS["alembic_version"]),
        "cad_aliquotas": build_cad_aliquotas(),
        "cad_contas": accounts,
        "cad_contratos": contracts,
        "cad_lancamentos": build_cad_lancamentos(rng, contracts, accounts),
        "cad_operacoes": operations,
        **build_dimensions(),
        "meta_update_status": build_meta_update_status(),
        "rel_contas_hierarquias": build_rel_contas_hierarquias(accounts),
        "rel_contrato_operacao": build_rel_contrato_operacao(contracts, operations),
    }
    assert set(tables) == set(SCHEMAS)
    return tables


def write_source(root: Path) -> SourceBase:
    """Grava a base sob ``root``: uma pasta por tabela, as partições Hive, os chunks e o ``schema.json`` solto na raiz."""
    root.mkdir(parents=True, exist_ok=True)
    source = SourceBase(root=root)
    for table, content in build_tables().items():
        folder = root / table
        if isinstance(content, pa.Table):
            # Toda tabela sem partição da origem cabe num único chunk_0.parquet.
            source.files[table] = write_chunks(folder, content, chunk_rows=content.num_rows, pandas_key=written_by_pandas(table))
            source.rows[table] = content.num_rows
            continue
        partition = PARTITIONS[table]
        source.files[table] = []
        source.partition_rows[table] = {}
        for value, data in content.items():
            source.files[table].extend(write_chunks(folder / f"{partition.column}={value}", data, pandas_key=written_by_pandas(table, value)))
            source.partition_rows[table][value] = data.num_rows
        source.rows[table] = sum(source.partition_rows[table].values())

    # O controle de esquema da biblioteca anterior, como está na raiz da base real; a carga o ignora.
    shutil.copyfile(SCHEMA_CONTROL, root / "schema.json")
    return source
