"""A base Parquet de origem ``db_projetado``, fictícia, com a estrutura lida nas duas bases reais.

``probes/parquet_source.py`` leu a base de desenvolvimento em 2026-09-20 e a de produção em
2026-09-21 (as duas com 14 pastas de tabela, 205 arquivos, 3,76 GB e 187 milhões de linhas, e a
mesma seção 3), e ``write_source`` reproduz o que as leituras fixaram, com poucas linhas por tabela:

- as 14 tabelas com as mesmas colunas, na mesma ordem, com os mesmos tipos Arrow e a mesma
  nulidade declarada;
- a partição Hive por ``data_str`` (``cad_contratos``, ``cad_operacoes``,
  ``rel_contrato_operacao``) e por ``data_base_str`` (``cad_lancamentos``), com o valor no caminho
  e nunca dentro do arquivo, igual a ``data`` ou ``data_base`` em toda linha da partição, fins de
  mês não contíguos;
- vários arquivos ``chunk_<n>.parquet`` por partição, numerados de 0 sem zeros à esquerda, o
  último com o resto das linhas;
- um row group por arquivo, SNAPPY, sem dicionário, formato 1.0, timestamps em ``INT96`` sem
  estatísticas, nenhum ``field_id``;
- a chave ``pandas`` no rodapé de parte dos arquivos: em todos na base de desenvolvimento; na de
  produção, em 5 de 8, 111 de 144, 9 de 13 e 16 de 30 arquivos das tabelas particionadas e em
  nenhum de ``alembic_version`` e ``meta_update_status``; aqui, fora da última partição de cada
  tabela particionada e dessas duas tabelas;
- ``alembic_version`` e ``meta_update_status`` fora do modelo;
- na raiz, o ``schema.json`` da biblioteca anterior, o arquivo real
  (``source_db_projetado_schema.json``): o controle de esquema no formato da reflexão do
  SQLAlchemy, com colunas, nulidade, chaves estrangeiras, índices e restrições de unicidade de
  cada tabela.

Os valores são fictícios, determinísticos e consistentes com o modelo de referência: toda chave
estrangeira do modelo tem a linha referenciada, toda chave é única, ``rel_contas_hierarquias`` é uma
árvore de contas, sem conta pai de si mesma (leitura do usuário na base real, 2026-09-23), e as
quatro tabelas particionadas têm as mesmas quatro datas, para que cada ``data_base`` de
``cad_lancamentos`` tenha os seus ``cad_contratos`` (a leitura mostrou 2026-01-31 só em
``cad_lancamentos``). ``rel_contrato_operacao`` é a relação N×N entre contratos e operações da mesma
data: toda operação tem contratos, todo contrato está em uma ou duas operações, e ``fator_rateio``
reparte cada contrato entre as suas operações e soma 1 por contrato (leitura do usuário na base de
produção, 2026-09-21). Os valores reproduzem o que a carga inicial tem de tratar: ``valor`` com três
casas (o par extremo ``±11846195394.628``), ``fator`` com cinco, ``data_assinatura`` nula em mais da
metade das linhas, ``meta`` sempre nula, ``id_lancamento`` até 1.113.599.996 em ``int32``, e as sete
colunas de ``cad_contratos`` declaradas anuláveis nos arquivos sem nulo algum. Os valores que
diferem entre as duas bases são os da leitura de desenvolvimento: a de produção tem 113 linhas a
menos, ids máximos menores (``id_lancamento`` até 952.517.158) e o extremo de ``valor`` em
``±11846195394.62801``. O ``timestamp`` tem precisão de microssegundo; a parte sub-microssegundo da
origem é desconhecida, porque o ``INT96`` não tem estatística.

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

# A origem corta cada partição a cada 1.000.000 de linhas; 20 dá a mesma estrutura de arquivos em
# poucas linhas.
CHUNK_ROWS = 20
SEED = 20260920
INT96_TIMESTAMP = pa.timestamp("ns")
SCHEMA_CONTROL = Path(__file__).with_name("source_db_projetado_schema.json")


@dataclass(frozen=True)
class Partition:
    """A partição Hive de uma tabela.

    O nome no caminho, a coluna do arquivo que tem o mesmo valor e os valores.
    """

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

# Linhas por partição. 2026-01-31 de cad_lancamentos passa de dez chunks (chunk_10 e chunk_11
# existem, e a ordem alfabética os põe antes de chunk_2). rel_contrato_operacao deriva dos
# contratos: cada contrato numa operação e um em quatro também na seguinte, 42 linhas em 2026-02-28,
# com um chunk_2 de duas linhas. Cada data tem mais contratos que operações, para toda operação ter
# contrato.
ROWS_PER_PARTITION: dict[str, dict[str, int]] = {
    "cad_contratos": {"2026-01-31": 38, "2026-02-28": 33, "2026-03-31": 50, "2026-06-30": 41},
    "cad_lancamentos": {"2026-01-31": 235, "2026-02-28": 60, "2026-03-31": 60, "2026-06-30": 60},
    "cad_operacoes": {"2026-01-31": 28, "2026-02-28": 25, "2026-03-31": 40, "2026-06-30": 30},
}

# O esquema de cada tabela como a leitura o mostrou: nome, tipo Arrow e nulidade, na ordem das
# colunas.
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
    "dom_mensuracoes": pa.schema(
        [
            pa.field("id_mensuracao", pa.int32(), nullable=False),
            pa.field("nome", pa.string(), nullable=False),
        ]
    ),
    "dom_negocios": pa.schema(
        [
            pa.field("id_negocio", pa.int32(), nullable=False),
            pa.field("id_segmento", pa.int32(), nullable=False),
            pa.field("nome", pa.string(), nullable=False),
        ]
    ),
    "dom_segmentos": pa.schema(
        [
            pa.field("id_segmento", pa.int32(), nullable=False),
            pa.field("nome", pa.string(), nullable=False),
        ]
    ),
    "dom_veiculos": pa.schema(
        [
            pa.field("id_veiculo", pa.int32(), nullable=False),
            pa.field("nome", pa.string(), nullable=False),
        ]
    ),
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

# As tabelas da origem que o modelo de referência não tem: a de controle do Alembic e o registro das
# cargas.
OUTSIDE_MODEL = ("alembic_version", "meta_update_status")
ALEMBIC_REVISION = "131d90070de7"

# As chaves do modelo de referência (tests/reference_model; test_reference_model.py confere a
# transcrição): a chave primária e as restrições de unicidade de cada tabela, e as chaves
# estrangeiras com a tabela e as colunas referenciadas. A base fictícia satisfaz todas.
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
    (
        "cad_lancamentos",
        ["data_base", "sistema", "contrato"],
        "cad_contratos",
        ["data", "sistema", "contrato"],
    ),
    ("rel_contrato_operacao", ["data", "operacao"], "cad_operacoes", ["data", "operacao"]),
    (
        "cad_contratos",
        ["data", "sistema", "contrato"],
        "rel_contrato_operacao",
        ["data", "sistema", "contrato"],
    ),
]
# As colunas NOT NULL do modelo que os arquivos declaram anuláveis; o cast do contrato as recusa com
# nulo.
MODEL_NOT_NULL_DECLARED_NULLABLE: dict[str, list[str]] = {
    "cad_contratos": [
        "sistema",
        "um",
        "to",
        "fonte",
        "taxa_juros_fixos",
        "data_primeira_amortizacao",
        "data_ultima_amortizacao",
    ],
}

SOURCE_SYSTEMS = (15, 43, 89)
MAX_ENTRY_ID = 1_113_599_996
EXTREME_AMOUNT = 11846195394.628
WRITE_TIMESTAMP = dt.datetime(2026, 3, 18, 16, 53, 22, 296000)
PROJECTION_HORIZON = dt.date(2026, 12, 31)

SEGMENT_NAMES = (
    "Crédito e Serviços",
    "Renda Variável",
    "Tesouraria e ALM",
    "Corporativo",
    "Remuneração do Acionista",
)
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
UPDATE_STATUS_IDS = (
    1, 2, 5, 6, 7, 49, 50, 51, 52, 53, 127, 128, 129, 130, 151, 152, 178, 179, 180, 181, 182
)


@dataclass
class SourceBase:
    """A base gravada: a raiz, os arquivos de cada tabela na ordem de gravação, as linhas por tabela
    e por partição."""

    root: Path
    files: dict[str, list[Path]] = field(default_factory=dict)
    rows: dict[str, int] = field(default_factory=dict)
    partition_rows: dict[str, dict[str, int]] = field(default_factory=dict)


def month_ends_after(date: dt.date) -> list[dt.date]:
    """Os fins de mês depois de ``date`` até ``PROJECTION_HORIZON``: os meses que um lançamento de
    ``data_base = date`` projeta."""
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
    """``count`` fatores de rateio iguais, que somam exatamente 1.

    Um contrato está em uma ou duas operações: os fatores 1 e 1/2 são frações diádicas, exatas em
    ponto flutuante, e a soma por contrato dá 1.0 sem erro.
    """
    return [1.0 / count] * count


def as_pandas_wrote(table: pa.Table) -> pa.Table:
    """A tabela como a origem a gravou: pelo pandas, sem índice, com o esquema explícito, e a chave
    ``pandas`` no rodapé."""
    return pa.Table.from_pandas(table.to_pandas(), schema=table.schema, preserve_index=False)


def written_by_pandas(table_name: str, value: str | None = None) -> bool:
    """Se o arquivo da tabela, ou da partição ``value``, leva a chave ``pandas`` no rodapé.

    A base de produção tem a chave em parte dos arquivos das tabelas particionadas e em nenhum de
    ``alembic_version`` e ``meta_update_status``; aqui a última partição de cada tabela particionada
    e essas duas tabelas saem sem ela.
    """
    if table_name in OUTSIDE_MODEL:
        return False
    return table_name not in PARTITIONS or value != PARTITIONS[table_name].values[-1]


def write_chunks(
    folder: Path, table: pa.Table, chunk_rows: int = CHUNK_ROWS, pandas_key: bool = True
) -> list[Path]:
    """Grava ``table`` em ``folder`` como ``chunk_0.parquet``, ``chunk_1.parquet``, ...,
    ``chunk_rows`` linhas por arquivo."""
    folder.mkdir(parents=True, exist_ok=True)
    paths = []
    for index, offset in enumerate(range(0, table.num_rows, chunk_rows)):
        path = folder / f"chunk_{index}.parquet"
        chunk = table.slice(offset, chunk_rows)
        # A conversão pelo pandas é por chunk, e não da partição inteira: o numpy_type da chave
        # pandas segue os nulos de cada arquivo, e uma coluna int32 com nulo sai como float64.
        if pandas_key:
            chunk = as_pandas_wrote(chunk)
        # Um row group por arquivo, SNAPPY, sem dicionário, formato 1.0 e INT96: o layout físico que
        # a leitura mostrou.
        pq.write_table(
            chunk,
            path,
            version="1.0",
            compression="snappy",
            use_dictionary=False,
            use_deprecated_int96_timestamps=True,
        )
        paths.append(path)
    return paths


# --------------------------------------------------------------------------------------------------
# As tabelas sem partição


def build_cad_contas() -> pa.Table:
    """102 contas com ids entre 1 e 106, ``numero`` único de 3 a 13 caracteres e nomes que se
    repetem."""
    ids = [i for i in range(1, 107) if i not in (2, 3, 7, 105)]
    rows = []
    for position, account_id in enumerate(ids):
        # Uma letra a cada seis contas, e o número cresce com a profundidade: A.1, A.2, A.3.2, ...,
        # A.6.5.1.2.3.
        letter = "ABCDEFGHIJKLMNOPQRST"[position // 6]
        depth = position % 6
        number = f"{letter}.{depth + 1}"
        if depth >= 2:
            number += f".{position}"
        if depth == 5:
            number += ".1.2.3"
        name = ACCOUNT_NAMES[position % len(ACCOUNT_NAMES)]
        if position >= 40:
            name = f"{name} {position // len(ACCOUNT_NAMES)}"
        rows.append(
            {
                "id_conta": account_id,
                "nome": name,
                "numero": number,
                "permite_lancamentos": position % 7 != 0,
            }
        )
    return pa.Table.from_pylist(rows, schema=SCHEMAS["cad_contas"])


def build_rel_contas_hierarquias(accounts: pa.Table) -> pa.Table:
    """A hierarquia 1, uma árvore de contas contábeis: 93 relações, 93 filhos distintos e 32 pais
    distintos, nenhuma conta pai de si mesma.

    A raiz é a 32ª conta por id, a 35, a única que não é filha de nenhuma. A ordem é a raiz e depois
    as demais contas por id; cada uma das 93 seguintes aponta para um pai entre as 32 primeiras da
    ordem, sempre anterior a ela, dois ou três filhos por pai, o que dá cinco níveis sem ciclo. A
    conta de id 1 é pai e filha, como na leitura de produção, em que ``id_parent`` e ``id_child``
    começam em 1; as oito contas de id mais alto ficam fora da árvore.
    """
    relation_count = 93
    parent_count = 32
    ids = accounts.column("id_conta").to_pylist()
    root = ids[parent_count - 1]
    ordered = [root]
    for account in ids:
        if account != root:
            ordered.append(account)
    children = ordered[1:relation_count + 1]
    parents = []
    for position in range(relation_count):
        # O filho está na posição position + 1 da ordem, e o pai numa posição anterior.
        parents.append(ordered[position * parent_count // relation_count])
    return pa.table(
        {
            "id_rel_conta_hierarquia": list(range(1, relation_count + 1)),
            "id_hierarquia": [1] * relation_count,
            "id_parent": parents,
            "id_child": children,
        },
        schema=SCHEMAS["rel_contas_hierarquias"],
    )


def build_cad_aliquotas() -> pa.Table:
    """15 alíquotas, ids 2 a 16, pares (origem, destino) únicos, vigência desde 2024-01-01 e sem
    fim."""
    rows = []
    for position in range(15):
        origin = RATE_ORIGIN_ACCOUNTS[position % len(RATE_ORIGIN_ACCOUNTS)]
        destination = RATE_DESTINATION_ACCOUNTS[position % len(RATE_DESTINATION_ACCOUNTS)]
        rows.append(
            {
                "id": 2 + position,
                "id_conta_origem": origin,
                "id_conta_destino": destination,
                "data_fim_validade": None,
                "data_inicio_validade": dt.date(2024, 1, 1),
                "fator": RATE_FACTORS[position % len(RATE_FACTORS)],
            }
        )
    return pa.Table.from_pylist(rows, schema=SCHEMAS["cad_aliquotas"])


def build_dimensions() -> dict[str, pa.Table]:
    """As tabelas ``dom_*``: um veículo, uma hierarquia, três mensurações, cinco segmentos e sete
    negócios."""
    hierarchy_description = "Hierarquia de contas utilizada no Orçamento e na projeção"
    return {
        "dom_hierarquias_contas": pa.table(
            {"id_hierarquia": [1], "nome": ["Orçamento"], "descricao": [hierarchy_description]},
            schema=SCHEMAS["dom_hierarquias_contas"],
        ),
        "dom_mensuracoes": pa.table(
            {"id_mensuracao": [1, 2, 3], "nome": list(MEASUREMENT_NAMES)},
            schema=SCHEMAS["dom_mensuracoes"],
        ),
        "dom_negocios": pa.table(
            {
                "id_negocio": list(range(1, 8)),
                "id_segmento": [segment for _, segment in BUSINESSES],
                "nome": [name for name, _ in BUSINESSES],
            },
            schema=SCHEMAS["dom_negocios"],
        ),
        "dom_segmentos": pa.table(
            {"id_segmento": list(range(1, 6)), "nome": list(SEGMENT_NAMES)},
            schema=SCHEMAS["dom_segmentos"],
        ),
        "dom_veiculos": pa.table(
            {"id_veiculo": [1], "nome": ["BNDES"]}, schema=SCHEMAS["dom_veiculos"]
        ),
    }


def build_meta_update_status() -> pa.Table:
    """O registro das cargas: uma linha por tabela sem partição, uma por partição das demais, com a
    partição em JSON.

    Os ids seguem os 21 da leitura de desenvolvimento e continuam do último, porque a base fictícia
    tem 16 partições.
    """
    dimensions = (
        "dom_hierarquias_contas",
        "dom_veiculos",
        "dom_mensuracoes",
        "dom_segmentos",
        "dom_negocios",
    )
    entries: list[tuple[str, str | None]] = [(table, None) for table in dimensions]
    for table in ("cad_operacoes", "rel_contrato_operacao", "cad_contratos", "cad_lancamentos"):
        partition = PARTITIONS[table]
        for value in partition.values:
            partition_json = json.dumps({partition.source: {"__type__": "date", "value": value}})
            entries.append((table, partition_json))
    account_tables = ("cad_contas", "rel_contas_hierarquias", "cad_aliquotas")
    entries.extend((table, None) for table in account_tables)

    # Os ids da leitura, e depois do último um id novo para cada entrada que a leitura não tinha.
    first_new_id = UPDATE_STATUS_IDS[-1] + 1
    new_ids = range(first_new_id, first_new_id + len(entries) - len(UPDATE_STATUS_IDS))
    ids = list(UPDATE_STATUS_IDS) + list(new_ids)
    started = dt.datetime(2026, 4, 9, 20, 0, 23, 978000)
    timestamps = []
    for position in range(len(entries)):
        timestamps.append(started + dt.timedelta(days=position * 7, seconds=position * 61))
    return pa.table(
        {
            "id_update_status": ids,
            "table_name": [table for table, _ in entries],
            "partition": [partition for _, partition in entries],
            "timestamp": timestamps,
        },
        schema=SCHEMAS["meta_update_status"],
    )


# --------------------------------------------------------------------------------------------------
# As tabelas particionadas: uma tabela Arrow por partição


def operation_rates(position: int) -> dict[str, float | None]:
    """As taxas da operação na posição ``position`` de uma data, por coluna.

    Uma operação em 40 sai sem taxa alguma, e com ``custo_adicional`` só quando ``position`` é
    múltiplo de 3. Nas outras, ``spread_total`` é o ``spread_basico`` mais 0,1, e uma em 20 grava
    ``spread_basico`` como zero negativo sem mudar o ``spread_total``.
    """
    additional_cost = (14.4, 14.3, 15.3, 0.0, -0.25)[position % 5]
    if position % 40 == 7:
        cost_without_rates = additional_cost if position % 3 == 0 else None
        return {
            "spread_basico": None,
            "spread_risco": None,
            "spread_total": None,
            "taxa_total": None,
            "taxa_bndes": None,
            "custo_adicional": cost_without_rates,
        }

    basic_spread = (1.2, 0.5, 0.8)[position % 3]
    total_spread = round(basic_spread + 0.1, 2)
    bndes_rate = (1.4, 0.6, 1.0, 1.2, 16.1)[position % 5]
    return {
        "spread_basico": -0.0 if position % 20 == 5 else basic_spread,
        "spread_risco": 0.1,
        "spread_total": total_spread,
        "taxa_total": round(bndes_rate + total_spread + (position % 7) * 0.37, 2),
        "taxa_bndes": bndes_rate,
        "custo_adicional": additional_cost,
    }


def disbursement_code(value: str, position: int) -> str:
    """O código ``desemb-<data>-<n>`` da linha na posição ``position`` da partição ``value``."""
    return f"desemb-{value}-{position:06d}"


def operation_row(
    operation_id: int, month_index: int, value: str, position: int
) -> dict[str, object]:
    """A linha na posição ``position`` de ``cad_operacoes`` na partição ``value``.

    ``operacao`` é ``desemb-<data>-<n>`` em uma linha a cada quatro, e numérica de 11 dígitos nas
    outras.
    """
    if position % 4 == 3:
        operation = disbursement_code(value, position)
    else:
        operation = str(10000000001 + month_index * 1000 + position * 97)
    department = None if position % 3 == 0 else DEPARTMENTS[position % 3]
    instrument = None if position % 3 == 0 else FINANCIAL_INSTRUMENTS[position % 2]
    return {
        "id_operacao": operation_id,
        "data": dt.date.fromisoformat(value),
        "operacao": operation,
        "legado": position % 10 == 0,
        "area": OPERATION_AREAS[position % 4],
        "departamento": department,
        **operation_rates(position),
        "instrumento_financeiro": instrument,
    }


def build_cad_operacoes() -> dict[str, pa.Table]:
    """As operações de cada data: numéricas de 11 dígitos e ``desemb-<data>-<n>``, com as taxas
    nulas em bloco."""
    tables = {}
    next_id = 10027979
    for month_index, value in enumerate(PARTITION_VALUES):
        count = ROWS_PER_PARTITION["cad_operacoes"][value]
        rows = []
        for position in range(count):
            rows.append(operation_row(next_id, month_index, value, position))
            next_id += 13
        tables[value] = pa.Table.from_pylist(rows, schema=SCHEMAS["cad_operacoes"])
    return tables


def contract_row(
    rng: random.Random, contract_id: int, month_index: int, value: str, position: int
) -> dict[str, object]:
    """A linha na posição ``position`` de ``cad_contratos`` na partição ``value``.

    ``contrato`` é ``desemb-<data>-<n>`` em uma linha a cada cinco, e numérico nas outras;
    ``data_assinatura`` é nula em 7 de cada 13 linhas.
    """
    month = dt.date.fromisoformat(value)
    first_amortization = month + dt.timedelta(days=30 * (position % 12 + 1))
    # Um sorteio por linha, mesmo nas três em quatro que não o usam: a sequência do gerador fixa os
    # valores de cad_lancamentos, que sorteia depois.
    drawn_rate = round(rng.uniform(0.5, 27.5), 4)
    fixed_rate = (-0.0, 2.5, drawn_rate, 27.5)[position % 4]
    if position % 5 == 4:
        contract = disbursement_code(value, position)
    else:
        contract = str(10000495031 + month_index * 10000 + position * 53)
    if position % 13 in (1, 4, 6, 8, 11, 12):
        signature_date = month - dt.timedelta(days=(position * 37) % 20000)
    else:
        signature_date = None
    if position % 50 == 3:
        last_amortization = dt.date(2099, 12, 15)
    else:
        last_amortization = first_amortization + dt.timedelta(days=365 * (position % 20 + 1))
    family = None if position % 45 == 44 else FUNDING_SOURCE_FAMILIES[position % 4]
    stage = None if position % 30 == 29 else 1 + position % 3
    return {
        "id_contrato": contract_id,
        "data": month,
        "sistema": SOURCE_SYSTEMS[position % 3],
        "contrato": contract,
        "legado": position % 8 == 0,
        "um": CURRENCY_UNITS[position % len(CURRENCY_UNITS)],
        "to": TO_CODES[position % len(TO_CODES)],
        "fonte": FUNDING_SOURCES[position % len(FUNDING_SOURCES)],
        "fonte_familia": family,
        "estagio": stage,
        "taxa_juros_fixos": fixed_rate,
        "data_assinatura": signature_date,
        "data_primeira_amortizacao": first_amortization,
        "data_ultima_amortizacao": last_amortization,
    }


def build_cad_contratos(rng: random.Random) -> dict[str, pa.Table]:
    """Os contratos de cada data: ``data`` igual à partição, ``data_assinatura`` nula em 7 de cada
    13 linhas."""
    tables = {}
    next_id = 5786566
    for month_index, value in enumerate(PARTITION_VALUES):
        count = ROWS_PER_PARTITION["cad_contratos"][value]
        rows = []
        for position in range(count):
            rows.append(contract_row(rng, next_id, month_index, value, position))
            next_id += 11
        tables[value] = pa.Table.from_pylist(rows, schema=SCHEMAS["cad_contratos"])
    return tables


def contract_operation_links(
    contracts: list[dict[str, object]], operation_names: list[str]
) -> list[tuple[dict[str, object], str, float]]:
    """Os pares contrato e operação de uma data, com o fator de rateio, agrupados por contrato.

    O contrato na posição ``position`` fica na operação de mesma posição, e um contrato a cada
    quatro também na seguinte.
    """
    links = []
    for position, contract in enumerate(contracts):
        operations = [operation_names[position % len(operation_names)]]
        if position % 4 == 0:
            operations.append(operation_names[(position + 1) % len(operation_names)])
        factors = apportionment(len(operations))
        for operation, factor in zip(operations, factors, strict=True):
            links.append((contract, operation, factor))
    return links


def build_rel_contrato_operacao(
    contracts: dict[str, pa.Table], operations: dict[str, pa.Table]
) -> dict[str, pa.Table]:
    """A relação N×N de cada data: todo contrato numa operação, um em quatro também na seguinte, e
    os fatores somando 1.

    As linhas saem agrupadas por contrato, e ``apportionment`` reparte o contrato entre as suas
    operações.
    """
    tables = {}
    next_id = 2951753
    for value in PARTITION_VALUES:
        month = dt.date.fromisoformat(value)
        contract_rows = contracts[value].to_pylist()
        operation_names = operations[value].column("operacao").to_pylist()
        rows = []
        for contract, operation, factor in contract_operation_links(contract_rows, operation_names):
            rows.append(
                {
                    "id_rel_contrato_operacao": next_id,
                    "data": month,
                    "operacao": operation,
                    "sistema": contract["sistema"],
                    "contrato": contract["contrato"],
                    "fator_rateio": factor,
                }
            )
            next_id += 17
        tables[value] = pa.Table.from_pylist(rows, schema=SCHEMAS["rel_contrato_operacao"])
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


def entry_amount(rng: random.Random, partition_index: int, position: int) -> float:
    """O ``valor`` de um lançamento: o par extremo nas duas primeiras linhas da primeira data, e um
    sorteio com três casas nas outras."""
    if partition_index == 0 and position == 0:
        return EXTREME_AMOUNT
    if partition_index == 0 and position == 1:
        return -EXTREME_AMOUNT
    return round(rng.uniform(-5e9, 5e9), 3)


def build_cad_lancamentos(
    rng: random.Random, contracts: dict[str, pa.Table], accounts: pa.Table
) -> dict[str, pa.Table]:
    """Os lançamentos projetados de cada ``data_base``: ``data`` posterior à base, ``valor`` com
    três casas, ids esparsos.

    O último id é ``MAX_ENTRY_ID``. Cada lançamento cita um contrato de ``cad_contratos`` da mesma
    data, ou nenhum, com ``sistema`` e ``contrato`` nulos juntos (o lançamento associado a uma
    área).
    """
    total = sum(ROWS_PER_PARTITION["cad_lancamentos"].values())
    entry_ids = iter(evenly_spaced_ids(total, MAX_ENTRY_ID))
    postable = postable_account_ids(accounts)
    tables = {}
    for partition_index, value in enumerate(PARTITION_VALUES):
        base = dt.date.fromisoformat(value)
        horizon = month_ends_after(base)
        contract_rows = contracts[value].to_pylist()
        written_at = WRITE_TIMESTAMP + dt.timedelta(
            days=partition_index, seconds=partition_index * 37
        )
        count = ROWS_PER_PARTITION["cad_lancamentos"][value]
        rows = []
        for position in range(count):
            contract = contract_rows[position % len(contract_rows)]
            no_contract = position % 50 == 21
            rows.append(
                {
                    "id_lancamento": next(entry_ids),
                    "id_veiculo": 1,
                    "id_conta": postable[position % len(postable)],
                    "data": horizon[position % len(horizon)],
                    "valor": entry_amount(rng, partition_index, position),
                    "meta": None,
                    "timestamp": written_at,
                    "id_mensuracao": 2,
                    "id_segmento": None if position % 500 == 137 else 1 + position % 5,
                    "id_negocio": None if position % 5 < 3 else 1 + position % 7,
                    "data_base": base,
                    "sistema": None if no_contract else contract["sistema"],
                    "contrato": None if no_contract else contract["contrato"],
                    "area": None if position % 400 == 186 else ENTRY_AREAS[position % 4],
                }
            )
        tables[value] = pa.Table.from_pylist(rows, schema=SCHEMAS["cad_lancamentos"])
    return tables


def build_tables() -> dict[str, pa.Table | dict[str, pa.Table]]:
    """Toda a base em memória: uma ``pa.Table`` por tabela sem partição, um dicionário por partição
    nas demais."""
    rng = random.Random(SEED)
    accounts = build_cad_contas()
    operations = build_cad_operacoes()
    contracts = build_cad_contratos(rng)
    tables: dict[str, pa.Table | dict[str, pa.Table]] = {
        "alembic_version": pa.table(
            {"version_num": [ALEMBIC_REVISION]}, schema=SCHEMAS["alembic_version"]
        ),
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
    """Grava a base sob ``root``: uma pasta por tabela, as partições Hive, os chunks e o
    ``schema.json`` solto na raiz."""
    root.mkdir(parents=True, exist_ok=True)
    source = SourceBase(root=root)
    for table, content in build_tables().items():
        folder = root / table
        if isinstance(content, pa.Table):
            # Toda tabela sem partição da origem cabe num único chunk_0.parquet.
            pandas_key = written_by_pandas(table)
            source.files[table] = write_chunks(
                folder, content, chunk_rows=content.num_rows, pandas_key=pandas_key
            )
            source.rows[table] = content.num_rows
            continue
        partition = PARTITIONS[table]
        source.files[table] = []
        source.partition_rows[table] = {}
        for value, data in content.items():
            partition_folder = folder / f"{partition.column}={value}"
            pandas_key = written_by_pandas(table, value)
            source.files[table].extend(write_chunks(partition_folder, data, pandas_key=pandas_key))
            source.partition_rows[table][value] = data.num_rows
        source.rows[table] = sum(source.partition_rows[table].values())

    # O controle de esquema da biblioteca anterior, como está na raiz da base real; a carga o
    # ignora.
    shutil.copyfile(SCHEMA_CONTROL, root / "schema.json")
    return source
