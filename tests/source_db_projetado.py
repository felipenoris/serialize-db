"""A base Parquet de origem ``db_projetado`` fabricada com a estrutura que o probe leu nas duas bases reais.

``probes/parquet_source.py`` leu a base de desenvolvimento em 2026-09-20 e a de produção em
2026-09-21 (as duas com 14 pastas de tabela, 205 arquivos, 3,76 GB e 187 milhões de linhas, e a
mesma seção 3), e ``write_source`` reproduz o que as leituras fixaram, com poucas linhas por tabela: as 14 tabelas com as mesmas colunas, na mesma ordem, com os mesmos tipos Arrow e
a mesma nulidade declarada; a partição Hive por ``data_str`` (``cad_contratos``, ``cad_operacoes``,
``rel_contrato_operacao``) e por ``data_base_str`` (``cad_lancamentos``), com o valor no caminho e
nunca dentro do arquivo, igual a ``data`` ou ``data_base`` em toda linha da partição, fins de mês
não contíguos; vários arquivos ``chunk_<n>.parquet`` por partição, numerados de 0 sem zeros à
esquerda, o último menor que os demais; um row group por arquivo, SNAPPY, sem dicionário, formato
1.0, timestamps em ``INT96`` sem estatísticas, nenhum ``field_id``, e a chave ``pandas`` no rodapé de
parte dos arquivos (em todos na base de desenvolvimento; na de produção, em 5 de 8, 111 de 144, 9 de
13 e 16 de 30 arquivos das tabelas particionadas e em nenhum de ``alembic_version`` e
``meta_update_status``; aqui, fora da última partição de cada tabela particionada e dessas duas
tabelas);
``alembic_version`` e ``meta_update_status`` fora do modelo, e na raiz o ``schema.json`` da
biblioteca anterior, o arquivo real (``source_db_projetado_schema.json``): o controle de esquema
no formato da reflexão do SQLAlchemy, com colunas, nulidade, chaves estrangeiras, índices e
restrições de unicidade de cada tabela.

Os valores são fictícios, determinísticos e consistentes com o modelo de referência (decisão do
usuário de 2026-09-20): toda chave estrangeira do modelo tem a linha referenciada, toda chave é
única, e as quatro tabelas particionadas têm as mesmas quatro datas, para que cada ``data_base`` de
``cad_lancamentos`` tenha os seus ``cad_contratos`` (a leitura mostrou 2026-01-31 só em
``cad_lancamentos``). ``rel_contrato_operacao`` é a relação N×N entre contratos e operações da mesma
data: toda operação tem contratos, todo contrato está em uma ou duas operações, e ``fator_rateio``
soma 1 entre os contratos de cada operação. Os valores reproduzem o que a carga inicial tem de
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

import datetime as dt
import json
import random
import shutil
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd
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

SISTEMAS = (15, 43, 89)
MAX_ID_LANCAMENTO = 1_113_599_996
EXTREME_VALOR = 11846195394.628
WRITE_TIMESTAMP = dt.datetime(2026, 3, 18, 16, 53, 22, 296000)
PROJECTION_HORIZON = dt.date(2026, 12, 31)

SEGMENTOS = ("Crédito e Serviços", "Renda Variável", "Tesouraria e ALM", "Corporativo", "Remuneração do Acionista")
NEGOCIOS = (
    ("Crédito e Serviços", 1),
    ("Estruturação de Projetos", 1),
    ("Estruturação de Ofertas Públicas", 2),
    ("Renda Variável", 2),
    ("Tesouraria e ALM", 3),
    ("Corporativo", 4),
    ("Remuneração do Acionista", 5),
)
MENSURACOES = ("Realizado", "Projetado", "Orçado")
CONTA_NOMES = (
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
AREAS_OPERACAO = ("ADIG", "AI", "AC", "GP")
AREAS_LANCAMENTO = ("ADIG", "AI", "GP", "Sem Área")
DEPARTAMENTOS = ("DCRED2", "DEPRI", "JUCRE")
INSTRUMENTOS = ("AEROPORTOS DE PRIMEIRO CICLO", "TURISMO, COMÉRCIO E SERVIÇOS")
FONTE_FAMILIAS = ("FAT", "FMM", "FMC", "BND")
FONTES = (2277, 3964, 2300, 4199, 1112, 8014, 9023)
UNIDADES = (185, 604, 202, 777, 19, 145, 143)
TOS = ("01", "ZB", "ZD", "RC", "02", "RI", "05", "ZT")
ALIQUOTA_ORIGENS = (10, 106, 31, 8, 34, 9, 4, 47, 103, 20, 5, 19, 43)
ALIQUOTA_DESTINOS = (22, 25, 99, 24, 98, 50, 26, 21)
ALIQUOTA_FATORES = (0.66, 0.55, -0.15, 1.0, 0.59895, -0.0465, -1.0, -0.0925, 0.0465)
UPDATE_STATUS_IDS = (1, 2, 5, 6, 7, 49, 50, 51, 52, 53, 127, 128, 129, 130, 151, 152, 178, 179, 180, 181, 182)


@dataclass
class SourceBase:
    """A base gravada: a raiz, os arquivos de cada tabela na ordem de gravação, as linhas por tabela e por partição."""

    root: Path
    files: dict[str, list[Path]] = field(default_factory=dict)
    rows: dict[str, int] = field(default_factory=dict)
    partition_rows: dict[str, dict[str, int]] = field(default_factory=dict)

    def table_folder(self, table: str) -> Path:
        return self.root / table


def month_ends_after(date: dt.date, until: dt.date = PROJECTION_HORIZON) -> list[dt.date]:
    """Os fins de mês depois de ``date`` até ``until``: os meses que um lançamento de ``data_base = date`` projeta."""
    ends = []
    year, month = date.year, date.month
    while True:
        month += 1
        if month == 13:
            year, month = year + 1, 1
        following = dt.date(year + (month == 12), month % 12 + 1, 1) - dt.timedelta(days=1)
        if following > until:
            return ends
        ends.append(following)


def apportionment(count: int) -> list[float]:
    """``count`` fatores de rateio que somam exatamente 1: metade para o primeiro e o resto repartido do mesmo modo.

    As frações são diádicas (1, 1/2, 1/4, ...), exatas em ponto flutuante, e a soma por operação dá 1.0 sem erro.
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
    for k, id_conta in enumerate(ids):
        letter = "ABCDEFGHIJKLMNOPQRST"[k // 6]
        depth = k % 6
        numero = f"{letter}.{depth + 1}" + (f".{k}" if depth >= 2 else "") + (".1.2.3" if depth == 5 else "")
        base = CONTA_NOMES[k % len(CONTA_NOMES)]
        rows["id_conta"].append(id_conta)
        rows["nome"].append(base if k < 40 else f"{base} {k // len(CONTA_NOMES)}")
        rows["numero"].append(numero)
        rows["permite_lancamentos"].append(k % 7 != 0)
    return pa.table(rows, schema=SCHEMAS["cad_contas"])


def build_rel_contas_hierarquias(contas: pa.Table) -> pa.Table:
    """93 relações na hierarquia 1: 93 filhos distintos, 32 pais distintos."""
    ids = contas.column("id_conta").to_pylist()
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
            "id_conta_origem": [ALIQUOTA_ORIGENS[k % len(ALIQUOTA_ORIGENS)] for k in range(15)],
            "id_conta_destino": [ALIQUOTA_DESTINOS[k % len(ALIQUOTA_DESTINOS)] for k in range(15)],
            "data_fim_validade": [None] * 15,
            "data_inicio_validade": [dt.date(2024, 1, 1)] * 15,
            "fator": [ALIQUOTA_FATORES[k % len(ALIQUOTA_FATORES)] for k in range(15)],
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
        "dom_mensuracoes": pa.table({"id_mensuracao": [1, 2, 3], "nome": list(MENSURACOES)}, schema=SCHEMAS["dom_mensuracoes"]),
        "dom_negocios": pa.table(
            {"id_negocio": list(range(1, 8)), "id_segmento": [segment for _, segment in NEGOCIOS], "nome": [name for name, _ in NEGOCIOS]},
            schema=SCHEMAS["dom_negocios"],
        ),
        "dom_segmentos": pa.table({"id_segmento": list(range(1, 6)), "nome": list(SEGMENTOS)}, schema=SCHEMAS["dom_segmentos"]),
        "dom_veiculos": pa.table({"id_veiculo": [1], "nome": ["BNDES"]}, schema=SCHEMAS["dom_veiculos"]),
    }


def build_meta_update_status() -> pa.Table:
    """O registro das cargas: uma linha por tabela sem partição, uma por partição das demais, com a partição em JSON.

    Os ids seguem os 21 da leitura de desenvolvimento e continuam do último, porque a base fictícia tem 16 partições.
    """
    entries: list[tuple[str, str | None]] = [(table, None) for table in ("dom_hierarquias_contas", "dom_veiculos", "dom_mensuracoes", "dom_segmentos", "dom_negocios")]
    for table in ("cad_operacoes", "rel_contrato_operacao", "cad_contratos", "cad_lancamentos"):
        partition = PARTITIONS[table]
        entries.extend((table, json.dumps({partition.source: {"__type__": "date", "value": value}})) for value in partition.values)
    entries.extend((table, None) for table in ("cad_contas", "rel_contas_hierarquias", "cad_aliquotas"))
    ids = [*UPDATE_STATUS_IDS, *range(UPDATE_STATUS_IDS[-1] + 1, UPDATE_STATUS_IDS[-1] + 1 + len(entries) - len(UPDATE_STATUS_IDS))]
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


def build_cad_operacoes() -> dict[str, pa.Table]:
    """As operações de cada data: numéricas de 11 dígitos e ``desemb-<data>-<n>``, com as taxas nulas em bloco."""
    tables = {}
    next_id = 10027979
    for month_index, value in enumerate(PARTITION_VALUES):
        month = dt.date.fromisoformat(value)
        count = ROWS_PER_PARTITION["cad_operacoes"][value]
        rows: dict[str, list] = {name: [] for name in SCHEMAS["cad_operacoes"].names}
        for j in range(count):
            rates_missing = j % 40 == 7
            spread_basico = (1.2, 0.5, 0.8)[j % 3]
            spread_total = round(spread_basico + 0.1, 2)
            taxa_bndes = (1.4, 0.6, 1.0, 1.2, 16.1)[j % 5]
            rows["id_operacao"].append(next_id)
            rows["data"].append(month)
            rows["operacao"].append(f"desemb-{value}-{j:06d}" if j % 4 == 3 else str(10000000001 + month_index * 1000 + j * 97))
            rows["legado"].append(j % 10 == 0)
            rows["area"].append(AREAS_OPERACAO[j % 4])
            rows["departamento"].append(None if j % 3 == 0 else DEPARTAMENTOS[j % 3])
            rows["spread_basico"].append(None if rates_missing else (-0.0 if j % 20 == 5 else spread_basico))
            rows["spread_risco"].append(None if rates_missing else 0.1)
            rows["spread_total"].append(None if rates_missing else spread_total)
            rows["taxa_total"].append(None if rates_missing else round(taxa_bndes + spread_total + (j % 7) * 0.37, 2))
            rows["taxa_bndes"].append(None if rates_missing else taxa_bndes)
            rows["custo_adicional"].append(None if rates_missing and j % 3 != 0 else (14.4, 14.3, 15.3, 0.0, -0.25)[j % 5])
            rows["instrumento_financeiro"].append(None if j % 3 == 0 else INSTRUMENTOS[j % 2])
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
            rows["id_contrato"].append(next_id)
            rows["data"].append(month)
            rows["sistema"].append(SISTEMAS[j % 3])
            rows["contrato"].append(f"desemb-{value}-{j:06d}" if j % 5 == 4 else str(10000495031 + month_index * 10000 + j * 53))
            rows["legado"].append(j % 8 == 0)
            rows["um"].append(UNIDADES[j % len(UNIDADES)])
            rows["to"].append(TOS[j % len(TOS)])
            rows["fonte"].append(FONTES[j % len(FONTES)])
            rows["fonte_familia"].append(None if j % 45 == 44 else FONTE_FAMILIAS[j % 4])
            rows["estagio"].append(None if j % 30 == 29 else (1, 2, 3)[j % 3])
            rows["taxa_juros_fixos"].append((-0.0, 2.5, round(rng.uniform(0.5, 27.5), 4), 27.5)[j % 4])
            rows["data_assinatura"].append(None if j % 13 not in (1, 4, 6, 8, 11, 12) else month - dt.timedelta(days=(j * 37) % 20000))
            rows["data_primeira_amortizacao"].append(first)
            rows["data_ultima_amortizacao"].append(dt.date(2099, 12, 15) if j % 50 == 3 else first + dt.timedelta(days=365 * (j % 20 + 1)))
            next_id += 11
        tables[value] = pa.table(rows, schema=SCHEMAS["cad_contratos"])
    return tables


def build_rel_contrato_operacao(contratos: dict[str, pa.Table], operacoes: dict[str, pa.Table]) -> dict[str, pa.Table]:
    """A relação N×N de cada data: todo contrato numa operação, um em quatro também na seguinte, e os fatores somando 1.

    As linhas saem agrupadas por operação, e ``apportionment`` reparte a operação entre os seus contratos.
    """
    tables = {}
    next_id = 2951753
    for value in PARTITION_VALUES:
        month = dt.date.fromisoformat(value)
        contract_rows = contratos[value].to_pylist()
        operation_names = operacoes[value].column("operacao").to_pylist()
        links: dict[str, list[dict]] = {name: [] for name in operation_names}
        for j, contract in enumerate(contract_rows):
            links[operation_names[j % len(operation_names)]].append(contract)
            if j % 4 == 0:
                links[operation_names[(j + 1) % len(operation_names)]].append(contract)
        rows: dict[str, list] = {name: [] for name in SCHEMAS["rel_contrato_operacao"].names}
        for operation, contracts in links.items():
            for contract, factor in zip(contracts, apportionment(len(contracts)), strict=True):
                rows["id_rel_contrato_operacao"].append(next_id)
                rows["data"].append(month)
                rows["operacao"].append(operation)
                rows["sistema"].append(contract["sistema"])
                rows["contrato"].append(contract["contrato"])
                rows["fator_rateio"].append(factor)
                next_id += 17
        tables[value] = pa.table(rows, schema=SCHEMAS["rel_contrato_operacao"])
    return tables


def build_cad_lancamentos(rng: random.Random, contratos: dict[str, pa.Table], contas: pa.Table) -> dict[str, pa.Table]:
    """Os lançamentos projetados de cada ``data_base``: ``data`` posterior à base, ``valor`` com três casas, ids esparsos.

    O último id é ``MAX_ID_LANCAMENTO``. Cada lançamento cita um contrato de ``cad_contratos`` da mesma data, ou nenhum,
    com ``sistema`` e ``contrato`` nulos juntos (o lançamento associado a uma área).
    """
    total = sum(ROWS_PER_PARTITION["cad_lancamentos"].values())
    ids = iter(1 + round(i * (MAX_ID_LANCAMENTO - 1) / (total - 1)) for i in range(total))
    postable = [row["id_conta"] for row in contas.to_pylist() if row["permite_lancamentos"] and row["id_conta"] >= 8]
    tables = {}
    for partition_index, value in enumerate(PARTITION_VALUES):
        base = dt.date.fromisoformat(value)
        horizon = month_ends_after(base)
        contract_rows = contratos[value].to_pylist()
        count = ROWS_PER_PARTITION["cad_lancamentos"][value]
        rows: dict[str, list] = {name: [] for name in SCHEMAS["cad_lancamentos"].names}
        for j in range(count):
            contract = contract_rows[j % len(contract_rows)]
            no_contract = j % 50 == 21
            if partition_index == 0 and j < 2:
                valor = EXTREME_VALOR if j == 0 else -EXTREME_VALOR
            else:
                valor = round(rng.uniform(-5e9, 5e9), 3)
            rows["id_lancamento"].append(next(ids))
            rows["id_veiculo"].append(1)
            rows["id_conta"].append(postable[j % len(postable)])
            rows["data"].append(horizon[j % len(horizon)])
            rows["valor"].append(valor)
            rows["meta"].append(None)
            rows["timestamp"].append(WRITE_TIMESTAMP + dt.timedelta(days=partition_index, seconds=partition_index * 37))
            rows["id_mensuracao"].append(2)
            rows["id_segmento"].append(None if j % 500 == 137 else 1 + j % 5)
            rows["id_negocio"].append(None if j % 5 < 3 else 1 + j % 7)
            rows["data_base"].append(base)
            rows["sistema"].append(None if no_contract else contract["sistema"])
            rows["contrato"].append(None if no_contract else contract["contrato"])
            rows["area"].append(None if j % 400 == 186 else AREAS_LANCAMENTO[j % 4])
        tables[value] = pa.table(rows, schema=SCHEMAS["cad_lancamentos"])
    return tables


def build_tables() -> dict[str, pa.Table | dict[str, pa.Table]]:
    """Toda a base em memória: uma ``pa.Table`` por tabela sem partição, um dicionário por partição nas demais."""
    rng = random.Random(SEED)
    contas = build_cad_contas()
    operacoes = build_cad_operacoes()
    contratos = build_cad_contratos(rng)
    tables: dict[str, pa.Table | dict[str, pa.Table]] = {
        "alembic_version": pa.table({"version_num": [ALEMBIC_REVISION]}, schema=SCHEMAS["alembic_version"]),
        "cad_aliquotas": build_cad_aliquotas(),
        "cad_contas": contas,
        "cad_contratos": contratos,
        "cad_lancamentos": build_cad_lancamentos(rng, contratos, contas),
        "cad_operacoes": operacoes,
        **build_dimensions(),
        "meta_update_status": build_meta_update_status(),
        "rel_contas_hierarquias": build_rel_contas_hierarquias(contas),
        "rel_contrato_operacao": build_rel_contrato_operacao(contratos, operacoes),
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
