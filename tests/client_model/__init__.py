"""O modelo cliente: o modelo de dados que o código cliente apresenta ao ``serialize-db``.

É a cópia de ``tests/reference_model/`` (o modelo SQLAlchemy da base original em Parquet
particionado, que fica como está) corrigida como a biblioteca cliente a escreveria para usar o
pacote, segundo ``plan/PLAN-STAGE-1.md``:

- uma ``Base`` só, em ``base.py``, e cada tabela declarada uma vez (o original monta
  ``cad_lancamentos`` e ``cad_contratos`` em dois módulos por ``extend_existing``);
- ``BigInteger`` e ``autoincrement=False`` nas chaves primárias inteiras, e ``BigInteger`` nas
  colunas que as referenciam; os demais inteiros continuam ``Integer``;
- chaves estrangeiras sem ``DEFERRABLE``, inclusive as compostas, que a auditoria verifica, e
  sem a de ``cad_contratos`` para ``rel_contrato_operacao``, cujo destino não é único;
- ``String(n)`` com o comprimento escolhido das leituras da base (``plan/POC.md``), com folga;
- a coluna de partição ``data_str`` (``data_base_str`` em ``cad_lancamentos``), ``String(10)``
  em ``AAAA-MM-DD``, no fim das quatro tabelas particionadas, declarada em
  ``Table.info["serialize_db"]`` com ``partition_by``, ``partition_source`` e ``sort_key``;
- comentários de tabela e de coluna, uma primeira redação que o dono do modelo revisa no
  código;
- as colunas numéricas continuam ``Double`` (decisão de 2026-09-20).

Os índices não únicos e o ``sqlite_strict`` do original ficam de fora: nenhum motor da biblioteca
os usa. ``tests/test_client_model.py`` confere a cópia contra o original. ``statements.py`` traz
os statements Core do pipeline fictício (``STATEMENTS``) e ``sql/`` os arquivos de texto SQL
gerados deles pela etapa 2.

Exemplo::

    from client_model import Base

    tables = Base.metadata.tables                       # as 12 tabelas, na ordem de declaração
    options = tables["cad_lancamentos"].info["serialize_db"]
    options["partition_by"], options["partition_source"]   # (["data_base_str"], "data_base")
"""

from .base import Base
from .model_base_contabil import Conta, HierarquiaContas, RelacionamentoContaHierarquia, Veiculo
from .model_base_gerencial import (
    Contrato,
    Lancamento,
    Mensuracao,
    Negocio,
    Operacao,
    RelContratoOperacao,
    Segmento,
)
from .model_db_projetado import Aliquota

__all__ = [
    "Aliquota",
    "Base",
    "Conta",
    "Contrato",
    "HierarquiaContas",
    "Lancamento",
    "Mensuracao",
    "Negocio",
    "Operacao",
    "RelContratoOperacao",
    "RelacionamentoContaHierarquia",
    "Segmento",
    "Veiculo",
]
