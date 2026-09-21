"""As tabelas da base projetada: as alíquotas entre contas.

Correspondem a ``tests/reference_model/model_db_projetado.py``; o ``data_assinatura`` anulável
que o original acrescenta a ``cad_contratos`` já está em ``model_base_gerencial.py``.
"""

from datetime import date

from sqlalchemy import BigInteger, Date, Double, ForeignKey, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class Aliquota(Base):
    """``cad_aliquotas``: o fator aplicado entre um par de contas num período de vigência."""

    __tablename__ = "cad_aliquotas"
    __table_args__ = (
        UniqueConstraint("id_conta_origem", "id_conta_destino"),
        {"comment": "Alíquotas entre pares de contas"},
    )

    id: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, autoincrement=False, comment="Identificador da alíquota"
    )
    id_conta_origem: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("cad_contas.id_conta"), comment="Conta de origem"
    )
    id_conta_destino: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("cad_contas.id_conta"), comment="Conta de destino"
    )
    data_fim_validade: Mapped[date | None] = mapped_column(Date, comment="Fim da vigência")
    data_inicio_validade: Mapped[date] = mapped_column(Date, comment="Início da vigência")
    fator: Mapped[float] = mapped_column(Double, comment="Fator aplicado")
