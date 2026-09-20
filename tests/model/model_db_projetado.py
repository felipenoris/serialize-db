from datetime import date
from sqlalchemy import Double, ForeignKey, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
from lib_base_gerencial import Base


class Contrato(Base):
    __tablename__: str = 'cad_contratos'

    data_assinatura: Mapped[date] = mapped_column(nullable=True)  #modifica modelo da lib_base_gerencial para tornar esse campo anulavel

    __table_args__ = (
        {"extend_existing": True}
    )


class Aliquota(Base):
    __tablename__: str = 'cad_aliquotas'

    id: Mapped[int] = mapped_column(primary_key=True)
    id_conta_origem: Mapped[int] = mapped_column(ForeignKey("cad_contas.id_conta"))
    id_conta_destino: Mapped[int] = mapped_column(ForeignKey("cad_contas.id_conta"))

    data_fim_validade: Mapped[date] = mapped_column(nullable=True)
    data_inicio_validade: Mapped[date] = mapped_column()
    fator: Mapped[float] = mapped_column(Double)

    __table_args__ = (
        UniqueConstraint(id_conta_origem, id_conta_destino),
    )
