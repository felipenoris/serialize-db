"""A base contábil: o veículo, as hierarquias de contas, as contas e a relação entre elas.

Correspondem a ``tests/reference_model/model_base_contabil.py``. ``cad_lancamentos``, que o
original começa aqui e completa na base gerencial, está inteira em ``model_base_gerencial.py``.
"""

from sqlalchemy import BigInteger, Boolean, ForeignKey, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class Veiculo(Base):
    """``dom_veiculos``: a instituição a que os lançamentos pertencem."""

    __tablename__ = "dom_veiculos"
    __table_args__ = {"comment": "Veículos: a instituição a que os lançamentos pertencem"}

    id_veiculo: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, autoincrement=False, comment="Identificador do veículo"
    )
    nome: Mapped[str] = mapped_column(String(50), unique=True, comment="Nome do veículo")


class HierarquiaContas(Base):
    """``dom_hierarquias_contas``: as hierarquias em que as contas se organizam."""

    __tablename__ = "dom_hierarquias_contas"
    __table_args__ = {"comment": "Hierarquias de contas"}

    id_hierarquia: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, autoincrement=False, comment="Identificador da hierarquia"
    )
    nome: Mapped[str] = mapped_column(String(50), unique=True, comment="Nome da hierarquia")
    descricao: Mapped[str | None] = mapped_column(String(255), comment="Descrição da hierarquia")


class Conta(Base):
    """``cad_contas``: o plano de contas."""

    __tablename__ = "cad_contas"
    __table_args__ = {"comment": "Plano de contas"}

    id_conta: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, autoincrement=False, comment="Identificador da conta"
    )
    nome: Mapped[str] = mapped_column(String(100), comment="Nome da conta")
    numero: Mapped[str] = mapped_column(
        String(20), unique=True, comment="Número da conta no plano"
    )
    permite_lancamentos: Mapped[bool] = mapped_column(
        Boolean, default=True, comment="Se a conta recebe lançamentos"
    )


class RelacionamentoContaHierarquia(Base):
    """``rel_contas_hierarquias``: a relação pai-filho entre contas dentro de uma hierarquia."""

    __tablename__ = "rel_contas_hierarquias"
    __table_args__ = (
        UniqueConstraint("id_hierarquia", "id_child"),
        {"comment": "Relação pai-filho entre contas dentro de uma hierarquia"},
    )

    id_rel_conta_hierarquia: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, autoincrement=False, comment="Identificador da relação"
    )
    id_hierarquia: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("dom_hierarquias_contas.id_hierarquia"),
        comment="Hierarquia a que a relação pertence",
    )
    id_parent: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("cad_contas.id_conta"), comment="Conta pai"
    )
    id_child: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("cad_contas.id_conta"), comment="Conta filha"
    )
