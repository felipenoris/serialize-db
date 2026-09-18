from datetime import date, datetime
from sqlalchemy import Double, ForeignKey, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Veiculo(Base):
    __tablename__: str = 'dom_veiculos'

    id_veiculo: Mapped[int] = mapped_column(primary_key=True)
    nome: Mapped[str] = mapped_column(unique=True)


class HierarquiaContas(Base):
    __tablename__: str = 'dom_hierarquias_contas'

    id_hierarquia: Mapped[int] = mapped_column(primary_key=True)
    nome: Mapped[str] = mapped_column(unique=True)
    descricao: Mapped[str | None] = mapped_column()


class Conta(Base):
    __tablename__: str = 'cad_contas'

    id_conta: Mapped[int] = mapped_column(primary_key=True)
    nome: Mapped[str] = mapped_column()
    numero: Mapped[str] = mapped_column(unique=True)
    permite_lancamentos: Mapped[bool] = mapped_column(default=True)


class RelacionamentoContaHierarquia(Base):
    __tablename__: str = 'rel_contas_hierarquias'

    id_rel_conta_hierarquia: Mapped[int] = mapped_column(primary_key=True)
    id_hierarquia: Mapped[int] = mapped_column(ForeignKey("dom_hierarquias_contas.id_hierarquia", deferrable=True, initially='DEFERRED'), index=True)

    id_parent: Mapped[int] = mapped_column(ForeignKey("cad_contas.id_conta", deferrable=True, initially='DEFERRED'), index=True)
    id_child: Mapped[int] = mapped_column(ForeignKey("cad_contas.id_conta", deferrable=True, initially='DEFERRED'), index=True)

    __table_args__ = (
        UniqueConstraint(id_hierarquia, id_child),
        {'sqlite_strict': True}
    )


class Lancamento(Base):
    __tablename__: str = 'cad_lancamentos'

    id_lancamento: Mapped[int] = mapped_column(primary_key=True)

    id_veiculo: Mapped[int] = mapped_column(ForeignKey("dom_veiculos.id_veiculo", deferrable=True, initially='DEFERRED'))
    id_conta: Mapped[int] = mapped_column(ForeignKey("cad_contas.id_conta", deferrable=True, initially='DEFERRED'))

    data: Mapped[date] = mapped_column(index=True)
    valor: Mapped[float] = mapped_column(Double)
    meta: Mapped[str] = mapped_column(nullable=True)

    timestamp: Mapped[datetime] = mapped_column()
