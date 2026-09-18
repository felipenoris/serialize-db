from datetime import date, datetime
from sqlalchemy import Double, ForeignKey, ForeignKeyConstraint, Index
from sqlalchemy.orm import Mapped, mapped_column
from lib_base_contabil import Base


class Mensuracao(Base):
    __tablename__: str = 'dom_mensuracoes'

    id_mensuracao: Mapped[int] = mapped_column(primary_key=True)
    nome: Mapped[str] = mapped_column(unique=True)


class Segmento(Base):
    __tablename__: str = 'dom_segmentos'

    id_segmento: Mapped[int] = mapped_column(primary_key=True)
    nome: Mapped[str] = mapped_column(unique=True)


class Negocio(Base):
    __tablename__: str = 'dom_negocios'

    id_negocio: Mapped[int] = mapped_column(primary_key=True)
    id_segmento: Mapped[int] = mapped_column(ForeignKey("dom_segmentos.id_segmento", deferrable=True, initially='DEFERRED'))
    nome: Mapped[str] = mapped_column(unique=True)


class Operacao(Base):
    __tablename__: str = 'cad_operacoes'

    id_operacao: Mapped[int] = mapped_column(primary_key=True)

    data: Mapped[date] = mapped_column()
    operacao: Mapped[str] = mapped_column(index=True)
    legado: Mapped[bool] = mapped_column()
    area: Mapped[str] = mapped_column(nullable=True)
    departamento: Mapped[str] = mapped_column(nullable=True)
    spread_basico: Mapped[float] = mapped_column(Double, nullable=True)
    spread_risco: Mapped[float] = mapped_column(Double, nullable=True)
    spread_total: Mapped[float] = mapped_column(Double, nullable=True)
    taxa_total: Mapped[float] = mapped_column(Double, nullable=True)
    taxa_bndes: Mapped[float] = mapped_column(Double, nullable=True)
    custo_adicional: Mapped[float] = mapped_column(Double, nullable=True)
    instrumento_financeiro: Mapped[str] = mapped_column(nullable=True)

    __table_args__ = (
        Index('ix_operacoes_data_operacao', data, operacao, unique=True),
    )


class RelContratoOperacao(Base):
    __tablename__: str = 'rel_contrato_operacao'

    id_rel_contrato_operacao: Mapped[int] = mapped_column(primary_key=True)

    data: Mapped[date] = mapped_column()
    operacao: Mapped[str] = mapped_column()
    sistema: Mapped[int] = mapped_column()
    contrato: Mapped[str] = mapped_column()
    fator_rateio: Mapped[float] = mapped_column(Double)

    __table_args__ = (
        Index('ix_rel_contratos_operacoes_data_sistema_contrato', data, sistema, contrato),
        Index('ix_rel_contratos_operacoes_data_operacao', data, operacao),
        ForeignKeyConstraint(
            [data, operacao],
            ['cad_operacoes.data', 'cad_operacoes.operacao'],
            deferrable=True,
            initially='DEFERRED'
        )
    )


class Contrato(Base):
    __tablename__: str = 'cad_contratos'

    id_contrato: Mapped[int] = mapped_column(primary_key=True)

    data: Mapped[date] = mapped_column()
    sistema: Mapped[int] = mapped_column()
    contrato: Mapped[str] = mapped_column()
    legado: Mapped[bool] = mapped_column()
    um: Mapped[int] = mapped_column()
    to: Mapped[str] = mapped_column()
    fonte: Mapped[int] = mapped_column()
    fonte_familia: Mapped[str] = mapped_column(nullable=True)
    estagio: Mapped[int] = mapped_column(nullable=True)
    taxa_juros_fixos: Mapped[float] = mapped_column(Double)
    data_assinatura: Mapped[date] = mapped_column()
    data_primeira_amortizacao: Mapped[date] = mapped_column()
    data_ultima_amortizacao: Mapped[date] = mapped_column()

    __table_args__ = (
        Index('ix_contratos_data_sistema_contrato', data, sistema, contrato, unique=True),
        Index('ix_contratos_sistema_contrato', sistema, contrato),
        ForeignKeyConstraint(
            [data, sistema, contrato],
            ['rel_contrato_operacao.data', 'rel_contrato_operacao.sistema', 'rel_contrato_operacao.contrato'],
            deferrable=True,
            initially='DEFERRED'
        )
    )


class Lancamento(Base):
    __tablename__: str = 'cad_lancamentos'

    id_lancamento: Mapped[int] = mapped_column(primary_key=True)

    id_mensuracao: Mapped[int] = mapped_column(ForeignKey("dom_mensuracoes.id_mensuracao", deferrable=True, initially='DEFERRED'))
    id_veiculo: Mapped[int] = mapped_column(ForeignKey("dom_veiculos.id_veiculo", deferrable=True, initially='DEFERRED'))
    id_conta: Mapped[int] = mapped_column(ForeignKey("cad_contas.id_conta", deferrable=True, initially='DEFERRED'))
    id_segmento: Mapped[int] = mapped_column(ForeignKey("dom_segmentos.id_segmento", deferrable=True, initially='DEFERRED'), nullable=True)
    id_negocio: Mapped[int] = mapped_column(ForeignKey("dom_negocios.id_negocio", deferrable=True, initially='DEFERRED'), nullable=True)

    data: Mapped[date] = mapped_column()
    data_base: Mapped[date] = mapped_column()
    sistema: Mapped[int | None] = mapped_column(nullable=True)
    contrato: Mapped[str | None] = mapped_column(nullable=True)
    area: Mapped[str] = mapped_column(nullable=True)  # area aqui permite lancamentos que sejam associados a area, mas nao a contrato
    valor: Mapped[float] = mapped_column(Double)

    timestamp: Mapped[datetime] = mapped_column()

    __table_args__ = (
        Index('ix_cad_lancamentos', data_base, id_mensuracao, id_conta, id_segmento, area, id_veiculo, id_negocio, data),
        ForeignKeyConstraint(
            [data_base, sistema, contrato],
            ['cad_contratos.data', 'cad_contratos.sistema', 'cad_contratos.contrato'],
            deferrable=True,
            initially='DEFERRED'
        ),
        {"extend_existing": True}
    )
