"""A base gerencial: mensurações, segmentos, negócios, operações, contratos e lançamentos.

Correspondem a ``tests/reference_model/model_base_gerencial.py``, com ``cad_contratos`` já com o
``data_assinatura`` anulável que o original acrescenta em ``model_db_projetado.py`` e
``cad_lancamentos`` inteira, nas 14 colunas em que a base a grava.
"""

from datetime import date, datetime

from sqlalchemy import (
    BigInteger,
    Boolean,
    Date,
    DateTime,
    Double,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
)
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class Mensuracao(Base):
    """``dom_mensuracoes``: realizado, projetado e orçado."""

    __tablename__ = "dom_mensuracoes"
    __table_args__ = {"comment": "Mensurações: realizado, projetado e orçado"}

    id_mensuracao: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, autoincrement=False, comment="Identificador da mensuração"
    )
    nome: Mapped[str] = mapped_column(String(50), unique=True, comment="Nome da mensuração")


class Segmento(Base):
    """``dom_segmentos``: os segmentos de negócio."""

    __tablename__ = "dom_segmentos"
    __table_args__ = {"comment": "Segmentos de negócio"}

    id_segmento: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, autoincrement=False, comment="Identificador do segmento"
    )
    nome: Mapped[str] = mapped_column(String(50), unique=True, comment="Nome do segmento")


class Negocio(Base):
    """``dom_negocios``: os negócios, cada um num segmento."""

    __tablename__ = "dom_negocios"
    __table_args__ = {"comment": "Negócios, cada um num segmento"}

    id_negocio: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, autoincrement=False, comment="Identificador do negócio"
    )
    id_segmento: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("dom_segmentos.id_segmento"), comment="Segmento do negócio"
    )
    nome: Mapped[str] = mapped_column(String(50), unique=True, comment="Nome do negócio")


class Operacao(Base):
    """``cad_operacoes``: as operações de crédito de cada data-base, partição por ``data``."""

    __tablename__ = "cad_operacoes"
    __table_args__ = (
        Index("ix_operacoes_data_operacao", "data", "operacao", unique=True),
        {
            "comment": "Operações de crédito por data-base",
            "info": {
                "serialize_db": {
                    "partition_by": ["data_str"],
                    "partition_source": "data",
                    "sort_key": ["data", "operacao"],
                }
            },
        },
    )

    id_operacao: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, autoincrement=False, comment="Identificador da operação"
    )
    data: Mapped[date] = mapped_column(Date, comment="Data-base da operação")
    operacao: Mapped[str] = mapped_column(String(50), comment="Código da operação")
    legado: Mapped[bool] = mapped_column(Boolean, comment="Se a operação vem do sistema legado")
    area: Mapped[str | None] = mapped_column(String(256), comment="Área responsável")
    departamento: Mapped[str | None] = mapped_column(
        String(20), comment="Departamento responsável"
    )
    spread_basico: Mapped[float | None] = mapped_column(Double, comment="Spread básico")
    spread_risco: Mapped[float | None] = mapped_column(Double, comment="Spread de risco")
    spread_total: Mapped[float | None] = mapped_column(Double, comment="Spread total")
    taxa_total: Mapped[float | None] = mapped_column(Double, comment="Taxa total")
    taxa_bndes: Mapped[float | None] = mapped_column(Double, comment="Taxa BNDES")
    custo_adicional: Mapped[float | None] = mapped_column(Double, comment="Custo adicional")
    instrumento_financeiro: Mapped[str | None] = mapped_column(
        String(256), comment="Instrumento financeiro"
    )
    data_str: Mapped[str] = mapped_column(String(10), comment="Partição: data em AAAA-MM-DD")


class RelContratoOperacao(Base):
    """``rel_contrato_operacao``: as operações de cada contrato, com o fator de rateio."""

    __tablename__ = "rel_contrato_operacao"
    __table_args__ = (
        ForeignKeyConstraint(
            ["data", "operacao"], ["cad_operacoes.data", "cad_operacoes.operacao"]
        ),
        {
            "comment": "Operações de cada contrato, com o fator de rateio",
            "info": {
                "serialize_db": {
                    "partition_by": ["data_str"],
                    "partition_source": "data",
                    "sort_key": ["data", "sistema", "contrato", "operacao"],
                }
            },
        },
    )

    id_rel_contrato_operacao: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, autoincrement=False, comment="Identificador da relação"
    )
    data: Mapped[date] = mapped_column(Date, comment="Data-base")
    operacao: Mapped[str] = mapped_column(String(50), comment="Código da operação")
    sistema: Mapped[int] = mapped_column(Integer, comment="Sistema de origem do contrato")
    contrato: Mapped[str] = mapped_column(String(50), comment="Código do contrato")
    fator_rateio: Mapped[float] = mapped_column(
        Double, comment="Fração do contrato atribuída à operação; soma 1 por contrato"
    )
    data_str: Mapped[str] = mapped_column(String(10), comment="Partição: data em AAAA-MM-DD")


class Contrato(Base):
    """``cad_contratos``: os contratos de cada data-base, particionados por ``data``."""

    __tablename__ = "cad_contratos"
    __table_args__ = (
        Index("ix_contratos_data_sistema_contrato", "data", "sistema", "contrato", unique=True),
        # A chave estrangeira do original, de (data, sistema, contrato) para
        # rel_contrato_operacao, saiu: o destino não é único, porque o contrato está em N
        # operações (decisão do usuário de 2026-09-21).
        {
            "comment": "Contratos por data-base",
            "info": {
                "serialize_db": {
                    "partition_by": ["data_str"],
                    "partition_source": "data",
                    "sort_key": ["data", "sistema", "contrato"],
                }
            },
        },
    )

    id_contrato: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, autoincrement=False, comment="Identificador do contrato"
    )
    data: Mapped[date] = mapped_column(Date, comment="Data-base do contrato")
    sistema: Mapped[int] = mapped_column(Integer, comment="Sistema de origem do contrato")
    contrato: Mapped[str] = mapped_column(String(50), comment="Código do contrato")
    legado: Mapped[bool] = mapped_column(Boolean, comment="Se o contrato vem do sistema legado")
    um: Mapped[int] = mapped_column(Integer, comment="Unidade monetária do contrato")
    to: Mapped[str] = mapped_column(String(2), comment="Código TO do contrato")
    fonte: Mapped[int] = mapped_column(Integer, comment="Código da fonte de recursos")
    fonte_familia: Mapped[str | None] = mapped_column(
        String(3), comment="Família da fonte de recursos (FAT, FMM, FMC, BND)"
    )
    estagio: Mapped[int | None] = mapped_column(Integer, comment="Estágio do contrato, de 1 a 3")
    taxa_juros_fixos: Mapped[float] = mapped_column(Double, comment="Taxa de juros fixos")
    data_assinatura: Mapped[date | None] = mapped_column(Date, comment="Data de assinatura")
    data_primeira_amortizacao: Mapped[date] = mapped_column(
        Date, comment="Data da primeira amortização"
    )
    data_ultima_amortizacao: Mapped[date] = mapped_column(
        Date, comment="Data da última amortização"
    )
    data_str: Mapped[str] = mapped_column(String(10), comment="Partição: data em AAAA-MM-DD")


class Lancamento(Base):
    """``cad_lancamentos``: os lançamentos projetados de cada data-base, particionados por ela."""

    __tablename__ = "cad_lancamentos"
    __table_args__ = (
        ForeignKeyConstraint(
            ["data_base", "sistema", "contrato"],
            ["cad_contratos.data", "cad_contratos.sistema", "cad_contratos.contrato"],
        ),
        {
            "comment": "Lançamentos projetados por data-base",
            "info": {
                "serialize_db": {
                    "partition_by": ["data_base_str"],
                    "partition_source": "data_base",
                    "sort_key": ["data_base", "id_mensuracao", "id_veiculo", "id_conta"],
                }
            },
        },
    )

    id_lancamento: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, autoincrement=False, comment="Identificador do lançamento"
    )
    id_veiculo: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("dom_veiculos.id_veiculo"), comment="Veículo do lançamento"
    )
    id_conta: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("cad_contas.id_conta"), comment="Conta do lançamento"
    )
    data: Mapped[date] = mapped_column(Date, comment="Mês projetado, o fim do mês")
    valor: Mapped[float] = mapped_column(Double, comment="Valor do lançamento")
    meta: Mapped[str | None] = mapped_column(String(255), comment="Meta do lançamento")
    timestamp: Mapped[datetime] = mapped_column(
        DateTime, comment="Instante da gravação do lançamento"
    )
    id_mensuracao: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("dom_mensuracoes.id_mensuracao"),
        comment="Mensuração do lançamento",
    )
    id_segmento: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("dom_segmentos.id_segmento"), comment="Segmento do lançamento"
    )
    id_negocio: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("dom_negocios.id_negocio"), comment="Negócio do lançamento"
    )
    data_base: Mapped[date] = mapped_column(Date, comment="Data-base da projeção")
    sistema: Mapped[int | None] = mapped_column(
        Integer, comment="Sistema de origem do contrato, quando há contrato"
    )
    contrato: Mapped[str | None] = mapped_column(
        String(50), comment="Código do contrato, quando há contrato"
    )
    area: Mapped[str | None] = mapped_column(
        String(256), comment="Área, no lançamento associado a uma área e não a um contrato"
    )
    data_base_str: Mapped[str] = mapped_column(
        String(10), comment="Partição: data_base em AAAA-MM-DD"
    )
