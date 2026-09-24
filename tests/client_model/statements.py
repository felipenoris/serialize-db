"""Os statements Core do pipeline fictício: ``STATEMENTS``, o dicionário ``{nome: statement}``.

O caminho padrão do pipeline submete cada statement ao motor, que o compila pela cópia prefixada
com os parâmetros do cliente; ``serialize-db sql`` recebe o mesmo dicionário e gera o texto SQL
versionado, a opção de migração para fora do SQLAlchemy.

Cada entrada é ``{nome: statement}``, e o nome vira ``sql/<nome>.duckdb.sql`` e
``sql/<nome>.redshift.sql``, os arquivos versionados em ``tests/client_model/sql/``. Os quatro
statements são os de um pipeline de exemplo sobre o modelo cliente, com o que o texto gerado tem
de tratar: a partição de referência como parâmetro (``sa.bindparam("data_base_str")``, o mesmo
statement que roda num ``sqlalchemy.Connection`` do cliente), um literal
com ``%`` e outro com ``:``, as colunas ``to`` e ``timestamp``, palavras reservadas dos motores,
um ``CAST``, e um ``INSERT ... SELECT`` cujo alvo aparece de novo numa subconsulta.

- ``saldos_por_conta``: o saldo de cada conta que permite lançamentos na partição.
- ``rateio_por_operacao``: o valor dos lançamentos de cada contrato rateado entre as operações
  dele por ``fator_rateio``, na partição, só na área de TI.
- ``lancamentos_por_contrato``: os lançamentos da partição com o contrato, o prazo (``to``) e o
  carimbo (``timestamp``), o valor em centavos por ``CAST``.
- ``veiculos_novos``: os veículos que os lançamentos da partição citam e ``dom_veiculos`` ainda
  não tem, inseridos com um nome provisório.

Exemplo::

    from client_model import Base
    from client_model.statements import STATEMENTS
    from serialize_db import sql

    print(sql.render(STATEMENTS["saldos_por_conta"], "duckdb", Base.metadata))
"""

import sqlalchemy as sa

from .base import Base

TABLES = Base.metadata.tables
ENTRIES = TABLES["cad_lancamentos"]
ACCOUNTS = TABLES["cad_contas"]
CONTRACTS = TABLES["cad_contratos"]
APPORTIONMENTS = TABLES["rel_contrato_operacao"]
VEHICLES = TABLES["dom_veiculos"]

# A partição de referência, o parâmetro de toda execução; o tipo é o da coluna de partição. O
# bindparam sem valor sai como :nome em render e recebe o valor no Connection e nos motores.
PARTITION = sa.bindparam("data_base_str", type_=sa.String(10))

# O saldo de cada conta que permite lançamentos, na partição de referência.
BALANCE_BY_ACCOUNT = (
    sa.select(ACCOUNTS.c.numero, ACCOUNTS.c.nome, sa.func.sum(ENTRIES.c.valor).label("saldo"))
    .join_from(ENTRIES, ACCOUNTS, ENTRIES.c.id_conta == ACCOUNTS.c.id_conta)
    .where(ENTRIES.c.data_base_str == PARTITION, ACCOUNTS.c.permite_lancamentos)
    .group_by(ACCOUNTS.c.numero, ACCOUNTS.c.nome)
    .order_by(ACCOUNTS.c.numero)
)

# O valor dos lançamentos de cada contrato repartido entre as operações dele por fator_rateio,
# na área de TI (o literal com %); o filtro area != '1:2', sempre verdadeiro ao lado do LIKE, existe
# para o texto gerado ter um literal com :.
APPORTIONMENT_BY_OPERATION = (
    sa.select(
        APPORTIONMENTS.c.operacao,
        sa.func.sum(ENTRIES.c.valor * APPORTIONMENTS.c.fator_rateio).label("valor_rateado"),
    )
    .join_from(
        ENTRIES,
        APPORTIONMENTS,
        sa.and_(
            ENTRIES.c.sistema == APPORTIONMENTS.c.sistema,
            ENTRIES.c.contrato == APPORTIONMENTS.c.contrato,
            ENTRIES.c.data_base_str == APPORTIONMENTS.c.data_str,
        ),
    )
    .where(
        ENTRIES.c.data_base_str == PARTITION,
        ENTRIES.c.area.like("TI%"),
        ENTRIES.c.area != "1:2",
    )
    .group_by(APPORTIONMENTS.c.operacao)
    .order_by(APPORTIONMENTS.c.operacao)
)

# Os lançamentos da partição com o contrato: as colunas `to` e `timestamp` são palavras reservadas
# dos motores, e o valor sai em centavos por CAST.
ENTRIES_BY_CONTRACT = (
    sa.select(
        CONTRACTS.c.contrato,
        CONTRACTS.c.to,
        ENTRIES.c.timestamp,
        sa.cast(ENTRIES.c.valor, sa.Numeric(18, 2)).label("valor_centavos"),
    )
    .join_from(
        ENTRIES,
        CONTRACTS,
        sa.and_(
            ENTRIES.c.sistema == CONTRACTS.c.sistema,
            ENTRIES.c.contrato == CONTRACTS.c.contrato,
            ENTRIES.c.data_base_str == CONTRACTS.c.data_str,
        ),
    )
    .where(ENTRIES.c.data_base_str == PARTITION)
    .order_by(CONTRACTS.c.contrato, ENTRIES.c.id_lancamento)
)

# Os veículos que os lançamentos da partição citam e a dimensão ainda não tem, com um nome
# provisório; o alvo do INSERT aparece de novo na subconsulta do NOT EXISTS.
PROVISIONAL_NAME = sa.literal("veículo ") + sa.cast(ENTRIES.c.id_veiculo, sa.String(20))
CITED_VEHICLES = (
    sa.select(ENTRIES.c.id_veiculo, PROVISIONAL_NAME.label("nome"))
    .where(
        ENTRIES.c.data_base_str == PARTITION,
        ~sa.exists().where(VEHICLES.c.id_veiculo == ENTRIES.c.id_veiculo),
    )
    .distinct()
)
NEW_VEHICLES = sa.insert(VEHICLES).from_select(["id_veiculo", "nome"], CITED_VEHICLES)

STATEMENTS = {
    "saldos_por_conta": BALANCE_BY_ACCOUNT,
    "rateio_por_operacao": APPORTIONMENT_BY_OPERATION,
    "lancamentos_por_contrato": ENTRIES_BY_CONTRACT,
    "veiculos_novos": NEW_VEHICLES,
}
