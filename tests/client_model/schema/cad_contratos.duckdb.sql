CREATE TABLE "cad_contratos" (
    "id_contrato" BIGINT NOT NULL,
    "data" DATE NOT NULL,
    "sistema" INTEGER NOT NULL,
    "contrato" VARCHAR(50) NOT NULL,
    "legado" BOOLEAN NOT NULL,
    "um" INTEGER NOT NULL,
    "to" VARCHAR(2) NOT NULL,
    "fonte" INTEGER NOT NULL,
    "fonte_familia" VARCHAR(3),
    "estagio" INTEGER,
    "taxa_juros_fixos" DOUBLE NOT NULL,
    "data_assinatura" DATE,
    "data_primeira_amortizacao" DATE NOT NULL,
    "data_ultima_amortizacao" DATE NOT NULL,
    "data_str" VARCHAR(10) NOT NULL
)
