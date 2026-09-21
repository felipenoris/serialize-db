CREATE TABLE "rel_contrato_operacao" (
    "id_rel_contrato_operacao" BIGINT NOT NULL,
    "data" DATE NOT NULL,
    "operacao" VARCHAR(50) NOT NULL,
    "sistema" INTEGER NOT NULL,
    "contrato" VARCHAR(50) NOT NULL,
    "fator_rateio" DOUBLE PRECISION NOT NULL,
    "data_str" VARCHAR(10) NOT NULL
) SORTKEY ("data", "sistema", "contrato", "operacao")
