CREATE TABLE "cad_operacoes" (
    "id_operacao" BIGINT NOT NULL,
    "data" DATE NOT NULL,
    "operacao" VARCHAR(50) NOT NULL,
    "legado" BOOLEAN NOT NULL,
    "area" VARCHAR(256),
    "departamento" VARCHAR(20),
    "spread_basico" DOUBLE PRECISION,
    "spread_risco" DOUBLE PRECISION,
    "spread_total" DOUBLE PRECISION,
    "taxa_total" DOUBLE PRECISION,
    "taxa_bndes" DOUBLE PRECISION,
    "custo_adicional" DOUBLE PRECISION,
    "instrumento_financeiro" VARCHAR(256),
    "data_str" VARCHAR(10) NOT NULL
) SORTKEY ("data", "operacao")
