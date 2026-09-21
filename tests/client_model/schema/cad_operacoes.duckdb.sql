CREATE TABLE "cad_operacoes" (
    "id_operacao" BIGINT NOT NULL,
    "data" DATE NOT NULL,
    "operacao" VARCHAR(50) NOT NULL,
    "legado" BOOLEAN NOT NULL,
    "area" VARCHAR(20),
    "departamento" VARCHAR(20),
    "spread_basico" DOUBLE,
    "spread_risco" DOUBLE,
    "spread_total" DOUBLE,
    "taxa_total" DOUBLE,
    "taxa_bndes" DOUBLE,
    "custo_adicional" DOUBLE,
    "instrumento_financeiro" VARCHAR(100),
    "data_str" VARCHAR(10) NOT NULL
)
