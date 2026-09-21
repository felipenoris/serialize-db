CREATE TABLE "cad_lancamentos" (
    "id_lancamento" BIGINT NOT NULL,
    "id_veiculo" BIGINT NOT NULL,
    "id_conta" BIGINT NOT NULL,
    "data" DATE NOT NULL,
    "valor" DOUBLE PRECISION NOT NULL,
    "meta" VARCHAR(255),
    "timestamp" TIMESTAMP NOT NULL,
    "id_mensuracao" BIGINT NOT NULL,
    "id_segmento" BIGINT,
    "id_negocio" BIGINT,
    "data_base" DATE NOT NULL,
    "sistema" INTEGER,
    "contrato" VARCHAR(50),
    "area" VARCHAR(20),
    "data_base_str" VARCHAR(10) NOT NULL
) SORTKEY ("data_base", "id_mensuracao", "id_veiculo", "id_conta")
