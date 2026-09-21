CREATE TABLE "cad_aliquotas" (
    "id" BIGINT NOT NULL,
    "id_conta_origem" BIGINT NOT NULL,
    "id_conta_destino" BIGINT NOT NULL,
    "data_fim_validade" DATE,
    "data_inicio_validade" DATE NOT NULL,
    "fator" DOUBLE NOT NULL
)
