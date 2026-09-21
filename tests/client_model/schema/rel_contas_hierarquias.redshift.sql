CREATE TABLE "rel_contas_hierarquias" (
    "id_rel_conta_hierarquia" BIGINT NOT NULL,
    "id_hierarquia" BIGINT NOT NULL,
    "id_parent" BIGINT NOT NULL,
    "id_child" BIGINT NOT NULL
)
