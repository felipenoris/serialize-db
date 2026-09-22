SELECT "{prefix}cad_contas"."numero", "{prefix}cad_contas"."nome", sum("{prefix}cad_lancamentos"."valor") AS saldo
FROM "{prefix}cad_lancamentos" JOIN "{prefix}cad_contas" ON "{prefix}cad_lancamentos"."id_conta" = "{prefix}cad_contas"."id_conta"
WHERE "{prefix}cad_lancamentos"."data_base_str" = :data_base_str AND "{prefix}cad_contas"."permite_lancamentos" GROUP BY "{prefix}cad_contas"."numero", "{prefix}cad_contas"."nome" ORDER BY "{prefix}cad_contas"."numero"
