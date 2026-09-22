INSERT INTO "{prefix}dom_veiculos" ("id_veiculo", "nome") SELECT DISTINCT "{prefix}cad_lancamentos"."id_veiculo", 'veículo ' || CAST("{prefix}cad_lancamentos"."id_veiculo" AS VARCHAR(20)) AS nome
FROM "{prefix}cad_lancamentos"
WHERE "{prefix}cad_lancamentos"."data_base_str" = :data_base_str AND NOT (EXISTS (SELECT *
FROM "{prefix}dom_veiculos"
WHERE "{prefix}dom_veiculos"."id_veiculo" = "{prefix}cad_lancamentos"."id_veiculo"))
