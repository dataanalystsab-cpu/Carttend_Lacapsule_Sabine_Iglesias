-- logistique1_volume_par_entrepot.sql
-- Requête : Volume total traité par entrepôt
-- ------------------------------------------------------------------------------
-- Cette requête permet de :
--   Mesurer la capacité de traitement des entrepôts.
--   Suivre les volumes traités par mois et par site logistique.
--   Identifier les entrepôts les plus performants en termes de flux.
-- ------------------------------------------------------------------------------

SELECT 
  c.id_entrepot,                        -- Identifiant unique de l’entrepôt (clé de référence)
  d.localisation,                       -- Localisation géographique de l’entrepôt
  c.mois,                               -- Mois associé aux données (analyse temporelle)
  SUM(c.volume_traite) AS volume_total_traite   -- Volume total traité par l’entrepôt sur le mois
FROM {{ ref('facts_entrepots_machine') }} AS c  -- Table de faits : infos opérationnelles par machine/entrepôt
JOIN {{ ref('dim_entrepots') }} AS d            -- Table dimension entrepôts (données descriptives)
  ON c.id_entrepot = d.id_entrepot              -- Jointure entre faits et dimension via l’ID entrepôt
GROUP BY 
  c.id_entrepot,   -- Regroupement par entrepôt
  d.localisation,  -- Regroupement par localisation géographique
  c.mois           -- Regroupement par mois
ORDER BY 
  SUM(volume_traite) DESC   -- Tri décroissant pour voir les entrepôts les plus actifs en haut