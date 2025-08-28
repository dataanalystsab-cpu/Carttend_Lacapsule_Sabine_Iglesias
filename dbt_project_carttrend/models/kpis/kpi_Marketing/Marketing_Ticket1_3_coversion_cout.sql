-- Marketing_Ticket1_3_coversion_cout.sql
-- Requête : Analyse du coût par acquisition (CPA) par canal marketing
-- -------------------------------------------------------------------------------
-- Cette requête permet de calculer le budget total et le nombre total de conversions par canal.
-- Elle calcule ensuite le coût par conversion (CPA) pour mesurer l'efficacité des dépenses.
-- Seuls les canaux ayant généré au moins une conversion sont conservés.
-- Le résultat est trié par CPA croissant (canaux les plus efficaces en premier),
-- puis par nombre de conversions décroissant.
-- -------------------------------------------------------------------------------

SELECT
  dc.nom_canal AS canal,                               -- Canal marketing analysé (nom lisible)
  SUM(fc.budget) AS budget_total,                      -- Budget total dépensé par canal
  SUM(fc.conversions) AS total_conversions,            -- Nombre total de conversions
  ROUND(SUM(fc.budget) / NULLIF(SUM(fc.conversions), 0), 2) AS cpa  -- Coût par conversion
FROM {{ ref('facts_campaigns') }} AS fc
JOIN {{ ref('dim_canal') }} AS dc
  ON fc.id_canal_dim_canal = dc.id_canal               -- Jointure correcte via clé étrangère
GROUP BY dc.nom_canal
HAVING total_conversions > 0                           -- On ne garde que les canaux qui ont converti
ORDER BY cpa ASC, total_conversions DESC               -- On privilégie les canaux efficaces