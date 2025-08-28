-- Marketing_Ticket1_2_CPC_CPA_canal.sql
-- Requête :Analyse des performances des campagnes marketing par canal
-- -------------------------------------------------------------------------------
-- Cette requête permet de:
-- Agréger le budget, les clics et les conversions par canal afin d’évaluer
-- leur efficacité. La requête calcule également deux indicateurs clés :
--   - CPC (Coût par Clic)
--   - CPA (Coût par Acquisition)
-- Ces KPI permettent de comparer le rendement des investissements
-- marketing en fonction des canaux utilisés.
-- -------------------------------------------------------------------------------

SELECT
  dc.nom_canal AS canal,                              -- Canal marketing (Email, Social Media, etc.)
  SUM(fc.budget) AS budget_total,                     -- Budget total investi
  SUM(fc.clics) AS total_clics,                       -- Total des clics générés
  SUM(fc.conversions) AS total_conversions,           -- Total des conversions générées
  ROUND(SUM(fc.budget) / NULLIF(SUM(fc.clics), 0), 2) AS cpc, -- Coût par clic
  ROUND(SUM(fc.budget) / NULLIF(SUM(fc.conversions), 0), 2) AS cpa -- Coût par conversion
FROM {{ ref('facts_campaigns') }} AS fc               -- Table factuelle des campagnes
JOIN {{ ref('dim_canal') }} AS dc                     -- Dimension des canaux marketing
  ON fc.id_canal_dim_canal = dc.id_canal
GROUP BY dc.nom_canal                                 -- Regroupement par canal
ORDER BY budget_total DESC                            --  Tri décroissant sur le budget investi