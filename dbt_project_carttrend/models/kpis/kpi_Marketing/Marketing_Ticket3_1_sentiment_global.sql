-- Marketing_Ticket3_1_sentiment_global.sql
-- Requête : Analyse des volumes de mentions sociales par jour et par sentiment
--------------------------------------------------------------------------------
-- Cette requête permet de suivre l’évolution quotidienne des messages sur les réseaux sociaux
-- en les classant par tonalité (positif, neutre, négatif).
-- Utile pour détecter les pics d’activité ou les crises de réputation.
-- -------------------------------------------------------------------------------

SELECT
  d.date,                                   -- Date du message
  EXTRACT(YEAR FROM d.date) AS annee,       -- Année extraite de la date
  EXTRACT(MONTH FROM d.date) AS mois,       -- Mois extrait de la date
  c.nom_canal,                              -- Canal (réseau social, plateforme, etc.)
  COUNT(*) AS total_mentions,               -- Nombre total de messages ce jour-là
  SUM(                                      -- Comptage des mentions positives
    CASE WHEN LOWER(TRIM(p.sentiment_global)) = 'positif' 
         THEN 1 ELSE 0 END
  ) AS nb_positif,
  SUM(                                      -- Comptage des mentions neutres
    CASE WHEN LOWER(TRIM(p.sentiment_global)) = 'neutre' 
         THEN 1 ELSE 0 END
  ) AS nb_neutre,
  SUM(                                      -- Comptage des mentions négatives
    CASE WHEN LOWER(TRIM(p.sentiment_global)) = 'négatif' 
         THEN 1 ELSE 0 END
  ) AS nb_negatif
FROM {{ ref('facts_posts') }} AS p          -- Faits : messages publiés
JOIN {{ ref('dim_date') }} AS d             -- Dimension : date
  ON p.id_date = d.id_date
JOIN {{ ref('dim_canal') }} AS c            -- Dimension : canal
  ON p.id_canal = c.id_canal
GROUP BY d.date, c.nom_canal                -- Regroupement par jour et canal
ORDER BY d.date ASC, c.nom_canal            -- Résultats classés chronologiquement