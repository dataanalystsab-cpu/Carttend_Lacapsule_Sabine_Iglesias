-- Marketing_Ticket3_2b_corr_pics_mentions_satisfaction.sql
-- Requête : Détection des pics de mentions sociales et corrélation avec ventes & satisfaction
-- --------------------------------------------------------------------------------
-- Cette requête permet de:
--   1. Identifier les jours "anormaux" de mentions sociales (moyenne + 2σ).
--   2. Comparer ces jours aux ventes quotidiennes.
--   3. Ajouter la satisfaction client moyenne par jour.
-- Utile pour analyser l'impact de la notoriété ou des crises sur les ventes et la satisfaction.
-- --------------------------------------------------------------------------------

WITH mentions_stats AS (                                      -- CTE 1 : volumes de mentions par jour
  SELECT
    d1.date AS date_jour,                                    -- Jour calendaire
    SUM(f.volume_mentions) AS total_mentions                 -- Total mentions sociales sur ce jour
  FROM {{ ref('facts_posts') }} f                            -- Table des posts sociaux
  JOIN {{ ref('dim_date') }} d1 ON d1.id_date = f.id_date    -- Jointure pour retrouver la date réelle
  GROUP BY d1.date                                           -- Une ligne par jour
),

stats_calc AS (                                              -- CTE 2 : calcul des stats globales
  SELECT
    AVG(total_mentions) AS moyenne_mentions,                 -- Moyenne journalière des mentions
    STDDEV(total_mentions) AS ecart_type_mentions            -- Écart-type des mentions
  FROM mentions_stats
),

mentions_avec_pic AS (                                       -- CTE 3 : détection des jours "pics"
  SELECT
    m.date_jour,                                             -- Jour
    m.total_mentions,                                        -- Mentions totales ce jour
    CASE                                                     -- Détection statistique d’anomalie
      WHEN m.total_mentions > s.moyenne_mentions + 2 * s.ecart_type_mentions THEN 1 -- Pic = au-dessus du seuil
      ELSE 0
    END AS pic_mention                                       -- Flag 1/0
  FROM mentions_stats m
  CROSS JOIN stats_calc s                                    -- Applique moyenne/σ à chaque jour
),

ventes_par_jour AS (                                         -- CTE 4 : volumes de ventes par jour
  SELECT
    d.date AS date_jour,                                     -- Jour de commande
    SUM(dc.quantite) AS volume_ventes                        -- Total d’unités vendues
  FROM {{ ref('dim_details_commandes') }} dc                 -- Détails de commande
  JOIN {{ ref('facts_commandes') }} c ON dc.id_commande = c.id_commande  -- Lien avec entêtes
  JOIN {{ ref('dim_date') }} d ON c.id_date_commande = d.id_date          -- Date de commande
  WHERE LOWER(c.statut_commande) NOT IN ('annulée', 'cancelled')          -- Exclure commandes annulées
  GROUP BY d.date
),

satisfaction_par_jour AS (                                   -- CTE 5 : satisfaction client par jour
  SELECT
    d.date AS date_jour,                                     -- Jour de commande
    ROUND(AVG(s.note_client), 2) AS note_moyenne             -- Moyenne des notes clients arrondie
  FROM {{ ref('dim_satisfaction') }} s                       -- Dimension satisfaction (notes données)
  JOIN {{ ref('facts_commandes') }} c ON s.id_commande = c.id_commande
  JOIN {{ ref('dim_date') }} d ON c.id_date_commande = d.id_date
  WHERE LOWER(c.statut_commande) NOT IN ('annulée', 'cancelled')          -- Exclure commandes annulées
  GROUP BY d.date
)

SELECT
  m.date_jour,                                               -- Jour analysé
  EXTRACT(YEAR FROM m.date_jour) AS annee,                   -- Année extraite
  EXTRACT(MONTH FROM m.date_jour) AS mois,                   -- Mois extrait
  m.total_mentions,                                          -- Volume de mentions sociales
  m.pic_mention,                                             -- Indicateur de pic (1/0)
  COALESCE(v.volume_ventes, 0) AS volume_ventes,             -- Ventes du jour (0 si aucune donnée)
  COALESCE(s.note_moyenne, NULL) AS satisfaction_moyenne     -- Satisfaction client moyenne
FROM mentions_avec_pic m
LEFT JOIN ventes_par_jour v ON m.date_jour = v.date_jour
LEFT JOIN satisfaction_par_jour s ON m.date_jour = s.date_jour
ORDER BY m.date_jour ASC                                     -- Résultat en ordre chronologique