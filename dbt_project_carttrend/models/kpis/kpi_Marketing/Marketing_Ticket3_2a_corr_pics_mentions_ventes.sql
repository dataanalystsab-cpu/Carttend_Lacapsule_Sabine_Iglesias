-- Marketing_Ticket3_2a_corr_pics_mentions_ventes.sql
-- Requête : Pics de mentions sociales et rapprochement avec les ventes quotidiennes
--------------------------------------------------------------------------------
-- Cette requête permet de détecter les jours anormaux (pics = moyenne + 2σ) de mentions sociales
-- puis les comparer aux volumes de ventes du même jour.
-- -------------------------------------------------------------------------------

WITH mentions_stats AS (                                  -- CTE 1 : volumes de mentions par jour
  SELECT
    d1.date AS date_jour,                                 -- Jour calendaire (clé lisible)
    SUM(f.volume_mentions) AS total_mentions              -- Total des mentions toutes sources ce jour-là
  FROM {{ ref('facts_posts') }} f                         -- Faits des posts / mentions sociales
  JOIN {{ ref('dim_date') }} d1                           -- Dimension date (permet d’obtenir la date réelle)
    ON d1.id_date = f.id_date                             -- Jointure sur l’identifiant de date
  GROUP BY d1.date                                        -- Agrégation : une ligne par jour
),                                                        -- Fin CTE 1

stats_calc AS (                                           -- CTE 2 : statistiques globales sur les mentions
  SELECT
    AVG(total_mentions) AS moyenne_mentions,              -- Moyenne journalière des mentions (référence)
    STDDEV(total_mentions) AS ecart_type_mentions         -- Écart-type des mentions (dispersion)
  FROM mentions_stats                                     -- Calculé sur les volumes journaliers
),                                                        -- Fin CTE 2

mentions_avec_pic AS (                                    -- CTE 3 : marquage des jours de pic
  SELECT
    m.date_jour,                                          -- Jour analysé
    m.total_mentions,                                     -- Volume de mentions pour ce jour
    CASE                                                  -- Détection d’anomalie (seuil statistique)
      WHEN m.total_mentions > s.moyenne_mentions + 2 * s.ecart_type_mentions THEN 1  -- Pic : > moyenne + 2σ
      ELSE 0                                             -- Sinon : pas de pic
    END AS pic_mention                                    -- Indicateur binaire de pic (1/0)
  FROM mentions_stats m                                   -- Volumes journaliers
  CROSS JOIN stats_calc s                                 -- Réplication de la moyenne/σ sur chaque jour
),                                                        -- Fin CTE 3

ventes_par_jour AS (                                      -- CTE 4 : volumes de ventes par jour
  SELECT
    d.date AS date_jour,                                  -- Jour calendaire de commande
    SUM(dc.quantite) AS volume_ventes                     -- Total d’unités vendues ce jour
  FROM {{ ref('dim_details_commandes') }} dc              -- Lignes de commande (quantités)
  JOIN {{ ref('facts_commandes') }} c                     -- Entêtes de commande
    ON dc.id_commande = c.id_commande                     -- Jointure entête ↔ détail
  JOIN {{ ref('dim_date') }} d                            -- Dimension date de commande
    ON c.id_date_commande = d.id_date                     -- Récupération de la date réelle
  WHERE LOWER(c.statut_commande) NOT IN ('annulée', 'cancelled')  -- Exclure commandes annulées
  GROUP BY d.date                                         -- Agrégation : une ligne par jour
)                                                         -- Fin CTE 4

SELECT                                                    -- Résultat final : fusion mentions + ventes
  m.date_jour,                                            -- Jour
  EXTRACT(YEAR FROM m.date_jour) AS annee,                -- Année (pour analyses temporelles)
  EXTRACT(MONTH FROM m.date_jour) AS mois,                -- Mois (pour regroupements mensuels)
  m.total_mentions,                                       -- Volume de mentions ce jour
  m.pic_mention,                                          -- Flag de pic (1 = jour anormal)
  COALESCE(v.volume_ventes, 0) AS volume_ventes           -- Ventes du jour (0 si aucune donnée)
FROM mentions_avec_pic m                                  -- Base : mentions + indicateur de pic
LEFT JOIN ventes_par_jour v                               -- Jointure optionnelle des ventes
  ON m.date_jour = v.date_jour                            -- Alignement sur le même jour
ORDER BY m.date_jour ASC                                  -- Tri chronologique croissant