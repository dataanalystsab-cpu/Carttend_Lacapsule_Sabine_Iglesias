-- Ventes_10_CA_mois_pics_creux.sql
-- Requête : Analyse du chiffre d’affaires mensuel avec classification des variations
-- -----------------------------------------------------------------------------------
-- Objectif :
-- Cette requête calcule le chiffre d’affaires mensuel et compare chaque mois à la moyenne globale.
-- Elle permet de classer chaque mois selon sa performance :
--   - "Mois fort"   → CA supérieur à 130% de la moyenne globale
--   - "Mois faible" → CA inférieur à 70% de la moyenne globale
--   - "Normal"      → CA compris entre ces deux seuils
-- Utile pour détecter rapidement les mois atypiques (pics ou baisses significatives).
-- -----------------------------------------------------------------------------------

WITH ca_mensuel AS (
  SELECT
    EXTRACT(YEAR FROM dt.date) AS annee,                  -- Année de la commande
    EXTRACT(MONTH FROM dt.date) AS mois,                  -- Mois de la commande
    ROUND(SUM(dc.quantite * p.prix), 2) AS chiffre_affaires -- Chiffre d’affaires du mois
  FROM {{ ref('facts_commandes') }} c
  JOIN {{ ref('dim_date') }} dt 
    ON dt.id_date = c.id_date_commande                    -- Jointure avec la dimension temps
  JOIN {{ ref('dim_details_commandes') }} dc 
    ON c.id_commande = dc.id_commande                     -- Jointure avec les détails de commande
  JOIN {{ ref('dim_produits') }} p 
    ON dc.id_details_produits = p.id_produit              -- Jointure avec les produits (prix)
  WHERE LOWER(c.statut_commande) NOT IN ('annulée', 'cancelled') -- Exclusion des commandes annulées
  GROUP BY annee, mois                                    -- Agrégation par année et mois
)

SELECT
  CONCAT(annee, '-', LPAD(CAST(mois AS STRING), 2, '0')) AS periode, -- Format AAAA-MM
  chiffre_affaires,                                                  -- Chiffre d’affaires mensuel
  ROUND(AVG(chiffre_affaires) OVER (), 2) AS moyenne_globale,        -- Moyenne du CA sur toute la période
  CASE
    WHEN chiffre_affaires > AVG(chiffre_affaires) OVER () * 1.3 THEN 'Mois fort'   -- CA > 130% de la moyenne
    WHEN chiffre_affaires < AVG(chiffre_affaires) OVER () * 0.7 THEN 'Mois faible' -- CA < 70% de la moyenne
    ELSE 'Normal'                                                    -- Sinon = performance moyenne
  END AS variation
FROM ca_mensuel
ORDER BY periode                                                     -- Tri chronologique