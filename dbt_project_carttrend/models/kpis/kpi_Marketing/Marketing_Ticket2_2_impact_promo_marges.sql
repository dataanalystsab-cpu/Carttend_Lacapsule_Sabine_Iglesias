-- Marketing_Ticket2_2_impact_promo_marges.sql
-- Requête : Analyse mensuelle CA promo vs hors promo et budget marketing
--------------------------------------------------------------------------------
-- Cette requête permet de comparer chaque mois le chiffre d’affaires généré 
-- (promotions vs hors promotions) et le rapprocher des budgets
-- marketing engagés par canal et type d’événement.
--------------------------------------------------------------------------------

WITH ca_mensuel AS (                                      -- CTE 1 : Calcul du chiffre d’affaires par mois
  SELECT
    DATE_TRUNC(dt.date, MONTH) AS mois,                   -- Regroupement par mois
    ROUND(SUM(                                             -- Somme du CA généré par les ventes en promotion
      IF(
        promo.id_promotion IS NOT NULL,                   -- Si une promotion est active sur le produit
        dc.quantite * (                                   -- On multiplie la quantité par le prix remisé
          CASE 
            WHEN promo.type_promotion = 'Remise fixe'     -- Cas 1 : Remise en valeur fixe (€)
              THEN pr.prix - safe_cast(regexp_replace(cast(promo.valeur_promotion as string), r'[%€]', '') AS float64)
            WHEN promo.type_promotion = 'Pourcentage'     -- Cas 2 : Remise en pourcentage (%)
              THEN pr.prix * (1 - safe_cast(regexp_replace(cast(promo.valeur_promotion as string), r'[%€]', '') AS float64) / 100)
            ELSE pr.prix                                  -- Cas 3 : Sinon, prix normal
          END
        ),
        0                                                 -- Sinon, CA promo = 0
      )
    ), 2) AS ca_promo,                                    -- Résultat : chiffre d’affaires sous promo
    ROUND(SUM(
      IF(promo.id_promotion IS NULL, dc.quantite * pr.prix, 0)  -- CA hors promo (si pas de promo active)
    ), 2) AS ca_hors_promo
  FROM {{ ref('facts_commandes') }} c                     -- Faits commandes
  JOIN {{ ref('dim_details_commandes') }} dc              -- Détails commandes
    ON c.id_commande = dc.id_commande
  JOIN {{ ref('dim_date') }} dt                           -- Dimension date
    ON dt.id_date = c.id_date_commande 
  JOIN {{ ref('dim_produits') }} pr                       -- Dimension produits
    ON dc.id_details_produits = pr.id_produit
  LEFT JOIN {{ ref('dim_promotions') }} promo             -- Dimension promotions (pour prix remisé)
    ON pr.id_produit = promo.id_produit
   AND c.id_date_commande BETWEEN promo.id_date_debut AND promo.id_date_fin -- Vérifie si commande tombe pendant promo
  WHERE LOWER(c.statut_commande) NOT IN ('annulée', 'cancelled') -- Exclusion des commandes annulées
  GROUP BY mois                                           -- Agrégation par mois
),

budget_mensuel AS (                                      -- CTE 2 : Calcul des budgets marketing par mois
  SELECT
    DATE_TRUNC(dt.date, MONTH) AS mois,                  -- Regroupement par mois
    dc.nom_canal  AS canal,                              -- Nom du canal marketing
    fc.evenement_type,                                   -- Type d’événement (ex : campagne, promo, etc.)
    ROUND(SUM(fc.budget), 2) AS budget_marketing         -- Budget total alloué
  FROM {{ ref('facts_campaigns') }} fc                   -- Faits campagnes
  JOIN {{ ref('dim_date') }} dt 
    ON dt.id_date = fc.id_date
  JOIN {{ ref('dim_canal') }} AS dc                      -- Dimension canaux marketing
    ON dc.id_canal = fc.id_canal_dim_canal
  GROUP BY mois, fc.id_canal_dim_canal, fc.evenement_type, dc.nom_canal
),

jointure AS (                                            -- CTE 3 : Jointure CA et budget marketing
  SELECT
    IFNULL(c.mois, b.mois) AS mois,                      -- Mois (venant soit du CA, soit du budget)
    IFNULL(c.ca_promo, 0) AS ca_promo,                   -- CA promo (0 si absent)
    IFNULL(c.ca_hors_promo, 0) AS ca_hors_promo,         -- CA hors promo (0 si absent)
    (IFNULL(c.ca_promo, 0) + IFNULL(c.ca_hors_promo, 0)) AS ca_total, -- CA total du mois
    b.canal,                                             -- Canal marketing
    b.evenement_type,                                    -- Type d’événement
    IFNULL(b.budget_marketing, 0) AS budget_marketing    -- Budget marketing du mois (0 si absent)
  FROM ca_mensuel c
  FULL OUTER JOIN budget_mensuel b                       -- Jointure complète (pour ne rien perdre)
    ON c.mois = b.mois
)

SELECT                                                    -- Résultat final : CA + budget + ratios
  mois,                                                   -- Mois
  ca_promo,                                               -- CA généré par promotions
  ca_hors_promo,                                          -- CA généré hors promotions
  ca_total,                                               -- CA global
  canal,                                                  -- Canal marketing
  evenement_type,                                         -- Type d’événement marketing
  budget_marketing,                                       -- Budget engagé
  SAFE_DIVIDE(ca_promo, ca_total) * 100 AS impact_promo_CA -- KPI : % du CA lié aux promos
FROM jointure
ORDER BY mois, canal, evenement_type                      -- Classement du résultat