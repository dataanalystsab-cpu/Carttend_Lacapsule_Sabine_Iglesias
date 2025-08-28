-- ============================================================================
-- Requête : Analyse des produits en déclin (DBT + BigQuery)
-- Objectif :
--   1. Identifier les produits dont le chiffre d’affaires mensuel baisse
--      continuellement sur les 6 derniers mois.
--   2. Prendre en compte les promotions (pourcentage ou montant fixe).
--   3. Inclure les mois sans ventes (CA = 0).
--   4. Utiliser une fenêtre analytique (LAG) pour comparer mois après mois.
-- ============================================================================

-- Étape 1 : Calcul du chiffre d'affaires mensuel par produit
WITH ventes_par_mois AS (
  SELECT
    dc.id_details_produits,
    p.id_produit,                                -- Identifiant unique du produit
    p.categorie,                                 -- Catégorie du produit
    DATE_TRUNC(dt.date, MONTH) AS mois,          -- Mois de la commande

    -- ✅ Calcul du chiffre d’affaires tenant compte des promotions
    ROUND(SUM(
      dc.quantite *
      CASE
        -- Si la commande est passée pendant une période promotionnelle
        WHEN dt.date BETWEEN dpr_debut.date AND dpr_fin.date THEN
          GREATEST(                              -- Empêche prix < 0
            CASE
              -- Cas 1 : Promotion en pourcentage (ex: "20%")
              WHEN REGEXP_CONTAINS(pr.valeur_promotion, r'%$') THEN
                p.prix * (1 - CAST(REGEXP_REPLACE(pr.valeur_promotion, r'[%]', '') AS FLOAT64) / 100)
              -- Cas 2 : Promotion en valeur fixe (ex: "€5.00")
              ELSE
                p.prix - CAST(REGEXP_REPLACE(REGEXP_REPLACE(pr.valeur_promotion, r'€', ''), r',', '.') AS FLOAT64)
            END,
            0
          )
        -- Sinon : pas de promo → prix catalogue
        ELSE p.prix
      END
    ), 2) AS ca_mensuel                          -- CA mensuel arrondi à 2 décimales

  FROM {{ ref('dim_details_commandes') }} dc
  JOIN {{ ref('facts_commandes') }} c
    ON dc.id_commande = c.id_commande
  JOIN {{ ref('dim_date') }} dt
    ON c.id_date_commande = dt.id_date
  JOIN {{ ref('dim_produits') }} p
    ON dc.id_details_produits = p.id_produit
  LEFT JOIN {{ ref('dim_promotions') }} pr
    ON pr.id_produit = p.id_produit
  LEFT JOIN {{ ref('dim_date') }} dpr_debut
    ON dpr_debut.id_date = pr.id_date_debut
  LEFT JOIN {{ ref('dim_date') }} dpr_fin
    ON dpr_fin.id_date = pr.id_date_fin
  WHERE LOWER(c.statut_commande) NOT IN ('annulée', 'cancelled') -- Exclusion des commandes annulées
  GROUP BY dc.id_details_produits, p.id_produit, p.categorie, DATE_TRUNC(dt.date, MONTH)
),

-- Étape 2 : Construction d’une table complète (ajouter les mois sans ventes)
ventes_completes AS (
  SELECT
    p.id_produit,                                -- Produit
    p.categorie,                                 -- Catégorie
    c.mois,                                      -- Mois considéré
    COALESCE(v.ca_mensuel, 0) AS ca_mensuel      -- 0 si pas de ventes ce mois-là
  FROM {{ ref('dim_produits') }} p
  CROSS JOIN (                                   -- Liste des mois disponibles
    SELECT DISTINCT DATE_TRUNC(dt.date, MONTH) AS mois
    FROM {{ ref('facts_commandes') }} co
    JOIN {{ ref('dim_date') }} dt
      ON co.id_date_commande = dt.id_date
    WHERE LOWER(co.statut_commande) NOT IN ('annulée', 'cancelled')
  ) c
  LEFT JOIN ventes_par_mois v
    ON p.id_produit = v.id_produit AND c.mois = v.mois
),

-- Étape 3 : Ajout d’un historique (fenêtre glissante des 6 derniers mois)
avec_variations AS (
  SELECT
    *,
    LAG(ca_mensuel, 1) OVER (PARTITION BY id_produit ORDER BY mois) AS m1, -- Mois précédent
    LAG(ca_mensuel, 2) OVER (PARTITION BY id_produit ORDER BY mois) AS m2, -- -2 mois
    LAG(ca_mensuel, 3) OVER (PARTITION BY id_produit ORDER BY mois) AS m3, 
    LAG(ca_mensuel, 4) OVER (PARTITION BY id_produit ORDER BY mois) AS m4, 
    LAG(ca_mensuel, 5) OVER (PARTITION BY id_produit ORDER BY mois) AS m5
  FROM ventes_completes
),

-- Étape 4 : Sélection des produits en déclin (CA en baisse 6 mois consécutifs)
ventes_produits_en_declin AS (
  SELECT *
  FROM avec_variations
  WHERE ca_mensuel < m1   -- Mois en cours < Mois précédent
    AND m1 < m2
    AND m2 < m3
    AND m3 < m4
    AND m4 < m5
)

-- Résultat final : liste des produits en déclin
SELECT *
FROM ventes_produits_en_declin
ORDER BY id_produit, mois