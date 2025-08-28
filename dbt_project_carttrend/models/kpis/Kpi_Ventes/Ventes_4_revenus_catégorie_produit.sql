-- ============================================================================
-- Requête : Évolution quotidienne du chiffre d'affaires par catégorie de produits (dbt)
-- Objectif :
--   1. Calculer le chiffre d’affaires journalier pour chaque catégorie de produits.
--   2. Tenir compte des promotions (réduction en % ou en valeur fixe).
--   3. Exclure les commandes annulées pour ne garder que le CA réel.
--   4. Suivre les tendances journalières et comparer les performances des catégories.
-- ============================================================================

SELECT 
    d.date AS date_commande,                          -- Date de la commande
    p.categorie AS categorie,                         -- Catégorie du produit (jouet, électronique, etc.)

    -- ✅ Calcul du chiffre d’affaires (quantité × prix, tenant compte des promotions)
    ROUND(SUM(
        dc.quantite *                                 -- Quantité commandée
        CASE
            -- Si la commande est passée pendant une période promotionnelle
            WHEN d.date BETWEEN dpr_debut.date AND dpr_fin.date THEN
                GREATEST(                             -- Empêche que le prix descende en dessous de 0
                    CASE
                        -- Cas 1 : Promotion en pourcentage (ex: "20%")
                        WHEN REGEXP_CONTAINS(pr.valeur_promotion, r'%$') THEN
                            p.prix * (1 - CAST(REGEXP_REPLACE(pr.valeur_promotion, r'[%]', '') AS FLOAT64) / 100)
                        -- Cas 2 : Promotion en valeur fixe (ex: "€5.00")
                        ELSE
                            p.prix - CAST(REGEXP_REPLACE(REGEXP_REPLACE(pr.valeur_promotion, r'€', ''), r',', '.') AS FLOAT64)
                    END,
                    0 -- Prix minimum = 0
                )
            -- Sinon : pas de promotion → prix catalogue
            ELSE p.prix
        END
    ), 2) AS total_revenu                             -- Chiffre d’affaires journalier arrondi à 2 décimales

-- ============================================================================
-- Sources de données (via dbt ref)
-- ============================================================================
FROM {{ ref('dim_details_commandes') }} dc            -- Détails des commandes
JOIN {{ ref('facts_commandes') }} c                   -- Commandes globales
    ON dc.id_commande = c.id_commande
JOIN {{ ref('dim_date') }} d                          -- Date de la commande
    ON d.id_date = c.id_date_commande
JOIN {{ ref('dim_produits') }} p                      -- Produits (catégorie, prix catalogue)
    ON dc.id_details_produits = p.id_produit
LEFT JOIN {{ ref('dim_promotions') }} pr              -- Promotions
    ON pr.id_produit = p.id_produit
LEFT JOIN {{ ref('dim_date') }} dpr_debut             -- Date début de promo
    ON dpr_debut.id_date = pr.id_date_debut
LEFT JOIN {{ ref('dim_date') }} dpr_fin               -- Date fin de promo
    ON dpr_fin.id_date = pr.id_date_fin

-- ============================================================================
-- Filtres
-- ============================================================================
WHERE LOWER(c.statut_commande) NOT IN ('annulée', 'cancelled')  -- Exclusion des commandes annulées

-- ============================================================================
-- Agrégation et tri
-- ============================================================================
GROUP BY date_commande, categorie
ORDER BY date_commande, total_revenu DESC
