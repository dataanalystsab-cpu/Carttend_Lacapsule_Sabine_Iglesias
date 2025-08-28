-- ============================================================================
-- Ventes_9_CA_jours_semaines_mois.sql (version dbt)
-- Objectif :
--   - Calculer le chiffre d’affaires total généré chaque jour
--   - Tenir compte des promotions (réductions en % ou montant fixe)
--   - Exclure les commandes annulées
--   - Fournir un découpage par année, mois et jour
-- ============================================================================

SELECT
    dt.date AS periode,                                  -- Date complète de la commande
    EXTRACT(YEAR FROM dt.date) AS annee,                 -- Année extraite de la date
    EXTRACT(MONTH FROM dt.date) AS mois,                 -- Mois extrait de la date
    EXTRACT(DAY FROM dt.date) AS jour,                   -- Jour extrait de la date

    -- ✅ Calcul du chiffre d’affaires journalier avec gestion des promotions
    ROUND(SUM(
      dc.quantite *
      CASE
        -- Si la commande est passée pendant une période promotionnelle
        WHEN dt.date BETWEEN dpr_debut.date AND dpr_fin.date THEN
          GREATEST(                                      -- On empêche un prix négatif
            CASE
              -- Cas 1 : Promotion en pourcentage (ex: "30%")
              WHEN REGEXP_CONTAINS(pr.valeur_promotion, r'%$') THEN
                p.prix * (1 - CAST(REGEXP_REPLACE(pr.valeur_promotion, r'[%]', '') AS FLOAT64) / 100)
              -- Cas 2 : Promotion en valeur fixe (ex: "€10.00")
              ELSE
                p.prix - CAST(
                          REGEXP_REPLACE(
                            REGEXP_REPLACE(pr.valeur_promotion, r'€', ''), 
                          r',', '.') AS FLOAT64
                        )
            END,
            0
          )
        -- Sinon → prix catalogue (pas de promotion applicable)
        ELSE p.prix
      END
    ), 2) AS chiffre_affaires                           -- Arrondi à 2 décimales

FROM {{ ref('facts_commandes') }} c                      -- Table des commandes (infos globales)
JOIN {{ ref('dim_date') }} dt                            -- Dimension date (découpe année / mois / jour)
    ON dt.id_date = c.id_date_commande
JOIN {{ ref('dim_details_commandes') }} dc               -- Détails des commandes (quantité commandée)
    ON c.id_commande = dc.id_commande
JOIN {{ ref('dim_produits') }} p                         -- Produits (prix catalogue)
    ON dc.id_details_produits = p.id_produit
LEFT JOIN {{ ref('dim_promotions') }} pr                 -- Promotions (valeur, période)
    ON pr.id_produit = p.id_produit
LEFT JOIN {{ ref('dim_date') }} dpr_debut                -- Date début de promo
    ON dpr_debut.id_date = pr.id_date_debut
LEFT JOIN {{ ref('dim_date') }} dpr_fin                  -- Date fin de promo
    ON dpr_fin.id_date = pr.id_date_fin

-- ❌ On exclut les commandes annulées (pas de CA généré)
WHERE LOWER(c.statut_commande) NOT IN ('annulée', 'cancelled')

-- ✅ Agrégation par date
GROUP BY dt.date
ORDER BY dt.date