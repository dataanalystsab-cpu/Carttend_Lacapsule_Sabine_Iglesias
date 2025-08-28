-- ===========================================================
-- Requête : TOP produits par chiffre d’affaires (dbt)
-- Objectif :
--   1. Identifier les produits qui génèrent le plus de chiffre d’affaires.
--   2. Appliquer correctement les promotions (réduction en % ou en valeur fixe).
--   3. Différencier les produits ayant le même nom mais des prix différents,
--      en ajoutant la "variation" si nécessaire (ex: couleur, édition).
--   4. Exclure les commandes annulées pour obtenir un CA réel.
-- ===========================================================

-- Étape 1 : Préparation des produits (on détecte si plusieurs prix existent pour un même produit)
WITH produits_prepares AS (
    SELECT
        p.id_produit,                                -- Identifiant unique du produit
        p.produit,                                   -- Nom du produit
        p.categorie,                                 -- Catégorie du produit (jouet, électroménager, etc.)
        p.prix,                                      -- Prix catalogue du produit
        p.variation,                                 -- Variation éventuelle (ex: couleur, édition spéciale)
        COUNT(DISTINCT p.prix)                       -- Compte le nombre de prix distincts pour ce produit
            OVER (PARTITION BY p.produit) AS nb_prix -- Si >1, on saura qu’il faut afficher aussi la variation
    FROM {{ ref('dim_produits') }} p
)

-- Étape 2 : Calcul du chiffre d’affaires par produit
SELECT
    pp.categorie,                                   -- Catégorie du produit
    CASE 
        WHEN pp.nb_prix > 1 AND pp.variation IS NOT NULL 
            THEN CONCAT(pp.produit, ' - ', pp.variation)  -- Si plusieurs prix : ajoute la variation
        ELSE pp.produit                                   -- Sinon : garde le nom simple
    END AS produit,

    pp.prix,                                        -- Prix catalogue affiché (utile pour vérifier le CA)
    SUM(dc.quantite) AS total_vendus,               -- Total des unités vendues pour ce produit

    -- ✅ Calcul du chiffre d’affaires en tenant compte des promotions
    ROUND(SUM(
        dc.quantite *                               -- Quantité commandée
        CASE
            -- Si la date de commande tombe dans une période promotionnelle
            WHEN dcmd.date BETWEEN dpr_debut.date AND dpr_fin.date THEN
                GREATEST(                           -- On empêche que le prix devienne négatif
                    CASE
                        -- Cas 1 : Promotion exprimée en pourcentage (ex: "30%")
                        WHEN REGEXP_CONTAINS(pr.valeur_promotion, r'%$') THEN
                            pp.prix * (1 - CAST(REGEXP_REPLACE(pr.valeur_promotion, r'[%]', '') AS FLOAT64) / 100)
                        -- Cas 2 : Promotion exprimée en valeur absolue (ex: "€10.00")
                        ELSE
                            pp.prix - CAST(REGEXP_REPLACE(REGEXP_REPLACE(pr.valeur_promotion, r'€', ''), r',', '.') AS FLOAT64)
                    END,
                    0 -- Prix minimum = 0
                )
            -- Sinon : pas de promo → prix catalogue
            ELSE pp.prix
        END
    ), 2) AS chiffre_affaires                       -- On arrondit à 2 décimales (format monétaire)

-- Sources de données
FROM {{ ref('dim_details_commandes') }} dc          -- Détail des lignes de commande (produits, quantités)
JOIN {{ ref('facts_commandes') }} c                 -- Table des commandes globales (statut, client, date)
    ON dc.id_commande = c.id_commande
JOIN produits_prepares pp                           -- Jointure avec les produits préparés (nom + variation si besoin)
    ON dc.id_details_produits = pp.id_produit
JOIN {{ ref('dim_date') }} dcmd                     -- Table de dimension date (date de commande)
    ON dcmd.id_date = c.id_date_commande
LEFT JOIN {{ ref('dim_promotions') }} pr            -- Promotions applicables aux produits
    ON pr.id_produit = pp.id_produit
LEFT JOIN {{ ref('dim_date') }} dpr_debut           -- Date de début de promo
    ON dpr_debut.id_date = pr.id_date_debut
LEFT JOIN {{ ref('dim_date') }} dpr_fin             -- Date de fin de promo
    ON dpr_fin.id_date = pr.id_date_fin

-- Filtre : on enlève les commandes annulées
WHERE LOWER(c.statut_commande) NOT IN ('annulée', 'cancelled')

-- Agrégation finale : on regroupe par catégorie, produit (nom ou nom+variation), et prix
GROUP BY 
    pp.categorie, 
    produit, 
    pp.prix

-- Classement final : du plus gros CA au plus petit
ORDER BY chiffre_affaires DESC
