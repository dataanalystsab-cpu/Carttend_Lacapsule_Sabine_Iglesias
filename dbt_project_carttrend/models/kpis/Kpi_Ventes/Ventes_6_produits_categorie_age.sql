-- =========================================================== 
-- Requête : Analyse des ventes par tranche d’âge et produit
-- Objectif :
--   1. Identifier quels produits se vendent le plus selon les tranches d’âge.
--   2. Utiliser le bon prix (catalogue ou promotionnel) pour calculer le CA.
--   3. Différencier correctement les produits ayant le même nom mais 
--      des prix différents, en ajoutant la "variation" uniquement si nécessaire.
-- ===========================================================

-- Étape 1 : Préparation des produits (ajout de la variation si plusieurs prix pour un même produit)
WITH produits_prepares AS (
    SELECT
        p.id_produit,                                -- Identifiant unique du produit
        p.produit,                                   -- Nom du produit
        p.categorie,                                 -- Catégorie du produit 
        p.prix,                                      -- Prix catalogue du produit 
        p.variation,                                 -- Variation éventuelle 
        COUNT(DISTINCT p.prix) 
            OVER (PARTITION BY p.produit) AS nb_prix  -- Nombre de prix distincts par produit → sert à savoir si on doit afficher la variation
    FROM {{ ref('dim_produits') }} p
)

-- Étape 2 : Calcul des ventes par tranche d’âge et produit
SELECT
    -- Regroupement des clients en tranches d’âge
    CASE
        WHEN cl.age BETWEEN 18 AND 25 THEN '18-25'   -- Jeunes adultes
        WHEN cl.age BETWEEN 26 AND 35 THEN '26-35'   -- Adultes jeunes
        WHEN cl.age BETWEEN 36 AND 45 THEN '36-45'   -- Adultes matures
        WHEN cl.age BETWEEN 46 AND 60 THEN '46-60'   -- Seniors actifs
        ELSE '60+'                                   -- Seniors
    END AS tranche_age,

    -- Nom du produit : si plusieurs prix existent pour un même produit, on concatène la variation
    CASE 
        WHEN pp.nb_prix > 1 AND pp.variation IS NOT NULL -- Exemple : "Refurbished"
            THEN CONCAT(pp.produit, ' - ', pp.variation)  
        ELSE pp.produit                                  -- Sinon on garde juste le nom simple
    END AS produit,

    pp.categorie,                                    -- Catégorie du produit

    -- Quantité totale vendue (somme des quantités commandées)
    SUM(dc.quantite) AS quantite_vendue,

    -- Calcul du chiffre d’affaires (quantité × prix, en tenant compte des promotions)
    ROUND(SUM(
        dc.quantite *
        CASE
            -- Si la commande est passée pendant une période promotionnelle
            WHEN dcmd.date BETWEEN dpr_debut.date AND dpr_fin.date THEN
                GREATEST(   -- On empêche que le prix devienne négatif
                    CASE
                        -- Cas 1 : Promotion en pourcentage (ex: "45%")
                        WHEN REGEXP_CONTAINS(pr.valeur_promotion, r'%$') THEN
                            pp.prix * (1 - CAST(REGEXP_REPLACE(pr.valeur_promotion, r'[%]', '') AS FLOAT64) / 100)
                        -- Cas 2 : Promotion en valeur fixe (ex: "€10.00")
                        ELSE
                            pp.prix - CAST(REGEXP_REPLACE(REGEXP_REPLACE(pr.valeur_promotion, r'€', ''), r',', '.') AS FLOAT64)
                    END,
                    0  -- Prix minimum = 0
                )
            -- Sinon → prix catalogue
            ELSE pp.prix
        END
    ), 2) AS chiffre_affaires  -- Arrondi à 2 décimales

-- Sources de données : commandes, clients, produits, promotions et dates
FROM {{ ref('dim_details_commandes') }} dc           -- Détails des commandes (quantité × produit)
JOIN {{ ref('facts_commandes') }} c                  -- Commandes (informations globales)
    ON dc.id_commande = c.id_commande
JOIN produits_prepares pp                            -- Produits préparés avec variations
    ON dc.id_details_produits = pp.id_produit
JOIN {{ ref('dim_clients') }} cl                     -- Clients (âge)
    ON c.id_client = cl.id_client
JOIN {{ ref('dim_date') }} dcmd                      -- Date de la commande
    ON dcmd.id_date = c.id_date_commande
LEFT JOIN {{ ref('dim_promotions') }} pr             -- Promotions (type et valeur)
    ON pr.id_produit = pp.id_produit
LEFT JOIN {{ ref('dim_date') }} dpr_debut            -- Date de début de promotion
    ON dpr_debut.id_date = pr.id_date_debut
LEFT JOIN {{ ref('dim_date') }} dpr_fin              -- Date de fin de promotion
    ON dpr_fin.id_date = pr.id_date_fin

-- Filtre : on exclut les commandes annulées
WHERE LOWER(c.statut_commande) NOT IN ('annulée', 'cancelled')

-- Agrégation : résultats regroupés par tranche d’âge, produit et catégorie
GROUP BY 
    tranche_age, 
    produit, 
    pp.categorie

-- Tri final : par tranche d’âge puis par quantité vendue (ordre décroissant)
ORDER BY 
    tranche_age, 
    quantite_vendue DESC
