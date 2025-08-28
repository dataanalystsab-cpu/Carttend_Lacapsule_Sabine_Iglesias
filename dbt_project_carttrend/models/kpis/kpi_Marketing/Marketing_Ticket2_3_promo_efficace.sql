-- Marketing_Ticket2_3_promo_efficace.sql
-- Requête : Analyse du chiffre d’affaires et des quantités liées aux promotions
--------------------------------------------------------------------------------
-- Cette requête permet d'identifier l’impact des promotions sur le CA et les ventes
-- en comparant CA/quantités totales vs CA/quantités sous promo.
--------------------------------------------------------------------------------

WITH produits_promo AS (                                              -- CTE 1 : Liste des produits avec leurs promotions
    SELECT
        pr.id_promotion,                                              -- Identifiant de la promotion
        pr.id_produit,                                                -- Identifiant du produit
        pr.type_promotion,                                            -- Type de promotion (remise fixe / %)
        CASE 
            WHEN LOWER(pr.type_promotion) LIKE '%pourcentage%' THEN    -- Si la promo est un pourcentage
                (SAFE_CAST(                                           -- Conversion en FLOAT64 après nettoyage
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(CAST(pr.valeur_promotion AS STRING), r'[%€\s]', ''), 
                        r',', '.'
                    ) AS FLOAT64
                ))
            ELSE                                                      -- Sinon (remise fixe en € par ex.)
                SAFE_CAST(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(CAST(pr.valeur_promotion AS STRING), r'[%€\s]', ''), 
                        r',', '.'
                    ) AS FLOAT64
                )
        END AS valeur_promotion,                                      -- Valeur numérique de la promotion
        dt1.date AS date_debut,                                       -- Date de début de la promotion
        dt2.date AS date_fin                                          -- Date de fin de la promotion
    FROM {{ ref("dim_promotions") }} pr
    JOIN {{ ref("dim_date") }} dt1 ON dt1.id_date = pr.id_date_debut  -- Jointure sur la date de début
    JOIN {{ ref("dim_date") }} dt2 ON dt2.id_date = pr.id_date_fin    -- Jointure sur la date de fin
    WHERE pr.id_produit IS NOT NULL                                   -- Exclusion des promos sans produit
),

ca_detail AS (                                                        -- CTE 2 : CA et quantités par produit et date
    SELECT
        dc.id_details_produits AS id_produit,                         -- Produit commandé
        dt3.date AS date_commande,                                    -- Date de commande
        SUM(dc.quantite) AS quantite,                                 -- Quantité vendue
        ROUND(SUM(dc.quantite * p.prix), 2) AS ca_mensuel             -- CA généré (hors promo appliquée ici)
    FROM {{ ref("facts_commandes") }} c
    JOIN {{ ref("dim_date") }} dt3 ON dt3.id_date = c.id_date_commande
    JOIN {{ ref("dim_details_commandes") }} dc ON c.id_commande = dc.id_commande
    JOIN {{ ref("dim_produits") }} p ON dc.id_details_produits = p.id_produit
    GROUP BY id_produit, date_commande
),

promo_flagged AS (                                                    -- CTE 3 : Marquage des ventes en promo ou non
    SELECT
        p.id_promotion,                                               -- Promo associée
        ca.id_produit,                                                -- Produit concerné
        p.type_promotion,                                             -- Type de promo
        ca.date_commande,                                             -- Date de commande
        ca.ca_mensuel,                                                -- CA calculé
        ca.quantite,                                                  -- Quantité vendue
        p.valeur_promotion,                                           -- Valeur de la promotion
        MAX(                                                          -- Indicateur binaire si vente faite sous promo
            CASE
                WHEN ca.date_commande BETWEEN p.date_debut AND p.date_fin
                THEN 1 ELSE 0
            END
        ) AS en_promo
    FROM ca_detail ca
    JOIN produits_promo p ON ca.id_produit = p.id_produit             -- Association produit <-> promo
    GROUP BY p.id_promotion, ca.id_produit, p.type_promotion, 
             ca.date_commande, ca.ca_mensuel, ca.quantite, p.valeur_promotion
),

aggrege_ca AS (                                                       -- CTE 4 : Agrégation des résultats
    SELECT
        id_promotion,                                                 -- Identifiant de la promotion
        id_produit,                                                   -- Produit concerné
        type_promotion,                                               -- Type de promo
        valeur_promotion,                                             -- Valeur de la promo
        SUM(ca_mensuel) AS ca_total,                                  -- CA total du produit
        SUM(CASE WHEN en_promo = 1 THEN ca_mensuel ELSE 0 END) AS ca_promo, -- CA généré pendant promo
        SUM(quantite) AS quantite_total,                              -- Quantité totale vendue
        SUM(CASE WHEN en_promo = 1 THEN quantite ELSE 0 END) AS quantite_promo -- Quantité vendue en promo
    FROM promo_flagged
    GROUP BY id_promotion, id_produit, type_promotion, valeur_promotion
)

SELECT                                                                 -- Résultat final
    id_promotion,                                                      -- Identifiant promo
    id_produit,                                                        -- Identifiant produit
    type_promotion,                                                    -- Type de promo
    valeur_promotion,                                                  -- Valeur numérique de la promo
    ca_total,                                                          -- Chiffre d’affaires total
    ca_promo,                                                          -- Chiffre d’affaires généré sous promo
    quantite_total,                                                    -- Quantités totales vendues
    quantite_promo,                                                    -- Quantités vendues sous promo
    ROUND(ca_promo / NULLIF(ca_total, 0), 4) * 100 AS ratio_promo      -- % du CA réalisé en promo
FROM aggrege_ca
WHERE ca_total > 0                                                     -- On garde uniquement les produits vendus
ORDER BY ca_promo DESC                                                 -- Classement par CA promo décroissant