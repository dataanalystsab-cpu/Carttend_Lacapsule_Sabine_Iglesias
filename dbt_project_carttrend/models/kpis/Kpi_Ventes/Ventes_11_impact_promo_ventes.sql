-- Ventes_11_impact_promo_ventes.sql
-- Requête : Analyse de l’effet des promotions sur les ventes
-- ------------------------------------------------------------------------------------
-- Objectif :
-- Cette requête mesure l’impact des promotions sur les ventes des produits.
-- Elle compare les performances (CA, quantités vendues, nb de commandes) :
--   - Avant promo  : sur les 7 jours qui précèdent la promo
--   - Pendant promo : sur toute la période promotionnelle
--   - Après promo   : sur les 7 jours qui suivent la promo
--   - Hors période  : autres dates (exclues ensuite)
-- ------------------------------------------------------------------------------------

SELECT
    -- Classification des commandes par rapport à la période de promotion
    CASE
        WHEN dt3.date BETWEEN DATE_SUB(dt1.date, INTERVAL 7 DAY) AND DATE_SUB(dt1.date, INTERVAL 1 DAY)
            THEN 'Avant promo'      -- 7 jours avant la promo
        WHEN dt3.date BETWEEN dt1.date AND dt2.date
            THEN 'Pendant promo'    -- Période exacte de la promo
        WHEN dt3.date BETWEEN DATE_ADD(dt2.date, INTERVAL 1 DAY) AND DATE_ADD(dt2.date, INTERVAL 7 DAY)
            THEN 'Après promo'      -- 7 jours après la promo
        ELSE 'Hors période'         -- Autres dates (hors analyse)
    END AS periode_analyse,

    p.produit AS produit,             -- Nom du produit
    dt1.date AS debut_promo,          -- Date de début de promo
    dt2.date AS fin_promo,            -- Date de fin de promo
    dt3.date AS date_commande,        -- Date de la commande

    ROUND(SUM(dc.quantite * p.prix), 2) AS chiffre_affaires, -- Chiffre d’affaires généré
    SUM(dc.quantite) AS quantite_vendue,                     -- Quantité totale vendue
    COUNT(DISTINCT c.id_commande) AS nb_commandes            -- Nombre de commandes distinctes

FROM {{ ref('facts_commandes') }} c                          -- Table des commandes
JOIN {{ ref('dim_details_commandes') }} dc                   -- Détails des commandes (produits + quantités)
    ON c.id_commande = dc.id_commande
JOIN {{ ref('dim_produits') }} p                             -- Produits (nom + prix)
    ON dc.id_details_produits = p.id_produit
JOIN {{ ref('dim_promotions') }} pr                          -- Périodes de promotions
    ON pr.id_produit = p.id_produit
JOIN {{ ref('dim_date') }} dt1 ON dt1.id_date = pr.id_date_debut  -- Date de début promo
JOIN {{ ref('dim_date') }} dt2 ON dt2.id_date = pr.id_date_fin    -- Date de fin promo
JOIN {{ ref('dim_date') }} dt3 ON dt3.id_date = c.id_date_commande -- Date de la commande

WHERE dt3.date BETWEEN DATE_SUB(dt1.date, INTERVAL 7 DAY)  -- On limite l’analyse à la fenêtre
                  AND DATE_ADD(dt2.date, INTERVAL 7 DAY)   -- 7 jours avant → 7 jours après la promo
  AND LOWER(c.statut_commande) NOT IN ('annulée', 'cancelled') -- Exclusion des commandes annulées

GROUP BY periode_analyse, p.produit, debut_promo, fin_promo, date_commande -- Agrégation

ORDER BY 
    p.produit,
    CASE periode_analyse                        -- Classement logique des périodes
        WHEN 'Avant promo' THEN 1
        WHEN 'Pendant promo' THEN 2
        WHEN 'Après promo' THEN 3
        ELSE 4
    END