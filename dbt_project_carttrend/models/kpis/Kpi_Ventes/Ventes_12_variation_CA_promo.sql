-- Ventes_12_variation_CA_promo.sql
-- Requête :Analyse de l'impact des promotions sur les ventes des produits
-- --------------------------------------------------------------------------------
-- Cette requête permet de comparer les ventes avant et pendant une promotion
-- pour chaque produit, afin de mesurer :
--   - Le chiffre d’affaires (CA) généré
--   - La quantité de produits vendue
--   - La variation en pourcentage entre avant et pendant la promo
--   - Une interprétation automatique de la sensibilité du produit à la promo
-- --------------------------------------------------------------------------------

WITH ventes_par_periode AS (
  -- On calcule le chiffre d’affaires et la quantité vendue
  -- pour chaque produit selon la période ("avant" ou "pendant").
  SELECT
    p.id_produit AS id_produit,         -- Identifiant du produit
    p.produit AS produit,               -- Nom du produit
    CASE
      WHEN dt3.date BETWEEN DATE_SUB(dt1.date, INTERVAL 30 DAY) 
                               AND DATE_SUB(dt2.date, INTERVAL 1 DAY)
        THEN 'avant'                    -- 30 jours avant le début de la promotion
      WHEN dt3.date BETWEEN dt1.date AND dt2.date
        THEN 'pendant'                  -- Période pendant la promotion
      ELSE 'autre'                      -- Les autres périodes (exclues plus bas)
    END AS periode,
    SUM(dc.quantite * p.prix) AS chiffre_affaires, -- Calcul du CA : quantité * prix
    SUM(dc.quantite) AS quantite_vendue            -- Quantité totale vendue
  FROM {{ ref('facts_commandes') }} c             -- Table des commandes
  JOIN {{ ref('dim_details_commandes') }} dc      -- Détails des commandes (produits commandés)
    ON c.id_commande = dc.id_commande
  JOIN {{ ref('dim_produits') }} p                -- Table des produits
    ON dc.id_details_produits = p.id_produit
  JOIN {{ ref('dim_promotions') }} pr             -- Table des promotions (début & fin)
    ON pr.id_produit = p.id_produit
  JOIN {{ ref('dim_date') }} dt1 ON dt1.id_date = pr.id_date_debut -- Date début promo
  JOIN {{ ref('dim_date') }} dt2 ON dt2.id_date = pr.id_date_fin   -- Date fin promo
  JOIN {{ ref('dim_date') }} dt3 ON dt3.id_date = c.id_date_commande -- Date de la commande
  WHERE dt3.date BETWEEN DATE_SUB(dt1.date, INTERVAL 30 DAY) AND dt2.date -- Filtrage sur 30j avant → fin promo
    AND LOWER(c.statut_commande) NOT IN ('annulée', 'cancelled') -- Exclusion des commandes annulées
  GROUP BY p.id_produit, p.produit, periode       -- Agrégation par produit et par période
),

comparaison AS (
  -- On compare maintenant les données "avant" et "pendant" pour chaque produit
  SELECT
    v_avant.id_produit,                -- Identifiant du produit
    v_avant.produit,                   -- Nom du produit
    v_avant.chiffre_affaires AS ca_avant,   -- CA avant la promo
    v_pendant.chiffre_affaires AS ca_pendant, -- CA pendant la promo
    v_avant.quantite_vendue AS qte_avant,     -- Quantité vendue avant la promo
    v_pendant.quantite_vendue AS qte_pendant  -- Quantité vendue pendant la promo
  FROM ventes_par_periode v_avant
  JOIN ventes_par_periode v_pendant
    ON v_avant.id_produit = v_pendant.id_produit -- On fait correspondre le même produit
   AND v_avant.periode = 'avant'                 -- Données de la période "avant"
   AND v_pendant.periode = 'pendant'             -- Données de la période "pendant"
)

-- Sélection finale : on calcule les variations et on donne une interprétation
SELECT
  produit,                                   -- Nom du produit
  ca_avant,                                  -- CA avant la promotion
  ca_pendant,                                -- CA pendant la promotion
  ROUND(SAFE_DIVIDE(ca_pendant - ca_avant, ca_avant) * 100, 2) AS variation_CA_pct, -- Variation du CA en %
  qte_avant,                                 -- Quantité vendue avant promo
  qte_pendant,                               -- Quantité vendue pendant promo
  ROUND(SAFE_DIVIDE(qte_pendant - qte_avant, qte_avant) * 100, 2) AS variation_qte_pct, -- Variation des ventes en %
  CASE
    WHEN SAFE_DIVIDE(ca_pendant - ca_avant, ca_avant) >= 0.3 
      THEN 'Réagit très bien à la promo'     -- CA ↑ de 30% ou plus
    WHEN SAFE_DIVIDE(ca_pendant - ca_avant, ca_avant) <= 0.1 
      THEN 'Insensible à la promo'           -- CA ↑ de 10% ou moins
    ELSE 'Effet modéré'                      -- Entre 10% et 30%
  END AS interpretation                      -- Conclusion automatique
FROM comparaison
ORDER BY variation_CA_pct DESC               -- On classe les produits selon l’impact