-- =======================================================================================  
-- OBJECTIF :
-- Cette requête analyse l’impact des promotions sur les ventes produits.
-- Elle compare le chiffre d’affaires et les quantités vendues :
--   - 30 jours avant la promotion
--   - pendant la promotion
--   - 30 jours après la promotion
-- Puis elle calcule uniquement la variation % entre "pendant" et la moyenne de (avant + après).
-- Elle affiche le produit avec sa variation dans le nom seulement si nécessaire.
-- =======================================================================================

-- Étape 1 : Associer commandes, produits et promotions
WITH ventes_par_periode AS (
  SELECT
    p.id_produit,   -- Identifiant unique du produit
    p.produit,      -- Nom du produit
    p.variation,    -- Variation (ex: couleur, taille, édition spéciale)
    p.prix,         -- Prix catalogue (hors promo)
    pr.id_promotion, -- Identifiant unique de la promotion

    -- Classification temporelle par rapport aux dates de promo
    CASE
      WHEN dt3.date BETWEEN DATE_SUB(dt1.date, INTERVAL 30 DAY) 
                        AND DATE_SUB(dt1.date, INTERVAL 1 DAY) THEN 'avant'   -- 30j avant la promo
      WHEN dt3.date BETWEEN dt1.date AND dt2.date THEN 'pendant'              -- période promo
      WHEN dt3.date BETWEEN DATE_ADD(dt2.date, INTERVAL 1 DAY) 
                        AND DATE_ADD(dt2.date, INTERVAL 30 DAY) THEN 'apres'  -- 30j après la promo
      ELSE 'autre'                                                            -- hors plage (non utilisé ensuite)
    END AS periode,

    -- Calcul du prix appliqué : on choisit prix promo si "pendant" sinon prix normal
    CASE
      WHEN dt3.date BETWEEN dt1.date AND dt2.date THEN
        GREATEST(   -- GREATEST évite un prix négatif
          CASE
            -- Si promo en pourcentage (ex: "20%")
            WHEN REGEXP_CONTAINS(pr.valeur_promotion, r'%$') THEN
              p.prix * (1 - CAST(REGEXP_REPLACE(pr.valeur_promotion, r'[%]', '') AS FLOAT64) / 100)
            -- Sinon promo en valeur fixe (ex: "€10")
            ELSE
              p.prix - CAST(REGEXP_REPLACE(REGEXP_REPLACE(pr.valeur_promotion, r'€', ''), r',', '.') AS FLOAT64)
          END,
          0 -- minimum 0 pour éviter un prix négatif
        )
      ELSE p.prix  -- Prix catalogue si hors période promo
    END AS prix_applique,

    dc.quantite  -- Quantité commandée

  FROM {{ ref('facts_commandes') }} c   -- Table des commandes
  JOIN {{ ref('dim_details_commandes') }} dc  -- Détails des commandes (quantité, produit)
    ON c.id_commande = dc.id_commande
  JOIN {{ ref('dim_produits') }} p   -- Détails des produits
    ON dc.id_details_produits = p.id_produit
  JOIN {{ ref('dim_promotions') }} pr   -- Promotions appliquées
    ON pr.id_produit = p.id_produit
  JOIN {{ ref('dim_date') }} dt1   -- Date début de la promo
    ON dt1.id_date = pr.id_date_debut
  JOIN {{ ref('dim_date') }} dt2   -- Date fin de la promo
    ON dt2.id_date = pr.id_date_fin
  JOIN {{ ref('dim_date') }} dt3   -- Date de la commande
    ON dt3.id_date = c.id_date_commande
  WHERE dt3.date BETWEEN DATE_SUB(dt1.date, INTERVAL 30 DAY) 
                     AND DATE_ADD(dt2.date, INTERVAL 30 DAY)  -- On garde seulement la fenêtre avant/pendant/après
    AND LOWER(c.statut_commande) NOT IN ('annulée', 'cancelled') -- Exclure les commandes annulées
)

-- Étape 2 : Agrégation CA et quantités par période
, agr AS (
  SELECT
    id_produit,     -- Identifiant produit
    produit,        -- Nom du produit
    variation,      -- Variation éventuelle
    prix,           -- Prix catalogue
    id_promotion,   -- Promotion associée
    periode,        -- "avant", "pendant", "apres"
    SUM(quantite * prix_applique) AS chiffre_affaires,  -- CA par période
    SUM(quantite) AS quantite_vendue                   -- Quantités vendues par période
  FROM ventes_par_periode
  GROUP BY id_produit, produit, variation, prix, id_promotion, periode
)

-- Étape 3 : Recomposer en colonnes "avant / pendant / après"
, comparaison AS (
  SELECT
    id_produit,
    produit,
    variation,
    prix,
    id_promotion,
    MAX(IF(periode = 'avant', chiffre_affaires, 0)) AS ca_avant,      -- CA avant promo
    MAX(IF(periode = 'pendant', chiffre_affaires, 0)) AS ca_pendant,  -- CA pendant promo
    MAX(IF(periode = 'apres', chiffre_affaires, 0)) AS ca_apres,      -- CA après promo
    MAX(IF(periode = 'avant', quantite_vendue, 0)) AS qte_avant,      -- Quantité avant promo
    MAX(IF(periode = 'pendant', quantite_vendue, 0)) AS qte_pendant,  -- Quantité pendant promo
    MAX(IF(periode = 'apres', quantite_vendue, 0)) AS qte_apres       -- Quantité après promo
  FROM agr
  GROUP BY id_produit, produit, variation, prix, id_promotion
)

-- Étape 4 : Calcul variation seulement "pendant" vs moyenne (avant + après)
, produits_final AS (
  SELECT
    c.id_produit,
    -- Si le produit a plusieurs prix distincts, on ajoute la variation pour différencier
    CASE
      WHEN COUNT(DISTINCT c.prix) OVER (PARTITION BY c.produit) > 1 AND c.variation IS NOT NULL
        THEN CONCAT(c.produit, ' - ', c.variation)
      ELSE c.produit
    END AS produit_final,

    c.id_promotion,

    -- Valeurs brutes
    c.ca_avant,     -- Chiffre d'affaires avant
    c.ca_pendant,   -- Chiffre d'affaires pendant
    c.ca_apres,     -- Chiffre d'affaires après
    c.qte_avant,    -- Quantités avant
    c.qte_pendant,  -- Quantités pendant
    c.qte_apres,    -- Quantités après

    -- Variation % CA (pendant vs moyenne de avant/après)
    ROUND(
      SAFE_DIVIDE(
        c.ca_pendant - ((c.ca_avant + c.ca_apres) / 2),   -- Numérateur = différence avec la moyenne
        ((c.ca_avant + c.ca_apres) / 2)                   -- Dénominateur = moyenne
      ) * 100, 2
    ) AS variation_ca_pct_moy,

    -- Variation % Quantité (pendant vs moyenne de avant/après)
    ROUND(
      SAFE_DIVIDE(
        c.qte_pendant - ((c.qte_avant + c.qte_apres) / 2), -- Idem pour les quantités
        ((c.qte_avant + c.qte_apres) / 2)
      ) * 100, 2
    ) AS variation_qte_pct_moy

  FROM comparaison c
)

-- Étape 5 : Résultat final
SELECT *
FROM produits_final
ORDER BY ca_pendant DESC   -- Trier les produits par CA pendant promo
