-- Ventes_8_produit_achetes_ensembles_catégories.sql
-- Requête : Analyse des associations fréquentes entre produits achetés ensemble
-- ---------------------------------------------------------------------------------
-- Objectif : Identifier les produits qui apparaissent souvent ensemble dans les commandes.
--            Cela permet de comprendre les habitudes d’achat et d’identifier
--            des opportunités de cross-selling (ventes croisées).
-- Métriques utilisées :
--   - nb_achats_ensemble : nombre de commandes contenant les deux produits
--   - confidence : probabilité d’acheter le produit_2 sachant que le produit_1 a été acheté
--   - lift : force de l’association (corrige la popularité individuelle des produits)
-- ---------------------------------------------------------------------------------

WITH commandes_produits AS (
  SELECT
    id_commande,
    id_details_produits AS id_produit         -- On récupère les produits présents dans chaque commande
  FROM {{ ref('dim_details_commandes') }}
),

produits_frequents AS (
  SELECT
    id_produit,
    COUNT(DISTINCT id_commande) AS nb_achats  -- Nombre de commandes contenant ce produit
  FROM commandes_produits
  GROUP BY id_produit
),

cooccurrences AS (
  SELECT
    cp1.id_produit AS produit_1,
    cp2.id_produit AS produit_2,
    COUNT(*) AS nb_achats_ensemble            -- Nombre de fois où les deux produits apparaissent ensemble
  FROM commandes_produits cp1
  JOIN commandes_produits cp2
    ON cp1.id_commande = cp2.id_commande      -- Les deux produits dans la même commande
   AND cp1.id_produit < cp2.id_produit        -- Évite les doublons (p1,p2) et (p2,p1)
  GROUP BY produit_1, produit_2
  HAVING nb_achats_ensemble > 1               -- On ne garde que les paires significatives
),

produits_nommes AS (
  SELECT
    id_produit AS id,
    produit                                    -- Nom du produit
  FROM {{ ref('dim_produits') }}
),

total_cmds AS (
  SELECT COUNT(DISTINCT id_commande) AS total  -- Nombre total de commandes
  FROM commandes_produits
)

SELECT
  p1.produit AS nom_produit_1,                -- Nom du premier produit
  p2.produit AS nom_produit_2,                -- Nom du second produit
  c.nb_achats_ensemble,                       -- Nombre de commandes contenant la paire
  f1.nb_achats AS nb_achats_p1,               -- Nombre de commandes contenant produit_1
  f2.nb_achats AS nb_achats_p2,               -- Nombre de commandes contenant produit_2

  ROUND(c.nb_achats_ensemble / f1.nb_achats, 3) AS confidence_p1_to_p2, 
  -- Confiance : probabilité que produit_2 soit acheté sachant que produit_1 l’est

  ROUND(
    (c.nb_achats_ensemble / f1.nb_achats) / (f2.nb_achats / total.total),
    3
  ) AS lift
  -- Lift : mesure de l’intensité de l’association
  -- Si lift > 1 => les produits sont achetés ensemble plus souvent que par hasard

FROM cooccurrences c
JOIN produits_frequents f1 ON c.produit_1 = f1.id_produit
JOIN produits_frequents f2 ON c.produit_2 = f2.id_produit
JOIN produits_nommes p1 ON c.produit_1 = p1.id
JOIN produits_nommes p2 ON c.produit_2 = p2.id
CROSS JOIN total_cmds total

ORDER BY lift DESC                             -- Classement par association la plus forte