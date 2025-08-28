-- OBJECTIF DE LA REQUÊTE :
-- Identifier les paires de produits qui sont achetées ensemble par un même client le même jour,
-- afin d’analyser les co-occurrences et mieux comprendre les associations d’achats.
-- Les paires uniques (n’apparaissant qu’une fois) sont ignorées pour ne garder que les associations récurrentes.

WITH prix_par_nom AS ( 
    -- Étape 1 : Vérifie si un produit existe avec plusieurs prix différents (variations tarifaires)
    SELECT
        p.produit,                            -- Nom du produit (sans variation pour l’instant)
        COUNT(DISTINCT p.prix) AS nb_prix     -- Nombre de prix distincts pour ce produit
    FROM {{ ref('dim_produits') }} p          -- Table de référence des produits
    GROUP BY p.produit                        -- Regroupe par produit
),

produits_commandes_raw AS ( 
    -- Étape 2 : Liste brute des produits achetés par client et par commande
    SELECT
        fc.id_client,                         -- Identifiant du client
        fc.id_date_commande,                  -- Date de la commande (clé de la dimension date)
        fc.id_commande,                       -- Identifiant unique de la commande
        p.id_produit,                         -- Identifiant du produit (clé primaire de dim_produits)
        p.categorie,                          -- Catégorie du produit
        CASE
            -- Si un produit a plusieurs prix et possède une variation,
            -- on distingue les produits en concaténant "produit - variation"
            WHEN ppn.nb_prix > 1 AND p.variation IS NOT NULL
                THEN CONCAT(p.produit, ' - ', p.variation)
            -- Sinon, on garde juste le nom du produit
            ELSE p.produit
        END AS nom_produit
    FROM {{ ref('facts_commandes') }} fc      -- Table de faits des commandes (niveau commande)
    JOIN {{ ref('dim_details_commandes') }} ddc
        ON fc.id_commande = ddc.id_commande   -- Jointure : relie une commande à ses détails (lignes d’achat)
    JOIN {{ ref('dim_produits') }} p
        ON ddc.id_details_produits = p.id_produit  -- Jointure : récupère les infos produit
    JOIN prix_par_nom ppn
        ON ppn.produit = p.produit            -- Jointure : ajoute le nombre de prix distincts par produit
),

produits_commandes AS (
    -- Étape 3 : Déduplique les produits par client/jour/commande
    -- (évite de compter deux fois le même produit s’il est acheté en quantité > 1)
    SELECT DISTINCT
        id_client,                            -- Client
        id_date_commande,                     -- Date de la commande
        id_commande,                          -- Identifiant de la commande
        id_produit,                           -- Identifiant produit
        categorie,                            -- Catégorie produit
        nom_produit                           -- Nom unique du produit (avec variation si besoin)
    FROM produits_commandes_raw
),

paires AS (
    -- Étape 4 : Forme toutes les paires de produits achetés par un client le même jour
    SELECT
        pc1.id_client,                        -- Client
        pc1.id_date_commande,                 -- Date d’achat
        -- On ordonne les noms pour éviter les doublons (A,B) et (B,A)
        LEAST(pc1.nom_produit, pc2.nom_produit) AS produit_A,
        GREATEST(pc1.nom_produit, pc2.nom_produit) AS produit_B,
        -- On rattache aussi les identifiants produits correspondants
        CASE WHEN pc1.nom_produit < pc2.nom_produit THEN pc1.id_produit ELSE pc2.id_produit END AS id_produit_A,
        CASE WHEN pc1.nom_produit < pc2.nom_produit THEN pc2.id_produit ELSE pc1.id_produit END AS id_produit_B,
        -- On rattache aussi les catégories correspondantes
        CASE WHEN pc1.nom_produit < pc2.nom_produit THEN pc1.categorie ELSE pc2.categorie END AS categorie_A,
        CASE WHEN pc1.nom_produit < pc2.nom_produit THEN pc2.categorie ELSE pc1.categorie END AS categorie_B
    FROM produits_commandes pc1
    JOIN produits_commandes pc2
        -- On combine chaque produit avec les autres produits achetés
        ON pc1.id_client = pc2.id_client
       AND pc1.id_date_commande = pc2.id_date_commande
       -- Condition pour éviter doublons et auto-jointure (produit avec lui-même)
       AND pc1.nom_produit < pc2.nom_produit
)

-- Étape 5 : Compte les occurrences de chaque paire de produits
SELECT
    produit_A,                                -- Premier produit de la paire
    MIN(id_produit_A) AS id_produit_A,        -- Identifiant du produit A
    MIN(categorie_A)  AS categorie_A,         -- Catégorie du produit A
    produit_B,                                -- Second produit de la paire
    MIN(id_produit_B) AS id_produit_B,        -- Identifiant du produit B
    MIN(categorie_B)  AS categorie_B,         -- Catégorie du produit B
    COUNT(*) AS nb_occurrences                -- Nombre d’occurrences de la paire (combien de fois vue)
FROM paires
GROUP BY produit_A, produit_B                 -- On regroupe uniquement par les noms des produits
HAVING COUNT(*) > 1                           -- On ne garde que les paires récurrentes (au moins 2 fois)
ORDER BY nb_occurrences DESC                -- On trie de la paire la plus fréquente à la moins fréquente
