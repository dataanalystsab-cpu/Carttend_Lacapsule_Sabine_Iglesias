-- Marketing_Ticket2_1_vlvente_avec_sans_promo.sql
-- Requête : Analyse CA promo vs hors promo (version "jour complet")
--------------------------------------------------------------------------------
-- Cette requête permet de comparer la performance commerciale des jours avec promotion
-- et des jours sans promotion (CA total, nb de jours, CA moyen/jour
-- et ratios de comparaison).
---------------------------------------------------------------------------------

WITH dim_details_commandes AS (      -- CTE 1 : préparation d'une vue "à plat" des lignes de détail
    SELECT
        ROW_NUMBER() OVER (ORDER BY id_commande, id_details_produits) AS id_produit_tech, -- Identifiant technique (facultatif) pour les lignes
        quantite,                                   -- Quantité commandée sur la ligne
        emballage_special,                          -- Indicateur d’emballage spécial (si présent dans la source)
        id_commande,                                -- Clé de la commande
        id_details_produits                         -- Identifiant du produit présent sur la ligne de commande
    FROM {{ ref('dim_details_commandes') }}         -- Source : table des détails de commandes
),

-- Étape 1 : identifier les jours qui sont en promo
jours_promo AS (                                    -- CTE 2 : liste des dates où une promotion est active
    SELECT DISTINCT dt1.date AS date_commande       -- On récupère chaque date de commande une seule fois
    FROM dim_details_commandes ddc                  -- On part des lignes de détail (déjà préparées)
    JOIN {{ ref('facts_commandes') }} c             -- Jointure pour accéder aux commandes (statut, date, etc.)
        ON ddc.id_commande = c.id_commande          -- Lien logique détail -> commande
    JOIN {{ ref('dim_date') }} dt1                  -- Dimension date pour obtenir la date "réelle" de commande
        ON c.id_date_commande = dt1.id_date         -- Lien commande -> date de commande
    JOIN {{ ref('dim_produits') }} p                -- Dimension produits (pour relier aux promotions)
        ON ddc.id_details_produits = p.id_produit   -- Lien détail -> produit
    LEFT JOIN {{ ref('dim_promotions') }} pr        -- On cherche les promotions éventuelles sur ce produit
        ON p.id_produit = pr.id_produit             -- Lien produit -> promotion
    LEFT JOIN {{ ref('dim_date') }} dt2             -- Date de début de promotion (dimension date)
        ON pr.id_date_debut = dt2.id_date           -- Lien promo -> date début
    LEFT JOIN {{ ref('dim_date') }} dt3             -- Date de fin de promotion (dimension date)
        ON pr.id_date_fin = dt3.id_date             -- Lien promo -> date fin
    WHERE LOWER(c.statut_commande) NOT IN ('annulée', 'cancelled') -- On ignore les commandes annulées
      AND pr.id_produit IS NOT NULL                 -- On ne garde que les produits qui ont une promo
      AND dt1.date BETWEEN dt2.date AND dt3.date    -- La date de commande doit tomber pendant la promo
),

-- Étape 2 : associer chaque vente au statut "jour promo" ou "jour hors promo"
ventes_taggees AS (                                 -- CTE 3 : tag des ventes selon le type de jour
    SELECT
        ddc.id_details_produits,                    -- Identifiant produit côté détail
        p.id_produit AS id_produit_reel,            -- Identifiant produit côté dimension produit
        ddc.quantite,                               -- Quantité vendue sur la ligne
        p.prix,                                     -- Prix unitaire du produit
        dt1.date AS date_commande,                  -- Date de la commande
        CASE                                        -- Attribution du statut de la vente selon la date
            WHEN jp.date_commande IS NOT NULL THEN 'jour_promo'  -- Si la date est dans la liste des jours promo
            ELSE 'jour_hors_promo'                               -- Sinon, jour hors promo
        END AS statut_jour
    FROM dim_details_commandes ddc                  -- Repart des lignes de détail
    JOIN {{ ref('facts_commandes') }} c             -- Jointure commandes (statut, date…)
        ON ddc.id_commande = c.id_commande          -- Lien détail -> commande
    JOIN {{ ref('dim_date') }} dt1                  -- Dimension date pour récupérer la date de commande
        ON c.id_date_commande = dt1.id_date         -- Lien commande -> date
    JOIN {{ ref('dim_produits') }} p                -- Dimension produits (pour récupérer le prix)
        ON ddc.id_details_produits = p.id_produit   -- Lien détail -> produit
    LEFT JOIN jours_promo jp                        -- Jointure pour savoir si cette date est un "jour promo"
        ON dt1.date = jp.date_commande              -- Lien date -> jour promo
    WHERE LOWER(c.statut_commande) NOT IN ('annulée', 'cancelled') -- Exclusion des commandes annulées
),

-- Étape 3 : calcul CA total, jours distincts et CA moyen/jour
resume_ca AS (                                      -- CTE 4 : agrégation par type de jour
    SELECT
        statut_jour,                                -- Type de jour : 'jour_promo' / 'jour_hors_promo'
        ROUND(SUM(quantite * prix), 2) AS chiffre_affaires_total,  -- CA total du groupe
        COUNT(DISTINCT date_commande) AS nb_jours_vente,           -- Nombre de jours distincts observés
        ROUND(SUM(quantite * prix) / NULLIF(COUNT(DISTINCT date_commande), 0), 2) AS ca_moyen_par_jour -- CA/jour
    FROM ventes_taggees                              -- Base : ventes taggées par statut de jour
    GROUP BY statut_jour                             -- Agrégation par type de jour
),

-- Étape 4 : transformer en colonnes
pivot_ca AS (                                       -- CTE 5 : pivot pour comparaison côte à côte
    SELECT
        MAX(CASE WHEN statut_jour = 'jour_promo' THEN chiffre_affaires_total END) AS ca_jour_promo,           -- CA total sur jours promo
        MAX(CASE WHEN statut_jour = 'jour_hors_promo' THEN chiffre_affaires_total END) AS ca_jour_hors_promo, -- CA total sur jours hors promo
        MAX(CASE WHEN statut_jour = 'jour_promo' THEN ca_moyen_par_jour END) AS ca_moyen_jour_promo,          -- CA moyen/jour (promo)
        MAX(CASE WHEN statut_jour = 'jour_hors_promo' THEN ca_moyen_par_jour END) AS ca_moyen_jour_hors_promo,-- CA moyen/jour (hors promo)
        MAX(CASE WHEN statut_jour = 'jour_promo' THEN nb_jours_vente END) AS nb_jours_promo,                  -- Nb de jours promo
        MAX(CASE WHEN statut_jour = 'jour_hors_promo' THEN nb_jours_vente END) AS nb_jours_hors_promo         -- Nb de jours hors promo
    FROM resume_ca                                   -- Source : agrégats par type de jour
)

SELECT                                              -- Résultat final : comparatif + ratios
    ca_jour_promo,                                  -- Colonne : CA total jours promo
    ca_jour_hors_promo,                             -- Colonne : CA total jours hors promo
    nb_jours_promo,                                 -- Colonne : nombre de jours promo
    nb_jours_hors_promo,                            -- Colonne : nombre de jours hors promo
    ca_moyen_jour_promo,                            -- Colonne : CA moyen par jour (promo)
    ca_moyen_jour_hors_promo,                       -- Colonne : CA moyen par jour (hors promo)
    ROUND(SAFE_DIVIDE(ca_jour_promo, ca_jour_hors_promo), 2) AS ratio_ca_total_promo_vs_hors,         -- KPI : ratio CA total Promo/Hors
    ROUND(SAFE_DIVIDE(ca_moyen_jour_promo, ca_moyen_jour_hors_promo), 2) AS ratio_ca_moyen_jour_promo_vs_hors -- KPI : ratio CA/jour Promo/Hors
FROM pivot_ca                                       -- Lecture de la table pivot pour la comparaison finale