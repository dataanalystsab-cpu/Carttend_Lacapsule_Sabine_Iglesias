-- Ventes_2_produits_invendus.sql
-- Requête : Liste des produits jamais vendus
-- Cette requête permet d’identifier les produits du catalogue qui n’ont jamais été commandés
-- (aucune ligne de commande associée), ce qui peut servir pour des analyses de performance produit.

-- Produits invendus
SELECT 
    p.produit,                         -- On récupère le nom du produit
    0 AS total_vendus,                 -- On fixe le nombre de ventes à 0 car le produit n’a jamais été vendu
    'jamais_vendu' AS type_resultat     -- On ajoute une étiquette pour préciser qu’il s’agit d’un produit invendu

FROM {{ ref('dim_produits') }} p        -- On part de la table des produits (le catalogue complet)
LEFT JOIN {{ ref('dim_details_commandes') }} dc -- On fait une jointure externe gauche avec les détails de commande
    ON p.id_produit = dc.id_details_produits    -- Condition de jointure entre produit et détails de commande

WHERE dc.id_details_produits IS NULL   -- Filtre : on garde uniquement les produits sans aucune correspondance dans les commandes