-- Ventes_7_ajout_favoris.sql
-- Requête : Analyse des produits les plus ajoutés aux favoris
-- ------------------------------------------------------------------------------
-- Objectif : Identifier les produits qui sont le plus souvent ajoutés en favoris par les clients.
--            Cela permet de mesurer l’intérêt des clients et de repérer les produits 
--            qui suscitent le plus d’intentions d’achat (même sans passage immédiat en commande).
-- ------------------------------------------------------------------------------

SELECT
    p.produit,                                 -- Nom du produit
    COUNT(f.id_client) AS nb_fois_ajoute       -- Nombre de fois où le produit a été ajouté aux favoris

FROM {{ ref('dim_produits') }} p               -- Table des produits (catalogue)
JOIN {{ ref('dim_favoris') }} f                -- Table des favoris des clients
    ON p.id_produit = CONCAT('P', LPAD(SUBSTR(f.favoris, 2), 5, '0'))
    -- Ici, on fait correspondre l’ID produit avec le champ "favoris" de la table dim_favoris
    -- en reconstruisant le format d’identifiant (ex : P00001).

GROUP BY p.id_produit, p.produit               -- Agrégation par produit
ORDER BY nb_fois_ajoute DESC                   -- Tri décroissant (du plus populaire au moins populaire)