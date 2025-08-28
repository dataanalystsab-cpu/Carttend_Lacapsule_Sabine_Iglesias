-- Ventes_3_15_produits_moins_vendus.sql
-- Requête : Top 15 des produits les moins vendus
-- Cette requête permet d’identifier les produits qui se vendent le moins (faible rotation).
-- Utile pour prendre des décisions : retirer ces produits du catalogue, ajuster le prix,
-- ou mettre en place des actions promotionnelles.

SELECT 
    p.produit,                         -- On récupère le nom du produit
    SUM(dc.quantite) AS total_vendus,  -- On calcule le total des quantités vendues pour ce produit
    'moins_vendus' AS type_resultat    -- On ajoute une étiquette pour préciser qu’il s’agit d’un produit faiblement vendu

FROM {{ ref('dim_details_commandes') }} dc -- On part de la table des détails de commandes (ventes réelles)
JOIN {{ ref('dim_produits') }} p           -- On joint avec la table des produits pour récupérer les infos produit
    ON dc.id_details_produits = p.id_produit -- Condition de jointure entre produit et détails de commande

GROUP BY p.produit                -- On regroupe par produit pour agréger les ventes
HAVING total_vendus > 0           -- On exclut les produits jamais vendus (déjà couverts par la requête précédente)
ORDER BY total_vendus ASC         -- On trie par ordre croissant de ventes (du moins vendu au plus vendu)