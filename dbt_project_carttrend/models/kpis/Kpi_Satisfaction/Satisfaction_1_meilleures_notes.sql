-- Satisfaction_1_meilleures_notes.sql
-- Requête : Analyse des produits les mieux notés par catégorie
-- -------------------------------------------------------------------------------
-- Cette requête permet de calculer la note moyenne client et le nombre d'avis par produit,
-- en les regroupant par catégorie de produit. Elle permet d'identifier les
-- produits les mieux notés et les plus appréciés par les clients.
-- -------------------------------------------------------------------------------

SELECT
  p.categorie,                -- Catégorie du produit
  p.Produit,                  -- Nom du produit
  ROUND(AVG(s.note_client), 2) AS note_moyenne,  -- Note moyenne arrondie à 2 décimales
  COUNT(*) AS nb_avis         -- Nombre total d'avis reçus
FROM {{ ref('dim_satisfaction') }} s
JOIN {{ ref('dim_details_commandes') }} dc
  ON s.id_commande = dc.id_commande  -- Lien entre avis et détails de commande
JOIN {{ ref('dim_produits') }} p
  ON dc.id_details_produits = p.id_produit             -- Lien avec la table produits pour obtenir infos produit
GROUP BY p.categorie, p.Produit     -- Agrégation par catégorie et produit
ORDER BY note_moyenne DESC          -- Classement décroissant par note moyenne