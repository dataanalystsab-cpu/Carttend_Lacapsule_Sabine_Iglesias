-- Satisfaction_2_moins_bonnes_notes.sql
-- Requête : Analyse des produits les moins bien notés avec un nombre minimum d'avis
-- -------------------------------------------------------------------------------
-- Cette requête permet d'identifier les produits ayant reçu au moins 5 avis clients,
-- en calculant leur note moyenne. Elle classe les produits du moins bien notés
-- au mieux notés pour détecter ceux qui pourraient poser problème ou nécessiter
-- une attention particulière.
-- -------------------------------------------------------------------------------

SELECT
  p.categorie,                                -- Catégorie du produit
  p.id_produit,                               -- Identifiant du produit
  p.produit,                                  -- Nom du produit
  ROUND(AVG(s.note_client), 2) AS note_moyenne,  -- Moyenne des notes clients
  COUNT(*) AS nb_avis                         -- Nombre total d'avis pour le produit
FROM {{ ref('dim_satisfaction') }} s
JOIN {{ ref('dim_details_commandes') }} dc
  ON s.id_commande = dc.id_commande           -- Lien entre satisfaction et commande
JOIN {{ ref('dim_produits') }} p
  ON dc.id_details_produits = p.id_produit    -- Lien entre détails commandes et produit
GROUP BY p.categorie, p.id_produit, p.produit
HAVING COUNT(*) >= 5                          -- Minimum 5 avis pour être significatif
ORDER BY note_moyenne ASC                    -- Du produit le moins apprécié au plus apprécié