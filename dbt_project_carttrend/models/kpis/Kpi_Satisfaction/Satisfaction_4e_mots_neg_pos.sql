-- Satisfaction_4e_mots_neg_pos.sql
-- Requête : Analyse des mots-clés dans les commentaires clients selon la note donnée
-- -------------------------------------------------------------------------------
-- Cette requête permet d'identifier les thématiques les plus fréquemment mentionnées dans les commentaires
-- en fonction du niveau de satisfaction attribué (note_client).
-- Cela permet de relier directement les retours textuels aux niveaux de satisfaction.
-- -------------------------------------------------------------------------------

SELECT
  note_client,                                 --  Niveau de satisfaction donné par le client

  COUNT(*) AS nb_commentaires,                 -- Nombre total de commentaires associés à cette note

  -- Comptage des occurrences de mots-clés dans les commentaires
  SUM(CASE WHEN LOWER(commentaire) LIKE '%damaged%' THEN 1 ELSE 0 END) AS nb_damaged,          -- Produit endommagé
  SUM(CASE WHEN LOWER(commentaire) LIKE '%very happy%' THEN 1 ELSE 0 END) AS nb_very_happy,    -- Client très satisfait
  SUM(CASE WHEN LOWER(commentaire) LIKE '%delivery%' THEN 1 ELSE 0 END) AS nb_delivery,        -- Problèmes ou mentions de livraison
  SUM(CASE WHEN LOWER(commentaire) LIKE '%quality%' THEN 1 ELSE 0 END) AS nb_quality,          -- Qualité du produit
  SUM(CASE WHEN LOWER(commentaire) LIKE '%product%' THEN 1 ELSE 0 END) AS nb_product,          -- Référence explicite au produit
  SUM(CASE WHEN LOWER(commentaire) LIKE '%not great%' THEN 1 ELSE 0 END) AS nb_not_great,      -- Avis négatif explicite
  SUM(CASE WHEN LOWER(commentaire) LIKE '%experience%' THEN 1 ELSE 0 END) AS nb_experience,    -- Expérience globale
  SUM(CASE WHEN LOWER(commentaire) LIKE '%service%' THEN 1 ELSE 0 END) AS nb_service           -- Service client
FROM {{ ref('dim_satisfaction') }}                         --  Table des avis clients
WHERE commentaire IS NOT NULL                              --  Exclure les commentaires vides
GROUP BY note_client                                       --  Regrouper par niveau de satisfaction
ORDER BY note_client ASC                                   --  Trier du plus insatisfait au plus satisfait