-- -------------------------------------------------------------------------------
-- Analyse des mots-clés présents dans les commentaires selon la note de satisfaction
-- -------------------------------------------------------------------------------
-- Permet de compter le nombre de commentaires contenant certains mots-clés 
-- (ex. "average quality", "long", "damaged") regroupés par niveau de note client.
-- Cela permet d’identifier quels termes négatifs ou positifs sont associés à chaque score.
-- -------------------------------------------------------------------------------

SELECT
  note_client,                       -- Niveau de satisfaction donné par le client

  COUNT(*) AS nb_commentaires,      -- Nombre total de commentaires pour cette note

  -- Comptage des commentaires mentionnant "average quality" (qualité moyenne)
  SUM(CASE WHEN LOWER(commentaire) LIKE '%average quality%' THEN 1 ELSE 0 END) AS nb_average_quality,

  -- Comptage des commentaires mentionnant "long" (longue attente, délai, etc.)
  SUM(CASE WHEN LOWER(commentaire) LIKE '%long%' THEN 1 ELSE 0 END) AS nb_long,

  -- Comptage des commentaires mentionnant "damaged" (produit endommagé)
  SUM(CASE WHEN LOWER(commentaire) LIKE '%damaged%' THEN 1 ELSE 0 END) AS nb_damaged

FROM {{ ref('dim_satisfaction') }}
WHERE commentaire IS NOT NULL             -- Ne considérer que les commentaires non vides
GROUP BY note_client                      -- Regrouper par note de satisfaction
ORDER BY note_client ASC                  -- Trier du score le plus bas au plus élevé