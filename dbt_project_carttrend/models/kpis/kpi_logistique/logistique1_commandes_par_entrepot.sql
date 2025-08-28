-- logistique1_commandes_par_entrepot.sql
-- Requête : Nombre de commandes et taux de remplissage par entrepôt
-- ------------------------------------------------------------------------------
-- Cette requête permet de :
--   Compter le nombre total de commandes passées par entrepôt et par date.
--   Associer chaque entrepôt à son taux de remplissage.
--   Suivre l’activité commerciale et d’évaluer la capacité utilisée.
-- ------------------------------------------------------------------------------

SELECT 
  c.id_entrepot,                          -- Identifiant unique de l’entrepôt (clé de la table des commandes)
  d.localisation,                         -- Localisation géographique de l’entrepôt (table de dimension)
  d1.date,                                -- Date de la commande (issue de la table de dimension date)
  COUNT(c.id_entrepot) AS nombre_de_commandes,  -- Nombre total de commandes enregistrées pour cet entrepôt et cette date
  d.taux_remplissage                      -- Taux de remplissage de l’entrepôt (indicateur de capacité utilisée)
FROM {{ ref('facts_commandes') }} AS c    -- Table de faits : commandes passées
JOIN {{ ref('dim_entrepots') }} AS d      -- Table de dimension : infos sur les entrepôts
  ON d.id_entrepot = c.id_entrepot        -- Jointure : relie chaque commande à son entrepôt
JOIN {{ ref('dim_date') }} AS d1          -- Table de dimension : infos sur le temps (dates)
  ON c.id_date_commande = d1.id_date      -- Jointure : associe la commande à sa date
GROUP BY 
  c.id_entrepot,     -- Regroupement par entrepôt
  d.taux_remplissage,-- Regroupement par taux de remplissage (lié à l’entrepôt)
  d.localisation,    -- Regroupement par localisation géographique
  d1.date            -- Regroupement par date
ORDER BY 
  nombre_de_commandes DESC  -- Classement : du plus grand au plus petit nombre de commandes