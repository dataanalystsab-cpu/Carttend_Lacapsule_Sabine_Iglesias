-- logistique1_arret_par_entrepot.sql
-- Requête : Analyse des temps d’arrêt des machines par entrepôt et par mois
-- --------------------------------------------------------------------------------
-- Cette requête permet de:
--   Calculer le temps total d’arrêt des machines par entrepôt et par mois.
--   Identifier les entrepôts les plus impactés par les arrêts techniques.
--   Suivre la performance opérationnelle et cibler les problèmes.
-- --------------------------------------------------------------------------------

SELECT 
  c.id_entrepot,                               -- Identifiant unique de l’entrepôt (clé venant de la table de faits)
  d.localisation,                              -- Localisation géographique de l’entrepôt (table de dimension)
  c.mois,                                      -- Mois associé aux données (analyse temporelle mensuelle)
  SUM(c.temps_d_arret) AS temps_d_arret_total  -- Somme des temps d’arrêt (toutes machines confondues) pour cet entrepôt et ce mois
FROM {{ ref('facts_entrepots_machine') }} AS c -- Table de faits : enregistrements des temps d’arrêt par machine et entrepôt
JOIN {{ ref('dim_entrepots') }} AS d           -- Table de dimension : infos descriptives des entrepôts
  ON c.id_entrepot = d.id_entrepot             -- Jointure : relie les faits avec la dimension via l’ID entrepôt
GROUP BY 
  c.id_entrepot,   -- Regroupement par identifiant d’entrepôt
  d.localisation,  -- Regroupement par localisation géographique (permet lisibilité et vérification)
  c.mois           -- Regroupement par mois, pour obtenir un total mensuel
ORDER BY 
  SUM(c.temps_d_arret) DESC  -- Classement : du plus grand au plus petit temps d’arrêt total