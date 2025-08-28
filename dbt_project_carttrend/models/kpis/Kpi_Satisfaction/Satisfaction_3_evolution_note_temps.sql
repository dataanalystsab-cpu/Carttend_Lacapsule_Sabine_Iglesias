-- Satisfaction_3_evolution_note_temps.sql
-- Requête : Analyse mensuelle de la satisfaction client basée sur les notes
-- -------------------------------------------------------------------------------
-- Cette requête permet de calculer la note moyenne client par mois, en ne retenant que les 
-- commandes valides (non annulées) et les avis non nuls.
-- Elle inclut uniquement les mois où au moins 10 avis ont été déposés, assurant 
-- ainsi une représentativité statistique suffisante.
-- -------------------------------------------------------------------------------

SELECT
  EXTRACT(YEAR FROM d.date) AS annee,                        -- Extraire l'année à partir de la date de commande
  EXTRACT(MONTH FROM d.date) AS mois,                        -- Extraire le mois à partir de la date de commande
  FORMAT_DATE('%Y-%m', d.date) AS periode,                   -- Formater la date en 'YYYY-MM' pour une période mensuelle lisible
  ROUND(AVG(s.note_client), 2) AS note_moyenne,              -- Calculer la note moyenne client, arrondie à 2 décimales
  COUNT(*) AS nb_avis                                         -- Compter le nombre total d'avis pour la période
FROM {{ ref('dim_satisfaction') }} s                         -- Table des avis clients avec notes par commande
JOIN {{ ref('facts_commandes') }} c                           -- Table des commandes factuelles
  ON s.id_commande = c.id_commande                           -- Lier chaque avis à la commande correspondante
JOIN {{ ref('dim_date') }} d                                 -- Table des dates pour obtenir la date réelle de la commande
  ON c.id_date_commande = d.id_date                          -- Lier la commande à sa date de commande
WHERE s.note_client IS NOT NULL                              -- Exclure les avis sans note (notes NULL)
  AND LOWER(c.statut_commande) NOT IN ('annulée', 'cancelled') -- Exclure les commandes annulées, en français et anglais
GROUP BY annee, mois, periode                                -- Regrouper les résultats par année, mois, et période formatée
HAVING nb_avis >= 10                                         -- Ne garder que les mois où au moins 10 avis ont été déposés (fiabilité statistique)
ORDER BY periode                                            -- Trier par période chronologiquement (du plus ancien au plus récent)