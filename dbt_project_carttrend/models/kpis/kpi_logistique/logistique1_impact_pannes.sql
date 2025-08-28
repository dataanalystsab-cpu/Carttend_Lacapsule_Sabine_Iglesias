-- logistique1_impact_pannes.sql
-- Requête : Impact des pannes sur les volumes traités et identification des machines
-- ------------------------------------------------------------------------------
-- Cette requête permet de :
--   Identifier quelles machines connaissent le plus de pannes ou de maintenances.
--   Mesurer le temps total d’arrêt lié à ces incidents.
--   Comparer ce temps avec le volume total traité pour analyser l’impact opérationnel.
-- ------------------------------------------------------------------------------

SELECT 
  c.id_machine,                                   -- Identifiant unique de la machine
  d.localisation,                                -- Localisation géographique de l’entrepôt (table de dimension)
  c.mois,                                        -- Mois associé à l’enregistrement (analyse temporelle)
  
  COUNT(CASE                                     -- Compte le nombre d’incidents de panne ou de maintenance
           WHEN c.etat_machine IN ('En panne', 'En maintenance') 
           THEN c.temps_d_arret                   -- Chaque enregistrement "en panne/maintenance" est comptabilisé
        END) AS nombre_de_pannes,
  
  SUM(CASE                                       -- Somme des temps d’arrêt (en panne ou maintenance)
        WHEN c.etat_machine IN ('En panne', 'En maintenance') 
        THEN c.temps_d_arret 
        ELSE 0                                   -- Sinon, on ajoute 0 (machine en fonctionnement normal)
      END) AS temps_total_panne_ou_maintenance,
  
  SUM(c.volume_traite) AS volume_total_traite    -- Volume total traité par la machine (incluant périodes actives)
FROM {{ ref('facts_entrepots_machine') }} AS c   -- Table de faits : enregistrements par machine
JOIN {{ ref('dim_entrepots') }} AS d             -- Table de dimension : localisation des entrepôts
  ON c.id_entrepot = d.id_entrepot               -- Jointure entre machine et entrepôt
GROUP BY 
  c.id_machine,                                  -- Regroupement par machine (analyse individuelle)
  d.localisation,                                -- Regroupement par localisation d’entrepôt
  c.mois                                         -- Regroupement par mois pour analyse temporelle
ORDER BY 
  temps_total_panne_ou_maintenance DESC          -- Classement décroissant : les machines les plus impactées en haut