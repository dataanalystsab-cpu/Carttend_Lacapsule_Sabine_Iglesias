-- 📁 models/marts/facts/facts_entrepots_machines.sql
SELECT DISTINCT
  id AS id_intervention_machine,
  etat_machine AS etat_machine,
  temps_d_arret AS temps_d_arret,
  volume_traite AS volume_traite,
  mois,
  id_machine,
  id_entrepot AS id_entrepot
FROM {{ ref('stg_entrepots_machine') }}