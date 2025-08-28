-- 📁 models/marts/dim/dim_entrepots.sql
SELECT DISTINCT
  id_entrepot AS id_entrepot,
  localisation,
  capacite_max AS capacite_max,
  volume_stocke AS volume_stocke,
  taux_remplissage,
  temperature_moyenne_entrepot AS temp_moyenne
FROM {{ ref('stg_entrepots') }}