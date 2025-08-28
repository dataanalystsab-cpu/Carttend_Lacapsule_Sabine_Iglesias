-- 📁 models/marts/dim/dim_satisfaction.sql
SELECT
  id_satisfaction,
  note_client,
  commentaire,
  CAST(plainte AS STRING) AS plainte,
  temps_reponse_support AS temps_de_reponse,
  type_plainte,
  employe_support AS employe_support,
  id_commande
FROM {{ ref('stg_satisfaction') }}