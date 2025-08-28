-- 📁 models/marts/dim/dim_clients.sql
SELECT DISTINCT
  id_client,
  prenom_client,
  nom_client,
  email,
  age AS age,
  genre,
  frequence_visites AS frequence_visite,
  numero_telephone AS numero_telephone,
  favoris,
  adresse_ip
FROM {{ ref('stg_clients') }}