{{ config(
    materialized='table'
) }}

-- models/staging/stg_clients.sql
WITH clients_prepares AS (
  SELECT
    TRIM(id_client) AS id_client,
    TRIM(prenom_client) AS prenom_client,
    TRIM(nom_client) AS nom_client,
    TRIM(email) AS email,
    SAFE_CAST(age AS INT64) AS age,
    TRIM(genre) AS genre,
    SAFE_CAST(frequence_visites AS INT64) AS frequence_visites,
    TRIM(numero_telephone) AS numero_telephone,
    TRIM(favoris) AS favoris,
    TRIM(adresse_ip) AS adresse_ip,

    ROW_NUMBER() OVER (
      PARTITION BY SAFE_CAST(id_client AS STRING)
      ORDER BY
        (
          IF(prenom_client IS NOT NULL AND TRIM(prenom_client) <> '', 1, 0) +
          IF(nom_client IS NOT NULL AND TRIM(nom_client) <> '', 1, 0) +
          IF(email IS NOT NULL AND TRIM(email) <> '', 1, 0) +
          IF(age IS NOT NULL, 1, 0) +
          IF(genre IS NOT NULL AND TRIM(genre) <> '', 1, 0) +
          IF(frequence_visites IS NOT NULL, 1, 0) +
          IF(numero_telephone IS NOT NULL AND TRIM(numero_telephone) <> '', 1, 0) +
          IF(favoris IS NOT NULL AND TRIM(favoris) <> '', 1, 0) +
          IF(adresse_ip IS NOT NULL AND TRIM(adresse_ip) <> '', 1, 0)
        ) DESC
    ) AS rn

  FROM {{ source('carttrend_rawdata', 'carttrend_clients') }}

  WHERE
    id_client IS NOT NULL
    AND prenom_client IS NOT NULL
    AND nom_client IS NOT NULL
)

SELECT
  id_client,
  prenom_client,
  nom_client,
  email,
  age,
  genre,
  frequence_visites,
  numero_telephone,
  favoris,
  adresse_ip
FROM clients_prepares
WHERE rn = 1
