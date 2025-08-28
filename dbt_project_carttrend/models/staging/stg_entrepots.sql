{{ config(
    materialized='table'
) }}

WITH entrepots_prepare AS (
  SELECT
    TRIM(id_entrepot) AS id_entrepot,
    TRIM(localisation) AS localisation,
    SAFE_CAST(capacite_max AS INT64) AS capacite_max,
    SAFE_CAST(volume_stocke AS INT64) AS volume_stocke,
    SAFE_CAST(taux_remplissage AS FLOAT64) AS taux_remplissage,
    SAFE_CAST(temperature_moyenne_entrepot AS FLOAT64) AS temperature_moyenne_entrepot,

    -- Score de complétude pour identifier les lignes les plus riches en données
    ROW_NUMBER() OVER (
      PARTITION BY TRIM(id_entrepot)
      ORDER BY (
        IF(TRIM(localisation) <> '', 1, 0) +
        IF(capacite_max IS NOT NULL, 1, 0) +
        IF(volume_stocke IS NOT NULL, 1, 0) +
        IF(taux_remplissage IS NOT NULL, 1, 0) +
        IF(temperature_moyenne_entrepot IS NOT NULL, 1, 0)
      ) DESC
    ) AS rn

  FROM {{ source('carttrend_rawdata', 'carttrend_entrepots') }}
  WHERE
    id_entrepot IS NOT NULL AND TRIM(id_entrepot) <> ''
)

SELECT
  id_entrepot,
  localisation,
  capacite_max,
  volume_stocke,
  taux_remplissage,
  temperature_moyenne_entrepot
FROM entrepots_prepare
WHERE rn = 1
