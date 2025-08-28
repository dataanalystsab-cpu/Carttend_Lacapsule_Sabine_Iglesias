{{ config(
    materialized='table'
) }}

WITH campagnes_preparees AS (
  SELECT
    TRIM(id_campagne) AS id_campagne,
    SAFE_CAST(date AS DATE) AS date,
    TRIM(evenement_oui_non) AS evenement_oui_non,
    TRIM(evenement_type) AS evenement_type,
    TRIM(canal) AS canal,
    SAFE_CAST(budget AS FLOAT64) AS budget,
    SAFE_CAST(impressions AS INT64) AS impressions,
    SAFE_CAST(clics AS INT64) AS clics,
    SAFE_CAST(conversions AS INT64) AS conversions,
    SAFE_CAST(CTR AS FLOAT64) AS CTR,

    ROW_NUMBER() OVER (
      PARTITION BY TRIM(id_campagne)
      ORDER BY (
        IF(date IS NOT NULL, 1, 0) +
        IF(evenement_oui_non IS NOT NULL, 1, 0) +
        IF(TRIM(evenement_type) <> '', 1, 0) +
        IF(TRIM(canal) <> '', 1, 0) +
        IF(budget IS NOT NULL, 1, 0) +
        IF(impressions IS NOT NULL, 1, 0) +
        IF(clics IS NOT NULL, 1, 0) +
        IF(conversions IS NOT NULL, 1, 0) +
        IF(CTR IS NOT NULL, 1, 0)
      ) DESC
    ) AS rn
  FROM {{ source('carttrend_rawdata', 'carttrend_campaigns') }}
  WHERE
    id_campagne IS NOT NULL
    AND TRIM(id_campagne) <> ''
    AND date IS NOT NULL
)

SELECT
  id_campagne,
  date,
  evenement_oui_non,
  evenement_type,
  canal,
  budget,
  impressions,
  clics,
  conversions,
  CTR
FROM campagnes_preparees
WHERE rn = 1