{{ config(
    materialized='table'
) }}

WITH promotions_preparees AS (
  SELECT
    TRIM(id_promotion) AS id_promotion,
    TRIM(id_produit) AS id_produit,
    TRIM(type_promotion) AS type_promotion,
    SAFE_CAST(valeur_promotion AS STRING) AS valeur_promotion,
    SAFE_CAST(date_debut AS DATE) AS date_debut,
    SAFE_CAST(date_fin AS DATE) AS date_fin,
    TRIM(responsable_promotion) AS responsable_promotion,

    -- Score de complétude pour trier les doublons
    ROW_NUMBER() OVER (
      PARTITION BY TRIM(id_promotion), TRIM(id_produit)
      ORDER BY (
        IF(TRIM(type_promotion) <> '', 1, 0) +
        IF(valeur_promotion IS NOT NULL, 1, 0) +
        IF(date_debut IS NOT NULL, 1, 0) +
        IF(date_fin IS NOT NULL, 1, 0) +
        IF(TRIM(responsable_promotion) <> '', 1, 0)
      ) DESC
    ) AS rn

  FROM {{ source('carttrend_rawdata', 'carttrend_promotions') }}
  WHERE
    id_promotion IS NOT NULL AND TRIM(id_promotion) <> ''
    AND id_produit IS NOT NULL AND TRIM(id_produit) <> ''
)

SELECT
  id_promotion,
  id_produit,
  type_promotion,
  valeur_promotion,
  date_debut,
  date_fin,
  responsable_promotion
FROM promotions_preparees
WHERE rn = 1
