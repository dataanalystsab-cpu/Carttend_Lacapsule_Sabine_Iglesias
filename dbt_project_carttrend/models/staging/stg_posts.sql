{{ config(
    materialized='table'
) }}

WITH posts_prepare AS (
  SELECT
    TRIM(id_post) AS id_post,
    SAFE_CAST(date_post AS DATE) AS date_post,
    TRIM(canal_social) AS canal_social,
    SAFE_CAST(volume_mentions AS INT64) AS volume_mentions,
    TRIM(sentiment_global) AS sentiment_global,
    TRIM(contenu_post) AS contenu_post,

    -- Score de complétude pour garder la ligne la plus informative par post
    ROW_NUMBER() OVER (
      PARTITION BY TRIM(id_post)
      ORDER BY (
        IF(date_post IS NOT NULL, 1, 0) +
        IF(TRIM(canal_social) <> '', 1, 0) +
        IF(volume_mentions IS NOT NULL, 1, 0) +
        IF(TRIM(sentiment_global) <> '', 1, 0) +
        IF(TRIM(contenu_post) <> '', 1, 0)
      ) DESC
    ) AS rn
  FROM {{ source('carttrend_rawdata', 'carttrend_posts') }}
  WHERE id_post IS NOT NULL AND TRIM(id_post) <> ''
)

SELECT
  id_post,
  date_post,
  canal_social,
  volume_mentions,
  sentiment_global,
  contenu_post
FROM posts_prepare
WHERE rn = 1
