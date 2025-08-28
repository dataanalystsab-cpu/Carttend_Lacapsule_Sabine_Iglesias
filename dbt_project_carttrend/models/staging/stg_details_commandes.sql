{{ config(
    materialized = 'table'
) }}

WITH details_prepare AS (
  SELECT
    TRIM(id_commande) AS id_commande,
    TRIM(id_produit) AS id_produit,
    SAFE_CAST(quantite AS INT64) AS quantite,

    CASE
      WHEN LOWER(TRIM(emballage_special)) = 'oui' THEN TRUE
      WHEN LOWER(TRIM(emballage_special)) = 'non' THEN FALSE
      ELSE NULL
    END AS emballage_special,

    ROW_NUMBER() OVER (
      PARTITION BY TRIM(id_commande), TRIM(id_produit)
      ORDER BY (
        IF(quantite IS NOT NULL, 1, 0) +
        IF(emballage_special IS NOT NULL, 1, 0)
      ) DESC
    ) AS rn

  FROM {{ source('carttrend_rawdata', 'carttrend_details_commandes') }}
  WHERE
    id_commande IS NOT NULL 
    AND id_produit IS NOT NULL
)

SELECT
  id_commande,
  id_produit,
  quantite,
  emballage_special
FROM details_prepare
WHERE rn = 1
