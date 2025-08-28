{{ config(
    materialized='table'
) }}

WITH satisfaction_prepare AS (
  SELECT
    TRIM(id_satisfaction) AS id_satisfaction,
    TRIM(id_commande) AS id_commande,
    SAFE_CAST(note_client AS INT64) AS note_client,
    TRIM(commentaire) AS commentaire,
    SAFE_CAST(plainte AS BOOLEAN) AS plainte,
    SAFE_CAST(temps_reponse_support AS INT64) AS temps_reponse_support,
    TRIM(type_plainte) AS type_plainte,
    TRIM(employe_support) AS employe_support,

    -- Score de complétude pour garder la ligne la plus renseignée
    ROW_NUMBER() OVER (
      PARTITION BY TRIM(id_satisfaction)
      ORDER BY (
        IF(TRIM(id_commande) <> '', 1, 0) +
        IF(note_client IS NOT NULL, 1, 0) +
        IF(TRIM(commentaire) <> '', 1, 0) +
        IF(plainte IS NOT NULL, 1, 0) +
        IF(temps_reponse_support IS NOT NULL, 1, 0) +
        IF(TRIM(type_plainte) <> '', 1, 0) +
        IF(TRIM(employe_support) <> '', 1, 0)
      ) DESC
    ) AS rn
  FROM {{ source('carttrend_rawdata', 'carttrend_satisfaction') }}
  WHERE
    id_satisfaction IS NOT NULL AND TRIM(id_satisfaction) <> ''
    AND id_commande IS NOT NULL AND TRIM(id_commande) <> ''
)

-- Garder une seule ligne par id_satisfaction
SELECT
  id_satisfaction,
  id_commande,
  note_client,
  commentaire,
  plainte,
  temps_reponse_support,
  type_plainte,
  employe_support
FROM satisfaction_prepare
WHERE rn = 1