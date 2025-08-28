{{ config(
    materialized='table'
) }}

WITH cleaned AS (
  SELECT
    -- Nettoyage des champs string
    TRIM(id) AS id,
    TRIM(id_machine) AS id_machine,
    TRIM(id_entrepot) AS id_entrepot,
    TRIM(type_machine) AS type_machine,
    TRIM(etat_machine) AS etat_machine,

    -- Conversion numérique
    SAFE_CAST(temps_d_arret AS INT64) AS temps_d_arret,
    SAFE_CAST(volume_traite AS INT64) AS volume_traite,

    -- Extraction temporelle
    SAFE_CAST(SUBSTR(TRIM(mois), 6, 2) AS INT64) AS mois,
    SAFE_CAST(SUBSTR(TRIM(mois), 1, 4) AS INT64) AS annee,

    -- Détection de doublons avec score de complétude + ordre chrono
    ROW_NUMBER() OVER (
      PARTITION BY TRIM(id)
      ORDER BY (
        IF(TRIM(id_machine) IS NOT NULL AND TRIM(id_machine) <> '', 1, 0) +
        IF(TRIM(id_entrepot) IS NOT NULL AND TRIM(id_entrepot) <> '', 1, 0) +
        IF(TRIM(type_machine) IS NOT NULL AND TRIM(type_machine) <> '', 1, 0) +
        IF(TRIM(etat_machine) IS NOT NULL AND TRIM(etat_machine) <> '', 1, 0) +
        IF(temps_d_arret IS NOT NULL, 1, 0) +
        IF(volume_traite IS NOT NULL, 1, 0) +
        IF(mois IS NOT NULL AND REGEXP_CONTAINS(TRIM(mois), r'^\d{4}-\d{2}$'), 1, 0)
      ) DESC,
      SAFE_CAST(SUBSTR(TRIM(mois), 1, 4) AS INT64) DESC,
      SAFE_CAST(SUBSTR(TRIM(mois), 6, 2) AS INT64) DESC
    ) AS rn

  FROM {{ source('carttrend_rawdata', 'carttrend_entrepots_machine') }}

  WHERE
    id IS NOT NULL AND TRIM(id) <> ''
    AND id_machine IS NOT NULL AND TRIM(id_machine) <> ''
    AND id_entrepot IS NOT NULL AND TRIM(id_entrepot) <> ''
    AND mois IS NOT NULL AND REGEXP_CONTAINS(TRIM(mois), r'^\d{4}-\d{2}$')
)

SELECT
  id,
  id_machine,
  id_entrepot,
  type_machine,
  etat_machine,
  temps_d_arret,
  volume_traite,
  mois,
  annee
FROM cleaned
WHERE rn = 1
