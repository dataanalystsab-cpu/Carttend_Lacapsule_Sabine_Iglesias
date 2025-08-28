{{ config(
    materialized='table'
) }}

WITH commandes_prepare AS (
  SELECT
    TRIM(id_commande) AS id_commande,
    TRIM(id_client) AS id_client,
    TRIM(id_entrepot_depart) AS id_entrepot_depart,
    SAFE_CAST(date_commande AS DATE) AS date_commande,
    TRIM(statut_commande) AS statut_commande,
    TRIM(id_promotion_appliquee) AS id_promotion_appliquee,
    TRIM(mode_de_paiement) AS mode_de_paiement,
    TRIM(numero_tracking) AS numero_tracking,
    SAFE_CAST(date_livraison_estimee AS DATE) AS date_livraison_estimee,

    -- Score de complétude
    ROW_NUMBER() OVER (
      PARTITION BY TRIM(id_commande)
      ORDER BY (
        IF(id_client IS NOT NULL AND id_client != '', 1, 0) +
        IF(id_entrepot_depart IS NOT NULL AND id_entrepot_depart != '', 1, 0) +
        IF(date_commande IS NOT NULL, 1, 0) +
        IF(statut_commande IS NOT NULL AND statut_commande != '', 1, 0) +
        IF(id_promotion_appliquee IS NOT NULL AND id_promotion_appliquee != '', 1, 0) +
        IF(mode_de_paiement IS NOT NULL AND mode_de_paiement != '', 1, 0) +
        IF(numero_tracking IS NOT NULL AND numero_tracking != '', 1, 0) +
        IF(date_livraison_estimee IS NOT NULL, 1, 0)
      ) DESC
    ) AS rn

  FROM {{ source('carttrend_rawdata', 'carttrend_commandes') }}

  WHERE
    id_commande IS NOT NULL
    AND TRIM(id_commande) <> ''
)

SELECT
  id_commande,
  id_client,
  id_entrepot_depart,
  date_commande,
  statut_commande,
  id_promotion_appliquee,
  mode_de_paiement,
  numero_tracking,
  date_livraison_estimee
FROM commandes_prepare
WHERE rn = 1
