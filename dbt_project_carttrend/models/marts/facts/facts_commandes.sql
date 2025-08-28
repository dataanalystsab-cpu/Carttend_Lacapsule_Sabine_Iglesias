-- 📁 models/marts/facts/fact_commandes.sql
SELECT DISTINCT
  ca.id_commande,
  ca.statut_commande,
  ca.id_promotion_appliquee AS id_promotion,
  ca.mode_de_paiement,
  ca.numero_tracking AS id_tracking,
  ca.id_client,
  ca.id_entrepot_depart AS id_entrepot,
  d1.id_date AS id_date_commande,
  d2.id_date AS id_date_livraison
FROM {{ ref('stg_commandes') }} AS ca
JOIN {{ ref('dim_date') }} AS d1
  ON ca.date_commande = d1.date
JOIN {{ ref('dim_date') }} AS d2
  ON ca.date_livraison_estimee = d2.date