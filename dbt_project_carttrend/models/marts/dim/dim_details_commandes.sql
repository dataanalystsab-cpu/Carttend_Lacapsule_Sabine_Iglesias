-- 📁 models/marts/dim/dim_details_commandes.sql
SELECT
  ROW_NUMBER() OVER() AS id,
  quantite AS quantite,
  emballage_special AS emballage_special,
  id_commande,
  id_produit as id_details_produits
FROM {{ ref( 'stg_details_commandes') }}