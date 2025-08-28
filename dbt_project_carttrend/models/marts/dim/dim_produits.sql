-- 📁 models/marts/dim/dim_produit.sql
SELECT
  --ROW_NUMBER() OVER (ORDER BY produit) AS id_produit,
  id AS id_produit,
  produit,
  categorie,
  prix,
  variation
FROM {{ ref('stg_produits') }}
GROUP BY ID, produit, categorie, prix,variation