-- 📁 models/marts/dim/dim_promotions.sql
SELECT
  p.id_promotion,
  p.type_promotion,
  p.valeur_promotion,
  p.responsable_promotion,
  d1.id_date AS id_date_debut,
  d2.id_date AS id_date_fin,
  p.id_produit
FROM {{ ref('stg_promotions') }} AS p
LEFT JOIN {{ ref('dim_date') }} AS d1
  ON p.date_debut = d1.date
LEFT JOIN {{ ref('dim_date') }} AS d2
  ON p.date_fin = d2.date