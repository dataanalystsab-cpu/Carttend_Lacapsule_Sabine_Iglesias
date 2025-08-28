-- 📁 models/marts/facts/facts_posts.sql
SELECT DISTINCT
  p.id_post,
  p.volume_mentions,
  p.sentiment_global,
  p.contenu_post,
  c.id_canal AS id_canal,
  d.id_date
FROM {{ ref('stg_posts') }} AS p
JOIN {{ ref('dim_date') }} AS d
  ON p.date_post = d.date
JOIN {{ ref('dim_canal') }} AS c
  ON p.canal_social = c.nom_canal