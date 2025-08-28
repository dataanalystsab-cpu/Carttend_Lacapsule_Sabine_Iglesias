SELECT
  t.id_client,
  favoris
FROM {{ ref('stg_clients') }} AS t
CROSS JOIN UNNEST(SPLIT(t.favoris, ',')) AS favoris