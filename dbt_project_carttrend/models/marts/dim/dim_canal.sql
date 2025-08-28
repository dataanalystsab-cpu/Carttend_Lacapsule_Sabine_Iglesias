WITH canaux AS (
    SELECT canal AS nom_canal
    FROM {{ ref('stg_campaigns') }}

UNION DISTINCT

SELECT canal_social AS nom_canal
FROM {{ ref('stg_posts') }}
)

SELECT
    ROW_NUMBER() OVER() AS id_canal,
    nom_canal
FROM canaux
ORDER BY nom_canal