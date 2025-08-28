{{ config(
    materialized = 'table'
) }}

WITH produits_prepares AS (

    SELECT
        TRIM(ID) AS ID,
        TRIM(Categorie) AS Categorie,
        TRIM(Marque) AS Marque,

        -- Nettoyage du champ Produit (si variation présente à la fin, on la retire)
        CASE
            WHEN REGEXP_CONTAINS(
                    TRIM(Produit),
                    r'\s*-\s*' || REGEXP_REPLACE(TRIM(Variation), r'([\[\]\(\)\.\?\*\+\^\$\|\\])', r'\\\1') || r'$'
                 )
            THEN REGEXP_REPLACE(
                    TRIM(Produit),
                    r'\s*-\s*' || REGEXP_REPLACE(TRIM(Variation), r'([\[\]\(\)\.\?\*\+\^\$\|\\])', r'\\\1') || r'$',
                    ''
                 )
            ELSE TRIM(Produit)
        END AS Produit,

        SAFE_CAST(prix AS FLOAT64) AS prix,
        TRIM(`Sous-categorie`) AS sous_categorie,
        TRIM(Variation) AS Variation,

        -- Score de complétude
        ROW_NUMBER() OVER (
            PARTITION BY TRIM(ID)
            ORDER BY (
                IF(TRIM(ID) IS NOT NULL AND TRIM(ID) <> '', 1, 0) +
                IF(TRIM(Categorie) IS NOT NULL AND TRIM(Categorie) <> '', 1, 0) +
                IF(TRIM(Marque) IS NOT NULL AND TRIM(Marque) <> '', 1, 0) +
                IF(TRIM(Produit) IS NOT NULL AND TRIM(Produit) <> '', 1, 0) +
                IF(prix IS NOT NULL, 1, 0) +
                IF(TRIM(`Sous-categorie`) IS NOT NULL AND TRIM(`Sous-categorie`) <> '', 1, 0) +
                IF(TRIM(Variation) IS NOT NULL AND TRIM(Variation) <> '', 1, 0)
            ) DESC
        ) AS rn

    FROM {{ source('carttrend_rawdata', 'carttrend_produits') }}

    WHERE
        TRIM(ID) IS NOT NULL AND TRIM(ID) <> ''
        AND Produit IS NOT NULL AND TRIM(Produit) <> ''
        AND prix IS NOT NULL
)

SELECT
    id,
    categorie,
    marque,
    produit,
    prix,
    sous_categorie,
    variation
FROM produits_prepares
WHERE rn = 1
