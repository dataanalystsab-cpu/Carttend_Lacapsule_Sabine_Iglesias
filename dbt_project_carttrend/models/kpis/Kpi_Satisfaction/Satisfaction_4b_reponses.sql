-- Satisfaction_4b_reponses.sql
-- Requête : Analyse de la satisfaction client selon le délai de réponse du support
-- -------------------------------------------------------------------------------
-- Cette requête permet de segmenter les avis clients en fonction du temps de réponse du support,
-- regroupé en intervalles horaires pour mieux comprendre l’impact du délai sur la satisfaction.
-- Elle calcule la note moyenne et le nombre d’avis par intervalle, y compris les cas inconnus.
-- Cette analyse aide à identifier si un support rapide améliore la satisfaction client.
-- -------------------------------------------------------------------------------

SELECT
  CASE
    WHEN temps_de_reponse IS NULL THEN 'inconnu'      -- Cas où le délai n’est pas renseigné
    WHEN temps_de_reponse <= 1 THEN '≤ 1h'            -- Réponse ultra-rapide
    WHEN temps_de_reponse <= 4 THEN '1-4h'            -- Réponse rapide
    WHEN temps_de_reponse <= 12 THEN '4-12h'          -- Réponse modérée
    WHEN temps_de_reponse <= 24 THEN '12-24h'         -- Réponse lente
    ELSE '> 24h'                                           -- Réponse très lente (> 1 jour)
  END AS delai_support,                                    -- Catégorie de délai de réponse

  ROUND(AVG(note_client), 2) AS note_moyenne,             -- Note moyenne client par catégorie

  COUNT(*) AS nb_avis                                      -- Nombre d’avis dans chaque catégorie
FROM {{ ref('dim_satisfaction') }}
GROUP BY delai_support                                     -- Regroupement par intervalle de délai
ORDER BY note_moyenne ASC                                  -- Tri pour mettre en avant les délais avec les notes les plus basses