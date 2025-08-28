-- Marketing_Ticket1_1_nb_clic_canal.sql
-- Requête : Analyse des clics et acquisitions par canal marketing
-- -------------------------------------------------------------------------------
-- Cette requête permet de calculer le volume total de clics et de conversions obtenus par canal
-- afin d’identifier les leviers marketing les plus performants en termes
-- d’attraction (clics) et d’efficacité (acquisitions).
-- -------------------------------------------------------------------------------

SELECT
  dc.nom_canal AS canal,                   --  Nom du canal marketing (Email, Réseaux sociaux, etc.)
  SUM(fc.clics) AS total_clics,            --  Nombre total de clics générés par ce canal
  SUM(fc.conversions) AS total_acquisitions --  Nombre total de conversions / acquisitions
FROM {{ ref('facts_campaigns') }} AS fc    --  Table des campagnes factuelles
JOIN {{ ref('dim_canal') }} AS dc          --  Dictionnaire des canaux marketing
  ON fc.id_canal_dim_canal = dc.id_canal
GROUP BY dc.nom_canal                      --  Regroupement par canal
ORDER BY total_clics DESC                  --  Classement du plus cliqué au moins cliqué