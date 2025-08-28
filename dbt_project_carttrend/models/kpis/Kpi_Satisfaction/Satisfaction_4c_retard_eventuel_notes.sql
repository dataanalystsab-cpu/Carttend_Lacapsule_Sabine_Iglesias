-- Satisfaction_4c_retard_eventuel_notes.sql
-- Requête : Analyse de la satisfaction client selon la situation de livraison
-- -------------------------------------------------------------------------------
-- Cette requête permet de segmenter les commandes en fonction de leur situation de livraison,
-- afin d’évaluer l’impact de la situation de livraison (retard, annulée, dans les délais)
-- sur la satisfaction client :
--   - Détecter les retards probables en fonction de la date de livraison
--   - Distinguer les commandes annulées
--   - Comparer les notes moyennes et le volume de commandes par catégorie
-- -------------------------------------------------------------------------------

SELECT
  FORMAT_DATE('%Y-%m', dc.date) AS mois_commande,    --  Mois de la commande (format AAAA-MM)

  CASE 
    WHEN LOWER(c.statut_commande) = 'en transit' 
         AND dl.date < CURRENT_DATE()                --  Commande toujours "en transit" mais date de livraison dépassée
    THEN 'Retard probable'
    WHEN LOWER(c.statut_commande) = 'annulée'        --  Commande annulée
    THEN 'Annulée'
    ELSE 'Dans les délais ou terminée'               --  Commande livrée dans les délais ou déjà terminée
  END AS situation_livraison,                        --  Catégorie de livraison

  ROUND(AVG(s.note_client), 2) AS note_moyenne,      --  Moyenne des notes clients par situation
  COUNT(*) AS nb_commandes                           --  Nombre de commandes par catégorie
FROM {{ ref('facts_commandes') }} c                  --  Table des commandes
JOIN {{ ref('dim_satisfaction') }} s
  ON c.id_commande = s.id_commande                   --  Lien entre commande et note de satisfaction
JOIN {{ ref('dim_date') }} dc
  ON c.id_date_commande = dc.id_date                 --  Lien pour récupérer la date de commande
JOIN {{ ref('dim_date') }} dl
  ON c.id_date_livraison = dl.id_date                --  Lien pour récupérer la date de livraison prévue
WHERE s.note_client IS NOT NULL                      --  Exclure les commandes sans note client
GROUP BY mois_commande, situation_livraison          --  Regroupement par mois et situation de livraison
ORDER BY mois_commande, situation_livraison          --  Tri par mois puis par type de situation