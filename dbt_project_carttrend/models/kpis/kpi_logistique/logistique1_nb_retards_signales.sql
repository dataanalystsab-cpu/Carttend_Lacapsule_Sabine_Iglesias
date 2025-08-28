-- logistique1_nb_retards_signales.sql
-- Requête : Nombre de retards signalés par les clients
-- ------------------------------------------------------------------------------
-- Cette requête permet de :
--   Identifier les entrepôts ayant le plus de retards signalés par les clients.
--   Analyser la répartition temporelle (par date).
--   Aider à cibler les sites nécessitant des actions correctives.
-- ------------------------------------------------------------------------------

SELECT 
  d.id_entrepot,                                         -- Identifiant unique de l’entrepôt
  e.localisation,                                        -- Localisation géographique de l’entrepôt
  d1.date,                                               -- Date de la commande (table calendrier)
  
  COUNT(CASE                                             -- Nombre de commandes signalées comme "en retard"
           WHEN c.commentaire IN ('Delivery took too long.') 
           THEN c.id_commande                            -- Chaque commande avec ce commentaire est comptée
        END) AS nombre_de_retards
FROM {{ ref('dim_satisfaction') }} AS c                  -- Table satisfaction : feedbacks et commentaires clients
JOIN {{ ref('facts_commandes') }} AS d                   -- Table de faits commandes : relie commande ↔ entrepôt
  ON c.id_commande = d.id_commande                       -- Jointure satisfaction ↔ commandes
JOIN {{ ref('dim_entrepots') }} AS e                     -- Table dimension entrepôts : localisation
  ON e.id_entrepot = d.id_entrepot                       -- Jointure commandes ↔ entrepôt
JOIN {{ ref('dim_date') }} as d1                         -- Table calendrier pour analyse temporelle
  ON d.id_date_commande = d1.id_date                     -- Jointure commande ↔ date
GROUP BY 
  d.id_entrepot,                                         -- Agrégation par entrepôt
  e.localisation,                                        -- Agrégation par localisation
  d1.date                                                -- Agrégation par date (permet suivi quotidien)
ORDER BY 
  nombre_de_retards DESC                                 -- Classement décroissant : entrepôts avec le plus de retards