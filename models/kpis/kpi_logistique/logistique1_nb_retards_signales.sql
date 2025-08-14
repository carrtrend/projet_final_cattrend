-- Nombre de retards signalés par les clients
SELECT 
  d.id_entrepot,
  e.localisation,
  d1.date,
  COUNT(CASE 
           WHEN c.commentaire IN ('Delivery took too long.') 
           THEN c.id_commande
        END) AS nombre_de_retards
FROM {{ ref('dim_satisfaction') }} AS c
JOIN {{ ref('facts_commandes') }} AS d
  ON c.id_commande = d.id_commande
JOIN {{ ref('dim_entrepots') }} AS e
  ON e.id_entrepot = d.id_entrepot
JOIN {{ ref('dim_date') }} as d1
  ON d.id_date_commande=d1.id_date
GROUP BY d.id_entrepot, e.localisation, d1.date         -- Agrégation par entrepôt et sa localisation
ORDER BY nombre_de_retards DESC             -- Classement décroissant : entrepôts avec le plus de retards en haut 