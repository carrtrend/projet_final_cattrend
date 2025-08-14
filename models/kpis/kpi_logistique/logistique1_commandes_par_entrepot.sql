-- Nombre de commandes et taux de remplissage par entrepôt
SELECT 
  c.id_entrepot,
  d.localisation,
  d1.date,
  COUNT(c.id_entrepot) AS nombre_de_commandes,
  d.taux_remplissage
FROM {{ ref('facts_commandes') }} AS c
JOIN {{ ref('dim_entrepots') }} AS d
  ON d.id_entrepot = c.id_entrepot
  JOIN {{ ref('dim_date') }} as d1
  ON c.id_date_commande=d1.id_date
GROUP BY c.id_entrepot, d.taux_remplissage, d.localisation, d1.date
ORDER BY nombre_de_commandes DESC
