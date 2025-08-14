-- -------------------------------------------------------------------------------------
-- Objectif :
-- Cette requête calcule le chiffre d’affaires journalier à partir des commandes validées.
-- Elle retourne, pour chaque date de commande :
--   - la date complète (periode)
--   - l’année, le mois et le jour extraits de cette date
--   - le chiffre d’affaires total de la journée (quantité × prix unitaire)
-- Les commandes annulées (statut "annulée" ou "cancelled") sont exclues.
-- -------------------------------------------------------------------------------------

SELECT
  dt.date AS periode,
  EXTRACT(YEAR FROM dt.date) AS annee,
  EXTRACT(MONTH FROM dt.date) AS mois,
  EXTRACT(DAY FROM dt.date) AS jour,
  ROUND(SUM(dc.quantite * p.prix), 2) AS chiffre_affaires
FROM {{ ref('facts_commandes') }} c
JOIN {{ ref('dim_date') }} dt ON dt.id_date = c.id_date_commande
JOIN {{ ref('dim_details_commandes') }} dc ON c.id_commande = dc.id_commande
JOIN {{ ref('dim_produits') }} p ON dc.id_details_produits = p.id_produit
WHERE LOWER(c.statut_commande) NOT IN ('annulée', 'cancelled')
GROUP BY dt.date