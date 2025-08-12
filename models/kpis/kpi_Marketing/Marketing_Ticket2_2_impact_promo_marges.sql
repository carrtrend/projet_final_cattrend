WITH ca_mensuel AS (
  SELECT
    FORMAT_DATE('%Y-%m', c.date_commande) AS mois,
    ROUND(SUM(
      IF(promo.id_promotion IS NOT NULL, dc.quantite * pr.prix, 0)
    ), 2) AS ca_promo,
    ROUND(SUM(
      IF(promo.id_promotion IS NULL, dc.quantite * pr.prix, 0)
    ), 2) AS ca_hors_promo
  FROM {{ ref('facts_commandes') }} c
  JOIN {{ ref('dim_details_commandes') }} dc 
    ON c.id_commande = dc.id_commande
  JOIN {{ ref('dim_produits') }} pr
    ON dc.id_details_produits = pr.id_produit
  LEFT JOIN {{ ref('dim_promotions') }} promo
    ON pr.id_produit = promo.id_produit
   AND c.date_commande BETWEEN promo.date_debut AND promo.date_fin
  GROUP BY mois
),
budget_mensuel AS (
  SELECT
    FORMAT_DATE('%Y-%m', date) AS mois,
    canal,
    evenement_type,
    ROUND(SUM(budget), 2) AS budget_marketing
  FROM {{ ref('facts_campaigns') }}
  GROUP BY mois, canal, evenement_type
),
jointure AS (
  SELECT
    IFNULL(c.mois, b.mois) AS mois,
    IFNULL(c.ca_promo, 0) AS ca_promo,
    IFNULL(c.ca_hors_promo, 0) AS ca_hors_promo,
    (IFNULL(c.ca_promo, 0) + IFNULL(c.ca_hors_promo, 0)) AS ca_total,
    b.canal,
    b.evenement_type,
    IFNULL(b.budget_marketing, 0) AS budget_marketing
  FROM ca_mensuel c
  FULL OUTER JOIN budget_mensuel b 
    ON c.mois = b.mois
)
SELECT
  mois,
  ca_promo,
  ca_hors_promo,
  ca_total,
  canal,
  evenement_type,
  budget_marketing,
  SAFE_DIVIDE(ca_promo, ca_total) * 100 AS impact_promo_CA -- % du CA lié aux promos
FROM jointure
ORDER BY mois, canal, evenement_type