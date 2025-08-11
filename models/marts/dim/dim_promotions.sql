-- 📁 models/marts/dim/dim_promotions.sql
SELECT
  p.id_promotion,
  p.type_promotion,
  p.valeur_promotion,
  p.responsable_promotion,
  d1.date AS date_debut,
  d2.date AS date_fin,
  p.id_produit
FROM {{ ref('stg_promotions') }} AS p
JOIN {{ ref('dim_date') }} AS d1
  ON p.date_debut = d1.date
JOIN {{ ref('dim_date') }} AS d2
  ON p.date_fin = d2.date