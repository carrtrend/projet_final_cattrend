WITH mentions_stats AS (
  SELECT
    d1.date AS date_jour,
    SUM(f.volume_mentions) AS total_mentions
  FROM {{ ref('facts_posts') }} f
  JOIN {{ ref('dim_date') }} d1 ON d1.id_date = f.id_date
  GROUP BY d1.date
),

stats_calc AS (
  SELECT
    AVG(total_mentions) AS moyenne_mentions,
    STDDEV(total_mentions) AS ecart_type_mentions
  FROM mentions_stats
),

mentions_avec_pic AS (
  SELECT
    m.date_jour,
    m.total_mentions,
    CASE
      WHEN m.total_mentions > s.moyenne_mentions + 2 * s.ecart_type_mentions THEN 1
      ELSE 0
    END AS pic_mention
  FROM mentions_stats m
  CROSS JOIN stats_calc s
),

ventes_par_jour AS (
  SELECT
    d.date AS date_jour,
    SUM(dc.quantite) AS volume_ventes
  FROM {{ ref('dim_details_commandes') }} dc
  JOIN {{ ref('facts_commandes') }} c ON dc.id_commande = c.id_commande
  JOIN {{ ref('dim_date') }} d ON c.id_date_commande = d.id_date
  WHERE LOWER(c.statut_commande) NOT IN ('annulée', 'cancelled')
  GROUP BY d.date
),

satisfaction_par_jour AS (
  SELECT
    d.date AS date_jour,
    ROUND(AVG(s.note_client), 2) AS note_moyenne
  FROM {{ ref('dim_satisfaction') }} s
  JOIN {{ ref('facts_commandes') }} c ON s.id_commande = c.id_commande
  JOIN {{ ref('dim_date') }} d ON c.id_date_commande = d.id_date
  WHERE LOWER(c.statut_commande) NOT IN ('annulée', 'cancelled')
  GROUP BY d.date
)

SELECT
  m.date_jour,
  EXTRACT(YEAR FROM m.date_jour) AS annee,
  EXTRACT(MONTH FROM m.date_jour) AS mois,
  m.total_mentions,
  m.pic_mention,
  COALESCE(v.volume_ventes, 0) AS volume_ventes,
  COALESCE(s.note_moyenne, NULL) AS satisfaction_moyenne
FROM mentions_avec_pic m
LEFT JOIN ventes_par_jour v ON m.date_jour = v.date_jour
LEFT JOIN satisfaction_par_jour s ON m.date_jour = s.date_jour
ORDER BY m.date_jour ASC
