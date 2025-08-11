-- ============================================================================================
-- Analyse de l’impact des pics de mentions sur les réseaux sociaux sur les performances ventes
-- Agrégation par année et mois
-- ============================================================================================

WITH mentions_stats AS (
  SELECT
    d1.date AS date_jour,                  
    SUM(f.volume_mentions) AS total_mentions 
  FROM {{ ref('facts_posts') }} f
  JOIN {{ ref('dim_date') }} d1 
    ON d1.date = f.date
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
    m.date_jour AS date,
    m.total_mentions,
    CASE
      WHEN m.total_mentions > s.moyenne_mentions + 2 * s.ecart_type_mentions 
        THEN 1
      ELSE 0
    END AS pic_mention                                
  FROM mentions_stats m
  CROSS JOIN stats_calc s
),

ventes_par_jour AS (
  SELECT
    c.date_commande AS date_jour,                               
    SUM(dc.quantite) AS volume_ventes                  
  FROM {{ ref('dim_details_commandes') }} dc
  JOIN {{ ref('facts_commandes') }} c 
    ON dc.id_commande = c.id_commande
  GROUP BY c.date_commande
),

data_combinee AS (
  SELECT
    m.date,
    m.total_mentions,
    m.pic_mention,
    COALESCE(v.volume_ventes, 0) AS volume_ventes      
  FROM mentions_avec_pic m
  LEFT JOIN ventes_par_jour v 
    ON m.date = v.date_jour
)

SELECT
  EXTRACT(YEAR FROM date) AS annee,       
  EXTRACT(MONTH FROM date) AS mois,       
  pic_mention,                            
  COUNT(*) AS nb_jours_dans_mois,         
  ROUND(AVG(total_mentions), 2) AS mentions_moyennes,
  ROUND(AVG(volume_ventes), 2) AS ventes_moyennes
FROM data_combinee
GROUP BY annee, mois, pic_mention
ORDER BY annee, mois, pic_mention DESC
