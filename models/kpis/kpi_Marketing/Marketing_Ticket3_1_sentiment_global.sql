-- -------------------------------------------------------------------------------
-- Analyse des volumes de mentions sociales par jour et par sentiment
-- -------------------------------------------------------------------------------
-- Permet de suivre l’évolution quotidienne des messages sur les réseaux sociaux
-- en les classant par tonalité (positif, neutre, négatif).
-- Utile pour détecter les pics d’activité ou les crises de réputation.
-- -------------------------------------------------------------------------------

SELECT
  d.date, 
  EXTRACT(YEAR FROM d.date) AS annee,       -- Année extraite de la date
  EXTRACT(MONTH FROM d.date) AS mois,       -- Mois extrait de la date
  c.nom_canal,                              -- Nom du canal
  COUNT(*) AS total_mentions,
  SUM(CASE WHEN LOWER(TRIM(p.sentiment_global)) = 'positif' THEN 1 ELSE 0 END) AS nb_positif,
  SUM(CASE WHEN LOWER(TRIM(p.sentiment_global)) = 'neutre' THEN 1 ELSE 0 END) AS nb_neutre,
  SUM(CASE WHEN LOWER(TRIM(p.sentiment_global)) = 'négatif' THEN 1 ELSE 0 END) AS nb_negatif
FROM {{ ref('facts_posts') }} AS p
JOIN {{ ref('dim_date') }} AS d 
  ON p.id_date = d.id_date
JOIN {{ ref('dim_canal') }} AS c
  ON p.id_canal = c.id_canal
GROUP BY d.date, c.nom_canal
ORDER BY d.date ASC, c.nom_canal