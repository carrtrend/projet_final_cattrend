
SELECT
  note_client,                       -- Niveau de satisfaction donné par le client

  COUNT(*) AS nb_commentaires,      -- Nombre total de commentaires pour cette note

  SUM(CASE WHEN LOWER(commentaire) LIKE '%damaged%' THEN 1 ELSE 0 END) AS nb_damaged,
  SUM(CASE WHEN LOWER(commentaire) LIKE '%very happy%' THEN 1 ELSE 0 END) AS nb_very_happy,
  SUM(CASE WHEN LOWER(commentaire) LIKE '%delivery%' THEN 1 ELSE 0 END) AS nb_delivery,
  SUM(CASE WHEN LOWER(commentaire) LIKE '%quality%' THEN 1 ELSE 0 END) AS nb_quality,
  SUM(CASE WHEN LOWER(commentaire) LIKE '%product%' THEN 1 ELSE 0 END) AS nb_product,
  SUM(CASE WHEN LOWER(commentaire) LIKE '%not great%' THEN 1 ELSE 0 END) AS nb_not_great,
  SUM(CASE WHEN LOWER(commentaire) LIKE '%experience%' THEN 1 ELSE 0 END) AS nb_experience,
  SUM(CASE WHEN LOWER(commentaire) LIKE '%service%' THEN 1 ELSE 0 END) AS nb_service,
 

FROM {{ ref('dim_satisfaction') }}
WHERE commentaire IS NOT NULL             -- Ne considérer que les commentaires non vides
GROUP BY note_client                      -- Regrouper par note de satisfaction
ORDER BY note_client ASC                  -- Trier du score le plus bas au plus élevé