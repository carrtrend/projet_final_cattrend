/**
SELECT 
    CASE 
        WHEN cl.age BETWEEN 18 AND 34 THEN '18-34'       -- Jeunes adultes
        WHEN cl.age BETWEEN 35 AND 54 THEN '35-54'       -- Adultes
        WHEN cl.age BETWEEN 55 AND 60 THEN '55-60'       -- Pré-seniors
        ELSE '60+'                                       -- Seniors
    END AS tranche_age, 
    p.produit,                                           -- Nom du produit
    p.categorie,                                         -- Catégorie du produit
    SUM(dc.quantite) AS quantite_vendue,                 -- Quantité totale vendue
    ROUND(SUM(dc.quantite * p.prix), 2) AS chiffre_affaires  -- CA généré
FROM {{ ref('dim_details_commandes') }} dc 
JOIN {{ ref('facts_commandes') }} c 
    ON dc.id_commande = c.id_commande 
JOIN {{ ref('dim_produits') }} p 
    ON dc.id_details_produits = p.id_produit
JOIN {{ ref('dim_clients') }} cl 
    ON c.id_client = cl.id_client 
WHERE LOWER(c.statut_commande) NOT IN ('annulée', 'cancelled')  -- Exclure les commandes annulées
GROUP BY tranche_age, p.produit, p.categorie
ORDER BY tranche_age, quantite_vendue DESC
*/
/*
Résumé :
Cette requête analyse les ventes par tranche d'âge des clients,
en regroupant les résultats par produit et catégorie.  
Elle calcule pour chaque segment :
- La quantité totale vendue
- Le chiffre d'affaires généré  
Les commandes annulées sont exclues des résultats.  
Les tranches d'âge sont définies comme :  
  - 18-34 : Jeunes adultes  
  - 35-54 : Adultes  
  - 55-60 : Pré-seniors  
  - 60+   : Seniors
Résultats triés par tranche d'âge et par quantité vendue décroissante.
*/
/*
Résumé :
Cette requête analyse les ventes par tranche d'âge des clients,
en regroupant les résultats par produit et catégorie.  
Elle calcule pour chaque segment :
- La quantité totale vendue
- Le chiffre d'affaires généré  
Les commandes annulées sont exclues des résultats.  
Les tranches d'âge sont définies comme :  
  - 18-34 : Jeunes adultes  
  - 35-54 : Adultes  
  - 55-60 : Pré-seniors  
  - 60+   : Seniors
Résultats triés par tranche d'âge et par quantité vendue décroissante.
*/

SELECT 
    CASE 
        WHEN cl.age BETWEEN 18 AND 34 THEN '18-34'       -- Segment : Jeunes adultes
        WHEN cl.age BETWEEN 35 AND 54 THEN '35-54'       -- Segment : Adultes
        WHEN cl.age BETWEEN 55 AND 60 THEN '55-60'       -- Segment : Pré-seniors
        ELSE '60+'                                       -- Segment : Seniors
    END AS tranche_age, 
    p.produit,                                           -- Nom du produit
    p.categorie,                                         -- Catégorie du produit
    SUM(dc.quantite) AS quantite_vendue,                 -- Quantité totale vendue
    ROUND(SUM(dc.quantite * p.prix), 2) AS chiffre_affaires  -- CA total arrondi à 2 décimales
FROM {{ ref('dim_details_commandes') }} dc               -- Table de détails des commandes
JOIN {{ ref('facts_commandes') }} c                      -- Table de faits commandes
    ON dc.id_commande = c.id_commande                     -- Lien via ID commande
JOIN {{ ref('dim_produits') }} p                          -- Table des produits
    ON dc.id_details_produits = p.id_produit              -- Lien via ID produit
JOIN {{ ref('dim_clients') }} cl                          -- Table des clients
    ON c.id_client = cl.id_client                         -- Lien via ID client
WHERE LOWER(c.statut_commande) NOT IN ('annulée', 'cancelled')  -- Filtrer les commandes annulées
GROUP BY tranche_age, p.produit, p.categorie              -- Agrégation par segment, produit et catégorie
ORDER BY tranche_age, quantite_vendue DESC                -- Tri par tranche d'âge puis quantité vendue décroissante
