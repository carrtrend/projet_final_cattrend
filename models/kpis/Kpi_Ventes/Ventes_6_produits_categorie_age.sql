
SELECT 
    CASE 
        WHEN cl.age BETWEEN 18 AND 25 THEN '18-25'       -- Jeunes adultes
        WHEN cl.age BETWEEN 26 AND 35 THEN '26-35'       -- Jeunes adultes
        WHEN cl.age BETWEEN 36 AND 45 THEN '36-45'       -- Adultes matures
        WHEN cl.age BETWEEN 46 AND 60 THEN '46-60'       -- Pré-seniors
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