WITH produits_promo AS (
    SELECT
        pr.id_promotion,
        pr.id_produit,
        pr.type_promotion,
        CASE 
            WHEN LOWER(pr.type_promotion) LIKE '%pourcentage%' THEN 
        (SAFE_CAST(
            REGEXP_REPLACE(
                REGEXP_REPLACE(CAST(pr.valeur_promotion AS STRING), r'[%€\s]', ''), 
                r',', '.'
            ) AS FLOAT64
        ))
    ELSE
        SAFE_CAST(
            REGEXP_REPLACE(
                REGEXP_REPLACE(CAST(pr.valeur_promotion AS STRING), r'[%€\s]', ''), 
                r',', '.'
            ) AS FLOAT64
        )
        END AS valeur_promotion,
        dt1.date AS date_debut,
        dt2.date AS date_fin
    FROM {{ ref("dim_promotions") }} pr
    JOIN {{ ref("dim_date") }} dt1 ON dt1.id_date = pr.id_date_debut
    JOIN {{ ref("dim_date") }} dt2 ON dt2.id_date = pr.id_date_fin
    WHERE pr.id_produit IS NOT NULL
),

ca_detail AS (
    SELECT
        dc.id_details_produits AS id_produit,
        dt3.date AS date_commande,
        SUM(dc.quantite) AS quantite,
        ROUND(SUM(dc.quantite * p.prix), 2) AS ca_mensuel
    FROM {{ ref("facts_commandes") }} c
    JOIN {{ ref("dim_date") }} dt3 ON dt3.id_date = c.id_date_commande
    JOIN {{ ref("dim_details_commandes") }} dc ON c.id_commande = dc.id_commande
    JOIN {{ ref("dim_produits") }} p ON dc.id_details_produits = p.id_produit
    GROUP BY id_produit, date_commande
),
promo_flagged AS (
    SELECT
        p.id_promotion,
        ca.id_produit,
        p.type_promotion,      
        ca.date_commande,
        ca.ca_mensuel,
        ca.quantite,
        p.valeur_promotion,
        MAX(
            CASE
                WHEN ca.date_commande BETWEEN p.date_debut AND p.date_fin
                THEN 1 ELSE 0
            END
        ) AS en_promo
    FROM ca_detail ca
    JOIN produits_promo p ON ca.id_produit = p.id_produit
    GROUP BY p.id_promotion, ca.id_produit, p.type_promotion, ca.date_commande, ca.ca_mensuel, ca.quantite, p.valeur_promotion
),

aggrege_ca AS (
    SELECT
        id_promotion,
        id_produit,
        type_promotion,        
        valeur_promotion,
        SUM(ca_mensuel) AS ca_total,
        SUM(CASE WHEN en_promo = 1 THEN ca_mensuel ELSE 0 END) AS ca_promo,
        SUM(quantite) AS quantite_total,
        SUM(CASE WHEN en_promo = 1 THEN quantite ELSE 0 END) AS quantite_promo
    FROM promo_flagged
    GROUP BY id_promotion, id_produit, type_promotion, valeur_promotion
)

SELECT
    id_promotion,
    id_produit,
    type_promotion,           
    valeur_promotion,
    ca_total,
    ca_promo,
    quantite_total,
    quantite_promo,
    ROUND(ca_promo / NULLIF(ca_total, 0), 4) * 100 AS ratio_promo
FROM aggrege_ca
WHERE ca_total > 0
ORDER BY ca_promo DESC
