-- ========================================================================
-- Analyse CA promo vs hors promo (version "jour complet")
-- ========================================================================

WITH dim_details_commandes AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY id_commande, id_details_produits) AS id_produit_tech,
        quantite,
        emballage_special,
        id_commande,
        id_details_produits
    FROM {{ ref('dim_details_commandes') }}
),

-- Étape 1 : identifier les jours qui sont en promo
jours_promo AS (
    SELECT DISTINCT dt1.date AS date_commande
    FROM dim_details_commandes ddc
    JOIN {{ ref('facts_commandes') }} c
        ON ddc.id_commande = c.id_commande
    JOIN {{ ref('dim_date') }} dt1
        ON c.id_date_commande = dt1.id_date
    JOIN {{ ref('dim_produits') }} p
        ON ddc.id_details_produits = p.id_produit
    LEFT JOIN {{ ref('dim_promotions') }} pr
        ON p.id_produit = pr.id_produit
    LEFT JOIN {{ ref('dim_date') }} dt2
        ON pr.id_date_debut = dt2.id_date
    LEFT JOIN {{ ref('dim_date') }} dt3
        ON pr.id_date_fin = dt3.id_date
    WHERE LOWER(c.statut_commande) NOT IN ('annulée', 'cancelled')
      AND pr.id_produit IS NOT NULL
      AND dt1.date BETWEEN dt2.date AND dt3.date
),

-- Étape 2 : associer chaque vente au statut "jour promo" ou "jour hors promo"
ventes_taggees AS (
    SELECT
        ddc.id_details_produits,
        p.id_produit AS id_produit_reel,
        ddc.quantite,
        p.prix,
        dt1.date AS date_commande,
        CASE
            WHEN jp.date_commande IS NOT NULL THEN 'jour_promo'
            ELSE 'jour_hors_promo'
        END AS statut_jour
    FROM dim_details_commandes ddc
    JOIN {{ ref('facts_commandes') }} c
        ON ddc.id_commande = c.id_commande
    JOIN {{ ref('dim_date') }} dt1
        ON c.id_date_commande = dt1.id_date
    JOIN {{ ref('dim_produits') }} p
        ON ddc.id_details_produits = p.id_produit
    LEFT JOIN jours_promo jp
        ON dt1.date = jp.date_commande
    WHERE LOWER(c.statut_commande) NOT IN ('annulée', 'cancelled')
),

-- Étape 3 : calcul CA total, jours distincts et CA moyen/jour
resume_ca AS (
    SELECT
        statut_jour,
        ROUND(SUM(quantite * prix), 2) AS chiffre_affaires_total,
        COUNT(DISTINCT date_commande) AS nb_jours_vente,
        ROUND(SUM(quantite * prix) / NULLIF(COUNT(DISTINCT date_commande), 0), 2) AS ca_moyen_par_jour
    FROM ventes_taggees
    GROUP BY statut_jour
),

-- Étape 4 : transformer en colonnes
pivot_ca AS (
    SELECT
        MAX(CASE WHEN statut_jour = 'jour_promo' THEN chiffre_affaires_total END) AS ca_jour_promo,
        MAX(CASE WHEN statut_jour = 'jour_hors_promo' THEN chiffre_affaires_total END) AS ca_jour_hors_promo,
        MAX(CASE WHEN statut_jour = 'jour_promo' THEN ca_moyen_par_jour END) AS ca_moyen_jour_promo,
        MAX(CASE WHEN statut_jour = 'jour_hors_promo' THEN ca_moyen_par_jour END) AS ca_moyen_jour_hors_promo,
        MAX(CASE WHEN statut_jour = 'jour_promo' THEN nb_jours_vente END) AS nb_jours_promo,
        MAX(CASE WHEN statut_jour = 'jour_hors_promo' THEN nb_jours_vente END) AS nb_jours_hors_promo
    FROM resume_ca
)

SELECT
    ca_jour_promo,
    ca_jour_hors_promo,
    nb_jours_promo,
    nb_jours_hors_promo,
    ca_moyen_jour_promo,
    ca_moyen_jour_hors_promo,
    ROUND(SAFE_DIVIDE(ca_jour_promo, ca_jour_hors_promo), 2) AS ratio_ca_total_promo_vs_hors,
    ROUND(SAFE_DIVIDE(ca_moyen_jour_promo, ca_moyen_jour_hors_promo), 2) AS ratio_ca_moyen_jour_promo_vs_hors
FROM pivot_ca
