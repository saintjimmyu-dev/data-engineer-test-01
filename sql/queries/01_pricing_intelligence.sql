-- Pricing Intelligence – Comparación de precios por barrio

/* 1. Pricing Intelligence

Objetivo: entender precios por barrio, detectar sobreprecios o subprecios.

Los datos salen de dim_listing y dim_neighborhood.
*/

SELECT 
    dl.neighbourhood_group,
    dl.neighbourhood,
    COUNT(*) AS total_listings,
    AVG(dl.price) AS avg_price,
    MIN(dl.price) AS min_price,
    MAX(dl.price) AS max_price,
    AVG(dl.number_of_reviews) AS avg_reviews,
    AVG(dl.availability_365) AS avg_availability
FROM dim_listing dl
WHERE dl.is_current = 1
GROUP BY dl.neighbourhood_group, dl.neighbourhood
ORDER BY avg_price DESC;

