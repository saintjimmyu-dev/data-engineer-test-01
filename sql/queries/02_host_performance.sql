-- Host Performance – Actividad y portafolio de anfitriones

/*
2. Host Performance

Objetivo: analizar anfitriones, cuántos listings tienen y su desempeño.

Usa:

dim_host
dim_listing
fact_listings
*/

SELECT
    h.host_id,
    h.host_name,
    h.calculated_host_listings_count AS reported_listings,
    COUNT(DISTINCT f.listing_sk) AS listings_in_dw,
    AVG(dl.price) AS avg_price,
    SUM(dl.number_of_reviews) AS total_reviews
FROM dim_host h
LEFT JOIN fact_listings f
    ON f.host_sk = h.host_sk
LEFT JOIN dim_listing dl
    ON dl.listing_sk = f.listing_sk
WHERE h.is_current = 1
  AND dl.is_current = 1
GROUP BY
    h.host_id,
    h.host_name,
    h.calculated_host_listings_count
ORDER BY
    listings_in_dw DESC,
    total_reviews DESC;

