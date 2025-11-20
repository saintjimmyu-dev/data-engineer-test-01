-- Market Opportunities – Barrios con buen potencial

/*
3. Market Opportunities

Objetivo: detectar zonas con:

precios bajos

alta demanda (reviews altos)

disponibilidad alta o baja
*/

WITH global_stats AS (
    SELECT 
        AVG(price) AS global_avg_price,
        AVG(number_of_reviews) AS global_avg_reviews
    FROM dim_listing
    WHERE is_current = 1
)

SELECT 
    dl.neighbourhood_group,
    dl.neighbourhood,
    COUNT(*) AS total_listings,
    AVG(dl.price) AS avg_price,
    AVG(dl.number_of_reviews) AS avg_reviews,
    AVG(dl.availability_365) AS avg_availability
FROM dim_listing dl
CROSS JOIN global_stats g
WHERE dl.is_current = 1
GROUP BY dl.neighbourhood_group, dl.neighbourhood
HAVING 
    avg_price < g.global_avg_price      -- más baratos que el promedio
    AND avg_reviews > g.global_avg_reviews  -- con buena demanda
ORDER BY 
    avg_reviews DESC;
