SELECT
    engine_type,
    avg(rating) as rating,
    sum(num_reviews) as num_reviews,
    avg(company_rating) as company_rating
FROM {{ ref('shuttle_ratings') }}
WHERE rating > {{ param('review_cutoff_point') }}
GROUP BY engine_type
ORDER BY rating DESC, num_reviews DESC
