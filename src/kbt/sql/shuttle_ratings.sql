SELECT
    engine_type as engine_type,
    review_scores_rating as rating,
    number_of_reviews as num_reviews,
    c.id as company_id,
    c.company_rating as company_rating
FROM {{ ref('preprocessed_reviews') }} AS r LEFT JOIN {{ ref('preprocessed_shuttles') }} AS s
    ON r.shuttle_id = s.id
    INNER JOIN {{ ref('preprocessed_companies') }} AS c
    ON s.company_id = c.id
