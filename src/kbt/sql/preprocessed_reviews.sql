SELECT
    shuttle_id,
    review_scores_rating,
    review_scores_comfort,
    review_scores_amenities,
    review_scores_trip,
    review_scores_crew,
    review_scores_location,
    review_scores_price,
    number_of_reviews,
    reviews_per_month
FROM {{ source('reviews') }}
