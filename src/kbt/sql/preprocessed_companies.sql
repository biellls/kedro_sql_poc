
SELECT
    id,
    REPLACE(company_rating, '%', '')::NUMERIC AS company_rating,
    company_location,
    total_fleet_count,
    iata_approved = 't' as iata_approved
FROM {{ source('companies') }}