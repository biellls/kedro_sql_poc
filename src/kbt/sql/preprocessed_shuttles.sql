-- *{"id":63561,"shuttle_location":"Niue","shuttle_type":"Type V5","engine_type":"Quantum","engine_vendor":"ThetaBase Services","engines":1,"passenger_capacity":2,"cancellation_policy":"strict","crew":1,"d_check_complete":"f","moon_clearance_complete":"f","price":"$1,325.0","company_id":35029}

SELECT
    id,
    shuttle_location,
    shuttle_type,
    engine_type,
    engine_vendor,
    engines,
    passenger_capacity,
    cancellation_policy,
    crew,
    d_check_complete = 't' AS d_check_complete,
    moon_clearance_complete = 't' AS d_check_complete,
    REPLACE(REPLACE(price, ',', ''), '$', '')::NUMERIC AS price,
    company_id
FROM  {{ source('shuttles') }}
