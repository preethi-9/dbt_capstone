-- models/referee_info.sql
{{ config(
    materialized="table",
    unique_key="referee_id"
) }}

WITH referees AS (
    SELECT DISTINCT
        match_referees_id AS referee_id,
        match_referees AS referee_name
    FROM {{ source('raw', 'IPL_RAW_DATA') }} 
)

SELECT
    r.referee_id,
    r.referee_name
FROM
    referees r
ORDER BY r.referee_name    

-- For incremental loads, filter out records that are already in the table.
-- {% if is_incremental() %}
--     LEFT JOIN {{ this }} existing
--         ON r.referee_id = existing.referee_id
--     WHERE existing.referee_id IS NULL
-- {% endif %}