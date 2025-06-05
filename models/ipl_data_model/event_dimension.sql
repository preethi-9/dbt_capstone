{{config(
    materialized='incremental',
    unique_key='event_id'
)}}

with new_events as (
    SELECT DISTINCT
        MD5(concat(
            COALESCE(event_name, 'NULL'), '|', 
            COALESCE(season, 'NULL'), '|', 
            COALESCE(match_type, 'NULL'), '|', 
            COALESCE(gender, 'NULL'), '|',
            COALESCE(overs, 0), '|', 
            COALESCE(team_type, 'NULL'), '|',
            COALESCE(balls_per_over, 0))) AS event_id,
        event_name, season, match_type, gender, overs, team_type, balls_per_over
    FROM {{ source('raw', 'IPL_RAW_DATA') }} 
)

select 
    ne.event_id,
    ne.event_name, 
    ne.season, 
    ne.match_type, 
    ne.gender, 
    ne.overs, 
    ne.team_type,
    ne.balls_per_over
FROM new_events ne
order by ne.season