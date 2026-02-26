{{
  config(
    materialized = "table",
    description = "Latest position snapshot for each aircraft seen in the last 10 minutes — powers the live map view"
  )
}}

with latest_per_aircraft as (
    select *
    from {{ ref("stg_aircraft_states") }}
    where
        ingested_at >= timestamp_sub(current_timestamp(), interval 10 minute)
    qualify
        row_number() over (
            partition by icao24
            order by ingested_at desc
        ) = 1
),

enriched as (
    select
        icao24,
        callsign_clean                                    as callsign,
        origin_country,
        latitude,
        longitude,
        baro_altitude,
        geo_altitude,
        velocity_knots,
        true_track,
        vertical_rate,
        on_ground,
        altitude_band,
        distance_from_jkia_km,
        squawk,
        ingested_at                                       as last_seen_at,
        timestamp_diff(current_timestamp(), ingested_at, second)
                                                          as staleness_seconds,
        case
            when vertical_rate > 2 then "climbing"
            when vertical_rate < -2 then "descending"
            else "level"
        end                                               as vertical_trend
    from latest_per_aircraft
)

select * from enriched