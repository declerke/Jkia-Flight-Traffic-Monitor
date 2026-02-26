{{
  config(
    materialized = "view",
    description = "Staged and lightly cleaned aircraft state vectors from the raw BigQuery table"
  )
}}

with raw_states as (
    select
        icao24,
        trim(callsign)                                    as callsign,
        trim(origin_country)                              as origin_country,
        time_position,
        last_contact,
        longitude,
        latitude,
        baro_altitude,
        on_ground,
        velocity,
        true_track,
        vertical_rate,
        geo_altitude,
        squawk,
        spi,
        position_source,
        poll_timestamp,
        ingested_at,
        date(ingested_at)                                 as ingested_date,
        timestamp_seconds(poll_timestamp)                 as polled_at_ts
    from {{ source("jkia_raw", "aircraft_states") }}
    where
        ingested_at is not null
        and icao24 is not null
        and longitude between -180 and 180
        and latitude between -90 and 90
),

deduplicated as (
    select *
    from raw_states
    qualify
        row_number() over (
            partition by icao24, poll_timestamp
            order by ingested_at desc
        ) = 1
),

enriched as (
    select
        *,
        nullif(callsign, "")                              as callsign_clean,
        case
            when on_ground then "ground"
            when baro_altitude is null then "unknown"
            when baro_altitude < 300 then "approach_departure"
            when baro_altitude < 3000 then "low_altitude"
            when baro_altitude < 7500 then "mid_altitude"
            else "cruise"
        end                                               as altitude_band,
        round(
            st_distance(
                st_geogpoint(longitude, latitude),
                st_geogpoint({{ var("jkia_lon") }}, {{ var("jkia_lat") }})
            ) / 1000.0,
            2
        )                                                 as distance_from_jkia_km,
        case
            when velocity is not null
            then round(velocity * 1.94384, 1)
            else null
        end                                               as velocity_knots,
        extract(hour from ingested_at)                   as hour_utc,
        extract(dayofweek from ingested_at)               as day_of_week
    from deduplicated
)

select * from enriched
