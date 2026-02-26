{{
  config(
    materialized = "table",
    partition_by = {
      "field": "ingested_date",
      "data_type": "date",
      "granularity": "day"
    }
  )
}}

with candidate_states as (
    select
        icao24,
        callsign_clean,
        origin_country,
        ingested_at,
        ingested_date,
        polled_at_ts,
        latitude,
        longitude,
        baro_altitude,
        velocity_knots,
        vertical_rate,
        true_track,
        distance_from_jkia_km,
        on_ground,
        altitude_band
    from {{ ref("stg_aircraft_states") }}
    where
        not on_ground
        and baro_altitude between {{ var("holding_altitude_min_m") }} and {{ var("holding_altitude_max_m") }}
        and (velocity_knots is null or velocity_knots < {{ var("holding_velocity_max_knots") }})
        and distance_from_jkia_km <= {{ var("proximity_radius_km") }}
),

holding_scored as (
    select
        *,
        abs(coalesce(vertical_rate, 0)) as abs_vertical_rate,
        case
            when abs(coalesce(vertical_rate, 0)) < 2.0
            and (velocity_knots is null or velocity_knots < {{ var("holding_velocity_max_knots") }})
            and altitude_band in ("low_altitude", "approach_departure")
            then true
            else false
        end as is_holding_candidate
    from candidate_states
),

holding_events as (
    select
        icao24,
        callsign_clean,
        origin_country,
        ingested_date,
        min(ingested_at) as holding_start,
        max(ingested_at) as holding_end,
        timestamp_diff(max(ingested_at), min(ingested_at), minute) as holding_duration_minutes,
        count(*) as observation_count,
        round(avg(baro_altitude), 1) as avg_altitude_m,
        round(avg(velocity_knots), 1) as avg_velocity_knots,
        round(avg(distance_from_jkia_km), 2) as avg_distance_from_jkia_km,
        round(avg(abs_vertical_rate), 3) as avg_abs_vertical_rate
    from holding_scored
    where is_holding_candidate
    group by 1, 2, 3, 4
    having count(*) >= 3
),

classified as (
    select
        *,
        case
            when holding_duration_minutes < 5 then "brief_hold"
            when holding_duration_minutes < 20 then "standard_hold"
            when holding_duration_minutes < 60 then "extended_hold"
            else "significant_delay_proxy"
        end as hold_classification
    from holding_events
)

select * from classified
