{{
  config(
    materialized = "table",
    partition_by = {
      "field": "traffic_hour",
      "data_type": "timestamp",
      "granularity": "hour"
    },
    cluster_by = ["ingested_date"],
    description = "Hourly traffic aggregates around JKIA: aircraft counts, speed/altitude averages, country mix"
  )
}}

with hourly_base as (
    select
        timestamp_trunc(ingested_at, hour)                as traffic_hour,
        ingested_date,
        hour_utc,
        day_of_week,
        icao24,
        callsign_clean,
        origin_country,
        altitude_band,
        baro_altitude,
        velocity_knots,
        true_track,
        vertical_rate,
        distance_from_jkia_km,
        on_ground
    from {{ ref("stg_aircraft_states") }}
    where distance_from_jkia_km <= {{ var("proximity_radius_km") }}
),

hourly_agg as (
    select
        traffic_hour,
        ingested_date,
        hour_utc,
        day_of_week,
        count(distinct icao24)                            as unique_aircraft_count,
        count(*)                                          as total_observations,
        countif(on_ground)                                as ground_aircraft_count,
        countif(not on_ground)                            as airborne_aircraft_count,
        round(avg(baro_altitude), 1)                      as avg_baro_altitude_m,
        round(avg(velocity_knots), 1)                     as avg_velocity_knots,
        round(avg(distance_from_jkia_km), 2)              as avg_distance_from_jkia_km,
        round(min(distance_from_jkia_km), 2)              as min_distance_from_jkia_km,
        count(distinct origin_country)                    as unique_countries_count,
        round(
            countif(altitude_band = "approach_departure") * 100.0 / nullif(count(*), 0),
            1
        )                                                 as pct_approach_departure,
        round(
            countif(altitude_band = "cruise") * 100.0 / nullif(count(*), 0),
            1
        )                                                 as pct_cruise,
        round(
            countif(altitude_band = "ground") * 100.0 / nullif(count(*), 0),
            1
        )                                                 as pct_ground
    from hourly_base
    group by 1, 2, 3, 4
),

peak_flags as (
    select
        *,
        case
            when hour_utc between 4 and 10 then "morning_peak"
            when hour_utc between 14 and 22 then "evening_peak"
            else "off_peak"
        end                                               as traffic_period,
        avg(unique_aircraft_count) over (
            partition by day_of_week
            order by traffic_hour
            rows between 2 preceding and 2 following
        )                                                 as rolling_5h_avg_aircraft
    from hourly_agg
)

select * from peak_flags