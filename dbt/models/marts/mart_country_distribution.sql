{{
  config(
    materialized = "table",
    partition_by = {
      "field": "ingested_date",
      "data_type": "date",
      "granularity": "day"
    },
    description = "Daily origin country distribution of aircraft observed around JKIA"
  )
}}

with daily_country as (
    select
        ingested_date,
        origin_country,
        count(distinct icao24)                            as unique_aircraft,
        count(*)                                          as total_observations,
        round(avg(baro_altitude), 1)                      as avg_altitude_m,
        round(avg(velocity_knots), 1)                     as avg_velocity_knots,
        round(avg(distance_from_jkia_km), 2)              as avg_distance_from_jkia_km,
        countif(on_ground)                                as ground_observations,
        countif(not on_ground)                            as airborne_observations
    from {{ ref("stg_aircraft_states") }}
    where
        origin_country is not null
        and distance_from_jkia_km <= {{ var("proximity_radius_km") }}
    group by 1, 2
),

with_rank as (
    select
        *,
        rank() over (
            partition by ingested_date
            order by unique_aircraft desc
        )                                                 as country_rank_by_aircraft,
        round(
            unique_aircraft * 100.0 / nullif(
                sum(unique_aircraft) over (partition by ingested_date), 0
            ),
            2
        )                                                 as pct_of_daily_traffic
    from daily_country
)

select * from with_rank