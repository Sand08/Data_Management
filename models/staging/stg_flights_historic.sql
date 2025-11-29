-- models/staging/stg_flights_historic.sql

with raw as (
  -- point to YOUR project + dataset + table
  select * from `flightproject-479709.opensky_data.flights_historic_2024`
)

select
  -- map your CSV columns to the expected names/types
  cast(YEAR as int64)          as year,
  cast(MONTH as int64)         as month,
  cast(DAY_OF_MONTH as int64)  as day_of_month,
  cast(DAY_OF_WEEK as int64)   as day_of_week,

  -- rename FL_DATE to FlightDate
  FL_DATE                      as FlightDate,

  -- airline / flight info
  MKT_UNIQUE_CARRIER           as Marketing_Airline_Network,
  cast(MKT_CARRIER_FL_NUM as int64)  as marketing_flight_number,
  TAIL_NUM                     as Tail_Number,
  cast(OP_CARRIER_FL_NUM as int64)   as operating_flight_number,

  -- origin
  ORIGIN_AIRPORT_ID            as OriginAirportID,
  ORIGIN                       as Origin,
  ORIGIN_CITY_NAME             as OriginCityName,
  ORIGIN_STATE_ABR             as OriginState,
  ORIGIN_STATE_NM              as OriginStateName,

  -- destination
  DEST_AIRPORT_ID              as DestAirportID,
  DEST                         as Dest,
  DEST_CITY_NAME               as DestCityName,
  DEST_STATE_ABR               as DestState,
  DEST_STATE_NM                as DestStateName,

  -- timings / delays
  cast(DEP_TIME as int64)      as dep_time,
  cast(DEP_DELAY as int64)     as dep_delay,
  cast(TAXI_OUT as int64)      as taxi_out,
  cast(TAXI_IN as int64)       as taxi_in,
  cast(ARR_TIME as int64)      as arr_time,
  cast(ARR_DELAY as int64)     as arr_delay,
  cast(CRS_ELAPSED_TIME as int64)    as crs_elapsed_time,
  cast(ACTUAL_ELAPSED_TIME as int64) as actual_elapsed_time,
  cast(AIR_TIME as int64)      as air_time,
  cast(DISTANCE as int64)      as distance
from raw
where FL_DATE is not null