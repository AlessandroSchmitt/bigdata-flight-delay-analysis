# Analysis 3.1 — Airline statistics

## Goal

For every `(airline, departure airport)` pair, compute:

- number of flights;
- minimum arrival delay;
- maximum arrival delay;
- average arrival delay;
- cancellation rate;
- months in which the airline operates at the airport.

The same canonical input and output semantics must be used by Spark Core,
Spark SQL and Hive.

## Canonical input

`data/processed/flights_cleaned.csv`

Relevant columns:

- `month`
- `op_unique_carrier`
- `origin`
- `arr_delay`
- `cancelled`

The preprocessing phase has already removed diverted flights and invalid basic
keys. Cancelled flights remain in the dataset.

## Semantics

Grouping key:

`(op_unique_carrier, origin)`

For each key:

- `flight_count`: all canonical records in the group, including cancelled
  flights;
- `min_arr_delay`: minimum non-null `arr_delay`;
- `max_arr_delay`: maximum non-null `arr_delay`;
- `avg_arr_delay`: average over non-null `arr_delay` values only, rounded to 2
  decimal places;
- `cancellation_rate`: `cancelled_flights / flight_count`, rounded to 4 decimal
  places;
- `operating_months`: distinct months appearing in the group, sorted
  chronologically and formatted as `01|02|...|12`.

If all arrival delays in a group are null, min/max/average remain null.

## Technology-independent accumulator

For Spark Core, each record is mapped to:

Key:
`(carrier, origin)`

Value:
`(
  flight_count,
  valid_arrival_delay_count,
  arrival_delay_sum,
  arrival_delay_min,
  arrival_delay_max,
  cancelled_count,
  months_set
)`

The reducer merges two accumulators using addition, min/max over valid values,
and set union.

## Shuffle

The main shuffle is introduced by grouping/aggregating all records with the
same `(carrier, origin)` key:

- Spark Core: `reduceByKey`
- Spark SQL: `groupBy`
- Hive: `GROUP BY`

No global output ordering is part of the benchmarked job. Deterministic
sorting is performed only by the validation/preview utility after execution.

## Output schema

The three implementations produce the same eight logical fields:

1. `airline`
2. `departure_airport`
3. `flight_count`
4. `min_arr_delay`
5. `max_arr_delay`
6. `avg_arr_delay`
7. `cancellation_rate`
8. `operating_months`

Benchmark outputs do not contain a header row. Output order is unspecified.
