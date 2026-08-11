# Analysis 3.2 — Delay report by airport and month

## Goal

For every `(departure airport, month)` pair, compute:

- the number of non-cancelled flights in three departure-delay bands:
  - `LOW`: `dep_delay < 15`
  - `MEDIUM`: `15 <= dep_delay <= 60`
  - `HIGH`: `dep_delay > 60`
- for each band:
  - average departure delay;
  - average arrival delay;
- the three most frequent cancellation/delay causes.

The same canonical input and output semantics are used by Spark Core,
Spark SQL and Hive.

## Canonical input

`data/processed/flights_cleaned.csv`

Relevant columns:

- `month`
- `origin`
- `dep_delay`
- `arr_delay`
- `cancelled`
- `cancellation_cause`
- `carrier_delay`
- `weather_delay`
- `nas_delay`
- `security_delay`
- `late_aircraft_delay`

## Delay-band semantics

Delay bands are defined only for records satisfying:

`cancelled = 0 AND dep_delay IS NOT NULL`

The bands are mutually exclusive and exhaustive over those records:

- `LOW`: `dep_delay < 15`
- `MEDIUM`: `15 <= dep_delay <= 60`
- `HIGH`: `dep_delay > 60`

Negative departure delays are valid and therefore belong to `LOW`.

For each band:

- the count includes every record belonging to the band;
- average departure delay is computed over all records in the band;
- average arrival delay ignores null `arr_delay` values;
- averages are rounded to 2 decimal places with `HALF_UP` semantics.

Cancelled flights never belong to a delay band, even if a non-null departure
delay happens to be present.

## Cause-frequency semantics

Cause frequency is measured as **flight-cause incidence**.

A cancelled flight contributes one incidence for its normalized
`cancellation_cause`, if present.

A non-cancelled flight contributes one incidence for every delay-cause column
whose attributed minutes are strictly greater than zero:

- `carrier_delay > 0` -> `CARRIER`
- `weather_delay > 0` -> `WEATHER`
- `nas_delay > 0` -> `NAS`
- `security_delay > 0` -> `SECURITY`
- `late_aircraft_delay > 0` -> `LATE_AIRCRAFT`

A single non-cancelled flight may therefore contribute to multiple causes.
Delay minutes are not used as frequency weights.

For each `(origin, month)`, causes are ordered deterministically by:

1. incidence count descending;
2. cause name ascending.

The first three are reported. If fewer than three causes are available, the
missing cause/count fields are null.

## Output schema

One row is produced for every `(origin, month)` pair occurring in the
canonical dataset:

1. `departure_airport`
2. `month`
3. `low_count`
4. `low_avg_dep_delay`
5. `low_avg_arr_delay`
6. `medium_count`
7. `medium_avg_dep_delay`
8. `medium_avg_arr_delay`
9. `high_count`
10. `high_avg_dep_delay`
11. `high_avg_arr_delay`
12. `top1_cause`
13. `top1_count`
14. `top2_cause`
15. `top2_count`
16. `top3_cause`
17. `top3_count`

Benchmark outputs do not contain a header row and are not globally sorted.
Deterministic ordering is applied only by the validation/preview utility.

## Main distributed operations

### Spark Core

- one aggregation by `(origin, month)` for band statistics;
- one aggregation by `(origin, month, cause)` for cause frequencies;
- one aggregation by `(origin, month)` to retain the top 3 causes;
- one left join by `(origin, month)`.

The parsed canonical RDD is persisted with `MEMORY_AND_DISK` so the common
input is not reparsed independently for the two analytical branches.

### Spark SQL

- conditional aggregation for band statistics;
- cause-incidence expansion followed by `GROUP BY`;
- `row_number()` window per `(origin, month)` for deterministic Top-3;
- left join of band statistics and Top-3 causes.

The canonical DataFrame is persisted with `MEMORY_AND_DISK` because it is
referenced by both analytical branches.

### Hive

- one grouped branch for band statistics;
- one cause-incidence branch using `explode(array(...))`;
- grouped cause counts and `row_number()` for deterministic Top-3;
- left join on `(origin, month)`.

The physical execution engine is documented separately from the HiveQL
program itself.
