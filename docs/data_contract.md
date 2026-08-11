# Data contract — Flight Delay 2024

This document defines the canonical cleaned dataset used by every implementation (Hive, Spark Core, Spark SQL).

## Chosen analyses

- Analysis 3.1: statistics by `(airline, departure_airport)`.
- Analysis 3.2: delay report by `(departure_airport, month)`.

The project therefore uses departure airports, not routes, for Analysis 3.1.

## Canonical columns

| Column | Type | Nullable | Meaning / policy |
|---|---|---:|---|
| `month` | int | no | Month in `[1,12]`. Invalid rows are discarded. |
| `op_unique_carrier` | string | no | Airline code, trimmed and upper-cased. Empty values are discarded. |
| `origin` | string | no | Departure airport code, trimmed and upper-cased. Empty values are discarded. |
| `dep_delay` | double | yes | Departure delay in minutes. Negative values are valid (early departure) and are preserved. |
| `arr_delay` | double | yes | Arrival delay in minutes. Negative values are valid (early arrival) and are preserved. Null values are excluded from arrival-delay aggregates, not replaced with zero. |
| `cancelled` | int | no | Binary flag, `0` or `1`. Invalid values are discarded. |
| `cause_type` | string | yes | `CANCELLATION`, `DELAY`, or null. |
| `cause` | string | yes | Normalized category: `CARRIER`, `WEATHER`, `NAS`, `SECURITY`, `LATE_AIRCRAFT`, or null. |

## Row-level cleaning policy

1. Exclude diverted flights (`diverted != 0`). The two chosen analyses use arrival-delay statistics at the scheduled destination; diverted flights are therefore treated as not comparable for these metrics.
2. Keep cancelled flights. They are required to compute cancellation rates and cancellation causes.
3. Preserve negative `dep_delay` and `arr_delay`: they represent early departure/arrival, not errors.
4. Do not replace missing `dep_delay` or `arr_delay` with zero.
5. Do not perform deduplication after projection: two distinct flights can become identical after keeping only the analytical columns. Exact-duplicate analysis, if needed, must be performed on the original full row.

## Normalized cause

Cancellation codes are mapped to semantic categories:

- `A -> CARRIER`
- `B -> WEATHER`
- `C -> NAS`
- `D -> SECURITY`

For non-cancelled flights, the five delay-minute columns are used to determine a single *dominant* delay cause: the category with the largest number of attributed delay minutes. If every value is zero, `cause` is null.

If two delay categories have exactly the same maximum value, the deterministic priority is:

`CARRIER > WEATHER > NAS > SECURITY > LATE_AIRCRAFT`.

This is an implementation convention and must be documented in the final report.

## Metric semantics

### Analysis 3.1

Grouping key: `(op_unique_carrier, origin)`.

- `flight_count`: count of all cleaned records in the group, including cancelled flights.
- `arrival_delay_min/max/avg`: computed only on non-null `arr_delay` values.
- `cancellation_rate = cancelled_flights / flight_count`.
- `months`: distinct months sorted increasingly.

### Analysis 3.2

Grouping base: `(origin, month)`.

Delay bands are defined only for non-cancelled flights with a non-null `dep_delay`:

- `LOW`: `dep_delay < 15`
- `MEDIUM`: `15 <= dep_delay <= 60`
- `HIGH`: `dep_delay > 60`

For each band:

- count the flights in the band;
- average `dep_delay` over the band;
- average `arr_delay` only over non-null arrival delays.

Top-3 causes are computed over all cleaned records with a non-null normalized `cause` for the same `(origin, month)`. A cancelled flight contributes its cancellation cause; a non-cancelled delayed flight contributes its dominant delay cause. Ranking is deterministic: frequency descending, then cause name ascending.

## Benchmark rule

All technologies must consume the same canonical cleaned CSV. Preprocessing time is measured separately from analytical job time. Spark benchmark runs must avoid triggering the same logical computation twice merely to both preview and save the output.
