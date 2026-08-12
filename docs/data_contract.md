# Data contract — Flight Delay 2024

This document defines the canonical cleaned dataset consumed by every implementation (Hive, Spark Core, Spark SQL).

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
| `dep_delay` | double | yes | Departure delay in minutes. Negative values are valid and preserved. |
| `arr_delay` | double | yes | Arrival delay in minutes. Negative values are valid and preserved. Null values are excluded from arrival-delay aggregates, not replaced with zero. |
| `cancelled` | int | no | Binary flag, `0` or `1`. Invalid values are discarded. |
| `cancellation_cause` | string | yes | Normalized cancellation category: `CARRIER`, `WEATHER`, `NAS`, `SECURITY`, or null. |
| `carrier_delay` | double | no | Minutes attributed to carrier delay; missing values normalized to `0`. |
| `weather_delay` | double | no | Minutes attributed to weather delay; missing values normalized to `0`. |
| `nas_delay` | double | no | Minutes attributed to NAS delay; missing values normalized to `0`. |
| `security_delay` | double | no | Minutes attributed to security delay; missing values normalized to `0`. |
| `late_aircraft_delay` | double | no | Minutes attributed to late-arriving-aircraft delay; missing values normalized to `0`. |

## Row-level cleaning policy

1. Exclude diverted flights (`diverted != 0`). The chosen analyses use arrival-delay statistics at the scheduled destination; diverted flights are treated as not directly comparable for these metrics.
2. Keep cancelled flights. They are required for cancellation rates and cancellation causes.
3. Preserve negative `dep_delay` and `arr_delay`; they represent early departure/arrival, not errors.
4. Do not replace missing `dep_delay` or `arr_delay` with zero.
5. Do not deduplicate after projection: two distinct flights can become identical after keeping only analytical columns. Any duplicate analysis must therefore be performed on the original full row.

## Cause representation

Cancellation codes are normalized as follows:

- `A -> CARRIER`
- `B -> WEATHER`
- `C -> NAS`
- `D -> SECURITY`

For non-cancelled flights, **all five delay-cause minute columns are preserved** rather than collapsing them into a single dominant cause.

This decision follows the exploratory audit of the 10,000-row sample: 2,119 non-cancelled rows had at least one positive delay-cause category, and 1,086 had two or more positive categories (the audit was performed before removing the 42 diverted rows). Therefore, selecting only the maximum-valued cause would discard a substantial amount of available causal information.

For Analysis 3.2, cause frequency is defined as **flight-cause incidence**:

- a cancelled flight contributes one occurrence for its normalized cancellation cause, if available;
- a non-cancelled flight contributes one occurrence to every delay category whose attributed minutes are strictly greater than zero;
- cause minutes are not used as frequency weights.

Thus one flight may contribute to multiple delay-cause categories. This convention is explicit, reproducible, and preserves the source information needed by the requested Top-3 causes.

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

Top-3 causes are computed for the same `(origin, month)` from the flight-cause incidences defined above. Ranking is deterministic: frequency descending, then cause name ascending.

## Benchmark rule

All technologies must consume the same canonical cleaned CSV. Preprocessing is performed separately and is excluded from the analytical benchmark timings. Spark benchmark runs must avoid triggering the same logical computation twice merely to both preview and save the output.
