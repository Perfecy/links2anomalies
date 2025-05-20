# links2anomalies
Detect anomalies in subject‑object relationships stored in PostgreSQL

---

## Overview

This project provides a lightweight PL/pgSQL toolkit that inspects two core tables—`subject_object_links` and `subject_properties`—to flag suspicious or statistically unusual link patterns.  Typical use‑cases include fraud detection, data‑quality auditing, and graph hygiene.

Core deliverables:

1. **Schema helpers** – optional DDL to create the canonical table structure.
2. **`detect_anomalous_links()`** – a configurable PL/pgSQL function that returns a set of anomalous rows.
3. **Sample data & demo** – scripts that load synthetic data and show typical results.

---

## Requirements

* PostgreSQL **12** (or newer 12.x minor release)
* PL/pgSQL (enabled by default)
* psql or any SQL client with access to the target database

No external extensions are required; the implementation relies solely on built‑in window functions and statistics tables.

---

## Schema

```
CREATE TABLE subject_object_links (
    link_id     bigserial PRIMARY KEY,
    subject_id  bigint       NOT NULL,
    object_id   bigint       NOT NULL,
    link_type   text         NOT NULL,
    created_at  timestamptz  NOT NULL DEFAULT now()
);

CREATE TABLE subject_properties (
    subject_id   bigint      NOT NULL,
    prop_key     text        NOT NULL,
    prop_value   text,
    updated_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (subject_id, prop_key)
);
```

If your schema differs, adjust the view `v_links` inside `functions/anomaly_detection.sql`.

---

## Installation

1. Clone or download this repository.
2. Run the DDL (optional if your tables already exist):

   ```bash
   psql -d your_db -f schema/schema.sql
   ```
3. Install the anomaly‑detection function:

   ```bash
   psql -d your_db -f functions/anomaly_detection.sql
   ```

---

## Usage

```sql
SELECT *
FROM   detect_anomalous_links(
           _lookback_days := 30,   -- sliding window for peer statistics
           _z_threshold   := 3,    -- z‑score cut‑off for rarity of a link
           _min_degree    := 10    -- ignore subjects with fewer than N links
       );
```

| Parameter        | Type | Default | Meaning                                                |
| ---------------- | ---- | ------- | ------------------------------------------------------ |
| `_lookback_days` | int  | 30      | Time window (days) used to compute moving baselines.   |
| `_z_threshold`   | int  | 3       | Minimum absolute z‑score for a link to be flagged.     |
| `_min_degree`    | int  | 10      | Minimum out‑degree of a subject to enter the analysis. |

The function returns:

* `subject_id`
* `object_id`
* `link_type`
* `created_at`
* `z_score` – how many standard deviations this link deviates from the subject’s historical pattern.

---

## Example

Run the demo script to generate 10 000 synthetic links with seeded anomalies:

```bash
psql -d your_db -f demo/load_sample_data.sql
SELECT * FROM detect_anomalous_links();
```

You should see \~25 injected outliers with z‑scores ≥ 3.

---
