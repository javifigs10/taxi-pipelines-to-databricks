# ADR-0022: Record load order in an append-only landing ledger, not file modification times

- **Status:** **Proposed** 2026-11-01. Logged as a follow-up in the architecture-pass review; not built. Stays Proposed until the bronze contract change below has been discussed.
- **Behaviour:** changes how B1 ([ADR-0001](0001-b1-bounded-fct-trips-high-water-mark.md)) learns the load order; touches B7 ([ADR-0007](0007-b7-kestra-hash-identity-bronze.md)) bronze
- **Decided:** not yet
- **Differs from legacy output:** no, as long as files are landed once in order. It only changes what happens when a file is landed again.
- **⚠ Bronze contract change:** yes. It adds a table to bronze that is written by the landing task, and it changes what `load_batches` reads.

## Context

B1 drops late rows, so which trips `fct_trips` keeps depends on the **order** files were loaded. Today that order is inferred, not recorded:

```
bronze:   _file_modified_at = _metadata.file_modification_time          -- per row
silver:   load_batches.first_landed_at = min(_file_modified_at) per file_month
          batch_seq = row_number() over (order by first_landed_at, file_month)
gold:     fct_trips last_merge uses batch_seq + hwm
```

Anything that rewrites a file's modification time therefore reorders the batches. On the next full refresh, `fct_trips` changes silently: no code change, no failed expectation, and nothing in quarantine. Examples:
- re-copying a file into the volume;
- restoring the volume from backup;
- migrating storage or cloning the catalog;
- `download_month` landing a month again.

This isn't hypothetical. In the architecture-pass PR we used it on purpose twice to switch between the late and in-order scenarios with one refresh each. Re-landing the January files moved `fct_trips` from 15,500,838 to 7,405,469 rows, and re-landing February moved it back. That's convenient for a test, and exactly what a production pipeline must not do by accident.

This is the first thing a Databricks architect would push back on in an end-to-end review.

## Proposed decision

Make load order a fact the pipeline records once, at landing time:

```
-- bronze, append-only, written by download_month (and only by it)
nyc_taxi_bronze.landing_ledger(
  file_month  STRING,     -- yyyy-MM
  taxi        STRING,
  file_name   STRING,
  batch_seq   BIGINT,     -- next seq at first landing of this file_month
  landed_at   TIMESTAMP,
  bytes       BIGINT,
  sha256      STRING
)
-- silver
load_batches: batch_seq/first_landed_at come from landing_ledger (first row per file_month),
              not from min(_file_modified_at)
```

- **A re-landed file keeps its original `batch_seq`.** The ledger already has a row for that `file_month`. A new position takes a deliberate entry, for example a `--reorder` replay flag that appends a new batch.
- **`_file_modified_at` stays in bronze** as provenance, but nothing downstream orders by it.
- **The checksum turns "same file landed again" into something we can see.** A re-land with a different checksum fails `download_month` loudly instead of silently replacing data.

## Consequences

- **Good:** `fct_trips` depends only on data the pipeline recorded, so restores, re-copies, storage migrations and catalog clones can't change it. Replay becomes explicit and auditable.
- **Bad, the bronze contract changes:**
  - a new table that the operate principal writes (one more `CREATE TABLE`/ownership in `nyc_taxi_bronze`, already covered by the operate profile);
  - `download_month` takes on a write that has to succeed before the pipeline runs;
  - anyone who lands files outside the job has to write the ledger too, or their files never get a batch.
- **Bad, tests:** the parity replay recipe (re-land January to get the late scenario) stops working. Scenarios would be set through the ledger instead, a small fixture change in `tests/parity`.
- **Migration:** seed the ledger from today's `load_batches` (two rows) so the current order is kept. After that, a full refresh must give an identical `fct_trips`, and parity proves it.
- **Cost:** negligible, one row per file.

## Alternatives considered

- **Keep using modification times and document "never re-copy".** That relies on everyone remembering, and restores and migrations don't ask.
- **Order by `file_month`.** Simple, but it erases late arrival entirely, which is the B1 behaviour the user chose to keep.
- **Use Auto Loader's checkpoint/ingestion time (`_ingested_at`).** A full refresh resets it, so it's no more stable than the modification time.
