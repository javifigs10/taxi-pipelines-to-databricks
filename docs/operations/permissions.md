# Permissions: what the sandbox principal has, what it used, and what operating needs

**We started wide on purpose, and this doc tightens it.** The migration ran as one service principal, `Devin` (`e63a8604-…`), with `ALL_PRIVILEGES` on catalog `devin_dev` and ownership of it, so the build wouldn't stall on grants. This is the audit of what that identity actually used across modules 04 and 06 and the architecture build, and the smaller sets we'd run with from here.

## 1. What the principal holds (read from the workspace on 2026-10-31)

| Object | Privilege | Source |
|---|---|---|
| catalog `devin_dev` | owner | catalog created by javifigs10@gmail.com, ownership transferred to the principal |
| catalog `devin_dev` | `ALL_PRIVILEGES` | direct grant; inherited by every schema, table, MV and volume |
| schemas `nyc_taxi_{bronze,silver,gold,ops}`, `dbt_ref`, `dbt_ref_src` | owner | created by the principal (bundle, dbt) |
| volume `nyc_taxi_bronze.landing`, all tables and MVs | owner | created by the principal (bundle, pipeline, dbt, `check_refresh`). Since 2026-11-01 the 31 pipeline tables are owned by `devin-operate` (section 3) |
| pipeline `nyc_taxi`, job `nyc_taxi_daily` | `IS_OWNER` (run-as until 2026-11-01, now `devin-operate`, see section 3) | bundle deploy |
| warehouse `Serverless Starter Warehouse` | `CAN_USE` via `users` (per the workspace owner; the principal can't read the warehouse ACL) | group |
| workspace | entitlements `workspace-access`, `databricks-sql-access`, `workspace-consume`; group `users`; not an admin | |

`ALL_PRIVILEGES` on a catalog expands to: `USE CATALOG`, `USE SCHEMA`, `BROWSE`, `CREATE SCHEMA`, `CREATE TABLE`, `CREATE MATERIALIZED VIEW`, `CREATE VOLUME`, `CREATE FUNCTION`, `CREATE MODEL`, `READ VOLUME`, `WRITE VOLUME`, `SELECT`, `MODIFY`, `REFRESH`, `EXECUTE`, `APPLY TAG`, `EXTERNAL USE SCHEMA`, plus ownership's `MANAGE`.

## 2. What was actually used

| Privilege | Used? | For |
|---|---|---|
| `USE CATALOG`, `USE SCHEMA` | yes | everything |
| `CREATE SCHEMA` | yes | bundle schemas, `nyc_taxi_ops`, `dbt_ref`, `dbt_ref_src`, the preflight schema (created and dropped) |
| `CREATE VOLUME` | yes | `landing` |
| `READ VOLUME`, `WRITE VOLUME` | yes | `download_month`, seed copies, Auto Loader, the quarantine proof files |
| `CREATE TABLE` | yes | bronze streaming tables, `nyc_taxi_ops` tables, `dbt_ref` tables and views |
| `CREATE MATERIALIZED VIEW` | yes | silver and gold MVs |
| `SELECT` | yes | parity tests, `dbt_ref`, `check_refresh`, event-log analysis |
| `MODIFY` | yes, on owned tables only | `check_refresh` appends, dbt incremental `fct_trips` in `dbt_ref` |
| `APPLY TAG` | yes | `apply_tags` task |
| `MANAGE` (ownership) | yes, once | read-only grant to javifigs10@gmail.com |
| `CREATE FUNCTION`, `CREATE MODEL`, `EXECUTE`, `REFRESH`, `EXTERNAL USE SCHEMA` | **no** | |
| `BROWSE` | no (`USE` covers it) | |
| warehouse `CAN_USE` | yes | `dbt_ref` builds (dbt-databricks), the preflight `warehouses list` |
| `databricks-sql-access` | yes | same as the warehouse |
| `system.billing` | not held | the DBU query failed with `PERMISSION_DENIED` (no workaround) |
| Disable predictive optimization | not used | you chose to leave it on |

So 11 of the 17 catalog privileges were used, but only 3 are needed once things are running (section 3). The remaining 8 belong to building and testing, not operating.

## 3. Minimum set: operate profile (proven with `devin-operate`)

The operate identity is the job and pipeline **run-as**. It refreshes data, writes metrics, tags tables and nothing else. It doesn't deploy, create schemas, grant, or run parity. It is `devin-operate` (`6432d72f-…`), created by the workspace owner with the `workspace-access` entitlement and nothing else, and wired in through `var.run_as_sp` in `databricks.yml`.

**How it was proven (2026-11-01).** I granted exactly the profile derived in the first draft of this doc, deployed with `run_as` set to `devin-operate`, ran `nyc_taxi_daily` with `month=2019-02`, and added only what a real failure asked for:

| # | Run | Failed with | Added |
|---|---|---|---|
| 1 | job run `706773312798563` | `download_month`: *Cannot read the python file /Workspace/Users/e63a8604-…/.bundle/nyc_taxi/dev/files/src/tasks/download_month.py … the identity used to run this job, 6432d72f-…, lacks the required permissions* | read on the bundle's workspace folder |
| 2 | job run `512986504017596` | `refresh_pipeline`: *User 6432d72f-… does not have Run permissions on pipeline 58fb3a98-…* | `CAN_RUN` on the pipeline |
| 3 | job run `402228683487201` | `check_refresh`: *PERMISSION_DENIED: Principal 'devin-operate' does not have SELECT on Table 'devin_dev.nyc_taxi_ops.refresh_metrics'* | `SELECT, MODIFY` on the two `nyc_taxi_ops` tables |
| ✓ | repair of `402228683487201` (`check_refresh` only), then full refresh `1d690743` | — | all tasks and a full refresh green as `devin-operate` |

So the first draft was **short by three, and all three came from outside what I'd modelled**: two are workspace permissions (not Unity Catalog at all) and one comes from table ownership history.

**The proven set.** Unity Catalog:

```sql
-- devin-operate = 6432d72f-ea90-4201-a365-27d358d9f0ff
GRANT USE CATALOG ON CATALOG devin_dev TO `6432d72f-ea90-4201-a365-27d358d9f0ff`;
GRANT USE SCHEMA, CREATE TABLE, CREATE MATERIALIZED VIEW, APPLY TAG
  ON SCHEMA devin_dev.nyc_taxi_bronze TO `6432d72f-ea90-4201-a365-27d358d9f0ff`;
GRANT USE SCHEMA, CREATE TABLE, CREATE MATERIALIZED VIEW, APPLY TAG
  ON SCHEMA devin_dev.nyc_taxi_silver TO `6432d72f-ea90-4201-a365-27d358d9f0ff`;
GRANT USE SCHEMA, CREATE TABLE, CREATE MATERIALIZED VIEW, APPLY TAG
  ON SCHEMA devin_dev.nyc_taxi_gold TO `6432d72f-ea90-4201-a365-27d358d9f0ff`;
GRANT USE SCHEMA, CREATE TABLE, APPLY TAG ON SCHEMA devin_dev.nyc_taxi_ops TO `6432d72f-ea90-4201-a365-27d358d9f0ff`;
GRANT READ VOLUME, WRITE VOLUME ON VOLUME devin_dev.nyc_taxi_bronze.landing TO `6432d72f-ea90-4201-a365-27d358d9f0ff`;
-- added after failure 3: these two tables were created by Devin's earlier check runs
GRANT SELECT, MODIFY ON TABLE devin_dev.nyc_taxi_ops.refresh_metrics   TO `6432d72f-ea90-4201-a365-27d358d9f0ff`;
GRANT SELECT, MODIFY ON TABLE devin_dev.nyc_taxi_ops.quarantine_counts TO `6432d72f-ea90-4201-a365-27d358d9f0ff`;
```

Workspace (in the bundle, so it is re-applied on every deploy):

```yaml
# databricks.yml, added after failures 1 and 2
permissions:
  - service_principal_name: ${var.run_as_sp}
    level: CAN_RUN
```

Top-level `CAN_RUN` turns into `CAN_READ` on the bundle folder (failure 1), `CAN_RUN` on the pipeline (failure 2) and `CAN_MANAGE_RUN` on the job. The last one wasn't asked for by any failure; it's the side effect of fixing both in one bundle-native line. The alternative (a folder ACL plus a pipeline-only permission) can't be expressed in the bundle and would drift.

Plus, outside the catalog:
- **Entitlement:** `workspace-access` only. Proven: no `databricks-sql-access`, no warehouse, no cluster policy. Serverless job tasks and the serverless pipeline started without them.
- **For whoever deploys:** the "Service Principal: User" role on `devin-operate`. Databricks refuses to set `run_as` to a principal you can't use. That's on the deploy profile, not on operate.

**What changed on the workspace when `run_as` switched:**
- **Table ownership moved.** All 31 pipeline tables (streaming tables, MVs, their `__materialization_*` tables and the event log) went from `Devin` to `devin-operate` on deploy, as the Databricks docs describe. Schemas, the volume, the job and the pipeline stay owned by `Devin` (the deploy identity).
- **The event log closed to everyone but its owner.** After the switch, `Devin` got `INSUFFICIENT_PERMISSIONS … does not have SELECT on Table … event_log_58fb3a98…` despite `ALL_PRIVILEGES` on the catalog. `check_refresh` is unaffected (it runs as the owner). For ad hoc analysis from another identity, use the pipeline events API (`databricks pipelines list-pipeline-events`, pipeline `CAN_VIEW` is enough), or have the owner grant `SELECT`.
- **On a fresh deploy, the two `nyc_taxi_ops` table grants aren't needed:** `check_refresh` would create those tables as `devin-operate` on its first run and own them. They're in the list because this workspace has history; I chose a grant over an ownership transfer because ownership also carries `MANAGE`.

**What the proof does and doesn't show.** The set is proven **sufficient**: every task in the job and a full refresh ran as `devin-operate` with nothing else. I haven't proven each item is **necessary**. The pre-existing grants were never removed one at a time, which would take one pipeline run per grant. `CREATE TABLE` on the target schemas is listed as required in the Databricks run-as docs. `CREATE MATERIALIZED VIEW` and `APPLY TAG` follow from what the pipeline and `apply_tags` do.

- **Deliberately not in this profile:** `CREATE SCHEMA`, `CREATE VOLUME`, `MANAGE`, catalog ownership, `dbt_ref*`, `CREATE FUNCTION`/`MODEL`, warehouse `CAN_USE`, `databricks-sql-access`.

## 4. The other profiles

| Profile | Who | Needs |
|---|---|---|
| **deploy** | whoever runs `bundle deploy` (a person or a CD principal) | owner of the four `nyc_taxi_*` schemas and the volume (or `CREATE SCHEMA` / `CREATE VOLUME` on the catalog to create them), workspace-access, and the "Service Principal: User" role on `devin-operate` (proven: `run_as` is set by it). |
| **parity / test** | the person or principal running `tests/parity` | `USE CATALOG`; `USE SCHEMA` + `SELECT` on `nyc_taxi_*`; `USE SCHEMA` + `CREATE TABLE` on `dbt_ref`, `dbt_ref_src`; warehouse `CAN_USE`; serverless compute. The opt-in rerun test also needs `CAN_RUN` on the pipeline. Since the run-as switch the pipeline tables are owned by `devin-operate`, so this profile needs an explicit `SELECT` (Devin gets it from catalog `ALL_PRIVILEGES`), and it can't read the event log. |
| **CI** (`bundle validate`) | GitHub Actions via OIDC | workspace-access only. No Unity Catalog grants and no warehouse: validate never touches data. See below. |
| **reader** | javifigs10@gmail.com | `USE CATALOG`, `USE SCHEMA`, `SELECT` on `devin_dev` (in place today) |

## CI identity

`.github/workflows/ci.yml` authenticates with `DATABRICKS_AUTH_TYPE=github-oidc`: GitHub issues a short-lived OIDC token and Databricks exchanges it for a workspace token. No Databricks secret is stored in the repo or in GitHub. One-off setup, by an account admin:

1. **Create a service principal** `nyc-taxi-ci`, add it to the workspace with `workspace-access` only, and note its application (client) ID.
2. **Add a federation policy** on it that trusts this repo's PR runs:
   ```bash
   databricks account service-principal-federation-policy create <ci-sp-numeric-id> --json '{
     "oidc_policy": {
       "issuer": "https://token.actions.githubusercontent.com",
       "audiences": ["<databricks-account-id>"],
       "subject": "repo:javifigs10/taxi-pipelines-to-databricks:pull_request"
     }}'
   ```
3. **Set two repository variables** (Settings → Secrets and variables → Actions → *Variables*, not secrets): `DATABRICKS_HOST` = the workspace URL, and `DATABRICKS_CI_CLIENT_ID` = the CI principal's application ID. Neither is a credential.

Until that's done, the `bundle-validate` job skips with a warning annotation pointing here (it does not fail the PR), so watch for the warning. The `checks` job (ruff and the DBU-free tests) needs no setup.
