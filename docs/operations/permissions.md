# Permissions: what the sandbox principal has, what it used, and what operating needs

**We started wide on purpose, and this doc tightens it.** The migration ran as one service principal, `Devin` (`e63a8604-…`), with `ALL_PRIVILEGES` on catalog `devin_dev` and ownership of it, so the build wouldn't stall on grants. This is the audit of what that identity actually used across modules 04 and 06 and the architecture build, and the smaller sets we'd run with from here.

## 1. What the principal holds (read from the workspace on 2026-10-31)

| Object | Privilege | Source |
|---|---|---|
| catalog `devin_dev` | owner | catalog created by javifigs10@gmail.com, ownership transferred to the principal |
| catalog `devin_dev` | `ALL_PRIVILEGES` | direct grant; inherited by every schema, table, MV and volume |
| schemas `nyc_taxi_{bronze,silver,gold,ops}`, `dbt_ref`, `dbt_ref_src` | owner | created by the principal (bundle, dbt) |
| volume `nyc_taxi_bronze.landing`, all tables and MVs | owner | created by the principal (bundle, pipeline, dbt, `check_refresh`) |
| pipeline `nyc_taxi`, job `nyc_taxi_daily` | `IS_OWNER`, run-as | bundle deploy |
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

## 3. Minimum set: operate profile

The operate identity is the job and pipeline **run-as**. It refreshes data, writes metrics, tags tables and nothing else. It doesn't deploy, create schemas, grant, or run parity. The pipeline owner owns the tables it creates, so most access comes from ownership rather than grants.

```sql
-- `nyc-taxi-operate` = a new service principal (needs an account/workspace admin to create)
GRANT USE CATALOG ON CATALOG devin_dev TO `nyc-taxi-operate`;
GRANT USE SCHEMA, CREATE TABLE, CREATE MATERIALIZED VIEW, APPLY TAG
  ON SCHEMA devin_dev.nyc_taxi_bronze TO `nyc-taxi-operate`;
GRANT USE SCHEMA, CREATE TABLE, CREATE MATERIALIZED VIEW, APPLY TAG
  ON SCHEMA devin_dev.nyc_taxi_silver TO `nyc-taxi-operate`;
GRANT USE SCHEMA, CREATE TABLE, CREATE MATERIALIZED VIEW, APPLY TAG
  ON SCHEMA devin_dev.nyc_taxi_gold TO `nyc-taxi-operate`;
GRANT USE SCHEMA, CREATE TABLE, APPLY TAG ON SCHEMA devin_dev.nyc_taxi_ops TO `nyc-taxi-operate`;
GRANT READ VOLUME, WRITE VOLUME ON VOLUME devin_dev.nyc_taxi_bronze.landing TO `nyc-taxi-operate`;
```

- **Why these:**
  - `CREATE TABLE` / `CREATE MATERIALIZED VIEW` are needed because the pipeline creates or replaces its datasets on a definition change.
  - The volume grants are for `download_month` and Auto Loader.
  - `nyc_taxi_ops` is where `check_refresh` creates its two tables on the first run; after that it owns them.
  - `APPLY TAG` is for `apply_tags`.
- **No `SELECT` or `MODIFY` grants:** everything it reads or writes, it owns: the pipeline tables, the event log and the ops tables.
- **Workspace:** the `workspace-access` entitlement only. It needs no warehouse (job tasks and the pipeline run on serverless jobs compute) and no `databricks-sql-access`.
- **Ownership:** it should own the pipeline and job (or be their run-as), so the tables it creates are its own. `CAN_MANAGE_RUN` on the job goes to the on-call humans, not to this principal.
- **Deliberately not in this profile:** `CREATE SCHEMA`, `CREATE VOLUME`, `MANAGE`, catalog ownership, `dbt_ref*`, `CREATE FUNCTION`/`MODEL`, warehouse `CAN_USE`.
- **Not tested:** derived from what each task does. Proving it needs the new principal: deploy with `run_as` set to it, run the job once, and see if anything fails with `PERMISSION_DENIED`.

## 4. The other profiles

| Profile | Who | Needs |
|---|---|---|
| **deploy** | whoever runs `bundle deploy` (a person or a CD principal) | owner of the four `nyc_taxi_*` schemas and the volume (or `CREATE SCHEMA` / `CREATE VOLUME` on the catalog to create them), workspace-access. Sets `run_as` to the operate principal. |
| **parity / test** | the person or principal running `tests/parity` | `USE CATALOG`; `USE SCHEMA` + `SELECT` on `nyc_taxi_*`; `USE SCHEMA` + `CREATE TABLE` on `dbt_ref`, `dbt_ref_src`; warehouse `CAN_USE`; serverless compute. The opt-in rerun test also needs `CAN_RUN` on the pipeline. |
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
