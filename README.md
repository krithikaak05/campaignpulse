# CampaignPulse

An end to end marketing analytics pipeline built on Airflow, dbt, and Google BigQuery. It ingests a full year of real session data from the Google Merchandise Store public sample dataset, models it through a Bronze, Silver, Gold architecture, and produces channel performance, funnel, and cost per acquisition metrics visualized in an interactive Looker Studio dashboard.

**Live dashboard:** https://datastudio.google.com/reporting/15091e98-12eb-4b85-b382-84a640f25e73

## Why this project exists

Most portfolio data pipelines stop at "load some data and run a query." This one is built to mirror how a production marketing analytics stack is actually run: partitioned and clustered warehouse tables, a slowly changing dimension tracked with a snapshot, automated dbt testing on every pull request, orchestration with retries and failure alerting, and CI/CD authenticated without a single stored credential.

## The headline finding

Across the full year of data, paid channels (Paid Search and Display) spent $925,637 combined and returned only $121,574 in revenue, a 0.13x return on ad spend. Meanwhile Referral traffic, which costs nothing, generated $644,802, more than the paid channels combined. The dashboard is built specifically to make that comparison impossible to miss.

## Architecture

```
Google Merchandise Store sample dataset (BigQuery public data)
        |
   Airflow DAG (Docker, local)
        |
   Bronze  ->  ga_sessions_raw, synthetic_ad_spend  (partitioned by date)
        |
   dbt Silver -> stg_ga_sessions, stg_ad_spend, int_sessions_channel
        |
   dbt Gold  -> fct_channel_performance, fct_conversion_funnel
        |
   Looker Studio dashboard
```

A campaign metadata dimension is tracked with a dbt snapshot using the timestamp strategy, demonstrating Slowly Changing Dimension Type 2 history, similar to the SCD work done on production Azure pipelines.

Every model in Silver and Gold is a full table rebuild rather than incremental. Since Bronze is fully truncated and reloaded on every run (the source dataset is a fixed historical window, not a live rolling feed), there is no meaningful "new rows since last run" slice to merge, so a plain rebuild is both simpler and more correct than incremental logic here.

## Why this dataset instead of live GA4

The project originally used the GA4 obfuscated sample ecommerce dataset, but that dataset only spans a 92 day historical window, which ruled out any real month over month or seasonal comparison. It was swapped for the Google Merchandise Store sample dataset (`bigquery-public-data.google_analytics_sample.ga_sessions_*`), a Universal Analytics export spanning a full year, 2016-08-01 through 2017-08-01. This dataset also includes a native `channelGrouping` field, a maintained marketing taxonomy (Organic Search, Direct, Referral, Paid Search, Social, Display, Affiliates), which is used directly instead of hand rolled source and medium pattern matching.

Ad spend is not part of either public dataset, so a synthetic daily spend table is generated for the genuinely paid channels (Paid Search, Display) and clearly labeled as synthetic in `scripts/synthetic_spend_generator.py`. Unpaid channels correctly receive zero spend through the join logic rather than fabricated zero value rows.

## Cost control

Everything in this project is designed to stay inside free tiers.

- Airflow runs locally through Docker Compose. There is no Cloud Composer cost.
- BigQuery free tier includes 10 GB storage and 1 TB of query processing per month, which comfortably covers this project's scale.
- Every Bronze extraction query uses `maximum_bytes_billed` and every table is partitioned by date.
- dbt Core is open source with no license cost.
- Set a low dollar billing alert in the GCP console as a safety net.

## Repository structure

```
campaignpulse/
  dags/                     Airflow DAG and templated SQL for Bronze extraction
  dbt/
    models/staging/         Flattened, typed session and spend models
    models/intermediate/    Channel normalization using native channelGrouping
    models/marts/           Gold layer facts: channel performance, funnel
    snapshots/               SCD Type 2 campaign metadata
    seeds/                   Small reference data
  scripts/                  Synthetic ad spend generator
  .github/workflows/        CI pipeline running dbt build and test on every PR
  docker-compose.yaml       Local Airflow stack
  Dockerfile.airflow        Custom image with dbt and BigQuery adapter installed
```

## Running locally

1. Create a GCP project. Authenticate locally with `gcloud auth application-default login` (this project uses Application Default Credentials, not a downloaded service account key, since key creation is blocked by default on many GCP organizations now).
2. Copy `.env.example` to `.env` and fill in your project id.
3. Create the Bronze, Silver, Gold, and CI datasets in BigQuery.
4. Start the stack:

```
docker compose up airflow-init
docker compose up
```

5. Open `localhost:8080`, log in with the admin user created in `airflow-init`, and trigger the `campaignpulse_elt` DAG.
6. Once a run completes, dbt docs are generated at `dbt/target/index.html` and can be hosted on GitHub Pages.

## Continuous integration

Every pull request that touches the `dbt/` folder runs `dbt seed`, `dbt run`, and `dbt test` against a dedicated CI dataset in GitHub Actions. Authentication uses Workload Identity Federation, not a downloaded service account key, GitHub Actions proves its identity directly to Google Cloud through a short lived token exchange scoped to this specific repository, with no long lived credential ever stored as a secret. This is the current recommended pattern for CI to cloud authentication.

## Metrics produced

- Sessions, users, and revenue by channel and day
- Cost per acquisition and return on ad spend by channel and day
- Full funnel conversion rates from item view through purchase, by channel
- Slowly changing campaign ownership and budget tier history

## Dashboard

The Looker Studio dashboard includes a date range filter, a channel filter, four KPI scorecards (total revenue, total sessions, total paid spend, overall paid ROAS), and four charts: monthly session trend, revenue per session by channel, paid spend versus revenue trended monthly, and session share by channel.

View it live: https://datastudio.google.com/reporting/15091e98-12eb-4b85-b382-84a640f25e73

## Tech stack

Airflow, Docker, dbt Core, Google BigQuery, GitHub Actions, Workload Identity Federation, Looker Studio, Python.
