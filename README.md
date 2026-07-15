# CampaignPulse

An end to end marketing analytics pipeline built on Airflow, dbt, and Google BigQuery. It ingests the GA4 obfuscated sample ecommerce dataset from BigQuery public data, models it through a Bronze, Silver, Gold architecture, and produces channel performance, funnel, and cost per acquisition metrics that a marketing team would actually use.

## Why this project exists

Most portfolio data pipelines stop at "load some data and run a query." This one is built to mirror how a production marketing analytics stack is actually run: partitioned and clustered warehouse tables, incremental dbt models instead of full refreshes, a slowly changing dimension tracked with a snapshot, automated testing on every pull request, and orchestration with retries and failure alerting rather than a single flat script.

## Architecture

```
GA4 public dataset (BigQuery)
        |
   Airflow DAG (Docker, local)
        |
   Bronze  ->  ga4_events_raw, synthetic_ad_spend  (partitioned by date)
        |
   dbt Silver -> stg_ga4_events, stg_ad_spend, int_sessions, int_sessions_channel
        |
   dbt Gold  -> fct_channel_performance, fct_conversion_funnel
        |
   Looker Studio or Streamlit dashboard
```

A campaign metadata dimension is tracked with a dbt snapshot using the timestamp strategy, demonstrating Slowly Changing Dimension Type 2 history, similar to the SCD work done on production Azure pipelines.

## Why GA4 public data instead of a live API

The GA4 obfuscated sample ecommerce dataset already lives in BigQuery public data and spans multiple months of real event volume, several million rows, which lets the project demonstrate partition pruning, clustering, and query cost control against a realistic data size rather than a toy CSV. The pipeline still shows a full ingestion pattern by moving data into a project owned Bronze dataset with its own partitioning and retention.

Ad spend is not part of the GA4 export, so a synthetic daily spend table is generated per channel and clearly labeled as synthetic in `scripts/synthetic_spend_generator.py`. This makes cost per acquisition and return on ad spend calculations possible without fabricating anything about the actual event data.

## Cost control

Everything in this project is designed to stay inside free tiers.

- Airflow runs locally through Docker Compose. There is no Cloud Composer cost.
- BigQuery free tier includes 10 GB storage and 1 TB of query processing per month, which comfortably covers this project's scale.
- Every dbt model that queries a large table uses `maximum_bytes_billed` and is partitioned by date, so incremental runs only scan recent partitions instead of the full history.
- Google Cloud Storage usage stays under the 5 GB free tier.
- dbt Core is open source with no license cost.
- Set a low dollar billing alert in the GCP console as a safety net.

## Repository structure

```
campaignpulse/
  dags/                     Airflow DAG and templated SQL for Bronze extraction
  dbt/
    models/staging/         Flattened, typed source models
    models/intermediate/    Session rollups and channel normalization
    models/marts/           Gold layer facts: channel performance, funnel
    snapshots/               SCD Type 2 campaign metadata
    seeds/                   Small reference data
  scripts/                  Synthetic ad spend generator
  .github/workflows/        CI pipeline running dbt build and test on every PR
  docker-compose.yaml       Local Airflow stack
  Dockerfile.airflow        Custom image with dbt and BigQuery adapter installed
```

## Running locally

1. Create a GCP project and a service account with BigQuery Data Editor and Job User roles. Download the key as `gcp/service_account.json`.
2. Copy `.env.example` to `.env` and fill in your project id.
3. Create the Bronze, Silver, and Gold datasets in BigQuery, or let the first DAG run create them.
4. Start the stack:

```
docker compose up airflow-init
docker compose up
```

5. Open `localhost:8080`, log in with the admin user created in `airflow-init`, and trigger the `campaignpulse_elt` DAG.
6. Once a run completes, dbt docs are generated at `dbt/target/index.html` and can be hosted on GitHub Pages.

## Continuous integration

Every pull request that touches the `dbt/` folder runs `dbt seed`, `dbt run`, and `dbt test` against a dedicated CI dataset in GitHub Actions, using repository secrets for the service account key. This keeps the main branch protected from broken models before anything reaches production tables.

## Metrics produced

- Sessions, users, and revenue by channel and day
- Cost per acquisition and return on ad spend by channel and day
- Full funnel conversion rates from item view through purchase, by channel
- Slowly changing campaign ownership and budget tier history

## Tech stack

Airflow, Docker, dbt Core, Google BigQuery, Google Cloud Storage, GitHub Actions, Python.
