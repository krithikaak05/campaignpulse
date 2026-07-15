"""
Generates a synthetic daily ad spend table by channel and loads it into
BigQuery Bronze. The Google Merchandise Store sample dataset has no real
spend data, so this fills that gap in a transparent, clearly labeled way,
letting downstream marts calculate cost per acquisition and return on ad
spend.

Only genuinely paid channels are generated here. Unpaid channels
(organic, direct, referral, social, affiliates) correctly receive zero
spend automatically through the join logic in fct_channel_performance,
so there is no need to fabricate zero-value rows for them.
"""

import argparse
import random
from datetime import date, timedelta

import pandas as pd
from google.cloud import bigquery

CHANNELS = [
    "google / cpc",
    "bing / cpc",
    "display / cpm",
]

PAID_CHANNELS = {"google / cpc", "bing / cpc", "display / cpm"}


def build_spend_rows(lookback_days: int) -> pd.DataFrame:
    rows = []
    # Anchored to the last date available in the Google Merchandise Store
    # sample dataset, a static historical snapshot spanning 2016-08-01
    # through 2017-08-01, not live data. Using date.today() here would
    # generate spend dates that never match any session date.
    anchor_date = date(2017, 7, 31)
    for offset in range(lookback_days):
        day = anchor_date - timedelta(days=offset)
        for channel in CHANNELS:
            base_spend = random.uniform(200, 1500) if channel in PAID_CHANNELS else 0.0
            rows.append(
                {
                    "spend_date": day,
                    "channel": channel,
                    "daily_spend_usd": round(base_spend, 2),
                }
            )
    return pd.DataFrame(rows)


def load_to_bigquery(df: pd.DataFrame, project: str, dataset: str) -> None:
    client = bigquery.Client(project=project)
    table_id = f"{project}.{dataset}.synthetic_ad_spend"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=[
            bigquery.SchemaField("spend_date", "DATE"),
            bigquery.SchemaField("channel", "STRING"),
            bigquery.SchemaField("daily_spend_usd", "FLOAT64"),
        ],
    )
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    print(f"Loaded {len(df)} rows into {table_id}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--lookback-days", type=int, default=365)
    args = parser.parse_args()

    spend_df = build_spend_rows(args.lookback_days)
    load_to_bigquery(spend_df, args.project, args.dataset)