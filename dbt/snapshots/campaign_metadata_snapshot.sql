{% snapshot campaign_metadata_snapshot %}

{{
    config(
        target_schema="campaignpulse_silver",
        unique_key="campaign_name",
        strategy="timestamp",
        updated_at="updated_at",
    )
}}

select
    campaign_name,
    channel,
    owner,
    budget_tier,
    updated_at
from {{ ref('campaign_metadata_seed') }}

{% endsnapshot %}
