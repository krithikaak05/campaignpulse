{{ config(materialized="view") }}

select
    spend_date,
    lower(trim(channel)) as channel,
    case
        when lower(trim(channel)) like '%cpc%' then 'Paid Search'
        when lower(trim(channel)) like '%cpm%' or lower(trim(channel)) like '%display%' then 'Display'
        else 'Other'
    end as channel_group,
    daily_spend_usd
from {{ source('bronze', 'synthetic_ad_spend') }}