-- Channel level daily performance. Joins session activity against spend to
-- calculate cost per acquisition and return on ad spend, which are the two
-- metrics marketing stakeholders ask for most often. Grouped by
-- channel_group, GA's native marketing category taxonomy. Rebuilt fully
-- on every run rather than incrementally, since Bronze itself is fully
-- truncated and reloaded each run, there is no meaningful "new since last
-- run" slice to merge.

{{
  config(
    partition_by={"field": "activity_date", "data_type": "date"},
    cluster_by=["channel_group"]
  )
}}

with sessions as (
    select
        session_date as activity_date,
        channel_group,
        count(distinct session_key) as sessions,
        count(distinct user_pseudo_id) as users,
        sum(purchase_count) as purchases,
        sum(session_revenue_usd) as revenue_usd
    from {{ ref('int_sessions_channel') }}
    group by session_date, channel_group
),

spend as (
    select
        spend_date as activity_date,
        channel_group,
        sum(daily_spend_usd) as daily_spend_usd
    from {{ ref('stg_ad_spend') }}
    group by spend_date, channel_group
),

joined as (
    select
        to_hex(md5(concat(cast(coalesce(s.activity_date, sp.activity_date) as string),
            coalesce(s.channel_group, sp.channel_group)))) as channel_date_key,
        coalesce(s.activity_date, sp.activity_date) as activity_date,
        coalesce(s.channel_group, sp.channel_group) as channel_group,
        coalesce(s.sessions, 0) as sessions,
        coalesce(s.users, 0) as users,
        coalesce(s.purchases, 0) as purchases,
        coalesce(s.revenue_usd, 0.0) as revenue_usd,
        coalesce(sp.daily_spend_usd, 0.0) as spend_usd
    from sessions s
    full outer join spend sp
        on s.activity_date = sp.activity_date
        and s.channel_group = sp.channel_group
)

select
    channel_date_key,
    activity_date,
    channel_group,
    sessions,
    users,
    purchases,
    revenue_usd,
    spend_usd,
    coalesce(safe_divide(spend_usd, nullif(purchases, 0)), 0.0) as cost_per_acquisition,
    coalesce(safe_divide(revenue_usd, nullif(spend_usd, 0)), 0.0) as return_on_ad_spend
from joined