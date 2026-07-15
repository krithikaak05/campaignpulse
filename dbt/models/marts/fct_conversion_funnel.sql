-- Funnel drop off from item view through purchase, broken out by channel
-- group and day. Rebuilt fully on every run for the same reason as
-- fct_channel_performance, no incremental merge needed given Bronze is
-- fully truncated and reloaded each run.

{{
  config(
    partition_by={"field": "activity_date", "data_type": "date"},
    cluster_by=["channel_group"]
  )
}}

with base as (
    select
        session_date as activity_date,
        channel_group,
        sum(view_item_count) as view_item_events,
        sum(add_to_cart_count) as add_to_cart_events,
        sum(begin_checkout_count) as begin_checkout_events,
        sum(purchase_count) as purchase_events
    from {{ ref('int_sessions_channel') }}
    group by session_date, channel_group
)

select
    to_hex(md5(concat(cast(activity_date as string), channel_group))) as channel_date_key,
    activity_date,
    channel_group,
    view_item_events,
    add_to_cart_events,
    begin_checkout_events,
    purchase_events,
    coalesce(safe_divide(add_to_cart_events, nullif(view_item_events, 0)), 0.0) as view_to_cart_rate,
    coalesce(safe_divide(begin_checkout_events, nullif(add_to_cart_events, 0)), 0.0) as cart_to_checkout_rate,
    coalesce(safe_divide(purchase_events, nullif(begin_checkout_events, 0)), 0.0) as checkout_to_purchase_rate
from base