-- Flattens the Universal Analytics session export into a clean, typed
-- session level table. Each Bronze row is already one session, so unlike
-- the earlier GA4 event export, no separate rollup step is needed, the
-- ecommerce funnel counts are pulled directly out of the nested `hits`
-- array using correlated subqueries. A small number of sessions in this
-- public dataset appear twice across adjacent daily shards when a visit
-- spans midnight, so a qualify clause keeps just one row per session.

{{ config(materialized="table", partition_by={"field": "session_date", "data_type": "date"}, cluster_by=["device_category"]) }}

select
    to_hex(md5(concat(fullVisitorId, cast(visitId as string)))) as session_key,
    fullVisitorId as user_pseudo_id,
    event_date as session_date,
    timestamp_seconds(visitStartTime) as session_start,
    channelGrouping,
    device.deviceCategory as device_category,
    device.operatingSystem as operating_system,
    trafficSource.source as traffic_source_source,
    trafficSource.medium as traffic_source_medium,
    trafficSource.campaign as traffic_source_campaign,
    coalesce(totals.transactionRevenue, 0) / 1000000.0 as session_revenue_usd,
    coalesce(totals.transactions, 0) as purchase_count,
    (
        select count(*) from unnest(hits) h
        where h.eCommerceAction.action_type = '2'
    ) as view_item_count,
    (
        select count(*) from unnest(hits) h
        where h.eCommerceAction.action_type = '3'
    ) as add_to_cart_count,
    (
        select count(*) from unnest(hits) h
        where h.eCommerceAction.action_type in ('5', '8')
    ) as begin_checkout_count
from {{ source('bronze', 'ga_sessions_raw') }}
qualify row_number() over (
    partition by fullVisitorId, visitId
    order by visitStartTime
) = 1