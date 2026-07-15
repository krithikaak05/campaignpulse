-- Normalizes the native channelGrouping field from Universal Analytics
-- into the channel_group categories used throughout the marts. GA's own
-- channelGrouping is already a well maintained marketing taxonomy,
-- Organic Search, Direct, Referral, Paid Search, Social, Display,
-- Affiliates, so this is mostly a light pass through rather than the
-- manual source and medium pattern matching the earlier GA4 version
-- needed.

{{ config(materialized="view") }}

select
    session_key,
    user_pseudo_id,
    session_date,
    session_start,
    device_category,
    lower(concat(
        coalesce(traffic_source_source, 'direct'),
        ' / ',
        coalesce(traffic_source_medium, 'none')
    )) as channel,
    case
        when channelGrouping in (
            'Organic Search', 'Direct', 'Referral',
            'Paid Search', 'Social', 'Display', 'Affiliates'
        ) then channelGrouping
        else 'Other'
    end as channel_group,
    traffic_source_campaign,
    view_item_count,
    add_to_cart_count,
    begin_checkout_count,
    purchase_count,
    session_revenue_usd
from {{ ref('stg_ga_sessions') }}