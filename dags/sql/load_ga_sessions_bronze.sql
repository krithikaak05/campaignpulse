-- Extracts session level data from the Google Merchandise Store sample
-- dataset, a Universal Analytics export spanning a full year,
-- 2016-08-01 through 2017-08-01. This replaces the earlier GA4 event
-- export source, which only covered a 92 day historical window and
-- could not support month over month or seasonal comparisons.
--
-- Each row is already one session, with ecommerce hit detail nested
-- inside a repeated `hits` field, which is flattened downstream in the
-- staging layer rather than here, keeping this extraction query simple
-- and cheap to run.

SELECT
  PARSE_DATE('%Y%m%d', date) AS event_date,
  date,
  fullVisitorId,
  visitId,
  visitNumber,
  visitStartTime,
  channelGrouping,
  totals,
  trafficSource,
  device,
  hits
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
  _TABLE_SUFFIX BETWEEN '20160801' AND '20170801'