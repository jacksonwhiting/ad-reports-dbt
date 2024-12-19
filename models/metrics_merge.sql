{{ config(
    materialized='incremental',
    unique_key=['data_date', 'src_campaign_id', 'data_source_type']
) }}

-- Merge from Google Ads
SELECT 
    date as data_date,
    clicks,
    impressions as impr,
    (cost_micros / 1000000.0) as cost,
    CASE 
        WHEN impressions > 0 THEN ROUND(CAST(((cost_micros / 1000000.0) / impressions) * 1000 AS numeric), 2)
        ELSE 0 
    END as cpm,
    CASE 
        WHEN impressions > 0 THEN ROUND(CAST((clicks::float / impressions) * 100 AS numeric), 2)
        ELSE 0 
    END as ctr,
    CASE 
        WHEN clicks > 0 THEN ROUND(CAST((cost_micros / 1000000.0) / clicks AS numeric), 2)
        ELSE 0 
    END as cpc,
    EXTRACT(MONTH FROM date)::integer as month,
    EXTRACT(YEAR FROM date)::integer as year,
    'paid' as paid_organic,
    'Google Ads' as traffic_source,
    'Google Ads' as data_source_type,
    'Google' as platform,
    customer_id::text as src_account_id,
    id::text as src_campaign_id,
    name as src_campaign_name
FROM {{ source('google_ads', 'test_googleads_campaings') }}

UNION ALL

SELECT 
    date as data_date,
    clicks,
    impressions as impr,
    spend as cost,
    CASE 
        WHEN impressions > 0 THEN ROUND(CAST((spend / impressions) * 1000 AS numeric), 2)
        ELSE 0 
    END as cpm,
    CASE 
        WHEN impressions > 0 THEN ROUND(CAST((clicks::float / impressions) * 100 AS numeric), 2)
        ELSE 0 
    END as ctr,
    CASE 
        WHEN clicks > 0 THEN ROUND(CAST(spend / clicks AS numeric), 2)
        ELSE 0 
    END as cpc,
    EXTRACT(MONTH FROM date)::integer as month,
    EXTRACT(YEAR FROM date)::integer as year,
    'paid' as paid_organic,
    'Facebook Ads' as traffic_source,
    'Facebook Ads' as data_source_type,
    'Facebook' as platform,
    account_id::text as src_account_id,
    campaign_id::text as src_campaign_id,
    campaign_name as src_campaign_name
FROM {{ source('facebook_ads', 'test_facebook_ads') }}