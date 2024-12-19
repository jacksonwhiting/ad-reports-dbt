
  create view "postgres"."test_ad_reporting"."consolidated_ads__dbt_tmp"
    
    
  as (
    WITH facebook_data AS (
    SELECT 
        'facebook' as platform,
        date,
        account_id::text as account_id,
        campaign_id as campaign_id,
        campaign_name,
        clicks,
        impressions,
        spend as cost,
        CASE 
            WHEN impressions > 0 THEN ROUND(CAST((clicks::float / impressions::float) * 100 AS numeric), 2)
            ELSE 0 
        END as ctr,
        CASE 
            WHEN clicks > 0 THEN ROUND(CAST(spend / clicks AS numeric), 2)
            ELSE 0 
        END as cpc,
        CASE 
            WHEN impressions > 0 THEN ROUND(CAST((spend / impressions) * 1000 AS numeric), 2)
            ELSE 0 
        END as cpm
    FROM "postgres"."facebook_ads"."test_facebook_ads"
),

google_data AS (
    SELECT 
        'google' as platform,
        date,
        customer_id::text as account_id,
        id::text as campaign_id,
        name as campaign_name,
        clicks,
        impressions,
        (cost_micros / 1000000.0) as cost,
        CASE 
            WHEN impressions > 0 THEN ROUND(CAST((clicks::float / impressions) * 100 AS numeric), 2)
            ELSE 0 
        END as ctr,
        CASE 
            WHEN clicks > 0 THEN ROUND(CAST((cost_micros / 1000000.0) / clicks AS numeric), 2)
            ELSE 0 
        END as cpc,
        CASE 
            WHEN impressions > 0 THEN ROUND(CAST(((cost_micros / 1000000.0) / impressions) * 1000 AS numeric), 2)
            ELSE 0 
        END as cpm
    FROM "postgres"."test_googleads_schema"."test_googleads_campaings"
)

SELECT 
    platform,
    date,
    account_id,
    campaign_id,
    campaign_name,
    clicks,
    impressions,
    cost,
    ctr,
    cpc,
    cpm
FROM facebook_data

UNION ALL

SELECT 
    platform,
    date,
    account_id,
    campaign_id,
    campaign_name,
    clicks,
    impressions,
    cost,
    ctr,
    cpc,
    cpm
FROM google_data
  );