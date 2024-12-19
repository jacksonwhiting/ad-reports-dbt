{{ config(
    materialized='incremental',
    post_hook=[
        """
        INSERT INTO public.metrics (
            data_date,
            clicks,
            impr,
            cost,
            cpm,
            ctr,
            cpc,
            month,
            year,
            paid_organic,
            traffic_source,
            data_source_type,
            platform,
            src_account_id,
            src_campaign_id,
            src_campaign_name
        )
        SELECT 
            data_date,
            clicks,
            impr,
            cost,
            cpm,
            ctr,
            cpc,
            month,
            year,
            paid_organic,
            traffic_source,
            data_source_type,
            platform,
            src_account_id,
            src_campaign_id,
            src_campaign_name
        FROM {{ ref('metrics_merge') }}
        WHERE NOT EXISTS (
            SELECT 1 
            FROM public.metrics m 
            WHERE m.data_date = metrics_merge.data_date 
            AND m.src_campaign_id = metrics_merge.src_campaign_id 
            AND m.data_source_type = metrics_merge.data_source_type
        )
        """,
        """
        UPDATE public.metrics m
        SET 
            clicks = s.clicks,
            impr = s.impr,
            cost = s.cost,
            cpm = s.cpm,
            ctr = s.ctr,
            cpc = s.cpc,
            month = s.month,
            year = s.year,
            paid_organic = s.paid_organic,
            traffic_source = s.traffic_source,
            platform = s.platform,
            src_account_id = s.src_account_id,
            src_campaign_name = s.src_campaign_name,
            updated_at = CURRENT_TIMESTAMP
        FROM {{ ref('metrics_merge') }} s
        WHERE m.data_date = s.data_date 
        AND m.src_campaign_id = s.src_campaign_id 
        AND m.data_source_type = s.data_source_type
        """
    ])
}}

SELECT 1 