{{ config(
    materialized='table'
) }}

SELECT 
    d.coin_id,
    d.name,
    d.symbol,
    b.tag_name AS ecosystem,
    f.price,
    f.market_cap,
    f.volume_24h,
    f.last_updated
FROM {{ ref('dim_coin') }} d
JOIN {{ ref('brg_coin_tags') }} b 
    ON d.coin_id = b.coin_id
JOIN {{ ref('stg_cmc_listings') }} f 
    ON d.coin_id = f.coin_id