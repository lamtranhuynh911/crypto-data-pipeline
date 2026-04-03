{{ config(
    materialized='table',
    partition_by={
      "field": "hour_bucket",
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by=["symbol"]
) }}

WITH extracted_raw_data AS (
    -- Bước 1: Móc dữ liệu thô và bóc tách các trường từ JSON
    SELECT 
        id AS coin_id,
        name,
        symbol,
        slug,
        CAST(last_updated AS TIMESTAMP) AS updated_at,
        
        -- Các thông số cung tiền (Supply) & Xếp hạng
        CAST(cmc_rank AS INT64) AS cmc_rank,
        CAST(max_supply AS FLOAT64) AS max_supply,
        CAST(total_supply AS FLOAT64) AS total_supply,
        CAST(circulating_supply AS FLOAT64) AS circulating_supply,
        
        -- Dùng JSON_VALUE để bóc tách dữ liệu từ cột 'quote' (Mặc định CoinMarketCap trả về USD)
        CAST(JSON_VALUE(quote, '$.USD.price') AS FLOAT64) AS price,
        CAST(JSON_VALUE(quote, '$.USD.volume_24h') AS FLOAT64) AS volume_24h,
        CAST(JSON_VALUE(quote, '$.USD.market_cap') AS FLOAT64) AS market_cap,
        CAST(JSON_VALUE(quote, '$.USD.percent_change_1h') AS FLOAT64) AS percent_change_1h,
        CAST(JSON_VALUE(quote, '$.USD.percent_change_24h') AS FLOAT64) AS percent_change_24h

    FROM {{ source('crypto_raw', 'listing') }}
    WHERE last_updated IS NOT NULL
),

hourly_resampled AS (
    -- Bước 2: Gom nhóm theo từng block 1 giờ (Time Bucketing)
    SELECT 
        coin_id,
        name,
        symbol,
        slug,
        TIMESTAMP_TRUNC(updated_at, HOUR) AS hour_bucket,

        -- 1. XỬ LÝ NẾN GIÁ (PRICE OHLC)
        ARRAY_AGG(price ORDER BY updated_at ASC LIMIT 1)[OFFSET(0)] AS open_price,
        MAX(price) AS high_price,
        MIN(price) AS low_price,
        ARRAY_AGG(price ORDER BY updated_at DESC LIMIT 1)[OFFSET(0)] AS close_price,

        -- 2. XỬ LÝ CÁC CHỈ SỐ KHÁC (Chỉ lấy giá trị chốt cuối cùng của giờ đó)
        ARRAY_AGG(volume_24h ORDER BY updated_at DESC LIMIT 1)[OFFSET(0)] AS close_volume_24h,
        ARRAY_AGG(market_cap ORDER BY updated_at DESC LIMIT 1)[OFFSET(0)] AS close_market_cap,
        ARRAY_AGG(circulating_supply ORDER BY updated_at DESC LIMIT 1)[OFFSET(0)] AS close_circulating_supply,
        ARRAY_AGG(total_supply ORDER BY updated_at DESC LIMIT 1)[OFFSET(0)] AS close_total_supply,
        ARRAY_AGG(cmc_rank ORDER BY updated_at DESC LIMIT 1)[OFFSET(0)] AS close_cmc_rank,

        -- Biến động phần trăm (lấy mức cập nhật mới nhất trong giờ)
        ARRAY_AGG(percent_change_1h ORDER BY updated_at DESC LIMIT 1)[OFFSET(0)] AS close_percent_change_1h,
        ARRAY_AGG(percent_change_24h ORDER BY updated_at DESC LIMIT 1)[OFFSET(0)] AS close_percent_change_24h,

        -- 3. METADATA ĐỂ KIỂM SOÁT DATA QUALITY
        COUNT(*) AS sync_count,
        MIN(updated_at) AS first_sync_time,
        MAX(updated_at) AS last_sync_time

    FROM extracted_raw_data
    GROUP BY 
        coin_id,
        name,
        symbol,
        slug,
        TIMESTAMP_TRUNC(updated_at, HOUR)
)

-- Bước 3: Đẩy ra bảng cuối
SELECT * FROM hourly_resampled