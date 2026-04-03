WITH staged_data AS (
    SELECT 
        coin_id,
        name,
        symbol,
        last_updated
    FROM {{ ref('stg_cmc_listings') }}
),

-- Đề phòng Airbyte chạy nhiều lần sinh ra dữ liệu trùng, 
-- ta dùng ROW_NUMBER() để chỉ lấy thông tin mới nhất của mỗi đồng coin
deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER(PARTITION BY coin_id ORDER BY last_updated DESC) as rn
    FROM staged_data
)

SELECT 
    coin_id,
    name,
    symbol
FROM deduplicated
WHERE rn = 1