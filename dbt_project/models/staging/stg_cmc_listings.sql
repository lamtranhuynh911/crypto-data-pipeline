WITH raw_source AS (
    SELECT *
    FROM {{ source('crypto_raw', 'listing') }}
),

extracted AS (
    SELECT
        -- Các cột tĩnh đã được Airbyte tách sẵn, chỉ cần gọi tên và ép kiểu
        CAST(id AS STRING) AS coin_id,
        CAST(name AS STRING) AS name,
        CAST(symbol AS STRING) AS symbol,
        tags AS tags_array, -- Mảng tags để dành làm Bridge Table
        
        -- Cột quote thường được Airbyte lưu dạng JSON hoặc STRUCT. 
        -- Dùng TO_JSON_STRING bọc lại là cách "bao lô" an toàn nhất để trích xuất dữ liệu.
        CAST(JSON_EXTRACT_SCALAR(TO_JSON_STRING(quote), '$.USD.price') AS FLOAT64) AS price,
        CAST(JSON_EXTRACT_SCALAR(TO_JSON_STRING(quote), '$.USD.market_cap') AS FLOAT64) AS market_cap,
        CAST(JSON_EXTRACT_SCALAR(TO_JSON_STRING(quote), '$.USD.volume_24h') AS FLOAT64) AS volume_24h,
        
        -- Thời gian
        CAST(last_updated AS TIMESTAMP) AS last_updated,
        _airbyte_extracted_at AS extracted_at
    FROM raw_source
)

SELECT * FROM extracted