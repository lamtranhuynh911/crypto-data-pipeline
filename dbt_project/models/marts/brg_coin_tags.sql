WITH staged_data AS (
    SELECT 
        coin_id, 
        tags_array
    FROM {{ ref('stg_cmc_listings') }}
    -- Dùng ref() thay vì source() để dbt biết bảng này phụ thuộc vào bảng staging kia
    WHERE tags_array IS NOT NULL
),

unnested_tags AS (
    SELECT
        coin_id,
        -- Dùng hàm JSON để xử lý mảng an toàn nhất trên BigQuery
        CAST(tag AS STRING) AS tag_name 
    FROM staged_data,
    UNNEST(JSON_EXTRACT_ARRAY(TO_JSON_STRING(tags_array))) AS tag
)

-- Dùng DISTINCT để loại bỏ các tag bị trùng lặp (nếu có)
SELECT DISTINCT 
    coin_id, 
    REPLACE(tag_name, '"', '') AS tag_name -- Cắt bỏ dấu ngoặc kép thừa của JSON
FROM unnested_tags