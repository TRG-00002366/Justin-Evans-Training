USE DATABASE JUSTIN_DEV_DB;
USE SCHEMA BRONZE;

ALTER WAREHOUSE COMPUTE_WH SET AUTO_SUSPEND = 60;

-- View raw JSON
SELECT raw_data FROM RAW_CUSTOMER_EVENTS LIMIT 5;

-- View formatted JSON (Snowsight will format it nicely)
SELECT raw_data, TYPEOF(raw_data) AS data_type FROM RAW_CUSTOMER_EVENTS;

-- Insert more complex events with arrays
INSERT INTO RAW_CUSTOMER_EVENTS (raw_data)
SELECT PARSE_JSON(column1) FROM VALUES
('{"event_id": "E006", "type": "cart_update", "user_id": "U102", "items": [{"product_id": "P001", "qty": 1}, {"product_id": "P002", "qty": 2}]}'),
('{"event_id": "E007", "type": "cart_update", "user_id": "U103", "items": [{"product_id": "P003", "qty": 1}]}'),
('{"event_id": "E008", "type": "order", "user_id": "U102", "items": [{"product_id": "P001", "qty": 1, "price": 1299.99}, {"product_id": "P002", "qty": 2, "price": 29.99}], "total": 1359.97}');

-- Basic field extraction
SELECT 
    raw_data:event_id::STRING AS event_id,
    raw_data:type::STRING AS event_type,
    raw_data:user_id::STRING AS user_id,
    raw_data:timestamp::TIMESTAMP AS event_time
FROM RAW_CUSTOMER_EVENTS;

-- Extract nested properties
SELECT 
    raw_data:event_id::STRING AS event_id,
    raw_data:type::STRING AS event_type,
    raw_data:properties:page::STRING AS page_viewed,
    raw_data:properties:product_id::STRING AS product_id,
    raw_data:properties:query::STRING AS search_query
FROM RAW_CUSTOMER_EVENTS
WHERE raw_data:properties IS NOT NULL;

SELECT 
    raw_data:event_id::STRING AS event_id,
    raw_data:user_id::STRING AS user_id,
    raw_data:properties:order_id::STRING AS order_id,
    raw_data:properties:total::DECIMAL(12, 2) AS total
FROM RAW_CUSTOMER_EVENTS;


-- View events with items array
SELECT 
    raw_data:event_id::STRING AS event_id,
    raw_data:items AS items_array,
    ARRAY_SIZE(raw_data:items) AS item_count
FROM RAW_CUSTOMER_EVENTS
WHERE raw_data:items IS NOT NULL;

-- FLATTEN the items and calculate the total sum by event_id
SELECT 
    e.raw_data:event_id::STRING AS event_id,
    SUM(f.value:qty::INTEGER * f.value:price::DECIMAL(10,2)) AS total
FROM RAW_CUSTOMER_EVENTS e,
LATERAL FLATTEN(input => e.raw_data:items) f
WHERE e.raw_data:items IS NOT NULL
GROUP BY event_id;




SELECT 
    raw_data:event_id::STRING AS event_id,
    raw_data:type::STRING AS event_type,
    
    -- Conditional extraction based on event type
    CASE 
        WHEN raw_data:type = 'page_view' THEN raw_data:properties:page::STRING
        WHEN raw_data:type = 'search' THEN raw_data:properties:query::STRING
        WHEN raw_data:type = 'purchase' THEN raw_data:properties:order_id::STRING
        ELSE NULL
    END AS context_value,
    
    -- Safe extraction with COALESCE
    COALESCE(
        raw_data:properties:product_id::STRING,
        raw_data:items[0]:product_id::STRING,
        'N/A'
    ) AS primary_product
    
FROM RAW_CUSTOMER_EVENTS;


-- View 1: All events with extracted fields
CREATE OR REPLACE VIEW V_EVENTS_EXTRACTED AS
SELECT 
    raw_data:event_id::STRING AS event_id,
    raw_data:type::STRING AS event_type,
    raw_data:user_id::STRING AS user_id,
    raw_data:timestamp::TIMESTAMP AS event_timestamp,
    raw_data:properties AS properties
FROM RAW_CUSTOMER_EVENTS;

-- View 2: Cart items flattened
CREATE OR REPLACE VIEW V_CART_ITEMS AS
SELECT 
    e.raw_data:event_id::STRING AS event_id,
    e.raw_data:user_id::STRING AS user_id,
    f.index AS item_index,
    f.value:product_id::STRING AS product_id,
    f.value:qty::INTEGER AS quantity,
    f.value:price::DECIMAL(10,2) AS unit_price
FROM RAW_CUSTOMER_EVENTS e,
LATERAL FLATTEN(input => e.raw_data:items) f
WHERE e.raw_data:items IS NOT NULL;

-- Test the views
SELECT * FROM V_EVENTS_EXTRACTED;
SELECT * FROM V_CART_ITEMS;

-- How many events by each type
SELECT 
    raw_data:type::STRING AS event_type,
    COUNT(*) AS total_events
FROM RAW_CUSTOMER_EVENTS
GROUP BY event_type;



--User Activity: Which user has the most events?
SELECT
    raw_data:user_id::STRING AS user_id,
    COUNT(*) AS number_of_events
FROM RAW_CUSTOMER_EVENTS
GROUP BY user_id
ORDER BY number_of_events DESC
LIMIT 1;
    

--Cart Analysis: What is the average number of items per cart?
WITH temp_query AS (
SELECT
    ARRAY_SIZE(raw_data:items) AS item_count
FROM RAW_CUSTOMER_EVENTS
)
SELECT
    AVG(item_count)
FROM temp_query
WHERE item_count > 0;



--Search Analysis: What are the most common search queries?

SELECT
    raw_data:properties:query::STRING AS query_type,
    COUNT(*)
FROM RAW_CUSTOMER_EVENTS
WHERE query_type IS NOT NULL
GROUP BY query_type;




--Purchase Patterns: What is the average order total for purchase events?
SELECT 
    ROUND(AVG(raw_data:properties:total::DECIMAL(12,2)), 2) AS total
FROM RAW_CUSTOMER_EVENTS e,
WHERE raw_data:type::STRING = 'purchase';

