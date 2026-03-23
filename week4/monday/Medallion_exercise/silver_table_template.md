# Silver Table Design Template

## Table: [Web_Events]

### Source Information
| Attribute | Value |
|-----------|-------|
| Source Bronze Table | WEB_EVENTS |
| Primary Key | event_id |
| Deduplication Strategy |  |

### Column Definitions

| Column Name | Data Type | Source Expression | Transformation |
|-------------|-----------|-------------------|----------------|
| event_id | STRING | event_id::STRING | |
| event_type | STRING | event_type::STRING | |
| timestamp | TIMESTAMP | timestamp::STRING | |
| user_id | STRING | user_id::STRING | |
| session_id | STRING | session_id::STRING | |
| page_url | STRING | page_url::STRING | |
| device | STRING | device::STRING | |
| referrer | STRING | referrer::STRING | |
| product_id | STRING | product_id::STRING | |

### Transformations Applied

1. **Data Typing:**
   - 

2. **Standardization:**
   - 

3. **Null Handling:**
   - 

4. **Deduplication:**
   - 

### DDL Statement

```sql
CREATE OR REPLACE TABLE SILVER.[TABLE_NAME] (
    -- Primary key
    
    -- Business columns
    
    -- Metadata
    processed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    PRIMARY KEY ()
)
COMMENT = '[Description]';
```

### Sample Transformation Query

```sql
SELECT
    -- Extract and type cast from Bronze
FROM BRONZE.[SOURCE_TABLE]
WHERE [deduplication logic]
```
