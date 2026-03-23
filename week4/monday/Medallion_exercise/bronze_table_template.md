# Bronze Table Design Template

## Table: [Web Events]

### Source Information
| Attribute | Value |
|-----------|-------|
| Source System | Web Events |
| Data Format | JSON |
| Update Frequency | Continuous |
| Load Method | Append |

### Column Definitions

| Column Name | Data Type | Description | Sample Value |
|-------------|-----------|-------------|--------------|
| event_id | STRING | unique identifier | "e-abc123 |
| ingestion_ts | TIMESTAMP_NTZ | When data was loaded | 2024-01-15 10:30:00 |
| source_file | STRING | Source file name | web_events_20240115.csv |
| raw_data | VARIANT | Raw JSON payload | {...} |
| | | | |
| | | | |

### DDL Statement

```sql
CREATE OR REPLACE TABLE BRONZE.WEB_EVENTS (
    ingestion_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_file STRING,
    raw_data VARIANT
)
COMMENT = '[This table stores raw data, which has information regarding web events and their attributes]';
```

### Notes
- 
- 



# Bronze Table Design Template

## Table: [Orders]

### Source Information
| Attribute | Value |
|-----------|-------|
| Source System | Orders |
| Data Format | CSV |
| Update Frequency | Continuous |
| Load Method | Append |

### Column Definitions

| Column Name | Data Type | Description | Sample Value |
|-------------|-----------|-------------|--------------|
| ingestion_ts | TIMESTAMP_NTZ | When data was loaded | 2024-01-15 10:30:00 |
| source_file | STRING | Source file name | orders_20240115.csv |
| raw_data | VARIANT | Raw JSON payload | {...} |
| | | | |
| | | | |

### DDL Statement

```sql
CREATE OR REPLACE TABLE BRONZE.ORDERS (
    ingestion_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_file STRING,
    raw_data VARIANT
)
COMMENT = '[This table stores raw data, which has information regarding orders and their attributes]';
```

### Notes
- 
- 



# Bronze Table Design Template

## Table: [Products]

### Source Information
| Attribute | Value |
|-----------|-------|
| Source System | Products |
| Data Format | JSON |
| Update Frequency | Continuous |
| Load Method | Append |

### Column Definitions

| Column Name | Data Type | Description | Sample Value |
|-------------|-----------|-------------|--------------|
| ingestion_ts | TIMESTAMP_NTZ | When data was loaded | 2024-01-15 10:30:00 |
| source_file | STRING | Source file name | products_20240115.json |
| raw_data | VARIANT | Raw JSON payload | {...} |
| | | | |
| | | | |

### DDL Statement

```sql
CREATE OR REPLACE TABLE BRONZE.PRODUCTS (
    ingestion_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_file STRING,
    raw_data VARIANT
)
COMMENT = '[This table stores raw data, which has information regarding products and their attributes]';
```

### Notes
- 
- 
