What databases exist by default?
DEV_DB
SNOWFLAKE
SNOWFLAKE_LEARNING_DB
SNOWFLAKE_SAMPLE_DATA
USER$JUSTINE232


What is the name of the default virtual warehouse?

I couldn't find a concrete answer, but it seems like it is SNOWFLAKE_LEARNING_WH


What role are you currently using?
ACCOUNTADMIN




Cloud Services Layer - Basically the brain of snowflake. stores important metadata, table definitions, privileges, does query parsing/ optimization, etc.

Query Processing Layer (Virtual Warehouses) - Where you actually have the warehouse and can interact with said warehouse. This layer allows the creation of new schemas, databases, etc.. This layer can also be started and stopped on demand

Database Storage Layer - This is the layer where we store any and all data. It is noteworthy that at this level the actual cloud storage is typically being passed off onto cloud providers like S3, Azure Blob, etc.

| Component | What You Observed | Purpose |
|-----------|-------------------|---------|
| Virtual Warehouse | I selected a warehouse environment and defined a database and some schemas to be used in the warehouse | Warehouse can be kinda seen as an environment where databases and schema are ran |
| Database | created databases here | allows us to run queries |
| Schema | created several schemas | This allows us to separate bronze, silver, and gold quality data into different schemas |
| Table | utilized certain tables | tables are used to store data which can be queried for analytics |
| Role | user/role was selected | this is used to allow permissions to be given to certain users |