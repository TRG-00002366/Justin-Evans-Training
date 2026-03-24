+----------------+--------------------+------------------+
| CURRENT_USER() | CURRENT_DATABASE() | CURRENT_SCHEMA() |
|----------------+--------------------+------------------|
| JUSTINE232     | JUSTIN_DEV_DB      | BRONZE           |
+----------------+--------------------+------------------+


+------------+---------------+--------+--------------+
| WAREHOUSE  | DATABASE      | SCHEMA | ROLE         |
|------------+---------------+--------+--------------|
| COMPUTE_WH | JUSTIN_DEV_DB | BRONZE | ACCOUNTADMIN |
+------------+---------------+--------+--------------+

+----------------------------------+
| status                           |
|----------------------------------|
| Statement executed successfully. |
+----------------------------------+
1 Row(s) produced. Time Elapsed: 0.146s
+----------------------------------+
| status                           |
|----------------------------------|
| Statement executed successfully. |
+----------------------------------+
1 Row(s) produced. Time Elapsed: 0.116s
+--------------+----------------+-------------+
| C_MKTSEGMENT | CUSTOMER_COUNT | AVG_BALANCE |
|--------------+----------------+-------------|
| HOUSEHOLD    |          30189 |     4500.76 |
| BUILDING     |          30142 |     4508.28 |
| FURNITURE    |          29968 |     4480.08 |
| MACHINERY    |          29949 |     4488.93 |
| AUTOMOBILE   |          29752 |     4499.42 |
+--------------+----------------+-------------+
5 Row(s) produced. Time Elapsed: 1.659s

+--------------------+------------+-------------+-------------+
| CUSTOMER_NAME      | NATION     | ORDER_COUNT | TOTAL_SPENT |
|--------------------+------------+-------------+-------------|
| Customer#000143500 | IRAN       |          39 |  7012696.48 |
| Customer#000095257 | BRAZIL     |          36 |  6563511.23 |
| Customer#000087115 | KENYA      |          34 |  6457526.26 |
| Customer#000131113 | ETHIOPIA   |          37 |  6311428.86 |
| Customer#000103834 | IRAQ       |          31 |  6306524.23 |
| Customer#000134380 | ALGERIA    |          37 |  6291610.15 |
| Customer#000069682 | MOZAMBIQUE |          39 |  6287149.42 |
| Customer#000102022 | INDONESIA  |          41 |  6273788.41 |
| Customer#000098587 | CHINA      |          37 |  6265089.35 |
| Customer#000085102 | MOROCCO    |          34 |  6135483.63 |
+--------------------+------------+-------------+-------------+
10 Row(s) produced. Time Elapsed: 1.999s


+---------------+-------------------+
| YEAR_OF_ORDER | AVG(O_TOTALPRICE) |
|---------------+-------------------|
|          1992 |   151177.17746095 |
|          1993 |   151516.29234719 |
|          1998 |   151266.78719629 |
|          1995 |   151095.98701697 |
|          1996 |   151379.82889461 |
|          1997 |   150905.17471910 |
|          1994 |   151216.26846123 |
+---------------+-------------------+


Task 5 answers:

Which query took the longest?
SELECT * FROM CUSTOMER LIMIT 5; 
Which query scanned the most data?
SELECT
	EXTRACT(YEAR FROM O_ORDERDATE) AS YEAR_OF_ORDER,
	AVG(O_TOTALPRICE)
FROM ORDERS
GROUP BY YEAR_OF_ORDER;

What optimization might reduce scan time?
removing duplicates if they exist would possibly reduce scan time since there would be less to scan, though that would be done in moving to the silver layer.