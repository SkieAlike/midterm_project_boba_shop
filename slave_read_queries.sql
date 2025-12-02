-- TOTAL PAGEVIEWS
SELECT COUNT(pageview_id)
FROM pageviews
where pv_timestamp between '2020-01-01' AND '2025-04-01';
--pgbenchmark_response_time (without time filter):
-- {'runs': 1000, 'min_time': '0.056563', 'max_time': '0.11251', 'avg_time': '0.062877', 'median_time': '0.060717'}


-- TOTAL USERS, SESSIONS,
SELECT COUNT(DISTINCT USER_ID),
       COUNT(session_id)
FROM sessions
where session_start between '2020-01-01' AND '2025-04-01';
--pgbenchmark_response_time (without time filter):
-- {'runs': 1000, 'min_time': '0.056563', 'max_time': '0.11251', 'avg_time': '0.062877', 'median_time': '0.060717'}


-- Total Sessions by Channel
SELECT CHANNEL,
       count(session_id)
FROM sessions
where session_start between '2020-01-01' AND '2025-04-01'
GROUP BY channel;
--pgbenchmark_response_time (without time filter):
--{'runs': 100, 'min_time': '0.053067', 'max_time': '0.077558', 'avg_time': '0.058502', 'median_time': '0.056941'}


-- Total Sessions by Device
SELECT device,
       count(session_id)
FROM sessions
where session_start between '2020-01-01' AND '2025-04-01'
GROUP BY device;
--pgbenchmark_response_time (without time filter):
-- {'runs': 100, 'min_time': '0.052438', 'max_time': '0.077659', 'avg_time': '0.058549', 'median_time': '0.05675'}

-- Total pageviws by pagetype
SELECT pagetype,
       count(pageview_id)
FROM pageviews
GROUP BY pagetype;
--pgbenchmark_response_time:
-- {'runs': 1000, 'min_time': '0.120322', 'max_time': '0.166511', 'avg_time': '0.131289', 'median_time': '0.129399'}



-- Total transactions
SELECT COUNT(DISTINCT user_id),
       COUNT(DISTINCT session_id),
       count(transaction_id)
FROM transactions
WHERE t_timestamp between '2020-01-01' AND '2025-04-01';
--pgbenchmark_response_time (without the filter):
--{'runs': 1000, 'min_time': '0.167601', 'max_time': '0.24002', 'avg_time': '0.184924', 'median_time': '0.182418'}


-- Transaction Info - Get information about specific transaction
WITH purchase_event AS (SELECT *
                        FROM events
                        WHERE event_type_id = 5),
    transactions_filtered AS (SELECT session_id,
             t_timestamp
      FROM transactions
     WHERE transaction_id = 201901062028390000),
    get_event_id AS (SELECT event_id
                     FROM transactions_filtered AS TF
                     LEFT JOIN purchase_event AS PE on tf.t_timestamp = pe.event_timestamp
                                      and  tf.session_id = pe.session_id)
SELECT SKU,
       QUANTITY,
       price,
       currency
FROM event_attributes
WHERE event_type_id = 5 and event_id IN (SELECT event_id FROM get_event_id);
--pgbenchmark_response_time (with transaction filter):
--{'runs': 100, 'min_time': '0.003753', 'max_time': '0.023152', 'avg_time': '0.005586', 'median_time': '0.005308'}


-- Transactions Average Order Value
WITH purchase_event AS (SELECT SUM(quantity) AS NUMBER_OF_ITEMS,
                                SUM(price) AS ORDER_VALUE
                         FROM event_attributes
                         WHERE event_type_id = 5
                         GROUP BY event_id)
SELECT avg(NUMBER_OF_ITEMS) AS AVG_N_ITEMS_ORDER,
       AVG(ORDER_VALUE) AS AVG_ORDER_VALUE
FROM purchase_event;
--pgbenchmark_response_time:
--{'runs': 100, 'min_time': '0.260564', 'max_time': '0.358807', 'avg_time': '0.278448', 'median_time': '0.275797'}

-- Avg Cart Value
WITH view_cart_event AS (SELECT SUM(quantity) AS NUMBER_OF_ITEMS,
                                SUM(price) AS CART_VALUE
                         FROM event_attributes
                         WHERE event_type_id = 4
                         GROUP BY event_id)
SELECT avg(NUMBER_OF_ITEMS) AS AVG_N_CART_ITEMS,
       AVG(CART_VALUE) AS AVG_CART_VALUE
FROM view_cart_event
--pgbenchmark_response_time:
--{'runs': 100, 'min_time': '0.524041', 'max_time': '0.601521', 'avg_time': '0.54412', 'median_time': '0.540054'}


-- Count Rows
SELECT 'Sessions' AS table_name,
    COUNT(*) AS row_count
FROM sessions
UNION ALL
SELECT 'Pageviws' AS table_name,
    COUNT(*) AS row_count
FROM pageviews
UNION ALL
SELECT 'Transactions' AS table_name,
    COUNT(*) AS row_count
FROM transactions
UNION ALL
SELECT 'Events' AS table_name,
    COUNT(*) AS row_count
FROM events
UNION ALL
SELECT 'Event Attributes' AS table_name,
    COUNT(*) AS row_count
FROM event_attributes
UNION ALL
SELECT 'Event Name Mapping' AS table_name,
    COUNT(*) AS row_count
FROM event_name_mapping
