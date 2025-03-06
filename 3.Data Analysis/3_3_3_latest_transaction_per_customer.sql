CREATE OR REPLACE TABLE `my-etl-project-452702.etl_dataset.latest_transaction_per_customer` AS
SELECT
    t.customer_id,
    MAX(t.transaction_date) AS latest_transaction_date,
    DATE(MAX(t.transaction_date)) AS latest_transaction_date_only,
    DATE_DIFF(CURRENT_DATE(), DATE(MAX(t.transaction_date)), DAY) AS days_ago
FROM `my-etl-project-452702.etl_dataset.transactions` t
GROUP BY t.customer_id
ORDER BY latest_transaction_date DESC;
