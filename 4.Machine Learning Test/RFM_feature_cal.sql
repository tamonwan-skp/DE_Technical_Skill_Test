CREATE OR REPLACE TABLE `my-etl-project-452702.etl_dataset.rfm_features` AS
WITH transactions_agg AS (
    SELECT 
        customer_id,
        MAX(DATE(transaction_date)) AS last_purchase_date, 
        COUNT(DISTINCT DATE(transaction_date)) AS frequency, 
        SUM(quantity * price) AS monetary 
    FROM `my-etl-project-452702.etl_dataset.transactions`
    GROUP BY customer_id
),
rfm AS (
    SELECT 
        customer_id,
        DATE_DIFF(CURRENT_DATE(), last_purchase_date, DAY) AS recency,  
        frequency,
        monetary
    FROM transactions_agg
)
SELECT * FROM rfm;
