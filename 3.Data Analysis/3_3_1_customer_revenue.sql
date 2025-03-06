CREATE TABLE `my-etl-project-452702.etl_dataset.customer_revenue` AS
SELECT
    customer_id,
    SUM(quantity * price) AS total_revenue
FROM `my-etl-project-452702.etl_dataset.transactions`
GROUP BY customer_id;

WITH ranked_customers AS (
    SELECT 
        customer_id, 
        total_revenue, 
        PERCENT_RANK() OVER (ORDER BY total_revenue DESC) AS rank_percentile
    FROM `my-etl-project-452702.etl_dataset.customer_revenue`
)
SELECT customer_id, total_revenue
FROM ranked_customers
WHERE rank_percentile <= 0.10  -- Top 10%
ORDER BY total_revenue DESC;
