SELECT 
    p.category,
    SUM(t.quantity * t.price) AS total_revenue
FROM `my-etl-project-452702.etl_dataset.transactions` t
JOIN `my-etl-project-452702.etl_dataset.products` p
ON t.product_id = p.product_id
WHERE EXTRACT(YEAR FROM t.transaction_date) = 2024
GROUP BY p.category
ORDER BY total_revenue DESC
LIMIT 1;
