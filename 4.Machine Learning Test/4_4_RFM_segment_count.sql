SELECT 
    rfm_cluster,
    AVG(recency) AS avg_recency,
    AVG(frequency) AS avg_frequency,
    AVG(monetary) AS avg_monetary,
    COUNT(*) AS customer_count
FROM `my-etl-project-452702.etl_dataset.rfm_segments`
GROUP BY rfm_cluster
ORDER BY rfm_cluster;
