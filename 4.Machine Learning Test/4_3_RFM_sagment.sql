CREATE OR REPLACE TABLE `my-etl-project-452702.etl_dataset.rfm_segments` AS
SELECT 
    customer_id,
    recency,
    frequency,
    monetary,
    CENTROID_ID AS rfm_cluster  
FROM ML.PREDICT(
    MODEL `my-etl-project-452702.etl_dataset.rfm_kmeans`,
    (SELECT * FROM `my-etl-project-452702.etl_dataset.rfm_features`)
);
