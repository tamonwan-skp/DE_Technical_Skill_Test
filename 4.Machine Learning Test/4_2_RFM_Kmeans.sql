CREATE OR REPLACE MODEL `my-etl-project-452702.etl_dataset.rfm_kmeans`
OPTIONS(
    model_type='kmeans',
    num_clusters = 4 
) AS
SELECT 
    recency, frequency, monetary
FROM `my-etl-project-452702.etl_dataset.rfm_features`;
