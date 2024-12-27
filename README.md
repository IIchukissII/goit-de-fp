### Conclusion

This project demonstrates the development of both real-time streaming and batch processing pipelines for handling and analyzing sports data. By implementing the following components, the project highlights essential skills in modern data engineering:

1. **Streaming Pipeline**:  
   A real-time data pipeline built with Apache Spark and Kafka to process athlete data, integrate it with biological metrics, and produce aggregated statistics for model training and real-time analytics.

2. **Batch Data Lake**:  
   A batch processing architecture with a multi-hop design (bronze, silver, gold) that extracts, transforms, and loads data from raw FTP sources to structured and analytical-ready datasets using Apache Spark.

3. **Automation**:  
   Workflow orchestration was initially planned with Airflow. However, due to integration challenges between Airflow and Spark, the project uses **Prefect** for orchestration. Prefect provided a simpler and more robust approach to handling Spark jobs, enhancing the pipeline's reliability and flexibility.

4. **Implementation**:  
   The project utilizes Prefect's flow and task abstractions, combined with PySpark for data transformations and analytics, enabling efficient processing and management of large-scale datasets.

By combining streaming and batch approaches, this project equips a betting company with efficient tools to generate features for machine learning models and adjust betting coefficients dynamically. It also establishes a robust data lake for historical analysis and strategic decision-making.
