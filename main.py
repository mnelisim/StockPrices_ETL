from ingestion.data_fetching import DataExtractor
from pipeline.elt_pipeline import ETLpipeline
from pyspark.sql import SparkSession

spark = SparkSession.builder \
        .appName("StockETL") \
        .master("local[*]") \
        .getOrCreate()

if __name__ == "__main__":
   pipeline = ETLpipeline(spark)
   pipeline.run_pipeline()