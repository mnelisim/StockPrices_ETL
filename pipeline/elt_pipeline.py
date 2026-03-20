from ingestion.data_fetching import DataExtractor
from processing.cleaning import DataTransformer
from storage.db_connect import Connection
from storage.load_data import Dataload
from processing.data_validation import DataValidator
from utils.metrics import PipelineMetrics
from config import table_name,DB_NAME,DB_HOST,DB_USER,DB_PASSWORD,DB_PORT
from datetime import datetime
import pandas as pd
from utils.logger import log_message
from pyspark.sql.functions import col

class ETLpipeline:

    def __init__(self, spark):
        self.spark = spark
        self.extractor = DataExtractor()
        self.transformer = DataTransformer()
        self.connection = Connection()
        self.loader = Dataload()

   #extracting
    def extract(self):

        #current timestamp
        timestamp_file = datetime.now().strftime("%Y%m%d_%H%M%S")

        print("Extract stage running")

        validator = DataValidator()
        #extraction
        parquet_file = self.extractor.extract_data()
        
        #load raw data for validation
        df = pd.read_parquet(parquet_file)
        
        #validation
        validator.validate_stock_data(df, run_id=timestamp_file)

        row_count = len(df)
        log_message(f"Rows extracted: {row_count}", run_id=timestamp_file)
        return parquet_file, row_count


    #transform
    def transform(self, raw_path):

        #run_ts = datetime.now().strftime('%Y%m%d_%H%M%S')

        processed_path, metrics_path = self.transformer.transform(
            self.spark,
            raw_path
        )

        return processed_path, metrics_path


    #load
    def load(self, processed_path, metrics_path, table_name):

        print("Load stage running")

        #Read spark_df from parquet
        spark_df = self.spark.read.parquet(processed_path)
        metrics_df = self.spark.read.parquet(metrics_path)

        #df_close = self.spark.read.parquet(close_path)

        conn, cur = self.connection.create_connection()

        self.loader.create_table_if_not_exists(
            cur,
            conn,
            table_name
        )

        self.loader.store_data_in_db(
            cur,
            conn,
            spark_df,
            table_name
        )
  
        self.loader.create_table_stock_metrics(cur, conn)
        self.loader.store_stock_metrics_data(cur, conn, metrics_df)

        # 1. Clean the metrics_df by dropping Price if it exists there
        m_df = metrics_df.drop("Price")
        
        # 2. Select only necessary columns from spark_df
        s_df = spark_df.select("Date", "Ticker", "Price")

        # 3. Perform the join
        # Joining on the list ["Date", "Ticker"] merges them into single columns
        fact_df = m_df.join(s_df, on=["Date", "Ticker"], how="inner")

        # 4. Final selection to ensure absolute clarity for the loader
        final_fact_df = fact_df.select(
            col("Date").alias("date"),
            col("Ticker").alias("ticker"),
            col("Price").alias("price"),
            "daily_return",
            "ma_7",
            "volatility_30"
        )


        self.loader.create_star_schema(cur, conn)
        self.loader.load_dim_stock(cur, conn, spark_df)
        self.loader.load_dim_date(cur, conn, spark_df)
        self.loader.load_fact_table(cur, conn, final_fact_df)


        if cur:
            cur.close()

        if conn:
            conn.close()