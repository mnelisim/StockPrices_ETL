from pyspark.sql.functions import col, regexp_extract, expr, to_date, lag, avg, stddev
import os
from datetime import datetime
from pyspark.sql.window import Window

class DataTransformer:

    def transform(self, spark, raw_path):
        df = spark.read.parquet(raw_path)

        # Cast numeric columns to correct types
        for c in df.columns:
            if c != "Date":
                df = df.withColumn(c, col(c).cast("double"))

         # Convert date type  column
        df = df.withColumn(
            "Date",
            to_date(col("Date"), "yyyy-MM-dd")
        )
        
        #drop nulls
        df = df.dropna()
        
        #Convert to long format
        df = df.selectExpr(
            "Date",

            """
            stack(25,
                'AAPL','Close', Close_AAPL,
                'AAPL','High', High_AAPL,
                'AAPL','Low', Low_AAPL,
                'AAPL','Open', Open_AAPL,
                'AAPL','Volume', Volume_AAPL,

                'AMZN','Close', Close_AMZN,
                'AMZN','High', High_AMZN,
                'AMZN','Low', Low_AMZN,
                'AMZN','Open', Open_AMZN,
                'AMZN','Volume', Volume_AMZN,

                'GOOGL','Close', Close_GOOGL,
                'GOOGL','High', High_GOOGL,
                'GOOGL','Low', Low_GOOGL,
                'GOOGL','Open', Open_GOOGL,
                'GOOGL','Volume', Volume_GOOGL,

                'MSFT','Close', Close_MSFT,
                'MSFT','High', High_MSFT,
                'MSFT','Low', Low_MSFT,
                'MSFT','Open', Open_MSFT,
                'MSFT','Volume', Volume_MSFT,

                'TSLA','Close', Close_TSLA,
                'TSLA','High', High_TSLA,
                'TSLA','Low', Low_TSLA,
                'TSLA','Open', Open_TSLA,
                'TSLA','Volume', Volume_TSLA
            ) as (Ticker, PriceType, Price)
            """
        )
        #Preventing out of memory(OOM) error
        df = spark.createDataFrame(df.rdd, df.schema).cache()
        print(f"Intermediate count: {df.count()}") 

        df.printSchema()
        df.show(5)

        ###Compute metrics
        #filter close price
        df_close = df.filter(col("PriceType") == "Close")

        #Define windows for close metrics and Sourting the data by date
        window_day = Window.partitionBy("Ticker").orderBy("Date")
        window_7 = Window.partitionBy("Ticker").orderBy("Date").rowsBetween(-6, 0)
        window_30 = Window.partitionBy("Ticker").orderBy("Date").rowsBetween(-29, 0)

        #Calculate return compared to previous trading day(daily return)
        #Calculate average price of the last 7 days
        #Volatility measures price variability
        #Higher volatility = unstable price
        df_close = df_close.withColumn(
        "daily_return",
        (col("Price") - lag("Price", 1).over(window_day)) /
        lag("Price", 1).over(window_day)
        ).withColumn(
            "ma_7",
            avg(col("Price")).over(window_7)  
        ).withColumn(
            "volatility_30",
            stddev(col("Price")).over(window_30)
        )
    
        #Save full dataframe
        timestamp_file = datetime.now().strftime("%Y%m%d_%H%M%S")
        #checking if the folder exist
        os.makedirs("data/processed", exist_ok=True)
        processed_path = f"data/processed/stockprices_spark_{timestamp_file}.parquet"
        metrics_path = f"data/processed/stock_close{timestamp_file}.parquet"
    
        # Save parquet
        df.repartition(4).write.mode("overwrite").parquet(processed_path) 
        df_close.repartition(4).write.mode("overwrite").parquet(metrics_path)
        
        print(f"Processed data saved {df.count()} rows to: {processed_path}")
        print(f"Processed data saved {df.count()} rows to: {metrics_path}")

        return processed_path, metrics_path
