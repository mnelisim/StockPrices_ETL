from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
from config import table_name
from utils.metrics import PipelineMetrics
from storage.db_connect import Connection

# Project path
sys.path.append("/home/mnelisi/Desktop/DataEngineering/StockPrices_ETL")

from pipeline.elt_pipeline import ETLpipeline
from pyspark.sql import SparkSession


#Extracting data task
def extract_task(**context):
    
    start_time = datetime.now()

    # Create a Spark session
    spark = SparkSession.builder \
    .appName("StockETL") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()
    
    # Initialize the ETL pipeline 
    # Create an instance of ETLpipeline class with the Spark session
    pipeline = ETLpipeline(spark)

    #connection DB
    connection = Connection()
    metrics = PipelineMetrics(connection)
    metrics.create_monitoring_table()

    try:
        #Run extract 
        raw_path, row_count = pipeline.extract()

        metrics.log_metric(
            run_id=context["run_id"],
            task_name="extract",
            rows_processed=row_count,
            status="SUCCESS",
            start_time=start_time
        )

        return raw_path

    except Exception as e:

        metrics.log_metric(
            run_id=context["run_id"],
            task_name="extract",
            rows_processed=0,
            status="FAILED",
            start_time=start_time,
            error_message=str(e)
        )

        raise

    finally:
        spark.stop()

# Transform data task
def transform_task(**context):

    start_time = datetime.now()

    #creates a Spark session with memory configs to avoid OutOfMemory errors
    spark = SparkSession.builder \
    .appName("StockETL") \
    .master("local[2]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.driver.maxResultSize", "2g") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "2g") \
    .getOrCreate()

    # Initialize spark session
    # create and instance of ETLpipeline class with the spark session
    pipeline = ETLpipeline(spark)
    #connection DB
    connection = Connection()
    metrics = PipelineMetrics(connection)
    metrics.create_monitoring_table()

    # Get the raw data path from previous Airflow task
    raw_path = context["ti"].xcom_pull(
        task_ids="extract_task"           
    )

    try:
        # Run the transformation
        processed_path, metrics_path = pipeline.transform(raw_path)

        # 3. Get actual row count for metrics
        df_count = spark.read.parquet(metrics_path).count()

        metrics.log_metric(
            run_id=context["run_id"],
            task_name="transform",
            rows_processed=df_count,
            status="SUCCESS",
            start_time=start_time
        )

        # Return results to Airflow
        # Return a dictionary containing paths to transformed data and metrics so downstream tasks (like load_task) can use them
        return {"processed": processed_path, "metrics": metrics_path}

    except Exception as e:

        metrics.log_metric(
            run_id=context["run_id"],
            task_name="transform",
            rows_processed=0,
            status="FAILED",
            start_time=start_time,
            error_message=str(e)
        )
        raise
    finally:
        spark.stop()
   

#Load data task
def load_task(table_name, **context):
    
    start_time = datetime.now()

    # Pull the processed path from transform task
    paths = context['ti'].xcom_pull(task_ids='transform_task')
    processed_path = paths["processed"]
    metrics_path = paths["metrics"]

    if not processed_path:
        raise ValueError("No processed_path received from transform_task")
    if not metrics_path:
        raise ValueError("No metrics_path received from transform_task")

    #Create spark session
    spark = SparkSession.builder \
    .appName("StockETL") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

    #Initialize ETLpipeline
    #Creating instance of ETLpipeline class with the spark session
    pipeline = ETLpipeline(spark)
     #connection DB
    connection = Connection()
    metrics = PipelineMetrics(connection)
    metrics.create_monitoring_table()

    try:
        #Loading data to database
        pipeline.load(processed_path, metrics_path, table_name)

        # 3. FIX: Get actual row count from Spark, not the length of the path string
        # We read the metrics file because it represents the final 'fact' data
        final_row_count = spark.read.parquet(metrics_path).count()
        
        metrics.log_metric(
            run_id=context["run_id"],
            task_name="load",
            rows_processed=final_row_count,
            status="SUCCESS",
            start_time=start_time
        )

        return {
        "status": "success",
        "rows": final_row_count
        }
    
    except Exception as e:

        metrics.log_metric(
            run_id=context["run_id"],
            task_name="load",
            rows_processed=0,
            status="FAILED",
            start_time=start_time,
            error_message=str(e)
        )

        raise
    
    finally:
        spark.stop()

# DAG CONFIG
default_args = {
    "owner": "mnelisi",
    "start_date": datetime(2024, 1, 1),
    "retries": 3,
    "retry_delay": timedelta(minutes=2)
}

with DAG(
    dag_id="stock_prices_pipeline",
    default_args=default_args,
    schedule="0 15 * * *",
    catchup=False,
    tags=["etl", "stocks", "data_engineering"]
) as dag:

    extract = PythonOperator(
        task_id="extract_task",
        python_callable=extract_task
    )

    transform = PythonOperator(
        task_id="transform_task",
        python_callable=transform_task
    )

    load = PythonOperator(
        task_id="load_task",
        python_callable=load_task,
        op_kwargs={
        "table_name": table_name
      }
    )

    # Pipeline order
    extract >> transform >> load