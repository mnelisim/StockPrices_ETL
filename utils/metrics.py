from datetime import datetime
from utils.logger import log_message

class PipelineMetrics:

    def __init__(self, connection):
        self.connection = connection

    #Create monitoring table
    def create_monitoring_table(self):
        conn, cur = self.connection.create_connection()
        try:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS stocks.etl_job_monitoring(
                run_id VARCHAR(100),
                task_name VARCHAR(50),
                rows_processed INT,
                status VARCHAR(20),
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                error_message TEXT
            )
            """)
            conn.commit()
            log_message("Table etl_job_monitoring is ready")
        except Exception as e:
            conn.rollback()
            log_message(f"Failed to create the table: {str(e)}")
        finally:
            cur.close()
            conn.close()
    #Inserting data into database
    def log_metric(self,
                   run_id,
                   task_name,
                   rows_processed,
                   status,
                   start_time,
                   error_message=None):

        conn, cur = self.connection.create_connection()
        try:
            # 1. Ensure start_time is a datetime object
            if isinstance(start_time, str):
                from dateutil import parser
                start_time = parser.parse(start_time)

            end_time = datetime.now()

            cur.execute("""
            INSERT INTO stocks.etl_job_monitoring( run_id, task_name, rows_processed, status, start_time, end_time, error_message)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            """, (
                str(run_id),
                str(task_name),
                int(rows_processed),
                str(status),
                start_time,
                end_time,
                error_message
            ))
            conn.commit()
            log_message(f"Logged metric for task {task_name}")
        except Exception as e:
            conn.rollback()
            log_message(f"Failed to log metric: {str(e)}")
        finally:
            cur.close()
            conn.close()

    