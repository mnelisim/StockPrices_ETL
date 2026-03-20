import psycopg2
import time
from datetime import datetime
from config import table_name,DB_NAME,DB_HOST,DB_USER,DB_PASSWORD,DB_PORT
from utils.logger import log_message
from pyspark.sql.functions import year, month, dayofmonth, quarter

class Dataload:

    #create_table_if_not_exists
    def create_table_if_not_exists(self, cur, conn, table_name):
        try:
            cur.execute(f"""
                CREATE SCHEMA IF NOT EXISTS stocks;
                CREATE TABLE IF NOT EXISTS stocks.{table_name} (
                        id SERIAL PRIMARY KEY,
                        date DATE,
                        ticker TEXT,
                        pricetype TEXT,
                        price FLOAT)
            """)
            conn.commit()
            log_message(f"Table '{table_name}' is ready")
        except Exception as e:
            conn.rollback()
            log_message(f"Failed to create the table: {str(e)}")

    #Load data into database
    def store_data_in_db(self, cur, conn, df, table_name):
        try:
            df = df.select(
                "date",
                "ticker",
                "pricetype",
                "price"
            )

            query = f"""INSERT INTO stocks.{table_name} (date,ticker,pricetype,price)
            VALUES (%s,%s,%s,%s)""" 

            values = []
            for row in df.toLocalIterator():
                values.append((
                        row["date"],
                        row["ticker"],
                        row["pricetype"],
                        row["price"]
                        ))
            cur.executemany(query, values)
            #push data into db
            conn.commit()
            print(f"Data is successfully inserted to: {table_name}")
        except Exception as error:
            conn.rollback()
            log_message(f"Data not inserted in DB: {str(error)}")

    #create_table_if_not_exists
    def create_table_stock_metrics(self, cur, conn):
            try:
                cur.execute("""
                CREATE TABLE IF NOT EXISTS stocks.stock_metrics (
                    Date DATE,
                    Ticker VARCHAR(10),
                    daily_return FLOAT,
                    ma_7 FLOAT,
                    volatility_30 FLOAT
                )
                """)
                conn.commit()
                log_message("Table stock_metrics is ready")
            except Exception as e:
                conn.rollback()
                log_message(f"Failed to create the table: {str(e)}")
          
    #Load data into database
    def store_stock_metrics_data(self, cur, conn, df_close):

        try:
            df_close = df_close.select(
                "date",
                "ticker",
                "daily_return",
                "ma_7",
                "volatility_30"
            )

            query = """
                INSERT INTO stocks.stock_metrics
                VALUES (%s,%s,%s,%s,%s)
                """
            values = []
            for row in df_close.toLocalIterator():
                values.append((
                        row["date"],
                        row["ticker"],
                        row["daily_return"],
                        row["ma_7"],
                        row["volatility_30"]
                       ))

            cur.executemany(query, values)
            conn.commit()
            log_message(f"Inserted {len(values)} rows into stocks.stock_metrics")
        except Exception as e:
            conn.rollback()
            log_message(f"Failed to insert data: {str(e)}")
        
    
    def create_star_schema(self, cur, conn):
        try:
            cur.execute("""
            CREATE SCHEMA IF NOT EXISTS stocks;

            CREATE TABLE IF NOT EXISTS stocks.dim_stock (
                stock_id SERIAL PRIMARY KEY,
                ticker VARCHAR(10) UNIQUE
            );

            CREATE TABLE IF NOT EXISTS stocks.dim_date (
                date DATE PRIMARY KEY,
                year INT,
                month INT,
                day INT,
                quarter INT
            );

            CREATE TABLE IF NOT EXISTS stocks.fact_stock_prices (
                id SERIAL PRIMARY KEY,
                date DATE REFERENCES stocks.dim_date(date),
                stock_id INT REFERENCES stocks.dim_stock(stock_id),
                price FLOAT,
                daily_return FLOAT,
                ma_7 FLOAT,
                volatility FLOAT
            );
            """)
            conn.commit()
            log_message("Star schema tables created")
        except Exception as e:
            conn.rollback()
            log_message(f"Star schema creation failed: {str(e)}")

    def load_dim_stock(self, cur, conn, df):
        try:
            tickers = df.select("ticker").distinct()

            for row in tickers.toLocalIterator():
                cur.execute("""
                    INSERT INTO stocks.dim_stock (ticker)
                    VALUES (%s)
                    ON CONFLICT (ticker) DO NOTHING
                """, (row["ticker"],))

            conn.commit()
        except Exception as e:
            conn.rollback()
            log_message(f"dim_stock load failed: {str(e)}")

    def load_dim_date(self, cur, conn, df):
        try:
            date_df = df.select("date").distinct() \
                .withColumn("year", year("date")) \
                .withColumn("month", month("date")) \
                .withColumn("day", dayofmonth("date")) \
                .withColumn("quarter", quarter("date"))

            for row in date_df.toLocalIterator():
                cur.execute("""
                    INSERT INTO stocks.dim_date (date, year, month, day, quarter)
                    VALUES (%s,%s,%s,%s,%s)
                    ON CONFLICT (date) DO NOTHING
                """, (
                    row["date"],
                    row["year"],
                    row["month"],
                    row["day"],
                    row["quarter"]
                ))

            conn.commit()
        except Exception as e:
            conn.rollback()
            log_message(f"dim_date load failed: {str(e)}")

    def load_fact_table(self, cur, conn, df_close):
        try:
            # Map ticker → stock_id
            cur.execute("SELECT stock_id, ticker FROM stocks.dim_stock")
            stock_map = {ticker: stock_id for stock_id, ticker in cur.fetchall()}

            df_close = df_close.select(
                "date",
                "ticker",
                "price",
                "daily_return",
                "ma_7",
                "volatility_30"
            )

            values = []
            for row in df_close.toLocalIterator():
                values.append((
                    row["date"],
                    stock_map.get(row["ticker"]),
                    row["price"],
                    row["daily_return"],
                    row["ma_7"],
                    row["volatility_30"]
                ))

            cur.executemany("""
                INSERT INTO stocks.fact_stock_prices
                (date, stock_id, price, daily_return, ma_7, volatility)
                VALUES (%s,%s,%s,%s,%s,%s)
            """, values)

            conn.commit()
            log_message(f"Inserted {len(values)} rows into fact table")

        except Exception as e:
            conn.rollback()
            log_message(f"fact table load failed: {str(e)}")
            
            





