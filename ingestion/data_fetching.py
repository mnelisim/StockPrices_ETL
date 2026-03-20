import yfinance as yf
import pandas as pd
import os
import json
from datetime import datetime
import time
import pyarrow
from utils.logger import log_message

class DataExtractor:

    #Extracting data from yfinance
    def extract_data(self, retries=3):

        #current timestamp
        timestamp_file = datetime.now().strftime("%Y%m%d_%H%M%S")

        #List of Ticker
        symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"] 
        start_date = "2020-01-01"
        end_date = "2026-01-31"

        for attempt in range(retries):
            try:    
                log_message(
                    f"Downloadning yfinance data: (attempt {attempt + 1})",
                    level="INFO", 
                    run_id=timestamp_file)
                
                #Downloading historical stock price
                data = yf.download(symbols, start=start_date, end=end_date)

                #data validation
                if data is None or data.empty:
                    raise ValueError("Download failed - empty dataset", level="ERROR", )
                log_message(
                    f"yfinance data has been successfully downloded", 
                    level="INFO",
                    run_id=timestamp_file)

                # Flatten MultiIndex columns properly
                new_columns = []
                for col in data.columns:
                    a, b = col
                    new_columns.append(f"{a}_{b}")
                data.columns = new_columns        
                df = data.reset_index()
                
                #save csv
                os.makedirs(f"data/raw", exist_ok=True)
                csv_filename = f"data/raw/stockprices_etl_{timestamp_file}.csv"
                df.to_csv(csv_filename, index=False)
                
                #save parquet
                os.makedirs(f"data/parquet", exist_ok=True)
                parquet_filename = f"data/parquet/stockprices_etl_{timestamp_file}.parquet"
                df.to_parquet(parquet_filename, index=False)
                
                log_message(f"Raw data saved to csv: {csv_filename}", run_id=timestamp_file)
                log_message(f"Raw data saved to parquet: {parquet_filename}", run_id=timestamp_file)

                return parquet_filename

            
            except Exception as error:
                log_message(
                    f"Error occured during download: {error}",
                    level="ERROR", 
                    run_id=timestamp_file)
                
                #retry after failing to download
                if attempt < retries - 1:
                   log_message(
                       f"retry download (attempt {attempt + 1}/{retries})", 
                        level="WARNING", 
                        run_id=timestamp_file)
                   time.sleep(3)
                else:
                    log_message(
                        f"failed to download data after retry.",
                        level="ERROR", 
                        run_id=timestamp_file)
                    raise


