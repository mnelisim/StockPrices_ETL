from dotenv import load_dotenv
import os

# Path to .env file
load_dotenv() 

#=====================CONFIG TABLE====================
table_name = "stock_prices"

#=====================CONFIG DATABASE====================
DB_NAME = os.getenv("DB_NAME")
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT")

