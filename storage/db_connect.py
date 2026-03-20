import psycopg2
import time
from config import DB_NAME,DB_HOST,DB_USER,DB_PASSWORD,DB_PORT

class Connection:

    #Database connection
    def create_connection(self):
        while True:
            try:
                conn = psycopg2.connect(
                    database=DB_NAME,host=DB_HOST,user=DB_USER,password=DB_PASSWORD,port=DB_PORT) 
                cur = conn.cursor()
                
                print("Database connection successfully!")
                return conn, cur
            
            except Exception as error:
                print("Connection to database failed")
                print(f"Error: {error}")
                time.sleep(2) #Wait 2 seconds and retry
                