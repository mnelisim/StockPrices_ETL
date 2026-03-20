import time
import os
from datetime import datetime


# ====================== LOGGING FUNCTION ======================
def log_message(message, level="INFO", run_id=None):
    
    #Actual timestamp of the log event
    event_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    #Default run ID if none provided
    if run_id is None:
       run_id = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    #Ensure logs folder exist
    os.makedirs("logs", exist_ok=True)
    log_filename = f"logs/etl_log.txt"
    
    #write log
    with open(log_filename, "a") as log:
        log.write(f"[Run ID: {run_id}] [Time: {event_time}] {message}\n")

    #Print log to console
    print(f"\n[Run ID: {run_id}] [Time: {event_time}] {message}")