import azure.functions as func
import datetime
from datetime import timezone
from utils import fetch_options_data
from db_operations import save_options_data
import json
import os
import logging

app = func.FunctionApp()

TICKERS = os.getenv("TICKERS", "")
if not TICKERS:
    raise ValueError("Please set the TICKERS environment variable")

logging.basicConfig(filename='function_app.log', level=logging.INFO)

@app.timer_trigger(schedule="0 * * * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def optionsweekday(myTimer: func.TimerRequest) -> None:
    
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info(f"Python timer trigger function executed at: {datetime.datetime.now().isoformat()}")

    for ticker in TICKERS.split(","):
        docs = fetch_options_data(ticker.strip())
        if not docs:
            logging.warning(f"No data for {ticker}")
            continue

        try:
            save_options_data(docs)
        except Exception:
            logging.exception(f"Failed to save data for {ticker}")

    

