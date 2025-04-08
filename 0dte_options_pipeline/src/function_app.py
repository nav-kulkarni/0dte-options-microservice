import logging
import azure.functions as func
import os
from datetime import datetime, timezone
import json
from dotenv import load_dotenv
from utils import fetch_options_data
from db_operations import save_options_data, initialize_database


load_dotenv()
initialize_database()
app = func.FunctionApp()

@app.timer_trigger(schedule="0 30 10 * * 1-5", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def options_weekday_trigger(myTimer: func.TimerRequest) -> None:

    utc_timestamp = datetime.now(timezone.utc).isoformat()

    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)

    tickers = os.getenv('TICKERS').split(',')

    for ticker in tickers:
        try:
            # Fetch options data
            options_data = fetch_options_data(ticker)
            
            if options_data is not None and not options_data.empty:
                # Save to database
                save_options_data(options_data, ticker)
                logging.info(f"Successfully processed options data for {ticker}")
            else:
                logging.warning(f"No options data available for {ticker}")
                
        except Exception as e:
            logging.error(f"Error processing {ticker}: {str(e)}")