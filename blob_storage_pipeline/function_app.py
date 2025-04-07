import logging
import azure.functions as func
from azure.storage.blob import BlobServiceClient
import yfinance as yf
import pandas as pd
import os
from io import StringIO
from dotenv import load_dotenv, find_dotenv
from utils import fetch_options_data, format_timestamp
from db_operations import save_options_data, initialize_database
from datetime import datetime
import json

# TODO:
# 1. Test code for CRON job for 5 min interval
# 2. 3/29/2025 - could not get cron job to work
# --> Potential issues: 
# Python version for functions is 3.10 but I am using 3.12 for enviorment?
# Something to do with apple silicon versus other architecture - o3 mini mentioned this

# ADDED:
# 1. Add logging and basic error handling
# 2. Seperated utils file from functions app for better organization
# 3. Setup connection string 


load_dotenv(find_dotenv())

# Initialize database on startup
initialize_database()

app = func.FunctionApp()

@app.function_name(name="FetchOptionsData")
@app.schedule(schedule="0 */4 * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def fetch_options_data_timer(myTimer: func.TimerInfo) -> None:
    """
    Timer-triggered function to fetch options data and store it in the database.
    Runs every 4 hours.
    """
    utc_timestamp = datetime.utcnow().isoformat()
    
    if myTimer.past_due:
        logging.info('The timer is past due!')
    
    logging.info('Python timer trigger function ran at %s', utc_timestamp)
    
    # List of tickers to fetch
    tickers = os.getenv('TICKERS', 'SPY,QQQ,IWM').split(',')
    
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

@app.function_name(name="GetOptionsData")
@app.route(route="options/{ticker}", methods=["GET"])
def get_options_data_http(req: func.HttpRequest) -> func.HttpResponse:
    """
    HTTP-triggered function to retrieve options data from the database.
    """
    logging.info('Python HTTP trigger function processed a request.')
    
    ticker = req.route_params.get('ticker')
    if not ticker:
        return func.HttpResponse(
            "Please provide a ticker symbol in the URL path",
            status_code=400
        )
    
    try:
        from db_operations import get_latest_options_data
        
        # Get latest options data
        options_data = get_latest_options_data(ticker)
        
        if options_data is not None and not options_data.empty:
            # Convert to JSON
            result = options_data.to_json(orient='records')
            return func.HttpResponse(
                result,
                mimetype="application/json",
                status_code=200
            )
        else:
            return func.HttpResponse(
                f"No options data found for {ticker}",
                status_code=404
            )
            
    except Exception as e:
        logging.error(f"Error retrieving options data for {ticker}: {str(e)}")
        return func.HttpResponse(
            f"Error retrieving options data: {str(e)}",
            status_code=500
        )

@app.function_name(name="GetHistoricalOptionsData")
@app.route(route="options/{ticker}/history", methods=["GET"])
def get_historical_options_data_http(req: func.HttpRequest) -> func.HttpResponse:
    """
    HTTP-triggered function to retrieve historical options data from the database.
    """
    logging.info('Python HTTP trigger function processed a request.')
    
    ticker = req.route_params.get('ticker')
    if not ticker:
        return func.HttpResponse(
            "Please provide a ticker symbol in the URL path",
            status_code=400
        )
    
    # Get days parameter from query string
    days = req.params.get('days', '30')
    try:
        days = int(days)
    except ValueError:
        return func.HttpResponse(
            "Days parameter must be an integer",
            status_code=400
        )
    
    try:
        from db_operations import get_historical_options_data
        
        # Get historical options data
        options_data = get_historical_options_data(ticker, days)
        
        if options_data is not None and not options_data.empty:
            # Convert to JSON
            result = options_data.to_json(orient='records')
            return func.HttpResponse(
                result,
                mimetype="application/json",
                status_code=200
            )
        else:
            return func.HttpResponse(
                f"No historical options data found for {ticker}",
                status_code=404
            )
            
    except Exception as e:
        logging.error(f"Error retrieving historical options data for {ticker}: {str(e)}")
        return func.HttpResponse(
            f"Error retrieving historical options data: {str(e)}",
            status_code=500
        )