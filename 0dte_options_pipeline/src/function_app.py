import azure.functions as func
import logging
import os
from datetime import datetime
import json
from dotenv import load_dotenv
from utils import fetch_0dte_options_data, format_timestamp, is_collection_time
from db_operations import save_options_data, initialize_database, get_0dte_options_data, get_0dte_options_summary

# Load environment variables
load_dotenv()

# Initialize database on startup
initialize_database()

app = func.FunctionApp()

@app.function_name(name="Fetch0DTEOptionsData")
@app.schedule(schedule="0 30 11 * * 1-5", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def fetch_0dte_options_data_timer(myTimer: func.TimerInfo) -> None:
    """
    Timer-triggered function to fetch 0DTE options data and store it in the database.
    Runs every weekday at 11:30 AM EST.
    """
    utc_timestamp = datetime.utcnow().isoformat()
    
    if myTimer.past_due:
        logging.info('The timer is past due!')
    
    logging.info('Python timer trigger function ran at %s', utc_timestamp)
    
    # Check if it's collection time
    if not is_collection_time():
        logging.info('Not collection time, skipping data fetch')
        return
    
    # List of tickers to fetch
    tickers = os.getenv('TICKERS', 'SPY,QQQ').split(',')
    
    for ticker in tickers:
        try:
            # Fetch 0DTE options data
            options_data = fetch_0dte_options_data(ticker)
            
            if options_data is not None and not options_data.empty:
                # Save to database
                save_options_data(options_data, ticker)
                logging.info(f"Successfully processed 0DTE options data for {ticker}")
            else:
                logging.warning(f"No 0DTE options data available for {ticker}")
                
        except Exception as e:
            logging.error(f"Error processing {ticker}: {str(e)}")

@app.function_name(name="Get0DTEOptionsData")
@app.route(route="0dte/{ticker}", methods=["GET"])
def get_0dte_options_data_http(req: func.HttpRequest) -> func.HttpResponse:
    """
    HTTP-triggered function to retrieve 0DTE options data from the database.
    """
    logging.info('Python HTTP trigger function processed a request.')
    
    ticker = req.route_params.get('ticker')
    if not ticker:
        return func.HttpResponse(
            "Please provide a ticker symbol in the URL path",
            status_code=400
        )
    
    # Get date parameter from query string
    date = req.params.get('date')
    
    try:
        # Get 0DTE options data
        options_data = get_0dte_options_data(ticker, date)
        
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
                f"No 0DTE options data found for {ticker}",
                status_code=404
            )
            
    except Exception as e:
        logging.error(f"Error retrieving 0DTE options data for {ticker}: {str(e)}")
        return func.HttpResponse(
            f"Error retrieving 0DTE options data: {str(e)}",
            status_code=500
        )

@app.function_name(name="Get0DTEOptionsSummary")
@app.route(route="0dte/{ticker}/summary", methods=["GET"])
def get_0dte_options_summary_http(req: func.HttpRequest) -> func.HttpResponse:
    """
    HTTP-triggered function to retrieve a summary of 0DTE options data from the database.
    """
    logging.info('Python HTTP trigger function processed a request.')
    
    ticker = req.route_params.get('ticker')
    if not ticker:
        return func.HttpResponse(
            "Please provide a ticker symbol in the URL path",
            status_code=400
        )
    
    # Get date parameter from query string
    date = req.params.get('date')
    
    try:
        # Get 0DTE options summary
        summary = get_0dte_options_summary(ticker, date)
        
        if summary:
            # Convert to JSON
            result = json.dumps(summary)
            return func.HttpResponse(
                result,
                mimetype="application/json",
                status_code=200
            )
        else:
            return func.HttpResponse(
                f"No 0DTE options summary found for {ticker}",
                status_code=404
            )
            
    except Exception as e:
        logging.error(f"Error retrieving 0DTE options summary for {ticker}: {str(e)}")
        return func.HttpResponse(
            f"Error retrieving 0DTE options summary: {str(e)}",
            status_code=500
        ) 