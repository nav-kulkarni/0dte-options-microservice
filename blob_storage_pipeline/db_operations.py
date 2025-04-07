import os
import logging
from typing import Optional, List, Dict, Any
import pandas as pd
from sqlalchemy import create_engine, text, Table, Column, Integer, Float, String, DateTime, MetaData, Index
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database connection string
DB_CONNECTION_STRING = os.getenv('AZURE_SQL_CONNECTION_STRING')

def get_db_engine():
    """Create and return a SQLAlchemy engine for Azure SQL Database."""
    if not DB_CONNECTION_STRING:
        raise ValueError("Database connection string not found in environment variables")
    
    try:
        engine = create_engine(DB_CONNECTION_STRING)
        return engine
    except Exception as e:
        logging.error(f"Error creating database engine: {str(e)}")
        raise

def initialize_database():
    """Initialize the database schema if it doesn't exist."""
    engine = get_db_engine()
    metadata = MetaData()
    
    # Define the options_data table
    options_data = Table(
        'options_data', 
        metadata,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('ticker', String(10), nullable=False),
        Column('strike', Float, nullable=False),
        Column('open_interest', Integer, nullable=False),
        Column('expiration_date', DateTime, nullable=False),
        Column('option_type', String(4), nullable=False),  # 'call' or 'put'
        Column('stock_price', Float, nullable=False),
        Column('timestamp', DateTime, nullable=False),
        Column('implied_volatility', Float, nullable=True),
        Column('delta', Float, nullable=True),
        Column('gamma', Float, nullable=True),
        Column('theta', Float, nullable=True),
        Index('idx_ticker_exp_date', 'ticker', 'expiration_date'),
        Index('idx_timestamp', 'timestamp'),
        Index('idx_strike', 'strike')
    )
    
    try:
        # Create tables if they don't exist
        metadata.create_all(engine)
        logging.info("Database schema initialized successfully")
    except SQLAlchemyError as e:
        logging.error(f"Error initializing database schema: {str(e)}")
        raise

def save_options_data(df: pd.DataFrame, ticker: str):
    """
    Save options data to the database.
    
    Args:
        df (pd.DataFrame): DataFrame containing options data
        ticker (str): Ticker symbol
    """
    if df is None or df.empty:
        logging.warning(f"No data to save for {ticker}")
        return
    
    engine = get_db_engine()
    
    # Add ticker column if not present
    if 'ticker' not in df.columns:
        df['ticker'] = ticker
    
    # Ensure column names match database schema
    df = df.rename(columns={
        'type': 'option_type',
        'openInterest': 'open_interest'
    })
    
    # Convert timestamp to datetime if it's a string
    if 'timestamp' in df.columns and df['timestamp'].dtype == 'object':
        df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Convert expiration_date to datetime if it's a string
    if 'expiration_date' in df.columns and df['expiration_date'].dtype == 'object':
        df['expiration_date'] = pd.to_datetime(df['expiration_date'])
    
    try:
        # Save to database
        df.to_sql('options_data', engine, if_exists='append', index=False)
        logging.info(f"Successfully saved {len(df)} rows for {ticker} to database")
    except SQLAlchemyError as e:
        logging.error(f"Error saving data to database: {str(e)}")
        raise

def get_options_data(
    ticker: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    option_type: Optional[str] = None,
    min_strike: Optional[float] = None,
    max_strike: Optional[float] = None
) -> pd.DataFrame:
    """
    Retrieve options data from the database with filtering options.
    
    Args:
        ticker (str): Ticker symbol
        start_date (Optional[str]): Start date for filtering (YYYY-MM-DD)
        end_date (Optional[str]): End date for filtering (YYYY-MM-DD)
        option_type (Optional[str]): Filter by option type ('call' or 'put')
        min_strike (Optional[float]): Minimum strike price
        max_strike (Optional[float]): Maximum strike price
        
    Returns:
        pd.DataFrame: DataFrame containing filtered options data
    """
    engine = get_db_engine()
    
    # Build query
    query = "SELECT * FROM options_data WHERE ticker = :ticker"
    params = {"ticker": ticker}
    
    if start_date:
        query += " AND timestamp >= :start_date"
        params["start_date"] = start_date
    
    if end_date:
        query += " AND timestamp <= :end_date"
        params["end_date"] = end_date
    
    if option_type:
        query += " AND option_type = :option_type"
        params["option_type"] = option_type
    
    if min_strike is not None:
        query += " AND strike >= :min_strike"
        params["min_strike"] = min_strike
    
    if max_strike is not None:
        query += " AND strike <= :max_strike"
        params["max_strike"] = max_strike
    
    try:
        # Execute query
        with engine.connect() as conn:
            result = pd.read_sql_query(text(query), conn, params=params)
        
        logging.info(f"Retrieved {len(result)} rows for {ticker} from database")
        return result
    except SQLAlchemyError as e:
        logging.error(f"Error retrieving data from database: {str(e)}")
        raise

def get_latest_options_data(ticker: str) -> pd.DataFrame:
    """
    Get the most recent options data for a ticker.
    
    Args:
        ticker (str): Ticker symbol
        
    Returns:
        pd.DataFrame: DataFrame containing the latest options data
    """
    engine = get_db_engine()
    
    query = """
    SELECT * FROM options_data 
    WHERE ticker = :ticker 
    AND timestamp = (
        SELECT MAX(timestamp) 
        FROM options_data 
        WHERE ticker = :ticker
    )
    """
    
    try:
        with engine.connect() as conn:
            result = pd.read_sql_query(text(query), conn, params={"ticker": ticker})
        
        logging.info(f"Retrieved latest data for {ticker} from database")
        return result
    except SQLAlchemyError as e:
        logging.error(f"Error retrieving latest data from database: {str(e)}")
        raise

def get_historical_options_data(
    ticker: str,
    days: int = 30
) -> pd.DataFrame:
    """
    Get historical options data for a ticker over a specified number of days.
    
    Args:
        ticker (str): Ticker symbol
        days (int): Number of days of historical data to retrieve
        
    Returns:
        pd.DataFrame: DataFrame containing historical options data
    """
    engine = get_db_engine()
    
    query = """
    SELECT * FROM options_data 
    WHERE ticker = :ticker 
    AND timestamp >= DATEADD(day, -:days, GETDATE())
    ORDER BY timestamp, expiration_date, strike
    """
    
    try:
        with engine.connect() as conn:
            result = pd.read_sql_query(text(query), conn, params={"ticker": ticker, "days": days})
        
        logging.info(f"Retrieved {len(result)} rows of historical data for {ticker} from database")
        return result
    except SQLAlchemyError as e:
        logging.error(f"Error retrieving historical data from database: {str(e)}")
        raise 