import os
import logging
from typing import Optional, List, Dict, Any
import pandas as pd
from sqlalchemy import create_engine, text, Table, Column, Integer, Float, String, DateTime, MetaData, Index, Boolean
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database connection string
DB_USER = os.getenv('PG_USER')
DB_PASSWORD = os.getenv('PG_PASSWORD')
DB_SERVER = os.getenv('PG_SERVER')
DB_PORT = os.getenv('PG_PORT', '5432')
DB_NAME = os.getenv('PG_DATABASE')

DB_CONNECTION_STRING = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_SERVER}:{DB_PORT}/{DB_NAME}"

def get_db_engine():
    """Create and return a SQLAlchemy engine for Azure PostgreSQL."""
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
        Column('is_0dte', Boolean, nullable=False, default=True),
        Index('idx_ticker_exp_date', 'ticker', 'expiration_date'),
        Index('idx_timestamp', 'timestamp'),
        Index('idx_strike', 'strike'),
        Index('idx_is_0dte', 'is_0dte')
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
    
    # Add is_0dte column
    df['is_0dte'] = (df['expiration_date'].dt.date == pd.Timestamp.utcnow().date())
    
    try:
        # Save to database
        df.to_sql('options_data', engine, if_exists='append', index=False, method='multi')
        logging.info(f"Successfully saved {len(df)} rows for {ticker} to database")
    except SQLAlchemyError as e:
        logging.error(f"Error saving data to database: {str(e)}")
        raise

def get_0dte_options_data(
    ticker: str,
    date: Optional[str] = None
) -> pd.DataFrame:
    """
    Retrieve 0DTE options data from the database for a specific date.
    
    Args:
        ticker (str): Ticker symbol
        date (Optional[str]): Date to retrieve data for (YYYY-MM-DD)
        
    Returns:
        pd.DataFrame: DataFrame containing 0DTE options data
    """
    engine = get_db_engine()
    
    # Build query
    query = """
    SELECT * FROM options_data 
    WHERE ticker = :ticker 
    AND is_0dte = TRUE
    """
    params = {"ticker": ticker}
    
    if date:
        query += " AND DATE(timestamp) = :date"
        params["date"] = date
    
    query += " ORDER BY timestamp, strike"
    
    try:
        # Execute query
        with engine.connect() as conn:
            result = pd.read_sql_query(text(query), conn, params=params)
        
        logging.info(f"Retrieved {len(result)} rows of 0DTE data for {ticker} from database")
        return result
    except SQLAlchemyError as e:
        logging.error(f"Error retrieving data from database: {str(e)}")
        raise

def get_latest_0dte_options_data(ticker: str) -> pd.DataFrame:
    """
    Get the most recent 0DTE options data for a ticker.
    
    Args:
        ticker (str): Ticker symbol
        
    Returns:
        pd.DataFrame: DataFrame containing the latest 0DTE options data
    """
    engine = get_db_engine()
    
    query = """
    SELECT * FROM options_data 
    WHERE ticker = :ticker 
    AND is_0dte = TRUE
    AND timestamp = (
        SELECT MAX(timestamp) 
        FROM options_data 
        WHERE ticker = :ticker
        AND is_0dte = TRUE
    )
    ORDER BY strike
    """
    
    try:
        with engine.connect() as conn:
            result = pd.read_sql_query(text(query), conn, params={"ticker": ticker})
        
        logging.info(f"Retrieved latest 0DTE data for {ticker} from database")
        return result
    except SQLAlchemyError as e:
        logging.error(f"Error retrieving latest data from database: {str(e)}")
        raise
