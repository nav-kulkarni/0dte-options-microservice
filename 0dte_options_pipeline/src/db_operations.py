import os
import logging
from typing import Optional, List, Dict, Any
import pandas as pd
from sqlalchemy import create_engine, text, Table, Column, Integer, Float, String, DateTime, MetaData, Index, Boolean
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

# Load environment variables
load_dotenv()

# Get database connection string from environment variable
DB_CONNECTION_STRING = os.getenv('AZURE_POSTGRES_CONNECTION_STRING')
if not DB_CONNECTION_STRING:
    raise ValueError("Database connection string not found in environment variables")

def get_db_engine():
    """Create and return a SQLAlchemy engine for Azure PostgreSQL."""
    try:
        # Convert the connection string to SQLAlchemy format if needed
        if DB_CONNECTION_STRING.startswith('postgresql://'):
            sqlalchemy_conn_string = DB_CONNECTION_STRING.replace('postgresql://', 'postgresql+psycopg2://')
        else:
            sqlalchemy_conn_string = DB_CONNECTION_STRING
            
        engine = create_engine(sqlalchemy_conn_string)
        return engine
    except Exception as e:
        logging.error(f"Error creating database engine: {str(e)}")
        raise

def initialize_database():
    """Initialize the database schema if it doesn't exist."""
    engine = get_db_engine()
    metadata = MetaData()
    
    # Define the options_data table
    # This variable is used indirectly by metadata.create_all()
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

def get_db_connection():
    """Create and return a database connection."""
    try:
        conn = psycopg2.connect(DB_CONNECTION_STRING)
        return conn
    except Exception as e:
        logging.error(f"Error connecting to database: {str(e)}")
        raise

def save_options_data(data: pd.DataFrame, ticker: str):
    """
    Save options data to the database.
    
    Args:
        data (pd.DataFrame): DataFrame containing options data
        ticker (str): Stock ticker symbol
    """
    if data is None or data.empty:
        logging.warning("No data to save")
        return
    
    conn = None
    cur = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Prepare data for insertion
        timestamp = datetime.now()
        records = []
        for _, row in data.iterrows():
            record = (
                ticker,
                row['strike'],
                row['open_interest'],       # debug: updated column name to match
                row['expiration_date'],     
                row['option_type'],         
                row['stock_price'],  
                timestamp
            )
            records.append(record)
        
        # Insert data using execute_values for better performance
        insert_query = """
            INSERT INTO options_data 
            (ticker, strike, open_interest, expiration_date, option_type, stock_price, timestamp)
            VALUES %s
        """
        execute_values(cur, insert_query, records)
        
        conn.commit()
        logging.info(f"Successfully saved {len(records)} records for {ticker}")
        
    except Exception as e:
        if conn:
            conn.rollback()
        logging.error(f"Error saving data: {str(e)}")
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def get_latest_options_data(ticker: str, limit: int = 100):
    """
    Retrieve the latest options data for a given ticker.
    
    Args:
        ticker (str): Stock ticker symbol
        limit (int): Maximum number of records to return
        
    Returns:
        pd.DataFrame: DataFrame containing the latest options data
    """
    conn = None
    try:
        conn = get_db_connection()
        query = """
            SELECT * FROM options_data 
            WHERE ticker = %s 
            ORDER BY timestamp DESC 
            LIMIT %s
        """
        return pd.read_sql_query(query, conn, params=(ticker, limit))
        
    except Exception as e:
        logging.error(f"Error retrieving data: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()

def get_options_data(ticker: str, date: Optional[str] = None) -> pd.DataFrame:
    """
    Retrieve options data from the database for a specific date.
    
    Args:
        ticker (str): Ticker symbol
        date (Optional[str]): Date to retrieve data for (YYYY-MM-DD)
        
    Returns:
        pd.DataFrame: DataFrame containing options data
    """
    engine = get_db_engine()
    
    # Build query
    query = """
    SELECT * FROM options_data 
    WHERE ticker = :ticker 
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
        
        logging.info(f"Retrieved {len(result)} rows of options data for {ticker} from database")
        return result
    except SQLAlchemyError as e:
        logging.error(f"Error retrieving data from database: {str(e)}")
        raise

