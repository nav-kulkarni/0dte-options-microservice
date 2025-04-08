import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from datetime import datetime
import os
from src.db_operations import get_db_connection, save_options_data, get_latest_options_data

class TestDatabaseOperations(unittest.TestCase):
    def setUp(self):
        # Sample options data for testing
        self.sample_data = pd.DataFrame({
            'strike': [100.0, 101.0],
            'lastPrice': [1.5, 2.0],
            'bid': [1.4, 1.9],
            'ask': [1.6, 2.1],
            'volume': [100, 200],
            'openInterest': [500, 600],
            'impliedVolatility': [0.3, 0.35],
            'delta': [0.5, 0.6],
            'gamma': [0.1, 0.15],
            'theta': [-0.05, -0.06],
            'vega': [0.2, 0.25]
        })
        self.ticker = 'AAPL'

    @patch('psycopg2.connect')
    def test_get_db_connection(self, mock_connect):
        # Test successful connection
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        
        conn = get_db_connection()
        self.assertEqual(conn, mock_conn)
        mock_connect.assert_called_once()

    @patch('psycopg2.connect')
    def test_get_db_connection_error(self, mock_connect):
        # Test connection error
        mock_connect.side_effect = Exception("Connection error")
        
        with self.assertRaises(Exception):
            get_db_connection()

    @patch('src.db_operations.get_db_connection')
    def test_save_options_data(self, mock_get_conn):
        # Setup mock connection and cursor
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_get_conn.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cur

        # Test successful save
        save_options_data(self.sample_data, self.ticker)
        
        # Verify cursor was created and closed
        mock_conn.cursor.assert_called_once()
        mock_cur.close.assert_called_once()
        
        # Verify commit was called
        mock_conn.commit.assert_called_once()
        
        # Verify connection was closed
        mock_conn.close.assert_called_once()

    @patch('src.db_operations.get_db_connection')
    def test_save_options_data_error(self, mock_get_conn):
        # Setup mock to raise an exception
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_get_conn.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cur
        mock_cur.execute.side_effect = Exception("Database error")

        # Test error handling
        with self.assertRaises(Exception):
            save_options_data(self.sample_data, self.ticker)
        
        # Verify rollback was called
        mock_conn.rollback.assert_called_once()
        
        # Verify connection was closed
        mock_conn.close.assert_called_once()

    @patch('src.db_operations.get_db_connection')
    def test_get_latest_options_data(self, mock_get_conn):
        # Setup mock connection
        mock_conn = MagicMock()
        mock_get_conn.return_value = mock_conn
        
        # Create sample data to be returned
        sample_df = pd.DataFrame({
            'ticker': ['AAPL', 'AAPL'],
            'strike': [100.0, 101.0],
            'timestamp': [datetime.now(), datetime.now()]
        })
        
        # Mock pd.read_sql_query to return our sample data
        with patch('pandas.read_sql_query', return_value=sample_df):
            result = get_latest_options_data('AAPL', limit=2)
            
            self.assertIsInstance(result, pd.DataFrame)
            self.assertEqual(len(result), 2)
            self.assertEqual(list(result.columns), ['ticker', 'strike', 'timestamp'])
        
        # Verify connection was closed
        mock_conn.close.assert_called_once()

if __name__ == '__main__':
    unittest.main() 