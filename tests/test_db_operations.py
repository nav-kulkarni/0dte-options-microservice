import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from datetime import datetime
from db_operations import get_db_connection, save_options_data, get_latest_options_data

class TestDatabaseOperations(unittest.TestCase):
    def setUp(self):
        # Updated sample data with the correct column names expected by save_options_data
        self.sample_data = pd.DataFrame({
            'strike': [100.0, 101.0],
            'open_interest': [500, 600],
            'expiration_date': [datetime.now(), datetime.now()],
            'option_type': ['call', 'put'],
            'stock_price': [150.0, 150.0],
            'timestamp': [datetime.now(), datetime.now()]
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

    # Patch execute_values to prevent it from actually being executed
    @patch("src.db_operations.execute_values")
    @patch('src.db_operations.get_db_connection')
    def test_save_options_data(self, mock_get_conn, mock_execute_values):
        # Setup mock connection and cursor
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        # Set the encoding on the cursor's connection to a string literal "UTF8"
        mock_cur.connection = MagicMock()
        mock_cur.connection.encoding = "UTF8"
        
        mock_get_conn.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cur

        # Test successful save
        save_options_data(self.sample_data, self.ticker)
        
        # Verify that get_db_connection and execute_values were called as expected
        mock_conn.cursor.assert_called_once()
        mock_execute_values.assert_called_once()
        mock_conn.commit.assert_called_once()
        mock_conn.close.assert_called_once()
        mock_cur.close.assert_called_once()

    @patch("src.db_operations.execute_values")
    @patch('src.db_operations.get_db_connection')
    def test_save_options_data_error(self, mock_get_conn, mock_execute_values):
        # Setup mock to raise an exception during execution
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.connection = MagicMock()
        mock_cur.connection.encoding = "UTF8"
        
        mock_get_conn.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cur
        # Simulate an error during execute_values
        mock_execute_values.side_effect = Exception("Database error")
        
        # Test error handling in save_options_data
        with self.assertRaises(Exception):
            save_options_data(self.sample_data, self.ticker)
        
        # Verify rollback and connection close were called
        mock_conn.rollback.assert_called_once()
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
        
        # Patch pandas.read_sql_query to return our sample data
        with patch('pandas.read_sql_query', return_value=sample_df):
            result = get_latest_options_data('AAPL', limit=2)
            
            self.assertIsInstance(result, pd.DataFrame)
            self.assertEqual(len(result), 2)
            self.assertEqual(list(result.columns), ['ticker', 'strike', 'timestamp'])
        
        # Verify connection was closed
        mock_conn.close.assert_called_once()

if __name__ == '__main__':
    unittest.main()
