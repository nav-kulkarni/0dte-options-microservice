# Options Data Pipeline

This Azure Functions app fetches options data for specified tickers and stores it in an Azure SQL Database.

## Setup Instructions

### Prerequisites

- Python 3.9+
- Azure Functions Core Tools
- Azure SQL Database
- ODBC Driver 18 for SQL Server

### Environment Setup

1. Install the required Python packages:
   ```
   pip install -r requirements.txt
   ```

2. Install the ODBC Driver for SQL Server:
   - For macOS: `brew install microsoft/mssql-release/msodbcsql18`
   - For Ubuntu: Follow [Microsoft's instructions](https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server)

3. Configure the `.env` file with your Azure SQL Database connection string:
   ```
   AZURE_SQL_CONNECTION_STRING=mssql+pyodbc://username:password@server.database.windows.net/database?driver=ODBC+Driver+18+for+SQL+Server
   TICKERS=SPY,QQQ,IWM
   LOG_LEVEL=INFO
   ```

### Database Migration

If you're migrating from Azure Blob Storage to Azure SQL Database:

1. Ensure both connection strings are in your `.env` file:
   ```
   AZURE_BLOB_CONNECTION_STRING=your_blob_connection_string
   AZURE_SQL_CONNECTION_STRING=your_sql_connection_string
   ```

2. Run the migration script:
   ```
   python migrate_to_sql.py
   ```

3. Verify the migration by checking the database:
   ```
   python -c "from db_operations import get_options_data; print(get_options_data('SPY').head())"
   ```

## Function App Structure

- `function_app.py`: Main Azure Functions app with timer and HTTP triggers
- `utils.py`: Utility functions for fetching and processing options data
- `db_operations.py`: Database operations for storing and retrieving options data
- `migrate_to_sql.py`: Script to migrate data from Blob Storage to SQL Database

## API Endpoints

- `GET /api/options/{ticker}`: Get the latest options data for a ticker
- `GET /api/options/{ticker}/history?days=30`: Get historical options data for a ticker

## Deployment

1. Deploy to Azure Functions:
   ```
   func azure functionapp publish your-function-app-name
   ```

2. Configure application settings in the Azure Portal:
   - Add the `AZURE_SQL_CONNECTION_STRING` and `TICKERS` environment variables

## Troubleshooting

- **Connection Issues**: Ensure your IP is allowed in the Azure SQL Database firewall rules
- **ODBC Driver Issues**: Verify the correct ODBC driver is installed and specified in the connection string
- **Data Migration Issues**: Check the logs for specific errors during migration 