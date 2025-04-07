# 0DTE Options Data Pipeline

This Azure Functions app fetches 0DTE (zero days to expiration) options data for SPY and QQQ tickers and stores it in an Azure PostgreSQL database.

## Features

- Collects 0DTE options data once a day at 11:30 AM EST on weekdays
- Stores data in Azure PostgreSQL for efficient querying
- Focuses specifically on options expiring on the current trading day

## Setup Instructions

### Prerequisites

- Python 3.9+
- Azure Functions Core Tools
- Azure PostgreSQL Flexible Server
- Azure subscription

### Azure Setup

1. **Create Azure PostgreSQL Flexible Server**:
   - Go to Azure Portal
   - Create a new PostgreSQL Flexible Server
   - Configure firewall rules to allow Azure services and your IP
   - Create a database and user with appropriate permissions
   - Note the connection string for your application

2. **Create Azure Function App**:
   - Go to Azure Portal
   - Create a new Function App
   - Select Python as the runtime
   - Choose Consumption plan for cost efficiency
   - Enable Application Insights for monitoring

### Local Development Setup

1. Install the required Python packages:
   ```
   pip install -r requirements.txt
   ```

2. Configure the `.env` file with your Azure PostgreSQL connection string:
   ```
   AZURE_POSTGRES_CONNECTION_STRING=postgresql://username:password@server.postgres.database.azure.com:5432/database
   TICKERS=SPY,QQQ
   LOG_LEVEL=INFO
   COLLECTION_TIME=11:30
   ```

3. Create a `local.settings.json` file for local development:
   ```json
   {
     "IsEncrypted": false,
     "Values": {
       "FUNCTIONS_WORKER_RUNTIME": "python",
       "AzureWebJobsStorage": "UseDevelopmentStorage=true",
       "AZURE_POSTGRES_CONNECTION_STRING": "your_connection_string",
       "TICKERS": "SPY,QQQ",
       "LOG_LEVEL": "INFO",
       "COLLECTION_TIME": "11:30"
     }
   }
   ```

### Running Locally

1. Start the function app:
   ```
   func start
   ```

2. The app will run the following function:
   - `Fetch0DTEOptionsData`: Timer-triggered function that runs every weekday at 11:30 AM EST

## Deployment

1. Deploy to Azure Functions:
   ```
   func azure functionapp publish your-function-app-name
   ```

2. Configure application settings in the Azure Portal:
   - Add the `AZURE_POSTGRES_CONNECTION_STRING`, `TICKERS`, and `COLLECTION_TIME` environment variables

## Troubleshooting

- **Connection Issues**: Ensure your IP is allowed in the Azure PostgreSQL firewall rules
- **Data Collection Issues**: Check if it's during collection time (11:30 AM EST on weekdays)
- **Function App Issues**: Check the logs in the Azure Portal or Application Insights

## Project Structure

- `src/function_app.py`: Main Azure Functions app with timer trigger
- `src/utils.py`: Utility functions for fetching and processing 0DTE options data
- `src/db_operations.py`: Database operations for storing data
- `tests/`: Test files for the application 