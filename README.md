# 0dte-options-data

## Set Up
1. Create your branch for developing, for example: `dev-ryan`
2. Checkout the branch and develop on your own branch
3. Set up virtual environment using `venv` or `conda`. python=3.10 is fine.

## Developing
We will keep `main` intact. We will merge all our changes from our personal development branches into `develop`

You MUST make a PR request to merge your changes onto `develop`

## Phase 1: Azure Data Engineering

Creating a pipeline to pull data from yfinance every hour, and store data in Azure blob. 

### 1.1 Testing Yfinance Data Collection

Fetches options data and open interest for a given stock ticker using the `yfinance` library structured in a `pandas` data frame. To use, navigate to *yfinance_data* directory and install dependencies from .yaml file. Run python3 fetch_options_data.py, and the head of the dataframe will appear for your chosen ticker.

Note:
- strike: The strike price of the option.
- openInterest: The number of outstanding option contracts.
- expiration_date: The expiration date of the option.
- type: Whether the option is a "call" or "put".

## Phase 2: Using Data to Research and Test Algorithms

## Phase 3: Live Algotrading on Kalshi
