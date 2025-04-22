import logging
from datetime import datetime
from typing import List, Optional

import pandas as pd
import yfinance as yf

def fetch_options_data(ticker_symbol: str) -> Optional[List[dict]]:
    """
    Fetch the *current* snapshot for ALL expirations of a ticker.
    Returns a list of dicts, each with keys:
      ticker, expiration_date (datetime), option_type (str),
      strike (float), open_interest (int), volume (int),
      bid (float), ask (float), last_price (float),
      implied_volatility (float), stock_price (float),
      ts (datetime), time_to_expiry_days (float)
    """
    try:
        logging.info(f"Fetching options snapshot for {ticker_symbol}")
        ticker = yf.Ticker(ticker_symbol)

        # Note: This can only be run when market open, else --> return's empty df
        hist = ticker.history(period="1d")
        if hist.empty or "Close" not in hist:
            logging.error("No closing price for %s", ticker_symbol)
            return None
        stock_price = float(hist["Close"].iloc[-1])

        expirations = ticker.options
        if not expirations:
            logging.error("No expirations for %s", ticker_symbol)
            return None

        now = datetime.now()
        docs: List[dict] = []

        # 3) Iterate each expiration
        for exp_str in expirations:
            try:
                chain = ticker.option_chain(exp_str)
            except Exception as e:
                logging.warning("Skip %s @ %s: %s", ticker_symbol, exp_str, e)
                continue

            exp_dt = datetime.fromisoformat(exp_str)
            tte_days = (exp_dt - now).total_seconds() / 86400.0

            # 4) Extract calls and puts
            cols = ["strike","openInterest","volume","bid","ask","lastPrice","impliedVolatility"]
            for df, opt_type in ((chain.calls, "call"), (chain.puts, "put")):
                if df.empty or not set(cols).issubset(df.columns):
                    continue

                slice_df = df.loc[:, cols].copy().fillna(0)
                slice_df = slice_df.astype({
                    "strike": float,
                    "openInterest": int,
                    "volume": int,
                    "bid": float,
                    "ask": float,
                    "lastPrice": float,
                    "impliedVolatility": float
                })

                for r in slice_df.itertuples(index=False):
                    docs.append({
                        "ticker":              ticker_symbol,
                        "expiration_date":     exp_dt,
                        "option_type":         opt_type,
                        "strike":              r.strike,
                        "open_interest":       r.openInterest,
                        "volume":              r.volume,
                        "bid":                 r.bid,
                        "ask":                 r.ask,
                        "last_price":          r.lastPrice,
                        "implied_volatility":  r.impliedVolatility,
                        "stock_price":         stock_price,
                        "ts":                  now,
                        "time_to_expiry_days": tte_days
                    })

        if not docs:
            logging.error("No option data rows for %s", ticker_symbol)
            return None

        return docs

    except Exception:
        logging.exception("Fatal error fetching options for %s", ticker_symbol)
        return None

