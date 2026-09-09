#!/usr/bin/env python3
"""
Stock Market Data ETL Pipeline

Extract: Fetch stock data from Alpha Vantage API
Transform: Clean, validate, and enrich the data
Load: Store data in SQLite database

Built by Sandeep Cherukuri
"""

import argparse
import json
import logging
import os
import sqlite3
import sys
import time
from datetime import datetime, timedelta

import pandas as pd
import requests

# Import configuration
from config import (
    ALPHA_VANTAGE_API_KEY,
    BASE_URL,
    DATABASE_PATH,
    DEFAULT_END_DATE,
    DEFAULT_START_DATE,
    DEFAULT_SYMBOLS,
    LOG_FILE,
    LOG_LEVEL,
)


# Configure logging
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


class StockETLPipeline:
    """Main ETL Pipeline class"""

    def __init__(self, symbols=None, start_date=None, end_date=None):
        """
        Initialize the ETL pipeline
        
        Args:
            symbols (list): List of stock symbols to process
            start_date (str): Start date in YYYY-MM-DD format
            end_date (str): End date in YYYY-MM-DD format
        """
        self.symbols = symbols or DEFAULT_SYMBOLS
        self.start_date = start_date or DEFAULT_START_DATE
        self.end_date = end_date or DEFAULT_END_DATE
        self.run_id = None
        self.db_conn = None
        
        # Validate dates
        try:
            datetime.strptime(self.start_date, "%Y-%m-%d")
            datetime.strptime(self.end_date, "%Y-%m-%d")
        except ValueError as e:
            logger.error(f"Invalid date format: {e}")
            raise

    def run(self):
        """Run the complete ETL pipeline"""
        start_time = datetime.now()
        logger.info("Starting ETL Pipeline")
        logger.info(f"Processing symbols: {', '.join(self.symbols)}")
        logger.info(f"Date range: {self.start_date} to {self.end_date}")

        # Record pipeline start
        self._log_pipeline_start(start_time)

        try:
            # Extract
            logger.info("Starting EXTRACTION phase")
            raw_data = self.extract()
            logger.info(f"Extracted data for {len(raw_data)} symbols")

            # Transform
            logger.info("Starting TRANSFORMATION phase")
            transformed_data = self.transform(raw_data)
            logger.info(f"Transformed data for {len(transformed_data)} symbols")

            # Load
            logger.info("Starting LOAD phase")
            load_result = self.load(transformed_data)
            logger.info(f"Loaded {load_result['records_inserted']} records")

            # Mark as successful
            end_time = datetime.now()
            self._log_pipeline_success(start_time, end_time, load_result)
            logger.info("ETL Pipeline completed successfully!")
            
            return True, load_result

        except Exception as e:
            end_time = datetime.now()
            self._log_pipeline_failure(start_time, end_time, str(e))
            logger.error(f"ETL Pipeline failed: {e}", exc_info=True)
            return False, {"error": str(e)}

    def extract(self):
        """
        Extract stock data from Alpha Vantage API
        
        Returns:
            dict: Raw data for each symbol
        """
        raw_data = {}
        
        for symbol in self.symbols:
            logger.info(f"Fetching data for {symbol}...")
            
            try:
                # Fetch daily adjusted prices
                params = {
                    "function": "TIME_SERIES_DAILY_ADJUSTED",
                    "symbol": symbol,
                    "apikey": ALPHA_VANTAGE_API_KEY,
                    "outputsize": "full",
                }
                
                response = requests.get(BASE_URL, params=params, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                
                # Check for API errors
                if "Error Message" in data:
                    logger.warning(f"API Error for {symbol}: {data.get('Error Message')}")
                    # Rate limit - wait and retry
                    if "rate limit" in data.get("Error Message", "").lower():
                        logger.info("Rate limit hit, waiting 60 seconds...")
                        time.sleep(65)
                        continue
                    continue
                
                # Check if we have data
                if "Time Series (Daily)" not in data:
                    logger.warning(f"No time series data for {symbol}")
                    continue
                
                raw_data[symbol] = data
                logger.info(f"Successfully fetched data for {symbol}")
                
                # Respect API rate limit (5 requests per minute for free tier)
                time.sleep(13)  # ~12 seconds between requests
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to fetch data for {symbol}: {e}")
                continue
        
        return raw_data

    def transform(self, raw_data):
        """
        Transform raw API data into clean, structured format
        
        Args:
            raw_data (dict): Raw data from API
            
        Returns:
            dict: Transformed data ready for loading
        """
        transformed_data = {}
        
        for symbol, data in raw_data.items():
            logger.info(f"Transforming data for {symbol}...")
            
            try:
                time_series = data.get("Time Series (Daily)", {})
                meta_data = data.get("Meta Data", {})
                
                # Get company info
                company_name = meta_data.get("2. Symbol", symbol)
                currency = meta_data.get("8. Currency", "USD")
                
                # Convert to DataFrame
                records = []
                for date_str, values in time_series.items():
                    # Convert date string to datetime
                    try:
                        date = datetime.strptime(date_str, "%Y-%m-%d")
                    except ValueError:
                        continue
                    
                    # Filter by date range
                    if date < datetime.strptime(self.start_date, "%Y-%m-%d") or \
                       date > datetime.strptime(self.end_date, "%Y-%m-%d"):
                        continue
                    
                    record = {
                        "symbol": symbol,
                        "date": date_str,
                        "open": float(values.get("1. open", 0)),
                        "high": float(values.get("2. high", 0)),
                        "low": float(values.get("3. low", 0)),
                        "close": float(values.get("4. close", 0)),
                        "adjusted_close": float(values.get("5. adjusted close", 0)),
                        "volume": int(values.get("6. volume", 0)),
                        "dividend_amount": float(values.get("7. dividend amount", 0)),
                    }
                    records.append(record)
                
                if records:
                    transformed_data[symbol] = {
                        "meta": {
                            "symbol": symbol,
                            "name": company_name,
                            "currency": currency,
                        },
                        "prices": records,
                    }
                    logger.info(f"Transformed {len(records)} records for {symbol}")
                else:
                    logger.warning(f"No records in date range for {symbol}")
                    
            except Exception as e:
                logger.error(f"Failed to transform data for {symbol}: {e}")
                continue
        
        return transformed_data

    def load(self, transformed_data):
        """
        Load transformed data into SQLite database
        
        Args:
            transformed_data (dict): Transformed data to load
            
        Returns:
            dict: Load statistics
        """
        # Ensure data directory exists
        os.makedirs(os.path.dirname(DATABASE_PATH), exist_ok=True)
        
        # Connect to database
        self.db_conn = sqlite3.connect(DATABASE_PATH)
        cursor = self.db_conn.cursor()
        
        try:
            # Create tables if they don't exist
            self._create_tables(cursor)
            
            records_inserted = 0
            symbols_processed = 0
            
            for symbol, data in transformed_data.items():
                logger.info(f"Loading data for {symbol}...")
                
                try:
                    # Insert or update stock metadata
                    meta = data["meta"]
                    self._upsert_stock(cursor, meta)
                    
                    # Insert price data
                    prices = data["prices"]
                    inserted = self._insert_prices(cursor, symbol, prices)
                    records_inserted += inserted
                    
                    symbols_processed += 1
                    logger.info(f"Loaded {inserted} records for {symbol}")
                    
                except Exception as e:
                    logger.error(f"Failed to load data for {symbol}: {e}")
                    self.db_conn.rollback()
                    continue
            
            # Commit changes
            self.db_conn.commit()
            
            return {
                "symbols_processed": symbols_processed,
                "records_inserted": records_inserted,
            }
            
        except Exception as e:
            self.db_conn.rollback()
            raise e
        finally:
            if self.db_conn:
                self.db_conn.close()

    def _create_tables(self, cursor):
        """Create database tables if they don't exist"""
        # Stocks table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stocks (
                symbol TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                sector TEXT,
                currency TEXT NOT NULL DEFAULT 'USD',
                last_updated DATETIME
            )
        """)
        
        # Daily prices table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS daily_prices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                date DATE NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                adjusted_close REAL NOT NULL,
                volume INTEGER NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (symbol) REFERENCES stocks(symbol)
            )
        """)
        
        # Create index for faster queries
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_daily_prices_symbol_date ON daily_prices(symbol, date)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_daily_prices_date ON daily_prices(date)")
        
        # Pipeline runs table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                start_time DATETIME NOT NULL,
                end_time DATETIME NOT NULL,
                status TEXT NOT NULL,
                symbols_processed INTEGER,
                records_inserted INTEGER,
                error_message TEXT
            )
        """)
        
        logger.info("Database tables created/verified")

    def _upsert_stock(self, cursor, meta):
        """Insert or update stock metadata"""
        symbol = meta["symbol"]
        name = meta.get("name", symbol)
        currency = meta.get("currency", "USD")
        
        cursor.execute("""
            INSERT OR REPLACE INTO stocks (symbol, name, currency, last_updated)
            VALUES (?, ?, ?, ?)
        """, (symbol, name, currency, datetime.now().isoformat()))

    def _insert_prices(self, cursor, symbol, prices):
        """Insert price data, avoiding duplicates"""
        # Check for existing records to avoid duplicates
        existing_dates = set()
        cursor.execute("SELECT date FROM daily_prices WHERE symbol = ?", (symbol,))
        for row in cursor.fetchall():
            existing_dates.add(row[0])
        
        new_records = []
        for price in prices:
            if price["date"] not in existing_dates:
                new_records.append(price)
        
        if new_records:
            cursor.executemany("""
                INSERT INTO daily_prices (
                    symbol, date, open, high, low, close, 
                    adjusted_close, volume
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, [(
                r["symbol"],
                r["date"],
                r["open"],
                r["high"],
                r["low"],
                r["close"],
                r["adjusted_close"],
                r["volume"],
            ) for r in new_records])
        
        return len(new_records)

    def _log_pipeline_start(self, start_time):
        """Log pipeline start"""
        os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
        self.run_id = start_time.timestamp()

    def _log_pipeline_success(self, start_time, end_time, result):
        """Log successful pipeline run"""
        if self.db_conn:
            cursor = self.db_conn.cursor()
            cursor.execute("""
                INSERT INTO pipeline_runs (
                    start_time, end_time, status, 
                    symbols_processed, records_inserted
                ) VALUES (?, ?, ?, ?, ?)
            """, (
                start_time.isoformat(),
                end_time.isoformat(),
                "success",
                result.get("symbols_processed", 0),
                result.get("records_inserted", 0),
            ))
            self.db_conn.commit()

    def _log_pipeline_failure(self, start_time, end_time, error_message):
        """Log failed pipeline run"""
        if self.db_conn:
            cursor = self.db_conn.cursor()
            cursor.execute("""
                INSERT INTO pipeline_runs (
                    start_time, end_time, status, 
                    symbols_processed, records_inserted, error_message
                ) VALUES (?, ?, ?, ?, ?, ?)
            """, (
                start_time.isoformat(),
                end_time.isoformat(),
                "failure",
                0,
                0,
                error_message,
            ))
            self.db_conn.commit()


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Stock Market Data ETL Pipeline - Built by Sandeep Cherukuri"
    )
    parser.add_argument(
        "--symbol", "-s",
        type=str,
        default=",".join(DEFAULT_SYMBOLS),
        help="Comma-separated list of stock symbols (default: AAPL,MSFT,GOOGL,AMZN,TSLA)",
    )
    parser.add_argument(
        "--start",
        type=str,
        default=DEFAULT_START_DATE,
        help="Start date in YYYY-MM-DD format (default: 2024-01-01)",
    )
    parser.add_argument(
        "--end",
        type=str,
        default=DEFAULT_END_DATE,
        help="End date in YYYY-MM-DD format (default: 2024-12-31)",
    )
    
    args = parser.parse_args()
    
    # Parse symbols
    symbols = [s.strip().upper() for s in args.symbol.split(",") if s.strip()]
    
    # Create and run pipeline
    pipeline = StockETLPipeline(
        symbols=symbols,
        start_date=args.start,
        end_date=args.end,
    )
    
    success, result = pipeline.run()
    
    if not success:
        logger.error(f"Pipeline failed: {result.get('error', 'Unknown error')}")
        sys.exit(1)
    
    logger.info("Pipeline completed successfully!")
    logger.info(f"Symbols processed: {result.get('symbols_processed', 0)}")
    logger.info(f"Records inserted: {result.get('records_inserted', 0)}")


if __name__ == "__main__":
    main()
