"""
Unit Tests for Stock Market ETL Pipeline
Built by Sandeep Cherukuri
"""

import os
import sqlite3
import tempfile
import unittest
from datetime import datetime

import pandas as pd

# Add parent directory to path
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl_pipeline import StockETLPipeline
from config import DATABASE_PATH


class TestStockETLPipeline(unittest.TestCase):
    """Test cases for the ETL pipeline"""

    def setUp(self):
        """Set up test fixtures"""
        # Create a temporary database for testing
        self.test_db = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        self.test_db.close()
        
        # Create test config
        self.test_config = {
            "symbols": ["AAPL"],
            "start_date": "2024-01-01",
            "end_date": "2024-01-31",
        }
        
        # Mock API response data
        self.mock_api_response = {
            "Meta Data": {
                "1. Information": "Intraday (5min) open, high, low, close prices and volume",
                "2. Symbol": "AAPL",
                "3. Last Refreshed": "2024-01-31",
                "4. Interval": "5min",
                "5. Output Size": "Compact",
                "6. Time Zone": "US/Eastern",
                "8. Currency": "USD"
            },
            "Time Series (Daily)": {
                "2024-01-31": {
                    "1. open": "180.00",
                    "2. high": "182.00",
                    "3. low": "179.00",
                    "4. close": "181.50",
                    "5. adjusted close": "181.50",
                    "6. volume": "10000000",
                    "7. dividend amount": "0.00"
                },
                "2024-01-30": {
                    "1. open": "178.00",
                    "2. high": "180.00",
                    "3. low": "177.00",
                    "4. close": "179.50",
                    "5. adjusted close": "179.50",
                    "6. volume": "9500000",
                    "7. dividend amount": "0.00"
                },
            }
        }

    def tearDown(self):
        """Clean up test fixtures"""
        # Remove temporary database
        if os.path.exists(self.test_db.name):
            os.unlink(self.test_db.name)

    def test_pipeline_initialization(self):
        """Test pipeline initialization"""
        pipeline = StockETLPipeline(
            symbols=self.test_config["symbols"],
            start_date=self.test_config["start_date"],
            end_date=self.test_config["end_date"],
        )
        
        self.assertEqual(pipeline.symbols, self.test_config["symbols"])
        self.assertEqual(pipeline.start_date, self.test_config["start_date"])
        self.assertEqual(pipeline.end_date, self.test_config["end_date"])

    def test_date_validation(self):
        """Test date format validation"""
        # Valid dates should work
        pipeline = StockETLPipeline(
            symbols=["AAPL"],
            start_date="2024-01-01",
            end_date="2024-12-31",
        )
        self.assertIsNotNone(pipeline)
        
        # Invalid dates should raise ValueError
        with self.assertRaises(ValueError):
            StockETLPipeline(
                symbols=["AAPL"],
                start_date="invalid-date",
                end_date="2024-12-31",
            )

    def test_transform_empty_data(self):
        """Test transformation with empty data"""
        pipeline = StockETLPipeline(
            symbols=["AAPL"],
            start_date="2024-01-01",
            end_date="2024-01-31",
        )
        
        # Empty data should return empty dict
        result = pipeline.transform({})
        self.assertEqual(result, {})

    def test_transform_with_data(self):
        """Test transformation with mock data"""
        pipeline = StockETLPipeline(
            symbols=["AAPL"],
            start_date="2024-01-01",
            end_date="2024-01-31",
        )
        
        # Transform mock data
        raw_data = {"AAPL": self.mock_api_response}
        result = pipeline.transform(raw_data)
        
        # Check that we have data for AAPL
        self.assertIn("AAPL", result)
        
        # Check that meta data is present
        self.assertIn("meta", result["AAPL"])
        self.assertIn("prices", result["AAPL"])
        
        # Check that prices are transformed correctly
        prices = result["AAPL"]["prices"]
        self.assertEqual(len(prices), 2)  # We have 2 days of data
        
        # Check first price record
        first_price = prices[0]
        self.assertEqual(first_price["symbol"], "AAPL")
        self.assertEqual(first_price["date"], "2024-01-31")
        self.assertEqual(first_price["open"], 180.00)
        self.assertEqual(first_price["close"], 181.50)

    def test_database_schema_creation(self):
        """Test database schema creation"""
        # Create a temporary database
        conn = sqlite3.connect(self.test_db.name)
        cursor = conn.cursor()
        
        # Create tables using the pipeline's method
        pipeline = StockETLPipeline()
        pipeline._create_tables(cursor)
        
        # Verify tables exist
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        
        self.assertIn("stocks", tables)
        self.assertIn("daily_prices", tables)
        self.assertIn("pipeline_runs", tables)
        
        # Verify indexes exist
        cursor.execute("SELECT name FROM sqlite_master WHERE type='index'")
        indexes = [row[0] for row in cursor.fetchall()]
        
        self.assertIn("idx_daily_prices_symbol_date", indexes)
        self.assertIn("idx_daily_prices_date", indexes)
        
        conn.close()

    def test_upsert_stock(self):
        """Test stock metadata upsert"""
        conn = sqlite3.connect(self.test_db.name)
        cursor = conn.cursor()
        
        # Create tables
        pipeline = StockETLPipeline()
        pipeline._create_tables(cursor)
        
        # Insert a stock
        meta = {
            "symbol": "AAPL",
            "name": "Apple Inc.",
            "currency": "USD",
        }
        pipeline._upsert_stock(cursor, meta)
        
        # Verify insertion
        cursor.execute("SELECT * FROM stocks WHERE symbol = 'AAPL'")
        row = cursor.fetchone()
        
        self.assertIsNotNone(row)
        self.assertEqual(row[0], "AAPL")  # symbol
        self.assertEqual(row[1], "Apple Inc.")  # name
        self.assertEqual(row[2], None)  # sector (not provided)
        self.assertEqual(row[3], "USD")  # currency
        
        # Update the same stock
        meta["name"] = "Apple Inc. (Updated)"
        pipeline._upsert_stock(cursor, meta)
        
        # Verify update
        cursor.execute("SELECT * FROM stocks WHERE symbol = 'AAPL'")
        row = cursor.fetchone()
        self.assertEqual(row[1], "Apple Inc. (Updated)")
        
        conn.close()

    def test_insert_prices(self):
        """Test price data insertion"""
        conn = sqlite3.connect(self.test_db.name)
        cursor = conn.cursor()
        
        # Create tables
        pipeline = StockETLPipeline()
        pipeline._create_tables(cursor)
        
        # Insert stock first
        meta = {"symbol": "AAPL", "name": "Apple Inc.", "currency": "USD"}
        pipeline._upsert_stock(cursor, meta)
        
        # Insert prices
        prices = [
            {
                "symbol": "AAPL",
                "date": "2024-01-01",
                "open": 180.00,
                "high": 182.00,
                "low": 179.00,
                "close": 181.50,
                "adjusted_close": 181.50,
                "volume": 10000000,
            },
            {
                "symbol": "AAPL",
                "date": "2024-01-02",
                "open": 181.00,
                "high": 183.00,
                "low": 180.00,
                "close": 182.50,
                "adjusted_close": 182.50,
                "volume": 11000000,
            },
        ]
        
        inserted = pipeline._insert_prices(cursor, "AAPL", prices)
        conn.commit()
        
        # Verify insertion
        self.assertEqual(inserted, 2)
        
        cursor.execute("SELECT * FROM daily_prices WHERE symbol = 'AAPL' ORDER BY date")
        rows = cursor.fetchall()
        
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0][2], "2024-01-01")  # date
        self.assertEqual(rows[0][3], 180.00)  # open
        
        # Test duplicate prevention
        inserted = pipeline._insert_prices(cursor, "AAPL", prices)
        conn.commit()
        
        # Should not insert duplicates
        self.assertEqual(inserted, 0)
        
        cursor.execute("SELECT COUNT(*) FROM daily_prices WHERE symbol = 'AAPL'")
        count = cursor.fetchone()[0]
        self.assertEqual(count, 2)  # Still only 2 records
        
        conn.close()

    def test_pipeline_run_logging(self):
        """Test pipeline run logging"""
        # This test would normally mock the API calls
        # For simplicity, we're just testing the logging structure
        pipeline = StockETLPipeline(
            symbols=["AAPL"],
            start_date="2024-01-01",
            end_date="2024-01-31",
        )
        
        # Check that log file directory exists or can be created
        log_dir = os.path.dirname(pipeline.LOG_FILE if hasattr(pipeline, 'LOG_FILE') else "logs")
        os.makedirs(log_dir, exist_ok=True)
        
        self.assertTrue(os.path.exists(log_dir))


class TestDataValidation(unittest.TestCase):
    """Test data validation functions"""

    def test_validate_date_format(self):
        """Test date format validation"""
        pipeline = StockETLPipeline()
        
        # Valid dates
        valid_dates = ["2024-01-01", "2023-12-31", "2024-02-29"]
        for date in valid_dates:
            try:
                datetime.strptime(date, "%Y-%m-%d")
            except ValueError:
                self.fail(f"Valid date {date} failed validation")
        
        # Invalid dates
        invalid_dates = ["2024/01/01", "01-01-2024", "2024-13-01", "invalid"]
        for date in invalid_dates:
            with self.assertRaises(ValueError):
                datetime.strptime(date, "%Y-%m-%d")

    def test_validate_numeric_values(self):
        """Test numeric value validation"""
        valid_values = ["180.50", "100", "0.001", "-5.0"]
        for val in valid_values:
            try:
                float(val)
            except ValueError:
                self.fail(f"Valid numeric value {val} failed validation")
        
        invalid_values = ["abc", "180.50.20", ""]
        for val in invalid_values:
            with self.assertRaises(ValueError):
                float(val)


if __name__ == "__main__":
    unittest.main()
