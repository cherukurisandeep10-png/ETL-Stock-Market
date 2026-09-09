"""
Configuration for Stock Market ETL Pipeline
Built by Sandeep Cherukuri
"""

# API Configuration
# Get your free API key from: https://www.alphavantage.co/support/#api-key
ALPHA_VANTAGE_API_KEY = "YOUR_API_KEY_HERE"
BASE_URL = "https://www.alphavantage.co/query"

# Database Configuration
DATABASE_PATH = "data/stocks.db"

# Default Stock Symbols
DEFAULT_SYMBOLS = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]

# Date Range Configuration
# Format: YYYY-MM-DD
DEFAULT_START_DATE = "2024-01-01"
DEFAULT_END_DATE = "2024-12-31"

# Logging Configuration
LOG_FILE = "logs/etl_pipeline.log"
LOG_LEVEL = "INFO"  # Options: DEBUG, INFO, WARNING, ERROR, CRITICAL

# API Rate Limit (Free tier: 5 requests per minute)
API_RATE_LIMIT = 5  # requests per minute
API_REQUEST_DELAY = 13  # seconds between requests (60/5 + buffer)

# Data Quality Configuration
MIN_RECORDS_PER_SYMBOL = 10  # Minimum records to consider valid
MAX_NULL_PERCENTAGE = 0.1  # 10% null values allowed
