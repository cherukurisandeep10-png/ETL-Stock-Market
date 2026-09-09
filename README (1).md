# Stock Market Data ETL Pipeline

A production-ready **Extract, Transform, Load (ETL)** pipeline for stock market data. This project fetches historical and real-time stock data from Alpha Vantage API, cleans and transforms it, and loads it into a SQLite database for analysis and visualization.

## Built by Sandeep Cherukuri

---

## Features

- **Extract:** Fetches stock data from Alpha Vantage API (free tier available)
- **Transform:** Cleans, validates, and enriches the data
- **Load:** Stores data in SQLite database with proper schema
- **Automate:** Scheduled runs using cron or manual execution
- **Monitor:** Logging system to track pipeline execution
- **Analyze:** Basic analytics and visualization capabilities

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Alpha      │────▶│   Extract    │────▶│   Transform  │────▶│    Load     │
│   Vantage    │     │   (API)      │     │   (Clean)    │     │   (SQLite)  │
│    API       │     └─────────────┘     └─────────────┘     └─────────────┘
└─────────────┘                                                          
                                                                       ▼
                                 ┌─────────────────────┐
                                 │   SQLite Database    │
                                 │   (stocks.db)        │
                                 └─────────────────────┘
                                       ▼
                              ┌─────────────────┐
                              │   Analysis &     │
                              │   Visualization  │
                              └─────────────────┘
```

## Technologies Used

- **Python 3.8+**
- **Requests** - HTTP requests to API
- **Pandas** - Data manipulation and transformation
- **SQLite3** - Database storage
- **Logging** - Pipeline monitoring
- **Matplotlib/Seaborn** - Data visualization (optional)
- **Cron** - Scheduling (optional)

## Project Structure

```
sandeep-stock-etl/
├── README.md                 # Project documentation
├── LICENSE                  # MIT License
├── requirements.txt          # Python dependencies
├── config.py                # Configuration settings
├── etl_pipeline.py          # Main ETL script
├── database_schema.sql      # Database schema definition
├── data/
│   └── stocks.db            # SQLite database (generated)
├── logs/
│   └── etl_pipeline.log     # Pipeline logs (generated)
├── scripts/
│   └── analyze.py            # Data analysis script
└── tests/
    └── test_etl.py           # Unit tests
```

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Get API Key

1. Go to [https://www.alphavantage.co/support/#api-key](https://www.alphavantage.co/support/#api-key)
2. Get a **free API key** (no credit card required)
3. Add it to `config.py`:
   ```python
   ALPHA_VANTAGE_API_KEY = "YOUR_API_KEY_HERE"
   ```

### 3. Run the Pipeline

```bash
# Run for a single stock
python etl_pipeline.py --symbol AAPL

# Run for multiple stocks
python etl_pipeline.py --symbol AAPL,MSFT,GOOGL,AMZN,TSLA

# Run with custom date range
python etl_pipeline.py --symbol AAPL --start 2024-01-01 --end 2024-12-31

# Run with all defaults
python etl_pipeline.py
```

### 4. Analyze the Data

```bash
python scripts/analyze.py
```

This will generate:
- Summary statistics
- Price trend visualizations
- Volume analysis

## Configuration

Edit `config.py` to customize:

```python
# API Settings
ALPHA_VANTAGE_API_KEY = "YOUR_API_KEY"
BASE_URL = "https://www.alphavantage.co/query"

# Database Settings
DATABASE_PATH = "data/stocks.db"

# Default Stocks
DEFAULT_SYMBOLS = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]

# Date Range
DEFAULT_START_DATE = "2024-01-01"
DEFAULT_END_DATE = "2024-12-31"

# Logging
LOG_FILE = "logs/etl_pipeline.log"
LOG_LEVEL = "INFO"
```

## Database Schema

The pipeline creates these tables:

### 1. `stocks` - Metadata about each stock
- `symbol` (TEXT, PRIMARY KEY) - Stock ticker symbol
- `name` (TEXT) - Company name
- `sector` (TEXT) - Industry sector
- `currency` (TEXT) - Trading currency
- `last_updated` (DATETIME) - Last update timestamp

### 2. `daily_prices` - Daily price data
- `id` (INTEGER, PRIMARY KEY) - Auto-increment ID
- `symbol` (TEXT, FOREIGN KEY) - Stock symbol
- `date` (DATE) - Trading date
- `open` (REAL) - Opening price
- `high` (REAL) - Highest price
- `low` (REAL) - Lowest price
- `close` (REAL) - Closing price
- `adjusted_close` (REAL) - Adjusted closing price
- `volume` (INTEGER) - Trading volume
- `created_at` (DATETIME) - When record was created

### 3. `pipeline_runs` - ETL execution history
- `id` (INTEGER, PRIMARY KEY)
- `start_time` (DATETIME)
- `end_time` (DATETIME)
- `status` (TEXT) - success/failure
- `symbols_processed` (INTEGER)
- `records_inserted` (INTEGER)
- `error_message` (TEXT)

## Automation

### Using Cron (Linux/macOS)

```bash
# Edit crontab
crontab -e

# Add this line to run daily at 6 PM
0 18 * * * /usr/bin/python3 /path/to/sandeep-stock-etl/etl_pipeline.py --symbol AAPL,MSFT,GOOGL >> /path/to/sandeep-stock-etl/logs/cron.log 2>&1
```

### Using Task Scheduler (Windows)

1. Create a batch file `run_pipeline.bat`:
   ```batch
   python C:\path\to\sandeep-stock-etl\etl_pipeline.py --symbol AAPL,MSFT,GOOGL
   ```
2. Schedule it to run daily using Task Scheduler

## Data Analysis

The `scripts/analyze.py` script provides:

- **Summary Statistics:** Mean, median, std dev, min, max for each stock
- **Price Trends:** Line charts of closing prices
- **Volume Analysis:** Bar charts of trading volumes
- **Moving Averages:** 7-day and 30-day moving averages
- **Correlation Matrix:** Relationships between stocks
- **Returns Analysis:** Daily and cumulative returns

Run it with:
```bash
python scripts/analyze.py --symbol AAPL --start 2024-01-01 --end 2024-12-31
```

## Testing

Run the test suite:
```bash
python -m pytest tests/test_etl.py -v
```

Tests include:
- API connection testing
- Data validation
- Database operations
- Error handling

## Error Handling

The pipeline handles:
- API rate limits (5 requests per minute for free tier)
- Network errors
- Invalid data
- Database errors
- Missing files/directories

All errors are logged to `logs/etl_pipeline.log`

## Deployment Options

### 1. Local Development
Just run the scripts locally as shown above.

### 2. Docker Container
Create a Dockerfile for easy deployment:
```dockerfile
FROM python:3.9-slim
WORKDIR /app
COPY . .
RUN pip install -r requirements.txt
CMD ["python", "etl_pipeline.py"]
```

### 3. Cloud Deployment
- **AWS Lambda** - Serverless execution
- **Google Cloud Functions** - Event-driven
- **Azure Functions** - Microsoft cloud

## Future Enhancements

- [ ] Add more data sources (Yahoo Finance, IEX Cloud)
- [ ] Implement incremental loading (only fetch new data)
- [ ] Add email notifications for failures
- [ ] Create a web dashboard for visualization
- [ ] Add real-time data streaming
- [ ] Implement data quality checks
- [ ] Add support for cryptocurrencies

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

**Built with passion by Sandeep Cherukuri**

For questions or feedback, please contact: sandeep.cherukuri@example.com
