-- Stock Market Data Database Schema
-- Built by Sandeep Cherukuri

-- ============================================
-- TABLE: stocks
-- Purpose: Store metadata about each stock/company
-- ============================================
CREATE TABLE IF NOT EXISTS stocks (
    symbol TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    sector TEXT,
    currency TEXT NOT NULL DEFAULT 'USD',
    last_updated DATETIME
);

-- ============================================
-- TABLE: daily_prices
-- Purpose: Store daily price and volume data
-- ============================================
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
    dividend_amount REAL DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (symbol) REFERENCES stocks(symbol) ON DELETE CASCADE
);

-- ============================================
-- INDEXES: For faster query performance
-- ============================================

-- Index for symbol and date (most common query pattern)
CREATE INDEX IF NOT EXISTS idx_daily_prices_symbol_date 
ON daily_prices(symbol, date);

-- Index for date only (for time-based queries)
CREATE INDEX IF NOT EXISTS idx_daily_prices_date 
ON daily_prices(date);

-- Index for symbol only
CREATE INDEX IF NOT EXISTS idx_daily_prices_symbol 
ON daily_prices(symbol);

-- ============================================
-- TABLE: pipeline_runs
-- Purpose: Track ETL pipeline execution history
-- ============================================
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    start_time DATETIME NOT NULL,
    end_time DATETIME NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('success', 'failure')),
    symbols_processed INTEGER DEFAULT 0,
    records_inserted INTEGER DEFAULT 0,
    error_message TEXT
);

-- ============================================
-- VIEWS: For common queries
-- ============================================

-- View: Latest prices for all stocks
CREATE VIEW IF NOT EXISTS vw_latest_prices AS
SELECT 
    s.symbol,
    s.name,
    dp.date,
    dp.close,
    dp.volume,
    dp.high,
    dp.low
FROM stocks s
JOIN daily_prices dp ON s.symbol = dp.symbol
WHERE dp.date = (
    SELECT MAX(date) 
    FROM daily_prices dp2 
    WHERE dp2.symbol = dp.symbol
);

-- View: Price changes from previous day
CREATE VIEW IF NOT EXISTS vw_daily_changes AS
SELECT 
    s.symbol,
    s.name,
    dp1.date,
    dp1.close AS current_close,
    dp2.close AS previous_close,
    (dp1.close - dp2.close) AS absolute_change,
    ROUND(((dp1.close - dp2.close) / dp2.close) * 100, 2) AS percentage_change,
    dp1.volume
FROM stocks s
JOIN daily_prices dp1 ON s.symbol = dp1.symbol
JOIN daily_prices dp2 ON s.symbol = dp2.symbol 
    AND dp2.date = (
        SELECT MAX(date)
        FROM daily_prices dp3
        WHERE dp3.symbol = dp1.symbol AND dp3.date < dp1.date
    )
ORDER BY dp1.date DESC, s.symbol;

-- View: Moving averages
CREATE VIEW IF NOT EXISTS vw_moving_averages AS
WITH ranked_prices AS (
    SELECT 
        symbol,
        date,
        close,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date) AS row_num
    FROM daily_prices
)
SELECT 
    symbol,
    date,
    close,
    AVG(close) OVER (
        PARTITION BY symbol 
        ORDER BY date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS ma_7,
    AVG(close) OVER (
        PARTITION BY symbol 
        ORDER BY date 
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS ma_30
FROM ranked_prices
WHERE row_num >= 7;

-- ============================================
-- TRIGGERS: For automatic updates
-- ============================================

-- Trigger: Update last_updated when stock data is inserted
CREATE TRIGGER IF NOT EXISTS trg_update_stock_timestamp 
AFTER INSERT ON daily_prices
FOR EACH ROW
BEGIN
    UPDATE stocks 
    SET last_updated = CURRENT_TIMESTAMP 
    WHERE symbol = NEW.symbol;
END;

-- ============================================
-- SAMPLE QUERIES
-- ============================================

-- Get all stocks with their latest price
-- SELECT * FROM vw_latest_prices;

-- Get daily changes for a specific stock
-- SELECT * FROM vw_daily_changes WHERE symbol = 'AAPL' ORDER BY date DESC;

-- Get moving averages for a stock
-- SELECT * FROM vw_moving_averages WHERE symbol = 'AAPL' ORDER BY date DESC;

-- Get pipeline run history
-- SELECT * FROM pipeline_runs ORDER BY start_time DESC;

-- Get price history for a stock
-- SELECT date, open, high, low, close, volume 
-- FROM daily_prices 
-- WHERE symbol = 'AAPL' 
-- ORDER BY date DESC 
-- LIMIT 100;
