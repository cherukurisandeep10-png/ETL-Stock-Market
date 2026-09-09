#!/usr/bin/env python3
"""
Stock Market Data Analysis Script

Provides statistical analysis and visualization of stock data
from the ETL pipeline database.

Built by Sandeep Cherukuri
"""

import argparse
import os
import sys
from datetime import datetime

import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend for saving plots
import matplotlib.pyplot as plt
import pandas as pd
import sqlite3

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DATABASE_PATH


class StockAnalyzer:
    """Stock data analyzer"""

    def __init__(self, db_path=None):
        """
        Initialize the analyzer
        
        Args:
            db_path (str): Path to SQLite database
        """
        self.db_path = db_path or DATABASE_PATH
        self.conn = None

    def connect(self):
        """Connect to the database"""
        if not os.path.exists(self.db_path):
            raise FileNotFoundError(f"Database not found: {self.db_path}")
        
        self.conn = sqlite3.connect(self.db_path)
        return self.conn

    def close(self):
        """Close the database connection"""
        if self.conn:
            self.conn.close()
            self.conn = None

    def get_available_symbols(self):
        """Get list of available stock symbols"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT symbol FROM stocks ORDER BY symbol")
        return [row[0] for row in cursor.fetchall()]

    def get_price_data(self, symbol, start_date=None, end_date=None):
        """
        Get price data for a stock
        
        Args:
            symbol (str): Stock symbol
            start_date (str): Start date in YYYY-MM-DD format
            end_date (str): End date in YYYY-MM-DD format
            
        Returns:
            DataFrame: Price data
        """
        query = """
            SELECT date, open, high, low, close, adjusted_close, volume
            FROM daily_prices
            WHERE symbol = ?
        """
        params = [symbol]
        
        if start_date:
            query += " AND date >= ?"
            params.append(start_date)
        
        if end_date:
            query += " AND date <= ?"
            params.append(end_date)
        
        query += " ORDER BY date"
        
        df = pd.read_sql_query(query, self.conn, params=params)
        return df

    def get_multiple_stocks_data(self, symbols, start_date=None, end_date=None):
        """
        Get price data for multiple stocks
        
        Args:
            symbols (list): List of stock symbols
            start_date (str): Start date in YYYY-MM-DD format
            end_date (str): End date in YYYY-MM-DD format
            
        Returns:
            dict: DataFrames keyed by symbol
        """
        data = {}
        for symbol in symbols:
            df = self.get_price_data(symbol, start_date, end_date)
            if not df.empty:
                data[symbol] = df
        return data

    def calculate_summary_statistics(self, df):
        """
        Calculate summary statistics for a stock
        
        Args:
            df (DataFrame): Price data
            
        Returns:
            dict: Summary statistics
        """
        if df.empty:
            return {}
        
        stats = {
            "symbol": df.name if hasattr(df, 'name') else "Unknown",
            "start_date": df["date"].min(),
            "end_date": df["date"].max(),
            "num_days": len(df),
            "open_mean": df["open"].mean(),
            "open_std": df["open"].std(),
            "close_mean": df["close"].mean(),
            "close_std": df["close"].std(),
            "high_mean": df["high"].mean(),
            "low_mean": df["low"].mean(),
            "volume_mean": df["volume"].mean(),
            "volume_std": df["volume"].std(),
            "min_price": df["low"].min(),
            "max_price": df["high"].max(),
            "price_range": df["high"].max() - df["low"].min(),
        }
        
        # Calculate returns
        if len(df) > 1:
            df["daily_return"] = df["close"].pct_change()
            stats["avg_daily_return"] = df["daily_return"].mean()
            stats["volatility"] = df["daily_return"].std()
            stats["cumulative_return"] = (df["close"].iloc[-1] / df["close"].iloc[0] - 1) * 100
        
        return stats

    def calculate_moving_averages(self, df, window=20):
        """
        Calculate moving averages
        
        Args:
            df (DataFrame): Price data
            window (int): Moving average window size
            
        Returns:
            DataFrame: Data with moving averages added
        """
        df = df.copy()
        df["ma_7"] = df["close"].rolling(window=7).mean()
        df["ma_20"] = df["close"].rolling(window=20).mean()
        df["ma_50"] = df["close"].rolling(window=50).mean()
        return df

    def calculate_correlation(self, data):
        """
        Calculate correlation matrix for multiple stocks
        
        Args:
            data (dict): DataFrames keyed by symbol
            
        Returns:
            DataFrame: Correlation matrix
        """
        # Get closing prices for all stocks
        closes = {}
        for symbol, df in data.items():
            closes[symbol] = df.set_index("date")["close"]
        
        # Combine into a single DataFrame
        combined = pd.concat(closes, axis=1)
        
        # Calculate correlation
        return combined.corr()

    def plot_price_trend(self, df, symbol, output_dir="output"):
        """
        Plot price trend for a stock
        
        Args:
            df (DataFrame): Price data
            symbol (str): Stock symbol
            output_dir (str): Directory to save plot
        """
        os.makedirs(output_dir, exist_ok=True)
        
        plt.figure(figsize=(12, 6))
        plt.plot(df["date"], df["close"], label="Close Price", color="blue")
        plt.title(f"{symbol} - Price Trend")
        plt.xlabel("Date")
        plt.ylabel("Price ($)")
        plt.grid(True, alpha=0.3)
        plt.legend()
        plt.tight_layout()
        
        filename = os.path.join(output_dir, f"{symbol}_price_trend.png")
        plt.savefig(filename, dpi=300, bbox_inches="tight")
        plt.close()
        
        return filename

    def plot_moving_averages(self, df, symbol, output_dir="output"):
        """
        Plot price with moving averages
        
        Args:
            df (DataFrame): Price data with moving averages
            symbol (str): Stock symbol
            output_dir (str): Directory to save plot
        """
        os.makedirs(output_dir, exist_ok=True)
        
        plt.figure(figsize=(12, 6))
        plt.plot(df["date"], df["close"], label="Close Price", color="blue", alpha=0.5)
        plt.plot(df["date"], df["ma_7"], label="7-Day MA", color="orange")
        plt.plot(df["date"], df["ma_20"], label="20-Day MA", color="green")
        plt.plot(df["date"], df["ma_50"], label="50-Day MA", color="red")
        plt.title(f"{symbol} - Price with Moving Averages")
        plt.xlabel("Date")
        plt.ylabel("Price ($)")
        plt.grid(True, alpha=0.3)
        plt.legend()
        plt.tight_layout()
        
        filename = os.path.join(output_dir, f"{symbol}_moving_averages.png")
        plt.savefig(filename, dpi=300, bbox_inches="tight")
        plt.close()
        
        return filename

    def plot_volume(self, df, symbol, output_dir="output"):
        """
        Plot trading volume
        
        Args:
            df (DataFrame): Price data
            symbol (str): Stock symbol
            output_dir (str): Directory to save plot
        """
        os.makedirs(output_dir, exist_ok=True)
        
        plt.figure(figsize=(12, 6))
        plt.bar(df["date"], df["volume"], color="purple", alpha=0.6)
        plt.title(f"{symbol} - Trading Volume")
        plt.xlabel("Date")
        plt.ylabel("Volume")
        plt.grid(True, alpha=0.3, axis="y")
        plt.tight_layout()
        
        filename = os.path.join(output_dir, f"{symbol}_volume.png")
        plt.savefig(filename, dpi=300, bbox_inches="tight")
        plt.close()
        
        return filename

    def plot_correlation_heatmap(self, corr_matrix, output_dir="output"):
        """
        Plot correlation heatmap
        
        Args:
            corr_matrix (DataFrame): Correlation matrix
            output_dir (str): Directory to save plot
        """
        os.makedirs(output_dir, exist_ok=True)
        
        plt.figure(figsize=(10, 8))
        plt.matshow(corr_matrix, fignum=1)
        plt.xticks(range(len(corr_matrix.columns)), corr_matrix.columns, rotation=45)
        plt.yticks(range(len(corr_matrix.columns)), corr_matrix.columns)
        plt.colorbar()
        plt.title("Stock Correlation Heatmap")
        plt.tight_layout()
        
        filename = os.path.join(output_dir, "correlation_heatmap.png")
        plt.savefig(filename, dpi=300, bbox_inches="tight")
        plt.close()
        
        return filename

    def generate_report(self, symbols, start_date=None, end_date=None, output_dir="output"):
        """
        Generate a comprehensive analysis report
        
        Args:
            symbols (list): List of stock symbols to analyze
            start_date (str): Start date in YYYY-MM-DD format
            end_date (str): End date in YYYY-MM-DD format
            output_dir (str): Directory to save outputs
        """
        self.connect()
        
        try:
            # Get data for all symbols
            data = self.get_multiple_stocks_data(symbols, start_date, end_date)
            
            if not data:
                print("No data found for the specified symbols and date range.")
                return
            
            # Create output directory
            os.makedirs(output_dir, exist_ok=True)
            
            # Generate summary statistics
            print("\n" + "="*80)
            print("STOCK ANALYSIS REPORT")
            print("="*80)
            print(f"Date Range: {start_date or 'All'} to {end_date or 'All'}")
            print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print("="*80 + "\n")
            
            # Individual stock analysis
            all_stats = []
            for symbol, df in data.items():
                print(f"\n{'='*80}")
                print(f"ANALYSIS FOR {symbol}")
                print("="*80)
                
                # Calculate statistics
                stats = self.calculate_summary_statistics(df)
                all_stats.append(stats)
                
                # Print statistics
                print(f"\nSummary Statistics:")
                print(f"  Period: {stats['start_date']} to {stats['end_date']}")
                print(f"  Trading Days: {stats['num_days']}")
                print(f"\nPrice Statistics:")
                print(f"  Opening Price (Mean): ${stats['open_mean']:.2f}")
                print(f"  Closing Price (Mean): ${stats['close_mean']:.2f}")
                print(f"  High (Mean): ${stats['high_mean']:.2f}")
                print(f"  Low (Mean): ${stats['low_mean']:.2f}")
                print(f"  Price Range: ${stats['min_price']:.2f} - ${stats['max_price']:.2f}")
                
                if 'avg_daily_return' in stats:
                    print(f"\nReturn Statistics:")
                    print(f"  Average Daily Return: {stats['avg_daily_return']*100:.2f}%")
                    print(f"  Volatility: {stats['volatility']*100:.2f}%")
                    print(f"  Cumulative Return: {stats['cumulative_return']:.2f}%")
                
                print(f"\nVolume Statistics:")
                print(f"  Average Volume: {stats['volume_mean']:,.0f}")
                print(f"  Volume Std Dev: {stats['volume_std']:,.0f}")
                
                # Calculate and plot moving averages
                df_ma = self.calculate_moving_averages(df)
                
                # Generate plots
                self.plot_price_trend(df, symbol, output_dir)
                self.plot_moving_averages(df_ma, symbol, output_dir)
                self.plot_volume(df, symbol, output_dir)
                
                print(f"\nPlots generated:")
                print(f"  - {symbol}_price_trend.png")
                print(f"  - {symbol}_moving_averages.png")
                print(f"  - {symbol}_volume.png")
            
            # Multi-stock analysis
            if len(data) > 1:
                print(f"\n{'='*80}")
                print("MULTI-STOCK ANALYSIS")
                print("="*80)
                
                # Calculate correlation
                corr_matrix = self.calculate_correlation(data)
                print(f"\nCorrelation Matrix:")
                print(corr_matrix.to_string())
                
                # Generate heatmap
                self.plot_correlation_heatmap(corr_matrix, output_dir)
                print(f"\nCorrelation heatmap saved to: correlation_heatmap.png")
            
            # Save summary to file
            summary_file = os.path.join(output_dir, "analysis_summary.txt")
            with open(summary_file, "w") as f:
                f.write("STOCK ANALYSIS SUMMARY\n")
                f.write("="*80 + "\n\n")
                f.write(f"Report Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Date Range: {start_date or 'All'} to {end_date or 'All'}\n\n")
                
                for stats in all_stats:
                    f.write(f"\n{stats.get('symbol', 'Unknown')}\n")
                    f.write("-"*40 + "\n")
                    for key, value in stats.items():
                        if isinstance(value, float):
                            f.write(f"{key}: {value:.4f}\n")
                        else:
                            f.write(f"{key}: {value}\n")
            
            print(f"\n{'='*80}")
            print("ANALYSIS COMPLETE")
            print("="*80)
            print(f"\nSummary saved to: {summary_file}")
            print(f"Plots saved to: {output_dir}/\n")
            
        finally:
            self.close()


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Stock Market Data Analysis - Built by Sandeep Cherukuri"
    )
    parser.add_argument(
        "--symbol", "-s",
        type=str,
        default=None,
        help="Comma-separated list of stock symbols to analyze",
    )
    parser.add_argument(
        "--start",
        type=str,
        default=None,
        help="Start date in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--end",
        type=str,
        default=None,
        help="End date in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--output", "-o",
        type=str,
        default="output",
        help="Output directory for plots and reports (default: output)",
    )
    parser.add_argument(
        "--all", "-a",
        action="store_true",
        help="Analyze all available stocks in the database",
    )
    
    args = parser.parse_args()
    
    # Create analyzer
    analyzer = StockAnalyzer()
    
    # Get symbols
    if args.all:
        symbols = analyzer.get_available_symbols()
        if not symbols:
            print("No stocks found in the database. Run the ETL pipeline first.")
            sys.exit(1)
    elif args.symbol:
        symbols = [s.strip().upper() for s in args.symbol.split(",") if s.strip()]
    else:
        print("Please specify symbols with --symbol or use --all")
        sys.exit(1)
    
    # Generate report
    analyzer.generate_report(
        symbols=symbols,
        start_date=args.start,
        end_date=args.end,
        output_dir=args.output,
    )


if __name__ == "__main__":
    main()
