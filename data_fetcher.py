"""
Data fetcher module for retrieving stock data from tvdatafeed.
"""
from tvdatafeed import TvDatafeed, Interval
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Optional


class DataFetcher:
    """Class to fetch stock data using tvdatafeed."""
    
    def __init__(self):
        """Initialize tvdatafeed connection."""
        self.tv = TvDatafeed()
    
    def fetch_stock_data(
        self, 
        symbol: str, 
        exchange: str = 'NSE',
        n_bars: int = 500,
        interval: Interval = Interval.in_daily
    ) -> Optional[pd.DataFrame]:
        """
        Fetch stock data for a given symbol.
        
        Args:
            symbol: Stock symbol (e.g., 'RELIANCE', 'TCS')
            exchange: Exchange name (default: 'NSE')
            n_bars: Number of bars to fetch
            interval: Time interval (default: daily)
        
        Returns:
            DataFrame with OHLCV data or None if fetch fails
        """
        try:
            data = self.tv.get_hist(
                symbol=symbol,
                exchange=exchange,
                interval=interval,
                n_bars=n_bars
            )
            
            if data is not None and not data.empty:
                # Reset index to have datetime as a column
                data = data.reset_index()
                
                # Rename columns to lowercase
                data.columns = [col.lower() for col in data.columns]
                
                # Ensure datetime column is named 'date'
                if 'datetime' in data.columns:
                    data = data.rename(columns={'datetime': 'date'})
                
                # Convert date to date only (remove time component)
                data['date'] = pd.to_datetime(data['date']).dt.date
                
                # Add symbol column
                data['symbol'] = symbol
                
                print(f"Fetched {len(data)} records for {symbol}")
                return data
            else:
                print(f"No data found for {symbol}")
                return None
                
        except Exception as e:
            print(f"Error fetching data for {symbol}: {str(e)}")
            return None
    
    def fetch_multiple_stocks(
        self, 
        symbols: List[str], 
        exchange: str = 'NSE',
        n_bars: int = 500
    ) -> pd.DataFrame:
        """
        Fetch data for multiple stock symbols.
        
        Args:
            symbols: List of stock symbols
            exchange: Exchange name
            n_bars: Number of bars to fetch per symbol
        
        Returns:
            Combined DataFrame with all stocks data
        """
        all_data = []
        
        for symbol in symbols:
            print(f"\nFetching data for {symbol}...")
            data = self.fetch_stock_data(symbol, exchange, n_bars)
            
            if data is not None:
                all_data.append(data)
        
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            print(f"\n\nTotal records fetched: {len(combined_df)}")
            return combined_df
        else:
            print("No data fetched for any symbol")
            return pd.DataFrame()


if __name__ == "__main__":
    # Test the data fetcher
    fetcher = DataFetcher()
    
    # Test with a single stock
    test_symbols = ['RELIANCE', 'TCS']
    data = fetcher.fetch_multiple_stocks(test_symbols, n_bars=50)
    
    if not data.empty:
        print("\nSample data:")
        print(data.head())
        print(f"\nColumns: {data.columns.tolist()}")
