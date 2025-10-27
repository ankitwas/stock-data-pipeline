"""
Technical indicators calculation module.
"""
import pandas as pd
import numpy as np
from typing import List
from config import Config


class TechnicalIndicators:
    """Class to calculate various technical indicators."""
    
    @staticmethod
    def calculate_sma(data: pd.Series, period: int) -> pd.Series:
        """
        Calculate Simple Moving Average (SMA/DMA).
        
        Args:
            data: Price series
            period: Period for moving average
        
        Returns:
            Series with SMA values
        """
        return data.rolling(window=period, min_periods=period).mean()
    
    @staticmethod
    def calculate_ema(data: pd.Series, period: int) -> pd.Series:
        """
        Calculate Exponential Moving Average (EMA).
        
        Args:
            data: Price series
            period: Period for moving average
        
        Returns:
            Series with EMA values
        """
        return data.ewm(span=period, adjust=False, min_periods=period).mean()
    
    @staticmethod
    def calculate_t_score(df: pd.DataFrame) -> pd.Series:
        """
        Calculate T-Score (Technical Score).
        A momentum indicator based on price position relative to recent range.
        
        Formula: (Close - Low_N) / (High_N - Low_N) * 100
        where N is the lookback period (e.g., 14 days)
        
        Args:
            df: DataFrame with high, low, close columns
        
        Returns:
            Series with T-Score values
        """
        lookback = 14
        
        high_n = df['high'].rolling(window=lookback, min_periods=lookback).max()
        low_n = df['low'].rolling(window=lookback, min_periods=lookback).min()
        
        t_score = ((df['close'] - low_n) / (high_n - low_n + 1e-10)) * 100
        
        return t_score
    
    @staticmethod
    def calculate_f_score(df: pd.DataFrame) -> pd.Series:
        """
        Calculate F-Score (Fundamental/Financial Score).
        A simplified version based on price and volume momentum.
        
        Components:
        1. Price momentum (above/below MA)
        2. Volume trend
        3. Price trend
        
        Args:
            df: DataFrame with close and volume columns
        
        Returns:
            Series with F-Score values (0-9)
        """
        score = pd.Series(0, index=df.index)
        
        # Calculate 50-day moving average
        ma_50 = df['close'].rolling(window=50, min_periods=50).mean()
        
        # 1. Price above 50-day MA (1 point)
        score += (df['close'] > ma_50).astype(int)
        
        # 2. Price increasing over last 20 days (1 point)
        price_20d_ago = df['close'].shift(20)
        score += (df['close'] > price_20d_ago).astype(int)
        
        # 3. 50-day MA trending up (1 point)
        ma_50_20d_ago = ma_50.shift(20)
        score += (ma_50 > ma_50_20d_ago).astype(int)
        
        # 4. Volume above average (1 point)
        avg_volume = df['volume'].rolling(window=50, min_periods=50).mean()
        score += (df['volume'] > avg_volume).astype(int)
        
        # 5. Short-term momentum (10-day MA > 20-day MA) (1 point)
        ma_10 = df['close'].rolling(window=10, min_periods=10).mean()
        ma_20 = df['close'].rolling(window=20, min_periods=20).mean()
        score += (ma_10 > ma_20).astype(int)
        
        # 6. Medium-term momentum (20-day MA > 50-day MA) (1 point)
        score += (ma_20 > ma_50).astype(int)
        
        # 7. Price above 200-day MA (1 point)
        ma_200 = df['close'].rolling(window=200, min_periods=200).mean()
        score += (df['close'] > ma_200).astype(int)
        
        # 8. Positive 5-day return (1 point)
        returns_5d = df['close'].pct_change(5)
        score += (returns_5d > 0).astype(int)
        
        # 9. Positive 20-day return (1 point)
        returns_20d = df['close'].pct_change(20)
        score += (returns_20d > 0).astype(int)
        
        return score
    
    @staticmethod
    def calculate_52_week_high(df: pd.DataFrame) -> pd.Series:
        """
        Check if current price is at 52-week high.
        
        Args:
            df: DataFrame with high column
        
        Returns:
            Boolean series indicating 52-week high
        """
        period = Config.WEEK_52_DAYS
        rolling_max = df['high'].rolling(window=period, min_periods=1).max()
        return df['high'] >= rolling_max
    
    @staticmethod
    def calculate_52_week_low(df: pd.DataFrame) -> pd.Series:
        """
        Check if current price is at 52-week low.
        
        Args:
            df: DataFrame with low column
        
        Returns:
            Boolean series indicating 52-week low
        """
        period = Config.WEEK_52_DAYS
        rolling_min = df['low'].rolling(window=period, min_periods=1).min()
        return df['low'] <= rolling_min
    
    @staticmethod
    def calculate_all_time_high(df: pd.DataFrame) -> pd.Series:
        """
        Check if current price is at all-time high.
        
        Args:
            df: DataFrame with high column
        
        Returns:
            Boolean series indicating all-time high
        """
        expanding_max = df['high'].expanding(min_periods=1).max()
        return df['high'] >= expanding_max
    
    @staticmethod
    def calculate_all_time_low(df: pd.DataFrame) -> pd.Series:
        """
        Check if current price is at all-time low.
        
        Args:
            df: DataFrame with low column
        
        Returns:
            Boolean series indicating all-time low
        """
        expanding_min = df['low'].expanding(min_periods=1).min()
        return df['low'] <= expanding_min
    
    @classmethod
    def add_all_indicators(cls, df: pd.DataFrame, symbol: str = None) -> pd.DataFrame:
        """
        Add all technical indicators to the dataframe.
        
        Args:
            df: DataFrame with OHLCV data
            symbol: Stock symbol (optional, for filtering)
        
        Returns:
            DataFrame with all indicators added
        """
        # Work with a copy
        result_df = df.copy()
        
        # If multiple symbols, process each separately
        if symbol:
            mask = result_df['symbol'] == symbol
            symbol_df = result_df[mask].copy().sort_values('date')
        else:
            symbol_df = result_df.copy().sort_values('date')
        
        # Calculate T-Score
        symbol_df['t_score'] = cls.calculate_t_score(symbol_df)
        
        # Calculate F-Score
        symbol_df['f_score'] = cls.calculate_f_score(symbol_df)
        
        # Calculate DMAs
        for period in Config.DMA_PERIODS:
            symbol_df[f'dma_{period}'] = cls.calculate_sma(symbol_df['close'], period)
        
        # Calculate EMAs
        for period in Config.EMA_PERIODS:
            symbol_df[f'ema_{period}'] = cls.calculate_ema(symbol_df['close'], period)
        
        # Calculate boolean indicators
        symbol_df['is_52_week_high'] = cls.calculate_52_week_high(symbol_df)
        symbol_df['is_52_week_low'] = cls.calculate_52_week_low(symbol_df)
        symbol_df['is_all_time_high'] = cls.calculate_all_time_high(symbol_df)
        symbol_df['is_all_time_low'] = cls.calculate_all_time_low(symbol_df)
        
        # Update the result dataframe
        if symbol:
            result_df.loc[mask] = symbol_df.values
        else:
            result_df = symbol_df
        
        return result_df
    
    @classmethod
    def process_multiple_symbols(cls, df: pd.DataFrame) -> pd.DataFrame:
        """
        Process multiple symbols in a dataframe.
        
        Args:
            df: DataFrame with data for multiple symbols
        
        Returns:
            DataFrame with indicators for all symbols
        """
        all_processed = []
        
        symbols = df['symbol'].unique()
        
        for symbol in symbols:
            print(f"Calculating indicators for {symbol}...")
            symbol_data = df[df['symbol'] == symbol].copy()
            symbol_data = symbol_data.sort_values('date')
            processed = cls.add_all_indicators(symbol_data)
            all_processed.append(processed)
        
        result = pd.concat(all_processed, ignore_index=True)
        return result


if __name__ == "__main__":
    # Test the indicators
    from data_fetcher import DataFetcher
    
    fetcher = DataFetcher()
    data = fetcher.fetch_multiple_stocks(['RELIANCE'], n_bars=300)
    
    if not data.empty:
        indicators = TechnicalIndicators()
        processed_data = indicators.process_multiple_symbols(data)
        
        print("\nProcessed data with indicators:")
        print(processed_data.tail())
        print(f"\nColumns: {processed_data.columns.tolist()}")
