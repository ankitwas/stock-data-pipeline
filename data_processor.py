"""
Data processor module for fetching, processing, and storing stock data.
"""
import pandas as pd
from datetime import datetime, timedelta
from typing import List
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import and_

from data_fetcher import DataFetcher
from indicators import TechnicalIndicators
from database import DatabaseManager, StockData
from config import Config


class DataProcessor:
    """Main processor for stock data pipeline."""
    
    def __init__(self):
        """Initialize processor with database and data fetcher."""
        self.db_manager = DatabaseManager()
        self.data_fetcher = DataFetcher()
        self.indicators = TechnicalIndicators()
    
    def process_and_store_stocks(
        self, 
        symbols: List[str], 
        exchange: str = 'NSE',
        n_bars: int = None
    ) -> bool:
        """
        Fetch, process, and store stock data for multiple symbols.
        
        Args:
            symbols: List of stock symbols
            exchange: Exchange name
            n_bars: Number of historical bars to fetch (default from config)
        
        Returns:
            True if successful, False otherwise
        """
        if n_bars is None:
            n_bars = Config.LOOKBACK_DAYS + 200  # Extra buffer for indicator calculation
        
        try:
            # Fetch data for all symbols
            print("=" * 80)
            print("FETCHING STOCK DATA")
            print("=" * 80)
            raw_data = self.data_fetcher.fetch_multiple_stocks(
                symbols=symbols,
                exchange=exchange,
                n_bars=n_bars
            )
            
            if raw_data.empty:
                print("No data fetched. Exiting.")
                return False
            
            # Calculate technical indicators
            print("\n" + "=" * 80)
            print("CALCULATING TECHNICAL INDICATORS")
            print("=" * 80)
            processed_data = self.indicators.process_multiple_symbols(raw_data)
            
            # Store data in database
            print("\n" + "=" * 80)
            print("STORING DATA IN DATABASE")
            print("=" * 80)
            self._store_data(processed_data)
            
            print("\n" + "=" * 80)
            print("PROCESSING COMPLETE!")
            print("=" * 80)
            
            return True
            
        except Exception as e:
            print(f"Error in processing: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
    
    def _store_data(self, df: pd.DataFrame):
        """
        Store processed data in the database.
        Uses upsert to handle duplicates.
        
        Args:
            df: Processed DataFrame with all indicators
        """
        session = self.db_manager.get_session()
        
        try:
            # Prepare records
            records = df.to_dict('records')
            total_records = len(records)
            
            print(f"Storing {total_records} records...")
            
            # Batch insert with upsert
            batch_size = 1000
            inserted_count = 0
            updated_count = 0
            
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                
                # Convert date objects if needed
                for record in batch:
                    if isinstance(record.get('date'), str):
                        record['date'] = datetime.strptime(record['date'], '%Y-%m-%d').date()
                
                # Create insert statement with ON CONFLICT UPDATE
                stmt = insert(StockData).values(batch)
                
                # Define update dictionary for conflict resolution
                update_dict = {
                    'open': stmt.excluded.open,
                    'high': stmt.excluded.high,
                    'low': stmt.excluded.low,
                    'close': stmt.excluded.close,
                    'volume': stmt.excluded.volume,
                    't_score': stmt.excluded.t_score,
                    'f_score': stmt.excluded.f_score,
                    'dma_10': stmt.excluded.dma_10,
                    'dma_21': stmt.excluded.dma_21,
                    'dma_50': stmt.excluded.dma_50,
                    'dma_100': stmt.excluded.dma_100,
                    'ema_10': stmt.excluded.ema_10,
                    'ema_21': stmt.excluded.ema_21,
                    'ema_50': stmt.excluded.ema_50,
                    'ema_100': stmt.excluded.ema_100,
                    'is_52_week_high': stmt.excluded.is_52_week_high,
                    'is_52_week_low': stmt.excluded.is_52_week_low,
                    'is_all_time_high': stmt.excluded.is_all_time_high,
                    'is_all_time_low': stmt.excluded.is_all_time_low,
                }
                
                stmt = stmt.on_conflict_do_update(
                    index_elements=['symbol', 'date'],
                    set_=update_dict
                )
                
                session.execute(stmt)
                
                inserted_count += len(batch)
                print(f"Progress: {inserted_count}/{total_records} records processed")
            
            session.commit()
            print(f"\nSuccessfully stored/updated {total_records} records in database!")
            
        except Exception as e:
            session.rollback()
            print(f"Error storing data: {str(e)}")
            import traceback
            traceback.print_exc()
            raise
        finally:
            session.close()
    
    def get_data_stats(self):
        """Get statistics about stored data."""
        session = self.db_manager.get_session()
        
        try:
            # Get total records
            total_records = session.query(StockData).count()
            
            # Get unique symbols
            symbols = session.query(StockData.symbol).distinct().all()
            symbols = [s[0] for s in symbols]
            
            # Get date range
            from sqlalchemy import func
            date_range = session.query(
                func.min(StockData.date),
                func.max(StockData.date)
            ).first()
            
            print("\n" + "=" * 80)
            print("DATABASE STATISTICS")
            print("=" * 80)
            print(f"Total Records: {total_records}")
            print(f"Unique Symbols: {len(symbols)}")
            print(f"Symbols: {', '.join(symbols)}")
            print(f"Date Range: {date_range[0]} to {date_range[1]}")
            print("=" * 80)
            
            return {
                'total_records': total_records,
                'symbols': symbols,
                'date_range': date_range
            }
            
        finally:
            session.close()
    
    def update_single_symbol(self, symbol: str, exchange: str = 'NSE'):
        """
        Update data for a single symbol.
        
        Args:
            symbol: Stock symbol
            exchange: Exchange name
        """
        print(f"\nUpdating data for {symbol}...")
        return self.process_and_store_stocks([symbol], exchange)
    
    def close(self):
        """Close database connections."""
        self.db_manager.close()


if __name__ == "__main__":
    # Example usage
    processor = DataProcessor()
    
    # Initialize database
    processor.db_manager.create_tables()
    
    # Example: Process some Indian stocks
    test_symbols = [
        'RELIANCE',
        'TCS',
        'INFY',
        'HDFCBANK',
        'ICICIBANK'
    ]
    
    # Process and store data
    success = processor.process_and_store_stocks(
        symbols=test_symbols,
        exchange='NSE',
        n_bars=400
    )
    
    if success:
        # Show statistics
        processor.get_data_stats()
    
    processor.close()
