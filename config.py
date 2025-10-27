"""
Configuration module for the stock data pipeline.
"""
import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    """Configuration class for database and data settings."""
    
    # Database Configuration
    DB_HOST = os.getenv('DB_HOST', 'localhost')
    DB_PORT = os.getenv('DB_PORT', '5432')
    DB_NAME = os.getenv('DB_NAME', 'stock_data')
    DB_USER = os.getenv('DB_USER', 'postgres')
    DB_PASSWORD = os.getenv('DB_PASSWORD', '')
    
    @classmethod
    def get_database_url(cls):
        """Get the database URL for SQLAlchemy."""
        return f"postgresql://{cls.DB_USER}:{cls.DB_PASSWORD}@{cls.DB_HOST}:{cls.DB_PORT}/{cls.DB_NAME}"
    
    # Data Configuration
    LOOKBACK_DAYS = int(os.getenv('LOOKBACK_DAYS', 200))
    PRECOMPUTE_DAYS = int(os.getenv('PRECOMPUTE_DAYS', 365))
    
    # Technical Indicator Periods
    DMA_PERIODS = [10, 21, 50, 100]
    EMA_PERIODS = [10, 21, 50, 100]
    
    # Time periods for high/low calculations
    WEEK_52_DAYS = 252  # Trading days in a year (approx 52 weeks)
