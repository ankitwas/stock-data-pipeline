"""
Database module for creating and managing the PostgreSQL database schema.
"""
from sqlalchemy import (
    create_engine, Column, String, Float, Boolean, Date, 
    Integer, UniqueConstraint, Index
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from config import Config

Base = declarative_base()


class StockData(Base):
    """Stock data model with technical indicators."""
    
    __tablename__ = 'stock_data'
    
    # Primary key
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Identification
    symbol = Column(String(20), nullable=False, index=True)
    date = Column(Date, nullable=False, index=True)
    
    # OHLCV Data
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    volume = Column(Float, nullable=False)
    
    # Technical Scores
    t_score = Column(Float)
    f_score = Column(Float)
    
    # DMA (Daily Moving Averages)
    dma_10 = Column(Float)
    dma_21 = Column(Float)
    dma_50 = Column(Float)
    dma_100 = Column(Float)
    
    # EMA (Exponential Moving Averages)
    ema_10 = Column(Float)
    ema_21 = Column(Float)
    ema_50 = Column(Float)
    ema_100 = Column(Float)
    
    # Boolean Indicators
    is_52_week_high = Column(Boolean, default=False)
    is_52_week_low = Column(Boolean, default=False)
    is_all_time_high = Column(Boolean, default=False)
    is_all_time_low = Column(Boolean, default=False)
    
    # Unique constraint on symbol and date combination
    __table_args__ = (
        UniqueConstraint('symbol', 'date', name='uix_symbol_date'),
        Index('idx_symbol_date', 'symbol', 'date'),
    )
    
    def __repr__(self):
        return f"<StockData(symbol={self.symbol}, date={self.date}, close={self.close})>"


class DatabaseManager:
    """Manager class for database operations."""
    
    def __init__(self):
        """Initialize database connection."""
        self.engine = create_engine(Config.get_database_url())
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
    
    def create_tables(self):
        """Create all tables in the database."""
        Base.metadata.create_all(bind=self.engine)
        print("Database tables created successfully!")
    
    def drop_tables(self):
        """Drop all tables in the database."""
        Base.metadata.drop_all(bind=self.engine)
        print("Database tables dropped successfully!")
    
    def get_session(self):
        """Get a new database session."""
        return self.SessionLocal()
    
    def close(self):
        """Close database connection."""
        self.engine.dispose()


if __name__ == "__main__":
    # Create database and tables
    db_manager = DatabaseManager()
    db_manager.create_tables()
