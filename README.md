# Stock Data Pipeline

A comprehensive Python-based system for fetching, processing, and querying stock market data with precomputed technical indicators stored in PostgreSQL.

## Features

- 📊 **Data Fetching**: Automatic fetching of historical stock data using tvdatafeed
- 🔢 **Technical Indicators**: Precomputed indicators including:
  - T-Score (Technical Score)
  - F-Score (Fundamental Score)
  - DMA (Daily Moving Average): 10, 21, 50, 100 periods
  - EMA (Exponential Moving Average): 10, 21, 50, 100 periods
  - Boolean indicators: 52-week high/low, all-time high/low
- 💾 **PostgreSQL Storage**: Efficient storage with upsert capabilities
- 🔍 **Query Wrapper**: Easy-to-use wrapper class for data retrieval
- 🚀 **Scalable**: Processes multiple stocks with batch operations

## Quick Start

See [QUICKSTART.md](QUICKSTART.md) for a 5-minute setup guide.

## Installation

```bash
# Clone repository
git clone https://github.com/ankitwas/stock-data-pipeline.git
cd stock-data-pipeline

# Install dependencies
pip install -r requirements.txt

# Configure database
cp .env.example .env
# Edit .env with your PostgreSQL credentials

# Setup database
python main.py setup

# Process stocks
python main.py process RELIANCE TCS INFY --exchange NSE --bars 500
```

## Usage

### Query Stock Data

```python
from wrapper import StockDataWrapper
from datetime import date

# Get data for specific symbol and date
with StockDataWrapper() as wrapper:
    data = wrapper.get_stock_data('RELIANCE', date(2024, 1, 15))
    
    print(f"Close: ₹{data['close']:.2f}")
    print(f"F-Score: {data['f_score']}/9")
    print(f"T-Score: {data['t_score']:.2f}/100")
    print(f"50-day DMA: ₹{data['dma_50']:.2f}")
    print(f"52-week high: {data['is_52_week_high']}")
```

## Technical Indicators

All indicators are precomputed and stored:

- **T-Score**: Technical momentum score (0-100)
- **F-Score**: Fundamental score (0-9)
- **DMA**: Daily Moving Average (10, 21, 50, 100 periods)
- **EMA**: Exponential Moving Average (10, 21, 50, 100 periods)
- **Boolean Flags**: 52-week high/low, all-time high/low

## Documentation

- [QUICKSTART.md](QUICKSTART.md) - Quick setup guide
- [PROJECT_SUMMARY.md](PROJECT_SUMMARY.md) - Project overview
- [demo.py](demo.py) - Simple usage demo
- [example_usage.py](example_usage.py) - Comprehensive examples

## CLI Commands

```bash
# Setup database
python main.py setup

# Process stocks
python main.py process RELIANCE TCS INFY --exchange NSE --bars 500

# Query data
python main.py query RELIANCE --date 2024-01-15

# List symbols
python main.py list

# Show statistics
python main.py stats
```

## License

MIT License

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
