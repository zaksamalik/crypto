# crypto

_This repository is for NON-commercial purposes only._

### Overview

This package provides a toolkit for managing cryptocurrency-related data. The codebase covers both ETL & data analysis.

__Sub-packages__
* `api` - interact with various APIs
* `etl_spark` - (PySpark) normalize data for analysis
* `helpers` - repo-wide helper modules 


__Repository Structure__
```bash
├── LICENSE
├── README.md
├── api
│   ├── __init__.py
│   ├── blockchain_com
│   │   ├── helpers.py
│   │   └── scripts
│   │       └── all_charts.py
│   └── crypto_compare
│       ├── helpers.py
│       └── scripts
│           ├── coin_list.py
│           ├── historical_ohlcv_daily_all.py
│           ├── historical_ohlcv_hourly_all.py
│           └── historical_ohlcv_minute_all.py
├── etl_spark
│   ├── __init__.py
│   ├── crypto_compare
│   │   └── scripts
│   │       └── coin_list.py
│   └── helpers.py
└── helpers
    ├── __init__.py
    ├── aws.py
    └── general.py
```

