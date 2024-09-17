# Dynamic fee and loss-versus-rebalancing models for Balancer pools

## Project structure

- **raw_data**: Initial transformations and DuckDB table creations for scraped vault and pool event data.
- **cex_data**: Creates DuckDB tables from CEX price data. The data used is 1s klines data from https://www.binance.com/en/landing/data.
- **swaps_tvl**: Calculates swaps and token reserves for pools.
- **lvr**: LVR calculations for pools.

## How to run the models

This project uses DBT with DuckDB local data warehouse. 

Running the models locally requires around 100GB of hard drive space and 24GB of RAM. As a reference, all models take around 1.5h to run 
on a 16GB M1 Macbook while using .

### 1. Setup

Download the whole **balancer_dynamic_fee** folder

Run
```
pip install requirements
```

### 2. Raw data transformations

All scraped on-chain data used in the models could be found in this [zip file (10GB unzipped)](https://drive.google.com/file/d/1JxyESN1BwcMGfZa29XSVbV-uYJ3qxNyY/view?usp=drive_link). 

Download and extract the data to the raw_data folder and run all raw_[  ].ipynb files. This will transform the raw data into cleaned up DuckDB data tables.

### 3. CEX data transformations

CEX data tables include price data for all pool assets.

Download 1s K-Line data from https://www.binance.com/en/landing/data for the following pairs between Aug 2023 and June 2024
![Screenshot 2024-09-17 at 3 39 47 AM](https://github.com/user-attachments/assets/6100fb6d-df05-4efc-bee8-9b15cc3bbd79)

Extract klines files into correct folders.

Run klines python files to add raw cex data tables to the database
```
find models/cex_data -name "get_klines*" -type f -exec basename {} \; | sed 's/\.[^.]*$//' | xargs -I {} dbt run -m {}
```

Run CEX data sql files
```
dbt run -m tag: cex_data
```

### 4. Swap TVL transformations

Calculates historical swaps, swap fees, and token reserves.

Run all swap and TVL models
```
dbt run -m tag: swaps_tvl
```

### 5. LVR Models

Calculates fee functions and loss versus rebalancing models.

Especially Polygon models may be resource intensive and therefore running the models separately is adviced.

Run
```
dbt run -m tag: arbitrum_lvr
dbt run -m tag: mainnet_lvr
dbt run -m tag: polygon_lvr
```

### 6. Analysis

DuckDB tables can be fetched into e.g. Pandas dataframes for analysis. **visualization.ipynb** in the main folder demonstrates how to
visualize the LP returns with different fee tiers.

Following code fetches a DuckDB table into a Pandas dataframe. All tables can be fetched in the same way with the sql file name.
```
import duckdb
import pandas as pd

# Connect to the DuckDB database, fetch data
con = duckdb.connect('my_database.duckdb')
df = con.execute("SELECT * FROM metric_mainnet_lvr_impact_analysis_all").fetchdf()
con.close()
```

