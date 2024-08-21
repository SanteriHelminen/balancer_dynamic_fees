This project uses DBT with DuckDB local data warehouse

Project Structure

- raw_data: Initial transformations and DuckDB table creations for scraped vault and pool event data.
- cex_data: Creates DuckDB tables from CEX price data. The data used is 1s klines data from https://www.binance.com/en/landing/data.
- swaps_tvl: Calculates swaps and token reserves for pools.
- lvr: LVR calculations for pools.
