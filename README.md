# Dynamic Fee and Loss-Versus-Rebalancing Models for Balancer Pools

This project, funded by a Balancer Grant, explores dynamic fee models to mitigate arbitrage losses in Balancer pools. Our research examines the efficacy of various fee structures based on factors such as volatility, DEX trading volume, and gas prices across Ethereum mainnet, Arbitrum, and Polygon networks. The goal is to improve pool profitability by capturing a larger portion of CEX/DEX arbitrage opportunities, ultimately benefiting liquidity providers and the Balancer ecosystem.

This project is done in collaboration with [@AnteroE](https://github.com/AnteroE).

Link to the governance forum thread: ([link](https://forum.balancer.fi/t/research-grant-dynamic-fee-research/6017))

## Project Structure

This project handles the model calculations in a DBT ([documentation](https://docs.getdbt.com/docs/introduction)) project using a local data warehouse DuckDB ([documentation](https://duckdb.org/docs/index)).

- **raw_data**: Initial transformations and DuckDB table creations for scraped vault and pool event data.
- **cex_data**: Creates DuckDB tables from CEX price data. The data used is 1s klines data from https://www.binance.com/en/landing/data.
- **swaps_tvl**: Calculates swaps and token reserves for pools.
- **lvr**: LVR (CEX/DEX arbitrage) calculations for pools.

## Arbitrage Calculation

> [!IMPORTANT]
> "Loss Versus Rebalancing arbitrage" and "CEX/DEX arbitrage" are used interchangeably in the material. For our purposes, these two mean the same thing.

Our model calculates the theoretical CEX/DEX arbitrage opportunity in each block where a swap has historically occurred in the pool. This approach enables direct comparison with the current fee function.

Here's a detailed breakdown of the calculation process:

### 1. Data Preparation

- In the **fct** folder we collect pool reserves, prices, and swap fee data for each block where a swap occurred. Token reserves are normalized to 50/50 ratio in **fct_[chain]_sim_liquidity**.
- CEX (Binance) price data is matched to each block timestamp.


### 2. Fee Functions
The LVR models loop through various fee functions defined in the **lvr/[chain]/int/fees** directory. These fee functions include different approaches to dynamic fee calculation based on factors such as gas prices, volatility, and trading volume.


### 3. Price Target Calculation

The price target is the optimal price an arbitrageur would trade at to maximize profit, considering the pool's fee. The price target is calculated based on the CEX price and the pool's fee tier:
```sql
price_target = 
    if CEX_price > pool_price:
        CEX_price * (1 - fee_tier)
    else:
        CEX_price * (1 + fee_tier)
```

### 4. Arbitrage Calculation

**Liquidity**: We calculate the geometric mean of the USD values of both reserves to represent the pool's liquidity:
```sql
liquidity = sqrt(reserve_0_usd * reserve_1_usd)
```

**Executed Quantity**: This represents the theoretical amount of assets that would be traded in an arbitrage opportunity:
```sql
executed_qty = liquidity * abs((price_target - pool_price) / pool_price)
```
This formula estimates the trade size based on the price difference and the pool's liquidity. The executed quantity represents the theoretical amount of tokens the arbitrageur would need to trade to move the pool price to the price target.

**Average Price**: We estimate the average price at which the arbitrage trade would occur:
```sql
average_price = sqrt(pool_price * price_target)
```
This geometric mean represents a midpoint between the pool price and the target price.

**LVR Value**: This is the core of the LVR (CEX/DEX) calculation, representing the theoretical profit from the arbitrage:
```sql
lvr_value = executed_qty * abs((open_price - average_price) / average_price)
```
This formula calculates the profit by multiplying the executed quantity by the percentage difference between the open (CEX) price and the average trade price.

### 5. Fee Calculation
The fee collected by the pool is calculated as:
```sql
fee = fee_tier * executed_qty
```
This represents the revenue the pool would generate from the arbitrage trade.

### 6. Arbitrage Opportunity Identification
An arbitrage opportunity is identified when:
- The pool price is lower than the price target, which is lower than the open price, and the price ratio exceeds the fee tier.
- Or, the pool price is higher than the price target, which is higher than the open price, and the inverse price ratio exceeds the fee tier.

### Interpretation
The LVR represents the theoretical maximum arbitrage value available in a given block where a swap occurred. It quantifies the potential profit an arbitrageur could make by trading between the pool and the CEX, assuming they could execute at the calculated prices.

### Comparison Between Models

The comparison between different fee models is performed in a final analysis step. You can find the final tables in **metric_[chain]_lvr_impact_analysis_all**. Here's how it works:

1. **Baseline Calculation**: 
   - We calculate a baseline using the current fee structure of the pool.
   - This includes counting arbitrage occurrences, total LVR value, total fees collected, and average fee tier.

2. **Impact Analysis**:
   - For each fee model (volume-based, gas-based, volatility-based, and static), we calculate the same metrics as the baseline.
   - These calculations are performed for various multipliers or fee tiers within each model.

3. **Comparative Metrics**:
   - We then compare each model's performance against the baseline:
     - Change in arbitrage occurrences
     - Change in total LVR value (quantity change)
     - Change in total fees collected
   - We also track the average fee tier for each model.

4. **Results Compilation**:
   - The results are compiled into a final table that includes:
     - Pool name
     - Fee type ('DEX Volume Variance', 'Mean Gas')
     - Category (volume, gas, volatility, static)
     - Multiplier or fee tier
     - Percentage changes in occurrences, quantity, and fees
     - Average fee tier compared to the baseline

This comparison allows us to evaluate how different fee models perform relative to the current fee structure, helping identify potential improvements in capturing arbitrage value and generating fees for the pool.

## Limitations

1. **Ideal Conditions**: The model assumes perfect execution and doesn't account for slippage, transaction costs, or partial fills. This may distort the results during times with high gas fees, for example.
2. **Block-Level Granularity**: Calculations are done at the block level, which may not capture intra-block price movements or MEV opportunities.
3. **Single CEX Reference**: We use Binance as the sole reference for CEX prices. Adding other price data sources and averaging the prices in **cex_data** may give a more complete view.
4. **Simplified Price Impact**: The model uses a simplified approach to estimate price impact, which may not perfectly reflect real-world dynamics.
5. **No Cross-Pool Arbitrage**: The model doesn't account for potential arbitrage opportunities across multiple pools or protocols.
6. **Outlier Filtering**: We filter out the top 10% of price differences to reduce the impact of extreme outliers, which may exclude some valid arbitrage opportunities.
7. **Swap-Based Calculation**: By only calculating LVR for blocks where swaps occurred, we may miss potential arbitrage opportunities in blocks without swaps. However, this approach provides a more realistic comparison to historical pool performance.
8. **Historical Data Dependency**: The model's insights are based on past market conditions and may not perfectly predict future arbitrage opportunities or fee performance.

## How to run the models

Running the models locally requires around 100GB of hard drive space and >24GB of RAM. As a reference, all models take around 3-4h to run 
on a 16GB M1 Macbook.

The models on each chain can be run separately.

### 1. Setup

Download the whole **balancer_dynamic_fee** folder

Run
```
pip install -r requirements.txt
```

### 2. Raw Data Transformations

All scraped on-chain data used in the models could be found in this [zip file (10GB unzipped)](https://drive.google.com/file/d/1JxyESN1BwcMGfZa29XSVbV-uYJ3qxNyY/view?usp=drive_link). 

Download and extract the data to the raw_data folder and run all raw_[  ].ipynb files. This will transform the raw data into cleaned up DuckDB data tables.

### 3. CEX Data Transformations

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

### 4. Swap TVL Transformations

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

You will have to adjust fee function **multipliers**, **min_fee**, and **max_fee** pool by pool due to changing input variables.

### 6. Analysis

DuckDB tables can be fetched into e.g. Pandas dataframes for analysis. **visualization.ipynb** in the main folder demonstrates how to
visualize the LP returns with different fee tiers.

Following code fetches a DuckDB table into a Pandas dataframe. All tables can be fetched in the same way with the sql file name.
```
import duckdb
import pandas as pd

# Connect to the DuckDB database, fetch data
con = duckdb.connect('raw_data/balancer_dynamic_fee.duckdb')
df = con.execute("SELECT * FROM metric_mainnet_lvr_impact_analysis_all").fetchdf()
con.close()
```
