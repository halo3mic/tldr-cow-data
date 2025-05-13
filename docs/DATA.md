# Simulating Intents

The datasets outlined below are used to analyze the coincidence of wants peer-to-peer by converting on-chain trades to intents, and looking at the price improvement in execution. For each of the multiple datasets utilized in the empirical analysis, the metadata and instructions on how to pull the datasets are outlined below.

The dataset is curated by Miha Lotrič, and is part of the [TLDR 2025 fellowship program](https://www.thelatestindefi.org/fellowships).

Note that the repo contains data before quality checks were done, thus it could include trades that were invalidly indexed - do quality checks yourself and filter suspicious trades yourself!  

## [Trades](../data/trades/)

### Description

Blockchain trades fetched from Dune Analytics with query [trades.dune.sql](../queries/trades.dune.sql).

Query sources trades from `dex_aggregator.trades` and `dex.trades`.
Trades from `dex_aggregator.trades` and `dex.trades` are filtered to only ones where recipient of the transaction equals on of the whitelisted routers in [Flashbots' Router Labels query](https://dune.com/queries/3004150). Cowswap trades are ignored.

Each directory contains metadata file describing parameters used when fetching the data: `chain`, `date_from`, `date_to` and `pairs`.

## Data Format

parquet

### Schema

* id [String]: Trade identifier
* project [String]: Protocol the trade was facilitated through
* block_time [Int64]: Unix timestamp of the block the trade was included in
* amount_usd [Float64]: Trading amount in USD
* token_bought_address [String]: Address of the asset trader received
* token_sold_address [String]: Address of the asset trader gave
* token_bought_amount [Float64]: Fixed-point amount trader received
* token_sold_amount [Float64]: Fixed-point amount trader gave


### Collection

Data can be programmatically collected by running the script as shown below:

```bash
python -m utils.dune.trades --date-from "2025-03-01 00:00:00" --date-to "2025-03-23 00:00:00" --chain ethereum --performance medium --out-dir data/hist-trades --label ethereum-mar25-1
```

Parameters:
- `--pairs`: List of pairs to fetch the data for
- `--date-from`: Start date in format "YYYY-MM-DD HH:MM:SS" 
- `--date-to`: End date in format "YYYY-MM-DD HH:MM:SS"
- `--result-limit`: Maximum number of rows to fetch (default: 600,000)
- `--chain`: Blockchain network (default: ethereum)
- `--performance`: Query performance (choices: low, medium, high)
- `--label`: Label for the output directory (default: random UUID)
- `--out-dir`: Base output directory (default: data)


## [CowSwap Intents](../data/intents/cowswap/orders)

### Description

CowSwap protocol orders fetched from CowSwap's batch API.
Only a subset of original parameters is recorded.

File name indicates the chain orders relate to.

### Data Format

parquet

### Schema


* batch_id [Int64]: Unique CowSwap batch identifier
* uid [String]: Unique CowSwap order identifer
* sell_token [String]: Address of token trader is selling
* buy_token [String]: Address of token trader is buying
* sell_amount [String]: Amount trader is selling (in wei)
* buy_amount [String]: Amount trader is buying (in wei)
* created_at [Int64]: Unix timestamp of order creation
* valid_to [Int64]: Unix timestamp of order expiry
* kind [String]: Whether order is `sell` or `buy`
* partially_fillable [Bool]: Whether order is partially fillable
* class [String]: Order class (eg. limit)


### Collection

Data can be collected by running `utils.dune.cowswap_intent_loader.py`. Note that, as of writing, only batches from the last 28 days can be queried.


## [CowSwap Fills](../data/intents/cowswap/fills)


### Description

CowSwap protocol order fills fetched from Dune Analytics with query [cowswap_fills.dune.sql](../queries/cowswap_fills.dune.sql). 
The query sources fills from Dune's dataset `gnosis_protocol_v2_multichain.GPv2Settlement_evt_Trade`.

Each directory contains metadata file describing parameters used when fetching the data: `chain`, `date_from`, `date_to` and `tokens`.

### Data Format

parquet

### Schema

* tx_hash [String]: Transaction hash in which order was filled
* block_time [Int64]: Unix timestamp of the block fill was included in
* orderUid [String]: Unique CowSwap order identifer for filled order
* token_sold [String]: Address of token trader sold
* token_bought [String]: Address of token trader bought
* amount_sold [String]: Amount trader bought (in wei)
* amount_bought [String]: Amount trader sold (in wei)


### Collection


Data can be programmatically collected by running the script as shown below:
```bash
python -m utils.dune.cowswap_fills --tokens weth,usdc,wbtc --date-from "2024-12-18" --date-to "2025-04-27" --chain ethereum --out-dir data/intents/cowswap/fills --label ethereum_20241218_20250427
```


## [Fusion Intents](../data/intents/fusion/orders)

### Description

OneInch Fusion order data streamed from OneInch Fusion websocket feed.

File name indicates the chain order data relates to.

### Data Format

jsonl

### Schema

See [OneInch Fusion docs](https://portal.1inch.dev/documentation/apis/swap/fusion-plus/fusion-plus-sdk/for-resolvers/web-socket-api).

### Collection

TBD


## [Fusion Fills](../data/intents/fusion/fills)


### Description

OneInch Fusion order fills fetched from Dune Analytics with query [fusion_fills.dune.sql](../queries/fusion_fills.dune.sql). 
The query sources fills from Dune's dataset `oneinch.swaps`.

Each directory contains metadata file describing parameters used when fetching the data: `chain`, `date_from`, `date_to` and `tokens`.

### Data Format

parquet


### Schema

* tx_hash [String]: Transaction hash in which order was filled
* block_time [Int64]: Unix timestamp of the block fill was included in
* order_hash [String]: Unique Fusion order identifer for filled order
* amount_usd [Float64]: Trading amount in USD
* token_bought_address [String]: Address of the asset trader received
* token_sold_address [String]: Address of the asset trader gave
* token_bought_amount [Float64]: Amount trader received (in wei)
* token_sold_amount [Float64]: Amount trader gave (in wei)


### Collection


Data can be programmatically collected by running the script as shown below:
```bash
python -m utils.dune.fusion_fills --tokens weth,usdc,wbtc --date-from "2024-12-18" --date-to "2025-04-27" --chain ethereum --out-dir data/intents/fusion/fills --label ethereum_20241218_20250427
```


## [Daily Prices](../data/prices/daily_dune_prices)

### Description

Token price candles fetched from Dune Analytics using query [prices.dune.sql](../queries/prices.dune.sql).
Query sources prices from Dune's table `prices.usd_daily`.

Each directory contains metadata file describing parameters used when fetching the data: `chain`, `date_from`, `date_to` and `tokens`.

### Data Format 

parquet

### Schema

* day [String]: Date of the candle
* token [String]: Token address
* decimals [Int64]: Token decimals
* price [Float64]: Average price
* price_high [Float64]: Highest daily price
* price_low [Float64]: Lowest daily price

### Collection

Data can be programmatically collected by running the script as shown below:
```bash
python -m utils.dune.prices --tokens weth,usdc,wbtc --date-from "2024-12-18" --date-to "2025-04-27" --chain ethereum --out-dir data/prices --label ethereum_20241218_20250427
```


## [Block-by-block Prices](../data/prices/block_pool_prices)

### Description

Token price candles fetched from Dune Analytics using query [prices.dune.sql](../queries/prices.dune.sql).
Query sources prices from Dune's table `prices.usd_daily`.

Each directory contains metadata file describing parameters used when fetching the data:
* `chain_id`: Chain prices were fetched for 
* `start_block` and `end_block`: Block range for the prices
* `sources`: Which sources were used to obtain the prices
* `precision`: Precision factor, eg. if it is 15, then divide the prices by 10^15 to obtain the actual price 

### Data Format 

parquet

### Schema

* block_num[Uint64]
* block_timestamp[Uint64]
* source[String]
* price[String]
* quote_token[String]
* base_token[String]

### Collection

Data can be programmatically collected by using the PoolPriceFetcher tooling you can find [here](utils-rs/pool-price-fetcher).
