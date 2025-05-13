with
    erc20_transfers as (
        select
            evt_block_time block_timestamp,
            evt_tx_hash tx_hash,
            "from",
            "to",
            contract_address,
            value amount
        from erc20_ethereum.evt_Transfer
        where
            DATE(evt_block_time) <= DATE(TRY_CAST('{{date_to}}' AS TIMESTAMP))
            AND DATE(evt_block_time) >= DATE(TRY_CAST('{{date_from}}' AS TIMESTAMP))
            and contains(split('{{token_whitelist}}', ','), lower(cast(contract_address as varchar)))
            and contains(split('{{token_whitelist}}', ','), lower(cast(contract_address as varchar)))
            and "from" != 0x0000000000000000000000000000000000000000
            and "to" != 0x0000000000000000000000000000000000000000
    ),
    native_transfers as (
        select
            block_time block_timestamp,
            tx_hash,
            "from",
            "to",
            0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee as contract_address,
            value as amount
        from ethereum.traces
        where
            DATE(block_time) <= DATE(TRY_CAST('{{date_to}}' AS TIMESTAMP))
            AND DATE(block_time) >= DATE(TRY_CAST('{{date_from}}' AS TIMESTAMP))
            and contains(split('{{token_whitelist}}', ','), '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee')
            and value > uint256 '0'
    ), 
    transfers as (
        select * from erc20_transfers
        union all 
        select * from native_transfers
    ),
    transfers_in_grouped as (
        select block_timestamp, tx_hash, "to" account, contract_address, try_sum(amount) amount, 'in' kind
        from transfers
        group by block_timestamp, tx_hash, "to", contract_address
        having try_sum(amount) is not null
    ),
    transfers_out_grouped as (
        select block_timestamp, tx_hash, "from" account, contract_address, try_sum(amount) amount, 'out' kind
        from transfers
        group by block_timestamp, tx_hash, "from", contract_address
        having try_sum(amount) is not null
    ),
    grouped_transfers as (
        select * from transfers_in_grouped
        union all
        select * from transfers_out_grouped
    ),
    hourly_prices as (
        select
             "timestamp", token, min(price) price
        from ( 
            select
                price,
                contract_address token,
                "timestamp"
            from prices.hour
            where
                DATE("timestamp") <= DATE(TRY_CAST('{{date_to}}' AS TIMESTAMP))
                AND DATE("timestamp") >= DATE(TRY_CAST('{{date_from}}' AS TIMESTAMP))
                and contains(split('{{token_whitelist}}', ','), lower(cast(contract_address as varchar)))
                and contains(split('{{token_whitelist}}', ','), lower(cast(contract_address as varchar)))
                and blockchain = 'ethereum'
            
            union
    
            select
                median_price price,
                contract_address token,
                "hour" timestamp
            from dex.prices
            where
                DATE("hour") <= DATE(TRY_CAST('{{date_to}}' AS TIMESTAMP))
                AND DATE("hour") >= DATE(TRY_CAST('{{date_from}}' AS TIMESTAMP))
                and contains(split('{{token_whitelist}}', ','), lower(cast(contract_address as varchar)))
                and contains(split('{{token_whitelist}}', ','), lower(cast(contract_address as varchar)))
                and blockchain = 'ethereum'
        )
        group by "timestamp", token
    ),
    decimals as (
        select
            contract_address,
            decimals,
            symbol
        from tokens.erc20
        where
            blockchain = 'ethereum'
    ),
    enriched_grouped_transfers as (
        select
            grouped_transfers.block_timestamp,
            grouped_transfers.tx_hash,
            grouped_transfers.kind,
            grouped_transfers.contract_address token_address,
            grouped_transfers.account transferer,
            grouped_transfers.amount,
            cast(grouped_transfers.amount as double) / pow(10, decimals.decimals) amount_fixed,
            cast(grouped_transfers.amount as double) / pow(10, decimals.decimals) * hourly_prices.price amount_usd
        from grouped_transfers
        left join decimals
            on decimals.contract_address = grouped_transfers.contract_address
        left join hourly_prices
            on hourly_prices.token = grouped_transfers.contract_address
            and hourly_prices.timestamp = date_trunc('hour', grouped_transfers.block_timestamp)
        where 
            DATE(grouped_transfers.block_timestamp) <= DATE(TRY_CAST('{{date_to}}' AS TIMESTAMP))
            AND DATE(grouped_transfers.block_timestamp) >= DATE(TRY_CAST('{{date_from}}' AS TIMESTAMP))
    ),
    unix_ethereum_fills as (
        select
            evt_tx_hash tx_hash,
            evt_index event_index,
            evt_block_time block_time,
            evt_block_number block_number,
            swapper
        from uniswap_ethereum.V2DutchOrderReactor_evt_Fill
        where DATE(evt_block_time) <= DATE(TRY_CAST('{{date_to}}' AS TIMESTAMP))
            AND DATE(evt_block_time) >= DATE(TRY_CAST('{{date_from}}' AS TIMESTAMP))
            
        union all
        
        select
            evt_tx_hash tx_hash,
            evt_index event_index,
            evt_block_time block_time,
            evt_block_number block_number,
            swapper
        from uniswap_ethereum.ExclusiveDutchOrderReactor_evt_Fill
        where DATE(evt_block_time) <= DATE(TRY_CAST('{{date_to}}' AS TIMESTAMP))
            AND DATE(evt_block_time) >= DATE(TRY_CAST('{{date_from}}' AS TIMESTAMP))
    ),
    enriched_unix_ethereum_fills as (
        select
            'uniswap-x' project,
            'ethereum' blockchain,
            concat(cast(fills.tx_hash as varchar), '_', cast(fills.event_index as varchar)) id,
            fills.tx_hash,
            fills.block_number,
            fills.block_time,
            fills.swapper,
            t_in.token_address token_bought_address,
            t_in.amount_fixed token_bought_amount,
            t_out.token_address token_sold_address,
            t_out.amount_fixed token_sold_amount,
            (t_in.amount_usd + t_out.amount_usd)/2 amount_usd,
            t_out.amount_usd / t_in.amount_usd -1 amount_usd_rel_diff,
            count(1) over (partition by fills.tx_hash) -1 repeats
        from unix_ethereum_fills fills
        join enriched_grouped_transfers t_in
            on t_in.tx_hash = fills.tx_hash
            and t_in.transferer = fills.swapper
            and t_in.kind = 'in'
        join enriched_grouped_transfers t_out
            on t_out.tx_hash = fills.tx_hash
            and t_out.transferer = fills.swapper
            and t_out.kind = 'out'
            and t_out.token_address != t_in.token_address
    )

select *
from enriched_unix_ethereum_fills
where
    contains(split('{{token_whitelist}}', ','), lower(cast(token_bought_address as varchar)))
    and contains(split('{{token_whitelist}}', ','), lower(cast(token_sold_address as varchar)))
    and abs(amount_usd_rel_diff) < cast('{{rel_diff_limit}}' as double)