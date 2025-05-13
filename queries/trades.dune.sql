with
    unix_ethereum_fills as (
        select
            evt_tx_hash tx_hash,
            evt_index event_index,
            evt_block_time block_time,
            evt_block_number block_number,
            swapper
        from uniswap_ethereum.V2DutchOrderReactor_evt_Fill
        where
            date(evt_block_time) >= DATE(TRY_CAST('{{date_from}}' AS TIMESTAMP))
            and date(evt_block_time) <= DATE(TRY_CAST('{{date_to}}' AS TIMESTAMP))
            
        union all
        
        select
            evt_tx_hash tx_hash,
            evt_index event_index,
            evt_block_time block_time,
            evt_block_number block_number,
            swapper
        from uniswap_ethereum.ExclusiveDutchOrderReactor_evt_Fill
        where
            date(evt_block_time) >= DATE(TRY_CAST('{{date_from}}' AS TIMESTAMP))
            and date(evt_block_time) <= DATE(TRY_CAST('{{date_to}}' AS TIMESTAMP))
    ),
    erc20_transfers as (
        select
            evt_tx_hash tx_hash,
            "from",
            "to",
            contract_address,
            value amount
        from erc20_ethereum.evt_Transfer
        where
            date(evt_block_time) >= DATE(TRY_CAST('{{date_from}}' AS TIMESTAMP))
            and date(evt_block_time) <= DATE(TRY_CAST('{{date_to}}' AS TIMESTAMP))
            and "from" != 0x0000000000000000000000000000000000000000
            and "to" != 0x0000000000000000000000000000000000000000
    ),
    native_transfers as (
        select
            tx_hash,
            "from",
            "to",
            0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee as contract_address,
            value as amount
        from ethereum.traces
        where
            date(block_time) >= DATE(TRY_CAST('{{date_from}}' AS TIMESTAMP))
            and date(block_time) <= DATE(TRY_CAST('{{date_to}}' AS TIMESTAMP))
            and value > uint256 '0'
    ), 
    transfers as (
        select * from erc20_transfers
        union all 
        select * from native_transfers
    ),
    transfers_in_grouped as (
        select tx_hash, "to" account, contract_address, sum(amount) amount
        from transfers
        group by tx_hash, "to", contract_address
    ),
    transfers_out_grouped as (
        select tx_hash, "from" account, contract_address, sum(amount) amount
        from transfers
        group by tx_hash, "from", contract_address
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
                date("timestamp") >= DATE(TRY_CAST('{{date_from}}' AS TIMESTAMP))
                and date("timestamp") <= DATE(TRY_CAST('{{date_to}}' AS TIMESTAMP))
                and blockchain = '{{chain}}'
            
            union
    
            select
                median_price price,
                contract_address token,
                "hour" timestamp
            from dex.prices
            where
                date("hour") >= DATE(TRY_CAST('{{date_from}}' AS TIMESTAMP))
                and date("hour") <= DATE(TRY_CAST('{{date_to}}' AS TIMESTAMP))
                and blockchain = '{{chain}}'
        )
        group by "timestamp", token
    ),
    decimals as (
        select *
        from (
            select
                contract_address,
                decimals,
                symbol, 
                blockchain
            from tokens.erc20
            union
            select
                0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee contract_address,
                18 decimals,
                'eth' symbol,
                'ethereum' blockchain
        )
        where '{{chain}}' = blockchain
    ),
    unix_ethereum as (
        select
            'ethereum' blockchain,
            concat(cast(fills.tx_hash as varchar), '_', cast(fills.event_index as varchar)) id,
            fills.swapper,
            fills.block_number,
            fills.block_time,
            t_in.contract_address token_bought_address,
            t_in.amount token_bought_amount,
            t_out.contract_address token_sold_address,
            t_out.amount token_sold_amount
        from unix_ethereum_fills fills
        join transfers_in_grouped t_in
            on t_in.tx_hash = fills.tx_hash
            and t_in.account = fills.swapper
        join transfers_out_grouped t_out
            on t_out.tx_hash = fills.tx_hash
            and t_out.account = fills.swapper
    ),
    oneinch_fusion as ( 
        select
            blockchain,
            min(block_number) block_number, 
            min(block_time) block_time, 
            cast(order_hash as varchar) id,
            src_token_address token_sold_address,
            dst_token_address token_bought_address,
            -- array_agg(tx_hash) tx_hashes,
            sum(src_token_amount) token_sold_amount,
            sum(dst_token_amount) token_bought_amount,
            sum(amount_usd) amount_usd
        from oneinch.swaps
        where
            date(block_time) >= DATE(TRY_CAST('{{date_from}}' AS TIMESTAMP))
            and date(block_time) <= DATE(TRY_CAST('{{date_to}}' AS TIMESTAMP))
            and flags['fusion']
            and not flags['cross_chain']
        group by blockchain, order_hash, src_token_address, dst_token_address
        order by amount_usd desc
    ),
    _custom_indexed as (
        select
            id,
            project,
            block_number,
            block_time,
            token_bought_address,
            token_sold_address,
            cast(token_bought_amount as double)/pow(10, dec_bought.decimals) token_bought_amount,
            cast(token_sold_amount as double)/pow(10, dec_sold.decimals) token_sold_amount,
            p_hour_bought.price * cast(token_bought_amount as double)/pow(10, dec_bought.decimals) token_bought_amount_usd,
            p_hour_sold.price * cast(token_sold_amount as double)/pow(10, dec_sold.decimals) token_sold_amount_usd
        from (
            select
                id,
                'uniswap-x' project,
                blockchain,
                block_number,
                block_time,
                token_bought_address,
                token_bought_amount,
                token_sold_address,
                token_sold_amount
            from unix_ethereum
            union all
            select
                id,
                'oneinch-fusion' project,
                blockchain,
                block_number,
                block_time,
                token_bought_address,
                token_bought_amount,
                token_sold_address,
                token_sold_amount
            from oneinch_fusion
        ) trades
        -- Tolerate missing price
        left join hourly_prices p_hour_sold on
            p_hour_sold.token = token_sold_address 
            and p_hour_sold.timestamp = date_trunc('hour', block_time)
        left join hourly_prices p_hour_bought on
            p_hour_bought.token = token_bought_address
            and p_hour_bought.timestamp = date_trunc('hour', block_time)
        -- Do not tolerate missing decimals
        join decimals dec_sold on
            dec_sold.contract_address = token_sold_address
        join decimals dec_bought on
            dec_bought.contract_address = token_bought_address
        
        where
            trades.blockchain = '{{chain}}'
            and contains(split('{{token_whitelist}}', ','), lower(cast(token_bought_address as varchar)))
            and contains(split('{{token_whitelist}}', ','), lower(cast(token_sold_address as varchar))) 
    ),
    custom_indexed as (
        select
            id,
            token_bought_address,
            token_sold_address,
            token_bought_amount,
            token_sold_amount,
            (coalesce(token_sold_amount_usd, token_bought_amount_usd, 0)+coalesce(token_bought_amount_usd, token_sold_amount_usd, 0))/2 amount_usd,
            to_unixtime(block_time) block_time,
            project
        from _custom_indexed
    ),
    aggregator_trades as (
        SELECT
            concat(cast(tx_hash as varchar), '_',  cast(evt_index as varchar)) id,
            token_bought_address,
            token_sold_address,
            token_bought_amount,
            token_sold_amount,
            amount_usd,
            to_unixtime(block_time) as block_time,
            concat(project, ' - ', version) project 
        FROM dex_aggregator.trades
        WHERE
            DATE(block_time) <= DATE(TRY_CAST('{{date_to}}' AS TIMESTAMP))
            AND DATE(block_time) >= DATE(TRY_CAST('{{date_from}}' AS TIMESTAMP))
            AND blockchain = '{{chain}}'
            and contains(split('{{token_whitelist}}', ','), lower(cast(token_bought_address as varchar)))
            and contains(split('{{token_whitelist}}', ','), lower(cast(token_sold_address as varchar)))
            and (
                project_contract_address = tx_to
                or project = 'cow_protocol'
                or project = 'bebop'
                or (project = '0x API' and version = 'settler')
            )
    ),
    direct_trades as (
        select
            concat(cast(tx_hash as varchar), '_',  cast(evt_index as varchar)) id,
            token_bought_address,
            token_sold_address,
            token_bought_amount,
            token_sold_amount,
            amount_usd,
            to_unixtime(block_time) as block_time,
            concat(project, ' - ', version) project    
        from dex.trades
        where
            DATE(block_time) <= DATE(TRY_CAST('{{date_to}}' AS TIMESTAMP))
            AND DATE(block_time) >= DATE(TRY_CAST('{{date_from}}' AS TIMESTAMP))
            AND blockchain = '{{chain}}'
            and contains(split('{{token_whitelist}}', ','), lower(cast(token_bought_address as varchar)))
            and contains(split('{{token_whitelist}}', ','), lower(cast(token_sold_address as varchar)))
            and blockchain = '{{chain}}'
            and project_contract_address = tx_to
    ),
    trades as (
        select * from aggregator_trades
        union
        select * from direct_trades
        union
        select * from custom_indexed
    )


select *
from trades
order by block_time
limit {{result_limit}}