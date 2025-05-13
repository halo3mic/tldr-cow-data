with
    pair_whitelist as (
        select
            from_hex(pair[1]) as token0,
            from_hex(pair[2]) as token1
        from (
            select split(pair, '_') pair
            from unnest(split('{{pair_whitelist}}', ',')) as c(pair)
        )
    ),

    -- Trades from `dex.trades` (mono and multihop)

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
                DATE("timestamp") < DATE(TRY_CAST('{{date_to}}' AS TIMESTAMP))
                AND DATE("timestamp") >= DATE(TRY_CAST('{{date_from}}' AS TIMESTAMP))
                and blockchain = 'ethereum'
            
            union
    
            select
                median_price price,
                contract_address token,
                "hour" timestamp
            from dex.prices
            where
                DATE("hour") < DATE(TRY_CAST('{{date_to}}' AS TIMESTAMP))
                AND DATE("hour") >= DATE(TRY_CAST('{{date_from}}' AS TIMESTAMP))
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
            DATE(evt_block_time) < DATE(TRY_CAST('{{date_to}}' AS TIMESTAMP))
            AND DATE(evt_block_time) >= DATE(TRY_CAST('{{date_from}}' AS TIMESTAMP))
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
            DATE(block_time) < DATE(TRY_CAST('{{date_to}}' AS TIMESTAMP))
            AND DATE(block_time) >= DATE(TRY_CAST('{{date_from}}' AS TIMESTAMP))
            and value > uint256 '0'
    ), 
    transfers as (
        select * from erc20_transfers
        union all 
        select * from native_transfers
    ),
    enriched_transfers as (
        select
            transfers.block_timestamp,
            transfers.tx_hash,
            transfers.contract_address token_address,
            transfers."from" sender,
            transfers."to" recipient,
            transfers.amount,
            cast(transfers.amount as double) / pow(10, decimals.decimals) amount_fixed,
            cast(transfers.amount as double) / pow(10, decimals.decimals) * hourly_prices.price amount_usd
        from transfers
        left join decimals
            on decimals.contract_address = transfers.contract_address
        left join hourly_prices
            on hourly_prices.token = transfers.contract_address
            and hourly_prices.timestamp = date_trunc('hour', transfers.block_timestamp)
        where 
            DATE(transfers.block_timestamp) < DATE(TRY_CAST('{{date_to}}' AS TIMESTAMP))
            AND DATE(transfers.block_timestamp) >= DATE(TRY_CAST('{{date_from}}' AS TIMESTAMP))
    ),
    transfers_grouped as (
        select
            block_timestamp,
            tx_hash,
            sender,
            recipient,
            token_address,
            try_sum(amount) amount,
            sum(amount_fixed) amount_fixed,
            sum(amount_usd) amount_usd
        from enriched_transfers
        group by block_timestamp, tx_hash, sender, recipient, token_address
        having try_sum(amount) is not null
    ),
    transfers_in_grouped as (
        select
            block_timestamp,
            tx_hash,
            recipient,
            token_address,
            try_sum(amount) amount,
            sum(amount_fixed) amount_fixed,
            sum(amount_usd) amount_usd
        from transfers_grouped
        group by block_timestamp, tx_hash, recipient, token_address
        having try_sum(amount) is not null
    ),
    aggregator_trades as (
        select *
        from (
            select
                tx_hash,
                evt_index,
                block_time,
                amount_usd,
                token_bought_amount,
                token_sold_amount,
                tx_to,
                blockchain,
                project,
                "version",
                if(
                    token_bought_address = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee and '{{chain}}' = 'ethereum',
                    0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2,
                    token_bought_address
                ) token_bought_address,
                if(
                    token_sold_address = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee and '{{chain}}' = 'ethereum',
                    0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2,
                    token_sold_address
                ) token_sold_address
            from dex_aggregator.trades
        )
        join pair_whitelist pw
            on (pw.token0 = token_bought_address and pw.token1 = token_sold_address)
            or (pw.token1 = token_bought_address and pw.token0 = token_sold_address)
        where
            DATE(block_time) < DATE(TRY_CAST('{{date_to}}' AS TIMESTAMP))
            and DATE(block_time) >= DATE(TRY_CAST('{{date_from}}' AS TIMESTAMP))
            and blockchain = '{{chain}}'
            and token_bought_address != token_sold_address
            and token_bought_amount > 0
            and token_sold_amount > 0
    ),
    direct_trades as (
        select
            direct_trades.tx_hash,
            direct_trades.evt_index,
            direct_trades.token_bought_address,
            direct_trades.token_sold_address,
            direct_trades.token_bought_amount,
            direct_trades.token_sold_amount,
            direct_trades.amount_usd,
            to_unixtime(direct_trades.block_time) as block_time,
            concat(direct_trades.project, ' - ', version) project,
            router_labels.router,
            coalesce(router_labels.address, direct_trades.project_contract_address) router_address,
            direct_trades.project_contract_address hop_project_address,
            rank() over (partition by direct_trades.tx_hash order by direct_trades.evt_index) leg_rnk,
            count(1) over (partition by direct_trades.tx_hash) total_hops
        from dex.trades direct_trades
        -- Ensure that trade is a call within transaction to a known router
        join query_3004150 router_labels
            on (direct_trades.tx_to = router_labels.address or direct_trades.tx_to = direct_trades.project_contract_address)
        where
            DATE(direct_trades.block_time) < DATE(TRY_CAST('{{date_to}}' AS TIMESTAMP))
            AND DATE(direct_trades.block_time) >= DATE(TRY_CAST('{{date_from}}' AS TIMESTAMP))
            and direct_trades.blockchain = '{{chain}}'
    ),
    direct_trades_single as (
        select
            tx_hash,
            block_time,
            router,
            token_sold_address,
            token_bought_address,
            token_sold_amount,
            token_bought_amount,
            amount_usd,
            0 amount_usd_rel_diff_trades,
            null amount_usd_rel_diff_transfers,
            'monohop' direct_trade_kind,
            project
            
        from direct_trades
        where total_hops = 1
    ),
    direct_trades_combo as (
        select 
            first_leg.tx_hash,
            first_leg.block_time,
            first_leg.router,
            first_leg.token_sold_address,
            last_leg.token_bought_address,
            coalesce(
                trader_transfer_in.amount_fixed,
                first_leg.token_sold_amount
            ) token_sold_amount,
            coalesce(
                trader_transfer_out.amount_fixed,
                last_leg.token_bought_amount
            ) token_bought_amount,
            (
                coalesce(
                    trader_transfer_in.amount_usd,
                    first_leg.amount_usd
                )
                + coalesce(
                    trader_transfer_out.amount_usd,
                    last_leg.amount_usd
                )
            ) / 2 as amount_usd,
            first_leg.amount_usd / last_leg.amount_usd -1 amount_usd_rel_diff_trades,
            trader_transfer_in.amount_usd / trader_transfer_out.amount_usd -1 amount_usd_rel_diff_transfers,
            'multihop' direct_trade_kind,
            null project
            
        from (
            select * 
            from direct_trades
            where
                total_hops > 1
                and leg_rnk = 1
        ) first_leg
        
        join (
            select * 
            from direct_trades
            where
                total_hops > 1
                and leg_rnk = total_hops
        ) last_leg
            on first_leg.tx_hash = last_leg.tx_hash
            and first_leg.token_sold_address != last_leg.token_bought_address

        left join transfers_grouped trader_transfer_in
            on trader_transfer_in.tx_hash = first_leg.tx_hash
            and trader_transfer_in.token_address = first_leg.token_sold_address
            and (
                (trader_transfer_in.recipient = first_leg.router_address)
                or (trader_transfer_in.recipient = first_leg.hop_project_address)
            )
        left join transfers_in_grouped trader_transfer_out
            on trader_transfer_out.tx_hash = first_leg.tx_hash
            and trader_transfer_out.token_address = last_leg.token_bought_address
            and trader_transfer_out.recipient = trader_transfer_in.sender

        where
            first_leg.router != '1inch Router'
            and first_leg.router != 'Paraswap Router'
    ),
    direct_trades_final as (
        select
            direct_trades.*,
            concat(
                'trades_', 
                direct_trade_kind,
                if(amount_usd_rel_diff_transfers is null, '_without_fee', '_with_fee')
            ) kind
        from (
            select 
                tx_hash,
                block_time,
                token_sold_amount,
                token_bought_amount,
                amount_usd,
                router,
                project,
                direct_trade_kind,
                amount_usd_rel_diff_trades,
                amount_usd_rel_diff_transfers,
                if(
                token_bought_address = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee and '{{chain}}' = 'ethereum',
                    0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2,
                    token_bought_address
                ) token_bought_address,
                if(
                    token_sold_address = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee and '{{chain}}' = 'ethereum',
                    0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2,
                    token_sold_address
                ) token_sold_address
            from (
                select * from direct_trades_combo
                union
                select * from direct_trades_single
            )
        ) direct_trades
        join pair_whitelist pw
            on (pw.token0 = direct_trades.token_bought_address and pw.token1 = direct_trades.token_sold_address)
            or (pw.token1 = direct_trades.token_bought_address and pw.token0 = direct_trades.token_sold_address)
        left join aggregator_trades on direct_trades.tx_hash = aggregator_trades.tx_hash
        where
            abs(amount_usd_rel_diff_trades) < cast('{{allowed_rel_diff}}' as double)
            and direct_trades.token_bought_address != direct_trades.token_sold_address
            -- Ensure the trade is not an intermediate trade (hop) from `dex_aggregator.trades`
            and aggregator_trades.tx_hash is null
            and direct_trades.token_bought_amount > 0
            and direct_trades.token_sold_amount > 0
    ),

    -- Trades from `dex_aggregator.trades`
    
    filtered_aggregator_trades as (
        SELECT
            tx_hash, 
            evt_index,
            token_bought_address,
            token_sold_address,
            token_bought_amount,
            token_sold_amount,
            amount_usd,
            to_unixtime(block_time) as block_time,
            router_labels.router router,
            concat(project, ' - ', version) project
        FROM aggregator_trades
        join query_3004150 router_labels
            on tx_to = router_labels.address
        WHERE
            project != 'cow_protocol'
            and concat(project, ' - ', version) != '0x API - settler'
            and token_bought_address != token_sold_address
    ),
    agg_trades_with_repeat_count as (
        select
            *,
            count(1) over (partition by tx_hash) - 1 repeats
        from filtered_aggregator_trades
    ),
    agg_trades_final as (
        select *, 'agg_trades' kind
        from agg_trades_with_repeat_count
        where
            repeats = 0
    ), 
    trades_final as (
        select 
            tx_hash,
            block_time,
            token_sold_address,
            token_bought_address,
            token_sold_amount,
            token_bought_amount,
            amount_usd,
            router,
            project,
            kind
        from direct_trades_final
        
        union 
        
        select 
            tx_hash,
            block_time,
            token_sold_address,
            token_bought_address,
            token_sold_amount,
            token_bought_amount,
            amount_usd,
            router,
            project,
            kind
        from agg_trades_final
    )

select * from trades_final