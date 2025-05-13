with
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
                project_contract_address,
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
                ) token_sold_address,
                if(
                    token_bought_address = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee and '{{chain}}' = 'ethereum',
                    'WETH',
                    token_bought_symbol
                ) token_bought_symbol,
                if(
                    token_sold_address = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee and '{{chain}}' = 'ethereum',
                    'WETH',
                    token_sold_symbol
                ) token_sold_symbol
            from dex_aggregator.trades
        )
        where
            DATE(block_time) < DATE(TRY_CAST('{{date_to}}' AS TIMESTAMP))
            and DATE(block_time) >= DATE(TRY_CAST('{{date_from}}' AS TIMESTAMP))
            and blockchain = '{{chain}}'
            and token_bought_address != token_sold_address
            and token_bought_amount > 0
            and token_sold_amount > 0
    ),
    filtered_aggregator_trades as (
        SELECT
            tx_hash, 
            evt_index,
            token_bought_address,
            token_sold_address,
            token_bought_symbol,
            token_sold_symbol,
            token_bought_amount,
            token_sold_amount,
            amount_usd,
            block_time,
            router_labels.router router,
            concat(project, ' - ', version) project
        FROM aggregator_trades
        left join query_3004150 router_labels
            on tx_to = router_labels.address
        WHERE
            concat(project, ' - ', version) != '0x API - settler'
            and (router_labels.router is not null or tx_to = project_contract_address or project = 'cow_protocol')
            and token_bought_address != token_sold_address
            and token_bought_amount > 0
            and token_sold_amount > 0
    ),
    agg_trades_with_repeat_count as (
        select
            *,
            count(1) over (partition by tx_hash) - 1 repeats
        from filtered_aggregator_trades
    ),
    agg_trades_final as (
        select
            *,
            if(
                token_bought_address < token_sold_address,
                concat(cast(token_bought_address as varchar), '_', cast(token_sold_address as varchar)),
                concat(cast(token_sold_address as varchar), '_', cast(token_bought_address as varchar))
            ) pair,
            if(
                token_bought_address < token_sold_address,
                concat(token_bought_symbol, '_', token_sold_symbol),
                concat(token_sold_symbol, '_', token_bought_symbol)
            ) pair_symbol,
            if(
                token_bought_address < token_sold_address,
                token_sold_amount / token_bought_amount,
                token_bought_amount / token_sold_amount
            ) price,
            date_trunc('month', block_time) block_date_m
        from agg_trades_with_repeat_count
        where repeats = 0
    ),
    pair_log_returns as (
        select
            pair,
            block_time_h,
            vol_usd,
            date_trunc('day', block_time_h) block_time_d,
            ln(
                price
                / lag(price) over (partition by pair order by block_time_h)
            ) log_return
        from (
            select
                pair,
                block_time_h,
                sum(price*amount_usd)/sum(amount_usd) price,
                sum(amount_usd) vol_usd
            from (
                select
                    pair,
                    amount_usd,
                    date_trunc('hour', block_time) block_time_h,
                    price
                from agg_trades_final
            )
            group by pair, block_time_h
        )
    ),
    pair_daily_realized_var as (
        select
            pair,
            sum(vol_usd) vol_usd,
            sum(power(log_return, 2)) realized_variance
        from pair_log_returns
        group by pair, block_time_d
    ),
    pair_daily_rv as (
        select
            pair,
            sqrt(sum(realized_variance*vol_usd)/sum(vol_usd)) realized_daily_volatility
        from pair_daily_realized_var
        group by pair
    ),
    trades_stats as (
        select
            pair,
            pair_symbol,
            approx_percentile(freq, 0.5) freq,
            approx_percentile(rel_freq, 0.5) rel_freq,
            approx_percentile(vol_usd, 0.5) vol_usd,
            approx_percentile(rel_vol_usd, 0.5) rel_vol_usd
        from (
            select
                pair,
                pair_symbol,
                x.freq,
                x.vol_usd,
                cast(x.freq as double) / cast(total_monthly.freq as double) rel_freq,
                x.vol_usd / total_monthly.vol_usd rel_vol_usd          
            from (
                select
                    pair,
                    pair_symbol,
                    block_date_m,
                    count(1) freq,
                    sum(amount_usd) vol_usd
                from agg_trades_final
                group by pair, pair_symbol, agg_trades_final.block_date_m
            ) x
            join (
                select block_date_m, sum(amount_usd) vol_usd, count(1) freq
                from agg_trades_final
                group by block_date_m
            ) total_monthly on total_monthly.block_date_m = x.block_date_m
        )
        group by pair, pair_symbol
        order by rel_freq desc
    )


select * 
from (
    select
        trades_stats.*,
        rel_freq * 0.8 + rel_vol_usd * 0.2 score,
        realized_daily_volatility
    from trades_stats
    join pair_daily_rv on pair_daily_rv.pair = trades_stats.pair
)
order by score desc