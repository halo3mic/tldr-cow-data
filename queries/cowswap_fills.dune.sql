select
    evt_tx_hash tx_hash,
    evt_block_time block_time,
    orderUid order_id,
    sellAmount amount_sold,
    buyAmount amount_bought,
    sellToken token_sold,
    buyToken token_bought
from gnosis_protocol_v2_multichain.GPv2Settlement_evt_Trade
where
    date(evt_block_time) >= date(try_cast('{{date_from}}' as timestamp))
    and date(evt_block_time) <= date(try_cast('{{date_to}}' as timestamp))
    and (
        '{{token_whitelist}}' = 'all'
        or (
            contains(split('{{token_whitelist}}', ','), lower(cast(sellToken as varchar)))
            and contains(split('{{token_whitelist}}', ','), lower(cast(buyToken as varchar)))
        )
    )
    and chain = '{{chain}}'