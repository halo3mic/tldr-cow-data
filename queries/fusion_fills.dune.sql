select
    tx_hash,
    block_number block_number, 
    block_time block_time, 
    order_hash,
    src_token_address token_sold_address,
    dst_token_address token_bought_address,
    src_token_amount token_sold_amount,
    dst_token_amount token_bought_amount,
    amount_usd amount_usd
from oneinch.swaps
where
    date(block_time) >= DATE(TRY_CAST('{{date_from}}' AS TIMESTAMP))
    and date(block_time) <= DATE(TRY_CAST('{{date_to}}' AS TIMESTAMP))
    and contains(split('{{token_whitelist}}', ','), lower(cast(src_token_address as varchar)))
    and contains(split('{{token_whitelist}}', ','), lower(cast(dst_token_address as varchar)))
    and blockchain = '{{chain}}'
    and flags['fusion']
    and not flags['cross_chain']