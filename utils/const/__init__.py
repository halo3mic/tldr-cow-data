from . import eth_tokens as eth_tokens
from . import arb_tokens as arb_tokens

token_map = {
    "ethereum": eth_tokens._map,
    "arbitrum": arb_tokens._map,
}
default_pairs = {
    "ethereum": eth_tokens.default_pairs,
}