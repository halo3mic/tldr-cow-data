from collections import defaultdict

_tokens = [
    ("usdc", "0xaf88d065e77c8cc2239327c5edb3a432268e5831", "stable"),
    ("usdt", "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9", "stable"),
    ("usde", "0x5d3a1ff2b6bab83b63cd9ad0787074081a52ef34", "stable"),
    ("dai", "0xda10009cbd5d07dd0cecc66161fc93d7c9000da1", "stable"),

    ("weth", "0x82af49447d8a07e3bd95bd0d56f35241523fbab1", "blue_chip"),
    ("wbtc", "0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f", "blue_chip"),
    ("arb", "0x912ce59144191c1204e64559fe8253a0e49e6548", "blue_chip"),
    ("eth", "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", "blue_chip"),
    ("weETH", "0x35751007a407ca6feffe80b3cb397736d2cf4dbe", "blue_chip"),
    ("wstETH", "0x5979d7b546e38e414f7e9822514be443a4800529", "blue_chip"),

    ("link", "0xf97f4df75117a78c1a5a0dbb814af92458539fb4", "blue_chip"),
    ("zro", "0x6985884c4392d348587b19cb9eaaf157f13271cd", "blue_chip"),
    ("aave", "0xba5ddd1f9d7f570dc94a51479a000e3bce967196", "blue_chip"),
    ("uni", "0xfa7f8980b0f1e64a2062791cc3b0871572f1f7f0", "blue_chip"),
    ("pendle", "0x0c880f6761f1af8d9aa9c466984b80dab9a8c9e8", "blue_chip"),
    ("gmx", "0xfc5a1a6eb076a2c7ad06ed22c90d7e710e35ad0a", "blue_chip"),
    ("magic", "0x539bde0d7dbd336b79148aa742883198bbf60342", "blue_chip"),

    ("ape", "0x7f9fbf9bdd3f4105c478b996b648fe6e828a1e98", "meme"),
    ("pepe", "0x25d887ce7a35172c62febfd67a1856f20faebb00", "meme"),
]

_map = {}
tkn_class_to_tkn = defaultdict(list)
for tkn, addr, tkn_class in _tokens:
    tkn_class_to_tkn[tkn_class].append(addr)
    globals()[tkn.upper()] = addr
    _map[tkn] = addr

inverse_map = {v: k for k, v in _map.items()}
tkn_to_class = {t: c for (c, ts) in tkn_class_to_tkn.items() for t in ts}