from collections import defaultdict

_tokens = [
    ("usdc", "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", "stable"),
    ("usdt", "0xdac17f958d2ee523a2206206994597c13d831ec7", "stable"),
    ("dai", "0x6b175474e89094c44da98b954eedeac495271d0f", "stable"),

    ("wbtc", "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599", "blue_chip"),
    ("weth", "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2", "blue_chip"),
    ("eth", "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", "blue_chip"),
    ("steth", "0xae7ab96520de3a18e5e111b5eaab095312d7fe84", "blue_chip"),
    ("wsteth", "0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0", "blue_chip"),
    ("eigen", "0xec53bf9167f50cdeb3ae105f56099aaab9061f83", "blue_chip"),
    ("link", "0x514910771af9ca656af840dff83e8264ecf986ca", "blue_chip"),
    ("ena", "0x57e114b691db790c35207b2e685d4a43181e6061", "blue_chip"),
    ("cow", "0xdef1ca1fb7fbcdc777520aa7f396b4e015f497ab", "blue_chip"),
    ("uni", "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984", "blue_chip"),
    ("aave", "0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9", "blue_chip"),

    ("pepe", "0x6982508145454ce325ddbe47a25d4ec3d2311933", "meme"),
    ("spx", "0xe0f63a424a4439cbe457d80e4f4b51ad25b2c56c", "meme"),
    ("shib", "0x95ad61b0a150d79219dcf64e1e6cc01f0b64c4ce", "meme"),
    ("doge", "0x1121acc14c63f3c872bfca497d10926a6098aac5", "meme"),
]

_map = {}
tkn_class_to_tkn = defaultdict(list)
for tkn, addr, tkn_class in _tokens:
    tkn_class_to_tkn[tkn_class].append(addr)
    globals()[tkn.upper()] = addr
    _map[tkn] = addr

inverse_map = {v: k for k, v in _map.items()}
tkn_to_class = {t: c for (c, ts) in tkn_class_to_tkn.items() for t in ts}

default_pairs = {
	"stable": [
		(USDC, USDT),
		(DAI, USDC),
		(DAI, USDT),
		(WETH, WSTETH),
    ],
	"blue_chip": [
        (WETH, USDT),
        (USDC, WETH),
        (DAI, WETH),
        (WBTC, WETH),
        (WBTC, USDT),
        (LINK, WETH),
        (AAVE, WETH),
        (UNI, WETH),
        (WETH, EIGEN),
    ],
    "meme": [
        (DOGE, WETH),
        (SHIB, WETH),
        (PEPE, WETH),
        (SPX, WETH),
    ]
}