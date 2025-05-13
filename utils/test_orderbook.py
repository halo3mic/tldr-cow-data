import unittest
from orderbook_rs import Trade, MetaOrderBook as OrderBook


class TestOrderBook(unittest.TestCase):

    def setUp(self):
        self.eth_usdc_book = OrderBook("ETH", "USDC")
        self.wbtc_usdc_book = OrderBook("WBTC", "USDC")

    def test_add_trade_eth_usdc(self):
        trade = Trade("1", "ETH", "USDC", 1, 1500, 0)
        self.eth_usdc_book.add_trade(trade)
        self.assertEqual(self.eth_usdc_book.bid_size(), 1)
        self.assertEqual(self.eth_usdc_book.ask_size(), 0)

        trade = Trade("2", "USDC", "ETH", 1500, 1, 1)
        self.eth_usdc_book.add_trade(trade)
        self.assertEqual(self.eth_usdc_book.bid_size(), 1)
        self.assertEqual(self.eth_usdc_book.ask_size(), 1)

    def test_add_trade_wbtc_usdc(self):
        trade = Trade("1", "WBTC", "USDC", 1, 60000, 0)
        self.wbtc_usdc_book.add_trade(trade)
        self.assertEqual(self.wbtc_usdc_book.bid_size(), 1)
        self.assertEqual(self.wbtc_usdc_book.ask_size(), 0)

        trade = Trade("2", "USDC", "WBTC", 60000, 1, 1)
        self.wbtc_usdc_book.add_trade(trade)
        self.assertEqual(self.wbtc_usdc_book.bid_size(), 1)
        self.assertEqual(self.wbtc_usdc_book.ask_size(), 1)

    def test_invalid_trade(self):
        trade = Trade("1", "BTC", "ETH", 1, 20, 1000)
        with self.assertRaises(ValueError):
            self.eth_usdc_book.add_trade(trade)

    def test_order_sorting(self):
        # Bids should be sorted in a descending order
        self.eth_usdc_book.add_trade(Trade("1", "ETH", "USDC", 1, 1400, 0))
        self.eth_usdc_book.add_trade(Trade("2", "ETH", "USDC", 1, 1500, 1))
        self.eth_usdc_book.add_trade(Trade("3", "ETH", "USDC", 1, 1600, 2))
        
        self.assertEqual(self.eth_usdc_book.bid(0).id, "3")
        self.assertEqual(self.eth_usdc_book.bid(self.eth_usdc_book.bid_size()-1).id, "1")

        # Asks should be sorted in an ascending order
        self.eth_usdc_book.add_trade(Trade("4", "USDC", "ETH", 1600, 1, 1003))
        self.eth_usdc_book.add_trade(Trade("5", "USDC", "ETH", 1500, 1, 1004))
        self.eth_usdc_book.add_trade(Trade("6", "USDC", "ETH", 1400, 1, 1005))

        self.assertEqual(self.eth_usdc_book.ask(0).id, "6")
        self.assertEqual(self.eth_usdc_book.ask(self.eth_usdc_book.ask_size()-1).id, "4")

    def test_find_exact_matches(self):
        self.eth_usdc_book.add_trade(Trade("1", "ETH", "USDC", 1, 1500, 1000))  # Bid
        self.eth_usdc_book.add_trade(Trade("2", "USDC", "ETH", 1500, 1, 1001))  # Ask

        matches = self.eth_usdc_book.match_trades()

        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0].bid_id, "1")
        self.assertEqual(matches[0].ask_id, "2")

    def test_partial_match(self):
        self.eth_usdc_book.add_trade(Trade("1", "ETH", "USDC", 0.86, 1991, 1000))  # Bid
        # self.eth_usdc_book.add_trade(Trade("2", "ETH", "USDC", 2, 3100, 1000))  # Bid

        self.eth_usdc_book.add_trade(Trade("3", "USDC", "ETH", 9128, 4, 1001))  # Ask
        self.eth_usdc_book.add_trade(Trade("4", "USDC", "ETH", 12726, 5.58, 1002))  # Ask

        print(self.eth_usdc_book)

        matches = self.eth_usdc_book.match_trades()

        print(matches)

        # self.assertEqual(len(matches), 2)
        # self.assertEqual(matches[0].bid_id, "1")
        # self.assertEqual(matches[0].ask_id, "3")
        # self.assertEqual(matches[0].amount, 1)
        # self.assertEqual(matches[0].price, 1450)
        # self.assertEqual(matches[1].bid_id, "1")
        # self.assertEqual(matches[1].ask_id, "2")
        # self.assertEqual(matches[1].amount, 1)
        # self.assertEqual(matches[1].price, 1500)
        
        # self.assertEqual(self.eth_usdc_book.bid_size(), 1)
        # self.assertEqual(self.eth_usdc_book.bid(0).amount, 1)
        # self.assertEqual(self.eth_usdc_book.ask_size(), 0)

if __name__ == '__main__':
    unittest.main()