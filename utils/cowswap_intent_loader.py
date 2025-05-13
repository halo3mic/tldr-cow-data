from time import sleep
import pandas as pd
import requests


class CowSwapIntentsLoader:

    def __init__(self, _solver_endpoint, _write_path, _pause_ms=1000, _write_freq=1000):
        self.solver_endpoint = _solver_endpoint
        self.write_path = _write_path
        self.pause_ms = _pause_ms
        self.write_freq = _write_freq

        self.last_batch_order_ids = set()
        self.sink = []

    def fetch(self, initial_batch_id=None):
        batch_id = initial_batch_id if initial_batch_id else self._get_next_batch_id()
        while True:
            print(f"Processing batch {batch_id}")
            try:
                orders = self._fetch_raw_orders(batch_id)
                if not orders:
                    print(f"Batch {batch_id} is empty, skipping")
                    batch_id += 1 
                    continue

                self._parse_orders(batch_id, orders)

                batch_id += 1
                sleep(self.pause_ms / 1000)
            except Exception as e:
                print(f"Error fetching batch {batch_id}: {e}")
                sleep(5000)

    def _get_next_batch_id(self):
        last_id = 0
        try:
            last_id = self._read_stored_orders()["batch_id"].max() + 1
        except Exception as e:
            print(f"Error reading last batch id: {e}")
        finally:
            return last_id

    def _fetch_raw_orders(self, batch_id):
        endpoint = self._get_solver_endpoint(batch_id)
        res = requests.get(endpoint)
        if not res.ok:
            print(f"Error fetching orders for batch {batch_id}: {res.text}")
            return None
        res_json = res.json()
        return res_json.get("orders")

    def _parse_orders(self, batch_id, orders):
        parsed_orders = [
            self._parse_order(order, batch_id) 
            for order in orders 
            if not self._order_in_last_batch(order)
        ]
        print(f"Parsed {len(parsed_orders)} orders")
        self._write_orders(batch_id, parsed_orders)
        self.last_batch_order_ids = {order["uid"] for order in orders}

    def _write_orders(self, batch_id, orders):
        if batch_id % self.write_freq == 0 and len(self.sink) > 0:
            print(f"Writing batch {batch_id} to {self.write_path}")
            orders.extend(self.sink)
            orders_df = pd.DataFrame(orders)
            old_orders = self._read_stored_orders()
            if old_orders is not None:
                orders_df = pd.concat([old_orders, orders_df])
            orders_df.to_parquet(self.write_path, index=False)
            self.sink = []
        else:
            self.sink.extend(orders)

    def _get_solver_endpoint(self, batch_id):
        return f"{self.solver_endpoint}{batch_id}.json"
    
    def _order_in_last_batch(self, order):
        return self.last_batch_order_ids and order["uid"] in self.last_batch_order_ids
    
    def _read_stored_orders(self):
        out = None
        try:
            out = pd.read_parquet(self.write_path)
        finally:
            return out

    @staticmethod
    def _parse_order(order, batch_id):
        return {
            "batch_id": batch_id,
            "uid": order["uid"],
            "sell_token": order["sellToken"],
            "buy_token": order["buyToken"],
            "sell_amount": order["sellAmount"],
            "buy_amount": order["buyAmount"],
            "created_at": order["created"],
            "valid_to": order["validTo"],
            "kind": order["kind"],
            "partially_fillable": order["partiallyFillable"],
            "class": order["class"],
        }    
    

if __name__ == "__main__":
    import threading
    import dotenv
    import os

    dotenv.load_dotenv()

    configs = [
        {
            "batch_id": None,
            "write_path": "./data/intents/cowswap_mainnet.parquet",
            "solver_endpoint": os.getenv("SOLVER_ENDPOINT_MAINNET"),
        },
                {
            "batch_id": None,
            "write_path": "./data/intents/cowswap_arbitrum.parquet",
            "solver_endpoint": os.getenv("SOLVER_ENDPOINT_ARBITRUM"),
        },
                {
            "batch_id": None,
            "write_path": "./data/intents/cowswap_base.parquet",
            "solver_endpoint": os.getenv("SOLVER_ENDPOINT_BASE"),
        }
    ]

    def run(config):
        batch_id = config["batch_id"]
        write_path = config["write_path"]
        solver_endpoint = config["solver_endpoint"]

        print(f"Fetching intents from {solver_endpoint} starting at batch {batch_id}")
        print(f"Writing to {write_path}")

        loader = CowSwapIntentsLoader(solver_endpoint, write_path, _write_freq=15, _pause_ms=200)
        loader.fetch(batch_id)

    threads = [threading.Thread(target=run, args=(config,)) for config in configs]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    print("All threads finished 🎉")
