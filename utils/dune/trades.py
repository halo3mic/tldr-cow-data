from dotenv import load_dotenv
import os
import argparse
import uuid

from dune_client.types import QueryParameter
from dune_client.client import DuneClient
from dune_client.query import QueryBase

import utils.dune.helpers as dune_utils


load_dotenv()

DUNE_API_KEY = os.getenv('DUNE_API_KEY')
QUERY_ID = os.getenv('DUNE_TRADES_QUERY_ID')

dune = DuneClient.from_env()

DATE_FROM = "2024-10-01"
DATE_TO = "2024-11-01"
CHAIN = "ethereum"
PERFORMANCE = "large"

def get_dex_trades(
		pairs: str, 
		date_from: str = DATE_FROM,
		date_to: str = DATE_TO,
		chain: str = CHAIN,
        performance: str = PERFORMANCE,
        allowed_rel_diff: float = 0.03,
	):
    date_from = dune_utils.parse_date_str(date_from)
    date_to = dune_utils.parse_date_str(date_to)
    print(f"Fetching DEX trades data for {chain} {pairs} from {date_from} to {date_to}")
    query = QueryBase(
		query_id=QUERY_ID,
		params=[
			QueryParameter.date_type(name="date_from", value=date_from),
			QueryParameter.date_type(name="date_to", value=date_to),
			QueryParameter.text_type(name="pair_whitelist", value=pairs),
			QueryParameter.text_type(name="chain", value=chain),
            QueryParameter.text_type(name="allowed_rel_diff", value=allowed_rel_diff),
		]
    )
    return dune.run_query(query=query, performance=performance)

def parse_args():
    parser = argparse.ArgumentParser(description="Fetch DEX trades data from Dune Analytics")
    parser.add_argument("--pairs", nargs="+", required=False, help="List of pairs symbols in the format: 'weth-dai,usdc-wbtc'. If not specified, default pairs will be used.")
    parser.add_argument("--date-from", default=DATE_FROM, help="Start date (YYYY-MM-DD HH:MM:SS)")
    parser.add_argument("--date-to", default=DATE_TO, help="End date (YYYY-MM-DD HH:MM:SS)")
    parser.add_argument("--chain", default=CHAIN, help="Blockchain network")
    parser.add_argument("--performance", default=PERFORMANCE, choices=["free", "medium", "large"], help="Query performance")
    parser.add_argument("--label", default=str(uuid.uuid4()), help="Data label")
    parser.add_argument("--out-dir", default="data", help="Output directory")
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    pairs = dune_utils.parse_pairs_for_chain(args.chain, args.pairs)
    result = get_dex_trades(
        pairs=",".join(pairs),
        date_from=args.date_from,
        date_to=args.date_to,
        chain=args.chain,
        performance=args.performance
    )

    out_dir = dune_utils.parse_dir(args.out_dir, args.label)
    dune_utils.write_to_parquet(result.result.rows, out_dir)
    dune_utils.store_metadata(out_dir, pairs, args.date_from, args.date_to, args.chain)