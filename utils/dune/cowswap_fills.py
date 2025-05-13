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
QUERY_ID = os.getenv('DUNE_COWSWAP_FILLS_QUERY_ID')

dune = DuneClient.from_env()

DATE_FROM = "2024-12-18"
DATE_TO = dune_utils.date_now()

CHAIN = "ethereum"
PERFORMANCE = "medium"


def get_cowswap_fills(
		tokens: str, 
		date_from: str = DATE_FROM,
		date_to: str = DATE_TO,
		chain: str = CHAIN,
        performance: str = PERFORMANCE
	):
    print(f"Fetching cowswap fills for {chain} tokens: '{tokens}' from {date_from} to {date_to}")
    query = QueryBase(
		query_id=QUERY_ID,
		params=[
			QueryParameter.date_type(name="date_from", value=dune_utils.parse_date_str(date_from)),
			QueryParameter.date_type(name="date_to", value=dune_utils.parse_date_str(date_to)),
			QueryParameter.text_type(name="token_whitelist", value=tokens),
			QueryParameter.text_type(name="chain", value=chain)
		]
    )
    return dune.run_query(query=query, performance=performance)

def parse_args():
    parser = argparse.ArgumentParser(description="Fetch CowSwap fills from Dune Analytics")
    parser.add_argument("--tokens", nargs="+", required=False, help="List of supported tokens symbols or addresses")
    parser.add_argument("--all-tokens", action="store_true", help="Whether to disable token filtering")
    parser.add_argument("--date-from", default=DATE_FROM, help="Start date (YYYY-MM-DD HH:MM:SS)")
    parser.add_argument("--date-to", default=DATE_TO, help="End date (YYYY-MM-DD HH:MM:SS)")
    parser.add_argument("--chain", default=CHAIN, help="Blockchain network")
    parser.add_argument("--performance", default=PERFORMANCE, choices=["free", "medium", "large"], help="Query performance")
    parser.add_argument("--label", default=str(uuid.uuid4()), help="Data label")
    parser.add_argument("--out-dir", default="data", help="Output directory")
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()

    tokens = "all"
    if not args.all_tokens:
        tokens = dune_utils.parse_tokens_for_chain(args.chain, args.tokens)
        tokens = ",".join(tokens)        

    result = get_cowswap_fills(
        tokens=tokens,
        date_from=args.date_from,
        date_to=args.date_to,
        chain=args.chain,
        performance=args.performance
    )

    out_dir = dune_utils.parse_dir(args.out_dir, args.label)
    dune_utils.write_to_parquet(result.result.rows, out_dir)
    dune_utils.store_metadata(out_dir, tokens, args.date_from, args.date_to, args.chain)