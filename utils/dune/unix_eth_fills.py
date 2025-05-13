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
QUERY_ID = os.getenv('DUNE_UNIX_FILLS_QUERY_ID')

dune = DuneClient.from_env()

DATE_FROM = "2024-10-01"
DATE_TO = "2024-12-15"
CHAIN = "ethereum"
PERFORMANCE = "medium"
REL_DIFF_LIMIT = 0.3

def get_unix_fills(
		tokens: str, 
		date_from: str = DATE_FROM,
		date_to: str = DATE_TO,
        performance: str = PERFORMANCE,
        rel_diff_limit: float = REL_DIFF_LIMIT
	):
    date_from = dune_utils.parse_date_str(date_from)
    date_to = dune_utils.parse_date_str(date_to)
    print(f"Fetching token price data for {tokens} with rel_diff_limit={rel_diff_limit} from {date_from} to {date_to} with query ID {QUERY_ID}")
    query = QueryBase(
		query_id=QUERY_ID,
		params=[
			QueryParameter.date_type(name="date_from", value=date_from),
			QueryParameter.date_type(name="date_to", value=date_to),
			QueryParameter.text_type(name="token_whitelist", value=tokens),
			QueryParameter.number_type(name="rel_diff_limit", value=rel_diff_limit)
		]
    )
    return dune.run_query(query=query, performance=performance)

def parse_args():
    parser = argparse.ArgumentParser(description="Fetch Fusion fills from Dune Analytics")
    parser.add_argument("--tokens", nargs="+", required=False, help="List of supported tokens symbols or addresses")
    parser.add_argument("--rel_diff_limit", type=float, default=REL_DIFF_LIMIT, help="Allowed relative difference between amount-in and amount-out USD value")
    parser.add_argument("--date-from", default=DATE_FROM, help="Start date (YYYY-MM-DD HH:MM:SS)")
    parser.add_argument("--date-to", default=DATE_TO, help="End date (YYYY-MM-DD HH:MM:SS)")
    parser.add_argument("--performance", default=PERFORMANCE, choices=["free", "medium", "large"], help="Query performance")
    parser.add_argument("--label", default=str(uuid.uuid4()), help="Data label")
    parser.add_argument("--out-dir", default="data", help="Output directory")
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    tokens = dune_utils.parse_tokens_for_chain(CHAIN, args.tokens)
    result = get_unix_fills(
        tokens=",".join(tokens),
        date_from=args.date_from,
        date_to=args.date_to,
        performance=args.performance,
        rel_diff_limit=args.rel_diff_limit
    )

    out_dir = dune_utils.parse_dir(args.out_dir, args.label)
    dune_utils.write_to_parquet(result.result.rows, out_dir)
    dune_utils.store_metadata(out_dir, tokens, args.date_from, args.date_to, CHAIN)
