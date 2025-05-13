from dune_client.client import DuneClient
from dotenv import load_dotenv
import argparse
import uuid
import os

import utils.dune.helpers as dune_utils


load_dotenv()

DUNE_API_KEY = os.getenv('DUNE_API_KEY')
MAX_AGE_HOURS = 1
DUNE = DuneClient.from_env()


def fetch_latest_results(query_id, max_age_hours=MAX_AGE_HOURS):
    if not query_id:
        raise ValueError("Query ID is required")
    print(f"Fetching latest results for query ID {query_id} with max age {max_age_hours} hours")
    return DUNE.get_latest_result(query_id, max_age_hours=max_age_hours)

def parse_args():
    parser = argparse.ArgumentParser(description="Fetch latest execution result from Dune Analytics")
    parser.add_argument("--query-id", required=True, help="Dune query ID")
    parser.add_argument("--max-age-hours", type=int, default=MAX_AGE_HOURS, help="Max age of the result in hours")
    parser.add_argument("--label", default=str(uuid.uuid4()), help="Data label")
    parser.add_argument("--out-dir", default="data", help="Output directory")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    result = fetch_latest_results(args.query_id, args.max_age_hours)

    out_dir = dune_utils.parse_dir(args.out_dir, args.label)
    dune_utils.write_to_parquet(result.result.rows, out_dir)
    dune_utils.store_latest_execution_metadata(result.execution_id, args.query_id, out_dir)