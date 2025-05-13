from utils.const import token_map, default_pairs
import pandas as pd
import datetime
import json
import os

def parse_pairs_for_chain(chain, pairs=None):
    if chain not in token_map:
        raise KeyError(f"Unknown chain: {chain!r}")

    if pairs is None:
        _pairs = default_pairs[chain]
        return ["_".join(pair) for cat_pairs in _pairs.values() for pair in cat_pairs]
    else:
        raise NotImplementedError("Pair parsing not implemented")

def parse_tokens_for_chain(chain, tokens=None):
    if chain not in token_map:
        raise KeyError(f"Unknown chain: {chain!r}")
    chain_map = token_map[chain]

    if tokens is None:
        return list(chain_map.values())
    return [chain_map.get(tkn, tkn) for tkn in tokens]

def parse_dir(out_dir, label):
    out_dir = f"{out_dir}/{label}"
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    else: 
        print(f"Directory {out_dir} already exists. Overwrite? (y/n)")
        if input().lower() != "y":
            raise Exception("Directory already exists")
        
    return out_dir

def write_to_parquet(records, out_dir):
    path_out = f"{out_dir}/data.parquet"
    pd.DataFrame(records).to_parquet(path_out)
    print(f"Data saved to {path_out}")

def store_metadata(out_dir, tokens, date_from, date_to, chain):
    metadata = {
        "tokens": tokens,
        "date_from": date_from,
        "date_to": date_to,
        "chain": chain
    }
    _store_metadata(out_dir, metadata)

def store_latest_execution_metadata(out_dir, query_id, execution_id):
    metadata = {
        "query_id": query_id,
        "execution_id": execution_id
    }
    _store_metadata(out_dir, metadata)

def parse_date_str(date_str):
    dt = datetime.datetime.strptime(date_str, "%Y-%m-%d")
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def date_now():
    return datetime.datetime.now().strftime("%Y-%m-%d")

def _store_metadata(out_dir, metadata):
    with open(f"{out_dir}/metadata.json", "w") as f:
        json.dump(metadata, f, indent=4)
    print(f"Metadata saved to {out_dir}/metadata.json")