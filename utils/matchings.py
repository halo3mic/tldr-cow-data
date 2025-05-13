from multiprocess import Pool
from functools import partial
from typing import List, Dict

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

import importlib
import orderbook_rs
importlib.reload(orderbook_rs)
from orderbook_rs import Trade, MatchAnalysisPool, JobOptions, ExtRefPriceUpdate

from collections import defaultdict
from dataclasses import dataclass

BPS = 10_000


class Stat:
	def __init__(self, df: pd.DataFrame, col: str, weight_col: str, str_in_bps: bool = True):
		self.str_in_bps = str_in_bps
		self.mean = Stat.calc_mean(df, col)
		self.weighted_mean = Stat.calc_mean_w(df, col, weight_col)
		self.stddev = Stat.calc_stddev(df, col)
		self.weighted_stddev = Stat.calc_stddev_w(df, col, weight_col)

	@staticmethod
	def calc_mean(df: pd.DataFrame, col: str) -> float:
		return df[col].mean()
	
	@staticmethod
	def calc_mean_w(df: pd.DataFrame, col: str, weight_col: str) -> float:
		weighted_sum = df[weight_col] * df[col]
		return weighted_sum.sum() / df[weight_col].sum()

	@staticmethod
	def calc_stddev(df: pd.DataFrame, col: str) -> float:
		return np.std(df[col], ddof=1)
	
	@staticmethod
	def calc_stddev_w(df: pd.DataFrame, col: str, weight_col: str) -> float:
		weighted_diff = df[weight_col] * (df[col] - df[col].mean())**2
		return np.sqrt(weighted_diff.sum() / df[weight_col].sum())

	def __str__(self):
		m, u = (BPS, "[BPS]") if self.str_in_bps else (1, "")
		return f"{m*self.mean:.2f} ± {m*self.stddev:.2f} (W = {m*self.weighted_mean:.2f} ± {m*self.weighted_stddev:.2f}) {u}"
	


@dataclass
class MatchingOptions:
	base_asset: str
	quote_asset: str
	time_limit: int	= None
	min_delta: float = None
	batch_duration: int = 0

class MatchesStats:
	pi_only_matched: Stat
	pi_matched_only: Stat
	pi_with_mkt_fallback: Stat
	eff_pi_with_mkt_fallback: Stat

	wait_cost: Stat
	wait_cost_unmatched: Stat

	rel_matched_vol: float
	rel_matches: float
	total_trades: int
	total_volume_traded: float
	outliers: pd.DataFrame

	def __init__(self, df, trim_outliers=False):
		if trim_outliers:
			df = MatchesStats.trim_outliers(df)

		self.pi_no_fail_cost = Stat(df, "price_improvement", "amount_usd")
		self.pi_with_mkt_fallback = Stat(df, "price_improvement_with_cost", "amount_usd")
		self.eff_pi_with_mkt_fallback = Stat(df, "effective_price_improvement", "amount_usd")
		self.pi_matched_only = MatchesStats.calc_pi_only_matched_stats(df)

		self.wait_cost = Stat(df, "wait_cost", "amount_usd")
		self.wait_cost_unmatched = MatchesStats.calc_wait_cost_unmatched(df)
		
		self.rel_matched_vol = MatchesStats.calc_rel_matched_vol(df)
		self.rel_matches = MatchesStats.calc_rel_matches(df)
		self.total_trades = len(df)
		self.total_volume_traded = df["amount_usd"].sum()
		self.outliers = MatchesStats.get_outliers(df)

	def __str__(self):
		out = ""
		out += f"Price Improvement (No fail cost): {str(self.pi_no_fail_cost)}\n"
		out += f"Price Improvement (With Market Fallback): {str(self.pi_with_mkt_fallback)}\n"
		out += f"Effective Price Improvement (With Market Fallback): {str(self.eff_pi_with_mkt_fallback)}\n"
		out += f"Price Improvement (Only Matched): {str(self.pi_matched_only)}\n"

		out += f"Wait Cost: {str(self.wait_cost)}\n"
		out += f"Wait Cost (Unmatched): {str(self.wait_cost_unmatched)}\n"

		out += f"Relative Matched Volume: {self.rel_matched_vol:.2%}\n"
		out += f"Relative Matches: {self.rel_matches:.2%}\n"
		out += f"Total Trades: {self.total_trades:.0f}\n"
		out += f"Total Volume Traded: {self.total_volume_traded:.0f} USD\n"
		return out
	
	@staticmethod
	def calc_pi_only_matched_stats(df):
		only_matched = df[df["prop_matched"] > 0]
		only_matched.loc[:, "amount_usd_"] = only_matched["amount_usd"] * only_matched["prop_matched"]
		return Stat(only_matched, "price_improvement", "amount_usd_")

	@staticmethod
	def calc_wait_cost_unmatched(df):
		only_matched = df[df["prop_matched"] < 1]
		only_matched.loc[:, "amount_usd_"] = only_matched["amount_usd"] * (1 - only_matched["prop_matched"])
		return Stat(only_matched, "wait_cost", "amount_usd_")
	
	@staticmethod
	def calc_rel_matched_vol(df):
		amount_usd_sum = df["amount_usd"].sum()
		if amount_usd_sum == 0:
			return 0
		return (df["amount_usd"] * df["prop_matched"]).sum() / amount_usd_sum
	
	@staticmethod
	def calc_rel_matches(df):
		size = len(df)
		return len(df.loc[df["match_fills"] > 0] ) / size

	# Price improvement is not normally distributed, so we use IQR to cut outliers
	@staticmethod
	def trim_outliers(df, multiplier=1.5):
		Q1 = df['price_improvement'].quantile(0.25)
		Q3 = df['price_improvement'].quantile(0.75)
		IQR = Q3 - Q1
		lower_bound = Q1 - multiplier * IQR
		upper_bound = Q3 + multiplier * IQR
		return df[(df['price_improvement'] >= lower_bound) & 
				(df['price_improvement'] <= upper_bound)]
	
	@staticmethod
	def get_outliers(df, multiplier=1.5):
		Q1 = df['price_improvement'].quantile(0.25)
		Q3 = df['price_improvement'].quantile(0.75)
		IQR = Q3 - Q1
		lower_bound = Q1 - multiplier * IQR
		upper_bound = Q3 + multiplier * IQR
		return df[(df['price_improvement'] < lower_bound) | 
				(df['price_improvement'] > upper_bound)]


class DynamicMatchesResult:
	trades_df: pd.DataFrame
	matches_df: pd.DataFrame
	expired_orders: pd.DataFrame
	options: MatchingOptions

	def __init__(
			self, 
			pair, 
			trades_df,
			matches_df,
			expired_orders,
			options,
			inversed_prices
		):
		self.pair = pair
		self.options = options
		self.matches_df = matches_df
		self.expired_orders = expired_orders
		self.trades_df = trades_df
		self.inversed_prices = inversed_prices

	def make_enrich_matches(self, prices_df: pd.DataFrame=None):
		return enrich_matches(
			self.trades_df, 
			self.matches_df,
			self.expired_orders,
			self.options,
			prices_df=prices_df,
			inversed_prices=self.inversed_prices
		)
		
	def calc_stats(self, prices_df: pd.DataFrame=None, trim_outliers=False) -> MatchesStats:
		# Todo: no need to hold trades in memory if empty
		if self.matches_df.empty:
			return None 
		df = self.make_enrich_matches(prices_df)
		return MatchesStats(df, trim_outliers)
	
	def get_matched_for_trade(self, trade_id: str):
		enriched_matches_df = self.make_enrich_matches()
		is_ask = enriched_matches_df[lambda df: df["id"] == trade_id]["side"].iloc[0] == "ask"

		if is_ask:
			matched = self.matches_df.loc[self.matches_df["ask_id"] == trade_id]
			return enriched_matches_df.loc[enriched_matches_df["id"].isin(matched["bid_id"])]
		else:
			matched = self.matches_df.loc[self.matches_df["bid_id"] == trade_id]
			return enriched_matches_df.loc[enriched_matches_df["id"].isin(matched["ask_id"])]
		
	def plot_order(self, order_id: str):
		enriched_matches_df = self.make_enrich_matches()

		side_labels = ["Ask", "Bid"]

		batch_duration = self.options.batch_duration if self.options.batch_duration is not None else 0
		time_limit = self.options.time_limit if self.options.time_limit is not None else float('inf')
		em_df = enriched_matches_df
		matches_df = self.matches_df

		order = em_df[lambda x: x["id"] == order_id]
		if len(order) == 0:
			raise ValueError(f"Order {order_id} not found")
		order = order.iloc[0]

		timestamp_mask = lambda t: (t >= order["block_time"]-batch_duration) & (t <= order["block_time"] + time_limit)
		same_side_mask = lambda s: (s != order["side"])

		span_orders = em_df[lambda df: timestamp_mask(df["block_time"])]
		counter_orders = span_orders[lambda df: ~same_side_mask(df["side"])]
		same_side_orders = span_orders[lambda df: same_side_mask(df["side"])]

		_orders = em_df[["id", "block_time", "price_org"]]
		span_matches = matches_df.loc[lambda df: (df["bid_id"].isin(span_orders["id"])) | (df["ask_id"].isin(span_orders["id"]))]
		span_matches = span_matches.merge(_orders, left_on="bid_id", right_on="id", suffixes=("_match", "_bid"), how="left")
		span_matches = span_matches.merge(_orders, left_on="ask_id", right_on="id", suffixes=("_bid", "_ask"), how="left")


		for _, row in span_orders.iterrows():
			plt.annotate(f"${row['amount_usd']:.0f}", (row["block_time"], row["price_org"]), textcoords="offset points", xytext=(0, 5), ha='center')

		# Plot orders
		order_is_ask = order["side"] == "ask"
		plt.scatter(counter_orders["block_time"], counter_orders["price_org"], label=side_labels[not order_is_ask], color="#4e79a7")
		plt.scatter(same_side_orders["block_time"], same_side_orders["price_org"], label=side_labels[order_is_ask], color="#f28e2b")
		plt.axvline(order["block_time"], color="#e15759", label="Order start time", alpha=0.5, linestyle="--")
		plt.axvline(order["block_time"] + time_limit, color="#f28e2b", label="Order end time", alpha=0.5, linestyle="--")
		plt.axhline(order["price_org"], color="#76b7b2", label="Order price", alpha=0.5, linestyle="--")

		# Plot vertical lines indicating batches
		if batch_duration > 0:
			batch_offset = (order["block_time"] - em_df["block_time"].min()) % batch_duration
			x_ticks = np.arange(order["block_time"]-batch_offset, order["block_time"]+time_limit+1, batch_duration)
			for x in x_ticks:
				plt.axvline(x, color="black", alpha=0.5, linestyle="--", linewidth=0.7)

		# Plot matches between span orders
		ax = plt.gca()
		x_view_min, x_view_max = ax.get_xlim()
		y_view_min, y_view_max = ax.get_ylim()

		for _, row in span_matches.iterrows():
			is_input_order = row["bid_id"] == order_id or row["ask_id"] == order_id
			hex_color = "#3E2723" if is_input_order else "#9c755f"
			line = plt.plot([row["block_time_bid"], row["block_time_ask"]], 
							[row["price_org_bid"], row["price_org_ask"]], 
							color=hex_color, linewidth=0.5, linestyle="--")[0]
			line.set_clip_on(True)

		plt.xlim(x_view_min, x_view_max)
		plt.ylim(y_view_min, y_view_max)

		plt.ylabel("Price")
		plt.xlabel("Block Timestamp [sec]")
		plt.title(f"Order {order_id[:6]}... | Batch Duration: {batch_duration} | Time Limit: {time_limit}")
		plt.legend()

def enrich_matches(
		trades_df: pd.DataFrame, 
		matches_df: pd.DataFrame,
		expired_df: pd.DataFrame,
		options: MatchingOptions,
		prices_df: pd.DataFrame = None,
		inversed_prices: bool = None
	) -> pd.DataFrame:
	# todo: assume: prices and trades have the same market direction; there is "invert" column in trades_df 

	ROUND_DEC = np.finfo(float).precision-1
	base_asset = options.base_asset
	quote_asset = options.quote_asset
	time_limit = options.time_limit

	# Melt matches
	matches_melted_df = matches_df.melt(
		id_vars=["amount", "price", "timestamp", "ext_ref_price"], 
		value_vars=["bid_id", "ask_id"], 
		var_name="side", 
		value_name="id"
	).replace({"bid_id": "bid", "ask_id": "ask"}, inplace=False)
	matches_melted_df["matched_amount_base"] = matches_melted_df["amount"]
	matches_melted_df["matched_amount_quote"] = matches_melted_df["amount"] * matches_melted_df["price"]

	# Group by trade id
	matches_melted_df["price_x_amount"] = matches_melted_df["ext_ref_price"] * matches_melted_df["amount"]
	matches_melted_df["timestamp_x_amount"] = matches_melted_df["timestamp"] * matches_melted_df["amount"]
	matches_grouped_df = matches_melted_df \
		.sort_values("timestamp") \
		.groupby("id") \
		.agg(
			matched_amount_base=("matched_amount_base", "sum"), 
			matched_amount_quote=("matched_amount_quote", "sum"),
			match_time_max=("timestamp", "max"),
			price_x_amount=("price_x_amount", "sum"),
			timestamp_x_amount=("timestamp_x_amount", "sum"),
			# match_time_wmean=("timestamp", lambda x: np.average(x, weights=matches_melted_df.loc[x.index, "amount"])),
			# match_ext_ref_price_wmean=(
			# 	"ext_ref_price",
			# 	lambda s: (
			# 		np.average(s, weights=matches_melted_df.loc[s.index, "amount"])
			# 		if s.notna().any()
			# 		else np.nan
			# 	),
			# ),
			match_ex_ref_price_last=("ext_ref_price", "last"),
			match_fills=("side", "count"),
			# side=("side", "first")
		).reset_index()
	matches_grouped_df["match_ext_ref_price_wmean"] = matches_grouped_df["price_x_amount"] / matches_grouped_df["matched_amount_base"]
	matches_grouped_df["match_time_wmean"] = matches_grouped_df["timestamp_x_amount"] / matches_grouped_df["matched_amount_base"]

	# Merge trades with matches and expired orders
	trades_w_matches_df = trades_df.merge(matches_grouped_df.assign(is_matched = True), on="id", how="left")
	trades_w_matches_df = trades_w_matches_df.merge(
		expired_df.assign(is_expired = True).rename(columns={"ext_ref_price": "expiry_ext_ref_price"}), 
		on="id", 
		how="left"
	)
	trades_w_matches_df.loc[:, "is_matched"] = trades_w_matches_df["is_matched"].fillna(False)
	trades_w_matches_df.loc[:, "is_expired"] = trades_w_matches_df["is_expired"].fillna(False)

	# # Ignore trades that were neither matched nor did expire
	# not_matched_nor_expired_mask = ~trades_w_matches_df["is_matched"] & ~trades_w_matches_df["is_expired"]
	# print(f"Not matched nor expired: {sum(not_matched_nor_expired_mask)}")
	# trades_w_matches_df = trades_w_matches_df[~not_matched_nor_expired_mask]


	# Calculate wait time
	trades_w_matches_df["wait_time_wmean"] = trades_w_matches_df["match_time_wmean"] - trades_w_matches_df["block_time"]
	trades_w_matches_df["wait_time_max"] = trades_w_matches_df["match_time_max"] - trades_w_matches_df["block_time"]

	# Masks for ask and bid trades (if exact out the side is reversed)
	ask_mask = trades_w_matches_df["token_sold_address"] == base_asset
	trades_w_matches_df["is_ask"] = ask_mask

	# Calculate matched proportion

	trades_w_matches_df.loc[ask_mask, "prop_matched"] = trades_w_matches_df["matched_amount_base"] / trades_w_matches_df["token_sold_amount"]
	trades_w_matches_df.loc[~ask_mask, "prop_matched"] = trades_w_matches_df["matched_amount_quote"] / trades_w_matches_df["token_sold_amount"]
	trades_w_matches_df["prop_matched"] = trades_w_matches_df["prop_matched"].round(ROUND_DEC).fillna(0)

	
	# expect nan vals for trades that are not matched nor expired

	# todo
	# exact_out_mask = trades_w_matches_df.get("exact_out", pd.Series(False, index=trades_w_matches_df.index)).fillna(False)
	# trades_w_matches_df.loc[ask_mask & ~exact_out_mask, "prop_matched"] = trades_w_matches_df["matched_amount_base"] / trades_w_matches_df["token_sold_amount"]
	# trades_w_matches_df.loc[ask_mask & exact_out_mask, "prop_matched"] = trades_w_matches_df["matched_amount_quote"] / trades_w_matches_df["token_bought_amount"]
	# trades_w_matches_df.loc[~ask_mask & ~exact_out_mask, "prop_matched"] = trades_w_matches_df["matched_amount_quote"] / trades_w_matches_df["token_sold_amount"]
	# trades_w_matches_df.loc[~ask_mask & exact_out_mask, "prop_matched"] = trades_w_matches_df["matched_amount_base"] / trades_w_matches_df["token_bought_amount"]


	# todo: check proportions are correct by summing up the amounts

	# Calculate original price
	trades_w_matches_df.loc[ask_mask, "price_org"] = trades_w_matches_df["token_bought_amount"] / trades_w_matches_df["token_sold_amount"]
	trades_w_matches_df.loc[~ask_mask, "price_org"] = trades_w_matches_df["token_sold_amount"] / trades_w_matches_df["token_bought_amount"]

	# Calculate matched price (weighted average of fills)
	trades_w_matches_df["price_matched"] = (trades_w_matches_df["matched_amount_quote"] / trades_w_matches_df["matched_amount_base"]).fillna(0)
	# Calculate realized price: (matched_quote + remaining_quote) / (matched_base + remaining_base)
	trades_w_matches_df["price_realized"] = trades_w_matches_df["prop_matched"] * (trades_w_matches_df["price_matched"] - trades_w_matches_df["price_org"]) + trades_w_matches_df["price_org"]
	# Calculate relative price improvement
	trades_w_matches_df.loc[ask_mask, "price_improvement"] = (trades_w_matches_df["price_realized"] - trades_w_matches_df["price_org"]) / trades_w_matches_df["price_org"]
	trades_w_matches_df.loc[~ask_mask, "price_improvement"] = (trades_w_matches_df["price_org"] - trades_w_matches_df["price_realized"]) / trades_w_matches_df["price_org"]
	trades_w_matches_df["price_improvement"] = trades_w_matches_df["price_improvement"].round(ROUND_DEC)

	trades_w_matches_df.loc[ask_mask, "gross_pi"] = (trades_w_matches_df["price_matched"] - trades_w_matches_df["price_org"]) / trades_w_matches_df["price_org"]
	trades_w_matches_df.loc[~ask_mask, "gross_pi"] = (trades_w_matches_df["price_org"] - trades_w_matches_df["price_matched"]) / trades_w_matches_df["price_org"]

	if prices_df is not None or trades_w_matches_df["match_ext_ref_price_wmean"].sum() != 0:
		def rev_market_direction(base_s, quote_s):
			if ((base_s == base_asset) & (quote_s == quote_asset)).all():
				return False
			elif ((base_s == quote_asset) & (quote_s == base_asset)).all():
				return True
			else:
				raise Exception(f"Invalid pair")
			
		def inverse_prices(df, columns):
			for col in columns:
				df[col] = 1 / df[col]
			return df

		if prices_df is not None:
			if time_limit is not None:
				trades_w_matches_df.fillna({"match_time_max": trades_w_matches_df["block_time"] + time_limit}, inplace=True)
			else:
				assert trades_w_matches_df["match_time_max"].notna().all(), "match_time_max is null"

			trades_w_matches_df["match_time_max"] = trades_w_matches_df["match_time_max"].astype("int64")
			trades_w_matches_df.sort_values("match_time_max", inplace=True)
			prices_df.sort_values("block_time", inplace=True)

			trades_w_matches_df = pd.merge_asof(
				trades_w_matches_df,
				prices_df.rename(columns={"price": "end_mkt_price"}),
				by="pair",
				left_on=["match_time_max"],
				right_on=["block_time"],
				direction="forward",
				tolerance=7200, # todo: set this appropriately or have it as an option
				suffixes=("", "_y"),
			)

			na_market_price = trades_w_matches_df["end_mkt_price"].isna()
			assert not na_market_price.any(), f"end_mkt_price is null for {sum(na_market_price)/len(trades_w_matches_df):.%} rows"

			# Turn all market prices in the direction of the base-quote pair
			if rev_market_direction(prices_df["base_token"], prices_df["quote_token"]):
				# todo: we should not assume `creation_price` and `market_price_rel_offset` are turned the same way as `end_mkt_price`
				trades_w_matches_df = inverse_prices(trades_w_matches_df, ["end_mkt_price", "creation_price", "market_price_rel_offset"])
		else:
			# match_ext_ref_price_wmean (and end_mkt_price) is already in the direction of the base-quote pair
			trades_w_matches_df["end_mkt_price"] = (
				trades_w_matches_df["prop_matched"]
				* trades_w_matches_df["match_ext_ref_price_wmean"].fillna(0)
				+ (1 - trades_w_matches_df["prop_matched"]) * trades_w_matches_df["expiry_ext_ref_price"].fillna(trades_w_matches_df["match_ex_ref_price_last"])
			)
			# "creation_price", "market_price_rel_offset" might be in different direction
			assert inversed_prices is not None
			if inversed_prices:
				trades_w_matches_df = inverse_prices(trades_w_matches_df, ["creation_price", "market_price_rel_offset"])


		trades_w_matches_df["effective_expiry_market_price"] = trades_w_matches_df["end_mkt_price"] * trades_w_matches_df["market_price_rel_offset"]
		ask_mask = trades_w_matches_df["is_ask"]

		# Calculate realized price: (matched_quote + remaining_quote) / (matched_base + remaining_base)
		trades_w_matches_df["price_realized_with_cost"] = (
			(trades_w_matches_df["price_matched"] - trades_w_matches_df["effective_expiry_market_price"])
			* trades_w_matches_df["prop_matched"]
			+ trades_w_matches_df["effective_expiry_market_price"]
		)
		
		# Calculate relative price improvement
		trades_w_matches_df.loc[:, "price_improvement_with_cost"] = np.where(
			ask_mask,
			trades_w_matches_df["price_realized_with_cost"] / trades_w_matches_df["price_org"] -1,
			1 - trades_w_matches_df["price_realized_with_cost"] / trades_w_matches_df["price_org"]
		).round(ROUND_DEC)

		# Effective price improvement 
		trades_w_matches_df.loc[:, "effective_price_improvement"] = np.where(
			ask_mask,
			trades_w_matches_df["price_realized_with_cost"] / trades_w_matches_df["effective_expiry_market_price"] -1,
			1 - trades_w_matches_df["price_realized_with_cost"] / trades_w_matches_df["effective_expiry_market_price"]
		).round(ROUND_DEC)

		trades_w_matches_df["wait_cost"] = np.where(
			trades_w_matches_df["is_ask"],
			1 - trades_w_matches_df["end_mkt_price"] / trades_w_matches_df["creation_price"],
			trades_w_matches_df["end_mkt_price"] / trades_w_matches_df["creation_price"] - 1
		)
	else:
		trades_w_matches_df["price_improvement_with_cost"] = np.nan
		trades_w_matches_df["effective_price_improvement"] = np.nan
		trades_w_matches_df["wait_cost"] = np.nan

	if trades_w_matches_df["prop_matched"].max() > 1:
		misfits = trades_w_matches_df[trades_w_matches_df["prop_matched"] > 1]
		raise ValueError(f"prop_matched > 1 for {len(misfits)} rows")
	if trades_w_matches_df["price_improvement"].min() < 0:
		misfits = trades_w_matches_df[trades_w_matches_df["price_improvement"] < 0]
		trades_w_matches_df = trades_w_matches_df[trades_w_matches_df["price_improvement"] >= 0]
		raise ValueError(f"price_improvement < 0 for {len(misfits)} rows")

	return trades_w_matches_df

# todo: use mask for both prices and trades
# todo: port everything into Rust/Polars
# todo: test test test


class DynamicJobResults:

	def __init__(self, dyn_res: Dict[int, DynamicMatchesResult]):
		self.dyn_res = dyn_res

	def get_job_results(self, job_id: int) -> DynamicMatchesResult:
		return self.dyn_res[job_id]
	
	def get_aggregated_stats(
			self, 
			prices_df: pd.DataFrame=None, 
			trim_outliers=False,
			n_workers: int = None
	) -> pd.DataFrame:
		results = []
		for id, dyn_res in self.dyn_res.items():
			try:
				s = dyn_res.calc_stats(prices_df=prices_df, trim_outliers=trim_outliers)
			except Exception as e:
				print(f"Error for {dyn_res.pair}: {e}")
				continue
			if s is None:
				print(f"No results for {dyn_res.pair}")
				continue
			results.append(dict(
				pair=dyn_res.pair,
				time_limit=dyn_res.options.time_limit,
				batch_dur=dyn_res.options.batch_duration,
				pi_matched_only=s.pi_matched_only,
				pi_no_fail_cost=s.pi_no_fail_cost,
				pi_with_mkt_fallback=s.pi_with_mkt_fallback,
				eff_pi_with_mkt_fallback=s.eff_pi_with_mkt_fallback,
				wait_cost=s.wait_cost,
				wait_cost_unmatched=s.wait_cost_unmatched,
				rel_matched_vol=s.rel_matched_vol,
				rel_matches=s.rel_matches,
				total_volume_traded=s.total_volume_traded,
				total_trade_count=s.total_trades,
				job_id=id,
			))
			print(f"Job {id} done!")
		return pd.DataFrame(results)

		# worker_func = partial(DynamicJobResults._one_job, prices_df=prices_df, trim_outliers=trim_outliers)

		# with Pool(processes=n_workers) as pool:
		# 	rows = pool.map(worker_func, self.dyn_res.items())

		# return pd.DataFrame(rows)

	@staticmethod
	def _one_job(args, prices_df, trim_outliers):
		job_id, dyn_res = args
		s = dyn_res.calc_stats(prices_df, trim_outliers)
		print(f"Job {job_id} done")
		return dict(
			pair=dyn_res.pair,
			time_limit=dyn_res.options.time_limit,
			batch_dur=dyn_res.options.batch_duration,
			pi_matched_only=s.pi_matched_only,
			pi_no_fail_cost=s.pi_no_fail_cost,
			pi_with_mkt_fallback=s.pi_with_mkt_fallback,
			eff_pi_with_mkt_fallback=s.eff_pi_with_mkt_fallback,
			wait_cost=s.wait_cost,
			wait_cost_unmatched=s.wait_cost_unmatched,
			rel_matched_vol=s.rel_matched_vol,
			rel_matches=s.rel_matches,
			total_volume_traded=s.total_volume_traded,
			total_trade_count=s.total_trades,
			job_id=job_id,
		)

@dataclass
class Job:
	base_asset: str
	quote_asset: str
	trades_mask: np.ndarray
	options: List[JobOptions]

class PriceProvider:
	df: pd.DataFrame
	str_pairs: pd.Series
	unique_pairs: set

	def __init__(self, _df):
		self.df = _df
		self.str_pairs = _df["base_token"] + "_" + _df["quote_token"]
		self.unique_pairs = set(self.str_pairs.unique().tolist())
		# todo: ensure that pair is not provided in both directions

	def is_inversed(self, base_tkn, quote_tkn):
		if PriceProvider.pair_to_str(base_tkn, quote_tkn) in self.unique_pairs:
			return False
		elif PriceProvider.pair_to_str(quote_tkn, base_tkn) in self.unique_pairs:
			return True
		else: 
			raise Exception("Not found")
		
	def mask_for_pairs(self, pairs):
		pairs_str0 = [PriceProvider.pair_to_str(p[0], p[1]) for p in pairs]
		pairs_str1 = [PriceProvider.pair_to_str(p[1], p[0]) for p in pairs]
		mask = (self.str_pairs.isin(pairs_str0)) | (self.str_pairs.isin(pairs_str1))
		return mask

	@staticmethod
	def pair_to_str(t0, t1):
		return t0 + "_" + t1


class MatchAnalysis:
	price_provider: PriceProvider
	trades_df: pd.DataFrame
	jobs: List[Job]

	def __init__(self, trades_df: pd.DataFrame, prices_df: pd.DataFrame = None):
		self.trades_df = trades_df.sort_values("block_time")
		self.jobs = []
		self.price_provider = PriceProvider(prices_df) if prices_df is not None else None

	def add_job(
		self,
		base_asset: str,
		quote_asset: str,
		options: List[JobOptions] = [JobOptions()],
		mask=None
	):
		trades_mask = self.trades_mask(base_asset, quote_asset).tolist() if mask is None else mask
		if sum(trades_mask) == 0:
			print(f"No trades for {base_asset}/{quote_asset}")
			return
		self.jobs.append(Job(base_asset, quote_asset, trades_mask, options))
	
	def with_job(
		self,
		base_asset: str,
		quote_asset: str,
		options: List[JobOptions] = [JobOptions()]
	) -> "MatchAnalysis":
		self.add_job(base_asset, quote_asset, options)
		return self
	
	def execute(self, token_to_symbol=None) -> DynamicJobResults:
		trades = into_trades(self.trades_df)
		price_updates = self._price_updates()
		pool = MatchAnalysisPool(trades, price_updates)
		meta = self._add_jobs(pool)
		results = pool.execute()
		dyn_res = self._parse_exe_results(results, meta, token_to_symbol)
		return dyn_res

	def _parse_exe_results(self, results, meta, token_to_symbol=None) -> DynamicJobResults:
		dyn_results = defaultdict(list)
		for id, match_sim_result in results.items():
			matches = match_sim_result.matches
			expired_orders = match_sim_result.expired_orders
			(base_asset, quote_asset), option, trades_mask = meta[id]
			pair = f"{token_to_symbol[base_asset]}_{token_to_symbol[quote_asset]}" if token_to_symbol else f"{base_asset}/{quote_asset}"

			if len(matches) == 0:
				print(f"No results for {pair}")
				continue

			matches_df = pd.DataFrame(list(map(lambda x: x.to_dict(), matches)))
			expired_orders_df = pd.DataFrame(list(map(lambda x: x.to_dict(), expired_orders)))
			matching_opt = MatchingOptions(
				base_asset, 
				quote_asset,
				option.time_limit_sec,
				option.min_delta,
				option.batch_dur_sec
			)
			dyn_res = DynamicMatchesResult(
				pair,
				self.trades_df[trades_mask], 
				matches_df,
				expired_orders_df,
				matching_opt,
				self.price_provider.is_inversed(base_asset, quote_asset)
					if self.price_provider is not None
					else None
			 )
			dyn_results[id] = dyn_res

		return DynamicJobResults(dyn_results)
	
	def _add_jobs(self, pool):
		meta = defaultdict(dict)
		for job in self.jobs:
			ids = pool.add_job(
				job.base_asset,
				job.quote_asset,
				job.trades_mask,
				job.options
			)
			meta.update(dict((ids[i], ((job.base_asset, job.quote_asset), job.options[i], job.trades_mask)) for i in range(len(ids))))

		return meta
	
	def trades_mask(self, base_asset: str, quote_asset: str) -> np.ndarray:
		traded_tokens = [base_asset, quote_asset]
		return self.trades_df["token_bought_address"].isin(traded_tokens) & self.trades_df["token_sold_address"].isin(traded_tokens)

	def _price_updates(self):
		if self.price_provider is None:
			return None
		price_updates = defaultdict(lambda: defaultdict(list))
		price_mask = self._price_mask()
		for row in self.price_provider.df[price_mask].itertuples():
			price_updates[row.base_token][row.quote_token].append(ExtRefPriceUpdate(
				price=row.price,
				timestamp=row.block_time
			))
		return price_updates
	
	def _price_mask(self):
		pairs = {(job.base_asset, job.quote_asset) for job in self.jobs}
		mask = self.price_provider.mask_for_pairs(pairs)
		return mask


def into_trades(df: pd.DataFrame) -> List[Trade]:
	return list(map(
        lambda x: Trade(*x),
        _extract_trade_vals(df)
    ))

# todo: amount_usd is not neccessary here
# todo: rename block_time to smth else
def _extract_trade_vals(df: pd.DataFrame):
	_df = df[[
		"id", 
		"token_bought_address", 
		"token_sold_address", 
		"token_bought_amount", 
		"token_sold_amount", 
		"block_time", 
		"amount_usd"
	]].copy()
	_df.loc[:, "exact_out"] = df.get("exact_out", pd.Series(False, index=df.index)).fillna(False)
	_df.loc[:, "max_match_time"] = df.get("max_match_time", pd.Series(None, index=df.index))
	_df = _df.astype(object).where(pd.notnull(_df), None)
	return _df.to_numpy(dtype=object)