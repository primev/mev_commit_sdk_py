import asyncio
import polars as pl
import nest_asyncio
from mev_commit_sdk_py.hypersync_client import Hypersync

# expand polars df output
pl.Config.set_fmt_str_lengths(200)
pl.Config.set_fmt_float("full")


client = Hypersync()

asyncio.run(client.fetch_data(block_range=1000000))

txs = pl.read_parquet('data/transactions.parquet')

groupby_df = txs.group_by('from').agg(pl.len().alias(
    'count')).sort(by='count', descending=True).head(10)
print(groupby_df)