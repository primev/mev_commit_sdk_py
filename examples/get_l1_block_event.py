import asyncio
import polars as pl
from mev_commit_sdk_py.hypersync_client import Hypersync

# expand polars df output
pl.Config.set_fmt_str_lengths(200)
pl.Config.set_fmt_float("full")


client = Hypersync()

asyncio.run(client.get_new_l1_block_event_v1(block_range=10000))

# load parquet file
df = pl.read_parquet("data/decoded_logs.parquet")
print(df.head(5))
print(df.shape)
