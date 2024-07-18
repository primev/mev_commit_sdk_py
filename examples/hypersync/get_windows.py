import asyncio
import polars as pl
from mev_commit_sdk_py.hypersync_client import Hypersync

# expand polars df output
pl.Config.set_fmt_str_lengths(200)
pl.Config.set_fmt_float("full")


client = Hypersync(url='https://mev-commit.hypersync.xyz')

df = asyncio.run(client.get_window_deposits(100000))

print(df)
