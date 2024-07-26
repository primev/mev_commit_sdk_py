import asyncio
import polars as pl
from mev_commit_sdk_py.hypersync_client import Hypersync

# expand polars df output
pl.Config.set_fmt_str_lengths(200)
pl.Config.set_fmt_float("full")

# query window data
client = Hypersync(url='https://mev-commit.hypersync.xyz')

# commit_stores = asyncio.run(client.get_commit_stores())
# print(commit_stores)

encrypted_stores = asyncio.run(client.get_encrypted_commit_stores_v1())
print(encrypted_stores)
