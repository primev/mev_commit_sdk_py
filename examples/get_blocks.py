import asyncio
import polars as pl
from mev_commit_sdk_py.hypersync_client import Hypersync

# expand polars df output
pl.Config.set_fmt_str_lengths(200)
pl.Config.set_fmt_float("full")

mev_commit: str = 'https://mev-commit.hypersync.xyz'
holesky: str = 'https://holesky.hypersync.xyz'
client = Hypersync(holesky)

blocks_df = asyncio.run(client.get_blocks(block_range=100))

print(blocks_df)