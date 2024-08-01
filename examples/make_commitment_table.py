import asyncio
import hvplot
import polars as pl
import nest_asyncio
from mev_commit_sdk_py.hypersync_client import Hypersync

# expand polars df output
pl.Config.set_fmt_str_lengths(200)
pl.Config.set_fmt_float("full")

# to run asyncio loop in notebook
nest_asyncio.apply()

hvplot.extension('plotly')


# query window data
client = Hypersync(url='https://mev-commit.hypersync.xyz')
commit_stores: pl.DataFrame = asyncio.run(client.get_commit_stores_v1())
encrypted_stores: pl.DataFrame = asyncio.run(
    client.get_encrypted_commit_stores_v1())

# get commitment slashing
commits_processed = asyncio.run(client.get_commits_processed_v1())
