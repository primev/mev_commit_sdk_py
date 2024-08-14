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

# Initialize the Hypersync client
client = Hypersync(url='https://mev-commit.hypersync.xyz')

# Query window data using the new event names
commit_stores: pl.DataFrame = asyncio.run(
    client.execute_event_query('OpenedCommitmentStored'))
encrypted_stores: pl.DataFrame = asyncio.run(
    client.execute_event_query('UnopenedCommitmentStored'))

# Get commitment slashing
commits_processed = asyncio.run(
    client.execute_event_query('CommitmentProcessed'))

print(commit_stores.shape)
print(encrypted_stores.shape)
print(commits_processed.shape)

print(commit_stores.head(5))
