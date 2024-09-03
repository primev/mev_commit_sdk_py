import asyncio
import polars as pl
from mev_commit_sdk_py.hypersync_client import Hypersync

# expand polars df output
pl.Config.set_fmt_str_lengths(200)
pl.Config.set_fmt_float("full")

holesky: str = 'https://eth.hypersync.xyz'
client = Hypersync(holesky)

tx_search = [
                    "0x410eec15e380c6f23c2294ad714487b2300dd88a7eaa051835e0da07f16fc282",
                    "0x110753637c9ead4b97c37a7a6a36c30015ffcd0effa5736c574de05d4f7adeb5",   
            ]

# query is stored in memory
df = asyncio.run(client.search_txs(txs=tx_search, save_data=False))

print(df)