import asyncio
import polars as pl
from mev_commit_sdk_py.hypersync_client import Hypersync

# expand polars df output
pl.Config.set_fmt_str_lengths(200)
pl.Config.set_fmt_float("full")


client = Hypersync(url='https://mev-commit.hypersync.xyz')

l1_blocks = asyncio.run(client.get_new_l1_block_event_v1())

print(f'L1 blocks: {l1_blocks.shape}')

window_deposits = asyncio.run(client.get_window_deposits_v1())
print(f'Window deposits: {window_deposits.shape}')

window_withdrawals = asyncio.run(client.get_window_withdraws_v1())
print(f'Window withdraws: {window_withdrawals.shape}')

commit_stores = asyncio.run(client.get_commit_stores_v1())
print(f'Commit stores: {commit_stores.shape}')

encrypted_stores = asyncio.run(client.get_encrypted_commit_stores_v1())
print(f'Encrypted stores: {encrypted_stores.shape}')

commits_processed = asyncio.run(client.get_commits_processed_v1())
print(f'Commits processed: {commits_processed.shape}')

funds_retrieved = asyncio.run(client.get_funds_retrieved_v1())
print(f'Funds retrieved: {funds_retrieved.shape}')

funds_rewarded = asyncio.run(client.get_funds_rewarded_v1())
print(f'Funds rewarded: {funds_rewarded.shape}')

funds_slashed = asyncio.run(client.get_funds_slashed_v1())
print(f'Funds slashed: {funds_slashed.shape}')
