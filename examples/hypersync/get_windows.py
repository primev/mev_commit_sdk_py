import asyncio
import polars as pl
from mev_commit_sdk_py.hypersync_client import Hypersync

# expand polars df output
pl.Config.set_fmt_str_lengths(200)
pl.Config.set_fmt_float("full")

# query window data
client = Hypersync(url='https://mev-commit.hypersync.xyz')

deposits_df = asyncio.run(client.get_window_deposits(1000000))

# print(deposits_df.head(5))

withdraws_df = asyncio.run(client.get_window_withdraws(1000000))
# print(withdraws_df.head(5))


# extract windows with funds still locked
address: str = "0xe51EF1836Dbef052BfFd2eB3Fe1314365d23129d".lower()

# extract dataframe columns to lists
deposit_windows: list[int] = deposits_df['windowNumber'].to_list()
withdraw_windows: list[int] = withdraws_df['window'].to_list()
# take the set difference between deposit_windows and withdraw_windows
windows: list[int] = list(set(deposit_windows) - set(withdraw_windows))
# convert float to integer
# windows: list[int] = [int(window) for window in windows]
print('windows with funds still locked')
print(windows)

# total the amount of ETH locked
total_deposited = sum(deposits_df['depositedAmount'].to_list())
total_withdrawn = sum(withdraws_df['amount'].to_list())

print(f'Total ETH deposited: {total_deposited / 10**18}')
print(f'Total ETH withdrawn: {total_withdrawn/ 10**18}')
print(f'Total ETH locked: {(total_deposited - total_withdrawn) / 10**18}')
