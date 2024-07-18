import polars as pl
from mev_commit_sdk_py.cryo_client import CryoClient


client = CryoClient()

# bidder address
address: str = "0xe51EF1836Dbef052BfFd2eB3Fe1314365d23129d".lower()

# get all the deposits and withdraws for an address
deposits_df: pl.DataFrame = client.get_window_deposits().filter(
    pl.col('bidder') == address)
withdraws_df: pl.DataFrame = client.get_window_withdraws(
).filter(pl.col('bidder') == address)

# extract dataframe columns to lists
deposit_windows = deposits_df['windowNumber_f64'].to_list()
withdraw_windows = withdraws_df['window_f64'].to_list()

# take the set difference between deposit_windows and withdraw_windows
windows = list(set(deposit_windows) - set(withdraw_windows))

# convert float to integer
windows = [int(window) for window in windows]
print('windows with funds still locked')
print(windows)
