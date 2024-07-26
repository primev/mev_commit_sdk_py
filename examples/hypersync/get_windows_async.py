import asyncio
import polars as pl
from mev_commit_sdk_py.hypersync_client import Hypersync

# Expand Polars DataFrame output
pl.Config.set_fmt_str_lengths(200)
pl.Config.set_fmt_float("full")


async def main():
    # Initialize the client
    client = Hypersync(url='https://mev-commit.hypersync.xyz')

    # Run the queries concurrently
    deposits_df, withdraws_df = await asyncio.gather(
        client.get_window_deposits_v1(
            address='0xe51EF1836Dbef052BfFd2eB3Fe1314365d23129d'),
        client.get_window_withdraws_v1(
            address='0xe51EF1836Dbef052BfFd2eB3Fe1314365d23129d')
    )

    # Extract DataFrame columns to lists
    deposit_windows: list[int] = deposits_df['windowNumber'].to_list()
    withdraw_windows: list[int] = withdraws_df['window'].to_list()

    # Take the set difference between deposit_windows and withdraw_windows
    windows: list[int] = list(set(deposit_windows) - set(withdraw_windows))

    # Print windows with funds still locked
    print('Windows with funds still locked:')
    print(windows)

    # Total the amount of ETH locked
    total_deposited = sum(deposits_df.filter(
        pl.col('windowNumber').is_in(windows))['depositedAmount'].to_list())
    total_withdrawn = sum(withdraws_df.filter(
        pl.col('window').is_in(windows))['amount'].to_list())

    print(f'Total ETH deposited: {total_deposited / 10**18}')
    print(f'Total ETH withdrawn: {total_withdrawn / 10**18}')
    print(f'Total ETH locked: {(total_deposited - total_withdrawn) / 10**18}')

# Run the main function
asyncio.run(main())
