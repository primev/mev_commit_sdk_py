# mev-commit-sdk-py

## Introduction
The mev-commit-sdk-py is a Python-based SDK designed to interact with the MEV Commit protocol, enabling developers to efficiently query blockchain data such as events, transactions, and blocks. This SDK leverages Envio's Hypersync Indexer to streamline data retrieval from the MEV Commit chain.

## Features
* Event Querying: Retrieve and analyze specific events on the mev-commit blockchain.
* Block and Transaction Retrieval: Access detailed block and transaction data across a specified range of blocks.
* Data Transformation: Utilize Polars DataFrames to handle and process data efficiently.
Customizable Queries: Flexibility in creating custom queries based on specific needs, including event filtering by block range or address.

## Installation
Install with `pip install mev-commit-sdk-py`

This library uses `rye` as the project manager. First install [rye](https://rye.astral.sh/guide/installation/). Then clone the repository and run `rye sync` to install the dependencies and get setup. Current Python version is 3.10.12. Once installed, add a .env file with the variable `RPC='endpoint'` where endpoint is the url of the rpc server you want to connect to, this is for cryo. 

## Usage
This SDK is designed for ease of use in querying blockchain data. Below are some examples to help you get started:

## Event Querying
You can query specific events using event names:
* "NewL1Block": Tracks new L1 block events.
* "CommitmentProcessed": Tracks when commitments are processed.
* "BidderRegistered": Tracks bidder registration with deposited amount.
* "BidderWithdrawal": Tracks bidder withdrawals.
* "OpenedCommitmentStored": Tracks when opened commitments are stored.
* "FundsRetrieved": Tracks when funds are retrieved from a bidder.
* "FundsRewarded": Tracks when funds are rewarded from a bidder.
* "FundsSlashed": Tracks when funds are slashed from a provider.
* "FundsDeposited": Tracks when funds are deposited by a provider.
* "Withdraw": Tracks provider withdrawals.
* "ProviderRegistered": Tracks when a provider is registered.
* "UnopenedCommitmentStored": Tracks when unopened commitments are stored.


### Query Opened Commitments:
```python
import asyncio
import polars as pl
from mev_commit_sdk_py.hypersync_client import Hypersync

client = Hypersync(url='https://mev-commit.hypersync.xyz')

commit_stores = asyncio.run(client.execute_event_query('OpenedCommitmentStored'))
print(commit_stores.tail(10)) # tail gets most recent events
```

### Query Proposer Slashing
```python
import asyncio
import polars as pl
from mev_commit_sdk_py.hypersync_client import Hypersync

client = Hypersync(url='https://mev-commit.hypersync.xyz')

provider_slashes = asyncio.run(client.execute_event_query('FundsSlashed'))
# print(provider_slashes.tail(10)) # tail gets most recent events

provider_table = provider_slashes.with_columns(
    (pl.col('amount')/ 10**18).alias('slash_amt_eth')
).group_by('provider').agg(
    pl.col('slash_amt_eth').sum().alias('total_amt_slashed_eth'),
    pl.len().alias('slash_count')
)

print(provider_table)
```


### Block and Transaction Retrieval
To retrieve transactions and blocks for a specific range:

```python
import asyncio
import polars as pl
from mev_commit_sdk_py.hypersync_client import Hypersync

client = Hypersync(url='https://mev-commit.hypersync.xyz')

txs, blocks = asyncio.run(client.get_blocks_txs(from_block=0, to_block=100000))
print(txs.head())
print(blocks.head())
```
