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

This library uses `rye` as the project manager. First install [rye](https://rye.astral.sh/guide/installation/). Then clone the repository and run `rye sync` to install the dependencies and get setup. Current Python version is 3.12.2. Alternatively a different virtual environment manager can be used as long as it can use Python 3.12.2.

### Testing
Run the unit tests with `python -m unittest discover -s tests` command.


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

### Query Preconf Commitment Data:
To query and build a DataFrame of precommitment data:
```python
import asyncio
import polars as pl
from mev_commit_sdk_py.hypersync_client import Hypersync

client = Hypersync(url='https://mev-commit.hypersync.xyz')

# Encrypted commits have the dispatchTimestamp, which is the time when the provider decides to open the commitment to reveal the data. 
encrypted_stores: pl.DataFrame = await client.execute_event_query('UnopenedCommitmentStored', from_block=from_block)

# Opened commits have all of the bidding data such as bidder, bid amount, and decay function parameters.
commit_stores: pl.DataFrame = await client.execute_event_query('OpenedCommitmentStored', from_block=from_block)

# Get commitment slashing
commits_processed = await client.execute_event_query('CommitmentProcessed', from_block=from_block)

# Polars join log data to get comprehensive commitment data
commitments_df: pl.DataFrame = (
    encrypted_stores
    .join(commit_stores, on='commitmentIndex', how='left')
    .with_columns(('0x' + pl.col("txnHash")).alias('txnHash'))
    .join(commits_processed.select('commitmentIndex', 'isSlash'), on='commitmentIndex', how='inner')
).select(
    'block_number', 'blockNumber', 'txnHash', 'bid', 'commiter', 'bidder',
    'isSlash', 'decayStartTimeStamp', 'decayEndTimeStamp', 'dispatchTimestamp',
    'commitmentHash', 'commitmentIndex', 'commitmentDigest', 'commitmentSignature',
    'revertingTxHashes', 'bidHash', 'bidSignature', 'sharedSecretKey'
)

print(commitments_df.head(5))
```


### Query Provider Slashing
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

## 