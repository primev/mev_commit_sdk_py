import time
import hypersync
import polars as pl

from dataclasses import dataclass, field
from mev_commit_sdk_py.helpers import address_to_topic
from typing import List, Optional, Callable, Awaitable
from enum import Enum
from hypersync import TransactionField, DataType, BlockField


# Contract addresses for different components of the protocol


class Contracts(Enum):
    ORACLE = "0x6856Eb630C79D491886E104D328834643B3F69E3".lower()
    BLOCK_TRACKER = "0x2eEbF31f5c932D51556E70235FB98bB2237d065c".lower()
    BIDDER_REGISTER = "0x7ffa86fF89489Bca72Fec2a978e33f9870B2Bd25".lower()
    PROVIDER_REGISTRY = "0x4FC9b98e1A0Ff10de4c2cf294656854F1d5B207D".lower()
    COMMIT_STORE = "0xCAC68D97a56b19204Dd3dbDC103CB24D47A825A3".lower()


# Common transaction column mappings reused across events
COMMON_TRANSACTION_MAPPING = {
    TransactionField.GAS_USED: DataType.FLOAT64,
    TransactionField.MAX_PRIORITY_FEE_PER_GAS: DataType.FLOAT64,
    TransactionField.MAX_FEE_PER_GAS: DataType.FLOAT64,
    TransactionField.GAS_USED: DataType.FLOAT64,
    TransactionField.EFFECTIVE_GAS_PRICE: DataType.FLOAT64,
    TransactionField.NONCE: DataType.UINT64,
}

COMMMON_BLOCK_MAPPING = {
    BlockField.TIMESTAMP: DataType.UINT64,
    BlockField.BASE_FEE_PER_GAS: DataType.FLOAT64,
    BlockField.GAS_USED: DataType.UINT64,
}

# Event configurations with event names as keys, including signatures, contracts, and optional column mappings
EVENT_CONFIG = {
    "NewL1Block": {
        "signature": "NewL1Block(uint256 indexed blockNumber,address indexed winner,uint256 indexed window)",
        "contract": Contracts.BLOCK_TRACKER,
        "column_mapping": hypersync.ColumnMapping(transaction=COMMON_TRANSACTION_MAPPING, block=COMMMON_BLOCK_MAPPING)
    },

    "CommitmentProcessed": {
        "signature": "CommitmentProcessed(bytes32 indexed commitmentIndex, bool isSlash)",
        "contract": Contracts.ORACLE,
        "column_mapping": hypersync.ColumnMapping(transaction=COMMON_TRANSACTION_MAPPING, block=COMMMON_BLOCK_MAPPING)
    },

    "BidderRegistered": {
        "signature": "BidderRegistered(address indexed bidder, uint256 depositedAmount, uint256 windowNumber)",
        "contract": Contracts.BIDDER_REGISTER,
        "column_mapping": hypersync.ColumnMapping(
            decoded_log={'depositedAmount': hypersync.DataType.INT64,
                         'windowNumber': hypersync.DataType.INT64},
            transaction=COMMON_TRANSACTION_MAPPING, block=COMMMON_BLOCK_MAPPING
        )
    },

    "BidderWithdrawal": {
        "signature": "BidderWithdrawal(address indexed bidder, uint256 window, uint256 amount)",
        "contract": Contracts.BIDDER_REGISTER,
        "column_mapping": hypersync.ColumnMapping(
            decoded_log={'amount': hypersync.DataType.INT64,
                         'window': hypersync.DataType.INT64},
            transaction=COMMON_TRANSACTION_MAPPING, block=COMMMON_BLOCK_MAPPING
        )
    },

    "OpenedCommitmentStored": {
        "signature": "OpenedCommitmentStored(bytes32 indexed commitmentIndex, address bidder, address commiter, uint256 bid, uint64 blockNumber, bytes32 bidHash, uint64 decayStartTimeStamp, uint64 decayEndTimeStamp, string txnHash, string revertingTxHashes, bytes32 commitmentHash, bytes bidSignature, bytes commitmentSignature, uint64 dispatchTimestamp, bytes sharedSecretKey)",
        "contract": Contracts.COMMIT_STORE,
        "column_mapping": hypersync.ColumnMapping(
            decoded_log={
                "bid": hypersync.DataType.UINT64,
                "blockNumber": hypersync.DataType.UINT64,
                "decayStartTimeStamp": hypersync.DataType.UINT64,
                "decayEndTimeStamp": hypersync.DataType.UINT64,
                "dispatchTimestamp": hypersync.DataType.UINT64,
            },
            transaction=COMMON_TRANSACTION_MAPPING, block=COMMMON_BLOCK_MAPPING
        )
    },

    "FundsRetrieved": {
        "signature": "FundsRetrieved(bytes32 indexed commitmentDigest,address indexed bidder,uint256 window,uint256 amount)",
        "contract": Contracts.BIDDER_REGISTER,
        "column_mapping": hypersync.ColumnMapping(
            decoded_log={"window": hypersync.DataType.UINT64,
                         "amount": hypersync.DataType.UINT64},
            transaction=COMMON_TRANSACTION_MAPPING, block=COMMMON_BLOCK_MAPPING
        )
    },

    "FundsRewarded": {
        "signature": "FundsRewarded(bytes32 indexed commitmentDigest, address indexed bidder, address indexed provider, uint256 window, uint256 amount)",
        "contract": Contracts.BIDDER_REGISTER,
        "column_mapping": hypersync.ColumnMapping(
            decoded_log={"window": hypersync.DataType.UINT64,
                         "amount": hypersync.DataType.UINT64},
            transaction=COMMON_TRANSACTION_MAPPING, block=COMMMON_BLOCK_MAPPING
        )
    },

    "FundsSlashed": {
        "signature": "FundsSlashed(address indexed provider, uint256 amount)",
        "contract": Contracts.PROVIDER_REGISTRY,
        "column_mapping": hypersync.ColumnMapping(
            decoded_log={"amount": hypersync.DataType.UINT64},
            transaction=COMMON_TRANSACTION_MAPPING, block=COMMMON_BLOCK_MAPPING
        )
    },

    "FundsDeposited": {
        "signature": "FundsDeposited(address indexed provider, uint256 amount)",
        "contract": Contracts.PROVIDER_REGISTRY,
        "column_mapping": hypersync.ColumnMapping(
            decoded_log={"amount": hypersync.DataType.UINT64},
            transaction=COMMON_TRANSACTION_MAPPING, block=COMMMON_BLOCK_MAPPING
        )
    },

    "Withdraw": {
        "signature": "Withdraw(address indexed provider, uint256 amount)",
        "contract": Contracts.PROVIDER_REGISTRY,
        "column_mapping": hypersync.ColumnMapping(
            decoded_log={"amount": hypersync.DataType.UINT64},
            transaction=COMMON_TRANSACTION_MAPPING, block=COMMMON_BLOCK_MAPPING
        )
    },

    "ProviderRegistered": {
        "signature": "ProviderRegistered(address indexed provider, uint256 stakedAmount, bytes blsPublicKey)",
        "contract": Contracts.PROVIDER_REGISTRY,
        "column_mapping": hypersync.ColumnMapping(
            decoded_log={"stakedAmount": hypersync.DataType.UINT64},
            transaction=COMMON_TRANSACTION_MAPPING, block=COMMMON_BLOCK_MAPPING
        )
    },

    "UnopenedCommitmentStored": {
        "signature": "UnopenedCommitmentStored(bytes32 indexed commitmentIndex,address committer,bytes32 commitmentDigest,bytes commitmentSignature,uint64 dispatchTimestamp)",
        "contract": Contracts.COMMIT_STORE,
        "column_mapping": hypersync.ColumnMapping(
            decoded_log={"dispatchTimestamp": hypersync.DataType.UINT64},
            transaction=COMMON_TRANSACTION_MAPPING, block=COMMMON_BLOCK_MAPPING
        )
    }
}


def timer(func: Callable[..., Awaitable[None]]) -> Callable[..., Awaitable[None]]:
    """
    A decorator to measure and print the execution time of an asynchronous function.

    Args:
        func (Callable[..., Awaitable[None]]): The asynchronous function to measure.

    Returns:
        Callable[..., Awaitable[None]]: The wrapped function with timing functionality.
    """
    async def wrapper(*args, **kwargs):
        print_time = kwargs.pop('print_time', True)
        start_time = time.time()
        result = await func(*args, **kwargs)
        end_time = time.time()
        if print_time:
            print(f"{func.__name__} query finished in {
                  end_time - start_time:.2f} seconds.")
        return result
    return wrapper


@dataclass
class Hypersync:
    """
    A client wrapper around Hypersync Indexer to query transactions, blocks, and events from the blockchain.

    Attributes:
        url (str): The URL of the Hypersync service.
        client (hypersync.HypersyncClient): The Hypersync client instance, initialized in __post_init__.
    """
    url: str
    client: hypersync.HypersyncClient = field(init=False)

    def __post_init__(self):
        """Initialize the Hypersync client after the dataclass is instantiated."""
        self.client = hypersync.HypersyncClient(
            hypersync.ClientConfig(url=self.url))

    async def get_height(self) -> int:
        """
        Get the current block height from the blockchain.

        Returns:
            int: The current block height.
        """
        return await self.client.get_height()

    def create_query(self, from_block: int, to_block: int, logs: List[hypersync.LogSelection], transactions: Optional[List[hypersync.TransactionSelection]] = None, blocks: Optional[List[hypersync.BlockSelection]] = None) -> hypersync.Query:
        """
        Create a Hypersync query object for querying blockchain data.

        Args:
            from_block (int): The starting block number for the query.
            to_block (int): The ending block number for the query.
            logs (List[hypersync.LogSelection]): A list of log selections to filter the query.
            transactions (Optional[List[hypersync.TransactionSelection]]): Optional transaction selections for the query.

        Returns:
            hypersync.Query: The constructed query object.
        """
        return hypersync.Query(
            from_block=from_block,
            to_block=to_block,
            logs=logs,
            transactions=transactions or [],
            blocks=blocks or [],
            field_selection=hypersync.FieldSelection(
                log=[e.value for e in hypersync.LogField],
                transaction=[e.value for e in hypersync.TransactionField],
                block=[e.value for e in hypersync.BlockField],
            )
        )

    async def collect_data(self, query: hypersync.Query, config: hypersync.StreamConfig, save_data: bool, tx_data: bool = False) -> Optional[pl.DataFrame]:
        """
        Collect data using the Hypersync client and return it as a Polars DataFrame or save it as a parquet file.

        Args:
            query (hypersync.Query): The query object to execute.
            config (hypersync.StreamConfig): The configuration for the data stream.
            save_data (bool): Whether to save the data as a parquet file.
            tx_data (bool): Whether to include transaction data in the result.

        Returns:
            Optional[pl.DataFrame]: The collected data as a Polars DataFrame, or None if no data is returned.
        """
        if save_data:
            return await self.client.collect_parquet('data', query, config)
        else:
            data = await self.client.collect_arrow(query, config)
            decoded_logs_df = pl.from_arrow(data.data.decoded_logs)
            logs_df = pl.from_arrow(data.data.logs)
            transactions_df = pl.from_arrow(data.data.transactions)
            blocks_df = pl.from_arrow(data.data.blocks)

            txs_blocks_df = transactions_df.join(blocks_df.select(
                'number', 'timestamp', 'base_fee_per_gas', 'gas_used', 'parent_beacon_block_root').rename({'number': 'block_number'}), on='block_number', how='left', suffix='_block')

        if decoded_logs_df.is_empty() or logs_df.is_empty():
            # If both decoded_logs_df and logs_df are empty
            if txs_blocks_df.is_empty():
                return None  # All three DataFrames are empty
            else:
                  # Return txs_blocks_df if it's not empty
                return txs_blocks_df.select('hash', 'block_number', 'to', 'from', 'nonce', 'type', 'block_hash', 'timestamp', 'base_fee_per_gas', 'gas_used_block', 'parent_beacon_block_root', 'max_priority_fee_per_gas', 'max_fee_per_gas', 'effective_gas_price', 'gas_used')

        if tx_data:
            result_df = decoded_logs_df.hstack(
                logs_df.select('transaction_hash')
            ).rename({'transaction_hash': 'hash'}).join(
                txs_blocks_df.select('hash', 'block_number', 'to', 'from', 'nonce', 'type', 'block_hash', 'timestamp', 'base_fee_per_gas', 'gas_used_block', 'max_priority_fee_per_gas', 'max_fee_per_gas', 'effective_gas_price', 'gas_used'), on='hash', how='left'
            )
            return result_df
        else:
            return decoded_logs_df

    async def get_block_range(self, from_block: Optional[int] = None, to_block: Optional[int] = None, block_range: Optional[int] = None) -> dict[str, int]:
        """
        Determine the block range to be used in a query.

        Args:
            from_block (Optional[int]): The starting block number, optional.
            to_block (Optional[int]): The ending block number, optional.
            block_range (Optional[int]): The range of blocks, optional.

        Returns:
            dict[str, int]: A dictionary containing 'from_block' and 'to_block'.
        """
        to_block = to_block or await self.get_height()
        from_block = from_block or (
            to_block - block_range if block_range else 0)
        return {'from_block': from_block, 'to_block': to_block}

    def create_event_query(self, event_signature: str, from_block: int, to_block: int, address: Optional[str] = None) -> hypersync.Query:
        """
        Create a query for a specific event based on the event signature.

        Args:
            event_signature (str): The event signature to query.
            from_block (int): The starting block number for the query.
            to_block (int): The ending block number for the query.
            address (Optional[str]): Optional address to filter the event logs.

        Returns:
            hypersync.Query: The constructed query object.

        Raises:
            ValueError: If the event signature is not supported.
        """
        # Find the event configuration using the signature
        config = next((v for k, v in EVENT_CONFIG.items()
                      if v["signature"] == event_signature), None)
        if not config:
            raise ValueError(f"Unsupported event signature: {event_signature}")

        topic0 = hypersync.signature_to_topic0(event_signature)
        topics = [[topic0]]
        if address:
            topics.append([address_to_topic(address.lower())])

        return self.create_query(
            from_block=from_block,
            to_block=to_block,
            logs=[hypersync.LogSelection(
                address=[config["contract"].value], topics=topics)]
        )

    @timer
    async def execute_event_query(self, event_name: str, from_block: Optional[int] = None, to_block: Optional[int] = None, block_range: Optional[int] = None, save_data: bool = False, print_time: bool = True, address: Optional[str] = None, tx_data: bool = True) -> Optional[pl.DataFrame]:
        """
        Execute a query for a specific event by its name and collect the data.

        Available Events:
            - "NewL1Block": Tracks new L1 block events.
            - "CommitmentProcessed": Tracks when commitments are processed.
            - "BidderRegistered": Tracks bidder registration with deposited amount.
            - "BidderWithdrawal": Tracks bidder withdrawals.
            - "OpenedCommitmentStored": Tracks when opened commitments are stored.
            - "FundsRetrieved": Tracks when funds are retrieved from a bidder.
            - "FundsRewarded": Tracks when funds are rewarded from a bidder.
            - "FundsSlashed": Tracks when funds are slashed from a provider.
            - "FundsDeposited": Tracks when funds are deposited by a provider.
            - "Withdraw": Tracks provider withdrawals.
            - "ProviderRegistered": Tracks when a provider is registered.
            - "UnopenedCommitmentStored": Tracks when unopened commitments are stored.

        Args:
            event_name (str): The name of the event to query.
            from_block (Optional[int]): The starting block number, optional.
            to_block (Optional[int]): The ending block number, optional.
            block_range (Optional[int]): The range of blocks to query, optional.
            save_data (bool): Whether to save the data as a parquet file.
            print_time (bool): Whether to print the execution time of the query.
            address (Optional[str]): Optional address to filter the event logs.

        Returns:
            Optional[pl.DataFrame]: The collected data as a Polars DataFrame, or None if no data is returned.

        Raises:
            ValueError: If the event name is not supported or no data is returned.
        """
        # Retrieve the event configuration using the event name
        event_config = EVENT_CONFIG.get(event_name)
        if not event_config:
            raise ValueError(f"Unsupported event name: {event_name}")

        # Determine the block range for the query
        block_range_dict = await self.get_block_range(from_block, to_block, block_range)
        event_signature = event_config['signature']

        # Create the query object for the specified event
        query = self.create_event_query(
            event_signature, block_range_dict['from_block'], block_range_dict['to_block'], address)

        # Retrieve the column mapping for the event, if available
        column_mapping = event_config.get(
            "column_mapping", hypersync.ColumnMapping())

        # Configure the stream settings for the data collection
        config = hypersync.StreamConfig(
            hex_output=hypersync.HexOutput.PREFIXED,
            event_signature=event_signature,
            column_mapping=column_mapping
        )

        # Collect the data based on the query and configuration
        result = await self.collect_data(query, config, save_data, tx_data=tx_data)

        # Handle the case where no data is returned
        if result is None:
            raise ValueError(f"No data returned for event name: {event_name} from blocks {
                             block_range_dict['from_block']} to {block_range_dict['to_block']}")

        return result

    @timer
    async def get_blocks_txs(self, from_block: Optional[int] = None, to_block: Optional[int] = None, block_range: Optional[int] = None, save_data: bool = False, print_time: bool = True, blocks_only=False) -> Optional[pl.DataFrame]:
        """
        Query for blocks and transactions within a specified block range and optionally save results.

        Args:
            from_block (Optional[int]): The starting block number, optional.
            to_block (Optional[int]): The ending block number, optional.
            block_range (Optional[int]): The range of blocks to query, optional.
            save_data (bool): Whether to save the data as a parquet file.
            print_time (bool): Whether to print the execution time of the query.

        Returns:
            Optional[pl.DataFrame]: The collected blocks and transactions data as a Polars DataFrame, or None if no data is returned.
        """
        block_range_dict = await self.get_block_range(from_block, to_block, block_range)

        if blocks_only:
            query = self.create_query(
                from_block=block_range_dict['from_block'],
                to_block=block_range_dict['to_block'],
                logs=[],
                transactions=[]
                
            )
        else:
            query = self.create_query(
                from_block=block_range_dict['from_block'],
                to_block=block_range_dict['to_block'],
                logs=[],
                transactions=[hypersync.TransactionSelection()]
            )

        config = hypersync.StreamConfig(
            hex_output=hypersync.HexOutput.PREFIXED,
            column_mapping=hypersync.ColumnMapping(transaction=COMMON_TRANSACTION_MAPPING, block=COMMMON_BLOCK_MAPPING)
        )
        return await self.collect_data(query, config, save_data)

    @timer
    async def search_txs(self, txs: str | list[str], save_data: bool = False, print_time: bool = True) -> Optional[pl.DataFrame]:
        """
        Query for specific transactions or a list of transactions

        Args:
            save_data (bool): Whether to save the data as a parquet file.
            print_time (bool): Whether to print the execution time of the query.

        Returns:
            Optional[pl.DataFrame]: The collected blocks and transactions data as a Polars DataFrame, or None if no data is returned.
        """
        # Ensure txs is a list
        if isinstance(txs, str):
            txs = [txs]  # Convert single string to a list

        block_range_dict = await self.get_block_range(from_block=None)

        query = self.create_query(
            from_block=0,
            to_block=block_range_dict['to_block'],
            logs=[],
            transactions=[hypersync.TransactionSelection(
            hash=txs
            )]
        )

        config = hypersync.StreamConfig(
            hex_output=hypersync.HexOutput.PREFIXED,
            column_mapping=hypersync.ColumnMapping(transaction=COMMON_TRANSACTION_MAPPING, block=COMMMON_BLOCK_MAPPING)
        )
        return await self.collect_data(query, config, save_data)
    
    @timer
    async def get_blocks(self, from_block: Optional[int] = None, to_block: Optional[int] = None, block_range: Optional[int] = None, save_data: bool = False, print_time: bool = True) -> Optional[pl.DataFrame]:
        """
        Query for blocks within a specified block range and optionally save results.

        Args:
            from_block (Optional[int]): The starting block number, optional.
            to_block (Optional[int]): The ending block number, optional.
            block_range (Optional[int]): The range of blocks to query, optional.
            save_data (bool): Whether to save the data as a parquet file.
            print_time (bool): Whether to print the execution time of the query.

        Returns:
            Optional[pl.DataFrame]: The collected block data as a Polars DataFrame, or None if no data is returned.
        """
        # Get the block range to query
        block_range_dict = await self.get_block_range(from_block, to_block, block_range)

        # Create a query for blocks only
        query = self.create_query(
            from_block=block_range_dict['from_block'],
            to_block=block_range_dict['to_block'],
            logs=[],
            transactions=[],
            blocks=[hypersync.BlockSelection()]
        )

        # Configure the stream settings for blocks
        config = hypersync.StreamConfig(
            hex_output=hypersync.HexOutput.PREFIXED,
            column_mapping=hypersync.ColumnMapping(block=COMMMON_BLOCK_MAPPING)
        )

        # Collect block data
        data = await self.client.collect_arrow(query, config)
        blocks_df = pl.from_arrow(data.data.blocks)

        # Save data as parquet file if required
        if save_data and not blocks_df.is_empty():
            blocks_df.write_parquet("blocks_data.parquet")

        return blocks_df if not blocks_df.is_empty() else None