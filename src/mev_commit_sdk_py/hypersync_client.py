import time
import hypersync
import polars as pl

from dataclasses import dataclass, field
from mev_commit_sdk_py.helpers import address_to_topic
from typing import List, Optional, Callable, Awaitable
from hypersync import BlockField, TransactionField, HypersyncClient, ColumnMapping, DataType, LogSelection, FieldSelection, LogField, TransactionSelection

# https://docs.primev.xyz/developers/testnet#contract-addresses
# oracle contract
oracle_contract_v1: str = "0x6856Eb630C79D491886E104D328834643B3F69E3".lower()
# block tracker contract
block_tracker_contract_v1: str = "0x2eEbF31f5c932D51556E70235FB98bB2237d065c".lower()
bidder_register_contract_v1: str = "0x7ffa86fF89489Bca72Fec2a978e33f9870B2Bd25".lower()
provider_registry_contract_v1: str = "0x4FC9b98e1A0Ff10de4c2cf294656854F1d5B207D".lower()
commit_store_contract_v1: str = "0xCAC68D97a56b19204Dd3dbDC103CB24D47A825A3".lower()


def timer(func: Callable[..., Awaitable[None]]) -> Callable[..., Awaitable[None]]:
    """
    A decorator that measures the execution time of an asynchronous function.

    This decorator prints the time taken for the wrapped function to execute.
    It is useful for performance monitoring and debugging.

    Args:
        func (Callable[..., Awaitable[None]]): The asynchronous function to be wrapped.

    Returns:
        Callable[..., Awaitable[None]]: The wrapped function with timing functionality.
    """
    async def wrapper(*args, **kwargs):
        start_time = time.time()  # Start the timer
        result = await func(*args, **kwargs)
        end_time = time.time()  # End the timer
        duration = end_time - start_time
        print(f"{func.__name__} query finished in {duration:.2f} seconds.")
        return result
    return wrapper


@dataclass
class Hypersync:
    """
    Client wrapper aroound Hypersync Indexer to get transactions, blocks, and events.
    """
    url: str
    client: HypersyncClient = field(init=False)
    transactions: List[hypersync.TransactionField] = field(
        default_factory=list)
    blocks: List[hypersync.BlockField] = field(default_factory=list)

    def __post_init__(self):
        """
        Initialize the HypersyncClient after the dataclass is created.
        """
        self.client = HypersyncClient(
            hypersync.ClientConfig(
                url=self.url
            )
        )

    async def get_height(self) -> int:
        """
        Get the current block height from the blockchain.

        Returns:
            int: The current block height.
        """
        return await self.client.get_height()

    def create_query(self, from_block: int, to_block: int, logs: List[LogSelection], transactions: Optional[List[hypersync.TransactionSelection]] = None) -> hypersync.Query:
        """
        Create a hypersync query object.

        Args:
            from_block (int): The starting block number.
            to_block (int): The ending block number.
            logs (List[LogSelection]): The log selections for the query.
            transactions (Optional[List[hypersync.TransactionSelection]]): The transaction selections for the query.

        Returns:
            hypersync.Query: The created query object.
        """
        return hypersync.Query(
            from_block=from_block,
            to_block=to_block,
            logs=logs,
            transactions=transactions or [],
            field_selection=FieldSelection(
                log=[e.value for e in LogField],
                transaction=[e.value for e in TransactionField]
            )
        )

    async def collect_data(self, query: hypersync.Query, config: hypersync.StreamConfig, save_data: bool) -> Optional[pl.DataFrame]:
        """
        Collect data using the Hypersync client and returns output either as a polars dataframe or saves to disk as a parquet file.
        Only works for logs

        Args:
            query (hypersync.Query): The query object.
            config (hypersync.StreamConfig): The stream configuration.
            save_data (bool): Whether to save the data to a file.
            format (str): The format to save the data in ('parquet' or 'arrow').

        Returns:
            Optional[pl.DataFrame]: The collected data as a Polars DataFrame if save_data is False, otherwise None.
        """
        if save_data:
            return await self.client.collect_parquet('data', query, config)
        else:
            data = await self.client.collect_arrow(query, config)
            hstack_logs = pl.from_arrow(data.data.decoded_logs).hstack(
                pl.from_arrow(data.data.logs).select('transaction_hash'))

            return hstack_logs.rename({'transaction_hash': 'hash'}).join(pl.from_arrow(data.data.transactions), on='hash', how='left')

    async def get_block_range(self, from_block: Optional[int] = None, to_block: Optional[int] = None, block_range: Optional[int] = None) -> dict[str, int]:
        match (from_block, to_block, block_range):
            case (None, None, None):
                to_block = await self.get_height()
                from_block = 0
            case (None, None, block_range):
                to_block = await self.get_height()
                from_block = to_block - block_range
            case (None, to_block, block_range):
                from_block = to_block - block_range
            case (from_block, to_block, _):
                pass
        return {
            'from_block': from_block,
            'to_block': to_block
        }

    @timer
    async def get_blocks_txs(self, from_block: Optional[int] = None, to_block: Optional[int] = None, block_range: Optional[int] = None, save_data: bool = False) -> Optional[pl.DataFrame]:
        """
        Query for blocks and transactions and optionally save results.

        Args:
            from_block (Optional[int]): The block number to start the query from.
            to_block (Optional[int]): The block number to end the query at.
            block_range (Optional[int]): The number of blocks to include in the query.
            save_data (bool): Whether to save the data to a file.

        Returns:
            Optional[pl.DataFrame]: The collected data as a Polars DataFrame if save_data is False, otherwise None.
        """
        block_range_dict = await self.get_block_range(from_block, to_block, block_range)
        from_block, to_block = block_range_dict['from_block'], block_range_dict['to_block']

        query = hypersync.Query(
            from_block=from_block,
            to_block=to_block,
            include_all_blocks=True,
            transactions=[TransactionSelection()],
            field_selection=hypersync.FieldSelection(
                block=[e.value for e in BlockField],
                transaction=[e.value for e in TransactionField],
            )
        )

        config = hypersync.StreamConfig(
            hex_output=hypersync.HexOutput.PREFIXED,
            column_mapping=ColumnMapping(
                transaction={
                    TransactionField.GAS_USED: DataType.FLOAT64,
                    TransactionField.MAX_FEE_PER_BLOB_GAS: DataType.FLOAT64,
                    TransactionField.MAX_PRIORITY_FEE_PER_GAS: DataType.FLOAT64,
                    TransactionField.GAS_PRICE: DataType.FLOAT64,
                    TransactionField.CUMULATIVE_GAS_USED: DataType.FLOAT64,
                    TransactionField.EFFECTIVE_GAS_PRICE: DataType.FLOAT64,
                    TransactionField.NONCE: DataType.INT64,
                    TransactionField.GAS: DataType.FLOAT64,
                    TransactionField.MAX_FEE_PER_GAS: DataType.FLOAT64,
                    TransactionField.VALUE: DataType.FLOAT64,
                    TransactionField.CHAIN_ID: DataType.INT64,
                },
                block={
                    BlockField.GAS_LIMIT: DataType.FLOAT64,
                    BlockField.GAS_USED: DataType.FLOAT64,
                    BlockField.SIZE: DataType.FLOAT64,
                    BlockField.BLOB_GAS_USED: DataType.FLOAT64,
                    BlockField.EXCESS_BLOB_GAS: DataType.FLOAT64,
                    BlockField.BASE_FEE_PER_GAS: DataType.FLOAT64,
                    BlockField.TIMESTAMP: DataType.INT64,
                }
            )
        )
        if save_data:
            await self.client.collect_parquet('data', query, config)
        else:
            data = await self.client.collect_arrow(query, config)
            print(f"Collected data: {data}")
            blocks_df = pl.from_arrow(data.data.blocks)
            txs_df = pl.from_arrow(data.data.transactions)
            print(f"Blocks DataFrame shape: {blocks_df.shape}")
            print(f"Transactions DataFrame shape: {txs_df.shape}")
            return blocks_df, txs_df

    @timer
    async def get_new_l1_block_event_v1(self, from_block: Optional[int] = None, to_block: Optional[int] = None, block_range: Optional[int] = None, save_data: bool = False) -> Optional[pl.DataFrame]:
        """
        Query for new L1 block events and optionally save the data.

        Args:
            from_block (Optional[int]): The block number to start the query from.
            to_block (Optional[int]): The block number to end the query at.
            block_range (Optional[int]): The number of blocks to include in the query.
            save_data (bool): Whether to save the data to a file.

        Returns:
            Optional[pl.DataFrame]: The collected data as a Polars DataFrame if save_data is False, otherwise None.
        """
        block_range_dict = await self.get_block_range(from_block, to_block, block_range)
        from_block, to_block = block_range_dict['from_block'], block_range_dict['to_block']

        event_signature = "NewL1Block(uint256 indexed blockNumber,address indexed winner,uint256 indexed window)"
        topic0 = hypersync.signature_to_topic0(event_signature)
        query = self.create_query(
            from_block=from_block,
            to_block=to_block,
            logs=[LogSelection(
                address=[block_tracker_contract_v1], topics=[[topic0]])]
        )
        config = hypersync.StreamConfig(
            hex_output=hypersync.HexOutput.PREFIXED,
            event_signature=event_signature,
            column_mapping=ColumnMapping(
                transaction={
                    TransactionField.GAS_USED: DataType.FLOAT64,
                    TransactionField.MAX_FEE_PER_BLOB_GAS: DataType.FLOAT64,
                    TransactionField.MAX_PRIORITY_FEE_PER_GAS: DataType.FLOAT64,
                    TransactionField.GAS_PRICE: DataType.FLOAT64,
                    TransactionField.CUMULATIVE_GAS_USED: DataType.FLOAT64,
                    TransactionField.EFFECTIVE_GAS_PRICE: DataType.FLOAT64,
                    TransactionField.NONCE: DataType.INT64,
                    TransactionField.GAS: DataType.FLOAT64,
                    TransactionField.MAX_FEE_PER_GAS: DataType.FLOAT64,
                    TransactionField.VALUE: DataType.FLOAT64,
                    TransactionField.CHAIN_ID: DataType.INT64,
                }
            )
        )
        return await self.collect_data(query, config, save_data)

    @timer
    async def get_window_deposits_v1(self, address: Optional[str] = None, from_block: Optional[int] = None, to_block: Optional[int] = None, block_range: Optional[int] = None, save_data: bool = False) -> Optional[pl.DataFrame]:
        """
        Query for window deposit events and optionally save the data.

        Args:
            address (Optional[str]): The address to filter by.
            from_block (Optional[int]): The block number to start the query from.
            to_block (Optional[int]): The block number to end the query at.
            block_range (Optional[int]): The number of blocks to include in the query.
            save_data (bool): Whether to save the data to a file.

        Returns:
            Optional[pl.DataFrame]: The collected data as a Polars DataFrame if save_data is False, otherwise None.
        """
        block_range_dict = await self.get_block_range(from_block, to_block, block_range)
        from_block, to_block = block_range_dict['from_block'], block_range_dict['to_block']

        event_signature = "BidderRegistered(address indexed bidder, uint256 depositedAmount, uint256 windowNumber)"
        topic0 = hypersync.signature_to_topic0(event_signature)

        padded_address = address_to_topic(address.lower()) if address else None
        topics = [[topic0]]
        if padded_address:
            topics.append([padded_address])

        query = self.create_query(
            from_block=from_block,
            to_block=to_block,
            logs=[LogSelection(
                address=[bidder_register_contract_v1], topics=topics)]
        )
        config = hypersync.StreamConfig(
            hex_output=hypersync.HexOutput.PREFIXED,
            event_signature=event_signature,
            column_mapping=ColumnMapping(
                decoded_log={'depositedAmount': DataType.INT64,
                             'windowNumber': DataType.INT64}
            )
        )
        return await self.collect_data(query, config, save_data)

    @timer
    async def get_window_withdraws_v1(self, address: Optional[str] = None, from_block: Optional[int] = None, to_block: Optional[int] = None, block_range: Optional[int] = None, save_data: bool = False) -> Optional[pl.DataFrame]:
        """
        Query for window withdrawal events and optionally save the data.

        Args:
            address (Optional[str]): The address to filter by.
            from_block (Optional[int]): The block number to start the query from.
            to_block (Optional[int]): The block number to end the query at.
            block_range (Optional[int]): The number of blocks to include in the query.
            save_data (bool): Whether to save the data to a file.

        Returns:
            Optional[pl.DataFrame]: The collected data as a Polars DataFrame if save_data is False, otherwise None.
        """
        block_range_dict = await self.get_block_range(from_block, to_block, block_range)
        from_block, to_block = block_range_dict['from_block'], block_range_dict['to_block']

        event_signature = "BidderWithdrawal(address indexed bidder, uint256 window, uint256 amount)"
        topic0 = hypersync.signature_to_topic0(event_signature)

        padded_address = address_to_topic(address.lower()) if address else None
        topics = [[topic0]]
        if padded_address:
            topics.append([padded_address])

        query = self.create_query(
            from_block=from_block,
            to_block=to_block,
            logs=[LogSelection(
                address=[bidder_register_contract_v1], topics=topics)]
        )
        config = hypersync.StreamConfig(
            hex_output=hypersync.HexOutput.PREFIXED,
            event_signature=event_signature,
            column_mapping=ColumnMapping(
                decoded_log={'amount': DataType.INT64,
                             'window': DataType.INT64}
            )
        )
        return await self.collect_data(query, config, save_data)

    @timer
    async def get_commit_stores_v1(self, address: Optional[str] = None, from_block: Optional[int] = None, to_block: Optional[int] = None, block_range: Optional[int] = None, save_data: bool = False) -> Optional[pl.DataFrame]:
        """
        Retrieve CommitmentStored events from the blockchain.

        Args:
            address (Optional[str]): The specific address to filter events by. If None, events for all addresses are retrieved.
            from_block (Optional[int]): The block number to start the query from. Defaults to 0 if not provided.
            to_block (Optional[int]): The block number to end the query at. Defaults to the latest block if not provided.
            block_range (Optional[int]): The number of blocks to include in the query.
            save_data (bool): Whether to save the retrieved data to a file. Defaults to False.

        Returns:
            Optional[pl.DataFrame]: A DataFrame containing the retrieved events if save_data is False. Otherwise, returns None.

        Event Signature:
            CommitmentStored(
                bytes32 indexed commitmentIndex,
                address bidder,
                address commiter,
                uint256 bid,
                uint64 blockNumber,
                bytes32 bidHash,
                uint64 decayStartTimeStamp,
                uint64 decayEndTimeStamp,
                string txnHash,
                string revertingTxHashes,
                bytes32 commitmentHash,
                bytes bidSignature,
                bytes commitmentSignature,
                uint64 dispatchTimestamp,
                bytes sharedSecretKey
            )
        """
        block_range_dict = await self.get_block_range(from_block, to_block, block_range)
        from_block, to_block = block_range_dict['from_block'], block_range_dict['to_block']

        event_signature = "CommitmentStored(bytes32 indexed commitmentIndex, address bidder, address commiter, uint256 bid, uint64 blockNumber, bytes32 bidHash, uint64 decayStartTimeStamp, uint64 decayEndTimeStamp, string txnHash, string revertingTxHashes, bytes32 commitmentHash, bytes bidSignature, bytes commitmentSignature, uint64 dispatchTimestamp, bytes sharedSecretKey)"
        topic0 = hypersync.signature_to_topic0(event_signature)

        padded_address = address_to_topic(address.lower()) if address else None
        topics = [[topic0]]
        if padded_address:
            topics.append([padded_address])

        query = self.create_query(
            from_block=from_block,
            to_block=to_block,
            logs=[LogSelection(
                address=[commit_store_contract_v1], topics=topics)]
        )
        config = hypersync.StreamConfig(
            hex_output=hypersync.HexOutput.PREFIXED,
            event_signature=event_signature,
            column_mapping=ColumnMapping(
                decoded_log={
                    "bid": DataType.UINT64,
                    "blockNumber": DataType.UINT64,
                    "decayStartTimeStamp": DataType.UINT64,
                    "decayEndTimeStamp": DataType.UINT64,
                    "dispatchTimestamp": DataType.UINT64,
                },
                transaction={
                    TransactionField.GAS_USED: DataType.FLOAT64,
                    TransactionField.MAX_FEE_PER_BLOB_GAS: DataType.FLOAT64,
                    TransactionField.MAX_PRIORITY_FEE_PER_GAS: DataType.FLOAT64,
                    TransactionField.GAS_PRICE: DataType.FLOAT64,
                    TransactionField.CUMULATIVE_GAS_USED: DataType.FLOAT64,
                    TransactionField.EFFECTIVE_GAS_PRICE: DataType.FLOAT64,
                    TransactionField.NONCE: DataType.INT64,
                    TransactionField.GAS: DataType.FLOAT64,
                    TransactionField.MAX_FEE_PER_GAS: DataType.FLOAT64,
                    TransactionField.VALUE: DataType.FLOAT64,
                    TransactionField.CHAIN_ID: DataType.INT64,
                }
            )
        )
        return await self.collect_data(query, config, save_data)

    @timer
    async def get_encrypted_commit_stores_v1(self, address: Optional[str] = None, from_block: Optional[int] = None, to_block: Optional[int] = None, block_range: Optional[int] = None, save_data: bool = False) -> Optional[pl.DataFrame]:
        """
        Retrieve EncryptedCommitmentStored events from the blockchain.

        Args:
            address (Optional[str]): The specific address to filter events by. If None, events for all addresses are retrieved.
            from_block (Optional[int]): The block number to start the query from. Defaults to 0 if not provided.
            to_block (Optional[int]): The block number to end the query at. Defaults to the latest block if not provided.
            block_range (Optional[int]): The number of blocks to include in the query.
            save_data (bool): Whether to save the retrieved data to a file. Defaults to False.

        Returns:
            Optional[pl.DataFrame]: A DataFrame containing the retrieved events if save_data is False. Otherwise, returns None.

        Event Signature:
            EncryptedCommitmentStored(
                bytes32 indexed commitmentIndex,
                address commiter,
                bytes32 commitmentDigest,
                bytes commitmentSignature,
                uint64 dispatchTimestamp
            )
        """
        block_range_dict = await self.get_block_range(from_block, to_block, block_range)
        from_block, to_block = block_range_dict['from_block'], block_range_dict['to_block']

        event_signature = "EncryptedCommitmentStored(bytes32 indexed commitmentIndex, address commiter, bytes32 commitmentDigest, bytes commitmentSignature, uint64 dispatchTimestamp)"
        topic0 = hypersync.signature_to_topic0(event_signature)
        query = self.create_query(
            from_block=from_block,
            to_block=to_block,
            logs=[
                LogSelection(
                    address=[commit_store_contract_v1],
                    topics=[[topic0]],
                )
            ],
        )
        config = hypersync.StreamConfig(
            hex_output=hypersync.HexOutput.PREFIXED,
            event_signature=event_signature,
            column_mapping=ColumnMapping(
                decoded_log={
                    "dispatchTimestamp": DataType.UINT64,
                },
                transaction={
                    TransactionField.GAS_USED: DataType.FLOAT64,
                    TransactionField.MAX_FEE_PER_BLOB_GAS: DataType.FLOAT64,
                    TransactionField.MAX_PRIORITY_FEE_PER_GAS: DataType.FLOAT64,
                    TransactionField.GAS_PRICE: DataType.FLOAT64,
                    TransactionField.CUMULATIVE_GAS_USED: DataType.FLOAT64,
                    TransactionField.EFFECTIVE_GAS_PRICE: DataType.FLOAT64,
                    TransactionField.NONCE: DataType.INT64,
                    TransactionField.GAS: DataType.FLOAT64,
                    TransactionField.MAX_FEE_PER_GAS: DataType.FLOAT64,
                    TransactionField.VALUE: DataType.FLOAT64,
                    TransactionField.CHAIN_ID: DataType.INT64,
                }
            )
        )
        return await self.collect_data(query, config, save_data)

    @timer
    async def get_commits_processed_v1(self, from_block: Optional[int] = None, to_block: Optional[int] = None, block_range: Optional[int] = None, save_data: bool = False) -> Optional[pl.DataFrame]:
        """
        Retrieve CommitmentProcessed events from the blockchain.

        Args:
            from_block (Optional[int]): The block number to start the query from. Defaults to 0 if not provided.
            to_block (Optional[int]): The block number to end the query at. Defaults to the latest block if not provided.
            block_range (Optional[int]): The number of blocks to include in the query.
            save_data (bool): Whether to save the retrieved data to a file. Defaults to False.

        Returns:
            Optional[pl.DataFrame]: A DataFrame containing the retrieved events if save_data is False. Otherwise, returns None.

        Event Signature:
            CommitmentProcessed(
                bytes32 indexed commitmentIndex,
                bool isSlash
            )
        """
        block_range_dict = await self.get_block_range(from_block, to_block, block_range)
        from_block, to_block = block_range_dict['from_block'], block_range_dict['to_block']

        event_signature = "CommitmentProcessed(bytes32 indexed commitmentIndex, bool isSlash)"
        topic0 = hypersync.signature_to_topic0(event_signature)
        query = self.create_query(
            from_block=from_block,
            to_block=to_block,
            logs=[LogSelection(address=[oracle_contract_v1],
                               topics=[[topic0]])]
        )
        config = hypersync.StreamConfig(
            hex_output=hypersync.HexOutput.PREFIXED,
            event_signature=event_signature,
        )
        return await self.collect_data(query, config, save_data)

    @timer
    async def get_funds_retrieved_v1(self, from_block: Optional[int] = None, to_block: Optional[int] = None, block_range: Optional[int] = None, save_data: bool = False) -> Optional[pl.DataFrame]:
        """
        "FundsRetrieved(bytes32 indexed commitmentDigest,address indexed bidder,uint256 window,uint256 amount)"

        Args:
            from_block (Optional[int]): The block number to start the query from.
            to_block (Optional[int]): The block number to end the query at.
            block_range (Optional[int]): The number of blocks to include in the query.
            save_data (bool): Whether to save the data to a file.

        Returns:
            Optional[pl.DataFrame]: The collected data as a Polars DataFrame if save_data is False, otherwise None.
        """
        block_range_dict = await self.get_block_range(from_block, to_block, block_range)
        from_block, to_block = block_range_dict['from_block'], block_range_dict['to_block']

        event_signature = "FundsRetrieved(bytes32 indexed commitmentDigest,address indexed bidder,uint256 window,uint256 amount)"
        topic0 = hypersync.signature_to_topic0(event_signature)
        query = self.create_query(
            from_block=from_block,
            to_block=to_block,
            logs=[LogSelection(
                address=[bidder_register_contract_v1], topics=[[topic0]])]
        )
        config = hypersync.StreamConfig(
            hex_output=hypersync.HexOutput.PREFIXED,
            event_signature=event_signature,
            column_mapping=ColumnMapping(
                decoded_log={
                    "window": DataType.UINT64,
                    "amount": DataType.UINT64
                },
                transaction={
                    TransactionField.GAS_USED: DataType.FLOAT64,
                    TransactionField.MAX_FEE_PER_BLOB_GAS: DataType.FLOAT64,
                    TransactionField.MAX_PRIORITY_FEE_PER_GAS: DataType.FLOAT64,
                    TransactionField.GAS_PRICE: DataType.FLOAT64,
                    TransactionField.CUMULATIVE_GAS_USED: DataType.FLOAT64,
                    TransactionField.EFFECTIVE_GAS_PRICE: DataType.FLOAT64,
                    TransactionField.NONCE: DataType.INT64,
                    TransactionField.GAS: DataType.FLOAT64,
                    TransactionField.MAX_FEE_PER_GAS: DataType.FLOAT64,
                    TransactionField.VALUE: DataType.FLOAT64,
                    TransactionField.CHAIN_ID: DataType.INT64,
                }
            )
        )
        return await self.collect_data(query, config, save_data)

    @timer
    async def get_funds_rewarded_v1(self, from_block: Optional[int] = None, to_block: Optional[int] = None, block_range: Optional[int] = None, save_data: bool = False) -> Optional[pl.DataFrame]:
        """
        `FundsRewarded(bytes32 indexed commitmentDigest, address indexed bidder, address indexed provider, uint256 window, uint256 amount)`

        Args:
            from_block (Optional[int]): The block number to start the query from.
            to_block (Optional[int]): The block number to end the query at.
            block_range (Optional[int]): The number of blocks to include in the query.
            save_data (bool): Whether to save the data to a file.

        Returns:
            Optional[pl.DataFrame]: The collected data as a Polars DataFrame if save_data is False, otherwise None.
        """
        block_range_dict = await self.get_block_range(from_block, to_block, block_range)
        from_block, to_block = block_range_dict['from_block'], block_range_dict['to_block']

        event_signature = "FundsRewarded(bytes32 indexed commitmentDigest, address indexed bidder, address indexed provider, uint256 window, uint256 amount)"
        topic0 = hypersync.signature_to_topic0(event_signature)
        query = self.create_query(
            from_block=from_block,
            to_block=to_block,
            logs=[LogSelection(
                address=[bidder_register_contract_v1], topics=[[topic0]])]
        )
        config = hypersync.StreamConfig(
            hex_output=hypersync.HexOutput.PREFIXED,
            event_signature=event_signature,
            column_mapping=ColumnMapping(
                decoded_log={
                    "window": DataType.UINT64,
                    "amount": DataType.UINT64
                },
                transaction={
                    TransactionField.GAS_USED: DataType.FLOAT64,
                    TransactionField.MAX_FEE_PER_BLOB_GAS: DataType.FLOAT64,
                    TransactionField.MAX_PRIORITY_FEE_PER_GAS: DataType.FLOAT64,
                    TransactionField.GAS_PRICE: DataType.FLOAT64,
                    TransactionField.CUMULATIVE_GAS_USED: DataType.FLOAT64,
                    TransactionField.EFFECTIVE_GAS_PRICE: DataType.FLOAT64,
                    TransactionField.NONCE: DataType.INT64,
                    TransactionField.GAS: DataType.FLOAT64,
                    TransactionField.MAX_FEE_PER_GAS: DataType.FLOAT64,
                    TransactionField.VALUE: DataType.FLOAT64,
                    TransactionField.CHAIN_ID: DataType.INT64,
                }
            )
        )
        return await self.collect_data(query, config, save_data)

    @timer
    async def get_funds_slashed_v1(self, from_block: Optional[int] = None, to_block: Optional[int] = None, block_range: Optional[int] = None, save_data: bool = False) -> Optional[pl.DataFrame]:
        """
        `FundsSlashed(address indexed provider, uint256 amount)`

        Args:
            from_block (Optional[int]): The block number to start the query from.
            to_block (Optional[int]): The block number to end the query at.
            block_range (Optional[int]): The number of blocks to include in the query.
            save_data (bool): Whether to save the data to a file.

        Returns:
            Optional[pl.DataFrame]: The collected data as a Polars DataFrame if save_data is False, otherwise None.
        """
        block_range_dict = await self.get_block_range(from_block, to_block, block_range)
        from_block, to_block = block_range_dict['from_block'], block_range_dict['to_block']

        event_signature = "FundsSlashed(address indexed provider, uint256 amount)"
        topic0 = hypersync.signature_to_topic0(event_signature)
        query = self.create_query(
            from_block=from_block,
            to_block=to_block,
            logs=[LogSelection(
                address=[provider_registry_contract_v1], topics=[[topic0]])]
        )
        config = hypersync.StreamConfig(
            hex_output=hypersync.HexOutput.PREFIXED,
            event_signature=event_signature,
            column_mapping=ColumnMapping(
                decoded_log={
                    "amount": DataType.UINT64
                },
                transaction={
                    TransactionField.GAS_USED: DataType.FLOAT64,
                    TransactionField.MAX_FEE_PER_BLOB_GAS: DataType.FLOAT64,
                    TransactionField.MAX_PRIORITY_FEE_PER_GAS: DataType.FLOAT64,
                    TransactionField.GAS_PRICE: DataType.FLOAT64,
                    TransactionField.CUMULATIVE_GAS_USED: DataType.FLOAT64,
                    TransactionField.EFFECTIVE_GAS_PRICE: DataType.FLOAT64,
                    TransactionField.NONCE: DataType.INT64,
                    TransactionField.GAS: DataType.FLOAT64,
                    TransactionField.MAX_FEE_PER_GAS: DataType.FLOAT64,
                    TransactionField.VALUE: DataType.FLOAT64,
                    TransactionField.CHAIN_ID: DataType.INT64,
                }
            )
        )
        return await self.collect_data(query, config, save_data)
