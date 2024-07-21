import time
import hypersync
from dataclasses import dataclass, field
import polars as pl
from mev_commit_sdk_py.helpers import address_to_topic
from typing import List, Optional, Callable, Awaitable
from hypersync import BlockField, TransactionField, HypersyncClient, ColumnMapping, DataType, LogSelection, FieldSelection, LogField

# https://docs.primev.xyz/developers/testnet#contract-addresses
# oracle contract
oracle_contract: str = "0x6856Eb630C79D491886E104D328834643B3F69E3".lower()
# block tracker contract
block_tracker_contract: str = "0x2eEbF31f5c932D51556E70235FB98bB2237d065c".lower()
bidder_register_contract: str = "0x7ffa86fF89489Bca72Fec2a978e33f9870B2Bd25".lower()


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

    async def collect_data(self, query: hypersync.Query, config: hypersync.StreamConfig, save_data: bool, format: str = 'parquet') -> Optional[pl.DataFrame]:
        """
        Collect data using the Hypersync client and returns output either as a polars dataframe or saves to disk as a parquet file. 

        Args:
            query (hypersync.Query): The query object.
            config (hypersync.StreamConfig): The stream configuration.
            save_data (bool): Whether to save the data to a file.
            format (str): The format to save the data in ('parquet' or 'arrow').

        Returns:
            Optional[pl.DataFrame]: The collected data as a Polars DataFrame if save_data is False, otherwise None.
        """
        if save_data:
            if format == 'parquet':
                return await self.client.collect_parquet('data', query, config)
        else:
            data = await self.client.collect_arrow(query, config)
            return pl.from_arrow(data.data.decoded_logs)

    @timer
    async def get_blocks_txs(self, block_range: int, start_block: int = 0) -> None:
        """
        Query for blocks and transactions and save results as parquet files.

        Args:
            block_range (int): The number of blocks to include in the query.
            start_block (int): The block number to start the query from.
        """
        height = await self.get_height() if start_block == 0 else start_block
        query = hypersync.Query(
            from_block=height - block_range,
            to_block=height,
            transactions=[hypersync.TransactionSelection()],
            field_selection=hypersync.FieldSelection(
                transaction=[el.value for el in TransactionField],
                block=[el.value for el in BlockField],
            ),
            max_num_transactions=1_000  # for troubleshooting
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
                    TransactionField.MAX_FEE_PER_BLOB_GAS: DataType.FLOAT64,
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
        await self.client.collect_parquet('data', query, config)

    @timer
    async def get_new_l1_block_event(self, from_block: Optional[int] = None, to_block: Optional[int] = None, save_data: bool = False) -> Optional[pl.DataFrame]:
        """
        Query for new L1 block events and optionally save the data.

        Args:
            from_block (Optional[int]): The block number to start the query from.
            to_block (Optional[int]): The block number to end the query at.
            save_data (bool): Whether to save the data to a file.

        Returns:
            Optional[pl.DataFrame]: The collected data as a Polars DataFrame if save_data is False, otherwise None.
        """
        to_block = to_block or await self.get_height()
        from_block = from_block or 0

        query = self.create_query(
            from_block=from_block,
            to_block=to_block,
            logs=[LogSelection(address=[block_tracker_contract])]
        )
        config = hypersync.StreamConfig(
            hex_output=hypersync.HexOutput.PREFIXED,
            event_signature="NewL1Block(uint256 indexed blockNumber,address indexed winner,uint256 indexed window)"
        )
        return await self.collect_data(query, config, save_data)

    @timer
    async def get_window_deposits(self, address: Optional[str] = None, from_block: Optional[int] = None, to_block: Optional[int] = None, save_data: bool = False) -> Optional[pl.DataFrame]:
        """
        Query for window deposit events and optionally save the data.

        Args:
            address (Optional[str]): The address to filter by.
            from_block (Optional[int]): The block number to start the query from.
            to_block (Optional[int]): The block number to end the query at.
            save_data (bool): Whether to save the data to a file.

        Returns:
            Optional[pl.DataFrame]: The collected data as a Polars DataFrame if save_data is False, otherwise None.
        """
        to_block = to_block or await self.get_height()
        from_block = from_block or 0

        padded_address = address_to_topic(address.lower()) if address else None
        topics = [
            ["0x2ed10ffb7f7e5289e3bb91b8c3751388cb5d9b7f4533b9f0d59881a99822ddb3"]]
        if padded_address:
            topics.append([padded_address])

        query = self.create_query(
            from_block=from_block,
            to_block=to_block,
            logs=[LogSelection(
                address=[bidder_register_contract], topics=topics)]
        )
        config = hypersync.StreamConfig(
            hex_output=hypersync.HexOutput.PREFIXED,
            event_signature="BidderRegistered(address indexed bidder, uint256 depositedAmount, uint256 windowNumber)",
            column_mapping=ColumnMapping(
                decoded_log={'depositedAmount': DataType.INT64,
                             'windowNumber': DataType.INT64}
            )
        )
        return await self.collect_data(query, config, save_data)

    @timer
    async def get_window_withdraws(self, address: Optional[str] = None, from_block: Optional[int] = None, to_block: Optional[int] = None, save_data: bool = False) -> Optional[pl.DataFrame]:
        """
        Query for window withdrawal events and optionally save the data.

        Args:
            address (Optional[str]): The address to filter by.
            from_block (Optional[int]): The block number to start the query from.
            to_block (Optional[int]): The block number to end the query at.
            save_data (bool): Whether to save the data to a file.

        Returns:
            Optional[pl.DataFrame]: The collected data as a Polars DataFrame if save_data is False, otherwise None.
        """
        to_block = to_block or await self.get_height()
        from_block = from_block or 0

        padded_address = address_to_topic(address.lower()) if address else None
        topics = [
            ["0x2be239cccec761cb15b4070dda36677f39cb05afba45c7419fe7e27ed2c90b29"]]
        if padded_address:
            topics.append([padded_address])

        query = self.create_query(
            from_block=from_block,
            to_block=to_block,
            logs=[LogSelection(
                address=[bidder_register_contract], topics=topics)]
        )
        config = hypersync.StreamConfig(
            hex_output=hypersync.HexOutput.PREFIXED,
            event_signature="BidderWithdrawal(address indexed bidder, uint256 window, uint256 amount)",
            column_mapping=ColumnMapping(
                decoded_log={'amount': DataType.INT64,
                             'window': DataType.INT64}
            )
        )
        return await self.collect_data(query, config, save_data)
