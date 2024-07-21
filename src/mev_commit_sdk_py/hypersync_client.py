import time
import hypersync
from dataclasses import dataclass, field
import polars as pl
from mev_commit_sdk_py.helpers import address_to_topic
from typing import Awaitable, Callable, List, Optional
from hypersync import BlockField, TransactionField, HypersyncClient, ColumnMapping, DataType, LogSelection, FieldSelection, LogField

# https://docs.primev.xyz/developers/testnet#contract-addresses
oracle_contract: str = "0x6856Eb630C79D491886E104D328834643B3F69E3".lower()  # oracle contrac
# block tracker contract
block_tracker_contract: str = "0x2eEbF31f5c932D51556E70235FB98bB2237d065c".lower()
bidder_register_contract: str = "0x7ffa86fF89489Bca72Fec2a978e33f9870B2Bd25".lower()


def timer(func: Callable[..., Awaitable[None]]) -> Callable[..., Awaitable[None]]:
    """
    This decorator prints the time taken for the asynchronous wrapped function to execute.
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
        self.client = HypersyncClient(
            hypersync.ClientConfig(
                url=self.url
            )
        )

    @timer
    async def get_blocks_txs(self, block_range: int, start_block: int = 0) -> None:
        """

        Saves query results as parquet files in a data folder.
        """
        if start_block == 0:
            height = await self.client.get_height()

        query = hypersync.Query(
            from_block=height - (block_range),  # Calculate starting block.
            transactions=[
                hypersync.TransactionSelection(
                )
            ],
            to_block=height,
            field_selection=hypersync.FieldSelection(
                # Select transaction fields to fetch.
                transaction=[el.value for el in TransactionField],
                # Select block fields to fetch.
                block=[el.value for el in BlockField],
            ),
        )

        # Setting this number lower reduces client sync console error messages.
        query.max_num_transactions = 1_000  # for troubleshooting

        # configuration settings to predetermine type output here
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

        return await self.client.collect_parquet('data', query, config)

    @timer
    async def get_new_l1_block_event(self, from_block: Optional[int] = None, to_block: Optional[int] = None, save_data: bool = False) -> Optional[pl.DataFrame]:
        """
        if from_block is None, defaults to the current block height and run a full historical query.
        """
        if to_block is None:  # stop at the en of the chain
            to_block = await self.client.get_height()

        if from_block is None:  # start from beginning of the chain
            from_block = 0

        query = hypersync.Query(
            from_block=from_block,
            to_block=to_block,
            logs=[LogSelection(
                address=[block_tracker_contract],
            )],
            field_selection=FieldSelection(
                log=[e.value for e in LogField],
                transaction=[e.value for e in TransactionField]
            )
        )

        config = hypersync.StreamConfig(
            hex_output=hypersync.HexOutput.PREFIXED,
            event_signature="NewL1Block(uint256 indexed blockNumber,address indexed winner,uint256 indexed window)"
        )

        match save_data:
            case True:
                result = await self.client.collect_parquet('data', query, config)
            case False:
                data = await self.client.collect_arrow(query, config)
                result = pl.from_arrow(data.data.decoded_logs)

        return result

    @timer
    async def get_window_deposits(self, address: Optional[str] = None, from_block: Optional[int] = None, to_block: Optional[int] = None, save_data: bool = False) -> None:
        """
        Saves query results as parquet files in a data folder.
        """
        if to_block is None:  # stop at the en of the chain
            to_block = await self.client.get_height()

        if from_block is None:  # start from beginning of the chain
            from_block = 0

        # make address lowercase and pad
        padded_address = address_to_topic(
            address.lower()) if address is not None else None

        topics = [
            ["0x2ed10ffb7f7e5289e3bb91b8c3751388cb5d9b7f4533b9f0d59881a99822ddb3"]
        ]
        if padded_address:
            topics.append([padded_address])

        query = hypersync.Query(
            from_block=from_block,
            to_block=to_block,
            logs=[LogSelection(
                address=[bidder_register_contract],
                topics=topics,
            )],
            field_selection=FieldSelection(
                log=[e.value for e in LogField],
                transaction=[e.value for e in TransactionField]
            )
        )

        config = hypersync.StreamConfig(
            hex_output=hypersync.HexOutput.PREFIXED,
            event_signature="BidderRegistered(address indexed bidder, uint256 depositedAmount, uint256 windowNumber)",
            column_mapping=ColumnMapping(
                decoded_log={'depositedAmount': DataType.INT64,
                             'windowNumber': DataType.INT64}
            )
        )

        match save_data:
            case True:
                result = await self.client.collect_parquet('data', query, config)
            case False:
                data = await self.client.collect_arrow(query, config)
                result = pl.from_arrow(data.data.decoded_logs)

        return result

    @timer
    async def get_window_withdraws(self, address: Optional[str] = None, from_block: Optional[int] = None, to_block: Optional[int] = None, save_data: bool = False) -> None:
        """
        Saves query results as parquet files in a data folder.
        """
        if to_block is None:  # stop at the en of the chain
            to_block = await self.client.get_height()

        if from_block is None:  # start from beginning of the chain
            from_block = 0

        # make address lowercase and pad
        padded_address = address_to_topic(
            address.lower()) if address is not None else None

        topics = [
            ["0x2be239cccec761cb15b4070dda36677f39cb05afba45c7419fe7e27ed2c90b29"]
        ]
        if padded_address:
            topics.append([padded_address])

        query = hypersync.Query(
            from_block=from_block,
            to_block=to_block,
            logs=[LogSelection(
                address=[bidder_register_contract],
                topics=topics,
            )],
            field_selection=FieldSelection(
                log=[e.value for e in LogField],
                transaction=[e.value for e in TransactionField]
            )
        )

        config = hypersync.StreamConfig(
            hex_output=hypersync.HexOutput.PREFIXED,
            event_signature="BidderWithdrawal(address indexed bidder, uint256 window, uint256 amount)",
            column_mapping=ColumnMapping(
                decoded_log={'amount': DataType.INT64,
                             'window': DataType.INT64}
            )
        )

        match save_data:
            case True:
                result = await self.client.collect_parquet('data', query, config)
            case False:
                data = await self.client.collect_arrow(query, config)
                result = pl.from_arrow(data.data.decoded_logs)

        return result
