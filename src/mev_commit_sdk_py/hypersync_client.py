import hypersync
from dataclasses import dataclass, field
import polars as pl
from typing import List
from hypersync import BlockField, TransactionField, HypersyncClient, ColumnMapping, DataType, LogSelection, FieldSelection, LogField

# https://docs.primev.xyz/developers/testnet#contract-addresses
oracle_contract: str = "0x6856Eb630C79D491886E104D328834643B3F69E3".lower()  # oracle contrac
# block tracker contract
block_tracker_contract: str = "0x2eEbF31f5c932D51556E70235FB98bB2237d065c".lower()
bidder_register_contract: str = "0x7ffa86fF89489Bca72Fec2a978e33f9870B2Bd25".lower()


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

    async def get_new_l1_block_event(self, block_range: int, start_block: int = 0, save_data: bool = False) -> None:
        """

        Saves query results as parquet files in a data folder.
        """
        if start_block == 0:
            height = await self.client.get_height()

        query = hypersync.Query(
            from_block=height - (block_range),  # Calculate starting block.
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

        print("Running the query...")

        match save_data:
            case True:
                return await self.client.collect_parquet('data', query, config)
            case False:
                data = await self.client.collect_arrow(query, config)
                return pl.from_arrow(data.data.decoded_logs)

    async def get_window_deposits(self, block_range: int, start_block: int = 0, save_data: bool = False) -> None:
        """
        Saves query results as parquet files in a data folder.
        """
        if start_block == 0:
            height = await self.client.get_height()

        query = hypersync.Query(
            from_block=height - (block_range),  # Calculate starting block.
            logs=[LogSelection(
                address=[bidder_register_contract],
                topics=[
                    ["0x2ed10ffb7f7e5289e3bb91b8c3751388cb5d9b7f4533b9f0d59881a99822ddb3"]],
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

        print("Running the query...")

        match save_data:
            case True:
                return await self.client.collect_parquet('data', query, config)
            case False:
                data = await self.client.collect_arrow(query, config)
                return pl.from_arrow(data.data.decoded_logs)
