import asyncio
import hypersync
import polars as pl
from dataclasses import dataclass, field
from typing import List
from hypersync import BlockField, TransactionField, HypersyncClient, ColumnMapping, DataType


@dataclass
class Hypersync:
    client: HypersyncClient = field(
        default_factory=lambda: HypersyncClient(
            hypersync.ClientConfig(
                url='https://mev-commit.hypersync.xyz'
            )
        )
    )
    transactions: List[hypersync.TransactionField] = field(
        default_factory=list)
    blocks: List[hypersync.BlockField] = field(default_factory=list)

    async def fetch_data(self, block_range: int, start_block: int = 0, ) -> None:
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

    # def query_txs(self, address: Union[str, Dict[list, list]], period: int) -> pl.DataFrame:
    #     """ Query transactions for a given address and period.

    #      Parameters:
    #      - address (str): The blockchain address to query transactions for.
    #      - period (int): The time period over which transactions should be queried.

    #      Returns:
    #      - A DataFrame containing transaction details for the specified address and period.
    #     """
    #     asyncio.run(self.fetch_data(address=address, period=period))

    #     # Merge separate datasets into a single dataset
    #     txs_df = pl.scan_parquet('data/transactions.parquet')
    #     blocks_df = pl.scan_parquet(
    #         'data/blocks.parquet').rename({'number': 'block_number'})

    #     txs_blocks_joined = txs_df.join(
    #         blocks_df,
    #         on='block_number',
    #         how='left',
    #         coalesce=True,
    #         suffix='_block'
    #     ).unique()

    #     final_df = txs_blocks_joined.select(
    #         'block_number',
    #         'extra_data',
    #         'base_fee_per_gas',
    #         'timestamp',
    #         'hash',
    #         'from',
    #         'to',
    #         'gas',
    #         'transaction_index',
    #         'gas_price',
    #         'effective_gas_price',
    #         'gas_used',
    #         'cumulative_gas_used',
    #         'max_fee_per_gas',
    #         'max_priority_fee_per_gas',
    #     ).collect()

    #     return pl.DataFrame(final_df)
