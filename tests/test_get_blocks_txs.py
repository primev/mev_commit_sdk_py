import unittest
import asyncio
import polars as pl
from mev_commit_sdk_py.hypersync_client import Hypersync

# Configure polars for expanded output
pl.Config.set_fmt_str_lengths(200)
pl.Config.set_fmt_float("full")


class TestHypersyncClient(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        mev_commit_url = 'https://mev-commit.hypersync.xyz'
        cls.client = Hypersync(mev_commit_url)

    def test_get_blocks_txs_historical(self):
        """Test arbitrary block range query."""
        async def run_test():
            txs, blocks = await self.client.get_blocks_txs(from_block=0, to_block=100000)
            self.assertIsInstance(txs, pl.DataFrame)
            self.assertIsInstance(blocks, pl.DataFrame)
            # It's okay if txs.shape[0] is 0
            self.assertGreaterEqual(txs.shape[0], 0)
            # Ensure blocks have data
            self.assertGreater(blocks.shape[0], 0)
            print(txs.shape)
            print(blocks.shape)

        asyncio.run(run_test())

    def test_get_blocks_txs_recent_range(self):
        """Test the most recent block range query."""
        async def run_test():
            txs, blocks = await self.client.get_blocks_txs(block_range=100000)
            self.assertIsInstance(txs, pl.DataFrame)
            self.assertIsInstance(blocks, pl.DataFrame)
            # It's okay if txs.shape[0] is 0
            self.assertGreaterEqual(txs.shape[0], 0)
            # Ensure blocks have data
            self.assertGreater(blocks.shape[0], 0)
            print(txs.shape)
            print(blocks.shape)

        asyncio.run(run_test())


if __name__ == '__main__':
    unittest.main()
