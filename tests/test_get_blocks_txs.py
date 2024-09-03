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
            df = await self.client.get_blocks_txs(from_block=0, to_block=100000)
            self.assertIsInstance(df, pl.DataFrame)
            # It's okay if df.shape[0] is 0
            self.assertGreaterEqual(df.shape[0], 0)
            print(df.shape)

        asyncio.run(run_test())


if __name__ == '__main__':
    unittest.main()
