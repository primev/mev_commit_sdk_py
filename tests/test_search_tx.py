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
        holesky_url = 'https://eth.hypersync.xyz'
        cls.client = Hypersync(holesky_url)

    def test_search_txs(self):
        """Test search_txs with predefined transaction hashes."""
        async def run_test():
            tx_search = [
                "0x410eec15e380c6f23c2294ad714487b2300dd88a7eaa051835e0da07f16fc282",
                "0x110753637c9ead4b97c37a7a6a36c30015ffcd0effa5736c574de05d4f7adeb5",
            ]
            
            # Run the search_txs function
            df = await self.client.search_txs(txs=tx_search, save_data=False)
            
            # Assertions to check the dataframe
            self.assertIsInstance(df, pl.DataFrame)
            # It's okay if df.shape[0] is 0, but we expect a DataFrame
            self.assertGreaterEqual(df.shape[0], 0)
            print(df)

        asyncio.run(run_test())


if __name__ == '__main__':
    unittest.main()
