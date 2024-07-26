import asyncio
import polars as pl
import unittest
from mev_commit_sdk_py.hypersync_client import Hypersync

# expand polars df output
pl.Config.set_fmt_str_lengths(200)
pl.Config.set_fmt_float("full")


class TestHypersyncEvents(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.client = Hypersync(url='https://mev-commit.hypersync.xyz')

    def test_l1_blocks(self):
        l1_blocks = asyncio.run(self.client.get_new_l1_block_event_v1())
        self.assertIsInstance(l1_blocks, pl.DataFrame)
        self.assertGreater(
            l1_blocks.shape[0], 0, "L1 blocks should not be empty")

    def test_window_deposits(self):
        window_deposits = asyncio.run(self.client.get_window_deposits_v1())
        self.assertIsInstance(window_deposits, pl.DataFrame)
        self.assertGreater(
            window_deposits.shape[0], 0, "Window deposits should not be empty")

    def test_window_withdrawals(self):
        window_withdrawals = asyncio.run(self.client.get_window_withdraws_v1())
        self.assertIsInstance(window_withdrawals, pl.DataFrame)
        self.assertGreater(
            window_withdrawals.shape[0], 0, "Window withdraws should not be empty")

    def test_commit_stores(self):
        commit_stores = asyncio.run(self.client.get_commit_stores_v1())
        self.assertIsInstance(commit_stores, pl.DataFrame)
        self.assertGreater(
            commit_stores.shape[0], 0, "Commit stores should not be empty")

    def test_encrypted_stores(self):
        encrypted_stores = asyncio.run(
            self.client.get_encrypted_commit_stores_v1())
        self.assertIsInstance(encrypted_stores, pl.DataFrame)
        self.assertGreater(
            encrypted_stores.shape[0], 0, "Encrypted stores should not be empty")

    def test_commits_processed(self):
        commits_processed = asyncio.run(self.client.get_commits_processed_v1())
        self.assertIsInstance(commits_processed, pl.DataFrame)
        self.assertGreater(
            commits_processed.shape[0], 0, "Commits processed should not be empty")

    def test_funds_retrieved(self):
        funds_retrieved = asyncio.run(self.client.get_funds_retrieved_v1())
        self.assertIsInstance(funds_retrieved, pl.DataFrame)
        self.assertGreater(
            funds_retrieved.shape[0], 0, "Funds retrieved should not be empty")

    def test_funds_rewarded(self):
        funds_rewarded = asyncio.run(self.client.get_funds_rewarded_v1())
        self.assertIsInstance(funds_rewarded, pl.DataFrame)
        self.assertGreater(
            funds_rewarded.shape[0], 0, "Funds rewarded should not be empty")

    def test_funds_slashed(self):
        funds_slashed = asyncio.run(self.client.get_funds_slashed_v1())
        self.assertIsInstance(funds_slashed, pl.DataFrame)
        self.assertGreater(
            funds_slashed.shape[0], 0, "Funds slashed should not be empty")


if __name__ == '__main__':
    unittest.main()
