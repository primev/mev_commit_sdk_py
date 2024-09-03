import asyncio
import polars as pl
import unittest
from mev_commit_sdk_py.hypersync_client import Hypersync, EVENT_CONFIG

# expand polars df output
pl.Config.set_fmt_str_lengths(200)
pl.Config.set_fmt_float("full")


class TestHypersyncEvents(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.client = Hypersync(url='https://mev-commit.hypersync.xyz')

    def run_event_query_test(self, event_name: str):
        """Helper method to test a specific event name."""
        if event_name not in EVENT_CONFIG:
            self.fail(f"Event name {event_name} not found in EVENT_CONFIG")

        result = asyncio.run(
            self.client.execute_event_query(event_name, from_block=0, to_block=5_000_000)
        )
        self.assertIsInstance(result, pl.DataFrame)
        self.assertGreater(result.shape[0], 0, f"Results for {
                           event_name} should not be empty")

    def test_l1_blocks(self):
        event_name = "NewL1Block"
        self.run_event_query_test(event_name)

    def test_window_deposits(self):
        event_name = "BidderRegistered"
        self.run_event_query_test(event_name)

    def test_window_withdrawals(self):
        event_name = "BidderWithdrawal"
        self.run_event_query_test(event_name)

    def test_commit_stores(self):
        event_name = "OpenedCommitmentStored"
        self.run_event_query_test(event_name)

    def test_encrypted_stores(self):
        event_name = "UnopenedCommitmentStored"
        self.run_event_query_test(event_name)

    def test_commits_processed(self):
        event_name = "CommitmentProcessed"
        self.run_event_query_test(event_name)

    def test_funds_retrieved(self):
        event_name = "FundsRetrieved"
        self.run_event_query_test(event_name)

    def test_funds_rewarded(self):
        event_name = "FundsRewarded"
        self.run_event_query_test(event_name)

    def test_funds_slashed(self):
        event_name = "FundsSlashed"
        self.run_event_query_test(event_name)

    def test_funds_deposited(self):
        event_name = "FundsDeposited"
        self.run_event_query_test(event_name)

    def test_withdraw(self):
        event_name = "Withdraw"
        self.run_event_query_test(event_name)

    def test_provider_registered(self):
        event_name = "ProviderRegistered"
        self.run_event_query_test(event_name)


if __name__ == '__main__':
    unittest.main()
