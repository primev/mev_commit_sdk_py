import cryo
import os
import polars as pl

from dataclasses import dataclass
from dotenv import load_dotenv


@dataclass
class CryoClient:
    """
    Manages connecting to the mev-commit chain using cryo.
    """

    load_dotenv()
    rpc = os.getenv("RPC")

    def get_preconf_commit(self, block_number: str = ':latest') -> str:
        """
        Get the precommitted commit for a given block number.

        PreConfCommitmentStore Contract - 0xCAC68D97a56b19204Dd3dbDC103CB24D47A825A3

        event signature:
        `CommitmentStored(
        bytes32 indexed commitmentIndex, 
        address bidder, 
        address commiter, 
        uint64 bid, 
        uint64 blockNumber, 
        bytes32 bidHash, 
        uint64 decayStartTimeStamp, 
        uint64 decayEndTimeStamp, 
        string txnHash, 
        bytes32 commitmentHash, 
        bytes bidSignature, 
        bytes commitmentSignature, 
        uint64 dispatchTimestamp, 
        bytes sharedSecretKey
        )`
        """
        data_df: pl.DataFrame = cryo.collect(
            'logs',
            rpc=self.rpc,
            hex=True,
            blocks=[block_number],
            # inner_req_size is number of logs per request. Sparse logs can have a higehr value
            inner_request_size=10000,
            contract=['0xCAC68D97a56b19204Dd3dbDC103CB24D47A825A3'],
            event_signature='CommitmentStored(bytes32 indexed commitmentIndex, address bidder, address commiter, uint64 bid, uint64 blockNumber, bytes32 bidHash, uint64 decayStartTimeStamp, uint64 decayEndTimeStamp, string txnHash, bytes32 commitmentHash, bytes bidSignature, bytes commitmentSignature, uint64 dispatchTimestamp, bytes sharedSecretKey)',
            topic0=[
                '0xa4aab50afc443b845214b8f4e2e7c32ea42be39a84e532be779802c54ff8ffda']
        )

        # rename event columns
        # Create a dictionary mapping old column names to new column names
        rename_dict = {col: col.replace('event__', '')
                       for col in data_df.columns if col.startswith('event__')}

        return data_df.rename(rename_dict)

    def get_l1_window_events(self, block_number: str = ':latest') -> str:
        """
        Get the L1 block emitted events:

        event signature:
        `NewL1Block(
        uint256 indexed blockNumber,
        address indexed winner,
        uint256 indexed window
        )`
        """

        data_df: pl.DataFrame = cryo.collect(
            'logs',
            rpc=self.rpc,
            hex=True,
            blocks=[block_number],
            inner_request_size=10000,
            contract=['0x2eEbF31f5c932D51556E70235FB98bB2237d065c'],
            event_signature='NewL1Block(uint256 indexed blockNumber,address indexed winner,uint256 indexed window)',
            topic0=[
                '0x8323d3e5d25db513e1a772870aaa45e9b069a13d49879d72e70638b5c1c18cb7']
        )

        # rename event columns
        # Create a dictionary mapping old column names to new column names
        rename_dict = {col: col.replace('event__', '')
                       for col in data_df.columns if col.startswith('event__')}

        return data_df.rename(rename_dict).drop('blockNumber_binary', 'blockNumber_string', 'window_binary', 'window_string')

    def get_window_deposits(self, block_number: str = ':latest') -> str:
        """
        Get historical bidder deposits into windows.

        event signature:
        `BidderRegistered(
        address indexed bidder, 
        uint256 depositedAmount, 
        uint256 windowNumber
        )`
        """
        data_df: pl.DataFrame = cryo.collect(
            'logs',
            rpc=self.rpc,
            hex=True,
            blocks=[block_number],
            # inner_request_size is the number of logs per request. Sparse logs can have a higher value
            inner_request_size=10000,
            # Replace with the correct contract address
            contract=['0x7ffa86fF89489Bca72Fec2a978e33f9870B2Bd25'],
            event_signature='BidderRegistered(address indexed bidder, uint256 depositedAmount, uint256 windowNumber)',
        )

        # rename event columns
        # Create a dictionary mapping old column names to new column names
        rename_dict = {col: col.replace('event__', '')
                       for col in data_df.columns if col.startswith('event__')}

        return data_df.rename(rename_dict)

    def get_window_withdraws(self, block_number: str = ':latest') -> str:
        """
        Get historical bidder withdraws from the windows.

        event signature:
        `BidderWithdrawal(
        address indexed bidder, 
        uint256 window, 
        uint256 amount
        )`
        """
        data_df: pl.DataFrame = cryo.collect(
            'logs',
            rpc=self.rpc,
            hex=True,
            blocks=[block_number],
            # inner_request_size is the number of logs per request. Sparse logs can have a higher value
            inner_request_size=10000,
            # Replace with the correct contract address
            contract=['0x7ffa86fF89489Bca72Fec2a978e33f9870B2Bd25'],
            event_signature='BidderWithdrawal(address indexed bidder, uint256 window, uint256 amount)',
        )

        # rename event columns
        # Create a dictionary mapping old column names to new column names
        rename_dict = {col: col.replace('event__', '')
                       for col in data_df.columns if col.startswith('event__')}

        return data_df.rename(rename_dict)
