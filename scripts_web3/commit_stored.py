import json
import os
from web3 import Web3

# Constants
NEW_CONTRACT_ADDRESS = "0xCAC68D97a56b19204Dd3dbDC103CB24D47A825A3"
NODE_URL = "https://chainrpc.testnet.mev-commit.xyz"
EVENT_TOPIC_COMMITMENT_STORED = "0xa4aab50afc443b845214b8f4e2e7c32ea42be39a84e532be779802c54ff8ffda"

# Connect to the node
node = Web3(Web3.HTTPProvider(NODE_URL))
print('connected?', node.is_connected())

# Read ABI
abi_path = os.path.abspath("abi/PreConfCommitmentStore.abi")
with open(abi_path, "r") as file:
    abi_data = json.load(file)

# Create the contract instance
contract = node.eth.contract(address=NEW_CONTRACT_ADDRESS, abi=abi_data)

# Define the event filter parameters
event_filter_params = {
    'fromBlock': 'earliest',
    'toBlock': 'latest',
    'address': NEW_CONTRACT_ADDRESS,
    'topics': [EVENT_TOPIC_COMMITMENT_STORED]
}

# Get logs for the specified event
logs = node.eth.get_logs(event_filter_params)

# Decode the logs
for log in logs:
    decoded_event = contract.events.CommitmentStored().process_log(log)
    print(f"Commitment Index: {decoded_event['args']['commitmentIndex']}, "
          f"Bidder: {decoded_event['args']['bidder']}, "
          f"Commiter: {decoded_event['args']['commiter']}, "
          f"Bid: {decoded_event['args']['bid']}, "
          f"Block Number: {decoded_event['args']['blockNumber']}, "
          f"Bid Hash: {decoded_event['args']['bidHash']}, "
          f"Decay Start TimeStamp: {
              decoded_event['args']['decayStartTimeStamp']}, "
          f"Decay End TimeStamp: {
              decoded_event['args']['decayEndTimeStamp']}, "
          f"Txn Hash: {decoded_event['args']['txnHash']}, "
          f"Commitment Hash: {decoded_event['args']['commitmentHash']}, "
          f"Bid Signature: {decoded_event['args']['bidSignature']}, "
          f"Commitment Signature: {
              decoded_event['args']['commitmentSignature']}, "
          f"Dispatch Timestamp: {decoded_event['args']['dispatchTimestamp']}, "
          f"Shared Secret Key: {decoded_event['args']['sharedSecretKey']}")
