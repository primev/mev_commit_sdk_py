import json
import os
from web3 import Web3

# Constants
BIDDER_REGISTRY = "0x7ffa86fF89489Bca72Fec2a978e33f9870B2Bd25"
NODE_URL = "https://chainrpc.testnet.mev-commit.xyz"
# Replace with actual topic if different
EVENT_TOPIC_FUNDS_RETRIEVED = "0x4ee0e06b2d2e4d1f06e75df9f2bad2c919d860fbf843f3b1f12de3264471a102"

# Connect to the node
node = Web3(Web3.HTTPProvider(NODE_URL))
print('connected?', node.is_connected())

# Read ABI
abi_path = os.path.abspath("abi/BidderRegistry.abi")
with open(abi_path, "r") as file:
    abi_data = json.load(file)

# Create the contract instance
contract = node.eth.contract(address=BIDDER_REGISTRY, abi=abi_data)

# Define the event filter parameters
event_filter_params = {
    'fromBlock': 'earliest',
    'toBlock': 'latest',
    'address': BIDDER_REGISTRY,
    'topics': [EVENT_TOPIC_FUNDS_RETRIEVED]
}

# Get logs for the specified event
logs = node.eth.get_logs(event_filter_params)

# Decode the logs
for log in logs:
    decoded_event = contract.events.FundsRetrieved().process_log(log)
    print(f"Commitment Digest: {decoded_event['args']['commitmentDigest']}, "
          f"Bidder: {decoded_event['args']['bidder']}, "
          f"Window: {decoded_event['args']['window']}, "
          f"Amount: {decoded_event['args']['amount']}")
