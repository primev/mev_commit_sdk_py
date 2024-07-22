import requests
import polars as pl
from web3 import Web3

# Constants
NEW_CONTRACT_ADDRESS = "0xCAC68D97a56b19204Dd3dbDC103CB24D47A825A3"
NODE_URL = "https://chainrpc.testnet.mev-commit.xyz"
EVENT_TOPIC_COMMITMENT_STORED = "0xe44dd4d002deb2c79cf08ce285a9d80c69753f31ca65c8e49f0a60d27ed9fea3"

# Connect to the node
node = Web3(Web3.HTTPProvider(NODE_URL))
print('connected?', node.is_connected())

# Load the contract ABI from the URL
abi_url = 'https://raw.githubusercontent.com/primev/mev-commit/v0.4.3/contracts-abi/abi/PreConfCommitmentStore.abi'
response = requests.get(abi_url)

if response.status_code == 200:
    contract_abi = response.json()
else:
    print("Failed to fetch ABI from URL")
    exit()

# Create the contract instance
contract = node.eth.contract(address=NEW_CONTRACT_ADDRESS, abi=contract_abi)

# Define the event filter parameters
event_filter_params = {
    'fromBlock': 'earliest',
    'toBlock': 'latest',
    'address': NEW_CONTRACT_ADDRESS,
    'topics': [EVENT_TOPIC_COMMITMENT_STORED]
}

# Get logs for the specified event
logs = node.eth.get_logs(event_filter_params)

# List to hold all decoded events
decoded_events = []

# Decode the logs and store in the list
for log in logs:
    decoded_event = contract.events.CommitmentStored().process_log(log)
    event_data = {
        "Commitment Index": decoded_event['args']['commitmentIndex'],
        "Bidder": decoded_event['args']['bidder'],
        "Commiter": decoded_event['args']['commiter'],
        "Bid": decoded_event['args']['bid'],
        "Block Number": decoded_event['args']['blockNumber'],
        "Bid Hash": decoded_event['args']['bidHash'],
        "Decay Start TimeStamp": decoded_event['args']['decayStartTimeStamp'],
        "Decay End TimeStamp": decoded_event['args']['decayEndTimeStamp'],
        "Txn Hash": log['transactionHash'],
        "Commitment Hash": decoded_event['args']['commitmentHash'],
        "Bid Signature": decoded_event['args']['bidSignature'],
        "Commitment Signature": decoded_event['args']['commitmentSignature'],
        "Dispatch Timestamp": decoded_event['args']['dispatchTimestamp'],
        "Shared Secret Key": decoded_event['args']['sharedSecretKey']
    }
    decoded_events.append(event_data)

# Convert the decoded events to a Polars DataFrame
df = pl.DataFrame(decoded_events)

print(df.head(5))
# Save the DataFrame as a Parquet file
df.write_parquet('decoded_events.parquet')

print(f"Decoded events saved to decoded_events.parquet")
