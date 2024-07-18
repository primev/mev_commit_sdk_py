import json
import os
from web3 import Web3

# connect node
node = Web3(Web3.HTTPProvider("https://chainrpc.testnet.mev-commit.xyz"))
print('connected?', node.is_connected())

# Read ABI
with open(os.path.abspath("abi/BlockTracker.abi"), "r") as file:
    abi_data = json.load(file)

# Create the contract instance
contract = node.eth.contract(
    address="0x2eEbF31f5c932D51556E70235FB98bB2237d065c", abi=abi_data)

# Call the getCurrentWindow function = ((blockNum-1)/ blocksPerWindow) + 1
current_window = contract.functions.getCurrentWindow().call()

# Call the getBlocksPerWindow function
blocks_per_window = contract.functions.getBlocksPerWindow().call()

# Calculate the current L1 block number
current_l1_block_number = blocks_per_window * (current_window - 1) + 1

# Calculate the first and last block in the current window
# first_block = (current_window - 1) * blocks_per_window + 1
# last_block = current_window * blocks_per_window

print('Blocks Per Window:', blocks_per_window)
print('Current L1 Block Number:', current_l1_block_number)
print('Current Window:', current_window)