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


# Call the getCurrentWindow function
# Mikhal note - currentWindow = blockNumber/blocksPerWindow
current_window = contract.functions.getCurrentWindow().call()
# Call the getBlocksPerWindow function
window_range = contract.functions.getBlocksPerWindow().call()

# use current window mod window range to get the progress. Add a +1 because I might be doing mod 10 wrong
window_progress = ((current_window %
                    window_range) / window_range)

current_block = current_window * window_range

# Get first and last block from window progress
first_block = current_block - (window_range * window_progress)
last_block = current_block + (window_range * (1 - window_progress))

print('Window Range (getBlocksPerWindow):', window_range)
print('Current L1 Block Number:', current_block)
print('Window Progress', (window_progress * 100), '%')

print('First Block:', first_block)
print('Last Block:', last_block)
