from web3 import Web3
import json

# Replace with your Holesky network RPC endpoint
holesky_rpc_url = 'https://ethereum-holesky-rpc.publicnode.com'

# Initialize a Web3 instance
web3 = Web3(Web3.HTTPProvider(holesky_rpc_url))

# Check if connected
if web3.is_connected():
    print("Connected to the Holesky network")
else:
    print("Failed to connect to the Holesky network")

block = web3.eth.get_block('latest')
print(block)
