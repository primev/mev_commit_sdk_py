# mev-commit-sdk-py

## Introduction
This is a python based SDK to grab data from mev-commit chain, specifically logs and events. You can use this endpoint to lookup data `mev-commit.rpc.hypersync.xyz`. There is currently support for both cryo and hypersync tools.

## Installation
There are two ways to install. The first way uses pypi `pip install mev-commit-sdk-py`. 

The second way is to clone the repository and install the dependencies. The second method is required to use cryo using Rye. Since cryo doesn't support milisecond timestamps, version 0.3.2 was forked and modified to support milisecond timestamps and a local wheel comes with the repository for installation. Note that you will have to uncomment the pyproject.toml cryo dependency to install the modified version.

This library uses `rye` as the project manager. First install [rye](https://rye.astral.sh/guide/installation/). Then clone the repository and run `rye sync` to install the dependencies and get setup. Current Python version is 3.10.12. Once installed, add a .env file with the variable `RPC='endpoint'` where endpoint is the url of the rpc server you want to connect to, this is for cryo. 

The hypersync endpoint is `https://mev-commit.hypersync.xyz` and currently is not passed in as an .env variable

## Usage
There are various things you can do with the SDK.

### Get Window Deposits
`python examples/cryo/get_windows.py` gets all old windows that still have funds in them. Then take the list of windows generated and use the [`withdraw funds` command](https://docs.primev.xyz/get-started/bidders/bidder-node-commands#withdraw-funds) to withdraw from all of the windows.