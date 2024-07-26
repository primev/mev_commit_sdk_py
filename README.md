# mev-commit-sdk-py

## Introduction
This is a python based SDK to grab data from mev-commit chain powered by [Envio's](https://envio.dev/) hypersync indexer.

## Installation
Install with `pip install mev-commit-sdk-py`

This library uses `rye` as the project manager. First install [rye](https://rye.astral.sh/guide/installation/). Then clone the repository and run `rye sync` to install the dependencies and get setup. Current Python version is 3.10.12. Once installed, add a .env file with the variable `RPC='endpoint'` where endpoint is the url of the rpc server you want to connect to, this is for cryo. 

The hypersync endpoint is `https://mev-commit.hypersync.xyz` and currently is not passed in as an .env variable

## Usage
There are various things you can do with the SDK.....to be written up in more detail. For now check the tests and examples for basic idea how to run the code