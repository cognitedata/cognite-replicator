# Cognite Python Replicator
[![build](https://webhooks.dev.cognite.ai/build/buildStatus/icon?job=github-builds/cognite-replictor/master)](https://jenkins.cognite.ai/job/github-builds/job/cognite-replicator/job/master/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)

Copyright 2019 Cognite AS

Cognite Replicator is a Python package for replicating data across Cognite Data Fusion (CDF) projects. This package is
built on top of the Cognite Python SDK.

## Prerequisites
In order to start using the Replicator, you need:
* Python3 (>= 3.6)
* Cognite Python SDK
* Two API keys, one for your source tenant and one for your destination tenant. Never include the API key directly in the code or upload the key to github. Instead, set the API key as an environment variable.

This is how you set the API key as an environment variable on Mac OS and Linux:
```bash
$ export COGNITE_SOURCE_API_KEY=<your source API key>
$ export COGNITE_DESTINATION_API_KEY=<your destination API key>
```

## Installation
The replicator is currently distribuated as Python wheels, but it can also be executed as a standalone script.

On this GitHub-page under **release** can you find the `.whl` file. By clicking on the file, you will automatically download the file. Then go into Databricks and into your cluster. Click on **Libraries** and **Install New**.  Choose your library type to be a **Python Whl**. By clicking on the area **Drop WHL here** you can navigate to where you have your `.whl`-file (most likely in your dowloads folder). Choose the `.whl` file, let the new library install and you are ready to replicate!

## Usage

### Setup
```python
import os

from cognite.client import CogniteClient
from cognite.replicator import assets, events, time_series

SRC_API_KEY = os.environ.get("COGNITE_SOURCE_API_KEY")
DST_API_KEY = os.environ.get("COGNITE_DESTINATION_API_KEY")
PROJECT_SRC = "Name of source tenant"
PROJECT_DST = "Name of destination tenant"
CLIENT_NAME = "cognite-replicator"
BATCH_SIZE = 10000 # this is the max size of a batch to be posted
NUM_THREADS= 10 # this is the max number of threads to be used

CLIENT_SRC = CogniteClient(api_key=SRC_API_KEY, project=PROJECT_SRC, client_name=CLIENT_NAME)
CLIENT_DST = CogniteClient(api_key=DST_API_KEY, project=PROJECT_DST, client_name=CLIENT_NAME, timeout=90)
```

### Replicate Assets
```python

assets.replicate(CLIENT_SRC, CLIENT_DST)
```

### Replicate Events
```python

events.replicate(CLIENT_SRC, CLIENT_DST, BATCH_SIZE, NUM_THREADS)
```

### Replicate Time Series
```python

time_series.replicate(CLIENT_SRC, PROJECT_DST, BATCH_SIZE, NUM_THREADS)
```

## Development
We are using [Poetry](https://poetry.eustace.io/) to manage dependencies and building the library.

Install poetry:
```bash
curl -sSL https://raw.githubusercontent.com/sdispater/poetry/master/get-poetry.py | python
```

To install dependencies:
```bash
poetry install
```

To build the wheel:
```bash
poetry build
```

### Testing
We are using pytest framework. To run the test suite (after poetry and dependencies are installed):

```python
poetry run pytest --cov cognite
```
