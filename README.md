<a href="https://cognite.com/">
    <img src="https://github.com/cognitedata/cognite-python-docs/blob/master/img/cognite_logo.png" alt="Cognite logo" title="Cognite" align="right" height="80" />
</a>

# Cognite Python Replicator
[![build](https://webhooks.dev.cognite.ai/build/buildStatus/icon?job=github-builds/cognite-replicator/master)](https://jenkins.cognite.ai/job/github-builds/job/cognite-replicator/job/master/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)

Cognite Replicator is a Python package for replicating data across Cognite Data Fusion (CDF) projects. This package is
built on top of the Cognite Python SDK.

Copyright 2019 Cognite AS

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

### Setup as Python library
```python
import os

from cognite.client import CogniteClient
from cognite.replicator import assets, events, time_series, datapoints

SRC_API_KEY = os.environ.get("COGNITE_SOURCE_API_KEY")
DST_API_KEY = os.environ.get("COGNITE_DESTINATION_API_KEY")
PROJECT_SRC = "Name of source tenant"
PROJECT_DST = "Name of destination tenant"
CLIENT_NAME = "cognite-replicator"
BATCH_SIZE = 10000 # this is the max size of a batch to be posted
NUM_THREADS= 10 # this is the max number of threads to be used

CLIENT_SRC = CogniteClient(api_key=SRC_API_KEY, project=PROJECT_SRC, client_name=CLIENT_NAME)
CLIENT_DST = CogniteClient(api_key=DST_API_KEY, project=PROJECT_DST, client_name=CLIENT_NAME, timeout=90)

assets.replicate(CLIENT_SRC, CLIENT_DST)
events.replicate(CLIENT_SRC, CLIENT_DST, BATCH_SIZE, NUM_THREADS)
time_series.replicate(CLIENT_SRC, CLIENT_DST, BATCH_SIZE, NUM_THREADS)
datapoints.replicate(CLIENT_SRC, CLIENT_DST)
```

### Run it from databricks notebook
```python
import os
import logging

from cognite.client import CogniteClient
from cognite.replicator import assets, configure_databricks_logger

SRC_API_KEY = dbutils.secrets.get("cdf-api-keys", "source-tenant")
DST_API_KEY = dbutils.secrets.get("cdf-api-keys", "destination-tenant")

CLIENT_SRC = CogniteClient(api_key=SRC_API_KEY, client_name="cognite-replicator")
CLIENT_DST = CogniteClient(api_key=DST_API_KEY, client_name="cognite-replicator")

logger = logging.getLogger(__name__)

configure_databricks_logger(log_level=logging.INFO, logger=logger)
assets.replicate(CLIENT_SRC, CLIENT_DST)
```

### Run it from command line
```bash
poetry run replicator -h
```

## Changelog
Wondering about upcoming or previous changes to the SDK? Take a look at the [CHANGELOG](https://github.com/cognitedata/cognite-replicator/blob/master/CHANGELOG.md).

## Contributing
Want to contribute? Check out [CONTRIBUTING](https://github.com/cognitedata/cognite-replicator/blob/master/CONTRIBUTING.md).
