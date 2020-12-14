<a href="https://cognite.com/">
    <img src="https://raw.githubusercontent.com/cognitedata/cognite-python-docs/master/img/cognite_logo.png" alt="Cognite logo" title="Cognite" align="right" height="80" />
</a>

# Cognite Python Replicator
[![build](https://webhooks.dev.cognite.ai/build/buildStatus/icon?job=github-builds/cognite-replicator/master)](https://jenkins.cognite.ai/job/github-builds/job/cognite-replicator/job/master/)
[![codecov](https://codecov.io/gh/cognitedata/cognite-replicator/branch/master/graph/badge.svg)](https://codecov.io/gh/cognitedata/cognite-replicator)
[![Documentation Status](https://readthedocs.com/projects/cognite-cognite-replicator/badge/?version=latest)](https://cognite-cognite-replicator.readthedocs-hosted.com/en/latest/)
[![PyPI version](https://badge.fury.io/py/cognite-replicator.svg)](https://pypi.org/project/cognite-replicator/)
[![tox](https://img.shields.io/badge/tox-3.6%2B-blue.svg)](https://www.python.org/downloads/release/python-366/)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/cognite-replicator)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)

Cognite Replicator is a Python package for replicating data across Cognite Data Fusion (CDF) projects. This package is
built on top of the Cognite Python SDK.

Copyright 2019 Cognite AS

## Prerequisites
In order to start using the Replicator, you need:
* Python3 (>= 3.6)
* Two API keys, one for your source tenant and one for your destination tenant. Never include the API key directly in the code or upload the key to github. Instead, set the API key as an environment variable.

This is how you set the API key as an environment variable on Mac OS and Linux:
```bash
$ export COGNITE_SOURCE_API_KEY=<your source API key>
$ export COGNITE_DESTINATION_API_KEY=<your destination API key>
```

## Installation
The replicator is available on [PyPI](https://pypi.org/project/cognite-replicator/), and can also be executed as a standalone script.

To run it from command line, run:
```bash
pip install cognite-replicator
python -m cognite.replicator config/filepath.yml
```
If no file is specified then replicator will use config/default.yml.

Alternatively, build and run it as a docker container. The image is avaible on [docker hub](https://hub.docker.com/r/cognite/cognite-replicator):
```bash
docker build -t cognite-replicator .
docker run -it cognite-replicator
```

For *Databricks* you can install it on a cluster. First, click on **Libraries** and **Install New**.  Choose your library type to be **PyPI**, and enter **cognite-replicator** as Package. Let the new library install and you are ready to replicate!


## Usage

### Setup as Python library
```python
import os

from cognite.client import CogniteClient

SRC_API_KEY = os.environ.get("COGNITE_SOURCE_API_KEY")
DST_API_KEY = os.environ.get("COGNITE_DESTINATION_API_KEY")
PROJECT_SRC = "Name of source tenant"
PROJECT_DST = "Name of destination tenant"
CLIENT_NAME = "cognite-replicator"
BATCH_SIZE = 10000 # this is the max size of a batch to be posted
NUM_THREADS= 10 # this is the max number of threads to be used
SRC_BASE_URL = "https://api.cognitedata.com"
DST_BASE_URL = "https://api.cognitedata.com"
TIMEOUT = 90

if __name__ == '__main__': # this is necessary because threading
    from cognite.replicator import assets, events, files, time_series, datapoints

    CLIENT_SRC = CogniteClient(api_key=SRC_API_KEY, project=PROJECT_SRC, base_url=SRC_BASE_URL, client_name=CLIENT_NAME)
    CLIENT_DST = CogniteClient(api_key=DST_API_KEY, project=PROJECT_DST, base_url=DST_BASE_URL, client_name=CLIENT_NAME, timeout=TIMEOUT)

    assets.replicate(CLIENT_SRC, CLIENT_DST)
    events.replicate(CLIENT_SRC, CLIENT_DST, BATCH_SIZE, NUM_THREADS)
    files.replicate(CLIENT_SRC, CLIENT_DST, BATCH_SIZE, NUM_THREADS)
    time_series.replicate(CLIENT_SRC, CLIENT_DST, BATCH_SIZE, NUM_THREADS)
    datapoints.replicate(CLIENT_SRC, CLIENT_DST)
```

### Run it from databricks notebook
```python
import logging

from cognite.client import CogniteClient
from cognite.replicator import assets, configure_databricks_logger

SRC_API_KEY = dbutils.secrets.get("cdf-api-keys", "source-tenant")
DST_API_KEY = dbutils.secrets.get("cdf-api-keys", "destination-tenant")

CLIENT_SRC = CogniteClient(api_key=SRC_API_KEY, client_name="cognite-replicator")
CLIENT_DST = CogniteClient(api_key=DST_API_KEY, client_name="cognite-replicator")

configure_databricks_logger(log_level=logging.INFO)
assets.replicate(CLIENT_SRC, CLIENT_DST)
```

## Changelog
Wondering about upcoming or previous changes? Take a look at the [CHANGELOG](https://github.com/cognitedata/cognite-replicator/blob/master/CHANGELOG.md).

## Contributing
Want to contribute? Check out [CONTRIBUTING](https://github.com/cognitedata/cognite-replicator/blob/master/CONTRIBUTING.md).
