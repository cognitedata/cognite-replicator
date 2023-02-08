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

Copyright 2023 Cognite AS

## Prerequisites
In order to start using the Replicator, you need:
* Python3 (>= 3.6)
* Credentials for both the source and destination projects: 
** CLIENT_ID ("Client ID from Azure")
** CLIENT_SECRET ("Client secret", if necessary)
** CLUSTER ("Name of CDF cluster")
** TENANT_ID ("Tenant ID from Azure"
** PROJECT ("Name of source project")

This is how you set the client secret as an environment variable on Mac OS and Linux:
```bash
$ export SOURCE_CLIENT_SECRET=<your source client secret>
$ export DEST_CLIENT_SECRET=<your destination client secret>
```

## Installation
The replicator is available on [PyPI](https://pypi.org/project/cognite-replicator/), and can also be executed .

To run it from command line, run:
```bash
pip install cognite-replicator
```

Alternatively, build and run it as a docker container. The image is avaible on [docker hub](https://hub.docker.com/r/cognite/cognite-replicator):
```bash
docker build -t cognite-replicator .
```

## Usage

### Run with a configuration file as a standalone script

Create a configuration file based on the config/default.yml and update the values corresponding to your environment
If no file is specified then replicator will use config/default.yml.

via Python 

```bash
python -m cognite.replicator config/filepath.yml
```

or alternatively via docker

```bash
docker run -it cognite-replicator -e SOURCE_CLIENT_SECRET -e DEST_CLIENT_SECRET -v config/filepath.yml:/config.yml cognite-replicator /config.yml
```

### Setup as Python library
 ```python
import os

from cognite.client import CogniteClient

# Source
SOURCE_CLIENT_ID = "Client ID from Azure"
SOURCE_CLIENT_SECRET = os.environ.get("SOURCE_CLIENT_SECRET")
SOURCE_CLUSTER = "Name of CDF cluster for the source"
SOURCE_SCOPES = [f"https://{SOURCE_CLUSTER}.cognitedata.com/.default"]
SOURCE_TENANT_ID = "Tenant ID from Azure"
SOURCE_TOKEN_URL = f"https://login.microsoftonline.com/{SOURCE_TENANT_ID}/oauth2/v2.0/token"
SOURCE_PROJECT = "Name of source project"
SOURCE_CLIENT_NAME = "Replicator Source project"

# Destination
DEST_CLIENT_ID = "Client ID from Azure"
DEST_CLIENT_SECRET = os.environ.get("DEST_CLIENT_SECRET")
DEST_CLUSTER = "Name of CDF cluster for the destination"
DEST_SCOPES = [f"https://{DEST_CLUSTER}.cognitedata.com/.default"]
DEST_TENANT_ID = "Tenant ID from Azure"
DEST_TOKEN_URL = f"https://login.microsoftonline.com/{DEST_TENANT_ID}/oauth2/v2.0/token"
DEST_PROJECT = "Name of destination project"
DEST_CLIENT_NAME = "Replicator Destination project"

# Config
BATCH_SIZE = 10000 # this is the max size of a batch to be posted
NUM_THREADS= 10 # this is the max number of threads to be used
TIMEOUT = 90

if __name__ == '__main__': # this is necessary because threading
    from cognite.replicator import assets, events, files, time_series, datapoints, sequences, sequence_rows

    SOURCE_CLIENT = CogniteClient(
        token_url=SOURCE_TOKEN_URL,
        token_client_id=SOURCE_CLIENT_ID,
        token_client_secret=SOURCE_CLIENT_SECRET,
        token_scopes=SOURCE_SCOPES,
        project=SOURCE_PROJECT,
        base_url=f"https://{SOURCE_CLUSTER}.cognitedata.com",
        client_name="cognite-replicator-source",
        debug=False,
    )
    DEST_CLIENT = CogniteClient(
        token_url=DEST_TOKEN_URL,
        token_client_id=DEST_CLIENT_ID,
        token_client_secret=DEST_CLIENT_SECRET,
        token_scopes=DEST_SCOPES,
        project=DEST_PROJECT,
        base_url=f"https://{DEST_CLUSTER}.cognitedata.com",
        client_name="cognite-replicator-destination",
        debug=False,
    )
    assets.replicate(SOURCE_CLIENT, DEST_CLIENT)
    events.replicate(SOURCE_CLIENT, DEST_CLIENT, BATCH_SIZE, NUM_THREADS)
    files.replicate(SOURCE_CLIENT, DEST_CLIENT, BATCH_SIZE, NUM_THREADS)
    time_series.replicate(SOURCE_CLIENT, DEST_CLIENT, BATCH_SIZE, NUM_THREADS)
    datapoints.replicate(SOURCE_CLIENT, DEST_CLIENT)
    sequences.replicate(SOURCE_CLIENT, DEST_CLIENT, BATCH_SIZE, NUM_THREADS)
    sequence_rows.replicate(SOURCE_CLIENT, DEST_CLIENT, BATCH_SIZE, NUM_THREADS)
```

### Development

Change the version in the files
- cognite/replicator/_version.py
- pyproject.toml
- .github/workflows/cd.yml


## Changelog
Wondering about upcoming or previous changes? Take a look at the [CHANGELOG](https://github.com/cognitedata/cognite-replicator/blob/master/CHANGELOG.md).

## Contributing
Want to contribute? Check out [CONTRIBUTING](https://github.com/cognitedata/cognite-replicator/blob/master/CONTRIBUTING.md).
