# Cognite Python Replicator
Cognite Replicator is a Python package for replicating data across Cognite Data Fusion (CDF) tenants. This package is
built on top of the Cognite Python SDK. Currently this package supports replicating assets, events, and time series
metadata.

## Prerequisites
In order to start using the Replicator, you need:
* Python3 (>= 3.6)
* Cognite Python SDK (>=1.0.2)
* Two API keys, one for your source tenant and one for your destination tenant.
    * Read x to learn more about what capabilities these keys should have.
    * Never include the API key directly in the code or upload the key to github.
    Instead, set the API key as an environment variable or as a secret if using Databricks.

This is how you set the API key as an environment variable on Mac OS and Linux:
```python
$ export SRC_API_KEY=<your source API key>
$ export DST_API_KEY=<your destination API key>
```

## Installation


## Documentation


## Usage

### Setup
```python
from cognite.client import CogniteClient
from cognite_replicator import replication
from cognite_replicator import assets

PROJECT_SRC = "Name of source tenant"
PROJECT_DST = "Name of destination tenant"
CLIENT_NAME = "cognite-replicator"

CLIENT_SRC = CogniteClient(api_key=SRC_API_KEY, project=PROJECT_SRC, client_name=CLIENT_NAME)
CLIENT_DST = CogniteClient(api_key=DST_API_KEY, project=PROJECT_DST, client_name=CLIENT_NAME, timeout=90)
```

### Replicate Assets
```python
from cognite_replicator import assets

assets.replicate(PROJECT_SRC, CLIENT_SRC, PROJECT_DST, CLIENT_DST)
```

### Replicate Events
```python
from cognite_replicator import events

events.replicate(PROJECT_SRC, CLIENT_SRC, PROJECT_DST, CLIENT_DST)
```

### Replicate Time Series
```python
from cognite_replicator import time_series

time_series.replicate(PROJECT_SRC, CLIENT_SRC, PROJECT_DST, CLIENT_DST)
```
