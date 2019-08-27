## Development Instructions
### Setup
Get the code!
```bash
$ git clone https://github.com/cognitedata/cognite-replicator.git
$ cd cognite-replicator
```
Install dependencies and initialize a shell within the virtual environment.
```bash
$ poetry install
$ poetry shell
```
Install pre-commit hooks
```bash
$ pre-commit install
```

### Environment Variables
Set the following environment variables in a .env file:
```bash
COGNITE_CLIENT_NAME = python-replicator-integration-tests
COGNITE_MAX_RETRIES = 20

# Only necessary for running integration tests
COGNITE_PROJECT = python-replicator-test
COGNITE_API_KEY = <api-key>
COGNITE_BASE_URL = https://greenfield.cognitedata.com
```

### Testing
Set up tests for all new functionality.

Initiate unit tests by running the following command from the root directory:

`$ poetry run pytest --cov cognite`

If you want to generate code coverage reports run:

```
poetry run pytest --cov cognite --cov-report html \
                                --cov-report xml \
                                
```

Open `htmlcov/index.html` in the browser to navigate through the report.

### Documentation
Build html files of documentation locally by running
```bash
$ cd docs
$ make html
```
Documentation will be automatically generated from the google-style docstrings in the source code. It is then built and released when changes are merged into master.

### Release version conventions
See https://semver.org/
