## Development Instructions
### Setup
Get the code!
```bash
$ git clone https://github.com/cognitedata/cognite-replicator.git
$ cd cognite-replicator
```

Install poetry:
```bash
curl -sSL https://raw.githubusercontent.com/sdispater/poetry/master/get-poetry.py | python
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

### Run it from the command line

```bash
poetry run replicator -h
```


### Release version conventions
See https://semver.org/
