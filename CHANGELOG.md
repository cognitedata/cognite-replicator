# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Changes are grouped as follows
- `Added` for new features.
- `Changed` for changes in existing functionality.
- `Deprecated` for soon-to-be removed features.
- `Removed` for now removed features.
- `Fixed` for any bug fixes.
- `Security` in case of vulnerabilities.

## [Unreleased]

### Added
- Dockerfile to build docker image of the replicator
- Command line arguments for running the replicators. Try 'poetry run replicator'

### Fixed
- Use pre-commit hooks to run black and unit tests
- Send logs to google cloud stackdriver if configured, needed the right dependencis.

## [0.2.2] - 2019-27-08

### Fixed
- Time series replication no longer attempts to create security category-protected time series

## [0.2.3] - 2019-19-08

### Fixed
- Events without assetIds can now be replicated as expected

## [0.2.4] - 2019-19-09

### Added
- Support for asset replication by subtree
- Support for restricting event and time series replication to events/time series with replicated assets

## [0.3.0] - 2019-25-09

### Added
- Ability to fetch datapoints for a list of timeseries specified by external ids

### Changed
- Time series overlap checks between destination and source much faster

### Fixed
- Boundary cases of datapoint replication are handled properly - no duplicates at start time,
and no exclusion of final datapoint


## [0.3.1] - 2019-25-09

### Changed
- Determines number of batches/jobs to do based on num_batches, rather than only on num_threads

### Fixed
- Handle exceptions in datapoint replication

## [0.3.2] - 2019-27-09

### Added
- Push to docker hub within Jenkins

### Changed
- Logging for datapoint replication simplified

### Fixed
- batch_size parameter for datapoints now consistent with other resource types

## [0.4.0] - 2019-02-10

### Added
- Support for file metadata replication

## [0.4.1] - 2019-03-10

### Added
- Replication method clear_replication_metadata to remove the metadata added during replication

### Fixed
- Amount of data points pulled into memory now limited by default