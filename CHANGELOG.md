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


## [Planned]
- Support for replicating Sequences
- Support for replicating file data

## [Unreleased]

### Changed
- Use version for tag on images published to docker hub

## [0.7.1] - 2019-11-12

### Fixed
- Switch time series regex filter to external id instead of name field

## [0.7.0] - 2019-11-05

### Added
- Add ability to replicate time series by list of external ids
- Add ability to replicate unlinkable (asset-less) time series
- Add ability to replicate datapoints in a specific [start, end) range

### Fixed
- Added missing yaml dependency

## [0.6.0] - 2019-10-22

### Changed
- Running the package as a script now uses a yaml file for configuration, rather than command-line args.

## [0.5.0] - 2019-10-17

### Added
- Datapoint replication now provides `src_datapoint_transform` parameter to allow for transformations of
  source datapoints (e.g. adjust value, adjust timestamp)

- Datapoint replication now provides `timerange_transform` parameter to allow replication of arbitrary
  time ranges

## [0.4.1] - 2019-10-03

### Added
- Replication method `clear_replication_metadata` to remove the metadata added during replication

### Fixed
- Amount of data points pulled into memory now limited by default

## [0.4.0] - 2019-10-02

### Added
- Support for file metadata replication

## [0.3.2] - 2019-09-27

### Added
- Push to docker hub within Jenkins

### Changed
- Logging for datapoint replication simplified

### Fixed
- batch_size parameter for datapoints now consistent with other resource types

## [0.3.1] - 2019-09-25

### Changed
- Determines number of batches/jobs to do based on num_batches, rather than only on num_threads

### Fixed
- Handle exceptions in datapoint replication

## [0.3.0] - 2019-09-25

### Added
- Ability to fetch datapoints for a list of timeseries specified by external ids

### Changed
- Time series overlap checks between destination and source much faster

### Fixed
- Boundary cases of datapoint replication are handled properly - no duplicates at start time,
and no exclusion of final datapoint

## [0.2.4] - 2019-09-19

### Added
- Support for asset replication by subtree
- Support for restricting event and time series replication to events/time series with replicated assets

## [0.2.3] - 2019-09-19

### Fixed
- Events without assetIds can now be replicated as expected

## [0.2.2] - 2019-09-17

### Added
- Dockerfile to build docker image of the replicator
- Command line arguments for running the replicators. Try `poetry run replicator`

### Fixed
- Time series replication no longer attempts to create security category-protected time series
- Use pre-commit hooks to run black and unit tests
- Send logs to google cloud stackdriver if configured, needed the right dependencies.
