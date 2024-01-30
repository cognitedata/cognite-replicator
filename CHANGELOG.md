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

- Support for replicating file data

## [1.3.2] - 2023-01-30

## Fixed
- Obsolete datapoints insert API fixed.

## [1.3.1] - 2023-01-30

## Fixed
- Obsolete datapoints API usage fixed.

## [1.3.0] - 2023-01-29
- python upgrade to ^3.11
- pyyaml upgrade to ^6.0.1
- cognite-sdk-upgrade to ^7.13.8

## [1.2.6] - 2023-03-20

## Fixed
- naming convention fixed

## [1.2.5] - 2023-03-15

## Fixed
- typo fix

## [1.2.4] - 2023-03-10

## Added
- Adding replication for relationships
## Fixed
- Bug fix on dataset id
- Fixed replication for sequences
- Bug fix on deletion of objects not in source or destination

## [1.2.3] - 2023-02-21

## Fixed
- Bug fix on parameters

## [1.2.2] - 2023-02-09

## Fixed
- Updated python sdk version
- Bug fix

## [1.2.1] - 2023-10-08

## Fixed
- Datapoints copy update
- Updated python sdk version
- README update

## [1.0.1] - 2022-03-01

## Fixed
- Move to major version and bug fixes

## [0.9.2] - 2020-01-19

## Fixed
- Notification and error handling for identical timeseries entries in the config file
https://github.com/cognitedata/cognite-replicator/issues/126 

## [0.9.1] - 2020-01-15

## Changed
- Possibility to ignore replication of some fields, like metadata
https://github.com/cognitedata/cognite-replicator/issues/143 

## [0.9.0] - 2020-01-10

## Changed
- Support for replicating Sequences

## [0.8.2] - 2020-11-18

## Changed
- CI/CD from Jenkins to github actions
## Fixed
- Fix broken behavior: add arg to copy functions of events and files replicators (code was broken as a result of the addition of a 7th arg to the invoking replication.thread function)
- Add filtering by external id that existed in time series to events and files replicators
- Add exclude pattern by regex that existed in time series to events and files replicators

## [0.8.1] - 2020-07-06

## Fixed
- Typo fix: value_manipluation_lambda_fnc parameter (optional) renamed as value_manipulation_lambda_fnc

## [0.8.0] - 2020-06-19

## Added
- value_manipluation_lambda_fnc parameter (optional) added to datapoints.replicate function. A lambda function string 
  can be provided. This function takes each datapoint.value and gets applied to each point of a given timeseries. 
  Example: "lambda x: x*15"

## [0.7.6] - 2020-02-04

## Fixed
- Does not fail if timeseries does not exist
- Support copy to existing timeseries that were not copied by the replicator

## [0.7.5] - 2020-01-03
- Support configurable base url

## [0.7.4] - 2019-12-09

## Changed
- Default value for getting fetching datapoints 0 -> 31536000000, due to api restrictions

## Added
- Option for batch size specification with respect to datapoint replication
- Cleanup


## [0.7.3] - 2019-12-02

## Added
- Config parameter "timeseries_exclude_pattern" will now have effect on datapoints, opposed to only time series.
- The replicator will raise an exception if timeseries_exclude_pattern and timeseries_external_ids is given.

## [0.7.2] - 2019-11-15

## Added
- Provide config file path with env var COGNITE_CONFIG_FILE

### Changed
- Use version for tag on images published to docker hub
- Replicate datapoints with a consistent time series order

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
- Send logs to google cloud stackdriver if configured, needed the right dependencies
