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
- Command line arguments for running the replicators. Try 'poetry run replicator'

### Fixed
- Use pre-commit hooks to run black and unit tests
- Send logs to google cloud stackdriver if configured, needed the right dependencis.

## [0.2.2] - 2019-27-08
