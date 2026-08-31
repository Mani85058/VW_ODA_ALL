# Changelog
All notable changes to this project will be documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html.

## [1.2.x] - xxxx-xx-xx - [SQA-xxxx](https://devstack.vwgroup.com/jira/browse/SQA-xxxx)

### Added
- x
### Changed
- x
### Removed
- x
## [2.0.0.2] - 2025-09-30 - [SQA-18317](https://devstack.vwgroup.com/jira/browse/SQA-18317)

### Changed
- fixed schema change issue for RVS core files

## [2.0.0.1] - 2024-06-12 - [SQA-12465](https://devstack.vwgroup.com/jira/browse/SQA-12465)

### Changed
- Usage of CoRE file to RVS core file

## [1.4.1.01] - 2024-04-04 - [SQA-12833](https://devstack.vwgroup.com/jira/browse/SQA-12833)

### Changed
- Revert packages version to be able to execute via Argo & Openshift
- Ignore vulnerability (-i 54576 -i 62892 -i 64192) (see https://devstack.vwgroup.com/bamboo/download/SQA-SQABDPCONBDEV-JOB1/build_logs/SQA-SQABDPCONBDEV-JOB1-70.log)

## [1.4.1.00] - 2024-03-25 - [SQA-12433](https://devstack.vwgroup.com/jira/browse/SQA-12433)

### Changed
- API version in configuration files from v8 to v11
- CoRE path from delta to Static file *MODEL_CORE.csv*
- SparkApplication session configuration to align with new spark version
- Spark function for retrieving productID from MODEL_CORE
- JSON extracting logic when process with *ecuVersions_items*

## [1.2.5] - 2023-08-28 - [SQA-9469](https://devstack.vwgroup.com/jira/browse/SQA-9469)

### Changed
- Added sonar.python.xunit.skipDetails=true line to sonar properties file


## [1.2.4] - 2023-08-14 - [SQA-8659](https://devstack.vwgroup.com/jira/browse/SQA-8659)

### Changed
- Improved the codebase by resolving bugs, vulnerabilities, and code smells suggested by SonarQube Scanning
- Upgraded the package 'sqlalchemy' from 1.4.26 to 2.0.19 due to vulnerability thread (see https://pyup.io/v/51668/742/)
- Upgraded the package 'cryptography' from 41.0.0 to 41.0.3 due to vulnerability thread (see https://pyup.io/v/59473/742/)

## [1.2.3] - 2023-07-24 - [SQA-5315](https://devstack.vwgroup.com/jira/browse/SQA-5315)

### Changed
- Updated to cryptography = "==41.0.0" pyopenssl = "==23.2.0" versions because of Vulnerability ID: 59062 (see https://pyup.io/vulnerabilities/CVE-2023-2650/59062/)

## [1.2.2] - 2023-06-26 - [SQA-5364](https://devstack.vwgroup.com/jira/browse/SQA-5364)

### Changed
- New sonar.projectKey and sonar.projectName
- Quick fix to requests version from 2.26.0 -> 2.31.0 

## [1.2.1] - 2023-05-26 - [SQA-7628](https://devstack.vwgroup.com/jira/browse/SQA-7628)

### Changed
- Changed pyspark [ 3.1.2 -> 3.2.2] and delta-spark [ 1.0.0 -> 2.0.0 ] versions.
