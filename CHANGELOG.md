# Change Log

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/)
and this project adheres to [Semantic Versioning](https://semver.org/).

## [v2.1.0] - 2021-06-29

### Added

- *: Implement GSP-87 Feature Gates (#26)
- storage: Implement GSP-93 Add ObjectMode Pair (#29)

### Changed

- *: Implement GSP-109 Redesign Features (#29)
- *: Implement GSP-117 Rename Service to System as the Opposite to Global (#29)

### Upgraded

- build(deps): bump github.com/qiniu/go-sdk/v7 from 7.9.5 to 7.9.6 (#27)

## [v2.0.0] - 2021-05-24

### Added

- *: Implement GSP-47 & GSP-51 (#23)

### Changed

- storage: Idempotent storager delete operation (#22)
- *: Implement GSP-73 & GSP-76 (#24)

## [v1.1.0] - 2021-04-24

### Added

- *: Implement default pair support for service (#8)
- storage: Implement Create API (#15)
- *: Add UnimplementedStub (#17)
- tests: Introduce STORAGE_KODO_INTEGRATION_TEST (#18)
- storage: Implement GSP-40 (#20)

### Changed

- ci: Only run Integration Test while push to master

### Upgraded

- build(deps): bump github.com/qiniu/go-sdk/v7 from 7.9.0 to 7.9.5 (#19)

## v1.0.0 - 2021-02-18

### Added

- Implement kodo services.

[v2.1.0]: https://github.com/beyondstorage/go-service-kodo/compare/v2.0.0...v2.1.0
[v2.0.0]: https://github.com/beyondstorage/go-service-kodo/compare/v1.1.0...v2.0.0
[v1.1.0]: https://github.com/beyondstorage/go-service-kodo/compare/v1.0.0...v1.1.0
