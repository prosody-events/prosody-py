# Changelog

## [2.2.0](https://github.com/cincpro/prosody-py/compare/v2.1.2...v2.2.0) (2026-03-12)


### Features

* add Kafka telemetry emitter ([#123](https://github.com/cincpro/prosody-py/issues/123)) ([d85ea39](https://github.com/cincpro/prosody-py/commit/d85ea3913f8f7b54920499aef1c3a53a0c920a17))

## [2.1.2](https://github.com/cincpro/prosody-py/compare/v2.1.1...v2.1.2) (2026-03-05)


### Bug Fixes

* prevent false OffsetDeleted errors from concurrent loader requests ([#121](https://github.com/cincpro/prosody-py/issues/121)) ([88b0c17](https://github.com/cincpro/prosody-py/commit/88b0c17d8d87797c129b4867828be14ad32efeda))

## [2.1.1](https://github.com/cincpro/prosody-py/compare/v2.1.0...v2.1.1) (2026-02-26)


### Bug Fixes

* graceful slab_loader shutdown, remove timer backpressure gaps, and new API surface ([#117](https://github.com/cincpro/prosody-py/issues/117)) ([bf34f07](https://github.com/cincpro/prosody-py/commit/bf34f07fdc3c7460b7f7552f9cf0cef1bc1aa3c9))
* only log events, not spans ([#116](https://github.com/cincpro/prosody-py/issues/116)) ([1b8df39](https://github.com/cincpro/prosody-py/commit/1b8df391f6c8920bdf13ea042f210a2fc4aff48a))
* remove max_enqueued_per_key from type signatures ([#118](https://github.com/cincpro/prosody-py/issues/118)) ([f4b0f88](https://github.com/cincpro/prosody-py/commit/f4b0f885245785ebe7486f85ac1c725bc3e6b9e9))


### Performance Improvements

* use jemalloc allocator ([#114](https://github.com/cincpro/prosody-py/issues/114)) ([e4c8b1f](https://github.com/cincpro/prosody-py/commit/e4c8b1f16f307f286a4e528c9dc595b00fd3303a))

## [2.1.0](https://github.com/cincpro/prosody-py/compare/v2.0.7...v2.1.0) (2026-01-23)


### Features

* non-blocking timer retry ([#113](https://github.com/cincpro/prosody-py/issues/113)) ([8ebcc6c](https://github.com/cincpro/prosody-py/commit/8ebcc6c800d00e8fcc15b398a03fe20528cd2f44))

## [2.0.7](https://github.com/cincpro/prosody-py/compare/v2.0.6...v2.0.7) (2026-01-18)


### Bug Fixes

* update prosody to fix partition resume after rebalance ([#110](https://github.com/cincpro/prosody-py/issues/110)) ([c26845d](https://github.com/cincpro/prosody-py/commit/c26845df86ebcce67110d4dd8cda33b4a3fccb06))

## [2.0.6](https://github.com/cincpro/prosody-py/compare/v2.0.5...v2.0.6) (2026-01-10)


### Bug Fixes

* update prosody to fix timeout-induced partition stalls ([#108](https://github.com/cincpro/prosody-py/issues/108)) ([5897408](https://github.com/cincpro/prosody-py/commit/5897408257e3c30c9595e29fd2a3a56098c1a5fc))

## [2.0.5](https://github.com/cincpro/prosody-py/compare/v2.0.4...v2.0.5) (2025-12-22)


### Bug Fixes

* cache parent context instead of span in Kafka loader ([68b21fa](https://github.com/cincpro/prosody-py/commit/68b21fa8665e54d3b7eef12032cc9ea6d5226f77))

## [2.0.4](https://github.com/cincpro/prosody-py/compare/v2.0.3...v2.0.4) (2025-12-18)


### Bug Fixes

* downgrade span parent extraction failures to debug level ([#105](https://github.com/cincpro/prosody-py/issues/105)) ([1e13123](https://github.com/cincpro/prosody-py/commit/1e131237806e3692e2aaaf7d2752d2e94229ad2f))

## [2.0.3](https://github.com/cincpro/prosody-py/compare/v2.0.2...v2.0.3) (2025-12-16)


### Bug Fixes

* update prosody to fix cross-topic key collisions ([#103](https://github.com/cincpro/prosody-py/issues/103)) ([432d71e](https://github.com/cincpro/prosody-py/commit/432d71e243c36254974b5640e536c82934f05da0))

## [2.0.2](https://github.com/cincpro/prosody-py/compare/v2.0.1...v2.0.2) (2025-12-16)


### Bug Fixes

* bundle zlib statically ([#101](https://github.com/cincpro/prosody-py/issues/101)) ([0980502](https://github.com/cincpro/prosody-py/commit/09805028d5dcb0bb3c98d4466969458c75f36616))

## [2.0.1](https://github.com/cincpro/prosody-py/compare/v2.0.0...v2.0.1) (2025-12-16)


### Bug Fixes

* remove deprecated x86 macOS builds from release workflow ([#99](https://github.com/cincpro/prosody-py/issues/99)) ([98e3d40](https://github.com/cincpro/prosody-py/commit/98e3d4002bbd9acecc69927195a1810dcfb74dc6))

## [2.0.0](https://github.com/cincpro/prosody-py/compare/v1.3.0...v2.0.0) (2025-12-16)


### ⚠ BREAKING CHANGES

* add QoS middleware and rename shutdown to cancel ([#95](https://github.com/cincpro/prosody-py/issues/95))

### Features

* add QoS middleware and rename shutdown to cancel ([#95](https://github.com/cincpro/prosody-py/issues/95)) ([5a11b5d](https://github.com/cincpro/prosody-py/commit/5a11b5dabeb384d74aca426c0cf20c819efeab74))


### Bug Fixes

* replace pyo3-log with non-blocking Python logging layer ([#98](https://github.com/cincpro/prosody-py/issues/98)) ([169d7ec](https://github.com/cincpro/prosody-py/commit/169d7ec5e203875878cdcb725f2a0f5b0d958125))
* use thread-safe channels in timer tests ([#97](https://github.com/cincpro/prosody-py/issues/97)) ([ca80186](https://github.com/cincpro/prosody-py/commit/ca8018674017a8a74d23e8460eb48e55cdf77d45))

## [1.3.0](https://github.com/cincpro/prosody-py/compare/v1.2.0...v1.3.0) (2025-09-22)


### Features

* expose admin client ([#93](https://github.com/cincpro/prosody-py/issues/93)) ([12a070a](https://github.com/cincpro/prosody-py/commit/12a070aef4f596c544ee164c048d3e6f657c37a6))

## [1.2.0](https://github.com/cincpro/prosody-py/compare/v1.1.1...v1.2.0) (2025-09-19)


### Features

* expose source system and remove deprecated methods ([#90](https://github.com/cincpro/prosody-py/issues/90)) ([e19be1f](https://github.com/cincpro/prosody-py/commit/e19be1f11d65cbbbfa79944c216bde57f7e6009c))


### Bug Fixes

* propagate span traces through context methods ([#92](https://github.com/cincpro/prosody-py/issues/92)) ([4376b68](https://github.com/cincpro/prosody-py/commit/4376b682e13748388fe0632e034ab566a3a0f2be))

## [1.1.1](https://github.com/cincpro/prosody-py/compare/v1.1.0...v1.1.1) (2025-09-12)


### Performance Improvements

* bound message buffering ([3180942](https://github.com/cincpro/prosody-py/commit/31809423d07934d0cf79982a72920119d40f3460))

## [1.1.0](https://github.com/cincpro/prosody-py/compare/v1.0.2...v1.1.0) (2025-08-25)


### Features

* support regex topic subscriptions ([#87](https://github.com/cincpro/prosody-py/issues/87)) ([ce50fb3](https://github.com/cincpro/prosody-py/commit/ce50fb3878850c1c9d90025a2d36e2d0e3e298a4))

## [1.0.2](https://github.com/cincpro/prosody-py/compare/v1.0.1...v1.0.2) (2025-08-22)


### Bug Fixes

* add migration locks and jitter to timer slab loads ([#85](https://github.com/cincpro/prosody-py/issues/85)) ([076bb7c](https://github.com/cincpro/prosody-py/commit/076bb7c7cc3ddae9b975d7e4748e4c71c38d2daf))

## [1.0.1](https://github.com/cincpro/prosody-py/compare/v1.0.0...v1.0.1) (2025-08-12)


### Bug Fixes

* bump prosody to pick up OTEL fixes ([#83](https://github.com/cincpro/prosody-py/issues/83)) ([6c4714e](https://github.com/cincpro/prosody-py/commit/6c4714eaa5b033331804cdfc0e72e70293d214de))

## [1.0.0](https://github.com/cincpro/prosody-py/compare/v0.10.5...v1.0.0) (2025-07-23)


### ⚠ BREAKING CHANGES

* add timer scheduling support with Cassandra persistence ([#81](https://github.com/cincpro/prosody-py/issues/81))

### Features

* add timer scheduling support with Cassandra persistence ([#81](https://github.com/cincpro/prosody-py/issues/81)) ([1e12560](https://github.com/cincpro/prosody-py/commit/1e12560c31a13be9fd15852fcc0d042ad5c44daa))

## [0.10.5](https://github.com/cincpro/prosody-py/compare/v0.10.4...v0.10.5) (2025-03-21)


### Bug Fixes

* **consumer:** use librdkafka to commit and add additional liveness checks ([#75](https://github.com/cincpro/prosody-py/issues/75)) ([5a21543](https://github.com/cincpro/prosody-py/commit/5a215435578e46264d04ace61c6460dcb11ef2ee))

## [0.10.4](https://github.com/cincpro/prosody-py/compare/v0.10.3...v0.10.4) (2025-03-19)


### Bug Fixes

* **consumer:** don’t commit empty topic lists ([#73](https://github.com/cincpro/prosody-py/issues/73)) ([7f3ab54](https://github.com/cincpro/prosody-py/commit/7f3ab54b75bd696f92973876b228b84c349348dd))

## [0.10.3](https://github.com/cincpro/prosody-py/compare/v0.10.2...v0.10.3) (2025-03-19)


### Bug Fixes

* **consumer:** don't commit final offsets during a rebalance ([#71](https://github.com/cincpro/prosody-py/issues/71)) ([a3aa394](https://github.com/cincpro/prosody-py/commit/a3aa39482ab42afb2c839a37b2a412e2b9904739))

## [0.10.2](https://github.com/cincpro/prosody-py/compare/v0.10.1...v0.10.2) (2025-03-18)


### Bug Fixes

* **consumer:** prevent commits during rebalancing, add concurrency limit, fix watermark tracking ([#69](https://github.com/cincpro/prosody-py/issues/69)) ([f8874e0](https://github.com/cincpro/prosody-py/commit/f8874e09ad0bb88658b7508b477cef4c95e9bcfe))

## [0.10.1](https://github.com/cincpro/prosody-py/compare/v0.10.0...v0.10.1) (2025-03-14)


### Bug Fixes

* respect stall threshold ([#67](https://github.com/cincpro/prosody-py/issues/67)) ([24a7dff](https://github.com/cincpro/prosody-py/commit/24a7dffb2663f208c6c79f0884a8ac036f3d1855))

## [0.10.0](https://github.com/cincpro/prosody-py/compare/v0.9.3...v0.10.0) (2025-03-13)


### Features

* **consumer:** add shutdown timeout parameter ([#65](https://github.com/cincpro/prosody-py/issues/65)) ([d5ec01c](https://github.com/cincpro/prosody-py/commit/d5ec01c63a25ad840def284fb011c32bc338c278))

## [0.9.3](https://github.com/cincpro/prosody-py/compare/v0.9.2...v0.9.3) (2025-03-11)


### Bug Fixes

* always check for shutdown before retrying ([#63](https://github.com/cincpro/prosody-py/issues/63)) ([13331bb](https://github.com/cincpro/prosody-py/commit/13331bba5c8cbf6e16a27cd9ac7b5705f0c090aa))

## [0.9.2](https://github.com/cincpro/prosody-py/compare/v0.9.1...v0.9.2) (2025-03-07)


### Bug Fixes

* group id and source system env var fallback ([#61](https://github.com/cincpro/prosody-py/issues/61)) ([c93feb9](https://github.com/cincpro/prosody-py/commit/c93feb94094856f914525501cf24c9a07d1daaeb))

## [0.9.1](https://github.com/cincpro/prosody-py/compare/v0.9.0...v0.9.1) (2025-03-04)


### Bug Fixes

* ensure latest Rust is installed on MacOS builds ([#58](https://github.com/cincpro/prosody-py/issues/58)) ([a54dd15](https://github.com/cincpro/prosody-py/commit/a54dd155dd03549bddf8003f91a3cd9504a02afe))

## [0.9.0](https://github.com/cincpro/prosody-py/compare/v0.8.0...v0.9.0) (2025-03-04)


### Features

* add event filtering, source tracking, and fix backpressure deadlock ([#56](https://github.com/cincpro/prosody-py/issues/56)) ([8368a45](https://github.com/cincpro/prosody-py/commit/8368a459b3669cf2d75d50b7a0f17dc3d187b374))

## [0.8.0](https://github.com/cincpro/prosody-py/compare/v0.7.2...v0.8.0) (2025-01-08)


### Features

* Add message deduplication support ([#53](https://github.com/cincpro/prosody-py/issues/53)) ([b42293a](https://github.com/cincpro/prosody-py/commit/b42293a9ab0cd39e444f8a3ac32d76b4a70b39d3))

## [0.7.2](https://github.com/cincpro/prosody-py/compare/v0.7.1...v0.7.2) (2024-12-30)


### Bug Fixes

* attach traceback to error span ([#51](https://github.com/cincpro/prosody-py/issues/51)) ([a161f03](https://github.com/cincpro/prosody-py/commit/a161f03d7f6a0800b45114dcc22d91ae043e6384))

## [0.7.1](https://github.com/cincpro/prosody-py/compare/v0.7.0...v0.7.1) (2024-12-19)


### Bug Fixes

* upgrade macos CI runner ([5db8e64](https://github.com/cincpro/prosody-py/commit/5db8e64012d58e854efb53813c1585c08aabe14b))

## [0.7.0](https://github.com/cincpro/prosody-py/compare/v0.6.1...v0.7.0) (2024-12-19)


### Features

* best-effort mode ([#48](https://github.com/cincpro/prosody-py/issues/48)) ([069f9b8](https://github.com/cincpro/prosody-py/commit/069f9b838f100015dfb00fd9ac95a104d8a094d6))

## [0.6.1](https://github.com/cincpro/prosody-py/compare/v0.6.0...v0.6.1) (2024-12-02)


### Bug Fixes

* fix mocking, ci, and bump deps ([#46](https://github.com/cincpro/prosody-py/issues/46)) ([c817385](https://github.com/cincpro/prosody-py/commit/c81738534526a37087f53ba5ab9e629c809dc017))

## [0.6.0](https://github.com/RealGeeks/prosody-py/compare/v0.5.0...v0.6.0) (2024-10-23)


### Features

* **consumer:** add health check probes and stall detection ([#42](https://github.com/RealGeeks/prosody-py/issues/42)) ([48cdc9d](https://github.com/RealGeeks/prosody-py/commit/48cdc9dab028516b5d53ceebd5a7fa851aacef03))

## [0.5.0](https://github.com/RealGeeks/prosody-py/compare/v0.4.0...v0.5.0) (2024-09-18)


### Features

* asyncio.CancelledError propagation no longer required ([#33](https://github.com/RealGeeks/prosody-py/issues/33)) ([6cdd30d](https://github.com/RealGeeks/prosody-py/commit/6cdd30d15459adc05c68db88233619e82f435bc2))

## [0.4.0](https://github.com/RealGeeks/prosody-py/compare/v0.3.2...v0.4.0) (2024-09-17)


### Features

* permanent error support ([#31](https://github.com/RealGeeks/prosody-py/issues/31)) ([58fa4f2](https://github.com/RealGeeks/prosody-py/commit/58fa4f200b0c7a6e7d3e936e82361ba3781285b4))

## [0.3.2](https://github.com/RealGeeks/prosody-py/compare/v0.3.1...v0.3.2) (2024-08-29)


### Bug Fixes

* short circuit failures and properly configure producer mode ([#28](https://github.com/RealGeeks/prosody-py/issues/28)) ([a943d94](https://github.com/RealGeeks/prosody-py/commit/a943d94c2df87f8bcdf847fb68ddbad161007011))


### Performance Improvements

* **json:** upgrade Prosody ([#30](https://github.com/RealGeeks/prosody-py/issues/30)) ([90174a6](https://github.com/RealGeeks/prosody-py/commit/90174a64cb443a640c82c062baa4c21a4fa391e7))

## [0.3.1](https://github.com/RealGeeks/prosody-py/compare/v0.3.0...v0.3.1) (2024-08-19)


### Performance Improvements

* **consumer:** reduce cloning and message data movement ([#20](https://github.com/RealGeeks/prosody-py/issues/20)) ([3ff62dc](https://github.com/RealGeeks/prosody-py/commit/3ff62dc55db04eaf76c25da3d99d0b22729c56fe))

## [0.3.0](https://github.com/RealGeeks/prosody-py/compare/v0.2.0...v0.3.0) (2024-08-12)


### Features

* **consumer:** improve graceful shutdown ([#15](https://github.com/RealGeeks/prosody-py/issues/15)) ([c0b4c5d](https://github.com/RealGeeks/prosody-py/commit/c0b4c5d509940e9907efc2ebb354498255d6b275))

## [0.2.0](https://github.com/RealGeeks/prosody-py/compare/v0.1.0...v0.2.0) (2024-08-08)


### Features

* **consumer:** Implement graceful shutdown and error handling ([#12](https://github.com/RealGeeks/prosody-py/issues/12)) ([25c5f4a](https://github.com/RealGeeks/prosody-py/commit/25c5f4a5a7d6ef44268c3fc50344588c812bfe0d))

## 0.1.0 (2024-08-02)


### Features

* basic message consumption ([65f97d8](https://github.com/RealGeeks/prosody-py/commit/65f97d89bdb0d7657852c3429be31e7ecc025f91))
* capture send spans ([f5f94aa](https://github.com/RealGeeks/prosody-py/commit/f5f94aa1608c1eefde1679d6b043173db57caaa8))
* initial commit ([3f476b3](https://github.com/RealGeeks/prosody-py/commit/3f476b36a37861c6f5011d803f05d2b4fa445d7c))
* support high-level modes ([00630a8](https://github.com/RealGeeks/prosody-py/commit/00630a8ca450de2b72fbc999731c2643d38a0d7a))


### Bug Fixes

* bump dep for Windows rdkafka build support ([c0597ae](https://github.com/RealGeeks/prosody-py/commit/c0597ae46e0ad5fa4953903a43262cdee4867463))
* context propagation ([77d1e40](https://github.com/RealGeeks/prosody-py/commit/77d1e409d058467ac87e8edbfb8a51e5233b8305))
* ensure handler coroutine is run in the asyncio event loop ([d55a3ac](https://github.com/RealGeeks/prosody-py/commit/d55a3acdd9e050ac33f5aa6ad218bd8a183b6dfa))
* make handler available to gc ([6421b7d](https://github.com/RealGeeks/prosody-py/commit/6421b7d36f200af0eea095ed57d2ae76975595dc))
* package name ([#4](https://github.com/RealGeeks/prosody-py/issues/4)) ([808cd7b](https://github.com/RealGeeks/prosody-py/commit/808cd7b3292ec8d517c2fe49308c43bf7135d1e0))
* trigger other workflows on release branch ([#6](https://github.com/RealGeeks/prosody-py/issues/6)) ([7f16f3b](https://github.com/RealGeeks/prosody-py/commit/7f16f3b30080540f8f5035c1835d10d1125ba6fa))


### Performance Improvements

* don’t allocate and convert the datetime unless needed ([1bbe5b9](https://github.com/RealGeeks/prosody-py/commit/1bbe5b96a599f177df6b4911980dd19a569dfbfb))
* use args instead of kwargs ([d1c5940](https://github.com/RealGeeks/prosody-py/commit/d1c594045d3bb4ad917783ad5ff98718dcdc6a26))


### Miscellaneous Chores

* release 0.0.1 ([4288579](https://github.com/RealGeeks/prosody-py/commit/428857929d34ceed32beb79a87e40111a5eeeec7))
* release 0.1.0 ([9c2a84c](https://github.com/RealGeeks/prosody-py/commit/9c2a84cbd778489b8ec29d0f08e73e8075c3f943))
