# Changelog

## [0.1.2](https://github.com/prosody-events/prosody-py/compare/prosody-py-v0.1.1...prosody-py-v0.1.2) (2026-04-30)


### Bug Fixes

* **deps:** upgrade prosody to 0.2.1 ([#12](https://github.com/prosody-events/prosody-py/issues/12)) ([f650f0c](https://github.com/prosody-events/prosody-py/commit/f650f0c0f31f554aadf3bd70c71d5e4d23e69692))


### Performance Improvements

* **deps:** migrate from jemalloc to mimalloc v3 ([#14](https://github.com/prosody-events/prosody-py/issues/14)) ([0d157d1](https://github.com/prosody-events/prosody-py/commit/0d157d137379aea482a4597817971c1118bdc9b3))

## [0.1.1](https://github.com/prosody-events/prosody-py/compare/prosody-py-v0.1.0...prosody-py-v0.1.1) (2026-04-20)


### Bug Fixes

* **deps:** upgrade prosody to 0.1.2 ([#9](https://github.com/prosody-events/prosody-py/issues/9)) ([1d27696](https://github.com/prosody-events/prosody-py/commit/1d276967dae4b12b615396fe9a250a04d9e02c14))

## 0.1.0 (2026-04-13)


### ⚠ BREAKING CHANGES

* add QoS middleware and rename shutdown to cancel ([#95](https://github.com/prosody-events/prosody-py/issues/95))
* add timer scheduling support with Cassandra persistence ([#81](https://github.com/prosody-events/prosody-py/issues/81))

### Features

* add event filtering, source tracking, and fix backpressure deadlock ([#56](https://github.com/prosody-events/prosody-py/issues/56)) ([9d6f3ad](https://github.com/prosody-events/prosody-py/commit/9d6f3adcefe4492b3946ae98d3de8e241e222018))
* add Kafka telemetry emitter ([#123](https://github.com/prosody-events/prosody-py/issues/123)) ([64517fc](https://github.com/prosody-events/prosody-py/commit/64517fcd3dca7386c0e6b49f0e17cb74ea7a3b58))
* Add message deduplication support ([#53](https://github.com/prosody-events/prosody-py/issues/53)) ([62d568d](https://github.com/prosody-events/prosody-py/commit/62d568d56839cced483a6930a3f44a0cfd7a6f68))
* add QoS middleware and rename shutdown to cancel ([#95](https://github.com/prosody-events/prosody-py/issues/95)) ([3b0d2db](https://github.com/prosody-events/prosody-py/commit/3b0d2db8a0657219d024cd270fbd3a4699a24814))
* add timer scheduling support with Cassandra persistence ([#81](https://github.com/prosody-events/prosody-py/issues/81)) ([282f278](https://github.com/prosody-events/prosody-py/commit/282f2788d79456fe4f96a94b14ae4eb8e912116e))
* asyncio.CancelledError propagation no longer required ([#33](https://github.com/prosody-events/prosody-py/issues/33)) ([444f174](https://github.com/prosody-events/prosody-py/commit/444f174a727f4f0abfd56780f3038cec70e8ea40))
* basic message consumption ([65f97d8](https://github.com/prosody-events/prosody-py/commit/65f97d89bdb0d7657852c3429be31e7ecc025f91))
* best-effort mode ([#48](https://github.com/prosody-events/prosody-py/issues/48)) ([f17dc8d](https://github.com/prosody-events/prosody-py/commit/f17dc8dfb5bb8e58e922444a698d3047d36a9ec1))
* capture send spans ([27b3041](https://github.com/prosody-events/prosody-py/commit/27b30412c1cbec952472ecb875712ded7795607a))
* configuration error surfacing, per-type timer semaphores, timer read performance ([#128](https://github.com/prosody-events/prosody-py/issues/128)) ([470859b](https://github.com/prosody-events/prosody-py/commit/470859bbccb2efe434143ee48f588f904b72c37e))
* **consumer:** add health check probes and stall detection ([#42](https://github.com/prosody-events/prosody-py/issues/42)) ([4a8dbc6](https://github.com/prosody-events/prosody-py/commit/4a8dbc66bc0f01001bacf145d77f4ed3709d3568))
* **consumer:** add shutdown timeout parameter ([#65](https://github.com/prosody-events/prosody-py/issues/65)) ([a73c452](https://github.com/prosody-events/prosody-py/commit/a73c45214f09d72f1ad9718035d02e5f409ee3f7))
* **consumer:** Implement graceful shutdown and error handling ([#12](https://github.com/prosody-events/prosody-py/issues/12)) ([83916a2](https://github.com/prosody-events/prosody-py/commit/83916a262942bb9c6734ddb53eb8cff6eb0a1702))
* **consumer:** improve graceful shutdown ([#15](https://github.com/prosody-events/prosody-py/issues/15)) ([a633b23](https://github.com/prosody-events/prosody-py/commit/a633b2358f3e736c7196967e2a409f3b2957424a))
* expose admin client ([#93](https://github.com/prosody-events/prosody-py/issues/93)) ([9bb65f7](https://github.com/prosody-events/prosody-py/commit/9bb65f7ee5f8c62d5e2d87bc5a7d50feaa51ab9d))
* expose source system and remove deprecated methods ([#90](https://github.com/prosody-events/prosody-py/issues/90)) ([3fb3c2a](https://github.com/prosody-events/prosody-py/commit/3fb3c2ab170f9abbf61214e4fd712ae6f9e6c566))
* initial commit ([3f476b3](https://github.com/prosody-events/prosody-py/commit/3f476b36a37861c6f5011d803f05d2b4fa445d7c))
* non-blocking timer retry ([#113](https://github.com/prosody-events/prosody-py/issues/113)) ([c3a3ccb](https://github.com/prosody-events/prosody-py/commit/c3a3ccbc8a8b5e3b2f4538f15333682816ab08b9))
* permanent error support ([#31](https://github.com/prosody-events/prosody-py/issues/31)) ([f739b0e](https://github.com/prosody-events/prosody-py/commit/f739b0e773254c92afbe3bf38b35283ed42d4595))
* persistent deduplication with global cache and Cassandra backend ([#130](https://github.com/prosody-events/prosody-py/issues/130)) ([46cc1b7](https://github.com/prosody-events/prosody-py/commit/46cc1b73afb1728ead676805868f44ee2615263e))
* protect client methods against post-fork usage ([#133](https://github.com/prosody-events/prosody-py/issues/133)) ([edbdc08](https://github.com/prosody-events/prosody-py/commit/edbdc08614b07073a2d0c07c6591f302df372b68))
* Sentry error monitoring for handler dispatch ([#132](https://github.com/prosody-events/prosody-py/issues/132)) ([f172da7](https://github.com/prosody-events/prosody-py/commit/f172da76e4086ce27828b6efe38b6821e3034e18))
* shutdown grace period and configurable span relation ([#134](https://github.com/prosody-events/prosody-py/issues/134)) ([e6766dc](https://github.com/prosody-events/prosody-py/commit/e6766dc15d7690bf19e2b2c5a065ad78a2332b61))
* support high-level modes ([00630a8](https://github.com/prosody-events/prosody-py/commit/00630a8ca450de2b72fbc999731c2643d38a0d7a))
* support regex topic subscriptions ([#87](https://github.com/prosody-events/prosody-py/issues/87)) ([2ba0d6c](https://github.com/prosody-events/prosody-py/commit/2ba0d6c24a493bb38215e5bf82a6ddc4c68b4173))


### Bug Fixes

* add migration locks and jitter to timer slab loads ([#85](https://github.com/prosody-events/prosody-py/issues/85)) ([d4693c9](https://github.com/prosody-events/prosody-py/commit/d4693c9433bf49e6cc751945b5bb813ae46afb3f))
* always check for shutdown before retrying ([#63](https://github.com/prosody-events/prosody-py/issues/63)) ([5106440](https://github.com/prosody-events/prosody-py/commit/510644047a5184f2f45cb61a0124e63b50c98caf))
* attach traceback to error span ([#51](https://github.com/prosody-events/prosody-py/issues/51)) ([de4ca93](https://github.com/prosody-events/prosody-py/commit/de4ca93f4118e5369f46e7052a071427f832712d))
* bump dep for Windows rdkafka build support ([c0597ae](https://github.com/prosody-events/prosody-py/commit/c0597ae46e0ad5fa4953903a43262cdee4867463))
* bump prosody to pick up OTEL fixes ([#83](https://github.com/prosody-events/prosody-py/issues/83)) ([4c5fe25](https://github.com/prosody-events/prosody-py/commit/4c5fe25e61bb1284c1b99e4cde4639e3999a0f15))
* bundle zlib statically ([#101](https://github.com/prosody-events/prosody-py/issues/101)) ([7f23f01](https://github.com/prosody-events/prosody-py/commit/7f23f0150b2efed309547c7e0a19a408f339a330))
* cache parent context instead of span in Kafka loader ([b8142f7](https://github.com/prosody-events/prosody-py/commit/b8142f7090a59c4fba4228fb430d512f4a42e945))
* **consumer:** don’t commit empty topic lists ([#73](https://github.com/prosody-events/prosody-py/issues/73)) ([7a44f93](https://github.com/prosody-events/prosody-py/commit/7a44f931f3d2bbbaa2391ae057ddff790eee58be))
* **consumer:** don't commit final offsets during a rebalance ([#71](https://github.com/prosody-events/prosody-py/issues/71)) ([f7bfbee](https://github.com/prosody-events/prosody-py/commit/f7bfbee207b6776b78b3cf409b3faccbbdc7fc4e))
* **consumer:** prevent commits during rebalancing, add concurrency limit, fix watermark tracking ([#69](https://github.com/prosody-events/prosody-py/issues/69)) ([d8c1ff8](https://github.com/prosody-events/prosody-py/commit/d8c1ff84abbaa57435b2add15bab3a2fb68d3941))
* **consumer:** use librdkafka to commit and add additional liveness checks ([#75](https://github.com/prosody-events/prosody-py/issues/75)) ([00d1983](https://github.com/prosody-events/prosody-py/commit/00d1983489a9248c53454275022ba6b21ebb0ee2))
* context propagation ([90ffde9](https://github.com/prosody-events/prosody-py/commit/90ffde9a299209ac7632b4595a3cd57f112da1ba))
* downgrade span parent extraction failures to debug level ([#105](https://github.com/prosody-events/prosody-py/issues/105)) ([cce6610](https://github.com/prosody-events/prosody-py/commit/cce6610f31df1f7914c8d6adcbf5282d5ba5e77b))
* ensure handler coroutine is run in the asyncio event loop ([d55a3ac](https://github.com/prosody-events/prosody-py/commit/d55a3acdd9e050ac33f5aa6ad218bd8a183b6dfa))
* ensure latest Rust is installed on MacOS builds ([#58](https://github.com/prosody-events/prosody-py/issues/58)) ([0404988](https://github.com/prosody-events/prosody-py/commit/0404988435c184d50e12eeff7aa573a4c27c8f0d))
* fix mocking, ci, and bump deps ([#46](https://github.com/prosody-events/prosody-py/issues/46)) ([1c49bfe](https://github.com/prosody-events/prosody-py/commit/1c49bfee6f35067794a9b820acd1b8f569a60c6a))
* graceful slab_loader shutdown, remove timer backpressure gaps, and new API surface ([#117](https://github.com/prosody-events/prosody-py/issues/117)) ([5abec25](https://github.com/prosody-events/prosody-py/commit/5abec251d32d4e25464f42a9114389a0cd55de6f))
* group id and source system env var fallback ([#61](https://github.com/prosody-events/prosody-py/issues/61)) ([ef3d72c](https://github.com/prosody-events/prosody-py/commit/ef3d72c12dd4e23691c841665f67795aecd5fa0c))
* make handler available to gc ([6421b7d](https://github.com/prosody-events/prosody-py/commit/6421b7d36f200af0eea095ed57d2ae76975595dc))
* only log events, not spans ([#116](https://github.com/prosody-events/prosody-py/issues/116)) ([61fa1fb](https://github.com/prosody-events/prosody-py/commit/61fa1fbc2f0e6245150ff4aa80bfc5d93dadb11d))
* package name ([#4](https://github.com/prosody-events/prosody-py/issues/4)) ([ab2f375](https://github.com/prosody-events/prosody-py/commit/ab2f375baba28361104330fbfa88863b24e5a505))
* prevent false OffsetDeleted errors from concurrent loader requests ([#121](https://github.com/prosody-events/prosody-py/issues/121)) ([5bba955](https://github.com/prosody-events/prosody-py/commit/5bba9550655240c618787579103cff32b96dedfa))
* propagate span traces through context methods ([#92](https://github.com/prosody-events/prosody-py/issues/92)) ([dcb63fe](https://github.com/prosody-events/prosody-py/commit/dcb63fe589fcf3e7eacd0bdf784552de10772fc3))
* remove deprecated x86 macOS builds from release workflow ([#99](https://github.com/prosody-events/prosody-py/issues/99)) ([6622539](https://github.com/prosody-events/prosody-py/commit/6622539c69c4ecdb6d65f3c095dbfffbb617d639))
* remove max_enqueued_per_key from type signatures ([#118](https://github.com/prosody-events/prosody-py/issues/118)) ([9163c23](https://github.com/prosody-events/prosody-py/commit/9163c23e6ff262707e21866e1e634ca8a6810e9f))
* replace pyo3-log with non-blocking Python logging layer ([#98](https://github.com/prosody-events/prosody-py/issues/98)) ([11f8215](https://github.com/prosody-events/prosody-py/commit/11f821542312e86ee83148ceb3aea61d251b0cc0))
* respect stall threshold ([#67](https://github.com/prosody-events/prosody-py/issues/67)) ([a9060a3](https://github.com/prosody-events/prosody-py/commit/a9060a3dbbaeb95a05c0004193d09feaa07ab70e))
* short circuit failures and properly configure producer mode ([#28](https://github.com/prosody-events/prosody-py/issues/28)) ([2744603](https://github.com/prosody-events/prosody-py/commit/27446036459cc22e45fdd20d3476aa9cda46c9bf))
* telemetry event_time uses millisecond precision with Z suffix ([#126](https://github.com/prosody-events/prosody-py/issues/126)) ([947b541](https://github.com/prosody-events/prosody-py/commit/947b54133cabdf16f15ebeb91c551855cc2b2920))
* trigger other workflows on release branch ([#6](https://github.com/prosody-events/prosody-py/issues/6)) ([70a17c3](https://github.com/prosody-events/prosody-py/commit/70a17c38fce477f464d16c4106430dca85a54daa))
* update prosody to fix cross-topic key collisions ([#103](https://github.com/prosody-events/prosody-py/issues/103)) ([c85d91d](https://github.com/prosody-events/prosody-py/commit/c85d91dd0202462fca124138fc61fb08688197e4))
* update prosody to fix partition resume after rebalance ([#110](https://github.com/prosody-events/prosody-py/issues/110)) ([3a7f97c](https://github.com/prosody-events/prosody-py/commit/3a7f97ccc2e209d5aabe49189d1f42cf7e3f04ac))
* update prosody to fix timeout-induced partition stalls ([#108](https://github.com/prosody-events/prosody-py/issues/108)) ([845ae1a](https://github.com/prosody-events/prosody-py/commit/845ae1a63d4b4524c02cf31a30936c69c5a92d77))
* upgrade macos CI runner ([befb975](https://github.com/prosody-events/prosody-py/commit/befb9757550be8cfe20604cb2548d18580b1bf06))
* use thread-safe channels in timer tests ([#97](https://github.com/prosody-events/prosody-py/issues/97)) ([f7b60d7](https://github.com/prosody-events/prosody-py/commit/f7b60d7cd2ce83bc576353ab2abed78ad5b95a88))


### Performance Improvements

* bound message buffering ([35278ba](https://github.com/prosody-events/prosody-py/commit/35278bab0f4a43323006bf08d34d98911edff1bb))
* **consumer:** reduce cloning and message data movement ([#20](https://github.com/prosody-events/prosody-py/issues/20)) ([1dbbc46](https://github.com/prosody-events/prosody-py/commit/1dbbc4698c8ecb21f48cf3ac844ce40f2536d7e8))
* don’t allocate and convert the datetime unless needed ([1bbe5b9](https://github.com/prosody-events/prosody-py/commit/1bbe5b96a599f177df6b4911980dd19a569dfbfb))
* **json:** upgrade Prosody ([#30](https://github.com/prosody-events/prosody-py/issues/30)) ([60d7432](https://github.com/prosody-events/prosody-py/commit/60d7432574e5391da70a698f63969ad4f99520c7))
* use args instead of kwargs ([f72d513](https://github.com/prosody-events/prosody-py/commit/f72d513003616dd3c87720d81ec462178281f630))
* use jemalloc allocator ([#114](https://github.com/prosody-events/prosody-py/issues/114)) ([68f77d5](https://github.com/prosody-events/prosody-py/commit/68f77d5c86700388f3b467e20c8e37b4042ae3a3))


### Miscellaneous Chores

* release 0.0.1 ([fa8c40f](https://github.com/prosody-events/prosody-py/commit/fa8c40fcc89173ebdfcc95d9532582149db19062))
* release 0.1.0 ([a75476c](https://github.com/prosody-events/prosody-py/commit/a75476c2f025a86d247f8840c02145f3dacaf339))
