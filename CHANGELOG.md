# Changelog

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
