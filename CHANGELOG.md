# Changelog

## [0.2.0](https://github.com/RealGeeks/prosody-py/compare/v0.1.0...v0.2.0) (2024-08-02)


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


### Performance Improvements

* don’t allocate and convert the datetime unless needed ([1bbe5b9](https://github.com/RealGeeks/prosody-py/commit/1bbe5b96a599f177df6b4911980dd19a569dfbfb))
* use args instead of kwargs ([d1c5940](https://github.com/RealGeeks/prosody-py/commit/d1c594045d3bb4ad917783ad5ff98718dcdc6a26))
