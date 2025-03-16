# CHANGELOG


## v0.5.0 (2025-03-16)

### Bug Fixes

- Moves sql higher in build precedence
  ([`2553f32`](https://github.com/UCSD-E4E/fishsense-data-processing-spider/commit/2553f3243039b35b841657540bc135c46ed58ddf))

### Features

- Added batching
  ([`aefa3d0`](https://github.com/UCSD-E4E/fishsense-data-processing-spider/commit/aefa3d0c6a6e3e95ee9a8aa0333110497a6a4d63))

- Adds dive checksumming
  ([`87f4216`](https://github.com/UCSD-E4E/fishsense-data-processing-spider/commit/87f421663966589a29aec9d3cf1deeaf8b5bd38b))

- Adds ignore column
  ([`96e834f`](https://github.com/UCSD-E4E/fishsense-data-processing-spider/commit/96e834f4d844318485fdd4e5460f9393461cf25e))


## v0.4.0 (2025-03-15)

### Chores

- Increases summary interval
  ([`6e790e3`](https://github.com/UCSD-E4E/fishsense-data-processing-spider/commit/6e790e3ebc87e49dfa09f42ca56d9ac98ef0ba21))

### Continuous Integration

- Fixes concurency
  ([`ead1b7f`](https://github.com/UCSD-E4E/fishsense-data-processing-spider/commit/ead1b7f316ccc5417087a3dfcc0d006754c00145))

### Features

- Adds summary scraper
  ([`38dd18e`](https://github.com/UCSD-E4E/fishsense-data-processing-spider/commit/38dd18eff24f25a90684aeb0c1a976dcf080719b))


## v0.3.1 (2025-03-15)

### Bug Fixes

- Adds error catch for exiftool
  ([`0ff8a43`](https://github.com/UCSD-E4E/fishsense-data-processing-spider/commit/0ff8a43ff9fe8d6b7306c30374d79e59f470d0ae))


## v0.3.0 (2025-03-15)

### Bug Fixes

- Fixes exif
  ([`7d7fee1`](https://github.com/UCSD-E4E/fishsense-data-processing-spider/commit/7d7fee1044b097805f243ed2db1f0440684a09ce))

### Code Style

- Removes unused import
  ([`e51bd3b`](https://github.com/UCSD-E4E/fishsense-data-processing-spider/commit/e51bd3bb1a791e2f1ce2106a3f6a0afe710283c8))

### Features

- Adds exif
  ([`8a95b0f`](https://github.com/UCSD-E4E/fishsense-data-processing-spider/commit/8a95b0f1b74a09908f9581591e933c0890a02de3))


## v0.2.0 (2025-03-15)

### Bug Fixes

- Changes mount point
  ([`d5934df`](https://github.com/UCSD-E4E/fishsense-data-processing-spider/commit/d5934df5c8d271a67c49d5a604f323fe0c9cf22d))

- Updates data path
  ([`9fd9dfd`](https://github.com/UCSD-E4E/fishsense-data-processing-spider/commit/9fd9dfd1d6344ec903f84097005ef100f35f1f6f))

### Chores

- Adds postgres.username to settings file
  ([`a7f7496`](https://github.com/UCSD-E4E/fishsense-data-processing-spider/commit/a7f749634108fa47e37c4fd40f771917cfdbcf1e))

### Code Style

- Fixing pylint errors
  ([`8b07076`](https://github.com/UCSD-E4E/fishsense-data-processing-spider/commit/8b0707631b6443b2089a0cc608a7f0391e1d60d4))

### Continuous Integration

- Adds memory limit
  ([`aac0200`](https://github.com/UCSD-E4E/fishsense-data-processing-spider/commit/aac02001f6e5aa26df331bc9cf19143585c4ed06))

- Fixes memory limit
  ([`8606ceb`](https://github.com/UCSD-E4E/fishsense-data-processing-spider/commit/8606ceb30f0cffe6818618206ce179d987faaa4b))

- Switches to env var for testing
  ([`4a1877d`](https://github.com/UCSD-E4E/fishsense-data-processing-spider/commit/4a1877dabae5a06c6369f560bd59d02a9245dc2a))

### Features

- Adds image checksumming
  ([`85d73ba`](https://github.com/UCSD-E4E/fishsense-data-processing-spider/commit/85d73baeaf6eea03608c1adad4fb546db3621b51))

- Adds logging
  ([`bed78a7`](https://github.com/UCSD-E4E/fishsense-data-processing-spider/commit/bed78a790911afb4b61f87d95c6090758494cdac))

- Adds logic to prevent re-processing old images
  ([`fc535ce`](https://github.com/UCSD-E4E/fishsense-data-processing-spider/commit/fc535ce1d92650eb167ca38945109cdc6212043b))

- Adds main scraping logic
  ([`ea7f844`](https://github.com/UCSD-E4E/fishsense-data-processing-spider/commit/ea7f84451254c61ffd60ef33ed87c2bde9ac0bc5))

- Adds postgres
  ([`66daa19`](https://github.com/UCSD-E4E/fishsense-data-processing-spider/commit/66daa19dce6923f0a53c87abf7435a32976f7972))

- Adds scrape interval
  ([`04f7b77`](https://github.com/UCSD-E4E/fishsense-data-processing-spider/commit/04f7b77a3a9c57381b2303ad601e9e2256ee220e))


## v0.1.0 (2025-03-15)

### Continuous Integration

- Adds catch for release
  ([`fdcf0ff`](https://github.com/UCSD-E4E/fishsense-data-processing-spider/commit/fdcf0ff9a965b10d77c0adfb49db14c595b48298))

- Fixes docker tag regex
  ([`be0045b`](https://github.com/UCSD-E4E/fishsense-data-processing-spider/commit/be0045b7bee02703f416fa2f7772559f1ee4bb85))

### Features

- Adds dynaconf
  ([`beb7f20`](https://github.com/UCSD-E4E/fishsense-data-processing-spider/commit/beb7f20ae1a126a7e731971799086d64a1dd87b2))


## v0.0.0 (2025-03-15)

### Chores

- Create .gitignore
  ([`2a17db3`](https://github.com/UCSD-E4E/fishsense-data-processing-spider/commit/2a17db34611e65adaa21bd7e5b79086ccc0e3da3))

- Initial project structure
  ([`863cb63`](https://github.com/UCSD-E4E/fishsense-data-processing-spider/commit/863cb63fae31a1358935554ad9bc38a7a18b3a2d))

### Continuous Integration

- Fixes python version
  ([`3dd3fb8`](https://github.com/UCSD-E4E/fishsense-data-processing-spider/commit/3dd3fb8222e853fd24bebe9af82d1e4640994f52))
