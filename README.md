# Copy filter plugin for Embulk

[![Release CI Status Badge](https://github.com/civitaspo/embulk-filter-copy/workflows/Release%20CI/badge.svg)](https://github.com/civitaspo/embulk-filter-copy/actions?query=workflow%3A%22Release+CI%22)

Copy records and run another embulk by using them as input data source.

The Document for Japanese is [here](http://qiita.com/Civitaspo/items/da8483c28817071d90dc).

## Overview

* **Plugin type**: filter

## Configuration

- **copy**: Another embulk configurations except `in`. (array of `CopyEmbulkConfig`, optional)
  - Either **copy** or **config** option is required.
  - When **config** option is removed, this option become a required option.
- **config**: [DEPRECATED: Use **copy** option] Another embulk configurations except `in`. (`CopyEmbulkConfig`, optional)
  - ref. http://www.embulk.org/docs/built-in.html#embulk-configuration-file-format

### Configuration for `CopyEmbulkConfig`

- **name**: The name of the bulk load to copy. (string, optional)
- **exec**: The embulk executor plugin configuration. (config, optional)
  - **max_threads**: The maximum number of threads for that the bulk load runs concurrently. (int, default: The number of available CPU cores)
- **filters**: The embulk filter plugin configurations. (array of config, default: `[]`)
- **out**: The embulk output plugin configuration.


## Example

```yaml
filters:
  - type: copy
    copy:
      - name: copy-01
        filters:
          - type: remove_columns
            remove: ["t"]
        out:
          type: stdout
      - exec:
          max_threads: 4
        filters:
          - type: remove_columns
            remove: ["payload"]
        out:
          type: stdout
```

## Development

### Run the example

```shell
$ ./gradlew gem
$ embulk run example/config.yml -Ibuild/gemContents/lib
```

### Run tests

```shell
$ ./gradlew scalatest
```

### Build

```
$ ./gradlew gem --write-locks  # -t to watch change of files and rebuild continuously
```

### Release gem
Fix [build.gradle](./build.gradle), then


```shell
$ ./gradlew gemPush
```

## CHANGELOG

[CHANGELOG.md](./CHANGELOG.md)

## License

[MIT LICENSE](./LICENSE)
