# Copy filter plugin for Embulk

Copy records and run another embulk by using them as input data source.

The Document for Japanese is [here](http://qiita.com/Civitaspo/items/da8483c28817071d90dc).

## Overview

* **Plugin type**: filter

## Configuration

- **copy**: another embulk configurations except `in`. (array of config, optional)
  - Either **copy** or **config** option is required.
  - When **config** option is removed, this option become a required option.
- **config**: [DEPRECATED: Use **copy** option] another embulk configurations except `in`. (config, optional)
  - ref. http://www.embulk.org/docs/built-in.html#embulk-configuration-file-format

## Example

```yaml
filters:
  - type: copy
    copy:
      - filters:
          - type: remove_columns
            remove: ["id"]
        out:
          type: stdout
        exec:
          max_threads: 8
      - filters:
          - type: remove_columns
            remove: ["id"]
        out:
          type: stdout
        exec:
          max_threads: 8
```

## Note

- This plugin is **experimental** yet, so the specification may be changed.
- If you have any problems or opinions, I'm glad if you raise an issue.


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
