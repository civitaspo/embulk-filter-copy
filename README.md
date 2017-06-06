# Copy filter plugin for Embulk

Copy records and run another embulk by using them as input data source.

## Overview

* **Plugin type**: filter

## Configuration

- **config**: another embulk configurations except `in`.
  - ref. http://www.embulk.org/docs/built-in.html#embulk-configuration-file-format

## Example

```yaml
filters:
  - type: copy
    config:
      filters:
        - type: remove_columns
          remove: ["id"]
      out:
        type: stdout
      exec:
        max_threads: 8
```

## Note

- This plugin works only on Java 1.8 or later.
- This plugin is **experimental** yet, so the specification may be changed.
- This plugin has more options than I write, but I do not write them because you do not have to change them currently.
- This plugin has no test yet, so may have some bugs.
- This plugin does not work on [embulk-executor-mapreduce](https://github.com/embulk/embulk-executor-mapreduce) yet.
- This plugin uses lots of memory now, because embulk run twice.

## Dependencies
- https://github.com/okumin/influent
- https://github.com/komamitsu/fluency
  - This plugin must use 1.1.0 for using the same msgpack version as embulk. 

## Development

### Run example:

```shell
$ ./gradlew classpath
$ embulk run example/config.yml -Ilib
```

### Run test:

TBD

### Release gem:
Fix [build.gradle](./build.gradle), then


```shell
$ ./gradlew gemPush

```

## ChangeLog

[CHANGELOG.md](./CHANGELOG.md)
