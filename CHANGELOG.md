0.2.0 (2020-05-07)
==================
- [Enhancement] [#15](https://github.com/civitaspo/embulk-filter-copy/pull/15) Use the `embulk-plugins` gradle plugin.
- [Enhancement] [#15](https://github.com/civitaspo/embulk-filter-copy/pull/15) Re-write as a scala project.
- [Enhancement] [#15](https://github.com/civitaspo/embulk-filter-copy/pull/15) Deploy the gem to rubygems.org automatically.
- [Enhancement] [#15](https://github.com/civitaspo/embulk-filter-copy/pull/15) Support multiple copies
- [Breaking Change] [#15](https://github.com/civitaspo/embulk-filter-copy/pull/15) Remove [the Fluentd forward protocol](https://github.com/fluent/fluentd/wiki/Forward-Protocol-Specification-v1) dependencies.
  - Remove the public internal configuration for the network.

0.1.0 (2019-09-26)
==================
* Follow embulk v0.9.x interfaces.
* Packing as MessagePack become faster because, when parsing or formatting `Timestamp` values, use only `epochSecond` and `nanoAdjustment` instead of `TimestampFormatter` and `TimestampParser`.

0.0.2 (2017-07-01)
==================
* Write a default values test.
* Change EmbulkExecutorService to EmbulkExecutor/LocalThreadExecutor for #6

0.0.1 (2017-06-07)
==================
* first release.
