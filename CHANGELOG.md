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
