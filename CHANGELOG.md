## 2.0.4
 - Add constructional method to allow an eviction specific block to be set.

## 2.0.3
 - Add pseudo codec IdentityMapCodec.  Support class for identity based multiline processing.

## 2.0.0
 - Plugins were updated to follow the new shutdown semantic, this mainly allows Logstash to instruct input plugins to terminate gracefully,
   instead of using Thread.raise on the plugins' threads. Ref: https://github.com/elastic/logstash/pull/3895
 - Dependency on logstash-core update to 2.0

