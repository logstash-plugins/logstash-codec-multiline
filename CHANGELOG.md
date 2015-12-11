## 2.0.5
 - Add auto_flush config option, with no default. If not set, no auto_flush is done.
 - Add evict method to identity_map_codec that allows for an input, when done with an identity, to auto_flush and remove the identity from the map.

## 2.0.4
 - Add constructional method to allow an eviction specific block to be set.

## 2.0.3
 - Add pseudo codec IdentityMapCodec.  Support class for identity based multiline processing.

## 2.0.0
 - Plugins were updated to follow the new shutdown semantic, this mainly allows Logstash to instruct input plugins to terminate gracefully,
   instead of using Thread.raise on the plugins' threads. Ref: https://github.com/elastic/logstash/pull/3895
 - Dependency on logstash-core update to 2.0

