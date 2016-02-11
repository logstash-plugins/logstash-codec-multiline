## 2.0.9
 - Fix for issue #30, eviction_timeout nil when cleaner thread is doing map cleanup.

## 2.0.8
 - Fix for issue #28, Memory leak when using cancel + execute on ScheduledTask.

## 2.0.7
 - Fix for issue #23, Thread deadlock on auto-flush when using multiple inputs with this codec. Use cancel + execute on ScheduledTask instead of reset.

## 2.0.6
 - Isolate spec helper classes in their own namespace.

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

