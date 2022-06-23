# kstreams-error-demo

Sample project showcasing the issue with having multiple state stores and multiple Kafka Streams applications on Kafka 3.1.x.

The test `KStreamsErrorDemoApplicationTests` passes fine with Spring Boot 2.6.8 / Kafka 3.0, but does not pass with Spring Boot 2.7.0 / Kafka 3.1.
The stores cannot be queried (unless stars are properly aligned) - `InteractiveQueryService.get()` throws an unexpected exception:

```
org.apache.kafka.streams.errors.InvalidStateStoreException: The state store, store2, may have migrated to another instance.
```


The issue appears to be caused by `InteractiveQueryService` calling `org.apache.kafka.streams.KafkaStreams#store(StoreQueryParameters<T>...)` which slightly
changed behavior in Kafka 3.1 - instead of returning just the stores actually used in the topology, it checks all stores in the topology. This, in 
combination with Spring Cloud Stream `AbstractKafkaStreamsBinderProcessor` adding all the `StoreBuilder` beans in context to every `StreamsBuilder`,
is causing the `KafkaStreams#store(StoreQueryParameters<T>...)` method to always return a store no matter the `KafkaStreams` instance, but one that
cannot be used, unless the right `KafkaStreams` instance happens the first to be checked.
