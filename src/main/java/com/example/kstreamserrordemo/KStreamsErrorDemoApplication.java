package com.example.kstreamserrordemo;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class KStreamsErrorDemoApplication {

    private static final Logger log = LoggerFactory.getLogger(KStreamsErrorDemoApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(KStreamsErrorDemoApplication.class, args);
    }

    public static final String STORE_1_NAME = "store1";
    public static final String STORE_2_NAME = "store2";

    @Bean
    public StoreBuilder<KeyValueStore<String, String>> store1() {
        return Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(STORE_1_NAME), Serdes.String(), Serdes.String());
    }

    @Bean
    public Consumer<KStream<String, String>> app1() {
        return s -> s
                .transformValues(() -> new UppercaseTransformer(STORE_1_NAME), STORE_1_NAME)
                .foreach((k, v) -> log.info("Stored {} -> {} into {}", k, v, STORE_1_NAME));
    }

    @Bean
    public StoreBuilder<KeyValueStore<String, String>> store2() {
        return Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(STORE_2_NAME), Serdes.String(), Serdes.String());
    }

    @Bean
    public Consumer<KStream<String, String>> app2() {
        return s -> s
                .transformValues(() -> new UppercaseTransformer(STORE_2_NAME), STORE_2_NAME)
                .foreach((k, v) -> log.info("Stored {} -> {} into {}", k, v, STORE_2_NAME));
    }

    static class UppercaseTransformer implements ValueTransformerWithKey<String, String, String> {

        private final String storeName;
        private KeyValueStore<String, String> store;

        UppercaseTransformer(String storeName) {
            this.storeName = storeName;
        }

        @Override
        public void init(ProcessorContext context) {
            this.store = context.getStateStore(storeName);
        }

        @Override
        public String transform(String key, String value) {
            var result = value.toUpperCase();
            store.put(key, result);
            return result;
        }

        @Override
        public void close() {
        }
    }
}
