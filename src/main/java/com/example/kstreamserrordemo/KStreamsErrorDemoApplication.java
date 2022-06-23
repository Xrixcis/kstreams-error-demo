package com.example.kstreamserrordemo;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.function.Consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class KstreamsErrorDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(KstreamsErrorDemoApplication.class, args);
    }


    @Bean
    public StoreBuilder<KeyValueStore<String, String>> store1() {
        return Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("store1"), Serdes.String(), Serdes.String());
    }

    @Bean
    public Consumer<KStream<String, String>> app1() {
        return s -> s.transform().foreach();
    }

    @Bean
    public StoreBuilder<KeyValueStore<String, String>> store2() {
        return Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("store2"), Serdes.String(), Serdes.String());
    }

    static class UppercaseTransformer implements ValueTransformerWithKey<String, String, String> {

        private static final String storeName;
        private KeyValueStore<String, String> store;

        public UppercaseTransformer(KeyValueStore<String, String> store) {
            this.store = store;
        }

        @Override
        public void init(ProcessorContext context) {
            this.store = context.getStateStore()
        }

        @Override
        public String transform(String readOnlyKey, String value) {
            return null;
        }

        @Override
        public void close() {
        }
    }
}
