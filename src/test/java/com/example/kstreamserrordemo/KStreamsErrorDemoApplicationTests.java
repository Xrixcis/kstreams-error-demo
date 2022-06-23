package com.example.kstreamserrordemo;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.RepeatedTest;

import java.time.Duration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.cloud.stream.binder.kafka.streams.KafkaStreamsRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;

import static com.example.kstreamserrordemo.KStreamsErrorDemoApplication.STORE_2_NAME;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Awaitility.given;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

@SpringBootTest
@EmbeddedKafka(topics = { "input-1", "input-2"})
class KStreamsErrorDemoApplicationTests {

    private static final String KEY = "key";
    @Autowired
    private KafkaTemplate<String, String> template;

    @Autowired
    private InteractiveQueryService queryService;

    @Autowired
    private KafkaStreamsRegistry registry;

    @RepeatedTest(5) // sometimes it might work on KStreams 3.1 if the stars are well aligned
    void sendMessageAndAssertStore() {

        waitForRunningStreams();

        var testMessage = "Ahoy!";
        template.send("input-2", KEY, testMessage);

        // wait until the store has something or timeout passes
        try {
            given()
                    .ignoreExceptions()
                    .await()
                    .atMost(Duration.ofSeconds(10))
                    .until(() -> {
                        var store = queryService.getQueryableStore(STORE_2_NAME, QueryableStoreTypes.keyValueStore());
                        return store.get(KEY) != null;
                    });
        } catch (ConditionTimeoutException e) {
            // ignored
        }

        // this will (usually) throw "The state store, store2, may have migrated to another instance." on KStreams 3.1
        var store = queryService.getQueryableStore(STORE_2_NAME, QueryableStoreTypes.keyValueStore());
        assertThat(store.get(KEY), equalTo(testMessage.toUpperCase()));
    }

    private void waitForRunningStreams() {
        await()
                .atMost(Duration.ofSeconds(60))
                .until(() -> registry.streamsBuilderFactoryBeans().stream().allMatch(x -> x.getKafkaStreams().state().equals(KafkaStreams.State.RUNNING)));
    }
}
