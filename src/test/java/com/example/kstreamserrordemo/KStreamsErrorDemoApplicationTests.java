package com.example.kstreamserrordemo;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.stream.Stream;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.cloud.stream.binder.kafka.streams.KafkaStreamsRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;

import static com.example.kstreamserrordemo.KStreamsErrorDemoApplication.STORE_1_NAME;
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

    @Test
    void sendMessageAndAssertStore() {

        waitForRunningStreams();

        var testMessage = "Ahoy!";
        template.send("input-1", KEY, testMessage);
        template.send("input-2", KEY, testMessage);

        // wait until the stores have something or timeout passes
        try {
            given()
                    .ignoreExceptions()
                    .await()
                    .atMost(Duration.ofSeconds(10))
                    .until(this::checkStoresPopulated);
        } catch (ConditionTimeoutException e) {
            // ignored
        }

        // this will throw "The state store, <name>, may have migrated to another instance." on KStreams 3.1
        assertThat(checkStoresPopulated(), equalTo(true));
    }

    private boolean checkStoresPopulated() {
        return Stream.of(STORE_1_NAME, STORE_2_NAME)
                .map(name -> queryService.getQueryableStore(name, QueryableStoreTypes.keyValueStore()))
                .allMatch(store -> store.get(KEY) != null);
    }

    private void waitForRunningStreams() {
        await()
                .atMost(Duration.ofSeconds(60))
                .until(() -> registry.streamsBuilderFactoryBeans().stream().allMatch(x -> x.getKafkaStreams().state().equals(KafkaStreams.State.RUNNING)));
    }
}
