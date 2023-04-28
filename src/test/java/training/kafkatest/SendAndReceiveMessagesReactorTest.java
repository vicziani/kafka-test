package training.kafkatest;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.stream.IntStream;

@SpringBootTest
@Slf4j
@Testcontainers
class SendAndReceiveMessagesReactorTest {

    static final String TOPIC = "test-topic";

    static final String GROUP_ID = "test-group";

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.0.9")).withKraft();

    @Autowired
    KafkaTemplate<Integer, String> kafkaTemplate;

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @Test
    void testSendAndReceive() {
        // Send messages
        IntStream.rangeClosed(1, 3)
                .forEach(i -> kafkaTemplate.send(TOPIC, i, "hello kafka %d".formatted(i)));

        // Receive messages
        var properties = new HashMap<String, Object>();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
//        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        var options = ReceiverOptions.<Integer, String>create(properties)
                .subscription(Collections.singleton(TOPIC));

        var kafkaFlux = KafkaReceiver.create(options).receive();

        StepVerifier.create(kafkaFlux)
                .expectNextMatches(r -> r.value().equals("hello kafka 1"))
                .expectNextMatches(r -> r.value().equals("hello kafka 2"))
                .expectNextMatches(r -> r.value().equals("hello kafka 3"))
                .thenCancel()
                .verify(Duration.of(10, ChronoUnit.SECONDS));
    }
}
