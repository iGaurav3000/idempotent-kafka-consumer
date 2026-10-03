package io.github.igaurav3000.idempotency;

import java.time.Duration;
import java.util.UUID;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The claim under test: the same event delivered three times moves the balance once.
 *
 * <p>Redelivery is simulated by publishing the identical event id repeatedly, which
 * is what a consumer actually sees after a rebalance revokes partitions mid-batch
 * or a pod dies between processing and commit.
 */
@SpringBootTest
@Testcontainers
class RedeliveryIdempotencyTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"))
                    .withDatabaseName("ledger")
                    .withUsername("ledger")
                    .withPassword("ledger");

    @Container
    static final org.testcontainers.kafka.KafkaContainer KAFKA =
            new org.testcontainers.kafka.KafkaContainer(DockerImageName.parse("apache/kafka:3.8.1"));

    @DynamicPropertySource
    static void wireContainers(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    @Autowired
    KafkaTemplate<String, PaymentEvent> kafka;

    @Autowired
    PaymentService payments;

    @Test
    void sameEventDeliveredThreeTimesAppliesOnce() {
        String eventId = UUID.randomUUID().toString();
        String account = "acct-1";
        long before = payments.balanceOf(account);

        PaymentEvent event = new PaymentEvent(eventId, account, 2_500L);
        for (int i = 0; i < 3; i++) {
            kafka.send("payments", account, event);
        }

        Awaitility.await()
                .atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(payments.balanceOf(account)).isEqualTo(before + 2_500L));

        // Hold long enough for the two duplicates to be consumed and rejected,
        // so a passing assertion above cannot simply be an early read.
        Awaitility.await()
                .during(Duration.ofSeconds(3))
                .atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(payments.balanceOf(account)).isEqualTo(before + 2_500L));
    }

    @Test
    void distinctEventsEachApply() {
        String account = "acct-1";
        long before = payments.balanceOf(account);

        for (int i = 0; i < 3; i++) {
            kafka.send("payments", account,
                    new PaymentEvent(UUID.randomUUID().toString(), account, 1_000L));
        }

        Awaitility.await()
                .atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(payments.balanceOf(account)).isEqualTo(before + 3_000L));
    }
}
