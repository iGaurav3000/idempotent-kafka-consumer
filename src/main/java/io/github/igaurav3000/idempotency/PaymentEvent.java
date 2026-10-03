package io.github.igaurav3000.idempotency;

/**
 * The deduplication key is {@code eventId} — assigned by the producer and stable
 * across redeliveries.
 *
 * <p>Deliberately NOT the Kafka offset: an offset identifies a position in a
 * partition, not a business fact. Replay the topic, migrate to a new cluster, or
 * republish the event and every offset changes while the event stays the same.
 */
public record PaymentEvent(String eventId, String accountId, long amountMinor) {
}
