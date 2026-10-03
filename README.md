# Idempotent Kafka Consumer

A small, runnable Spring Boot service demonstrating the one thing every at-least-once consumer needs and most tutorials skip: **processing that stays correct when the same message arrives twice.**

Companion code for [Kafka delivery semantics and idempotency](https://github.com/iGaurav3000/java-backend-interview-handbook/blob/main/distributed-systems/kafka-delivery-semantics.md) in the Java Backend Interview Handbook.

---

## The problem

Your consumer reads a payment event and updates a balance. The pod is killed between the database write and the offset commit. Kafka redelivers. Without protection, the customer is charged twice.

"Kafka guarantees at-least-once" is the easy half of that answer. The question is what *you* did to make redelivery safe.

## The mechanism

Two lines, one transaction:

```java
@Transactional
public boolean apply(PaymentEvent event) {
    int inserted = jdbc.update("""
            INSERT INTO processed_events (event_id)
            VALUES (?)
            ON CONFLICT (event_id) DO NOTHING
            """, event.eventId());

    if (inserted == 0) {
        return false;           // already handled
    }

    jdbc.update("UPDATE accounts SET balance_minor = balance_minor + ? WHERE id = ?",
                event.amountMinor(), event.accountId());
    return true;
}
```

The primary key on `processed_events` does the real work. Two consumers racing on the same event: one insert lands, the other conflicts and backs out. Both commit a consistent result.

What makes it correct is that the marker and the effect **share one transaction**. Mark it in Redis first and write to the database second, and a crash in the gap leaves the event flagged as done while the money never moved — silent data loss that takes a week to find.

## Three details worth the detour

**`ON CONFLICT` rather than catching `DuplicateKeyException`.** In PostgreSQL a constraint violation aborts the surrounding transaction. Catch the exception and carry on and you are committing a dead transaction. The conflict clause keeps the transaction alive and gives you a row count to branch on.

**The dedup key is the producer-assigned `eventId`, never the offset.** An offset identifies a position in a partition, not a business fact. Replay the topic, move clusters, or republish the event and every offset changes while the event stays the same.

**`enable-auto-commit: false` is the load-bearing setting.** Auto-commit fires inside `poll()` once the interval elapses, committing offsets for records it has already handed you — finished or not. You get message loss sometimes and duplicates other times, and which one depends on load. See `application.yml`, where every non-obvious setting carries a comment explaining what breaks without it.

## Running it

```bash
docker compose up -d
./mvnw spring-boot:run
```

Publish the same event id three times and watch the balance move once.

## The test

```bash
./mvnw test
```

`RedeliveryIdempotencyTest` spins up real Kafka and PostgreSQL via Testcontainers — no mocks, since mocking the broker would mock away the thing being tested. It publishes one event id three times, asserts the balance moved once, then holds to confirm the duplicates were genuinely consumed and rejected rather than merely slow to arrive. A second test confirms three distinct events still apply three times, so the dedup isn't just swallowing everything.

Requires Docker. Java 21.

## Where this stops

Kafka transactions give real exactly-once for consume → process → produce, entirely within Kafka. The moment you write to PostgreSQL, call a payment API or send an email, you are outside that boundary, and no configuration extends across it. Kafka cannot roll back a `POST`.

That is why idempotency is the mechanism and not the fallback.

## License

MIT.
