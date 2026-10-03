package io.github.igaurav3000.idempotency;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class PaymentService {

    private final JdbcTemplate jdbc;

    public PaymentService(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Applies a payment exactly once, however many times it is delivered.
     *
     * <p>The marker row and the balance change share one transaction. That is the
     * whole mechanism. Split them — mark in Redis, then write to the database —
     * and a crash in the gap leaves the event flagged as handled while the money
     * never moved, which is silent data loss that surfaces weeks later.
     *
     * @return true if this call applied the payment, false if it was a duplicate
     */
    @Transactional
    public boolean apply(PaymentEvent event) {
        int inserted = jdbc.update("""
                INSERT INTO processed_events (event_id)
                VALUES (?)
                ON CONFLICT (event_id) DO NOTHING
                """, event.eventId());

        // ON CONFLICT rather than catching DuplicateKeyException: in PostgreSQL a
        // constraint violation aborts the surrounding transaction, so catching the
        // exception and carrying on leaves you committing a dead transaction.
        if (inserted == 0) {
            return false;
        }

        jdbc.update("""
                UPDATE accounts
                   SET balance_minor = balance_minor + ?
                 WHERE id = ?
                """, event.amountMinor(), event.accountId());

        return true;
    }

    public long balanceOf(String accountId) {
        Long balance = jdbc.queryForObject(
                "SELECT balance_minor FROM accounts WHERE id = ?", Long.class, accountId);
        return balance == null ? 0L : balance;
    }
}
