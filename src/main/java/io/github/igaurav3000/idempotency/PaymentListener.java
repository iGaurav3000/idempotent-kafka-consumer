package io.github.igaurav3000.idempotency;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class PaymentListener {

    private static final Logger log = LoggerFactory.getLogger(PaymentListener.class);

    private final PaymentService payments;

    public PaymentListener(PaymentService payments) {
        this.payments = payments;
    }

    /**
     * At-least-once by construction: the container acknowledges the record only
     * after this method returns normally (ack-mode: record, auto-commit off).
     *
     * <p>Throw from here and the offset is not committed, so the record comes back.
     * Return and the offset moves on. Everything about the delivery guarantee is
     * decided by that ordering — not by a configuration flag named after it.
     */
    @KafkaListener(topics = "${app.topic:payments}")
    public void onPayment(PaymentEvent event) {
        boolean applied = payments.apply(event);
        if (applied) {
            log.info("applied {} to {}", event.amountMinor(), event.accountId());
        } else {
            log.info("duplicate {} ignored", event.eventId());
        }
    }
}
