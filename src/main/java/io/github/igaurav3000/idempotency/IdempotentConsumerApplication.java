package io.github.igaurav3000.idempotency;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class IdempotentConsumerApplication {

    public static void main(String[] args) {
        SpringApplication.run(IdempotentConsumerApplication.class, args);
    }
}
