package br.com.gtmany.poc.kafka;

import br.com.gtmany.poc.kafka.model.Email;
import br.com.gtmany.poc.kafka.model.Order;
import br.com.gtmany.poc.kafka.types.TOPIC_ENUM;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try(KafkaDispatcher  orderDispatcher = new KafkaDispatcher<Order>()){
            try(KafkaDispatcher emailDispatcher = new KafkaDispatcher<Email>()) {
                String email = Math.random() + "@email.com";
                for (int i = 0; i < 10; i++) {
                    String orderId = UUID.randomUUID().toString();
                    BigDecimal amount = new BigDecimal(Math.random() * 5000 + 1);

                    Order order = new Order(email, orderId, amount, email);
                    orderDispatcher.send(TOPIC_ENUM.ECOMMERCE_NEW_ORDER.name(), email, order);

                    String emailContent = "Thanks! We are processing your order!";
                    Email emailCode = new Email("New Order", emailContent);
                    emailDispatcher.send(TOPIC_ENUM.ECOMMERCE_SEND_EMAIL.name(), email, emailContent);
                }
            }
        }
    }

}
