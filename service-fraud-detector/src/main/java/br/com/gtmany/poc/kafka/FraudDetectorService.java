package br.com.gtmany.poc.kafka;

import br.com.gtmany.poc.kafka.consumer.KafkaService;
import br.com.gtmany.poc.kafka.dispatcher.KafkaDispatcher;
import br.com.gtmany.poc.kafka.model.Order;
import br.com.gtmany.poc.kafka.types.TOPIC_ENUM;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;

public class FraudDetectorService {

    private static Logger logger = LoggerFactory.getLogger(FraudDetectorService.class);

    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        FraudDetectorService fraudDetectorService = new FraudDetectorService();
        try(KafkaService service = new KafkaService<>(FraudDetectorService.class.getName(), TOPIC_ENUM.ECOMMERCE_NEW_ORDER.name(),
                fraudDetectorService::parse, new HashMap<>())) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Message<Order>> record) throws InterruptedException, ExecutionException {
//        Thread.sleep(5000);
        logger.info("-------------------------------------------");
        logger.info("Processing new order, checking for fraud");
        logger.info("KEY: " + record.key().toString());
        logger.info("VALUE: " + record.value().toString());
        logger.info("PARTITION: " + String.valueOf(record.partition()));
        logger.info("OFFSET: " + String.valueOf(record.offset()));
        logger.info("Order processed.");

        Message<Order> message = record.value();
        Order order = message.getPayload();
        if(isFraud(order)){
            logger.info("Order is a fraud!");
            orderDispatcher.send("ECOMMERCE_ORDER_REJECTED", order.getUserId(),
                    message.getId().continueWith(FraudDetectorService.class.getSimpleName()),
                    order);
        } else {
            logger.info("Order Approved: " + order);
            orderDispatcher.send("ECOMMERCE_ORDER_APPROVED", order.getUserId(),
                    message.getId().continueWith(FraudDetectorService.class.getSimpleName()),
                    order);
        }
    }

    private boolean isFraud(Order order) {
        return order.getAmount().compareTo(new BigDecimal("4500")) >= 0;
    }
}
