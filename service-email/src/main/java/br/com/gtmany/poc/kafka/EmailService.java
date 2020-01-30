package br.com.gtmany.poc.kafka;

import br.com.gtmany.poc.kafka.consumer.ServiceRunner;
import br.com.gtmany.poc.kafka.types.TOPIC_ENUM;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmailService implements ConsumerService<String> {

    private static Logger logger = LoggerFactory.getLogger(EmailService.class);

    public static void main(String[] args) {
        new ServiceRunner(EmailService::new).start(5);
    }

    public String getConsumerGroup() {
        return EmailService.class.getName();
    }

    public String getTopic() {
        return TOPIC_ENUM.ECOMMERCE_SEND_EMAIL.name();
    }

    public void parse(ConsumerRecord<String, Message<String>> record) {
        logger.info("-------------------------------------------");
        logger.info("Sending email..");
        logger.info("KEY: " + record.key().toString());
        logger.info("VALUE: " + record.value().toString());
        logger.info("PARTITION: " + String.valueOf(record.partition()));
        logger.info("OFFSET: " + String.valueOf(record.offset()));
        logger.info("Order processed.");
    }
}
