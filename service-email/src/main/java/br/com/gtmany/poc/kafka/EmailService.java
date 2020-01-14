package br.com.gtmany.poc.kafka;

import br.com.gtmany.poc.kafka.types.TOPIC_ENUM;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;
import java.util.UUID;

public class EmailService {

    private static Logger logger = LoggerFactory.getLogger(EmailService.class);

    public static void main(String[] args) throws InterruptedException {
        EmailService emailService = new EmailService();
        try(KafkaService kafkaService = new KafkaService(EmailService.class.getName(), TOPIC_ENUM.ECOMMERCE_SEND_EMAIL.name(),
                emailService::parse, String.class, new HashMap<>())){
            kafkaService.run();
        }
    }

    private void parse(ConsumerRecord<String, String> record) throws InterruptedException {
        logger.info("-------------------------------------------");
        logger.info("Sending email..");
        logger.info("KEY: " + record.key().toString());
        logger.info("VALUE: " + record.value().toString());
        logger.info("PARTITION: " + String.valueOf(record.partition()));
        logger.info("OFFSET: " + String.valueOf(record.offset()));
        logger.info("Order processed.");
        Thread.sleep(5000);
    }
}
