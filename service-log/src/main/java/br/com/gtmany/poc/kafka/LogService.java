package br.com.gtmany.poc.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class LogService {

    private static Logger logger = LoggerFactory.getLogger(LogService.class);

    public static void main(String[] args) throws InterruptedException {
        Map<String, String> params = new HashMap<>();
        params.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        LogService logService = new LogService();
        KafkaService kafkaService = new KafkaService(LogService.class.getName(), Pattern.compile("ECOMMERCE.*"), logService::parse,
                String.class, params);
        kafkaService.run();
    }

    private void parse(ConsumerRecord<String, String> record) throws InterruptedException {
        logger.info("-------------------------------------------");
        logger.info("LOGGING.." + record.topic());
        logger.info("VALUE: " + record.value().toString());
        logger.info("PARTITION: " + String.valueOf(record.partition()));
        logger.info("OFFSET: " + String.valueOf(record.offset()));
        Thread.sleep(5000);
    }
}
