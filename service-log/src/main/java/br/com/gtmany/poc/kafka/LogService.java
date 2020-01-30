package br.com.gtmany.poc.kafka;

import br.com.gtmany.poc.kafka.consumer.KafkaService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

public class LogService {

    private static Logger logger = LoggerFactory.getLogger(LogService.class);

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        Map<String, String> params = new HashMap<>();
//        params.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        LogService logService = new LogService();
        KafkaService<String> kafkaService = new KafkaService<>(LogService.class.getName(), Pattern.compile("ECOMMERCE.*"), logService::parse,
                params);
        kafkaService.run();
    }

    private void parse(ConsumerRecord<String, Message<String>> record) throws InterruptedException {

        Message<String> message = record.value();
        logger.info("-------------------------------------------");
        logger.info("LOGGING.." + record.topic());
        logger.info("VALUE: " + message.toString());
        logger.info("PARTITION: " + String.valueOf(record.partition()));
        logger.info("OFFSET: " + String.valueOf(record.offset()));
        Thread.sleep(5000);
    }
}
