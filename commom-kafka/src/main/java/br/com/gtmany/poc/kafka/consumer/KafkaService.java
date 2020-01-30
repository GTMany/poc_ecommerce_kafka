package br.com.gtmany.poc.kafka.consumer;

import br.com.gtmany.poc.kafka.Message;
import br.com.gtmany.poc.kafka.dispatcher.GsonSerializer;
import br.com.gtmany.poc.kafka.dispatcher.KafkaDispatcher;
import br.com.gtmany.poc.kafka.types.TOPIC_ENUM;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

public class KafkaService<T> implements Closeable {

    private static Logger logger = LoggerFactory.getLogger(KafkaService.class);
    private final KafkaConsumer<String, Message<T>> consumer;
    private final ConsumerFunction parse;

    public KafkaService(String groupId, String topic, ConsumerFunction<T> parse, Map<String, String> properties) throws InterruptedException {
        this(parse, groupId, properties);
        consumer.subscribe(Collections.singleton(topic));
    }

    public KafkaService(String groupId, Pattern topic, ConsumerFunction<T> parse, Map<String, String> properties) {
        this(parse, groupId, properties);
        consumer.subscribe(topic);
    }

    private KafkaService(ConsumerFunction<T> parse, String groupId, Map<String, String> properties) {
        this.parse = parse;
        this.consumer = (KafkaConsumer<String, Message<T>>) new KafkaConsumer<String, T>(getProperties(groupId, properties));
    }

    public void run() throws InterruptedException, ExecutionException {
        try(KafkaDispatcher deadLetterDispatcher = new KafkaDispatcher<>()){
            while(true){
                ConsumerRecords<String, Message<T>> records = consumer.poll(Duration.ofMillis(1000));
                if(records.isEmpty()){
                    logger.warn("Nenhum registro encontrado.");
                    continue;
                }

                for (ConsumerRecord record: records) {
                    try {
                        this.parse.consume(record);
                    } catch (Exception e) {
                        Message<Object> message = (Message<Object>) record.value();
                        deadLetterDispatcher.send(TOPIC_ENUM.ECOMMERCE_DEAD_LETTER.name(), message.getId().toString(),
                                message.getId().continueWith("DeadLetter"),
                                new GsonSerializer().serialize("", message));
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    private Properties getProperties(String groupId, Map<String, String> overrideProperties) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9091");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"); // ATTENTION
        properties.putAll(overrideProperties);
        return properties;
    }

    @Override
    public void close() {
        consumer.close();
    }
}
