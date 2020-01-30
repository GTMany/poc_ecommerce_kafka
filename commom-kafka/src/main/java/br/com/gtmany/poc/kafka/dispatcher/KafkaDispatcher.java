package br.com.gtmany.poc.kafka.dispatcher;

import br.com.gtmany.poc.kafka.CorrelationId;
import br.com.gtmany.poc.kafka.Message;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaDispatcher<T> implements Closeable {

    private static Logger logger = LoggerFactory.getLogger(KafkaDispatcher.class);

    private final KafkaProducer<String, Message<T>> producer;

    public KafkaDispatcher() {
        this.producer = new KafkaProducer<String, Message<T>>(properties());
    }

    public void send(String topic, String key, CorrelationId id, T payload) throws ExecutionException, InterruptedException {
        Future<RecordMetadata> future = sendAsync(topic, key, id, payload);
        future.get();
    }

    public Future<RecordMetadata> sendAsync(String topic, String key, CorrelationId id, T payload) {
        Message value = new Message<T>(id.continueWith("_" + topic), payload);
        ProducerRecord<String, T> record = new ProducerRecord<String, T>(topic, key, (T) value);
        Callback callback = (data, ex) -> {
            if (ex != null) {
                logger.error(ex.getMessage());
                return;
            }

            logger.info("Mensagem enviada ao kafka: " + data.topic() + ":::partition " + data.partition() + "/ offset " + data.offset());
        };

        return producer.send((ProducerRecord<String, Message<T>>) record, callback);
    }

    private Properties properties() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9091");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        return properties;
    }

    @Override
    public void close() {
        producer.close();
    }
}
