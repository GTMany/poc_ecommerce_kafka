package br.com.gtmany.poc.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaDispatcher<T> implements Closeable {

    private static Logger logger = LoggerFactory.getLogger(KafkaDispatcher.class);

    private final KafkaProducer<String, T> producer;

    public KafkaDispatcher() {
        this.producer = new KafkaProducer<String, T>(properties());
    }

    public void send(String topic, String key, T value) throws ExecutionException, InterruptedException {
        ProducerRecord<String, T> record = new ProducerRecord<String, T>(topic, key, value);
        Callback callback = (data, ex) -> {
            if (ex != null) {
                logger.error(ex.getMessage());
                return;
            }

            logger.info("Mensagem enviada ao kafka: " + data.topic() + ":::partition " + data.partition() + "/ offset " + data.offset());
        };

        producer.send(record, callback).get();
    }

    private Properties properties() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer.class.getName());
        return properties;
    }

    @Override
    public void close() {
        producer.close();
    }
}
