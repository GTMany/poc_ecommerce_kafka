package br.com.gtmany.poc.kafka.consumer;

import br.com.gtmany.poc.kafka.Message;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

public interface ConsumerFunction<T> {
    void consume(ConsumerRecord<String, Message<T>> records) throws Exception;

}
