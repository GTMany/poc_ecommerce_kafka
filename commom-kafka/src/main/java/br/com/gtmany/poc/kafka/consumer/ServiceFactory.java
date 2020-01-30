package br.com.gtmany.poc.kafka.consumer;

import br.com.gtmany.poc.kafka.ConsumerService;

import java.sql.SQLException;

public interface ServiceFactory<T> {
    ConsumerService<T> create() throws Exception;
}
