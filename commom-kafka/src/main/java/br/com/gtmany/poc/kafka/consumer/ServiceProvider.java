package br.com.gtmany.poc.kafka.consumer;

import br.com.gtmany.poc.kafka.ConsumerService;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

public class ServiceProvider<T> implements Callable<Void> {

    private final ServiceFactory<T> factory;

    public ServiceProvider(ServiceFactory<T> factory) {
        this.factory = factory;
    }

    public Void call() throws Exception {
        ConsumerService<T> myService = factory.create();
        Map<String, String> params = new HashMap<>();
        try(KafkaService service = new KafkaService(myService.getConsumerGroup(),
            myService.getTopic(),
            myService::parse,
            params)){
            service.run();
            return null;
        }

    }
}
