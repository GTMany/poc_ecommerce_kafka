package br.com.gtmany.poc.kafka.consumer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ServiceRunner<T> {
    private final ServiceProvider<T> provider;

    public ServiceRunner(ServiceFactory<T> provider) {
        this.provider = new ServiceProvider<T>(provider);
    }

    public void start(int threadCount) {
        ExecutorService pool = Executors.newFixedThreadPool(threadCount);
        for(int i=0; i<threadCount; i++){
            pool.submit(provider);
        }
    }
}
