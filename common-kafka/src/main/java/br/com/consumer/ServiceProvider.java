package br.com.consumer;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

public class ServiceProvider<T> implements Callable<Void> {
    private ServiceFatory<T> factory;

    public ServiceProvider(ServiceFatory<T> factory) {
        this.factory = factory;
    }

    public Void call() throws Exception {

        var myService =factory.create();
        try (var service = new KafkaService(myService.getConsumerGroup(), myService.getTopic(),
                myService::parse,
                Map.of());) {
            service.run();
        }

        return null;
    }
}
