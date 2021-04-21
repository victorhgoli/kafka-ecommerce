package br.com.consumer;

import java.util.concurrent.Executors;

public class ServiceRunner<T> {
    private final ServiceProvider<T> provider;

    public ServiceRunner(ServiceFatory<T> factory) {
        this.provider = new ServiceProvider<>(factory);
    }

    public void start(int threadCount) {
        var pool = Executors.newFixedThreadPool(threadCount);

        for (int i = 0; i <= threadCount; i++) {
            pool.submit(provider);
        }
    }
}
