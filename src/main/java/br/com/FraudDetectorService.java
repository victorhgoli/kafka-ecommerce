package br.com;

import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class FraudDetectorService {

    public static void main(String[] args) {
        var fraudDetectorService = new FraudDetectorService();
        try (var service = new KafkaService(FraudDetectorService.class.getSimpleName(), 
                "ECOMMERCE_NEW_ORDER",
                fraudDetectorService::parse,
                Order.class,
                Map.of())    ) {

            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Order> record) {
        System.out.println("Processing new Order, check for fraud");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            // ignored
        }

        System.out.println("Order Processed");
    }

}
