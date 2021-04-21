package br.com;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import br.com.consumer.ConsumerService;
import br.com.consumer.KafkaService;
import br.com.consumer.ServiceRunner;
import br.com.dispatcher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class EmailNewOrderService implements ConsumerService<Order> {


    public static void main(String[] args) {
        new ServiceRunner(EmailNewOrderService::new).start(1);
    }

    private final KafkaDispatcher<String> emailDispatcher = new KafkaDispatcher<>();

    @Override
    public String getTopic() {
        return "ECOMMERCE_NEW_ORDER";
    }

    @Override
    public String getConsumerGroup() {
        return EmailNewOrderService.class.getSimpleName();
    }

    public void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException {
        System.out.println("Processing new Order, preparing email");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());

        var message = record.value();
        var order = record.value().getPayload();

        var emailCode = "isso será um email de envio de ordem!";
        emailDispatcher.send("ECOMMERCE_SEND_EMAIL", order.getEmail(),
                message.getId().continueWith(EmailNewOrderService.class.getSimpleName()),
                emailCode);
    }
}

