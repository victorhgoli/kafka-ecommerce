package br.com;

import br.com.consumer.ConsumerService;
import br.com.consumer.ServiceRunner;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class EmailService implements ConsumerService<String> {

    public static void main(String[] args) {
        new ServiceRunner(EmailService::new).start(5);
    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_SEND_EMAIL";
    }

    @Override
    public String getConsumerGroup() {
        return EmailService.class.getSimpleName();
    }


    public void parse(ConsumerRecord<String, Message<String>> record) {
        System.out.println("Sending email");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            // ignored
        }
        System.out.println("Email send!");
    }
}
