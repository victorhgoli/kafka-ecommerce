package br.com;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class NewOrderMain {

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        var producer = new KafkaProducer<String, String>(properties());

        for (var i = 0; i < 15; i++) {

            var key = UUID.randomUUID().toString();
            var value = key + ":121232,32132131,5455";

            Callback callback = (data, ex) -> {
                if (ex != null) {
                    ex.printStackTrace();
                    return;
                }

                System.out.println("SUCESSO!!! TOPIC#: " + data.topic() + ", PARTITION#: " + data.partition()
                        + ", OFFSET#: " + data.offset() + ", TIMESTAMP#: " + data.timestamp());
            };

            var record = new ProducerRecord<>("ECOMMERCE_NEW_ORDER", key, value);
            producer.send(record, callback).get();

            var email = "isso será um email de envio de ordem!";
            var emailRecord = new ProducerRecord<>("ECOMMERCE_SEND_EMAIL", key, email);
            producer.send(emailRecord, callback);
        }
    }

    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return properties;
    }

}
