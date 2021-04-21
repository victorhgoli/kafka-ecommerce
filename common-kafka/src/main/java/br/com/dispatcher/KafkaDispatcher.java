package br.com.dispatcher;

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import br.com.CorrelationId;
import br.com.Message;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaDispatcher<T> implements Closeable {

    private final KafkaProducer<String, Message<T>> producer;

    public KafkaDispatcher() {
        this.producer = new KafkaProducer<String, Message<T>>(properties());
    }

    public void send(String topic, String key, CorrelationId id, T payload) throws InterruptedException, ExecutionException {
        Future<RecordMetadata> future = sendAsync(topic, key, id, payload);
        future.get();
    }

    public Future<RecordMetadata> sendAsync(String topic, String key, CorrelationId id, T payload) {
        var value = new Message<T>(id.continueWith("_" + topic), payload);
        Callback callback = (data, ex) -> {
            if (ex != null) {
                ex.printStackTrace();
                return;
            }

            System.out.println("SUCESSO!!! TOPIC#: " + data.topic() + ", PARTITION#: " + data.partition()
                    + ", OFFSET#: " + data.offset() + ", TIMESTAMP#: " + data.timestamp());
        };

        var record = new ProducerRecord<>(topic, key, value);
        var future = producer.send(record, callback);
        return future;
    }

    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");

        return properties;
    }

    @Override
    public void close() {
        producer.close();
    }

}
