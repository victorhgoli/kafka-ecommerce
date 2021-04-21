package br.com.consumer;

import br.com.Message;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

public interface ConsumerService<T> {

    String getTopic();
    String getConsumerGroup();
    void parse(ConsumerRecord<String, Message<T>> record) throws Exception;
}
