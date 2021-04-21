package br.com.consumer;

import java.sql.SQLException;

public interface ServiceFatory<T> {
    ConsumerService<T> create() throws Exception;
}
