package br.com;

import br.com.consumer.ConsumerService;
import br.com.consumer.KafkaService;
import br.com.consumer.ServiceRunner;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class CreateUserService implements ConsumerService<Order> {

    private final LocalDataBase database;

    CreateUserService() throws SQLException {
        this.database = new LocalDataBase("user_database");
        database.createIfNotExists(" create table Users (" +
                "uuid varchar(200) primary key," +
                "email varchar(200)" +
                ")");
    }

    public static void main(String[] args) throws SQLException, ExecutionException, InterruptedException {
        new ServiceRunner<>(CreateUserService::new).start(1);
    }


    @Override
    public String getTopic() {
        return "ECOMMERCE_NEW_ORDER";
    }

    @Override
    public String getConsumerGroup() {
        return CreateUserService.class.getSimpleName();
    }

    public void parse(ConsumerRecord<String, Message<Order>> record) throws SQLException {
        System.out.println("Processing new Order, check for new user:" + record.value());

        var order = record.value().getPayload();

        if (isNewUser(order.getEmail())) {
            insertNewUser(order.getEmail());
        } else {
            System.out.println("Usuario já existe");
        }
    }

    private void insertNewUser(String email) throws SQLException {
        database.update("insert into Users (uuid, email) values (?, ?)", UUID.randomUUID().toString(), email);


        System.out.println("User adicionado");
    }

    private boolean isNewUser(String email) throws SQLException {
        var results = database.query(" select uuid from Users where email = ? limit 1", email);

        return !results.next();

    }

}
