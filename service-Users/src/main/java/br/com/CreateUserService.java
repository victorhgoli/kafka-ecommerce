package br.com;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class CreateUserService {

    private final Connection connection;

    CreateUserService() throws SQLException {
        String url = "jdbc:sqlite:target/user_database.db";
        this.connection = DriverManager.getConnection(url);
        try {
            connection.createStatement().execute(" create table Users (" +
                    "uuid varchar(200) primary key," +
                    "email varchar(200)" +
                    ")");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws SQLException {
        var createuserService = new CreateUserService();
        try (var service = new KafkaService(CreateUserService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER",
                createuserService::parse,
                Order.class,
                Map.of())) {

            service.run();
        }
    }


    private void parse(ConsumerRecord<String, Order> record) throws SQLException {
        System.out.println("Processing new Order, check for new user:" + record.value());

        var order = record.value();

        if (isNewUser(order.getEmail())) {
            insertNewUser(order.getEmail());
        } else {
            System.out.println("Usuario já existe");
        }
    }

    private void insertNewUser(String email) throws SQLException {
        var insert = connection.prepareStatement("insert into Users (uuid, email) values (?m?)");

        insert.setString(1, UUID.randomUUID().toString());
        insert.setString(2, email);

        insert.execute();

        System.out.println("User adicionado");
    }

    private boolean isNewUser(String email) throws SQLException {
        var select = connection.prepareStatement(" select uuid from Users where email = ? limit 1");

        select.setString(1, email);

        var results = select.executeQuery();

        return !results.next();

    }

}
