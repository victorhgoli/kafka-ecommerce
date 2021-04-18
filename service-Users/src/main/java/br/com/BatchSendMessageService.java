package br.com;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class BatchSendMessageService {

    private final Connection connection;

    BatchSendMessageService() throws SQLException {
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
        var batchSendMessageService = new BatchSendMessageService();
        try (var service = new KafkaService(BatchSendMessageService.class.getSimpleName(),
                "SEND_MESSAGE_TO_ALL_USERS",
                batchSendMessageService::parse,
                String.class,
                Map.of())) {

            service.run();
        }
    }

    private final KafkaDispatcher<User> userDispatcher = new KafkaDispatcher<>();

    private void parse(ConsumerRecord<String, String> record) throws SQLException, ExecutionException, InterruptedException {
        System.out.println("Processing new batch");
        System.out.println("Topic: " + record.value());

        var users = getAllUsers();

        System.out.println("Sending email to  " + users.size() + " users;");

        for (User user : users) {
            userDispatcher.send(record.value(), user.getUuid(), user);
        }
    }


    private List<User> getAllUsers() throws SQLException {
        var select = connection.prepareStatement(" select uuid from Users");

        var results = select.executeQuery();

        List<User> users = new ArrayList<>();
        while (results.next()) {
            users.add(new User(results.getString(1)));
        }

        return users;
    }
}
