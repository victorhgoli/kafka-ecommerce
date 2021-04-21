package br.com;

import br.com.consumer.KafkaService;
import br.com.dispatcher.KafkaDispatcher;
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

    public static void main(String[] args) throws SQLException, ExecutionException, InterruptedException {
        var batchSendMessageService = new BatchSendMessageService();
        try (var service = new KafkaService(BatchSendMessageService.class.getSimpleName(),
                "ECOMMERCE_SEND_MESSAGE_TO_ALL_USERS",
                batchSendMessageService::parse,
                Map.of())) {

            service.run();
        }
    }

    private final KafkaDispatcher<User> userDispatcher = new KafkaDispatcher<>();

    private void parse(ConsumerRecord<String, Message<String>> record) throws SQLException, ExecutionException, InterruptedException {
        System.out.println("Processing new batch");
        var message = record.value();
        System.out.println("Topic: " + message.getPayload());

        var users = getAllUsers();

        System.out.println("Sending email to  " + users.size() + " users;");

        for (User user : users) {
            userDispatcher.sendAsync(message.getPayload(), user.getUuid(),
                    message.getId().continueWith(BatchSendMessageService.class.getSimpleName()),
                    user);

            System.out.println("Enviado para " + user);
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
