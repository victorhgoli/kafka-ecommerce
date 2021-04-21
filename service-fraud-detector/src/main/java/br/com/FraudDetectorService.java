package br.com;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import br.com.consumer.ConsumerService;
import br.com.consumer.KafkaService;
import br.com.consumer.ServiceRunner;
import br.com.dispatcher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class FraudDetectorService implements ConsumerService<Order> {

    private final LocalDataBase database;

    FraudDetectorService() throws SQLException {
        this.database = new LocalDataBase("frauds_database");
        database.createIfNotExists(" create table Orders (" +
                "uuid varchar(200) primary key," +
                "is_fraud boolean" +
                ")");
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        new ServiceRunner(FraudDetectorService::new).start(1);
    }

    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();

    @Override
    public String getTopic() {
        return "ECOMMERCE_NEW_ORDER";
    }

    @Override
    public String getConsumerGroup() {
        return FraudDetectorService.class.getSimpleName();
    }

    public void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException, SQLException {
        System.out.println("Processing new Order, check for fraud");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());

        var message = record.value();
        var order = message.getPayload();

        if (wasProcessed(order)) {
            return;
        }


        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            // ignored
        }


        if (isFraud(order)) {
            System.out.println("Its a fraud: " + order);
            database.update("insert into Orders(uuid, is_fraud) values (?,true)", order.getOrderId());
            orderDispatcher.send("ECOMMERCE_ORDER_REJECTED", order.getEmail(),
                    message.getId().continueWith(FraudDetectorService.class.getSimpleName()),
                    order);
        } else {
            System.out.println("Aproved: " + order);
            database.update("insert into Orders(uuid, is_fraud) values (?,false)", order.getOrderId());
            orderDispatcher.send("ECOMMERCE_ORDER_APPROVED", order.getEmail(),
                    message.getId().continueWith(FraudDetectorService.class.getSimpleName()),
                    order);
        }
    }

    private boolean wasProcessed(Order order) throws SQLException {
        var results = database.query("select uuid from Orders where uuid = ? limit 1", order.getOrderId());
        return results.next();
    }

    private boolean isFraud(Order order) {
        return order.getAmount().compareTo(new BigDecimal("4500")) >= 0;
    }

}
