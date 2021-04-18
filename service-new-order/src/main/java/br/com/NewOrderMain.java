package br.com;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        try (var orderDispatcher = new KafkaDispatcher<Order>();) {
            try (var emailDispatcher = new KafkaDispatcher<String>();) {
                for (var i = 0; i < 10; i++) {

                   // var userId = UUID.randomUUID().toString();
                    var orderId = UUID.randomUUID().toString();
                    var amount = new BigDecimal(Math.random() * 5000 + 1);
                    var email = Math.random() + "@email.com";

                    var order = new Order(orderId, amount,email);

                    orderDispatcher.send("ECOMMERCE_NEW_ORDER", email,
                            new CorrelationId(NewOrderMain.class.getSimpleName()),
                            order);

                    var emailCode = "isso será um email de envio de ordem!";
                    emailDispatcher.send("ECOMMERCE_SEND_EMAIL", email,
                            new CorrelationId(NewOrderMain.class.getSimpleName()),
                            emailCode);
                }
            }
        }
    }
}
