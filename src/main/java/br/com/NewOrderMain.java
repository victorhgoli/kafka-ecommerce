package br.com;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        try (var dispatcher = new KafkaDispatcher();) {
            for (var i = 0; i < 15; i++) {

                var key = UUID.randomUUID().toString();
                var value = key + ":121232,32132131,5455";

                dispatcher.send("ECOMMERCE_NEW_ORDER", key, value);

                var email = "isso será um email de envio de ordem!";
                dispatcher.send("ECOMMERCE_SEND_EMAIL", key, email);
            }
        }
    }
}
