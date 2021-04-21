package br.com;

import br.com.br.com.api.OrdersDataBase;
import br.com.dispatcher.KafkaDispatcher;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

@RestController
@RequestMapping("new")
public class NewOrderController {

    private KafkaDispatcher orderDispatcher = new KafkaDispatcher<Order>();

    @GetMapping
    public void newOrder(@RequestParam String email, @RequestParam BigDecimal amount) throws ExecutionException, InterruptedException, SQLException {

        // var userId = UUID.randomUUID().toString();
        var orderId = UUID.randomUUID().toString();
        //  var amount = new BigDecimal(Math.random() * 5000 + 1);
        //var email = Math.random() + "@email.com";

        var order = new Order(orderId, amount, email);

        try (var database = new OrdersDataBase();) {
            if (database.saveNew(order)) {
                System.out.println("New order recivied" + order);
                System.out.println(order);
                orderDispatcher.send("ECOMMERCE_NEW_ORDER", email,
                        new CorrelationId(NewOrderController.class.getSimpleName()),
                        order);
            } else {
                System.out.println("Old order recivied" + order);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @PreDestroy
    public void destroy() {
        System.out.println(
                "Callback triggered - @PreDestroy.");
    }
}
