package br.com.br.com.api.controller;

import br.com.CorrelationId;
import br.com.KafkaDispatcher;
import br.com.br.com.api.model.Order;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.PreDestroy;
import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

@RestController
@RequestMapping("new")
public class NewOrderController {

    private KafkaDispatcher orderDispatcher = new KafkaDispatcher<Order>();
    private KafkaDispatcher emailDispatcher = new KafkaDispatcher<String>();

    @GetMapping
    public void newOrder(@RequestParam String email, @RequestParam BigDecimal amount) throws ExecutionException, InterruptedException {

        // var userId = UUID.randomUUID().toString();
        var orderId = UUID.randomUUID().toString();
        //  var amount = new BigDecimal(Math.random() * 5000 + 1);
        //var email = Math.random() + "@email.com";

        var order = new Order(orderId, amount, email);
        System.out.println(order);
        orderDispatcher.send("ECOMMERCE_NEW_ORDER", email,
                new CorrelationId(NewOrderController.class.getSimpleName()),
                order);

        var emailCode = "isso será um email de envio de ordem!";
        emailDispatcher.send("ECOMMERCE_SEND_EMAIL", email,
                new CorrelationId(NewOrderController.class.getSimpleName()),
                emailCode);
    }

    @PreDestroy
    public void destroy() {
        System.out.println(
                "Callback triggered - @PreDestroy.");
    }
}
