package br.com.br.com.api.controller;

import br.com.CorrelationId;
import br.com.KafkaDispatcher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.PreDestroy;
import java.util.concurrent.ExecutionException;

@RestController
@RequestMapping("/admin/generate-reports")
public class GenerateAllReportsController {

    private KafkaDispatcher<String> batchDispatcher = new KafkaDispatcher<>();

    @GetMapping
    public ResponseEntity<?> generateAllReports() {

        try {
            batchDispatcher.send("ECOMMERCE_SEND_MESSAGE_TO_ALL_USERS", "ECOMMERCE_USER_GENERATE_READING_REPORT",
                    new CorrelationId(GenerateAllReportsController.class.getSimpleName()),
                    "ECOMMERCE_USER_GENERATE_READING_REPORT");
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        System.out.println("Sent generate report to all users");
        return ResponseEntity.ok().build();
    }

}
