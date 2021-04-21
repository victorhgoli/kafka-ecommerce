package br.com;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import br.com.consumer.ConsumerService;
import br.com.consumer.KafkaService;
import br.com.consumer.ServiceRunner;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class ReadingReportService implements ConsumerService<User> {

    private static final Path SOURCE = new File("src/main/resources/report.txt").toPath();

    public static void main(String[] args) {
        new ServiceRunner(ReadingReportService::new).start(5);
    }

    public void parse(ConsumerRecord<String, Message<User>> record) throws IOException {
        var message = record.value();

        var user = message.getPayload();

        System.out.println("----------------------------------------");
        System.out.println("Processing report for:" + user);

        var target = new File(user.getReportPath());

        IO.copyTo(SOURCE, target);
        IO.append(target, "Created for " + user.getUuid());

        System.out.println("File created: " + target.getAbsolutePath());
    }


    @Override
    public String getTopic() {
        return "ECOMMERCE_USER_GENERATE_READING_REPORT";
    }

    @Override
    public String getConsumerGroup() {
        return ReadingReportService.class.getSimpleName();
    }
}
