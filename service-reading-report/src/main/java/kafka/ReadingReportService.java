package kafka;

import br.com.gtmany.poc.kafka.ConsumerService;
import br.com.gtmany.poc.kafka.consumer.KafkaService;
import br.com.gtmany.poc.kafka.Message;
import br.com.gtmany.poc.kafka.consumer.ServiceRunner;
import br.com.gtmany.poc.kafka.types.TOPIC_ENUM;
import kafka.model.User;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;

public class ReadingReportService implements ConsumerService<User> {

    private static Logger logger = LoggerFactory.getLogger(ReadingReportService.class);

    private static final Path SOURCE = new File("src/main/resources/report.txt").toPath();

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        new ServiceRunner(ReadingReportService::new).start(5);
    }

    public void parse(ConsumerRecord<String, Message<User>> record) throws IOException {
        logger.info("-------------------------------------------");
        logger.info("Processing report for "+ record.value());

        Message<User> message = record.value();
        User user = message.getPayload();
        File target = new File(user.getReportPath());
        IO.copyTo(SOURCE, target);
        IO.append(target, "Created for" + user.geUuid());
    }


    @Override
    public String getTopic() {
        return TOPIC_ENUM.ECOMMERCE_USER_GENERATE_READING_REPORT.name();
    }

    @Override
    public String getConsumerGroup() {
        return ReadingReportService.class.getName();
    }
}
