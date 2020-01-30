import br.com.gtmany.poc.kafka.ConsumerService;
import br.com.gtmany.poc.kafka.CorrelationId;
import br.com.gtmany.poc.kafka.Message;
import br.com.gtmany.poc.kafka.consumer.ServiceRunner;
import br.com.gtmany.poc.kafka.dispatcher.KafkaDispatcher;
import br.com.gtmany.poc.kafka.types.TOPIC_ENUM;
import model.Order;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

public class EmailNewOrderService implements ConsumerService<Order> {

    private static Logger logger = LoggerFactory.getLogger(EmailNewOrderService.class);

    private final KafkaDispatcher<String> emailDispatcher = new KafkaDispatcher<>();

    public static void main(String[] args) {
        new ServiceRunner(EmailNewOrderService::new).start(1);
    }

    @Override
    public void parse(ConsumerRecord<String, Message<Order>> record) throws InterruptedException, ExecutionException {
        Message<Order> message = record.value();

        logger.info("-------------------------------------------");
        logger.info("Processing new order, preparing email");
        logger.info("KEY: " + record.key().toString());
        logger.info("VALUE: " + message.toString());

        String emailContent = "Thanks! We are processing your order!";
        Order order = message.getPayload();
        CorrelationId id = message.getId();
        emailDispatcher.send(TOPIC_ENUM.ECOMMERCE_SEND_EMAIL.name(), order.getEmail(),
                id.continueWith(EmailNewOrderService.class.getSimpleName()), emailContent);
    }

    @Override
    public String getTopic() {
        return EmailNewOrderService.class.getName();
    }

    @Override
    public String getConsumerGroup() {
        return  TOPIC_ENUM.ECOMMERCE_NEW_ORDER.name();
    }
}
