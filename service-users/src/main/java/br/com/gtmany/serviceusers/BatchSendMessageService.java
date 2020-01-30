package br.com.gtmany.serviceusers;

import br.com.gtmany.poc.kafka.dispatcher.KafkaDispatcher;
import br.com.gtmany.poc.kafka.consumer.KafkaService;
import br.com.gtmany.poc.kafka.Message;
import br.com.gtmany.poc.kafka.User;
import br.com.gtmany.poc.kafka.types.TOPIC_ENUM;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class BatchSendMessageService {
    private static Logger logger = LoggerFactory.getLogger(CreateUserService.class);
    private final Connection connection;

    BatchSendMessageService () throws SQLException {
        String url = "jdbc:sqlite:target/users_database.db";
        connection = DriverManager.getConnection(url);
        connection.createStatement().execute("create table IF NOT EXISTS Users (" +
                "uuid varchar(200) primary key, " +
                "email varchar(200))");
    }

    public static void main(String[] args) throws InterruptedException, SQLException {
        BatchSendMessageService batchSendMessageService = new BatchSendMessageService();
        try(KafkaService service = new KafkaService<>(BatchSendMessageService.class.getName(), TOPIC_ENUM.ECOMMERCE_SEND_MESSAGE_TO_ALL_USERS.name(),
                batchSendMessageService::parse, new HashMap<>())) {
            try {
                service.run();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
    }


    private final KafkaDispatcher<User> userKafkaDispatcher = new KafkaDispatcher<User>();

    private void parse(ConsumerRecord<String, Message<String>> record) throws InterruptedException, ExecutionException, SQLException {
        logger.info("-------------------------------------------");
        logger.info("Processing new batch, checking for new user");
        logger.info("TOPIC: " + record.value());
        Message<String> message = record.value();
        for (User user: getAllUsers()){
            userKafkaDispatcher.send(message.getPayload(), user.getUuid(),
                    message.getId().continueWith(BatchSendMessageService.class.getSimpleName()),
                    user);
        }
    }

    private List<User> getAllUsers() throws SQLException {
        ResultSet results = connection.prepareStatement("select uuid from users").executeQuery();
        List<User> users = new ArrayList<>();
        while (results.next()){
            users.add(new User(results.getString(1)));
        }
        return users;
    }
}
