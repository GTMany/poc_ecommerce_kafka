package br.com.gtmany.serviceusers;

import br.com.gtmany.poc.kafka.ConsumerService;
import br.com.gtmany.poc.kafka.consumer.KafkaService;
import br.com.gtmany.poc.kafka.Message;
import br.com.gtmany.poc.kafka.consumer.ServiceRunner;
import br.com.gtmany.poc.kafka.types.TOPIC_ENUM;
import br.com.gtmany.serviceusers.model.Order;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class CreateUserService implements ConsumerService<Order> {

    private static Logger logger = LoggerFactory.getLogger(CreateUserService.class);
    private final Connection connection;

    CreateUserService () throws SQLException {
        String url = "jdbc:sqlite:target/users_database.db";
        connection = DriverManager.getConnection(url);
        connection.createStatement().execute("create table IF NOT EXISTS Users (" +
                "uuid varchar(200) primary key, " +
                "email varchar(200))");
    }

    public static void main(String[] args) {
        new ServiceRunner(CreateUserService::new).start(5);
    }

    public void parse(ConsumerRecord<String, Message<Order>> record) throws InterruptedException, ExecutionException, SQLException {
        Thread.sleep(5000);
        logger.info("-------------------------------------------");
        logger.info("Processing new order, checking for new user");
        logger.info("KEY: " + record.key().toString());
        logger.info("VALUE: " + record.value().toString());
        logger.info("PARTITION: " + String.valueOf(record.partition()));
        logger.info("OFFSET: " + String.valueOf(record.offset()));
        logger.info("Order processed.");
        Message<Order> message = record.value();
        Order order = message.getPayload();
        if(isNewUser(order.getEmail())){
            insertNewUser(order);
        }
    }

    @Override
    public String getTopic() {
        return TOPIC_ENUM.ECOMMERCE_NEW_ORDER.name();
    }

    @Override
    public String getConsumerGroup() {
        return CreateUserService.class.getName();
    }

    private void insertNewUser(Order order) throws SQLException {
        String insertSQL = "insert into Users (uuid, email) " +
                "values (?,?)";
        PreparedStatement statement = connection.prepareStatement(insertSQL);
        statement.setString(1, UUID.randomUUID().toString());
        statement.setString(2, order.getEmail());
        statement.execute();
        logger.info("User inserted successfully! - " + order.getEmail());
    }

    private boolean isNewUser(String email) throws SQLException {
        String selectSQL = "select uuid from Users where email = ? limit 1";
        PreparedStatement statement = connection.prepareStatement(selectSQL);
        statement.setString(1, email);
        ResultSet results = statement.executeQuery();
        return !results.next();
    }

}
