package br.com.gtmany.service.http.ecommerce;

import br.com.gtmany.poc.kafka.KafkaDispatcher;
import br.com.gtmany.poc.kafka.types.TOPIC_ENUM;
import org.eclipse.jetty.servlet.Source;

import javax.servlet.Servlet;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.UUID;

public class NewOrderServlet extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        try(KafkaDispatcher  orderDispatcher = new KafkaDispatcher<Order>()){
            try(KafkaDispatcher emailDispatcher = new KafkaDispatcher<Email>()) {
                String email = Math.random() + "@email.com";
                    String orderId = UUID.randomUUID().toString();
                    BigDecimal amount = new BigDecimal(Math.random() * 5000 + 1);

                    Order order = new Order(email, orderId, amount, email);
                    orderDispatcher.send(TOPIC_ENUM.ECOMMERCE_NEW_ORDER.name(), email, order);

                    String emailContent = "Thanks! We are processing your order!";
                    Email emailCode = new Email("New Order", emailContent);
                    emailDispatcher.send(TOPIC_ENUM.ECOMMERCE_SEND_EMAIL.name(), email, emailContent);
                }
        }
    }
}
