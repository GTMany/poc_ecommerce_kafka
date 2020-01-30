package br.com.gtmany.service.http.ecommerce;

import br.com.gtmany.poc.kafka.CorrelationId;
import br.com.gtmany.poc.kafka.dispatcher.KafkaDispatcher;
import br.com.gtmany.poc.kafka.types.TOPIC_ENUM;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderServlet extends HttpServlet {

    private static Logger logger = LoggerFactory.getLogger(NewOrderServlet.class);

    private final KafkaDispatcher  orderDispatcher = new KafkaDispatcher<Order>();
    private final KafkaDispatcher emailDispatcher = new KafkaDispatcher<Email>();

    @Override
    public void destroy() {
        super.destroy();
        orderDispatcher.close();
        emailDispatcher.close();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        try {
            String email = req.getParameter("email");
            String orderId = UUID.randomUUID().toString();
            BigDecimal amount = new BigDecimal(req.getParameter("amount"));

            Order order = new Order(email, orderId, amount, email);
            orderDispatcher.send(TOPIC_ENUM.ECOMMERCE_NEW_ORDER.name(), email,
                    new CorrelationId(NewOrderServlet.class.getSimpleName()),
                    order);

            String emailContent = "Thanks! We are processing your order!";
            Email emailCode = new Email("New Order", emailContent);
            emailDispatcher.send(TOPIC_ENUM.ECOMMERCE_SEND_EMAIL.name(), email,
                    new CorrelationId(NewOrderServlet.class.getSimpleName()),
                    emailContent);
            logger.info("New Order Sent Successfully.");
            resp.setStatus(HttpServletResponse.SC_OK);
            resp.getWriter().println("New Order Sent Successfully.");
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
