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
import java.util.concurrent.ExecutionException;

public class GenerateAllReportsServlet extends HttpServlet {

    private static Logger logger = LoggerFactory.getLogger(GenerateAllReportsServlet.class);

    private final KafkaDispatcher batchDispatcher = new KafkaDispatcher<>();

    @Override
    public void destroy() {
        super.destroy();
        batchDispatcher.close();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        try {
            batchDispatcher.send(TOPIC_ENUM.ECOMMERCE_SEND_MESSAGE_TO_ALL_USERS.name(), TOPIC_ENUM.ECOMMERCE_USER_GENERATE_READING_REPORT.name(),
                     new CorrelationId(GenerateAllReportsServlet.class.getSimpleName())
                    ,TOPIC_ENUM.ECOMMERCE_USER_GENERATE_READING_REPORT.name());
            logger.info("Sent generate report to all users.");
            resp.setStatus(HttpServletResponse.SC_OK);
            resp.getWriter().println("Report requests generated.");
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
