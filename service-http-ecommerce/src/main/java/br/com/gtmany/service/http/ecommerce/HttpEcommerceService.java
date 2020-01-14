package br.com.gtmany.service.http.ecommerce;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

public class HttpEcommerceService {

    public static void main(String[] args) {
        Server server = new Server(8080);

        ServletContextHandler context = new ServletContextHandler();
        context.setContextPath("/");
        context.addServlet(new ServletHolder(new NewOrderServlet()), "/new");
    }
}
