package demo.jetty.demo3;

import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.ServletHandler;

/**
 * 访问Servlet
 * 访问链接  http://localhost:8081/
 */

public class ServletServer {
    public static void main(String args[]) {
        Server server = new Server(8081);
        ServletHandler handler = new ServletHandler();
        handler.addServletWithMapping("demo.jetty.demo3.HelloServlet", "/*");
        server.setHandler(handler);
        try {
            server.start();
            server.join();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
