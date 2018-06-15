package demo.jetty.demo2;

import org.mortbay.jetty.Handler;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.DefaultHandler;
import org.mortbay.jetty.handler.ResourceHandler;

/**
 * 静态资源 ResourceHandler
 *
 * 访问链接  http://localhost:8081/ 或者 http://localhost:8081/index.html
 */
public class ResourceHandlerServer {
    public static void main(String args[]) {
        Server server = new Server(8081);
        ResourceHandler resourceHandler = new ResourceHandler();
        resourceHandler.setWelcomeFiles(new String[]{"index.html"});
        resourceHandler.setResourceBase("E:\\code\\intellij_code\\meteor\\program-learning\\src\\main\\java\\demo\\jetty\\demo2");

        // 绑定多个handler
        server.setHandlers(new Handler[]{resourceHandler, new DefaultHandler()});

        try {
            server.start();
            server.join();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
