package demo.jetty.demo1;

import org.mortbay.jetty.Request;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.AbstractHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * 启动jetty服务器
 * 访问链接  http://localhost:8081/
 */
class HelloHandler extends AbstractHandler {

    @Override
    public void handle(String target, HttpServletRequest request, HttpServletResponse response, int dispatch) throws IOException, ServletException {
        response.setContentType("text/html;charset=utf-8");
        response.setStatus(HttpServletResponse.SC_OK);
        response.getWriter().println("<h1>Hello World</h1>");
        ((Request)request).setHandled(true);
    }
}
public class SimplestServer {


    public static void main(String args[]) {
        Server server = new Server(8081); // 绑定端口8081。如果设置为0，那么程序会自动选择一个可用的端口
        server.setHandler(new HelloHandler()); // handler处理请求

        try {
            server.start();
            server.join();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
