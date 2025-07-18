package dev.dbos.transact.http;

import dev.dbos.transact.http.controllers.MyRestController;
import org.apache.catalina.Context;
import org.apache.catalina.startup.Tomcat;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class HttpServer {

    private Tomcat tomcat;
    private ResourceConfig resourceConfig;

    Logger logger = LoggerFactory.getLogger(HttpServer.class);

    private HttpServer() {

    }

    private void init() {

        tomcat = new Tomcat();
        tomcat.setPort(8080);

        tomcat.getConnector();

        String contextPath = "";
        String docBase = new File(".").getAbsolutePath();

        Context context = tomcat.addContext(contextPath, docBase);

        resourceConfig = new ResourceConfig(MyRestController.class);
        resourceConfig.packages("com.dbos.transact.http.controllers");

        // Add the REST API servlet
        var jerseyservlet = tomcat.addServlet(contextPath, "jersey-servlet", new ServletContainer(resourceConfig));
        // context.addServletMappingDecoded("/jersey/*", "jersey-servlet");
        context.addServletMappingDecoded("/*", "jersey-servlet");

    }

    public static HttpServer getInstance() {
        HttpServer s = new HttpServer();
        s.init();
        return s;

    }

    public void start() {

        try {
            tomcat.start();
            // tomcat.getServer().await(); blocks the main thread which we do not want
        } catch(Exception e) {
            logger.error("Error starting http server", e) ;
        }
    }

    public void stop() {
        try {
            tomcat.stop();
            tomcat.destroy();
        } catch(Exception e) {
            logger.error("Error stopping httpserver", e) ;
        }
    }

    public void register(String... packages) {
        resourceConfig.packages(packages);
    }

    public static void main(String[] args) throws Exception{
        System.out.println("Hello Dbos");

        HttpServer s = HttpServer.getInstance();
        s.start();

    }
}
