package dev.dbos.transact.http;

import dev.dbos.transact.http.controllers.AdminController;
import org.apache.catalina.Context;
import org.apache.catalina.startup.Tomcat;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class HttpServer {

    private Tomcat tomcat;
    private ResourceConfig resourceConfig;
    private int port;
    private List<String> httpPackages = new ArrayList<>();

    Logger logger = LoggerFactory.getLogger(HttpServer.class);

    private HttpServer(int port, List<String> pkgs) {
        this.port = port == 0 ? 8080 : port;
        this.httpPackages.add("dev.dbos.transact.http.controller");

        this.httpPackages.addAll(pkgs);
        this.resourceConfig = new ResourceConfig();
    }

    private void init() {

        tomcat = new Tomcat();
        // tomcat.setPort(8080);
        tomcat.setPort(port);

        tomcat.getConnector();

        String contextPath = "";
        String docBase = new File(".").getAbsolutePath();

        Context context = tomcat.addContext(contextPath, docBase);

        //resourceConfig = new ResourceConfig(AdminController.class);
        resourceConfig.packages("dev.dbos.transact.http.controllers");
        // resourceConfig = new ResourceConfig();
        for (String pkg : httpPackages) {
            resourceConfig.packages(pkg);
        }

        // Add the REST API servlet
        var jerseyservlet = tomcat.addServlet(contextPath, "jersey-servlet", new ServletContainer(resourceConfig));
        // context.addServletMappingDecoded("/jersey/*", "jersey-servlet");
        context.addServletMappingDecoded("/*", "jersey-servlet");

    }

    public static HttpServer getInstance(int port, List<String> httpPackages) {
        HttpServer s = new HttpServer(port, httpPackages);
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

    /* public static void main(String[] args) throws Exception{
        System.out.println("Hello Dbos");

        HttpServer s = HttpServer.getInstance();
        s.start();

    } */
}
