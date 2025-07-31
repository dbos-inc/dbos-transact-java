package dev.dbos.transact.http;

import dev.dbos.transact.http.controllers.AdminController;
import jakarta.servlet.Servlet;
import org.apache.catalina.Context;
import org.apache.catalina.Wrapper;
import org.apache.catalina.connector.Connector;
import org.apache.catalina.core.StandardContext;
import org.apache.catalina.core.StandardWrapper;
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
    private int port;
    private AdminController adminController;

    Logger logger = LoggerFactory.getLogger(HttpServer.class);

    private HttpServer(int port, AdminController ac) {
        this.port = port == 0 ? 3001 : port;
        this.adminController = ac ;
    }

    private void init() {
        tomcat = new Tomcat();
        setUpContext();
    }

    public static HttpServer getInstance(int port, AdminController ac) {
        HttpServer s = new HttpServer(port, ac);
        s.init();
        return s;

    }

    public void start() {

        try {
            tomcat.start();
        } catch(Exception e) {
            logger.error("Error starting http server", e) ;
        }
    }

    public void startAndBlock() {
        start();
        tomcat.getServer().await();
    }

    public void stop() {
        try {
            tomcat.stop();
            tomcat.destroy();
        } catch(Exception e) {
            logger.error("Error stopping httpserver", e) ;
        }
    }
    

    private void setUpContext() {

        tomcat.setPort(port);
        tomcat.getConnector(); // default connector

        String contextPath = "";
        String docBase = new File(".").getAbsolutePath();

        Context context = tomcat.addContext(contextPath, docBase);

        ResourceConfig resourceConfig = new ResourceConfig() ;
        resourceConfig.registerInstances(adminController) ;

        // In future if we need to scan from a package
        //    resourceConfig.packages(pkg);

        // Add the REST API servlet
        var jerseyservlet = tomcat.addServlet(contextPath, "jersey-servlet", new ServletContainer(resourceConfig));
        context.addServletMappingDecoded("/*", "jersey-servlet");

    }

}
