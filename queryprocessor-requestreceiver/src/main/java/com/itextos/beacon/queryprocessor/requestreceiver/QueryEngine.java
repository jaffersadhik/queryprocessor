package com.itextos.beacon.queryprocessor.requestreceiver;

import java.io.FileReader;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

import com.itextos.beacon.queryprocessor.databaseconnector.ConnectionPoolSingleton;

public class QueryEngine
{

    public static Server     server        = null;
    static final Properties  mySQL_cfg_val = new Properties();
    private static final Log log           = LogFactory.getLog(QueryEngine.class);

    public static void main(
            String[] args)
    {

        try
        {
            final String cfg_fn ="/req_receiver.properties_"+System.getenv("profile");// args[0];
            log.info("Reading values from config file: " + cfg_fn);
            final FileReader file = new FileReader(cfg_fn);
            mySQL_cfg_val.load(file);
            file.close();
            final ConnectionPoolSingleton connPool    = ConnectionPoolSingleton.createInstance(mySQL_cfg_val);

            final int                     server_port = Integer.parseInt(mySQL_cfg_val.getProperty("server.port"));
            final int                     min_threads = Integer
                    .parseInt(mySQL_cfg_val.getProperty("server.min.threads"));
            final int                     max_threads = Integer
                    .parseInt(mySQL_cfg_val.getProperty("server.max.threads"));
            log.info("Query Processor Server Port: " + server_port);
            log.info("Query Processor Server Min Threads: " + min_threads);
            log.info("Query Processor Server Max Threads: " + max_threads);

            final QueuedThreadPool threadPool = new QueuedThreadPool(max_threads, min_threads);
            server = new Server(threadPool);
            final ServerConnector connector = new ServerConnector(server);
            connector.setPort(server_port);
            server.addConnector(connector);

            final ServletContextHandler handler = new ServletContextHandler(server, "/");

            log.info("Preparing api routes");
            handler.addServlet(GetData.class, "/get_data");

            handler.addServlet(CreateLogDataQueue.class, "/log_queue/initiate");

            handler.addServlet(LogDataQueueStatus.class, "/log_queue/status");

            handler.addServlet(LogDataQueueList.class, "/log_queue/list");

            server.start();

            log.info("Query Processor Server started");

            server.join();
        }
        catch (final Exception ex)
        {
            log.error("Error Occurred", ex);
            ex.printStackTrace();
        }
    }

}
