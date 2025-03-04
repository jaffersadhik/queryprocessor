package com.itextos.beacon.queryprocessor.requestreceiver;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.jetty.http.HttpStatus;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;


public class Metrics
        extends
        HttpServlet
{

    private static final Log log           = LogFactory.getLog(Metrics.class);
  

    @SuppressWarnings("unchecked")
    @Override
    protected void doPost(
            HttpServletRequest req,
            HttpServletResponse resp)
            throws ServletException,
            IOException
    {
        // add timestamp of the server

        try
        {
            resp.getWriter().println("ok");
        }
        catch (

        final Exception e)
        {
            log.error("Error Occurred", e);
            resp.setStatus(HttpStatus.INTERNAL_SERVER_ERROR_500);
            resp.getWriter().println("err");
        }
       
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void doGet(
            HttpServletRequest req,
            HttpServletResponse resp)
            throws ServletException,
            IOException
    {
        // add timestamp of the server
    	doPost(req, resp);
       
    }
}
