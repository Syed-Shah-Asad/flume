package com.aamend.hadoop.flume.interceptor;

import org.apache.flume.Event;
import org.apache.flume.event.JSONEvent;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

/**
 * Author: antoine.amend@gmail.com
 * Date: 11/06/14
 */
@RunWith(JUnit4.class)
public class CustomHostInterceptorTest {

    private static String BODY = "this is my body";
    private static String HOST = "host";

    private String host;
    private Event event;
    private Map<String, String> headers;
    private CustomHostInterceptor interceptor;

    @Before
    public void prepare()
            throws UnknownHostException {

        host = InetAddress.getLocalHost().getHostName();

        headers = new HashMap<String, String>(1);
        headers.put("existingKey", "existingValue");

        event = new JSONEvent();
        event.setBody(BODY.getBytes());
        event.setHeaders(headers);

        interceptor = new CustomHostInterceptor(HOST);
        interceptor.initialize();
    }

    @Test
    public void testInterception(){

        Event interceptedEvent =
                interceptor.intercept(event);

        assertEquals("Event body should not have been altered",
                BODY,
                new String(interceptedEvent.getBody()));

        assertTrue("Header should now contain host",
                interceptedEvent.getHeaders().containsKey(HOST));

        assertEquals("Header's hostname should be correct",
                host,
                interceptedEvent.getHeaders().get(HOST));


    }



}
