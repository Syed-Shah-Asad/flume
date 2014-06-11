package com.aamend.hadoop.flume.interceptor;

import static junit.framework.Assert.*;
import org.apache.flume.Event;
import org.apache.flume.event.JSONEvent;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.HashMap;
import java.util.Map;

/**
 * Author: antoine.amend@gmail.com
 * Date: 11/06/14
 */
@RunWith(JUnit4.class)
public class CustomInterceptorTest {

    private static String KEY = "foo";
    private static String BODY = "this is my body";
    private Event event;
    private Map<String, String> headers;
    private CustomInterceptor interceptor;

    @Before
    public void prepare(){

        headers = new HashMap<String, String>(1);
        headers.put("myKey", "myValue");
        event = new JSONEvent();
        event.setBody(BODY.getBytes());
        event.setHeaders(headers);

        interceptor = new CustomInterceptor(KEY);
        interceptor.initialize();
    }

    @Test
    public void testInterception(){

        Event interceptedEvent =
                interceptor.intercept(event);

        assertEquals("Event body should not have been altered",
                BODY,
                new String(interceptedEvent.getBody()));

        assertTrue("Header should now contain custom key / value",
                interceptedEvent.getHeaders().containsKey(KEY));

        for(String key : event.getHeaders().keySet()){
            assertEquals("Existing header should not have been altered",
                    event.getHeaders().get(key),
                    interceptedEvent.getHeaders().get(key));
        }



    }



}
