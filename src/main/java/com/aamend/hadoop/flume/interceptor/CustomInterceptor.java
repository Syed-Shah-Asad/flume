package com.aamend.hadoop.flume.interceptor;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Author: antoine.amend@gmail.com
 * Date: 11/06/14
 */
public class CustomInterceptor implements Interceptor {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(CustomInterceptor.class);

    private String myValue;
    private String myKey;
    private Gson gson;

    public CustomInterceptor(String myKey){
        this.myKey = myKey;
    }

    @Override
    public void initialize() {

        gson = new GsonBuilder().create();

        // Do whatever you want here...
        myValue = "bar";
    }

    @Override
    public Event intercept(Event event) {

        LOGGER.info("Before interception : {}",
                gson.toJson(event));

        // This is the event's body
        String body = new String(event.getBody());

        // These are the event's headers
        Map<String, String> headers = event.getHeaders();

        // Enrich header with custom Key
        headers.put(myKey, myValue);

        // Update event's headers
        event.setHeaders(headers);

        LOGGER.info("After  interception : {}",
                gson.toJson(event));

        // Let the enriched event go
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {

        List<Event> interceptedEvents =
                new ArrayList<Event>(events.size());
        for (Event event : events) {
            Event interceptedEvent = intercept(event);
            interceptedEvents.add(interceptedEvent);
        }

        return interceptedEvents;
    }

    @Override
    public void close() {
        // Nothing to close here
    }

    public static class Builder implements Interceptor.Builder {

        public static String DEFAULT_KEY = "DEFAULT";
        private String myKey;

        @Override
        public void configure(Context context) {
            myKey = context.getString("my.key");
            if(StringUtils.isEmpty(myKey)){
                myKey = DEFAULT_KEY;
            }
        }

        @Override
        public Interceptor build() {
            return new CustomInterceptor(myKey);
        }
    }
}
