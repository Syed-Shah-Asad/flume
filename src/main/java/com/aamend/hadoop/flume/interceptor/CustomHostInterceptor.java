package com.aamend.hadoop.flume.interceptor;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Author: antoine.amend@gmail.com
 * Date: 11/06/14
 */
public class CustomHostInterceptor implements Interceptor {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(CustomHostInterceptor.class);

    private Gson gson;
    private String hostValue;
    private String hostHeader;

    public CustomHostInterceptor(String hostHeader){
        this.hostHeader = hostHeader;
    }

    @Override
    public void initialize() {
        gson = new GsonBuilder().create();
        try {
            hostValue =
                    InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            LOGGER.error("Cannot get Hostname", e);
            throw new FlumeException("Cannot get Hostname", e);
        }
    }

    @Override
    public Event intercept(Event event) {

        LOGGER.info("Before interception : {}",
                gson.toJson(event));

        // This is the event's body
        String body = new String(event.getBody());

        // These are the event's headers
        Map<String, String> headers = event.getHeaders();

        // Enrich header with hostname
        headers.put(hostHeader, hostValue);

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

    public static class Builder
            implements Interceptor.Builder {

        private String hostHeader;

        @Override
        public void configure(Context context) {
            hostHeader = context.getString("hostHeader");
        }

        @Override
        public Interceptor build() {
            return new CustomHostInterceptor(hostHeader);
        }
    }
}
