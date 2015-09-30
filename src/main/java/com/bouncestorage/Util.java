package com.bouncestorage;

import java.io.IOException;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.jclouds.blobstore.options.ListContainerOptions;
import org.json.simple.JSONObject;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import com.sun.net.httpserver.HttpExchange;

public class Util {
    public static final String DATE_FORMAT = "EEE, dd MMM yyyy HH:mm:ss zzz";

    public static Multimap<String, String> parseQuery(String query) {
        Multimap<String, String> map = LinkedHashMultimap.create();
        for (String q : query.split("&")) {
            String[] kv = q.split("=");
            map.put(kv[0], kv[1]);
        }
        return map;
    }

    public static ListContainerOptions getOptions(String query) {
        ListContainerOptions options = ListContainerOptions.NONE;
        if (query == null) {
            return options;
        }
        Multimap<String, String> queryParams = Util.parseQuery(query);
        String marker = Iterables.get(queryParams.get("marker"), 0);
        String limit = Iterables.get(queryParams.get("limit"), 0);
        if (limit != null) {
            options.maxResults(Integer.parseInt(limit));
        }
        if (marker != null) {
            options.afterMarker(marker);
        }

        return options;
    }

    public static String getTimeStamp() {
        SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
        format.setTimeZone(TimeZone.getTimeZone("GMT"));
        return format.format(new Date());
    }

    public static void sendJSON(HttpExchange httpExchange, Response.Status code, JSONObject json) throws IOException {
        String jsonResponse = json.toJSONString();
        httpExchange.getResponseHeaders().put("Content-type", ImmutableList.of(MediaType.APPLICATION_JSON));
        httpExchange.sendResponseHeaders(code.getStatusCode(), jsonResponse.length());
        try (OutputStream os = httpExchange.getResponseBody()) {
            os.write(jsonResponse.getBytes());
        }
    }

    public static void sendBadRequest(HttpExchange httpExchange) throws IOException {
        httpExchange.sendResponseHeaders(Response.Status.BAD_REQUEST.getStatusCode(), -1);
    }
}
