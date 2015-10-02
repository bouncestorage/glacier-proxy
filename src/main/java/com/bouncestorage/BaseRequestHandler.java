package com.bouncestorage;

import java.io.IOException;
import java.util.Map;

import javax.ws.rs.core.Response;

import com.sun.net.httpserver.HttpExchange;

abstract class BaseRequestHandler {
    protected final GlacierProxy proxy;

    public BaseRequestHandler(GlacierProxy proxy) {
        this.proxy = proxy;
    }

    public final void handleRequest(HttpExchange httpExchange, Map<String, String> parameters) {
        String method = httpExchange.getRequestMethod();
        try {
            switch (httpExchange.getRequestMethod()) {
                case "GET":
                    handleGet(httpExchange, parameters);
                    break;
                case "POST":
                    handlePost(httpExchange, parameters);
                    break;
                case "PUT":
                    handlePut(httpExchange, parameters);
                    break;
                case "DELETE":
                    handleDelete(httpExchange, parameters);
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported method " + method);
            }
        } catch (IOException e) {
            try {
                httpExchange.sendResponseHeaders(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), -1);
            } catch (IOException sendError) {
                sendError.printStackTrace();
            }
        }
    }

    protected void handleGet(HttpExchange httpExchange, Map<String, String> parameters) throws IOException {
        httpExchange.sendResponseHeaders(Response.Status.METHOD_NOT_ALLOWED.getStatusCode(), -1);
    }
    protected void handlePut(HttpExchange httpExchange, Map<String, String> parameters) throws IOException {
        httpExchange.sendResponseHeaders(Response.Status.METHOD_NOT_ALLOWED.getStatusCode(), -1);
    }

    protected void handlePost(HttpExchange httpExchange, Map<String, String> parameters) throws IOException {
        httpExchange.sendResponseHeaders(Response.Status.METHOD_NOT_ALLOWED.getStatusCode(), -1);
    }

    protected void handleDelete(HttpExchange httpExchange, Map<String, String> parameters) throws IOException {
        httpExchange.sendResponseHeaders(Response.Status.METHOD_NOT_ALLOWED.getStatusCode(), -1);
    }
}
