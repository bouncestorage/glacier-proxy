package com.bouncestorage.glacierproxy;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

public class GlacierProxyHandler implements HttpHandler {
    private static final Logger logger = LoggerFactory.getLogger(GlacierProxy.class);

    private GlacierProxy server;

    GlacierProxyHandler(GlacierProxy server) {
        this.server = server;
    }

    // Caller may specify "-" and expect account to be looked up from the credentials
    static final String VAULT_NAME = "(?<vault>[a-zA-Z0-9\\.\\-_]+)";
    static final String VAULT_PREFIX = "^/(?<account>(\\d{12}|-))/vaults";
    static final Pattern JOBS_RE = Pattern.compile(String.format("%s/%s/jobs(/(?<job>[a-zA-Z0-9-_]+))?(/output)?",
            VAULT_PREFIX, VAULT_NAME));
    static final Pattern ARCHIVES_RE = Pattern.compile(String.format("%s/%s/archives(/(?<archive>[a-zA-Z0-9-_]+))?",
            VAULT_PREFIX, VAULT_NAME));
    static final Pattern MULTIPART_RE = Pattern.compile(
            String.format("%s/%s/multipart-uploads(/(?<upload>[a-zA-Z0-9-_]+))?", VAULT_PREFIX, VAULT_NAME));
    static final Pattern VAULTS_RE = Pattern.compile(String.format("%s(/%s)?", VAULT_PREFIX, VAULT_NAME));

    static final String VERSION_HEADER = "x-amz-glacier-version";
    static final String CURRENT_VERSION = "2012-06-01";

    public void handle(HttpExchange httpExchange) throws IOException {
        if (!httpExchange.getRequestHeaders().getFirst(VERSION_HEADER).equals(CURRENT_VERSION)) {
            logger.warn("Invalid or missing API version: {}; expected {}",
                    httpExchange.getRequestHeaders().getFirst(VERSION_HEADER), CURRENT_VERSION);
            Util.sendBadRequest("Unsupported or missing API version", httpExchange);
            return;
        }

        Map<String, String> parameters = new HashMap<>();
        httpExchange.getResponseHeaders().put("x-amzn-RequestId", ImmutableList.of("glacier-proxy"));
        String requestPath = httpExchange.getRequestURI().getPath();

        logger.debug("Processing {}", requestPath);

        Matcher matcher = JOBS_RE.matcher(requestPath);
        if (matcher.matches()) {
            setParameters(matcher, ImmutableList.of("account", "vault", "job"), parameters);
            server.getJobHandler(parameters).handleRequest(httpExchange, parameters);
            return;
        }

        matcher = ARCHIVES_RE.matcher(requestPath);
        if (matcher.matches()) {
            setParameters(matcher, ImmutableList.of("account", "vault", "archive"), parameters);
            server.getArchive(parameters).handleRequest(httpExchange, parameters);
            return;
        }

        matcher = MULTIPART_RE.matcher(requestPath);
        if (matcher.matches()) {
            setParameters(matcher, ImmutableList.of("account", "vault", "upload"), parameters);
            server.getMultipartHandler(parameters).handleRequest(httpExchange, parameters);
            return;
        }

        matcher = VAULTS_RE.matcher(requestPath);
        if (matcher.matches()) {
            setParameters(matcher, ImmutableList.of("account", "vault"), parameters);
            server.getVault(parameters).handleRequest(httpExchange, parameters);
            return;
        }

        logger.debug("Unknown request {}", requestPath);
        Util.sendBadRequest("Unknown request", httpExchange);
        httpExchange.getResponseBody().close();
    }

    private void setParameters(Matcher matcher, List<String> keys, Map<String, String> parameters) {
        for (String key : keys) {
            if (matcher.group(key) != null) {
                parameters.put(key, matcher.group(key));
            }
        }
    }
}
