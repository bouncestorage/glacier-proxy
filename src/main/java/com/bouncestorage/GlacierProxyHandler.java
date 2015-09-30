package com.bouncestorage;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

public class GlacierProxyHandler implements HttpHandler {
    private GlacierProxy server;

    GlacierProxyHandler(GlacierProxy server) {
        this.server = server;
    }

    // Caller may specify "-" and expect account to be looked up from the credentials
    static final String VAULT_PREFIX = "^/(?<acct>(\\d{12}|-))/vaults";
    static final Pattern JOBS_RE = Pattern.compile(VAULT_PREFIX + "/(?<vault>[a-zA-Z0-9\\.-_]+)/jobs");
    static final Pattern ARCHIVES_RE = Pattern.compile(VAULT_PREFIX + "/(?<vault>[a-zA-Z0-9\\.-_]+)/archives");
    static final Pattern VAULTS_RE = Pattern.compile(VAULT_PREFIX + "(/(?<vault>[a-zA-Z0-9\\.-_]+))?");

    public void handle(HttpExchange httpExchange) throws IOException {
        httpExchange.getResponseHeaders().put("x-amzn-RequestId", ImmutableList.of("glacier-proxy"));
        String requestPath = httpExchange.getRequestURI().getPath();
        Matcher matcher = JOBS_RE.matcher(requestPath);
        if (matcher.matches()) {
            return;
        }

        matcher = ARCHIVES_RE.matcher(requestPath);
        if (matcher.matches()) {
            return;
        }

        matcher = VAULTS_RE.matcher(requestPath);
        if (matcher.matches()) {
            String vault = matcher.group("vault");
            ImmutableMap<String, String> parameters;
            if (matcher.group("vault") != null) {
                parameters = ImmutableMap.of("account", matcher.group("acct"), "vault", matcher.group("vault"));
            } else {
                parameters = ImmutableMap.of("account", matcher.group("acct"));
            }
            server.getVault(vault).handleRequest(httpExchange, parameters);
            return;
        }
    }
}
