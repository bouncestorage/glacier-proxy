package com.bouncestorage;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.ws.rs.core.Response;

import org.jclouds.blobstore.domain.Blob;

import com.google.common.collect.ImmutableList;
import com.sun.net.httpserver.HttpExchange;

public class Archive extends BaseRequestHandler {
    private static List<String> REQUIRED_POST_HEADERS = ImmutableList.of("x-amz-content-sha256",
            "x-amz-sha256-tree-hash");

    public Archive(GlacierProxy proxy) {
        super(proxy);
    }

    @Override
    public void handlePost(HttpExchange request, Map<String, String> parameters) throws IOException {
        // TODO: should verify the hashes
        for (String header : REQUIRED_POST_HEADERS) {
            if (!request.getRequestHeaders().containsKey(header)) {
                Util.sendBadRequest(request);
                return;
            }
        }

        int length = Integer.parseInt(request.getRequestHeaders().get("Content-Length").get(0));
        String treeHash = request.getRequestHeaders().get("x-amz-sha256-tree-hash").get(0);
        UUID uuid = UUID.randomUUID();
        Blob newBlob = proxy.getBlobStore().blobBuilder(uuid.toString())
                .payload(request.getRequestBody())
                .contentLength(length)
                .build();
        String etag = proxy.getBlobStore().putBlob(parameters.get("vault"), newBlob);
        if (etag == null) {
            Util.sendBadRequest(request);
            return;
        }

        request.getResponseHeaders().put("x-amz-sha256-tree-hash", ImmutableList.of(treeHash));
        request.getResponseHeaders().put("Location", ImmutableList.of(String.format("/%s/%s/%s",
                parameters.get("account"), parameters.get("vault"), uuid)));
        request.getResponseHeaders().put("x-amz-archive-id", ImmutableList.of(uuid.toString()));
        request.sendResponseHeaders(Response.Status.CREATED.getStatusCode(), -1);
    }

    @Override
    public void handleDelete(HttpExchange request, Map<String, String> parameters) throws IOException {
        if (!parameters.containsKey("archive")) {
            Util.sendBadRequest(request);
            return;
        }

        proxy.getBlobStore().removeBlob(parameters.get("vault"), parameters.get("archive"));
        request.sendResponseHeaders(Response.Status.NO_CONTENT.getStatusCode(), -1);
    }
}
