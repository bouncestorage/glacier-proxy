package com.bouncestorage.glacierproxy;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.ws.rs.core.Response;

import org.jclouds.blobstore.domain.Blob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.sun.net.httpserver.HttpExchange;

public class Archive extends BaseRequestHandler {
    private static final Logger logger = LoggerFactory.getLogger(Archive.class);

    private static final List<String> REQUIRED_POST_HEADERS = ImmutableList.of("x-amz-content-sha256",
            "x-amz-sha256-tree-hash");

    public Archive(GlacierProxy proxy) {
        super(proxy);
    }

    @Override
    public void handlePost(HttpExchange request, Map<String, String> parameters) throws IOException {
        // TODO: should verify the hashes
        for (String header : REQUIRED_POST_HEADERS) {
            if (!request.getRequestHeaders().containsKey(header)) {
                logger.warn("Missing x-amz-content-sha256 or x-amz-sha256-tree-hash hashes");
                Util.sendBadRequest("Missing content or tree hash", request);
                return;
            }
        }

        String vault = parameters.get("vault");
        long length = Long.parseLong(request.getRequestHeaders().getFirst("Content-Length"));
        String treeHash = request.getRequestHeaders().getFirst("x-amz-sha256-tree-hash");
        UUID uuid = UUID.randomUUID();
        Blob newBlob = proxy.getBlobStore().blobBuilder(uuid.toString())
                .payload(request.getRequestBody())
                .contentLength(length)
                .build();
        String etag = proxy.getBlobStore().putBlob(vault, newBlob);
        if (etag == null) {
            logger.warn("Failed to create blob in {}", vault);
            Util.sendServerError("Failed to create the archive", request);
            return;
        }

        logger.debug("Created a blob: {}/{}", vault, uuid.toString());
        request.getResponseHeaders().put("x-amz-sha256-tree-hash", ImmutableList.of(treeHash));
        request.getResponseHeaders().put("Location", ImmutableList.of(String.format("/%s/%s/%s",
                parameters.get("account"), vault, uuid)));
        request.getResponseHeaders().put("x-amz-archive-id", ImmutableList.of(uuid.toString()));
        request.sendResponseHeaders(Response.Status.CREATED.getStatusCode(), -1);
    }

    @Override
    public void handleDelete(HttpExchange request, Map<String, String> parameters) throws IOException {
        if (!parameters.containsKey("archive")) {
            Util.sendBadRequest("Archive ID must be specified", request);
            logger.debug("Delete arcihve called without an archive ID");
            return;
        }

        String blob = parameters.get("archive");
        String vault = parameters.get("vault");

        proxy.getBlobStore().removeBlob(vault, blob);
        logger.debug("Removed archive {}/{}", vault, blob);
        request.sendResponseHeaders(Response.Status.NO_CONTENT.getStatusCode(), -1);
    }
}
