package com.bouncestorage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import com.sun.net.httpserver.HttpExchange;

public class Job extends BaseRequestHandler {
    private static final List<String> JOB_TYPES = ImmutableList.of("archive-retrieval", "inventory-retrieval");

    public Job(GlacierProxy proxy) {
        super(proxy);
    }

    @Override
    public void handlePost(HttpExchange request, Map<String, String> parameters) throws IOException {
        JSONObject object = new JSONObject(new JSONTokener(request.getRequestBody()));
        if (object.getString("Type") == null) {
            Util.sendBadRequest(request);
            return;
        }

        if (!proxy.getBlobStore().containerExists(parameters.get("vault"))) {
            Util.sendBadRequest(request);
        }

        if (!JOB_TYPES.contains(object.getString("Type"))) {
            Util.sendBadRequest(request);
            return;
        }

        if (object.getString("Type").equals("archive-retrieval")) {
            String blobName = object.getString("ArchiveId");
            if (blobName == null) {
                Util.sendBadRequest(request);
                return;
            }
            if (!proxy.getBlobStore().blobExists(parameters.get("vault"), blobName)) {
                Util.sendBadRequest(request);
                return;
            }
        }

        UUID jobId = UUID.randomUUID();
        proxy.addJob(parameters.get("vault"), jobId, object);
        request.getResponseHeaders().put("x-amz-job-id", ImmutableList.of(jobId.toString()));
        request.getResponseHeaders().put("Location", ImmutableList.of(String.format("/%s/vaults/%s/jobs/%s",
                parameters.get("account"), parameters.get("vault"), jobId.toString())));
        request.sendResponseHeaders(Response.Status.ACCEPTED.getStatusCode(), -1);
    }

    @Override
    public void handleGet(HttpExchange request, Map<String, String> parameters) throws IOException {
        // Differentiate between List jobs, describe job, and get job output
        String path = request.getRequestURI().getPath();
        if (path.endsWith("output")) {
            String vault = parameters.get("vault");
            JSONObject jobRequest = proxy.getJob(parameters.get("vault"), UUID.fromString(parameters.get("job")));
            if (jobRequest.get("Type").equals("archive-retrieval")) {
                handleRetrieveArchiveJob(request, vault, jobRequest);
                return;
            } else {
                handleRetrieveInventoryJob(request, vault, jobRequest);
            }
        } else if (parameters.get("job") != null && path.endsWith(parameters.get("job"))) {
            handleDescribeJob(request, parameters);
        } else if (path.endsWith("jobs")) {
            handleListJobs(request, parameters);
        } else {
            Util.sendBadRequest(request);
        }
    }

    private void handleDescribeJob(HttpExchange httpExchange, Map<String, String> parameters) throws IOException {
        Util.sendBadRequest(httpExchange);
    }

    private void handleListJobs(HttpExchange httpExchange, Map<String, String> parameters) throws IOException {
        Util.sendBadRequest(httpExchange);
    }

    private void handleRetrieveInventoryJob(HttpExchange httpExchange, String vault, JSONObject job) throws
            IOException {
        JSONObject response = new JSONObject();
        response.put("VaultARN", String.format("arn:::::vaults/%s", vault));
        response.put("InventoryDate", new Date());
        JSONArray archives = new JSONArray();

        // TODO: support pagination
        for (StorageMetadata sm : proxy.getBlobStore().list(vault)) {
            JSONObject archive = new JSONObject();
            archive.put("ArchiveId", sm.getName());
            archive.put("CreationDate", sm.getCreationDate());
            archive.put("Size", sm.getSize());
            archive.put("SHA256TreeHash", "deadbeef");
            archive.put("ArchiveDescription", "NA");
            archives.put(archive);
        }
        response.put("ArchiveList", archives);

        String body = response.toString();
        httpExchange.getResponseHeaders().put("Content-Length", ImmutableList.of(Integer.toString(body.length())));
        httpExchange.getResponseHeaders().put("Content-Type", ImmutableList.of(MediaType.APPLICATION_JSON));
        httpExchange.sendResponseHeaders(Response.Status.OK.getStatusCode(), body.length());
        try (OutputStream to = httpExchange.getResponseBody()) {
            to.write(body.getBytes());
        }
    }

    private void handleRetrieveArchiveJob(HttpExchange httpExchange, String vault, JSONObject job) throws IOException{
        String blobName = job.getString("ArchiveId");
        Blob blob = proxy.getBlobStore().getBlob(vault, blobName);
        if (blob == null) {
            Util.sendBadRequest(httpExchange);
            return;
        }
        long size = blob.getMetadata().getSize();
        httpExchange.getResponseHeaders().put("Content-Length", ImmutableList.of(Long.toString(size)));
        httpExchange.getResponseHeaders().put("x-amz-sha256-tree-hash", ImmutableList.of("deadbeef"));
        httpExchange.sendResponseHeaders(Response.Status.OK.getStatusCode(), size);
        try (InputStream from = blob.getPayload().openStream(); OutputStream to = httpExchange.getResponseBody()){
            ByteStreams.copy(from, to);
        }
    }
}
