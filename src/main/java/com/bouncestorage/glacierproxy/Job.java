package com.bouncestorage.glacierproxy;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.ws.rs.core.Response;

import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.common.io.ByteStreams;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sun.net.httpserver.HttpExchange;

public class Job extends BaseRequestHandler {
    private static final List<String> JOB_TYPES = ImmutableList.of("archive-retrieval", "inventory-retrieval");
    private static final Logger logger = LoggerFactory.getLogger(Job.class);

    public Job(GlacierProxy proxy) {
        super(proxy);
    }

    @Override
    public void handlePost(HttpExchange request, Map<String, String> parameters) throws IOException {
        JsonParser jsonParser = new JsonParser();
        JsonObject object = jsonParser.parse(new InputStreamReader(request.getRequestBody())).getAsJsonObject();
        String jobType = object.get("Type").getAsString();
        if (jobType == null || !JOB_TYPES.contains(jobType)) {
            logger.warn("Invalid job type {}", object.get("Type"));
            Util.sendBadRequest(String.format("Invalid job type %s", jobType), request);
            return;
        }

        String vault = parameters.get("vault");
        if (!proxy.getBlobStore().containerExists(vault)) {
            logger.warn("POST job: vault {} does not exist", vault);
            Util.sendNotFound("vault", vault, request);
        }

        if (jobType.equals("archive-retrieval")) {
            String blobName = object.get("ArchiveId").getAsString();
            if (blobName == null) {
                Util.sendBadRequest("Missing archive ID", request);
                return;
            }
            if (!proxy.getBlobStore().blobExists(parameters.get("vault"), blobName)) {
                logger.warn("POST Archive retrieval job: archive does not exist {}/{}", vault, blobName);
                Util.sendNotFound("archive", blobName, request);
                return;
            }
        }

        UUID jobId = proxy.addJob(parameters.get("vault"), object);
        request.getResponseHeaders().put("x-amz-job-id", ImmutableList.of(jobId.toString()));
        request.getResponseHeaders().put("Location", ImmutableList.of(String.format("/%s/vaults/%s/jobs/%s",
                parameters.get("account"), parameters.get("vault"), jobId.toString())));
        logger.debug("Created {} job: {}", jobType, jobId);
        request.sendResponseHeaders(Response.Status.ACCEPTED.getStatusCode(), -1);
    }

    @Override
    public void handleGet(HttpExchange request, Map<String, String> parameters) throws IOException {
        // Differentiate between List jobs, describe job, and get job output
        String path = request.getRequestURI().getPath();
        if (parameters.get("job") != null) {
            String vault = parameters.get("vault");
            String jobId = parameters.get("job");
            JsonObject jobRequest = proxy.getJob(vault, UUID.fromString(jobId));
            if (jobRequest == null) {
                logger.debug("Job {} does not exist", jobId);
                Util.sendNotFound("job", jobId, request);
                return;
            }
            if (path.endsWith("output")) {
                if (jobRequest.get("Type").equals("archive-retrieval")) {
                    handleRetrieveArchiveJob(request, vault, jobRequest);
                    return;
                } else {
                    handleRetrieveInventoryJob(request, parameters, jobRequest);
                    return;
                }
            } else if (parameters.get("job") != null && path.endsWith(parameters.get("job"))) {
                handleDescribeJob(request, parameters, jobRequest);
            }
        } else if (path.endsWith("jobs")) {
            handleListJobs(request, parameters);
        } else {
            logger.warn("Invalid request {}", path);
            Util.sendBadRequest("Invalid request path", request);
        }
    }

    private void handleDescribeJob(HttpExchange httpExchange, Map<String, String> parameters, JsonObject jobRequest)
            throws IOException {
        JsonObject response;
        if (jobRequest.get("Type").getAsString().equals("archive-retrieval")) {
            response = handleDescribeRetrieveArchive(parameters, jobRequest);
        } else if (jobRequest.get("Type").getAsString().equals("inventory-retrieval")) {
            response = handleDescribeRetrieveInventory(parameters, jobRequest);
        } else {
            logger.warn("Invalid request {}", httpExchange.getRequestURI().getPath());
            Util.sendBadRequest("Invalid service path", httpExchange);
            return;
        }
        logger.debug("Describe job {}", parameters.get("job"));
        String vault = parameters.get("vault");
        response.add("Action", jobRequest.get("Type"));
        response.addProperty("Completed", true);
        response.add("CompletionDate", jobRequest.get("CreationDate"));
        response.add("CreationDate", jobRequest.get("CompletionDate"));
        response.add("JobDescription", jobRequest.get("JobDescription"));
        response.addProperty("JobId", parameters.get("job"));
        response.add("SNSTopic", null);
        response.addProperty("StatusCode", "Succeeded");
        response.addProperty("StatusMessage", "Succeeded");
        response.addProperty("VaultARN", Util.getARN(parameters.get("account"), vault));
        logger.debug("GET job: {}", response.toString());
        Util.sendJSON(httpExchange, Response.Status.OK, response);
    }

    private JsonObject handleDescribeRetrieveArchive(Map<String, String> parameters, JsonObject jobRequest) {
        JsonObject response = new JsonObject();
        String blobName = jobRequest.get("ArchiveId").getAsString();
        BlobMetadata metadata = proxy.getBlobStore().blobMetadata(parameters.get("vault"), blobName);
        response.add("ArchiveId", jobRequest.get("ArchiveId"));
        response.addProperty("ArchiveSize", metadata.getSize());
        response.addProperty("ArchiveSHA256TreeHash", "deadbeef");
        response.add("InventorySizeInBytes", null);
        response.addProperty("RetrievalByteRange", String.format("0-%d", metadata.getSize() - 1));
        response.add("SHA256TreeHash", null);
        return response;
    }

    private JsonObject handleDescribeRetrieveInventory(Map<String, String> parameters, JsonObject jobRequest) {
        JsonObject response = new JsonObject();
        response.add("ArchiveId", null);
        response.add("ArchiveSize", null);
        response.add("ArchiveSHA256TreeHash", null);
        response.addProperty("InventorySizeInBytes", -1);
        response.add("RetrievalByteRange", null);
        response.add("SHA256TreeHash", null);
        JsonObject inventoryParams = jobRequest.getAsJsonObject("InventoryRetrievalParameters");
        if (inventoryParams != null) {
            inventoryParams.addProperty("Format", "JSON");
            response.add("InventoryRetrievalParameters", inventoryParams);
        }
        return response;
    }

    private void handleListJobs(HttpExchange httpExchange, Map<String, String> parameters) throws IOException {
        Multimap<String, String> queryMap = Util.parseQuery(httpExchange.getRequestURI().getQuery());
        ListJobsOptions listJobsOptions;
        try {
            listJobsOptions = new ListJobsOptions(queryMap);
        } catch (IllegalArgumentException e) {
            logger.warn("Invalid list jobs argument {}", e.getMessage());
            Util.sendBadRequest(String.format("Invalid list jobs parameter: %s", e.getMessage()), httpExchange);
            return;
        }
        String vault = parameters.get("vault");
        JsonObject response = new JsonObject();
        // TODO: handle markers and > 1000 jobs
        response.add("Marker", null);
        JsonArray jsonJobs = new JsonArray();
        if (listJobsOptions.getCompleted() != null && !listJobsOptions.getCompleted()) {
            // All jobs are treated as immediately completed
            logger.debug("List jobs for {}: []", vault);
            response.add("JobList", jsonJobs);
            Util.sendJSON(httpExchange, Response.Status.OK, response);
            return;
        }

        if (listJobsOptions.getStatusCode() != null && !listJobsOptions.getStatusCode().equals("Succeeded")) {
            // All jobs are treated as immediately completed
            logger.debug("List jobs for {}: []", vault);
            response.add("JobList", jsonJobs);
            Util.sendJSON(httpExchange, Response.Status.OK, response);
            return;
        }

        Map<UUID, JsonObject> jobs = proxy.getVaultJobs(vault);
        jobs.forEach((uuid, json) -> {
            JsonObject jobObject = new JsonObject();
            jobObject.addProperty("Completed", true);
            jobObject.add("CreationDate", json.get("CreationDate"));
            jobObject.add("CompletionDate", json.get("CompletionDate"));
            jobObject.addProperty("StatusCode", "Succeeded");
            jobObject.addProperty("StatusMessage", "Succeeded");
            jobObject.addProperty("VaultARN", Util.getARN(parameters.get("account"), vault));
            jobObject.addProperty("JobId", uuid.toString());
            jobObject.add("JobDescription", json.get("JobDescription"));
            jobObject.add("SNSTopic", json.get("SNSTopic"));
            if (json.get("Type").equals("archive-retrieval")) {
                jobObject.addProperty("Action", "ArchiveRetrieval");
                jobObject.add("ArchiveId", json.get("ArchiveId"));
                BlobMetadata meta = proxy.getBlobStore().blobMetadata(vault, json.get("ArchiveId").getAsString());
                jobObject.addProperty("ArchiveSizeInBytes", meta.getSize());
                jobObject.addProperty("RetrievalByteRange", String.format("0-%d", meta.getSize()));
            } else {
                jobObject.add("SHA256TreeHash", null);
                jobObject.addProperty("Action", "InventoryRetriveal");
                jobObject.add("ArchiveSHA256TreeHash", null);
                jobObject.addProperty("InventorySizeInBytes", -1);
                jobObject.add("RetrievalByteRange", null);
                JsonObject inventoryParams = json.getAsJsonObject("InventoryRetrievalParameters");
                if (inventoryParams != null) {
                    inventoryParams.addProperty("Format", "JSON");
                    jobObject.add("InventoryRetrievalParameters", inventoryParams);
                }
            }
            jsonJobs.add(jobObject);
        });
        response.add("JobList", jsonJobs);
        logger.debug("List jobs for {}: {}", vault, response.toString());
        Util.sendJSON(httpExchange, Response.Status.OK, response);
    }

    private void handleRetrieveInventoryJob(HttpExchange httpExchange, Map<String, String> parameters, JsonObject job)
            throws IOException {
        String vault = parameters.get("vault");
        JsonObject response = new JsonObject();
        response.addProperty("VaultARN", Util.getARN(parameters.get("account"), vault));
        response.addProperty("InventoryDate", Util.getTimeStamp(null));
        JsonArray archives = new JsonArray();

        // TODO: support pagination
        proxy.getBlobStore().list(vault).forEach(sm -> {
            JsonObject archive = new JsonObject();
            archive.addProperty("ArchiveId", sm.getName());
            archive.addProperty("CreationDate", Util.getTimeStamp(sm.getLastModified()));
            archive.addProperty("Size", sm.getSize());
            archives.add(archive);
        });
        response.add("ArchiveList", archives);

        logger.debug("Job {}: Retrieve archive list for {}", parameters.get("job"), vault);
        Util.sendJSON(httpExchange, Response.Status.OK, response);
    }

    private void handleRetrieveArchiveJob(HttpExchange httpExchange, String vault, JsonObject job) throws IOException{
        String blobName = job.get("ArchiveId").getAsString();
        Blob blob = proxy.getBlobStore().getBlob(vault, blobName);
        if (blob == null) {
            Util.sendNotFound("archive", blobName, httpExchange);
            return;
        }
        logger.debug("Job {}: Retrieve archive {}/{}", job.get("JobId"), vault, blobName);
        long size = blob.getMetadata().getSize();
        httpExchange.getResponseHeaders().put("Content-Length", ImmutableList.of(Long.toString(size)));
        httpExchange.getResponseHeaders().put("x-amz-sha256-tree-hash", ImmutableList.of("deadbeef"));
        httpExchange.sendResponseHeaders(Response.Status.OK.getStatusCode(), size);
        try (InputStream from = blob.getPayload().openStream()){
            ByteStreams.copy(from, httpExchange.getResponseBody());
        }
    }

    private static class ListJobsOptions {
        private static final List<String> COMPLETED_OPTIONS = ImmutableList.of("true", "false");
        private static final int MAX_LIMIT = 1000;
        private static final List<String> STATUS_CODES = ImmutableList.of("Succeeded", "InProgress", "Failed");

        private Boolean completed;
        private Integer limit;
        private String marker;
        private String statusCode;

        public ListJobsOptions(Multimap<String, String> queryParams) {
            if (queryParams.containsKey("completed")) {
                String completedString = queryParams.get("completed").iterator().next();
                if (!COMPLETED_OPTIONS.contains(completedString)) {
                    throw new IllegalArgumentException("Invalid completed value");
                }
                if (completedString.equals("true")) {
                    completed = true;
                } else {
                    completed = false;
                }
            }

            if (queryParams.containsKey("limit")) {
                limit = Integer.parseInt(queryParams.get("limit").iterator().next());
                if (limit > MAX_LIMIT || limit < 1) {
                    throw new IllegalArgumentException("Invalid limit value");
                }
            }

            if (queryParams.containsKey("statuscode")) {
                statusCode = queryParams.get("statuscode").iterator().next();
                if (!STATUS_CODES.contains(statusCode)) {
                    throw new IllegalArgumentException("Invalid statuscode value");
                }
            }

            if (queryParams.containsKey("marker")) {
                marker = queryParams.get("marker").iterator().next();
            }
        }

        Boolean getCompleted() {
            return completed;
        }

        Integer getLimit() {
            return limit;
        }

        String getMarker() {
            return marker;
        }

        String getStatusCode() {
            return statusCode;
        }
    }
}
