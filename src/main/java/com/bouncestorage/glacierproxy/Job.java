package com.bouncestorage.glacierproxy;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.ws.rs.core.Response;

import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.common.io.ByteStreams;
import com.sun.net.httpserver.HttpExchange;

public class Job extends BaseRequestHandler {
    private static final List<String> JOB_TYPES = ImmutableList.of("archive-retrieval", "inventory-retrieval");
    private static final Logger logger = LoggerFactory.getLogger(Job.class);

    public Job(GlacierProxy proxy) {
        super(proxy);
    }

    @Override
    public void handlePost(HttpExchange request, Map<String, String> parameters) throws IOException {
        JSONObject object = new JSONObject(new JSONTokener(request.getRequestBody()));
        String jobType = object.optString("Type");
        if (jobType == null || !JOB_TYPES.contains(jobType)) {
            logger.warn("Invalid job type {}", object.getString("Type"));
            Util.sendBadRequest(request);
            return;
        }

        String vault = parameters.get("vault");
        if (!proxy.getBlobStore().containerExists(vault)) {
            logger.warn("POST job: vault {} does not exist", vault);
            Util.sendBadRequest(request);
        }

        if (jobType.equals("archive-retrieval")) {
            String blobName = object.getString("ArchiveId");
            if (blobName == null) {
                Util.sendBadRequest(request);
                return;
            }
            if (!proxy.getBlobStore().blobExists(parameters.get("vault"), blobName)) {
                logger.warn("POST Archive retrieval job: archive does not exist {}/{}", vault, blobName);
                Util.sendBadRequest(request);
                return;
            }
        }

        UUID jobId = UUID.randomUUID();
        proxy.addJob(parameters.get("vault"), jobId, object);
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
            JSONObject jobRequest = proxy.getJob(vault, UUID.fromString(parameters.get("job")));
            if (jobRequest == null) {
                logger.debug("Job {} does not exist", parameters.get("job"));
                Util.sendNotFound(request);
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
            Util.sendBadRequest(request);
        }
    }

    private void handleDescribeJob(HttpExchange httpExchange, Map<String, String> parameters, JSONObject jobRequest)
            throws IOException {
        JSONObject response = null;
        if (jobRequest.get("Type").equals("archive-retrieval")) {
            response = handleDescribeRetrieveArchive(parameters, jobRequest);
        } else if (jobRequest.get("Type").equals("inventory-retrieval")) {
            response = handleDescribeRetrieveInventory(parameters, jobRequest);
        }
        logger.debug("Describe job {}", parameters.get("job"));
        String vault = parameters.get("vault");
        response.put("Action", jobRequest.get("Type"));
        response.put("Completed", true);
        response.put("CompletionDate", Util.getTimeStamp());
        response.put("CreationDate", Util.getTimeStamp());
        response.put("JobDescription", jobRequest.opt("JobDescription"));
        response.put("JobId", parameters.get("job"));
        response.put("SNSTopic", JSONObject.NULL);
        response.put("StatusCode", "Succeeded");
        response.put("StatusMessage", "Succeeded");
        response.put("VaultARN", Util.getARN(parameters.get("account"), vault));
        Util.sendJSON(httpExchange, Response.Status.OK, response);
    }

    private JSONObject handleDescribeRetrieveArchive(Map<String, String> parameters, JSONObject jobRequest) {
        JSONObject response = new JSONObject();
        String blobName = (String) jobRequest.get("ArchiveId");
        BlobMetadata metadata = proxy.getBlobStore().blobMetadata(parameters.get("vault"), blobName);
        response.put("ArchiveId", jobRequest.get("ArchiveId"));
        response.put("ArchiveSize", metadata.getSize());
        response.put("ArchiveSHA256TreeHash", "deadbeef");
        response.put("InventorySizeInBytes", JSONObject.NULL);
        response.put("RetrievalByteRange", metadata.getSize()-1);
        response.put("SHA256TreeHash", JSONObject.NULL);
        return response;
    }

    private JSONObject handleDescribeRetrieveInventory(Map<String, String> parameters, JSONObject jobRequest) {
        JSONObject response = new JSONObject();
        response.put("ArchiveId", JSONObject.NULL);
        response.put("ArchiveSize", JSONObject.NULL);
        response.put("ArchiveSHA256TreeHash", JSONObject.NULL);
        response.put("InventorySizeInBytes", -1);
        response.put("RetrievalByteRange", JSONObject.NULL);
        response.put("SHA256TreeHash", JSONObject.NULL);
        JSONObject inventoryParams = jobRequest.optJSONObject("InventoryRetrievalParameters");
        if (inventoryParams != null) {
            inventoryParams.put("Format", jobRequest.opt("Format"));
            response.put("InventoryRetrievalParameters", inventoryParams);
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
            Util.sendBadRequest(httpExchange);
            return;
        }
        String vault = parameters.get("vault");
        JSONObject response = new JSONObject();
        // TODO: handle markers and > 1000 jobs
        response.put("Marker", JSONObject.NULL);
        JSONArray jsonJobs = new JSONArray();
        if (listJobsOptions.getCompleted() != null && !listJobsOptions.getCompleted()) {
            // All jobs are treated as immediately completed
            logger.debug("List jobs for {}: []", vault);
            response.put("JobList", jsonJobs);
            Util.sendJSON(httpExchange, Response.Status.OK, response);
            return;
        }

        if (listJobsOptions.getStatusCode() != null && !listJobsOptions.getStatusCode().equals("Succeeded")) {
            // All jobs are treated as immediately completed
            logger.debug("List jobs for {}: []", vault);
            response.put("JobList", jsonJobs);
            Util.sendJSON(httpExchange, Response.Status.OK, response);
            return;
        }

        Map<UUID, JSONObject> jobs = proxy.getVaultJobs(vault);
        jobs.forEach((uuid, json) -> {
            JSONObject jobObject = new JSONObject();
            jobObject.put("Completed", true);
            jobObject.put("CompletionDate", Util.getTimeStamp());
            jobObject.put("StatusCode", "Succeeded");
            jobObject.put("StatusMessage", "Succeeded");
            jobObject.put("VaultARN", Util.getARN(parameters.get("account"), vault));
            jobObject.put("JobId", uuid.toString());
            jobObject.put("JobDescription", json.get("JobDescription"));
            jobObject.put("SNSTopic", json.get("SNSTopic"));
            jobObject.put("SHA256TreeHash", JSONObject.NULL);
            if (json.get("Type").equals("archive-retrieval")) {
                jobObject.put("Action", "ArchiveRetrieval");
                jobObject.put("ArchiveId", json.get("ArchiveId"));
                // TODO: populate the size
                jobObject.put("ArchiveSizeInBytes", -1);
                jobObject.put("ArchiveSHA256TreeHash", "deadbeef");
                jobObject.put("RetrievalByteRange", -1);
            } else {
                jobObject.put("Action", "InventoryRetriveal");
                jobObject.put("ArchiveSHA256TreeHash", JSONObject.NULL);
                jobObject.put("InventorySizeInBytes", -1);
                JSONObject inventoryParams = json.optJSONObject("InventoryRetrievalParameters");
                if (inventoryParams != null) {
                    inventoryParams.put("Format", "JSON");
                    jobObject.put("InventoryRetrievalParameters", inventoryParams);
                }
                jobObject.put("RetrievalByteRange", JSONObject.NULL);
            }
        });
        response.put("JobList", jsonJobs);
        logger.debug("List jobs for {}: {}", vault, response.toString(4));
        Util.sendJSON(httpExchange, Response.Status.OK, response);
    }

    private void handleRetrieveInventoryJob(HttpExchange httpExchange, Map<String, String> parameters, JSONObject job)
            throws IOException {
        String vault = parameters.get("vault");
        JSONObject response = new JSONObject();
        response.put("VaultARN", Util.getARN(parameters.get("account"), vault));
        response.put("InventoryDate", new Date());
        JSONArray archives = new JSONArray();

        // TODO: support pagination
        proxy.getBlobStore().list(vault).forEach(sm -> {
            JSONObject archive = new JSONObject();
            archive.put("ArchiveId", sm.getName());
            archive.put("CreationDate", sm.getCreationDate());
            archive.put("Size", sm.getSize());
            archive.put("SHA256TreeHash", "deadbeef");
            archive.put("ArchiveDescription", "NA");
            archives.put(archive);
        });
        response.put("ArchiveList", archives);

        logger.debug("Job {}: Retrieve archive list for {}", parameters.get("job"), vault);
        Util.sendJSON(httpExchange, Response.Status.OK, response);
    }

    private void handleRetrieveArchiveJob(HttpExchange httpExchange, String vault, JSONObject job) throws IOException{
        String blobName = job.getString("ArchiveId");
        Blob blob = proxy.getBlobStore().getBlob(vault, blobName);
        if (blob == null) {
            Util.sendBadRequest(httpExchange);
            return;
        }
        logger.debug("Job {}: Retrieve archive {}/{}", job.get("JobId"), vault, blobName);
        long size = blob.getMetadata().getSize();
        httpExchange.getResponseHeaders().put("Content-Length", ImmutableList.of(Long.toString(size)));
        httpExchange.getResponseHeaders().put("x-amz-sha256-tree-hash", ImmutableList.of("deadbeef"));
        httpExchange.sendResponseHeaders(Response.Status.OK.getStatusCode(), size);
        try (InputStream from = blob.getPayload().openStream(); OutputStream to = httpExchange.getResponseBody()){
            ByteStreams.copy(from, to);
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
