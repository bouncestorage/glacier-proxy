package com.bouncestorage.glacierproxy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.ws.rs.core.Response;

import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.MultipartPart;
import org.jclouds.blobstore.domain.MultipartUpload;
import org.jclouds.io.Payload;
import org.jclouds.io.Payloads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.sun.net.httpserver.HttpExchange;

public class Multipart extends BaseRequestHandler {
    private static final Pattern CONTENT_RANGE_RE = Pattern.compile("bytes (?<start>[0-9]+)-(?<end>[0-9]+)/\\*");
    private static final ImmutableList<String> REQUIRED_PUT_HEADERS = ImmutableList.of("x-amz-content-sha256",
            "x-amz-sha256-tree-hash");
    private static final ImmutableList<String> REQUIRED_COMPLETE_HEADERS = ImmutableList.of("x-amz-archive-size",
            "x-amz-sha256-tree-hash");
    private static final Logger logger = LoggerFactory.getLogger(Multipart.class);

    public Multipart(GlacierProxy proxy) {
        super(proxy);
    }

    @Override
    public void handleGet(HttpExchange request, Map<String, String> params) throws IOException{
        String path = request.getRequestURI().getPath();
        if (path.endsWith("multipart-uploads")) {
            handleListUploads(request, params);
        } else if (params.containsKey("upload")) {
            handleListParts(request, params);
        } else {
            Util.sendBadRequest("Invalid service path", request);
        }
    }

    @Override
    public void handlePost(HttpExchange request, Map<String, String> params) throws IOException {
        if (request.getRequestURI().getPath().endsWith("multipart-uploads")) {
            handleCreateMultipartUpload(request, params);
        } else if (params.containsKey("upload")) {
            handleCompleteMultipartUpload(request, params);
        } else {
            Util.sendBadRequest("Invalid service path", request);
        }
    }

    @Override
    public void handleDelete(HttpExchange request, Map<String, String> params) throws IOException {
        if (!params.containsKey("upload")) {
            Util.sendNotFound("multipart upload", params.get("upload"), request);
            return;
        }

        Upload upload = proxy.getUpload(params.get("vault"), UUID.fromString(params.get("upload")));
        if (upload == null) {
            Util.sendNotFound("Multipart upload", params.get("upload"), request);
            return;
        }

        proxy.getBlobStore().abortMultipartUpload(upload.jcloudsUpload);
        request.sendResponseHeaders(Response.Status.NO_CONTENT.getStatusCode(), -1);
    }

    @Override
    public void handlePut(HttpExchange request, Map<String, String> params) throws IOException {
        if (!params.containsKey("upload")) {
            Util.sendNotFound("Upload", params.get("upload"), request);
            return;
        }

        for (String header : REQUIRED_PUT_HEADERS) {
            if (!request.getRequestHeaders().containsKey(header)) {
                Util.sendBadRequest(String.format("Header %s not found", header), request);
                return;
            }
        }

        UUID uploadId = UUID.fromString(params.get("upload"));
        Upload upload = proxy.getUpload(params.get("vault"), uploadId);
        if (upload == null) {
            Util.sendNotFound("Upload", params.get("upload"), request);
            return;
        }
        Matcher rangeMatcher = CONTENT_RANGE_RE.matcher(request.getRequestHeaders().getFirst("Content-Range"));
        if (!rangeMatcher.matches()) {
            Util.sendBadRequest("Invalid content range", request);
            return;
        }
        String startRangeString = rangeMatcher.group("start");
        String endRangeString = rangeMatcher.group("end");
        long start;
        long end;
        try {
            start = Long.parseLong(startRangeString);
            end = Long.parseLong(endRangeString);
        } catch (NumberFormatException e) {
            Util.sendBadRequest("Invalid range", request);
            return;
        }
        long size = end - start + 1;
        if (size > upload.partSize) {
            Util.sendBadRequest(String.format("Part size must be smaller than %s", upload.partSize), request);
            return;
        }

        if (start % upload.partSize != 0) {
            Util.sendBadRequest(String.format("Starting part range does not align %d", start), request);
            return;
        }

        if (upload.parts.size() > 10000) {
            Util.sendBadRequest("Cannot have more than 10000 parts", request);
            return;
        }

        // parts are 1-indexed
        int partNumber = (int) (start/upload.partSize + 1);

        Payload payload = Payloads.newInputStreamPayload(request.getRequestBody());
        MultipartPart uploadedPart = proxy.getBlobStore().uploadMultipartPart(upload.jcloudsUpload, partNumber,
                payload);
        if (uploadedPart == null) {
            Util.sendServerError("Failed to save the part", request);
            return;
        }
        String sha256TreeHash = request.getRequestHeaders().getFirst("x-amz-sha256-tree-hash");
        upload.parts.add(new UploadPart(sha256TreeHash, size));

        request.getResponseHeaders().put("x-amz-sha256-tree-hash",
                request.getRequestHeaders().get("x-amz-sha256-tree-hash"));
        request.sendResponseHeaders(Response.Status.NO_CONTENT.getStatusCode(), -1);
    }

    private void handleListUploads(HttpExchange request, Map<String, String> params) throws IOException {
        String vault = params.get("vault");
        if (!proxy.getBlobStore().containerExists(vault)) {
            Util.sendNotFound("vault", vault, request);
            return;
        }

        // TODO: implement pagination
        JsonObject response = new JsonObject();
        response.add("Marker", null);
        JsonArray uploadList = new JsonArray();
        Map<UUID, Upload> uploadMap = proxy.getUploads(vault);
        if (uploadMap != null) {
            uploadMap.entrySet().forEach(entry -> {
                JsonObject uploadJSON = entry.getValue().toJSON();
                uploadJSON.addProperty("MultipartUploadId", entry.getKey().toString());
                uploadJSON.addProperty("VaultARN", Util.getARN(params.get("account"), vault));
                uploadList.add(uploadJSON);
            });
        }
        response.add("UploadsList", uploadList);
        Util.sendJSON(request, Response.Status.OK, response);
    }

    private void handleListParts(HttpExchange request, Map<String, String> params) throws IOException {
        // TODO: we should list the parts from the blobstore; use the in-memory map for the time being
        // TODO: use the marker and limit
        String uploadIDParam = params.get("upload");
        UUID uploadID = retrieveUploadId(params);
        if (uploadID == null) {
            Util.sendNotFound("multipart upload", uploadIDParam, request);
            return;
        }

        String vault = params.get("vault");
        Upload upload = proxy.getUpload(vault, uploadID);
        if (upload == null) {
            Util.sendNotFound("multipart upload", uploadIDParam, request);
        }
        JsonObject response = new JsonObject();
        response.addProperty("ArchiveDescription", upload.description);
        response.addProperty("CreationDate", Util.getTimeStamp(upload.jcloudsUpload.blobMetadata().getCreationDate()));
        response.add("Marker", null);
        response.addProperty("MultipartUploadId", uploadIDParam);
        response.addProperty("PartSizeInBytes", upload.partSize);
        JsonArray parts = new JsonArray();
        long rangeStart = 0;
        for (UploadPart part : upload.parts) {
            JsonObject jsonPart = new JsonObject();
            jsonPart.addProperty("SHA256TreeHash", part.getSha256TreeHash());
            jsonPart.addProperty("RangeInBytes", String.format("%d-%d", rangeStart, rangeStart + part.getSize()-1));
            rangeStart += part.getSize();
            parts.add(jsonPart);
        }
        response.add("Parts", parts);
        response.addProperty("VaultARN", Util.getARN(params.get("account"), vault));
        Util.sendJSON(request, Response.Status.OK, response);
    }

    private void handleCompleteMultipartUpload(HttpExchange request, Map<String, String> params) throws IOException {
        String uploadIDParam = params.get("upload");
        UUID uploadID = retrieveUploadId(params);
        if (uploadID == null) {
            Util.sendNotFound("multipart upload", uploadIDParam, request);
            return;
        }
        String vault = params.get("vault");
        Upload upload = proxy.getUpload(vault, uploadID);
        if (upload == null) {
            Util.sendNotFound("multipart upload", uploadIDParam, request);
            return;
        }
        for (String header : REQUIRED_COMPLETE_HEADERS) {
            if (!request.getRequestHeaders().containsKey(header)) {
                Util.sendBadRequest(String.format("Missing header %s", header), request);
                return;
            }
        }
        Long requestUploadSize;
        try {
            requestUploadSize = Long.parseLong(request.getRequestHeaders().getFirst("x-amz-archive-size"));
        } catch (NumberFormatException e) {
            Util.sendBadRequest(String.format("Improper size %s",
                    request.getRequestHeaders().getFirst("x-amz-archive-size")), request);
            return;
        }

        long uploadedSize = 0;
        for(int i = 0; i < upload.parts.size(); i++) {
            UploadPart uploadPart = upload.parts.get(i);
            uploadedSize += uploadPart.getSize();
            if (i < upload.parts.size()-1 && uploadPart.getSize() != upload.partSize) {
                Util.sendBadRequest(String.format("Uploaded part is smaller than part size and is not last: %d",
                        uploadPart.getSize()), request);
                return;
            }
        }
        if (uploadedSize != requestUploadSize) {
            Util.sendBadRequest(String.format("Uploaded size does not match the header: %d %d", uploadedSize,
                    requestUploadSize), request);
            return;
        }
        List<MultipartPart> parts = proxy.getBlobStore().listMultipartUpload(upload.jcloudsUpload);
        String etag = proxy.getBlobStore().completeMultipartUpload(upload.jcloudsUpload, parts);
        if (etag == null){
            Util.sendServerError(String.format("Failed to completed upload %s", uploadID.toString()), request);
            return;
        }

        proxy.completeUpload(vault, uploadID);
        request.getResponseHeaders().put("x-amz-archive-id", ImmutableList.of(upload.jcloudsUpload.blobName()));
        request.getResponseHeaders().put("Location", ImmutableList.of(
                Util.getArchiveLocation(params.get("account"), vault, uploadIDParam)));
        request.sendResponseHeaders(Response.Status.CREATED.getStatusCode(), -1);
    }

    private void handleCreateMultipartUpload(HttpExchange request, Map<String, String> params) throws IOException {
        String vault = params.get("vault");
        if (!proxy.getBlobStore().containerExists(vault)) {
            Util.sendNotFound("Vault", vault, request);
            return;
        }

        String partSizeHeader = request.getRequestHeaders().getFirst("x-amz-part-size");
        if (partSizeHeader == null) {
            Util.sendBadRequest("Missing part size header (x-amz-part-size)", request);
            return;
        }
        long partSize;
        try {
            partSize = Long.parseLong(partSizeHeader);
        } catch (NumberFormatException e) {
            Util.sendBadRequest("Invalid part size", request);
            return;
        }
        UUID archiveId = UUID.randomUUID();
        Blob mpuBlob = proxy.getBlobStore().blobBuilder(archiveId.toString()).build();
        MultipartUpload mpu = proxy.getBlobStore().initiateMultipartUpload(vault, mpuBlob.getMetadata());
        if (mpu == null) {
            Util.sendServerError("Failed to create an upload", request);
            return;
        }
        String description = request.getRequestHeaders().getFirst("x-amz-archive-description");
        Upload upload = new Upload(partSize, description, mpu);
        UUID uploadId = proxy.createMultipartUpload(vault, upload);
        request.getResponseHeaders().put("x-amz-multipart-upload-id", ImmutableList.of(uploadId.toString()));
        request.getResponseHeaders().put("Location", ImmutableList.of(
                Util.getMultipartLocation(params.get("account"), vault, uploadId.toString())));
        request.sendResponseHeaders(Response.Status.CREATED.getStatusCode(), -1);
    }

    private UUID retrieveUploadId(Map<String, String> params) throws IOException {
        UUID uploadID = null;
        try {
            uploadID = UUID.fromString(params.get("upload"));
        } catch (IllegalArgumentException e) {
            return uploadID;
        }
        return uploadID;
    }

    public static class Upload {
        String description;
        long partSize;
        MultipartUpload jcloudsUpload;
        // required by the complete multipart method
        boolean smallerPartReceived;
        ArrayList<UploadPart> parts;

        public boolean wasSmallerPartReceived() {
            return smallerPartReceived;
        }

        public Upload(long partSize, String archiveDescription, MultipartUpload jcloudsUpload) {
            description = archiveDescription;
            this.partSize = partSize;
            this.jcloudsUpload = jcloudsUpload;
            parts = new ArrayList<>();
        }

        JsonObject toJSON() {
            JsonObject response = new JsonObject();
            response.addProperty("CreationDate", Util.getTimeStamp(jcloudsUpload.blobMetadata().getCreationDate()));
            if (description == null) {
                response.add("ArchiveDescription", null);
            } else {
                response.addProperty("ArchiveDescription", description);
            }
            response.addProperty("PartSizeInBytes", partSize);
            return response;
        }
    }

    public static class UploadPart {
        String sha256TreeHash;
        long size;

        UploadPart(String sha256TreeHash, long size) {
            this.sha256TreeHash = sha256TreeHash;
            this.size = size;
        }

        String getSha256TreeHash() {
            return sha256TreeHash;
        }

        long getSize() {
            return size;
        }
    }
}
