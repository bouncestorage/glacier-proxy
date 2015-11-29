package com.bouncestorage.glacierproxy;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.ContainerNotFoundException;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.options.ListContainerOptions;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.io.ByteSource;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sun.net.httpserver.HttpExchange;

public class Util {
    private static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ssZ";
    private static final String METADATA_SUFFIX = "_metadata";

    public static String putMetadataBlob(Map<String, String> metadata, BlobStore blobStore, String vault,
                                         String archiveName) {
        Gson gson = new Gson();
        String jsonString = gson.toJson(metadata);
        Blob metadataBlob = blobStore.blobBuilder(getMetadataBlobName(archiveName))
                .payload(ByteSource.wrap(jsonString.getBytes())).build();
        return blobStore.putBlob(vault, metadataBlob);
    }

    public static Multimap<String, String> parseQuery(String query) {
        Multimap<String, String> map = LinkedHashMultimap.create();
        if (query == null) {
            return map;
        }
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

    public static String getTimeStamp(Date date) {
        SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
        format.setTimeZone(TimeZone.getTimeZone("GMT"));
        if (date == null) {
            return format.format(new Date());
        }
        return format.format(date);
    }

    public static void sendJSON(HttpExchange httpExchange, Response.Status code, JsonObject json) throws IOException {
        String jsonResponse = json.toString();
        httpExchange.getResponseHeaders().put("Content-type", ImmutableList.of(MediaType.APPLICATION_JSON));
        httpExchange.sendResponseHeaders(code.getStatusCode(), jsonResponse.length());
        httpExchange.getResponseBody().write(jsonResponse.getBytes(StandardCharsets.UTF_8));
    }

    public static void sendBadRequest(String message, HttpExchange httpExchange) throws IOException {
        JsonObject response = new JsonObject();
        response.addProperty("code", "BadRequest");
        response.addProperty("message", message);
        response.addProperty("type", "client");
        sendJSON(httpExchange, Response.Status.BAD_REQUEST, response);
    }

    public static void sendServerError(String message, HttpExchange httpExchange) throws IOException {
        JsonObject response = new JsonObject();
        response.addProperty("code", "ServiceUnavailableException");
        response.addProperty("message", message);
        response.addProperty("type", "server");
        sendJSON(httpExchange, Response.Status.SERVICE_UNAVAILABLE, response);
    }

    public static void sendNotFound(String resourceType, String resourceId, HttpExchange httpExchange) throws
            IOException {
        JsonObject response = new JsonObject();
        response.addProperty("code", "ResourceNotFoundException");
        response.addProperty("message", String.format("The %s was not found: %s", resourceType, resourceId));
        response.addProperty("type", "client");
        sendJSON(httpExchange, Response.Status.NOT_FOUND, response);
    }

    public static String getARN(String accountId, String vault) {
        return String.format("arn:aws:glacier::%s:vaults/%s", accountId, vault);
    }

    public static String getMultipartLocation(String accountId, String vault, String archive) {
        return String.format("%s/vaults/%s/multipart-uploads/%s", accountId, vault, archive);
    }

    public static String getArchiveLocation(String accountId, String vault, String archive) {
        return String.format("%s/vaults/%s/archives/%s", accountId, vault, archive);
    }

    public static String getMetadataBlobName(String archiveName) {
        return archiveName + METADATA_SUFFIX;
    }

    public static boolean isMetadataBlob(String blobName) {
        return blobName.endsWith(METADATA_SUFFIX);
    }

    public static JsonObject getMetadata(BlobStore blobStore, String vault, String name) {
        Blob blob;
        try {
            blob = blobStore.getBlob(vault, Util.getMetadataBlobName(name));
        } catch (ContainerNotFoundException cnfe) {
            return new JsonObject();
        }
        if (blob == null) {
            return new JsonObject();
        }
        try (InputStream out = blob.getPayload().openStream()) {
            JsonParser parser = new JsonParser();
            return parser.parse(new InputStreamReader(out)).getAsJsonObject();
        } catch (IOException io) {
            return new JsonObject();
        }
    }
}
