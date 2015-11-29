package com.bouncestorage.glacierproxy;

import java.io.IOException;
import java.util.Map;

import javax.ws.rs.core.Response;

import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.sun.net.httpserver.HttpExchange;

public class Vault extends BaseRequestHandler {
    private static final Logger logger = LoggerFactory.getLogger(Vault.class);

    public Vault(GlacierProxy proxy) {
        super(proxy);
    }

    @Override
    protected void handleGet(HttpExchange httpExchange, Map<String, String> parameters) throws IOException {
        if (parameters.containsKey("vault") && parameters.get("vault") != null) {
            handleDescribe(httpExchange, parameters);
            return;
        }

        // TODO: Support pagination (markers) for vault listing
        PageSet<? extends StorageMetadata> results = proxy.getBlobStore().list();
        JsonObject response = new JsonObject();
        if (results.getNextMarker() == null) {
            response.add("Marker", null);
        } else {
            response.addProperty("Marker", results.getNextMarker());
        }
        JsonArray values = new JsonArray();
        for (StorageMetadata value : results) {
            JsonObject entry = new JsonObject();
            entry.addProperty("CreationDate", Util.getTimeStamp(value.getCreationDate()));
            entry.addProperty("LastInventoryDate", Util.getTimeStamp(value.getCreationDate()));
            entry.addProperty("SizeInBytes", -1);
            entry.addProperty("NumberOfArchives", 0);
            entry.addProperty("VaultName", value.getName());
            entry.addProperty("VaultARN", Util.getARN(parameters.get("account"), value.getName()));
            values.add(entry);
        }
        response.add("VaultList", values);
        logger.debug("List vaults: {}", response.toString());
        Util.sendJSON(httpExchange, Response.Status.OK, response);
    }

    @Override
    protected void handlePut(HttpExchange httpExchange, Map<String, String> parameters) throws IOException {
        if (!parameters.containsKey("vault") || parameters.get("vault") == null) {
            Util.sendBadRequest("Missing vault name", httpExchange);
            return;
        }
        String vault = parameters.get("vault");
        String account = parameters.get("account");

        proxy.getBlobStore().createContainerInLocation(null, vault);
        logger.debug("Created a new vault {}", vault);
        httpExchange.getResponseHeaders().put("Location", ImmutableList.of(String.format("/%s/vaults/%s", account,
                vault)));
        httpExchange.sendResponseHeaders(Response.Status.CREATED.getStatusCode(), -1);
    }

    @Override
    protected void handleDelete(HttpExchange httpExchange, Map<String, String> parameters) throws IOException{
        if (!parameters.containsKey("vault") || parameters.get("vault") == null) {
            logger.debug("Delete vault: invalid vault name");
            Util.sendBadRequest("Missing vault name", httpExchange);
            return;
        }

        String vault = parameters.get("vault");
        boolean result = proxy.getBlobStore().deleteContainerIfEmpty(vault);
        if (!result) {
            logger.warn("Failed to delete vault {}", vault);
            Util.sendBadRequest("Failed to delete vault. Vault possibly not empty", httpExchange);
            return;
        }
        logger.debug("Deleted vault {}", vault);
        httpExchange.sendResponseHeaders(Response.Status.NO_CONTENT.getStatusCode(), -1);
    }

    private void handleDescribe(HttpExchange httpExchange, Map<String, String> parameters) throws IOException {
        String vaultName = parameters.get("vault");
        if (!proxy.getBlobStore().containerExists(vaultName)) {
            logger.debug("Describe vault: vault {} does not exist", vaultName);
            Util.sendNotFound("vault", vaultName, httpExchange);
            return;
        }
        logger.debug("Describe vault request for {}", vaultName);
        JsonObject vault = new JsonObject();
        for (StorageMetadata container : proxy.getBlobStore().list()) {
            if (!container.getName().equals(vaultName)) {
                continue;
            }
            vault.addProperty("CreationDate", Util.getTimeStamp(container.getCreationDate()));
            vault.addProperty("LastInventoryDate", Util.getTimeStamp(container.getCreationDate()));
        }
        vault.addProperty("SizeInBytes", -1);
        vault.addProperty("VaultARN", Util.getARN(parameters.get("account"), parameters.get("vault")));
        vault.addProperty("VaultName", parameters.get("vault"));
        Util.sendJSON(httpExchange, Response.Status.OK, vault);
    }
}
