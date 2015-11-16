package com.bouncestorage.glacierproxy;

import java.io.IOException;
import java.util.Map;

import javax.ws.rs.core.Response;

import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
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
        JSONObject response = new JSONObject();
        if (results.getNextMarker() == null) {
            response.put("Marker", JSONObject.NULL);
        } else {
            response.put("Marker", results.getNextMarker());
        }
        JSONArray values = new JSONArray();
        for (StorageMetadata value : results) {
            JSONObject entry = new JSONObject();
            entry.put("CreationDate", Util.getTimeStamp(value.getCreationDate()));
            entry.put("LastInventoryDate", Util.getTimeStamp(value.getCreationDate()));
            entry.put("SizeInBytes", -1);
            entry.put("NumberOfArchives", 0);
            entry.put("VaultName", value.getName());
            entry.put("VaultARN", Util.getARN(parameters.get("account"), value.getName()));
            values.put(entry);
        }
        response.put("VaultList", values);
        logger.debug("List vaults: {}", response.toString(4));
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
        JSONObject vault = new JSONObject();
        for (StorageMetadata container : proxy.getBlobStore().list()) {
            if (!container.getName().equals(vaultName)) {
                continue;
            }
            vault.put("CreationDate", Util.getTimeStamp(container.getCreationDate()));
            vault.put("LastInventoryDate", Util.getTimeStamp(container.getCreationDate()));
        }
        vault.put("SizeInBytes", -1);
        vault.put("VaultARN", Util.getARN(parameters.get("account"), parameters.get("vault")));
        vault.put("VaultName", parameters.get("vault"));
        Util.sendJSON(httpExchange, Response.Status.OK, vault);
    }
}
