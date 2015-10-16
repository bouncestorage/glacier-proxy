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
            entry.put("CreationDate", value.getCreationDate());
            entry.put("LastInvetoryDate", value.getCreationDate());
            entry.put("SizeInBytes", value.getSize());
            entry.put("NumberOfArchives", -1);
            entry.put("VaultName", value.getName());
            entry.put("VaultARN", value.getName());
            values.put(entry);
        }
        response.put("VaultList", values);
        logger.debug("List vaults: {}", response.toString(4));
        Util.sendJSON(httpExchange, Response.Status.OK, response);
    }

    @Override
    protected void handlePut(HttpExchange httpExchange, Map<String, String> parameters) throws IOException {
        if (!parameters.containsKey("vault") || parameters.get("vault") == null) {
            Util.sendBadRequest(httpExchange);
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
            Util.sendBadRequest(httpExchange);
            return;
        }

        String vault = parameters.get("vault");
        boolean result = proxy.getBlobStore().deleteContainerIfEmpty(vault);
        if (!result) {
            logger.warn("Failed to delete vault {}", vault);
            Util.sendBadRequest(httpExchange);
            return;
        }
        logger.debug("Deleted vault {}", vault);
        httpExchange.sendResponseHeaders(Response.Status.NO_CONTENT.getStatusCode(), -1);
    }

    private void handleDescribe(HttpExchange httpExchange, Map<String, String> parameters) throws IOException {
        String vaultName = parameters.get("vault");
        if (!proxy.getBlobStore().containerExists(vaultName)) {
            logger.debug("Describe vault: vault {} does not exist", vaultName);
            httpExchange.sendResponseHeaders(Response.Status.NOT_FOUND.getStatusCode(), -1);
            return;
        }
        logger.debug("Describe vault request for {}", vaultName);
        JSONObject vault = new JSONObject();
        vault.put("CreationDate", Util.getTimeStamp());
        vault.put("LastInventoryDate", Util.getTimeStamp());
        vault.put("SizeInBytes", -1);
        vault.put("VaultARN", Util.getARN(parameters.get("account"), parameters.get("vault")));
        vault.put("VaultName", parameters.get("vault"));
        Util.sendJSON(httpExchange, Response.Status.OK, vault);
    }
}
