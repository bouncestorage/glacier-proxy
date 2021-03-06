package com.bouncestorage.glacierproxy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonObject;
import com.sun.net.httpserver.HttpServer;

public class GlacierProxy {
    private static final Logger logger = LoggerFactory.getLogger(GlacierProxy.class);

    private HttpServer server;
    private BlobStore blobStore;
    private Map<String, Map<UUID, JsonObject>> jobMap;
    private Map<String, Map<UUID, Multipart.Upload>> partsMap;

    public void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress(8081), 0);
        server.createContext("/", new GlacierProxyHandler(this));
        server.setExecutor(null);
        server.start();
        BlobStoreContext context = ContextBuilder.newBuilder("transient").credentials("", "")
                .build(BlobStoreContext.class);
        blobStore = context.getBlobStore();
        jobMap = new ConcurrentHashMap<>();
        partsMap = new ConcurrentHashMap<>();
        logger.info("Proxy started");
    }

    public void stop() {
        server.stop(0);
    }

    public Vault getVault(Map<String, String> parameters) {
        return new Vault(this);
    }

    public Archive getArchive(Map<String, String> parameters) {
        return new Archive(this);
    }

    public Job getJobHandler(Map<String, String> parameters) {
        return new Job(this);
    }

    public Multipart getMultipartHandler(Map<String, String> parameters) {
        return new Multipart(this);
    }

    public JsonObject getJob(String vault, UUID jobId) {
        Map<UUID, JsonObject> map = jobMap.get(vault);
        if (map == null) {
            return null;
        }
        return map.get(jobId);
    }

    public Map<UUID, JsonObject> getVaultJobs(String vault) {
        return jobMap.get(vault);
    }

    public UUID addJob(String vault, JsonObject json) {
        UUID uuid = UUID.randomUUID();
        json.addProperty("CreationDate", Util.getTimeStamp(null));
        json.addProperty("CompletionDate", Util.getTimeStamp(null));
        if (!jobMap.containsKey(vault)) {
            jobMap.put(vault, new ConcurrentHashMap<>());
        }
        jobMap.get(vault).put(uuid, json);
        return uuid;
    }

    public UUID createMultipartUpload(String vault, Multipart.Upload upload) {
        if (!partsMap.containsKey(vault)) {
            partsMap.put(vault, new ConcurrentHashMap<>());
        }
        UUID uuid = UUID.randomUUID();
        partsMap.get(vault).put(uuid, upload);
        return uuid;
    }

    public Map<UUID, Multipart.Upload> getUploads(String vault) {
        return partsMap.get(vault);
    }

    public Multipart.Upload getUpload(String vault, UUID uploadId) {
        Map<UUID, Multipart.Upload> map = partsMap.get(vault);
        if (map == null) {
            return null;
        }
        return map.get(uploadId);
    }

    public void completeUpload(String vault, UUID uploadId) {
        Map<UUID, Multipart.Upload> map = partsMap.get(vault);
        if (map == null) {
            return;
        }
        map.remove(uploadId);
    }

    public BlobStore getBlobStore() {
        return blobStore;
    }
}
