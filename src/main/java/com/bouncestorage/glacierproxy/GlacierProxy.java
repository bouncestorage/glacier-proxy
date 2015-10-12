package com.bouncestorage.glacierproxy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.json.JSONObject;

import com.sun.net.httpserver.HttpServer;

public class GlacierProxy {
    private HttpServer server;
    private BlobStore blobStore;
    private Map<String, Map<UUID, JSONObject>> jobMap;

    public void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress(8000), 0);
        server.createContext("/", new GlacierProxyHandler(this));
        server.setExecutor(null);
        server.start();
        BlobStoreContext context = ContextBuilder.newBuilder("transient").credentials("", "")
                .build(BlobStoreContext.class);
        blobStore = context.getBlobStore();
        jobMap = new ConcurrentHashMap<>();
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

    public JSONObject getJob(String vault, UUID jobId) {
        if (!jobMap.containsKey(vault)) {
            return null;
        }
        if (!jobMap.get(vault).containsKey(jobId)) {
            return null;
        }
        return jobMap.get(vault).get(jobId);
    }

    public void addJob(String vault, UUID jobId, JSONObject json) {
        if (!jobMap.containsKey(vault)) {
            jobMap.put(vault, new ConcurrentHashMap<>());
        }
        jobMap.get(vault).put(jobId, json);
    }

    public BlobStore getBlobStore() {
        return blobStore;
    }
}
