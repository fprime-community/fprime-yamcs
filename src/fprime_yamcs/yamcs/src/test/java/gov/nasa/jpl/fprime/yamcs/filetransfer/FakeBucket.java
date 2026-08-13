package gov.nasa.jpl.fprime.yamcs.filetransfer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

import org.yamcs.buckets.Bucket;
import org.yamcs.buckets.BucketLocation;
import org.yamcs.buckets.BucketProperties;
import org.yamcs.buckets.ObjectProperties;

/** In-memory {@link Bucket} for handler unit tests. */
class FakeBucket implements Bucket {

    final Map<String, byte[]> objects = new HashMap<>();

    /** When true, every put fails — for storage failure-path tests. */
    boolean failPuts;

    /** When true, every get fails — for uplink read failure-path tests. */
    boolean failGets;

    @Override
    public BucketLocation getLocation() {
        return null;
    }

    @Override
    public String getName() {
        return "fake";
    }

    @Override
    public CompletableFuture<BucketProperties> getPropertiesAsync() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void setMaxSize(long maxSize) {
    }

    @Override
    public void setMaxObjects(int maxObjects) {
    }

    @Override
    public CompletableFuture<List<ObjectProperties>> listObjectsAsync(
            String prefix, Predicate<ObjectProperties> p) {
        return CompletableFuture.completedFuture(List.of());
    }

    @Override
    public CompletableFuture<Void> putObjectAsync(
            String objectName, String contentType, Map<String, String> metadata, byte[] data) {
        if (failPuts) {
            return CompletableFuture.failedFuture(new java.io.IOException("disk full"));
        }
        objects.put(objectName, data.clone());
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<byte[]> getObjectAsync(String objectName) {
        if (failGets) {
            return CompletableFuture.failedFuture(new java.io.IOException("read error"));
        }
        return CompletableFuture.completedFuture(objects.get(objectName));
    }

    @Override
    public CompletableFuture<Void> deleteObjectAsync(String objectName) {
        objects.remove(objectName);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<ObjectProperties> findObjectAsync(String objectName) {
        return CompletableFuture.completedFuture(null);
    }
}
