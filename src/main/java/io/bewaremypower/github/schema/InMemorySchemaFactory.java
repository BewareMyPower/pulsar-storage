package io.bewaremypower.github.schema;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.schema.SchemaStorageFactory;
import org.apache.pulsar.common.protocol.schema.LatestVersion;
import org.apache.pulsar.common.protocol.schema.SchemaStorage;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.protocol.schema.StoredSchema;
import org.apache.pulsar.common.schema.LongSchemaVersion;
import org.jetbrains.annotations.NotNull;

public class InMemorySchemaFactory implements SchemaStorageFactory {

    @NotNull
    @Override
    public SchemaStorage create(PulsarService pulsar) {
        return new SchemaStorage() {

            final Map<String, List<byte[]>> schemaMap = new ConcurrentHashMap<>();

            @Override
            public CompletableFuture<SchemaVersion> put(String key, byte[] value, byte[] hash) {
                final var schemas = schemaMap.computeIfAbsent(key, __ -> new ArrayList<>());
                synchronized (schemas) {
                    final var version = schemas.size();
                    schemas.add(Arrays.copyOf(value, value.length));
                    return CompletableFuture.completedFuture(new LongSchemaVersion(version));
                }
            }

            @Override
            public CompletableFuture<StoredSchema> get(String key, SchemaVersion version) {
                final var schemas = schemaMap.get(key);
                if (schemas == null) {
                    return CompletableFuture.completedFuture(null);
                }
                synchronized (schemas) {
                    if (version instanceof LatestVersion) {
                        if (schemas.isEmpty()) {
                            return CompletableFuture.completedFuture(null);
                        } else {
                            final var index = schemas.size() - 1;
                            return CompletableFuture.completedFuture(new StoredSchema(schemas.get(index),
                                    new LongSchemaVersion(index)));
                        }
                    }
                    for (int i = 0; i < schemas.size(); i++) {
                        if (version.equals(new LongSchemaVersion(i))) {
                            return CompletableFuture.completedFuture(new StoredSchema(schemas.get(i), version));
                        }
                    }
                }
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public CompletableFuture<List<CompletableFuture<StoredSchema>>> getAll(String key) {
                final var schemas = schemaMap.get(key);
                if (schemas == null) {
                    // We should not return null, otherwise the default `put` method will encounter NPE
                    return CompletableFuture.completedFuture(List.of());
                }
                synchronized (schemas) {
                    final var list = new ArrayList<CompletableFuture<StoredSchema>>();
                    for (int i = 0; i < schemas.size(); i++) {
                        list.add(CompletableFuture.completedFuture(new StoredSchema(schemas.get(i),
                                new LongSchemaVersion(i))));
                    }
                    return CompletableFuture.completedFuture(list);
                }
            }

            @Override
            public CompletableFuture<SchemaVersion> delete(String key, boolean forcefully) {
                return delete(key);
            }

            @Override
            public CompletableFuture<SchemaVersion> delete(String key) {
                final var schemas = schemaMap.remove(key);
                return CompletableFuture.completedFuture(new LongSchemaVersion(schemas.size()));
            }

            @Override
            public SchemaVersion versionFromBytes(byte[] version) {
                ByteBuffer bb = ByteBuffer.wrap(version);
                return new LongSchemaVersion(bb.getLong());
            }

            @Override
            public void start() {
            }

            @Override
            public void close() {
            }
        };
    }
}
