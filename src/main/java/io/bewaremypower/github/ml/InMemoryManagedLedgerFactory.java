package io.bewaremypower.github.ml;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryMXBean;
import org.apache.bookkeeper.mledger.ManagedLedgerInfo;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.ReadOnlyCursor;
import org.apache.bookkeeper.mledger.impl.cache.EntryCacheManager;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.PersistentOfflineTopicStats;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;

@RequiredArgsConstructor
public class InMemoryManagedLedgerFactory implements ManagedLedgerFactory {

    private final Map<String, ManagedLedger> streams = new ConcurrentHashMap<>();
    private final MetadataStoreExtended metadataStore;
    private final AtomicLong id = new AtomicLong(0);

    @Override
    public ManagedLedger open(String name) {
        return open(name, new ManagedLedgerConfig());
    }

    @Override
    public ManagedLedger open(String name, ManagedLedgerConfig config) {
        return streams.computeIfAbsent(name, __ -> new InMemoryManagedLedger(id.getAndIncrement(), name, config));
    }

    @Override
    public void asyncOpen(String name, AsyncCallbacks.OpenLedgerCallback callback, Object ctx) {
        callback.openLedgerComplete(open(name), ctx);
    }

    @Override
    public void asyncOpen(String name, ManagedLedgerConfig config, AsyncCallbacks.OpenLedgerCallback callback,
                          Supplier<CompletableFuture<Boolean>> mlOwnershipChecker, Object ctx) {
        callback.openLedgerComplete(open(name, config), ctx);
    }

    @Override
    public ReadOnlyCursor openReadOnlyCursor(String managedLedgerName, Position startPosition,
                                             ManagedLedgerConfig config) throws ManagedLedgerException {
        throw notSupported("openReadOnlyCursor");
    }

    @Override
    public void asyncOpenReadOnlyCursor(String managedLedgerName, Position startPosition, ManagedLedgerConfig config,
                                        AsyncCallbacks.OpenReadOnlyCursorCallback callback, Object ctx) {
        callback.openReadOnlyCursorFailed(notSupported("asyncOpenReadOnlyCursor"), ctx);
    }

    @Override
    public void asyncOpenReadOnlyManagedLedger(String managedLedgerName,
                                               AsyncCallbacks.OpenReadOnlyManagedLedgerCallback callback,
                                               ManagedLedgerConfig config, Object ctx) {
        callback.openReadOnlyManagedLedgerFailed(notSupported("asyncOpenReadOnlyManagedLedger"), ctx);
    }

    @Override
    public ManagedLedgerInfo getManagedLedgerInfo(String name) {
        return new ManagedLedgerInfo();
    }

    @Override
    public void asyncGetManagedLedgerInfo(String name, AsyncCallbacks.ManagedLedgerInfoCallback callback, Object ctx) {
        callback.getInfoComplete(new ManagedLedgerInfo(), ctx);
    }

    @Override
    public void delete(String name) {
        final var future = new CompletableFuture<Void>();
        asyncDelete(name, new AsyncCallbacks.DeleteLedgerCallback() {
            @Override
            public void deleteLedgerComplete(Object ctx) {
                future.complete(null);
            }

            @Override
            public void deleteLedgerFailed(ManagedLedgerException exception, Object ctx) {
                future.completeExceptionally(exception);
            }
        }, null);
        future.join();
    }

    @Override
    public void delete(String name, CompletableFuture<ManagedLedgerConfig> mlConfigFuture) throws InterruptedException, ManagedLedgerException {
        delete(name);
    }

    @Override
    public void asyncDelete(String name, AsyncCallbacks.DeleteLedgerCallback callback, Object ctx) {
        final var stream = streams.remove(name);
        if (stream == null) {
            callback.deleteLedgerComplete(ctx);
        } else {
            metadataStore.deleteIfExists("/managed-ledgers/" + name, Optional.empty())
                    .whenComplete((__, e) -> {
                        if (e == null) {
                            callback.deleteLedgerComplete(ctx);
                        } else {
                            callback.deleteLedgerFailed(ManagedLedgerException.getManagedLedgerException(e), ctx);
                        }
                    });
        }
    }

    @Override
    public void asyncDelete(String name, CompletableFuture<ManagedLedgerConfig> mlConfigFuture, AsyncCallbacks.DeleteLedgerCallback callback, Object ctx) {
        asyncDelete(name, callback, ctx);
    }

    @Override
    public void shutdown() {
    }

    @Override
    public CompletableFuture<Void> shutdownAsync() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Boolean> asyncExists(String ledgerName) {
        return CompletableFuture.completedFuture(streams.containsKey(ledgerName));
    }

    @Override
    public EntryCacheManager getEntryCacheManager() {
        // TODO: this method is only called within ManagedLedgerImpl, we should remove it
        throw new RuntimeException(notSupported("getEntryCacheManager"));
    }

    @Override
    public void updateCacheEvictionTimeThreshold(long cacheEvictionTimeThresholdNanos) {
        // TODO:
    }

    @Override
    public long getCacheEvictionTimeThreshold() {
        // TODO:
        return 0;
    }

    @Override
    public CompletableFuture<Map<String, String>> getManagedLedgerPropertiesAsync(String name) {
        // TODO: migrate MetaStoreImpl#getManagedLedgerPropertiesAsync
        return CompletableFuture.completedFuture(Map.of());
    }

    @Override
    public Map<String, ManagedLedger> getManagedLedgers() {
        return new HashMap<>(streams);
    }

    @Override
    public ManagedLedgerFactoryMXBean getCacheStats() {
        return new DummyManagedLedgerFactoryMXBean();
    }

    @Override
    public void estimateUnloadedTopicBacklog(PersistentOfflineTopicStats offlineTopicStats, TopicName topicName,
                                             boolean accurate, Object ctx) throws Exception {
        // TODO:
    }

    @Override
    public ManagedLedgerFactoryConfig getConfig() {
        // TODO: the config is only parsed from broker's configuration
        return new ManagedLedgerFactoryConfig();
    }

    static ManagedLedgerException notSupported(String method) {
        return new ManagedLedgerException(method + " is not supported");
    }
}
