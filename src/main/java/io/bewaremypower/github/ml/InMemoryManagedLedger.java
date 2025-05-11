package io.bewaremypower.github.ml;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.collect.Range;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import it.unimi.dsi.fastutil.ints.IntObjectPair;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerMXBean;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionBound;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.bookkeeper.mledger.intercept.ManagedLedgerInterceptor;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.pulsar.broker.intercept.ManagedLedgerInterceptorImpl;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.policies.data.ManagedLedgerInternalStats;

@Slf4j
public class InMemoryManagedLedger implements ManagedLedger {

    private final NavigableMap<Long, IntObjectPair<ByteBuf>> entries = new ConcurrentSkipListMap<>();
    private final Map<String, ManagedCursor> cursors = new ConcurrentHashMap<>();
    private final long streamId;
    private final String name;
    private volatile ManagedLedgerConfig config;
    // key is the base offset, the integer of the value pair is the number of messages
    private volatile boolean closed = false;

    public InMemoryManagedLedger(long streamId, String name, ManagedLedgerConfig config) {
        this.streamId = streamId;
        this.name = name;
        this.config = config;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Position addEntry(byte[] data) throws InterruptedException, ManagedLedgerException {
        return addEntry(data, 1);
    }

    @Override
    public Position addEntry(byte[] data, int numberOfMessages) {
        final var future = new CompletableFuture<Position>();
        asyncAddEntry(Unpooled.wrappedBuffer(data), numberOfMessages, new AsyncCallbacks.AddEntryCallback() {
            @Override
            public void addComplete(Position position, ByteBuf entryData, Object ctx) {
                future.complete(position);
            }

            @Override
            public void addFailed(ManagedLedgerException exception, Object ctx) {
                future.completeExceptionally(exception);
            }
        }, null);
        return future.join();
    }

    @Override
    public void asyncAddEntry(byte[] data, AsyncCallbacks.AddEntryCallback callback, Object ctx) {
        asyncAddEntry(Unpooled.wrappedBuffer(data), 1, callback, ctx);
    }

    @Override
    public Position addEntry(byte[] data, int offset, int length) throws InterruptedException, ManagedLedgerException {
        throw InMemoryManagedLedgerFactory.notSupported("addEntry offset and length");
    }

    @Override
    public Position addEntry(byte[] data, int numberOfMessages, int offset, int length) throws ManagedLedgerException {
        throw InMemoryManagedLedgerFactory.notSupported("addEntry offset and length");
    }

    @Override
    public void asyncAddEntry(byte[] data, int offset, int length, AsyncCallbacks.AddEntryCallback callback, Object ctx) {
        callback.addFailed(InMemoryManagedLedgerFactory.notSupported("asyncAddEntry offset and length"), ctx);
    }

    @Override
    public void asyncAddEntry(byte[] data, int numberOfMessages, int offset, int length,
                              AsyncCallbacks.AddEntryCallback callback, Object ctx) {
        // TODO: this method is only called in ManagedLedgerImpl, we should remove it
        callback.addFailed(InMemoryManagedLedgerFactory.notSupported("asyncAddEntry offset and length"), ctx);
    }

    @Override
    public void asyncAddEntry(ByteBuf buffer, AsyncCallbacks.AddEntryCallback callback, Object ctx) {
        asyncAddEntry(buffer, 1, callback, ctx);
    }

    @Override
    public void asyncAddEntry(ByteBuf buffer, int numberOfMessages, AsyncCallbacks.AddEntryCallback callback,
                              Object ctx) {
        if (closed) {
            callback.addFailed(new ManagedLedgerException.ManagedLedgerAlreadyClosedException(streamId + "is closed"),
                    ctx);
            return;
        }
        final var lastEntry = entries.lastEntry();
        final var leo = (lastEntry == null) ? 0 : (lastEntry.getKey() + lastEntry.getValue().keyInt());
        entries.put(leo, IntObjectPair.of(numberOfMessages, buffer.retain()));
        callback.addComplete(PositionFactory.create(streamId, leo), buffer, ctx);
    }

    @Override
    public ManagedCursor openCursor(String name) {
        return openCursor(name, CommandSubscribe.InitialPosition.Earliest);
    }

    @Override
    public ManagedCursor openCursor(String name, CommandSubscribe.InitialPosition initialPosition) {
        return openCursor(name, initialPosition, Map.of(), Map.of());
    }

    @Override
    public ManagedCursor openCursor(String name, CommandSubscribe.InitialPosition initialPosition,
                                    Map<String, Long> properties, Map<String, String> cursorProperties) {
        // TODO: support recover from properties
        final var startPosition = PositionFactory.EARLIEST;
        return cursors.computeIfAbsent(name, __ -> new InMemoryCursor(name, this, startPosition, initialPosition,
                true));
    }

    @Override
    public ManagedCursor newNonDurableCursor(Position startCursorPosition) throws ManagedLedgerException {
        return newNonDurableCursor(startCursorPosition, "default-sub", CommandSubscribe.InitialPosition.Earliest, true);
    }

    @Override
    public ManagedCursor newNonDurableCursor(Position startPosition, String subscriptionName) throws ManagedLedgerException {
        return newNonDurableCursor(startPosition, subscriptionName, CommandSubscribe.InitialPosition.Earliest, true);
    }

    @Override
    public ManagedCursor newNonDurableCursor(Position startPosition, String subscriptionName, CommandSubscribe.InitialPosition initialPosition, boolean isReadCompacted) throws ManagedLedgerException {
        // TODO: differ the durable cursor and non-durable cursor
        return cursors.computeIfAbsent(subscriptionName, __ -> new InMemoryCursor(subscriptionName, this, startPosition,
                initialPosition, false));
    }

    @Override
    public void asyncDeleteCursor(String name, AsyncCallbacks.DeleteCursorCallback callback, Object ctx) {
        cursors.remove(name);
        callback.deleteCursorComplete(ctx);
    }

    @Override
    public void deleteCursor(String name) {
        cursors.remove(name);
    }

    @Override
    public void removeWaitingCursor(ManagedCursor cursor) {
        // TODO:
    }

    @Override
    public void asyncOpenCursor(String name, AsyncCallbacks.OpenCursorCallback callback, Object ctx) {
        callback.openCursorComplete(openCursor(name), ctx);
    }

    @Override
    public void asyncOpenCursor(String name, CommandSubscribe.InitialPosition initialPosition, AsyncCallbacks.OpenCursorCallback callback, Object ctx) {
        callback.openCursorComplete(openCursor(name, initialPosition), ctx);
    }

    @Override
    public void asyncOpenCursor(String name, CommandSubscribe.InitialPosition initialPosition,
                                Map<String, Long> properties, Map<String, String> cursorProperties,
                                AsyncCallbacks.OpenCursorCallback callback, Object ctx) {
        callback.openCursorComplete(openCursor(name, initialPosition), ctx);
    }

    @Override
    public Iterable<ManagedCursor> getCursors() {
        return new ArrayList<>(cursors.values());
    }

    @Override
    public Iterable<ManagedCursor> getActiveCursors() {
        return new ArrayList<>(cursors.values());
    }

    @Override
    public long getNumberOfEntries() {
        return entries.size();
    }

    @Override
    public long getNumberOfEntries(Range<Position> range) {
        // TODO:
        return getNumberOfEntries();
    }

    @Override
    public long getNumberOfActiveEntries() {
        // TODO:
        return getNumberOfEntries();
    }

    @Override
    public long getTotalSize() {
        // TODO:
        return 0;
    }

    @Override
    public long getEstimatedBacklogSize() {
        // TODO:
        return 0;
    }

    @Override
    public CompletableFuture<Long> getEarliestMessagePublishTimeInBacklog() {
        // TODO:
        return null;
    }

    @Override
    public long getOffloadedSize() {
        // TODO:
        return 0;
    }

    @Override
    public long getLastOffloadedLedgerId() {
        return streamId;
    }

    @Override
    public long getLastOffloadedSuccessTimestamp() {
        // TODO:
        return 0;
    }

    @Override
    public long getLastOffloadedFailureTimestamp() {
        // TODO:
        return 0;
    }

    @Override
    public void asyncTerminate(AsyncCallbacks.TerminateCallback callback, Object ctx) {
        // TODO:
        callback.terminateComplete(PositionFactory.EARLIEST, ctx);
    }

    @Override
    public CompletableFuture<Position> asyncMigrate() {
        return CompletableFuture.completedFuture(PositionFactory.EARLIEST);
    }

    @Override
    public CompletableFuture<Void> asyncAddLedgerProperty(long ledgerId, String key, String value) {
        // TODO: we should manage the property individually
        return CompletableFuture.failedFuture(InMemoryManagedLedgerFactory.notSupported("ml properties"));
    }

    @Override
    public CompletableFuture<Void> asyncRemoveLedgerProperty(long ledgerId, String key) {
        // TODO: we should manage the property individually
        return CompletableFuture.failedFuture(InMemoryManagedLedgerFactory.notSupported("ml properties"));
    }

    @Override
    public CompletableFuture<String> asyncGetLedgerProperty(long ledgerId, String key) {
        // TODO: we should manage the property individually
        return CompletableFuture.failedFuture(InMemoryManagedLedgerFactory.notSupported("ml properties"));
    }

    @Override
    public Position terminate() {
        // TODO:
        return PositionFactory.EARLIEST;
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public void asyncClose(AsyncCallbacks.CloseCallback callback, Object ctx) {
        closed = true;
        callback.closeComplete(ctx);
    }

    @Override
    public ManagedLedgerMXBean getStats() {
        return new DummyManagedLedgerMXBean();
    }

    @Override
    public void delete() {
    }

    @Override
    public void asyncDelete(AsyncCallbacks.DeleteLedgerCallback callback, Object ctx) {
        // TODO:
        callback.deleteLedgerComplete(ctx);
    }

    @Override
    public Position offloadPrefix(Position pos) {
        // TODO:
        return PositionFactory.EARLIEST;
    }

    @Override
    public void asyncOffloadPrefix(Position pos, AsyncCallbacks.OffloadCallback callback, Object ctx) {
        // TODO:
        callback.offloadComplete(PositionFactory.EARLIEST, ctx);
    }

    @Override
    public ManagedCursor getSlowestConsumer() {
        // TODO:
        return cursors.values().stream().findFirst().orElse(null);
    }

    @Override
    public boolean isTerminated() {
        // TODO:
        return false;
    }

    @Override
    public boolean isMigrated() {
        // TODO:
        return false;
    }

    @Override
    public ManagedLedgerConfig getConfig() {
        return config;
    }

    @Override
    public void setConfig(ManagedLedgerConfig config) {
        this.config = config;
    }

    @Override
    public Position getLastConfirmedEntry() {
        final var entry = entries.lastEntry();
        if (entry == null) {
            return PositionFactory.create(streamId, -1L);
        }
        return new PositionImpl(streamId, entry.getKey(), entry.getValue().leftInt());
    }

    @Override
    public void readyToCreateNewLedger() {
        // TODO: we should remove it since it expose the internal details
    }

    @Override
    public Map<String, String> getProperties() {
        return Map.of();
    }

    @Override
    public void setProperty(String key, String value) {
    }

    @Override
    public void asyncSetProperty(String key, String value, AsyncCallbacks.UpdatePropertiesCallback callback,
                                 Object ctx) {
        callback.updatePropertiesComplete(Map.of(), ctx);
    }

    @Override
    public void deleteProperty(String key) {

    }

    @Override
    public void asyncDeleteProperty(String key, AsyncCallbacks.UpdatePropertiesCallback callback, Object ctx) {
        callback.updatePropertiesComplete(Map.of(), ctx);
    }

    @Override
    public void setProperties(Map<String, String> properties) {

    }

    @Override
    public void asyncSetProperties(Map<String, String> properties, AsyncCallbacks.UpdatePropertiesCallback callback,
                                   Object ctx) {
        callback.updatePropertiesComplete(Map.of(), ctx);
    }

    @Override
    public void trimConsumedLedgersInBackground(CompletableFuture<?> promise) {
        promise.complete(null);
    }

    @Override
    public void rollCurrentLedgerIfFull() {
        // TODO: we should remove it since it expose the internal details
    }

    @Override
    public CompletableFuture<Position> asyncFindPosition(Predicate<Entry> predicate) {
        // TODO:
        return CompletableFuture.completedFuture(PositionFactory.EARLIEST);
    }

    @Override
    public ManagedLedgerInterceptor getManagedLedgerInterceptor() {
        // TODO: it's only used internally, we should remove it
        return new ManagedLedgerInterceptorImpl(Set.of(), Set.of());
    }

    @Override
    public CompletableFuture<MLDataFormats.ManagedLedgerInfo.LedgerInfo> getLedgerInfo(long ledgerId) {
        final var ledgerInfo = MLDataFormats.ManagedLedgerInfo.LedgerInfo.newBuilder()
                .setLedgerId(streamId)
                .build();
        return CompletableFuture.completedFuture(ledgerInfo);
    }

    @Override
    public Optional<MLDataFormats.ManagedLedgerInfo.LedgerInfo> getOptionalLedgerInfo(long ledgerId) {
        return Optional.of(getLedgerInfo(ledgerId).join());
    }

    @Override
    public CompletableFuture<Void> asyncTruncate() {
        // TODO:
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<ManagedLedgerInternalStats> getManagedLedgerInternalStats(boolean includeLedgerMetadata) {
        // TODO:
        return CompletableFuture.completedFuture(new ManagedLedgerInternalStats());
    }

    @Override
    public boolean checkInactiveLedgerAndRollOver() {
        // TODO:
        return false;
    }

    @Override
    public void checkCursorsToCacheEntries() {
        // TODO:
    }

    @Override
    public void asyncReadEntry(Position position, AsyncCallbacks.ReadEntryCallback callback, Object ctx) {
        final var entries = readEntries(new AtomicLong(position.getEntryId()), 1, Long.MAX_VALUE, position);
        if (entries.isEmpty()) {
            callback.readEntryFailed(new ManagedLedgerException("No entry"), ctx);
        } else {
            callback.readEntryComplete(entries.get(0), ctx);
        }
    }

    @Override
    public NavigableMap<Long, MLDataFormats.ManagedLedgerInfo.LedgerInfo> getLedgersInfo() {
        final var info = new ConcurrentSkipListMap<Long, MLDataFormats.ManagedLedgerInfo.LedgerInfo>();
        info.put(streamId, getLedgerInfo(streamId).join());
        return info;
    }

    @Override
    public Position getNextValidPosition(Position position) {
        // TODO:
        return PositionFactory.EARLIEST;
    }

    @Override
    public Position getPreviousPosition(Position position) {
        // TODO:
        return PositionFactory.EARLIEST;
    }

    @Override
    public long getEstimatedBacklogSize(Position position) {
        // TODO:
        return 0;
    }

    @Override
    public Position getPositionAfterN(Position startPosition, long n, PositionBound startRange) {
        // TODO:
        return PositionFactory.EARLIEST;
    }

    @Override
    public int getPendingAddEntriesCount() {
        // TODO:
        return 0;
    }

    @Override
    public long getCacheSize() {
        // TODO:
        return 0;
    }

    @Override
    public Position getFirstPosition() {
        final var entry = entries.firstEntry();
        if (entry == null) {
            return PositionFactory.create(streamId, -1L);
        }
        return PositionFactory.create(streamId, entry.getKey());
    }

    // This method is required for getLastMessageId()
    @Override
    public CompletableFuture<Position> getLastDispatchablePosition(Predicate<Entry> predicate, Position startPosition) {
        return CompletableFuture.completedFuture(getLastConfirmedEntry());
    }

    public synchronized List<Entry> readEntries(AtomicLong startOffset, int numberOfEntriesToRead, long maxSizeBytes,
                                                Position maxPosition) {
        checkArgument(numberOfEntriesToRead > 0);
        final long maxEndOffset;
        if (maxPosition instanceof PositionImpl position) {
            maxEndOffset = position.getEntryId() + position.numMessages();
        } else if (maxPosition != null && maxPosition.getEntryId() != Long.MAX_VALUE) {
            maxEndOffset = maxPosition.getEntryId() + 1;
        } else {
            maxEndOffset = Long.MAX_VALUE;
        }
        var currentOffset = startOffset.get();
        final var entries = new ArrayList<Entry>();
        final var maxReadOffset = Math.min(maxEndOffset, lastEndOffset());
        var readSize = 0L;
        for (int i = 0; i < numberOfEntriesToRead && currentOffset < maxReadOffset && readSize < maxSizeBytes; i++) {
            final var keyValue = this.entries.floorEntry(currentOffset);
            if (keyValue == null) {
                // TODO: should we fail here?
                // The consumer's offset is greater than the end offset
                log.warn("Consumer tried to fetch offset {}, which is greater than the end offset: {}",
                        currentOffset, this.entries.lastEntry());
                break;
            }
            if (currentOffset > keyValue.getKey()) {
                // It has reached the end
                break;
            }
            final var payload = keyValue.getValue().right().retainedDuplicate();
            final var numberOfMessages = keyValue.getValue().leftInt();
            if (currentOffset + numberOfMessages > maxReadOffset || readSize + payload.readableBytes() > maxSizeBytes) {
                payload.release();
                break;
            }
            currentOffset += numberOfMessages;
            readSize += payload.readableBytes();
            entries.add(EntryImpl.create(PositionFactory.create(streamId, keyValue.getKey()), payload));
        }
        startOffset.set(currentOffset);
        return entries;
    }

    private long lastEndOffset() {
        final var position = getLastConfirmedEntry();
        if (position instanceof PositionImpl ursaPosition) {
            return ursaPosition.baseOffset() + ursaPosition.numMessages();
        } else {
            return position.getEntryId() + 1;
        }
    }
}
