package io.bewaremypower.github.ml;

import com.google.common.collect.Range;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedCursorMXBean;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.policies.data.ManagedLedgerInternalStats;

public class InMemoryCursor implements ManagedCursor {

    private final AtomicLong readOffset = new AtomicLong(0L);
    private final String name;
    private final InMemoryManagedLedger managedLedger;
    private final boolean durable;

    public InMemoryCursor(String name, InMemoryManagedLedger managedLedger, Position startPosition,
                          CommandSubscribe.InitialPosition initialPosition, boolean durable) {
        this.name = name;
        this.managedLedger = managedLedger;
        this.durable = durable;
        final Position readPosition;
        if (startPosition == null || startPosition.compareTo(managedLedger.getLastConfirmedEntry()) > 0) {
            readPosition = (initialPosition == CommandSubscribe.InitialPosition.Earliest) ? PositionFactory.EARLIEST
                    : PositionFactory.LATEST;
        } else {
            readPosition = startPosition;
        }
        if (readPosition.equals(PositionFactory.EARLIEST)) {
            final var firstOffset = managedLedger.getFirstPosition().getEntryId();
            readOffset.set(firstOffset >= 0 ? firstOffset : 0L);
        } else if (readPosition.equals(PositionFactory.LATEST)) {
            final var lastConfirmedEntry = managedLedger.getLastConfirmedEntry();
            if (lastConfirmedEntry instanceof PositionImpl position) {
                readOffset.set(position.baseOffset() + position.numMessages());
            } // else: no last confirmed entry, keep 0 as default
        } else {
            readOffset.set(readPosition.getEntryId());
        }
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public long getLastActive() {
        return 0;
    }

    @Override
    public void updateLastActive() {
    }

    @Override
    public Map<String, Long> getProperties() {
        // TODO: we should manage properties separately
        return Map.of();
    }

    @Override
    public Map<String, String> getCursorProperties() {
        return Map.of();
    }

    @Override
    public CompletableFuture<Void> putCursorProperty(String key, String value) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> setCursorProperties(Map<String, String> cursorProperties) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> removeCursorProperty(String key) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public boolean putProperty(String key, Long value) {
        return true;
    }

    @Override
    public boolean removeProperty(String key) {
        return true;
    }

    @Override
    public List<Entry> readEntries(int numberOfEntriesToRead) {
        final var future = new CompletableFuture<List<Entry>>();
        asyncReadEntries(numberOfEntriesToRead, Long.MAX_VALUE, new AsyncCallbacks.ReadEntriesCallback() {
            @Override
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                future.complete(entries);
            }

            @Override
            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                future.completeExceptionally(exception);
            }
        }, null, PositionFactory.LATEST);
        return future.join();
    }

    @Override
    public void asyncReadEntries(int numberOfEntriesToRead, AsyncCallbacks.ReadEntriesCallback callback, Object ctx,
                                 Position maxPosition) {
        asyncReadEntries(numberOfEntriesToRead, Long.MAX_VALUE, callback, ctx, maxPosition);
    }

    @Override
    public void asyncReadEntries(int numberOfEntriesToRead, long maxSizeBytes,
                                 AsyncCallbacks.ReadEntriesCallback callback, Object ctx, Position maxPosition) {
        final var entries = managedLedger.readEntries(readOffset, numberOfEntriesToRead, maxSizeBytes, maxPosition);
        callback.readEntriesComplete(entries, ctx);
    }

    @Override
    public Entry getNthEntry(int n, IndividualDeletedEntries deletedEntries) throws InterruptedException, ManagedLedgerException {
        // TODO:
        throw InMemoryManagedLedgerFactory.notSupported("getNthEntry");
    }

    @Override
    public void asyncGetNthEntry(int n, IndividualDeletedEntries deletedEntries,
                                 AsyncCallbacks.ReadEntryCallback callback, Object ctx) {
        // TODO:
        callback.readEntryFailed(InMemoryManagedLedgerFactory.notSupported("asyncGetNthEntry"), ctx);
    }

    @Override
    public List<Entry> readEntriesOrWait(int numberOfEntriesToRead) {
        return readEntriesOrWait(numberOfEntriesToRead, Long.MAX_VALUE);
    }

    @Override
    public List<Entry> readEntriesOrWait(int maxEntries, long maxSizeBytes) {
        final var future = new CompletableFuture<List<Entry>>();
        asyncReadEntriesOrWait(maxEntries, maxSizeBytes, new AsyncCallbacks.ReadEntriesCallback() {
            @Override
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                future.complete(entries);
            }

            @Override
            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                future.completeExceptionally(exception);
            }
        }, null, PositionFactory.LATEST);
        return future.join();
    }

    @Override
    public void asyncReadEntriesOrWait(int numberOfEntriesToRead, AsyncCallbacks.ReadEntriesCallback callback,
                                       Object ctx, Position maxPosition) {
        asyncReadEntriesOrWait(numberOfEntriesToRead, Long.MAX_VALUE, callback, ctx, maxPosition);
    }

    @Override
    public void asyncReadEntriesOrWait(int maxEntries, long maxSizeBytes, AsyncCallbacks.ReadEntriesCallback callback,
                                       Object ctx, Position maxPosition) {
        asyncReadEntries(maxEntries, maxSizeBytes, new AsyncCallbacks.ReadEntriesCallback() {
            @Override
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                // TODO: support adding the cursor to the wait list if entries is empty
                callback.readEntriesComplete(entries, ctx);
            }

            @Override
            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                callback.readEntriesFailed(exception, ctx);
            }
        }, ctx, maxPosition);
    }

    @Override
    public boolean cancelPendingReadRequest() {
        // TODO:
        return false;
    }

    @Override
    public boolean hasMoreEntries() {
        final var lastConfirmedEntry = managedLedger.getLastConfirmedEntry();
        if (lastConfirmedEntry instanceof PositionImpl position) {
            return readOffset.get() < lastConfirmedEntry.getEntryId() + position.numMessages();
        } else {
            return readOffset.get() < lastConfirmedEntry.getEntryId() + 1;
        }
    }

    @Override
    public long getNumberOfEntries() {
        return managedLedger.getNumberOfEntries();
    }

    @Override
    public long getNumberOfEntriesInBacklog(boolean isPrecise) {
        // TODO: take the read position into consideration
        return managedLedger.getNumberOfEntries();
    }

    @Override
    public void markDelete(Position position) {
    }

    @Override
    public void markDelete(Position position, Map<String, Long> properties) {
    }

    @Override
    public void asyncMarkDelete(Position position, AsyncCallbacks.MarkDeleteCallback callback, Object ctx) {
        callback.markDeleteComplete(ctx);
    }

    @Override
    public void asyncMarkDelete(Position position, Map<String, Long> properties,
                                AsyncCallbacks.MarkDeleteCallback callback, Object ctx) {
        // TODO:
        callback.markDeleteComplete(ctx);
    }

    @Override
    public void delete(Position position) throws ManagedLedgerException {
        throw InMemoryManagedLedgerFactory.notSupported("delete");
    }

    @Override
    public void asyncDelete(Position position, AsyncCallbacks.DeleteCallback callback, Object ctx) {
        // TODO:
        callback.deleteFailed(InMemoryManagedLedgerFactory.notSupported("asyncDelete"), ctx);
    }

    @Override
    public void delete(Iterable<Position> positions) throws ManagedLedgerException {
        throw InMemoryManagedLedgerFactory.notSupported("delete");
    }

    @Override
    public void asyncDelete(Iterable<Position> position, AsyncCallbacks.DeleteCallback callback, Object ctx) {
        // TODO:
        callback.deleteFailed(InMemoryManagedLedgerFactory.notSupported("asyncDelete"), ctx);
    }

    @Override
    public Position getReadPosition() {
        return PositionFactory.create(managedLedger.getFirstPosition().getLedgerId(), readOffset.get());
    }

    @Override
    public Position getMarkDeletedPosition() {
        // TODO:
        return getReadPosition();
    }

    @Override
    public Position getPersistentMarkDeletedPosition() {
        // TODO:
        return getReadPosition();
    }

    @Override
    public void rewind() {
        // TODO:
    }

    @Override
    public void seek(Position newReadPosition, boolean force) {
        // TODO:
    }

    @Override
    public void clearBacklog() {
        // TODO:
    }

    @Override
    public void asyncClearBacklog(AsyncCallbacks.ClearBacklogCallback callback, Object ctx) {
        // TODO:
        callback.clearBacklogComplete(ctx);
    }

    @Override
    public void skipEntries(int numEntriesToSkip, IndividualDeletedEntries deletedEntries) {
        // TODO:
    }

    @Override
    public void asyncSkipEntries(int numEntriesToSkip, IndividualDeletedEntries deletedEntries,
                                 AsyncCallbacks.SkipEntriesCallback callback, Object ctx) {
        // TODO:
        callback.skipEntriesComplete(ctx);
    }

    @Override
    public Position findNewestMatching(Predicate<Entry> condition) {
        // TODO:
        return managedLedger.getLastConfirmedEntry();
    }

    @Override
    public Position findNewestMatching(FindPositionConstraint constraint, Predicate<Entry> condition) {
        // TODO:
        return managedLedger.getLastConfirmedEntry();
    }

    @Override
    public void asyncFindNewestMatching(FindPositionConstraint constraint, Predicate<Entry> condition,
                                        AsyncCallbacks.FindEntryCallback callback, Object ctx) {
        // TODO:
        callback.findEntryComplete(managedLedger.getLastConfirmedEntry(), ctx);
    }

    @Override
    public void asyncFindNewestMatching(FindPositionConstraint constraint, Predicate<Entry> condition,
                                        AsyncCallbacks.FindEntryCallback callback, Object ctx, boolean isFindFromLedger) {
        // TODO:
        callback.findEntryComplete(managedLedger.getLastConfirmedEntry(), ctx);
    }

    @Override
    public void resetCursor(Position position) {
        // TODO:
    }

    @Override
    public void asyncResetCursor(Position position, boolean forceReset, AsyncCallbacks.ResetCursorCallback callback) {
        // TODO:
        callback.resetComplete(null);
    }

    @Override
    public List<Entry> replayEntries(Set<? extends Position> positions) {
        // TODO:
        return List.of();
    }

    @Override
    public Set<? extends Position> asyncReplayEntries(Set<? extends Position> positions,
                                                      AsyncCallbacks.ReadEntriesCallback callback, Object ctx) {
        // TODO:
        callback.readEntriesComplete(List.of(), ctx);
        return Set.of();
    }

    @Override
    public Set<? extends Position> asyncReplayEntries(Set<? extends Position> positions,
                                                      AsyncCallbacks.ReadEntriesCallback callback, Object ctx, boolean sortEntries) {
        // TODO:
        callback.readEntriesComplete(List.of(), ctx);
        return Set.of();
    }

    @Override
    public void close() {
        // TODO:
    }

    @Override
    public void asyncClose(AsyncCallbacks.CloseCallback callback, Object ctx) {
        callback.closeComplete(ctx);
    }

    @Override
    public Position getFirstPosition() {
        return managedLedger.getFirstPosition();
    }

    @Override
    public void setActive() {
        // TODO:
    }

    @Override
    public void setInactive() {
        // TODO:
    }

    @Override
    public void setAlwaysInactive() {
        // TODO:
    }

    @Override
    public boolean isActive() {
        // TODO:
        return true;
    }

    @Override
    public boolean isDurable() {
        return durable;
    }

    @Override
    public long getNumberOfEntriesSinceFirstNotAckedMessage() {
        // TODO:
        return getNumberOfEntries();
    }

    @Override
    public int getTotalNonContiguousDeletedMessagesRange() {
        // TODO:
        return 0;
    }

    @Override
    public int getNonContiguousDeletedMessagesRangeSerializedSize() {
        // TODO:
        return 0;
    }

    @Override
    public long getEstimatedSizeSinceMarkDeletePosition() {
        // TODO:
        return 0;
    }

    @Override
    public double getThrottleMarkDelete() {
        // TODO:
        return 0.0;
    }

    @Override
    public void setThrottleMarkDelete(double throttleMarkDelete) {
        // TODO:
    }

    @Override
    public ManagedLedger getManagedLedger() {
        return managedLedger;
    }

    @Override
    public Range<Position> getLastIndividualDeletedRange() {
        // TODO:
        return Range.all();
    }

    @Override
    public void trimDeletedEntries(List<Entry> entries) {
        // TODO:
    }

    @Override
    public long[] getDeletedBatchIndexesAsLongArray(Position position) {
        // TODO:
        return new long[0];
    }

    @Override
    public ManagedCursorMXBean getStats() {
        return new DummyManagedCursorMXBean();
    }

    @Override
    public boolean checkAndUpdateReadPositionChanged() {
        // TODO:
        return false;
    }

    @Override
    public boolean isClosed() {
        // TODO:
        return false;
    }

    @Override
    public ManagedLedgerInternalStats.CursorStats getCursorStats() {
        // TODO:
        return new ManagedLedgerInternalStats.CursorStats();
    }

    @Override
    public boolean isMessageDeleted(Position position) {
        // TODO:
        return false;
    }

    @Override
    public ManagedCursor duplicateNonDurableCursor(String nonDurableCursorName) throws ManagedLedgerException {
        // TODO:
        return this;
    }

    @Override
    public long[] getBatchPositionAckSet(Position position) {
        // TODO:
        return new long[0];
    }

    @Override
    public int applyMaxSizeCap(int maxEntries, long maxSizeBytes) {
        // TODO: this method is only used in CompactedTopicImpl, we should remove it
        return 0;
    }

    @Override
    public void updateReadStats(int readEntriesCount, long readEntriesSize) {
        // TODO: this method is only used in CompactedTopicImpl, we should remove it
    }
}
