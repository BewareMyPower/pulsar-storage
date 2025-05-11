package io.bewaremypower.github.ml;

import org.apache.bookkeeper.mledger.ManagedLedgerMXBean;
import org.apache.bookkeeper.mledger.proto.PendingBookieOpsStats;
import org.apache.bookkeeper.mledger.util.StatsBuckets;

public class DummyManagedLedgerMXBean implements ManagedLedgerMXBean {

    @Override
    public String getName() {
        return "";
    }

    @Override
    public long getStoredMessagesSize() {
        return 0;
    }

    @Override
    public long getStoredMessagesLogicalSize() {
        return 0;
    }

    @Override
    public long getNumberOfMessagesInBacklog() {
        return 0;
    }

    @Override
    public double getAddEntryMessagesRate() {
        return 0;
    }

    @Override
    public double getAddEntryBytesRate() {
        return 0;
    }

    @Override
    public long getAddEntryBytesTotal() {
        return 0;
    }

    @Override
    public double getAddEntryWithReplicasBytesRate() {
        return 0;
    }

    @Override
    public long getAddEntryWithReplicasBytesTotal() {
        return 0;
    }

    @Override
    public double getReadEntriesRate() {
        return 0;
    }

    @Override
    public double getReadEntriesBytesRate() {
        return 0;
    }

    @Override
    public long getReadEntriesBytesTotal() {
        return 0;
    }

    @Override
    public double getMarkDeleteRate() {
        return 0;
    }

    @Override
    public long getMarkDeleteTotal() {
        return 0;
    }

    @Override
    public long getAddEntrySucceed() {
        return 0;
    }

    @Override
    public long getAddEntrySucceedTotal() {
        return 0;
    }

    @Override
    public long getAddEntryErrors() {
        return 0;
    }

    @Override
    public long getAddEntryErrorsTotal() {
        return 0;
    }

    @Override
    public long getEntriesReadTotalCount() {
        return 0;
    }

    @Override
    public long getReadEntriesSucceeded() {
        return 0;
    }

    @Override
    public long getReadEntriesSucceededTotal() {
        return 0;
    }

    @Override
    public long getReadEntriesErrors() {
        return 0;
    }

    @Override
    public long getReadEntriesErrorsTotal() {
        return 0;
    }

    @Override
    public double getReadEntriesOpsCacheMissesRate() {
        return 0;
    }

    @Override
    public long getReadEntriesOpsCacheMissesTotal() {
        return 0;
    }

    @Override
    public double getEntrySizeAverage() {
        return 0;
    }

    @Override
    public long[] getEntrySizeBuckets() {
        return new long[0];
    }

    @Override
    public double getAddEntryLatencyAverageUsec() {
        return 0;
    }

    @Override
    public long[] getAddEntryLatencyBuckets() {
        return new long[0];
    }

    @Override
    public long[] getLedgerSwitchLatencyBuckets() {
        return new long[0];
    }

    @Override
    public double getLedgerSwitchLatencyAverageUsec() {
        return 0;
    }

    @Override
    public StatsBuckets getInternalAddEntryLatencyBuckets() {
        return null;
    }

    @Override
    public StatsBuckets getInternalEntrySizeBuckets() {
        return null;
    }

    @Override
    public PendingBookieOpsStats getPendingBookieOpsStats() {
        return null;
    }

    @Override
    public double getLedgerAddEntryLatencyAverageUsec() {
        return 0;
    }

    @Override
    public long[] getLedgerAddEntryLatencyBuckets() {
        return new long[0];
    }

    @Override
    public StatsBuckets getInternalLedgerAddEntryLatencyBuckets() {
        return null;
    }
}
