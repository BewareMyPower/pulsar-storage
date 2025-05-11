package io.bewaremypower.github.ml;

import org.apache.bookkeeper.mledger.ManagedLedgerFactoryMXBean;

public class DummyManagedLedgerFactoryMXBean implements ManagedLedgerFactoryMXBean {

    @Override
    public int getNumberOfManagedLedgers() {
        return 0;
    }

    @Override
    public long getCacheUsedSize() {
        return 0;
    }

    @Override
    public long getCacheMaxSize() {
        return 0;
    }

    @Override
    public double getCacheHitsRate() {
        return 0;
    }

    @Override
    public long getCacheHitsTotal() {
        return 0;
    }

    @Override
    public double getCacheMissesRate() {
        return 0;
    }

    @Override
    public long getCacheMissesTotal() {
        return 0;
    }

    @Override
    public double getCacheHitsThroughput() {
        return 0;
    }

    @Override
    public long getCacheHitsBytesTotal() {
        return 0;
    }

    @Override
    public double getCacheMissesThroughput() {
        return 0;
    }

    @Override
    public long getCacheMissesBytesTotal() {
        return 0;
    }

    @Override
    public long getNumberOfCacheEvictions() {
        return 0;
    }

    @Override
    public long getNumberOfCacheEvictionsTotal() {
        return 0;
    }

    @Override
    public long getCacheInsertedEntriesCount() {
        return 0;
    }

    @Override
    public long getCacheEvictedEntriesCount() {
        return 0;
    }

    @Override
    public long getCacheEntriesCount() {
        return 0;
    }
}
