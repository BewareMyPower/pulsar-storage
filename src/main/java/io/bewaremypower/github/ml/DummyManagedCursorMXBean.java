package io.bewaremypower.github.ml;

import org.apache.bookkeeper.mledger.ManagedCursorMXBean;

public class DummyManagedCursorMXBean implements ManagedCursorMXBean {

    @Override
    public String getName() {
        return "";
    }

    @Override
    public String getLedgerName() {
        return "";
    }

    @Override
    public void persistToLedger(boolean success) {

    }

    @Override
    public void persistToZookeeper(boolean success) {

    }

    @Override
    public long getPersistLedgerSucceed() {
        return 0;
    }

    @Override
    public long getPersistLedgerErrors() {
        return 0;
    }

    @Override
    public long getPersistZookeeperSucceed() {
        return 0;
    }

    @Override
    public long getPersistZookeeperErrors() {
        return 0;
    }

    @Override
    public void addWriteCursorLedgerSize(long size) {

    }

    @Override
    public void addReadCursorLedgerSize(long size) {

    }

    @Override
    public long getWriteCursorLedgerSize() {
        return 0;
    }

    @Override
    public long getWriteCursorLedgerLogicalSize() {
        return 0;
    }

    @Override
    public long getReadCursorLedgerSize() {
        return 0;
    }
}
