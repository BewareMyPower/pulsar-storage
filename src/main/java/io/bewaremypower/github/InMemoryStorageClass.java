package io.bewaremypower.github;

import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.pulsar.broker.storage.ManagedLedgerStorageClass;

public class InMemoryStorageClass implements ManagedLedgerStorageClass {

    @Override
    public String getName() {
        return "";
    }

    @Override
    public ManagedLedgerFactory getManagedLedgerFactory() {
        return null;
    }
}
