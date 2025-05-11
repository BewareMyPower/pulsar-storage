package io.bewaremypower.github.ml;

import lombok.Setter;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.pulsar.broker.storage.ManagedLedgerStorageClass;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;

@Setter
public class InMemoryStorageClass implements ManagedLedgerStorageClass {

    private MetadataStoreExtended metadataStore;

    @Override
    public String getName() {
        return "in-memory-storage";
    }

    @Override
    public ManagedLedgerFactory getManagedLedgerFactory() {
        // TODO: implement a factory
        return new InMemoryManagedLedgerFactory(metadataStore);
    }
}
