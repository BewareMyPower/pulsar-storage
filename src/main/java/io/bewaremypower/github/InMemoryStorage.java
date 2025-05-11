package io.bewaremypower.github;

import io.netty.channel.EventLoopGroup;
import io.opentelemetry.api.OpenTelemetry;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import org.apache.pulsar.broker.BookKeeperClientFactory;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.storage.ManagedLedgerStorage;
import org.apache.pulsar.broker.storage.ManagedLedgerStorageClass;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;

public class InMemoryStorage implements ManagedLedgerStorage {

    private final InMemoryStorageClass storageClass = new InMemoryStorageClass();

    @Override
    public void initialize(ServiceConfiguration conf, MetadataStoreExtended metadataStore,
                           BookKeeperClientFactory bookkeeperProvider, EventLoopGroup eventLoopGroup,
                           OpenTelemetry openTelemetry) throws Exception {
    }

    @Override
    public Collection<ManagedLedgerStorageClass> getStorageClasses() {
        return List.of(storageClass);
    }

    @Override
    public Optional<ManagedLedgerStorageClass> getManagedLedgerStorageClass(String name) {
        return Optional.empty();
    }

    @Override
    public void close() throws IOException {
    }
}
