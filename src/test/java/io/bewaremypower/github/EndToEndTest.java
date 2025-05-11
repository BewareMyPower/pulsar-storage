package io.bewaremypower.github;

import io.bewaremypower.github.ml.InMemoryStorage;
import io.bewaremypower.github.schema.InMemorySchemaFactory;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.util.PortManager;
import org.apache.pulsar.compaction.DisabledTopicCompactionService;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class EndToEndTest {

    private static final String CLUSTER_NAME = "test";
    private final int brokerServicePort = PortManager.nextLockedFreePort();
    private PulsarService pulsar;

    @BeforeClass
    public void start() throws Exception {
        final var config = new ServiceConfiguration();
        config.setClusterName("test");
        config.setBrokerServicePort(Optional.of(brokerServicePort));
        config.setMetadataStoreUrl("memory:local");
        config.setManagedLedgerStorageClassName(InMemoryStorage.class.getName());
        config.setCompactionServiceFactoryClassName(DisabledTopicCompactionService.class.getName());
        config.setSchemaRegistryStorageClassName(InMemorySchemaFactory.class.getName());
        config.setAdvertisedAddress("127.0.0.1"); // avoid being affected by the local proxy
        pulsar = new PulsarService(config);
        pulsar.start();

        final var admin = pulsar.getAdminClient();
        admin.clusters().createCluster(CLUSTER_NAME, ClusterData.builder().build());
        admin.tenants().createTenant("public", TenantInfo.builder().allowedClusters(Set.of(CLUSTER_NAME))
                .build());
        admin.namespaces().createNamespace("public/default");
    }

    @AfterClass(alwaysRun = true)
    public void stop() throws Exception {
        if (pulsar != null) {
            pulsar.close();
        }
    }

    @Test
    public void test() throws PulsarClientException {
        @Cleanup final var client = PulsarClient.builder().serviceUrl("pulsar://localhost:" + brokerServicePort)
                .build();
        final var topic = "topic";
        final var numMessages = 10;
        final var producer = client.newProducer(Schema.STRING).topic(topic).create();
        for (int i = 0; i < numMessages; i++) {
            producer.send("msg-" + i);
        }

        final var consumer = client.newConsumer(Schema.STRING).topic(topic).subscriptionName("sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe();
        for (int i = 0; i < 10; i++) {
            final var msg = consumer.receive(3, TimeUnit.SECONDS);
            Assert.assertNotNull(msg);
            Assert.assertEquals(msg.getValue(), "msg-" + i);
        }
    }
}
