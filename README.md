# pulsar-storage

This branch is based on https://github.com/apache/pulsar/commit/ffce51ae9a9d78b4d66feff885bde4f1374be9ec

See [`EndToEndTest`](./src/test/java/io/bewaremypower/github/EndToEndTest.java), which shows an example that BookKeeper is completely removed.

```java
// The metadata store is an in-memory implementation
config.setMetadataStoreUrl("memory:local");
// The managed ledger implementation is customized that all entries are stored in memory
config.setManagedLedgerStorageClassName(InMemoryStorage.class.getName());
// The schema registry is also an in-memory implementation
config.setSchemaRegistryStorageClassName(InMemorySchemaFactory.class.getName());
```

The test runs a simple producer and consumer by:
1. Create a producer
2. Send 10 messages
3. Create a consumer with `Earliest` as the initial position
4. Receive 10 messages

There is also an implicit producer and consumer on the `__change_events` topic due to the system topic based topic policies.
