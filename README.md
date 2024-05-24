# GemFire Distributed Types

### What is this?

This is a collection of various Java data types that are backed by GemFire, thus making them
naturally distributed and highly available.

These types are intended to be used from GemFire clients.

### Collection types

- DSet
- DList

### Concurrency types

- DAtomicLong
- DSemaphore

### Examples

All the types are created using a `DTypeFactory`:

```java
ClientCache client = new ClientCacheFactory()
    .addPoolLocator("localhost", locatorPort)
    .create();

DTypeFactory factory = new DTypeFactory(client);

DList<String> list = factory.createDList("myList");
DSet<Account> accounts = factory.createDSet("accounts");
DSemaphore semaphore = factory.createDSemaphore("semaphore", 1);
```

### Implementation details

These collections are implemented using a partitioned region and function calls. Operations are
captured as lambdas and then routed to the primary bucket hosting the type, where the operation is
applied. Each collection implements GemFire's Delta interface which allows the operation to be sent
as a delta change.