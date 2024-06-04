# GemFire Distributed Types

### What is this?

This is a collection of various Java data types that are backed by GemFire, thus making them
naturally distributed and highly available.

These types are intended to be used from GemFire clients to provide concurrent and distributed
access.

### Collection types

- DSet
- DList
- DBlockingQueue
- DCircularQueue

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
DBlockingQueue queue = factory.createDQueue("queue", 5);
DCircularQueue circular = factory.createDCircularQueue("circular", 100);
```

### Details

`DSet`s and `DList`s implement the standard Java `Set` and `List` interfaces respectively.

`DBlockingQueue` implements Java's `BlockingDeque` interface and provides a double-ended queue which
provides both head and tail semantics. The current release of Distributed Types does not yet
implement any of the methods that allow for timeouts. This includes:
- `offerFirst`
- `offerLast`
- `pollFirst`
- `pollLast`
- `offer`
- `poll`
However, the non-timeout versions of these methods are all supported.

`DCircularQueue` implements the `Queue` interface and provides a first-in first-out queue with a
fixed size that replaces its oldest element if full.

`DSemaphore` provides an implementation with similar semantics to Java's `Semaphore` class.
DSemaphores provide a concurrency type that can be used to co-ordinate between clients running on
different JVMs. Since DSemaphores are highly available, they maintain their state even when
servers crash and clients re-connect to different servers.

`DAtomicLong` provides a counter implementation similar to Java's `AtomicLong` class.

### Developing and Deploying

The package can easily be used from either Maven or Gradle:

#### Maven:

```xml
<dependency>
  <groupId>dev.gemfire</groupId>
  <artifactId>gemfire-distributed-types</artifactId>
  <version>0.1.0</version>
</dependency>
```
#### Gradle:
```groovy
dependencies {
  implementation 'dev.gemfire:gemfire-distributed-types:0.1.0'
}
```

In order to deploy the package to a GemFire cluster it should be added as an extension. The
extension bundle can be found under (Releases)[https://github.com/gemfire/gemfire-distributed-types/releases].
The `.gfm` file should be added to the `extensions` directory within the GemFire distribution. 
Look for a line in the logs that state `Initialized service for GemFire Distributed Types` to 
verify that the extension has been found and initialized.

### Implementation details

These types are implemented using a partitioned region and function calls. Operations are
captured as lambdas and then routed to the server hosting the primary bucket for the given instance,
where the operation is applied. Each collection implements GemFire's Delta interface which allows
the operation to be sent as a delta change to the secondary server.

The backing region is called `DTYPES`. It is a Partitioned Region with a redundancy of 1 (i.e.
an additional copy of each structure is stored on a different server). Initially, this region is not
persisted and is not user-configurable.
