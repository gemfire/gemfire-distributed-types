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
- DAtomicReference
- DSemaphore
- DCountDownLatch

### Other

- DSnowflake
- 
### Examples

Almost all the types are created using a `DTypeFactory`:

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

(`DSnowflake`s are simply created with regular Java instantiation)

### Details

`DSet`s and `DList`s implement the standard Java `Set` and `List` interfaces respectively.

`DBlockingQueue` implements Java's `BlockingDeque` interface and provides a double-ended queue which
provides both head and tail semantics. Note that methods of this class that support timeouts, and
are thus interruptible, can only be interrupted locally. The interrupt 'signal' is not passed on to
the cluster member that is actually performing the operation. This may lead to unexpected results if
not taken into account. For example, if we have a thread trying to take an entry:

```java
Object obj = queue.poll(10, TimeUnit.SECONDS);
```
If this thread is interrupted, the thread executing on the server will continue. If, before the
timeout expires, another thread places an entry into the queue, the `poll`ing thread will retrieve
it which may leave the queue in an unexpected state as far as the client is concerned. 

`DCircularQueue` implements the `Queue` interface and provides a first-in first-out queue with a
fixed size that replaces its oldest element if full.

`DSemaphore` provides an implementation with similar semantics to Java's `Semaphore` class.
DSemaphores provide a concurrency type that can be used to co-ordinate between clients running on
different JVMs. Since DSemaphores are highly available, they maintain their state even when
servers crash and clients re-connect to different servers.

`DAtomicLong` provides a counter implementation similar to Java's `AtomicLong` class.

`DAtomicReference` provides an implementation similar to Java's `AtomicReference` class. One
difference to note is that the `compareAndSet` method does not compare on object identity, but
rather on object equality. This is because the identity on the client will obviously be different to
the identity on the server, where the operation is actually being performed. Hence the need to use
equality as a way to compare objects.

`DCountDownLatch` provides an implementation similar to Java's `CountDownLatch`.

Note that any methods which can wait (and block) will automatically be retried if the server they
are connected to crashes or stops. If the particular method semantics also provide a timeout, the
timeout will be restarted.

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

### Looking for Support

This project is supported by the GemFire community and is not an official part of the GemFire product. It is an extension used by our services team at several customer installations. You are welcome to review and deploy it at your own risk; however, please note that GemFire support does not cover this extension.

If you require additional assistance, our services team is here to help. Please contact your account executive and request services support for integrating GemFire with this extension. They will connect you with the appropriate team member.

