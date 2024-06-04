/*
 * Copyright 2024 Broadcom. All rights reserved.
 */

/**
 * GemFire Distributed Types
 * <p>
 * This is a collection of various Java data types that are backed by GemFire, thus making them
 * naturally distributed and highly available.
 * <p>
 * The following types are provided
 * <ul>
 * <li>{@link dev.gemfire.dtype.DList}</li>
 * <li>{@link dev.gemfire.dtype.DSet}</li>
 * <li>{@link dev.gemfire.dtype.DBlockingQueue}</li>
 * <li>{@link dev.gemfire.dtype.DCircularQueue}</li>
 * <li>{@link dev.gemfire.dtype.DAtomicLong}</li>
 * <li>{@link dev.gemfire.dtype.DSemaphore}</li>
 * </ul>
 * <p>
 * Distributed Type instances are created with a {@link dev.gemfire.dtype.DTypeFactory}. For
 * example:
 *
 * <pre>
 * ClientCache client = new ClientCacheFactory()
 *     .addPoolLocator("localhost", locatorPort)
 *     .create();
 *
 * DTypeFactory factory = new DTypeFactory(client);
 *
 * DList&lt;String&gt; list = factory.createDList("myList");
 * DSet&lt;Account&gt; accounts = factory.createDSet("accounts");
 * DSemaphore semaphore = factory.createDSemaphore("semaphore", 1);
 * DBlockingQueue&lt;String&gt; queue = factory.createDQueue("queue", 5);
 * DCircularQueue&lt;Requests&gt; queue = factory.createDCircularQueue("requests", 1000);
 * </pre>
 *
 * The various types are intended for use from GemFire clients and allow for concurrent and
 * distributed access from clients running in different JVMs. Client structures hold almost no state
 * and most all operations are performed on backend structures that are stored and managed by
 * GemFire.
 * <p>
 * Note that iteration operations will serialize and retrieve the full structure to the client.
 * Iteration is not performed remotely on the server. Only {@code DList} supports removal of
 * elements when iterating.
 */
package dev.gemfire.dtype;
