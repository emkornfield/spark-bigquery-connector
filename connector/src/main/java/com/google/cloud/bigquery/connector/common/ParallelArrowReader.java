/*
 * Copyright 2021 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.bigquery.connector.common;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.parquet.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * A utility class for taking up to N {@link ArrowReader} objects and reading data from them
 * asynchronously.
 *
 * This is useful in a few contexts:
 * * For InputPartitionReaders that have expensive synchronous CPU operations
 *   (e.g. parsing/decompression) this allows for pipelining operations with Spark application level
 *   code (assuming there are free CPU cycles).
 * * A way of composing underlying readers for increased throughput (e.g. if readers are each IO
 *   bound waiting on separate services).
 *
 */
public class ParallelArrowReader implements AutoCloseable {
  private static final Logger log = LoggerFactory.getLogger(ParallelArrowReader.class);
  private static final Object DONE_SENTINEL = new Object();
  public static final int DONE = -1;

  // Visible for testing.
  private final BlockingQueue<Object> queue;
  private final BlockingQueue<Integer> readerAvailableQueue;
  private final List<ArrowReader> readers;
  private final ExecutorService executor;
  private final BigQueryStorageReadRowsTracer rootTracer;
  private final BigQueryStorageReadRowsTracer[] tracers;
  private final AtomicInteger readersReady;
  private int currentIndex = DONE;

  // Background thread for reading from delegates.
  private Thread readerThread;

  /**
   * @param readers The readers to read from in a round robin order.
   * @param executor An ExecutorService to process the get method on the delegates. The service will
   *     be shutdown when this object is closed.
   */
  public ParallelArrowReader(
      List<ArrowReader> readers,
      ExecutorService executor,
      BigQueryStorageReadRowsTracer tracer) {
    this.readers = readers;
    // Reserve extra space for sentinel and one extra element processing.
    queue = new ArrayBlockingQueue<>(readers.size() + 2);
    this.readerAvailableQueue = new ArrayBlockingQueue<>(readers.size());
    for (int x = 0; x < readers.size(); x++) {
      readerAvailableQueue.add(x);
    }
    this.executor = executor;
    this.rootTracer = tracer;
    this.readersReady = new AtomicInteger(readers.size());
    tracers = new BigQueryStorageReadRowsTracer[readers.size()];
    for (int x = 0; x < readers.size(); x++) {
      tracers[x] = rootTracer.forkWithPrefix("reader-thread-" + x);
    }
    start();
  }

  public int next() throws IOException {
    if (currentIndex != DONE) {
      Preconditions.checkState(readerAvailableQueue.offer(currentIndex),
          "Expected room in return queue");
      currentIndex = DONE;
    }
    rootTracer.nextBatchNeeded();
    rootTracer.readRowsResponseRequested();
    try {
      Object nextObject = queue.take();
      if (nextObject == DONE_SENTINEL) {
        return DONE;
      }
      if (nextObject instanceof Throwable) {
        if (nextObject instanceof IOException) {
          throw (IOException) nextObject;
        }
        throw new IOException((Throwable) nextObject);
      }
      Preconditions.checkState(nextObject instanceof Integer, "Expected future object");
      currentIndex = (Integer)nextObject;
    } catch (InterruptedException e) {
      log.info("Interrupted when waiting for next batch.");
      return DONE;
    }

    // We don't have access to bytes here.
    rootTracer.readRowsResponseObtained(/*bytes=*/ 0);
    return currentIndex;
  }

  private void start() {
    readerThread = new Thread(this::consumeReaders);
    readerThread.setDaemon(true);
    readerThread.start();
    rootTracer.startStream();
  }

  private void consumeReaders() {
    try {
      // Tracks which readers have exhausted all of there elements
      AtomicBoolean[] hasData = new AtomicBoolean[readers.size()];
      long lastBytesRead[] = new long[readers.size()];
      VectorSchemaRoot[] roots = new VectorSchemaRoot[readers.size()];

      for (int x = 0; x < hasData.length; x++) {
        hasData[x] = new AtomicBoolean();
        hasData[x].set(true);
        lastBytesRead[x] = 0;
        roots[x] = readers.get(x).getVectorSchemaRoot();
        tracers[x].startStream();
      }

      while (readersReady.get() > 0) {
          // Ensure that we don't submit another task for the same reader
          // until the last one completed. This is necessary when some readers run out of
          // tasks.
          final int idx = readerAvailableQueue.take();

          if (idx == DONE || !hasData[idx].get()) {
            continue;
          }
          ArrowReader reader = readers.get(idx);

          executor.submit(
              () -> {
                synchronized (roots[idx]) {
                  if (!hasData[idx].get()) {
                    return;
                  }
                  try {
                    tracers[idx].readRowsResponseRequested();
                    hasData[idx].set(reader.loadNextBatch());
                    long incrementalBytesRead = reader.bytesRead() - lastBytesRead[idx];
                    tracers[idx].readRowsResponseObtained(/*bytesReceived=*/ incrementalBytesRead);
                    lastBytesRead[idx] = reader.bytesRead();
                  } catch (Throwable e) {
                    log.info("Exception caught while consuming reader.", e);
                    hasData[idx].set(false);
                    readersReady.set(0);
                    Preconditions.checkState(queue.offer(e), "Expected space in queue");
                  }
                  if (!hasData[idx].get()) {
                    int result = readersReady.addAndGet(-1);
                    if (result <= 0){
                      readerAvailableQueue.offer(DONE);
                    }
                    return;
                  }
                  try {
                    Preconditions.checkState(queue.offer(idx), "Expected space in queue");
                  } catch (Exception e) {
                    throw e;
                  }
                }
              });
      }
    } catch (Throwable e) {
      log.info("Read ahead caught exceptions", e);
      Preconditions.checkState(queue.offer(e), "Expected available capacity");
      return;
    }
    Preconditions.checkState(queue.offer(DONE_SENTINEL), "Expected available capacity");
  }

  @Override
  public void close() {
    rootTracer.finished();
    // Try to force reader thread to stop.
    if (readerThread != null) {
      readersReady.set(0);
      readerThread.interrupt();
      try {
        readerThread.join(10000);
      } catch (InterruptedException e) {
        log.info("Interrupted while waiting for reader thread to finish.");
      }
      if (readerThread.isAlive()) {
        log.warn("Reader thread did not shutdown in 10 seconds.");
      } else {
        log.info("Reader thread stopped.  Queue size: {}", queue.size());
      }
    }
    // Stop any queued tasks from processing.
    executor.shutdownNow();

    try {
      if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
        log.warn("executor did not terminate after 10 seconds");
      }
    } catch (InterruptedException e) {
      log.info("Interrupted when awaiting executor termination");
    }

    for (BigQueryStorageReadRowsTracer tracer : tracers) {
      tracer.finished();
    }

    for (ArrowReader reader : readers) {
      try {
        // Don't close the stream here because it will consume all of it.
        // We let other components worry about stream closure.
        reader.close(/*close underlying channel*/ false);
      } catch (Exception e) {
        log.info("Trouble closing delegate readers", e);
      }
    }
  }
}
