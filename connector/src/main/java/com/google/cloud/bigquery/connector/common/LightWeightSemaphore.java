package com.google.cloud.bigquery.connector.common;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Light weight semaphore for two-thread syncrhonization where release should
 * be lighter weight then acquisition.
 *
 * Profiling showed Release of Java's Semaphore take no-trivial
 * CPU time (via Unsafe.unpark).  Since release is meant to schedule work
 * on background we don't want release of the semaphore to block/take a lot of
 * time.
 */
class LightWeightSemaphore {
   private final AtomicInteger leases;

   public LightWeightSemaphore(int leases) {
     this.leases = new AtomicInteger(leases);
   }

   void acquire() throws InterruptedException {
      int count = leases.decrementAndGet();
      int timeout = 100;
      while (count < 0) {
        leases.incrementAndGet();
        TimeUnit.MICROSECONDS.sleep(timeout);
        timeout = Math.min(10000, timeout * 2);
        count = leases.decrementAndGet();
      }
   }

   void release() {
     leases.incrementAndGet();
   }
}
