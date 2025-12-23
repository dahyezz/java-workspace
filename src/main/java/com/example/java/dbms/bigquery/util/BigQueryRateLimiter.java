package com.example.java.dbms.bigquery.util;

import lombok.extern.log4j.Log4j2;

@Log4j2
public class BigQueryRateLimiter {

  private final double refillPerSecond;
  private final int bucketCapacity;
  private double tokens;
  private long lastRefillNanos;

  public BigQueryRateLimiter() {
    double qps = Double.parseDouble(
        System.getProperty("sinsiway.editor.bigquery.jobs.qps", "85.0")
    );
    int capacity = Math.max(1, (int) Math.ceil(qps));
    this.refillPerSecond = Math.max(0.0001d, qps);
    this.bucketCapacity = capacity;
    this.tokens = capacity;
    this.lastRefillNanos = System.nanoTime();
  }

  public synchronized void acquire() throws InterruptedException {
    while (true) {
      refill();
      if (tokens >= 1.0d) {
        tokens -= 1.0d;
        return;
      }
      long sleepMs = Math.max(1L, (long) (1000 / refillPerSecond));
      this.wait(Math.min(50L, sleepMs));
    }
  }

  private void refill() {
    long now = System.nanoTime();
    long elapsedNanos = now - lastRefillNanos;
    if (elapsedNanos <= 0) {
      return;
    }

    double add = (elapsedNanos / 1_000_000_000.0d) * refillPerSecond;
    tokens = Math.min(bucketCapacity, tokens + add);
    lastRefillNanos = now;
  }
}
