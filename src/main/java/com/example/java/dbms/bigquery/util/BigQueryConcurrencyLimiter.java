package com.example.java.dbms.bigquery.util;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class BigQueryConcurrencyLimiter {

  private final Semaphore semaphore;

  public BigQueryConcurrencyLimiter() {
    int concurrency = Integer.parseInt(
        System.getProperty("sinsiway.editor.bigquery.jobs.concurrency", "85")
    );
    this.semaphore = new Semaphore(concurrency, true); // fair mode
    log.info("BigQueryConcurrencyLimiter initialized with concurrency limit {}", concurrency);
  }

  /**
   * 동시성 제한 획득 (차단)
   */
  public void acquire() throws InterruptedException {
    semaphore.acquire();
  }

  /**
   * 타임아웃이 있는 획득 (필요 시 사용 가능)
   */
  public boolean tryAcquire(long timeout, TimeUnit unit) throws InterruptedException {
    return semaphore.tryAcquire(timeout, unit);
  }

  /**
   * 완료 후 허가증 반환
   */
  public void release() {
    semaphore.release();
  }

  /**
   * 남은 permit 수 조회 (모니터링용)
   */
  public int availablePermits() {
    return semaphore.availablePermits();
  }
}
