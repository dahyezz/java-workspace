package com.example.java.sync.queue;

import com.example.java.sync.constants.SyncType;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import lombok.extern.log4j.Log4j2;

//@Component
@Log4j2
public class QueueManager {

  private static QueueManager instance;
  private final BlockingQueue<SyncType> queue = new LinkedBlockingQueue<>();
  private SyncType currentType = null;

  public static synchronized QueueManager getInstance() {
    if (instance == null) {
      instance = new QueueManager();
    }
    return instance;
  }

  public void addRequest(SyncType type) {
    if (queue.contains(type)) {
      log.info("Already in queue: {}", type);
      return;
    }
    queue.add(type);
  }

  public SyncType getRequest() {
    try {
      currentType = queue.take();
      log.info("Get request: {}", currentType);
    } catch (InterruptedException e) {
      log.error("Error while getting request", e);
    }

    return currentType;
  }

  public void finishSync(SyncType type) {
    if (currentType != null) {
      log.info("Finish sync: {}", type);
      currentType = null;
    }
  }

}
