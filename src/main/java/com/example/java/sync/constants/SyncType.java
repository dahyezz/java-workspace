package com.example.java.sync.constants;

public enum SyncType {
  SYSTEM_SYNC(1, "SYSTEM_SYNC"),
  POLICY_SYNC(2, "POLICY_SYNC"),
  SYNC_SCHEDULE(3, "SYNC_SCHEDULE"),
  SYNC_CHECK(4, "SYNC_CHECK"),
  SYNC_TARGET(5, "SYNC_TARGET"),
  DELETE_EXPIRED_REQUEST(6, "DELETE_EXPIRED_REQUEST");

  private int code;
  private String type;

  SyncType(int code, String type) {
    this.code = code;
    this.type = type;
  }

  public int getCode() {
    return code;
  }

  public String getType() {
    return type;
  }
}
