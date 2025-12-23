package com.example.java.dbms.bigquery.util;

import com.google.api.services.bigquery.model.JobReference;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ImpersonatedCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.simba.googlebigquery.googlebigquery.client.authentication.AbstractAuthentication;
import com.simba.googlebigquery.googlebigquery.core.BQConnection;
import com.simba.googlebigquery.jdbc.jdbc42.S42Connection;
import com.simba.googlebigquery.support.exceptions.ErrorException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class BigQueryUtil {

  public static BQConnection getBQConnection(Object instance) {
    try {
      if (instance instanceof S42Connection) {
        Class<?> clazz = instance.getClass().getSuperclass().getSuperclass();
        java.lang.reflect.Field field = clazz.getDeclaredField("m_connection");
        if (field == null) {
          return null;
        }
        field.setAccessible(true);  // private/protected 필드 접근 허용
        Object value = field.get(instance);
        BQConnection bqConnection = (BQConnection) value;
        return bqConnection;
      }
      return null;
    } catch (Exception e) {
      log.error("getBQConnection Error: {}", e.getMessage());
      return null;
    }
  }

  public static BigQuery getBigQuery(Connection conn) throws SQLException, ErrorException {
    BQConnection bqConnection = BigQueryUtil.getBQConnection(conn);
    AbstractAuthentication authentication = BigQueryUtil.getAuthentication(bqConnection);
    String url = conn.getMetaData().getURL();
    String projectId = url.split(";")[1].split("=")[1];
    BigQuery bigQuery;
    if (authentication != null) {
      bigQuery = BigQueryOptions.newBuilder()
          .setCredentials(authentication.buildCredentials())
          .setProjectId(projectId)
          .build()
          .getService();

    } else {
      bigQuery = BigQueryOptions.newBuilder()
          .setProjectId(projectId)
          .build()
          .getService();
    }
    return bigQuery;
  }

  public static String getImpersonationToken(String serviceAccount) {
    try {
      GoogleCredentials bridge = GoogleCredentials
          .getApplicationDefault()
          .createScoped(List.of("https://www.googleapis.com/auth/cloud-platform"));

      ImpersonatedCredentials creds = ImpersonatedCredentials.create(
          bridge,
          serviceAccount,
          null,
          List.of("https://www.googleapis.com/auth/cloud-platform"),
          3600);

      return creds.refreshAccessToken().getTokenValue();
    } catch (Exception e) {

      e.printStackTrace();
      return "";
    }
  }

  public static AbstractAuthentication getAuthentication(Object instance) {
    try {
      if (instance instanceof BQConnection) {
        Class<?> clazz = instance.getClass();
        java.lang.reflect.Field field = clazz.getDeclaredField("m_authentication");
        if (field == null) {
          return null;
        }
        field.setAccessible(true);  // private/protected 필드 접근 허용
        Object value = field.get(instance);
        return (AbstractAuthentication) value;
      }
      return null;
    } catch (Exception e) {
      log.error("getAuthentication Error: {}", e.getMessage());
      return null;
    }
  }

  public static void cancel(Connection conn, JobReference jobReference) {
    try {
      BigQuery bigQuery = BigQueryUtil.getBigQuery(conn);

      String project = jobReference.getProjectId();
      String location = jobReference.getLocation();
      String jobName = jobReference.getJobId();
      log.info("Cancelling query job. project: {}, location: {}, jobName: {}",
          project, location, jobName);
      JobId jobId = JobId.newBuilder()
          .setProject(project)
          .setLocation(location)
          .setJob(jobName)
          .build();

      Job queryJob = bigQuery.getJob(jobId);
      if (queryJob != null) {
        queryJob.cancel();
        log.info("Query job {} cancelled.", queryJob.getJobId().getJob());
      } else {
        log.info("Query job not found.");
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}