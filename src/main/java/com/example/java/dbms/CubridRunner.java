package com.example.java.dbms;

import com.example.java.Application;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.stereotype.Component;

@Log4j2
@Component
public class CubridRunner {

  @Value("${db.cubrid.url}")
  private String url;
  @Value("${db.cubrid.user}")
  private String user;
  @Value("${db.cubrid.password}")
  private String password;

  public void run() throws Exception {
    Map<String, String> testCases = new LinkedHashMap<>();

    // 1. CREATE PROCEDURE (Standard)
    testCases.put("1. CREATE OR REPLACE PROCEDURE",
        "CREATE OR REPLACE PROCEDURE test_proc (arg1 INT, arg2 INT)\n" +
            "AS\n" +
            "BEGIN\n" +
            "  DBMS_OUTPUT.put_line (arg1 + arg2);\n" +
            "END;");

    // 2. PL/SQL 블럭 + 일반 SQL 혼합
    testCases.put("2. CREATE OR REPLACE PROCEDURE + / 포함",
        "CREATE OR REPLACE PROCEDURE test_proc_slash (arg1 INT, arg2 INT)\n" +
            "AS\n" +
            "BEGIN\n" +
            "  DBMS_OUTPUT.put_line (arg1 + arg2);\n" +
            "END;\n" +
            "select * FROM test.audit_log;");


    try (Connection conn = DriverManager.getConnection(url, user, password)) {
      log.info("========== CUBRID PL/SQL BLOCK TEST START ==========");

      testCases.forEach((testName, sql) -> {
        log.info("--------------------------------------------------");
        log.info("TestCase - {}", testName);
        log.info("SQL Text \n{}", sql);

        try (Statement stmt = conn.createStatement()) {
          boolean hasResultSet = stmt.execute(sql);
          log.info("[Success] hasResultSet - {}", hasResultSet);
        } catch (Exception e) {
          log.error("[Fail] ErrorMessage - {}", e.getMessage());
        }
      });

      log.info("========== CUBRID TEST FINISH ==========");

    } catch (SQLException e) {
      log.error(e.getMessage());
    }
  }

  public static void main(String[] args) throws Exception {
    try (ConfigurableApplicationContext ctx =
        SpringApplication.run(Application.class, args)) {

      CubridRunner runner = ctx.getBean(CubridRunner.class);
      runner.run();
    }
  }

}
