package com.example.java.dbms;

import com.example.java.Application;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
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
public class SqlServerRunner{

  @Value("${db.sqlserver.url}")
  private String url;
  @Value("${db.sqlserver.user}")
  private String user;
  @Value("${db.sqlserver.password}")
  private String password;

  public void run() throws Exception {
    Map<String, String> testCases = new LinkedHashMap<>();

    // 1. BEGIN ~ END
    testCases.put("1. BEGIN ~ END",
        "BEGIN\n" +
            "    DECLARE @x INT = 1;\n" +
            "    SET @x = @x + 1;\n" +
            "    SELECT @x AS x;\n" +
            "END;");

    // 2. BEGIN ~ END; GO (에러 발생)
    testCases.put("2. BEGIN ~ END + GO 포함",
        "BEGIN\n" +
            "    SELECT 1 AS test;\n" +
            "END;\n" +
            "GO");

    // 3. CREATE PROCEDURE
    testCases.put("3. CREATE PROCEDURE (Standard)",
        "CREATE OR ALTER PROCEDURE dbo.sp_demo\n" +
            "AS\n" +
            "BEGIN\n" +
            "    SELECT 'ok' AS msg;\n" +
            "END;");

    // 4. CREATE PROCEDURE + GO (에러 발생)
    testCases.put("4. CREATE PROCEDURE + GO 포함",
        "CREATE OR ALTER PROCEDURE dbo.sp_demo_go\n" +
            "AS\n" +
            "BEGIN\n" +
            "    SELECT 'ok' AS msg;\n" +
            "END;\n" +
            "GO");

    // 5. BEGIN TRY ~ END CATCH
    testCases.put("5. BEGIN TRY ~ END CATCH",
        "BEGIN TRY\n" +
            "    EXEC dbo.sp_demo;\n" +
            "END TRY\n" +
            "BEGIN CATCH\n" +
            "    SELECT ERROR_MESSAGE() AS err;\n" +
            "END CATCH;");

    // 6. PL/SQL 블럭 + 일반
    testCases.put("6. PL/SQL 블럭 + 일반 ",
        "DECLARE @v_name NVARCHAR(50) = 'Petra';\n" +
            "IF @v_name = 'Petra'\n" +
            "BEGIN\n" +
            "    SELECT 'Success' AS result;\n" +
            "END\n" +
            "SELECT * FROM emp;");

    try (Connection conn = DriverManager.getConnection(url, user, password)) {
      log.info("========== SQL_SERVER PL/SQL BLOCK TEST START ==========");

      testCases.forEach((testName, sql) -> {
        log.info("--------------------------------------------------");
        log.info("TestCase - {}", testName);
        log.info("SQL Text \n{}", sql);

        try (Statement stmt = conn.createStatement()) {
          boolean hasResultSet = stmt.execute(sql);
          log.info("[Success] hasResultSet - {}", hasResultSet);

          // 만약 ResultSet이 있다면 출력 (SELECT 문 포함 시)
          if (hasResultSet) {
            try (ResultSet rs = stmt.getResultSet()) {
              while (rs.next()) {
                log.info(">> Result Column 1: {}", rs.getString(1));
              }
            }
          }

        } catch (Exception e) {
          log.error("[Fail] ErrorMessage - {}", e.getMessage());
        }
        log.info("--------------------------------------------------");
      });

      log.info("========== SQL_SERVER TEST FINISH ==========");


    } catch (SQLException e) {
      log.error(e.getMessage());
    }
  }

  public static void main(String[] args) throws Exception {
    try (ConfigurableApplicationContext ctx =
        SpringApplication.run(Application.class, args)) {

      SqlServerRunner runner = ctx.getBean(SqlServerRunner.class);
      runner.run();
    }
  }
}
