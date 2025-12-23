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
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Log4j2
@Component
public class MysqlRunner {

  @Value("${db.mysql.url}")
  private String url;
  @Value("${db.mysql.user}")
  private String user;
  @Value("${db.mysql.password}")
  private String password;

  public void run() throws Exception {
    Map<String, String> testCases = new LinkedHashMap<>();

    // 1. DELIMITER가 포함된 케이스 (에러 발생)
    testCases.put("1. DELIMITER 포함 (에러 발생)",
        "DELIMITER //\n" +
            "CREATE PROCEDURE insert_emp_1(\n" +
            "    IN p_name VARCHAR(50),\n" +
            "    IN p_dept VARCHAR(50)\n" +
            ")\n" +
            "BEGIN\n" +
            "    INSERT INTO employee(name, dept)\n" +
            "    VALUES (p_name, p_dept);\n" +
            "END //\n" +
            "DELIMITER ;");

    // 2. DELIMITER 없는 방식 (표준 방식)
    testCases.put("2. DELIMITER 미포함",
        "CREATE PROCEDURE insert_emp_2(\n" +
            "    IN p_name VARCHAR(50),\n" +
            "    IN p_dept VARCHAR(50)\n" +
            ")\n" +
            "BEGIN\n" +
            "    INSERT INTO employee(name, dept)\n" +
            "    VALUES (p_name, p_dept);\n" +
            "END");

    // 3. 블럭 + 일반 SQL 혼합
    testCases.put("3. 블럭 + 일반 SQL 혼합",
        "DROP PROCEDURE IF EXISTS test_mixed_proc;\n" +
            "CREATE PROCEDURE test_mixed_proc()\n" +
            "BEGIN\n" +
            "    SELECT 'Inside Procedure' AS location;\n" +
            "END;\n" +
            "SELECT 'Outside SQL' AS status;");

    try (Connection conn = DriverManager.getConnection(url, user, password)) {
      log.info("========== MYSQL PL/SQL BLOCK TEST START ==========");

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
        log.info("--------------------------------------------------");
      });

      log.info("========== MYSQL TEST FINISH ==========");

    } catch (SQLException e) {
      log.error(e.getMessage());
    }
  }

  public static void main(String[] args) throws Exception {
    try (ConfigurableApplicationContext ctx =
        SpringApplication.run(Application.class, args)) {

      MysqlRunner runner = ctx.getBean(MysqlRunner.class);
      runner.run();
    }
  }

}
