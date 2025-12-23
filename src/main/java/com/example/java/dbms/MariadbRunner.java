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
public class MariadbRunner {

  @Value("${db.mariadb.url}")
  private String url;
  @Value("${db.mariadb.user}")
  private String user;
  @Value("${db.mariadb.password}")
  private String password;

  public void run() throws Exception {
    Map<String, String> testCases = new LinkedHashMap<>();

    // 1. DELIMITER 포함
    testCases.put("1. DELIMITER 포함 (에러 발생)",
        "DELIMITER //\n" +
            "CREATE PROCEDURE sp_demo_1(\n" +
            "    IN p_id INT\n" +
            ")\n" +
            "BEGIN\n" +
            "    INSERT INTO t_demo(id) VALUES (p_id);\n" +
            "    UPDATE t_demo SET updated_at = NOW() WHERE id = p_id;\n" +
            "END //\n" +
            "DELIMITER ;");

    // 2. DELIMITER 없는 방식 (표준 방식)
    testCases.put("2. DELIMITER 미포함",
        "CREATE PROCEDURE sp_demo_2(\n" +
            "    IN p_id INT\n" +
            ")\n" +
            "BEGIN\n" +
            "    INSERT INTO t_demo(id) VALUES (p_id);\n" +
            "    UPDATE t_demo SET updated_at = NOW() WHERE id = p_id;\n" +
            "END;");

    // 3. PL/SQL 블럭 + 일반 SQL 혼합
    testCases.put("3. PL/SQL 블럭 + 일반 SQL 혼합",
        "CREATE PROCEDURE test_mixed_proc()\n" +
            "BEGIN\n" +
            "    SELECT 'Inside Procedure' AS location;\n" +
            "END;\n" +
            "SELECT 'Outside SQL' AS status;");

    try (Connection conn = DriverManager.getConnection(url, user, password)) {
      log.info("========== MARIADB PL/SQL BLOCK TEST START ==========");

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

      log.info("========== MARIADB TEST FINISH ==========");

    } catch (SQLException e) {
      log.error(e.getMessage());
    }
  }

  public static void main(String[] args) throws Exception {
    try (ConfigurableApplicationContext ctx =
        SpringApplication.run(Application.class, args)) {

      MariadbRunner runner = ctx.getBean(MariadbRunner.class);
      runner.run();
    }
  }

}
