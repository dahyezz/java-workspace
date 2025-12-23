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
public class OracleRunner {

  @Value("${db.oracle.url}")
  private String url;
  @Value("${db.oracle.user}")
  private String user;
  @Value("${db.oracle.password}")
  private String password;

  public void run() throws Exception {
    Map<String, String> testCases = new LinkedHashMap<>();

    // 1. BEGIN ~ END;
    testCases.put("1. BEGIN ~ END;",
        "BEGIN\n" +
            "    DBMS_OUTPUT.PUT_LINE('HELLO WORLD');\n" +
            "END;");

    // 2. BEGIN ~ END; /
    testCases.put("2. BEGIN ~ END; / (에러 발생)",
        "BEGIN\n" +
            "    DBMS_OUTPUT.PUT_LINE('HELLO WORLD');\n" +
            "END;\n" +
            "/");

    // 3. DECLARE ~ BEGIN ~ END;
    testCases.put("3. DECLARE ~ BEGIN ~ END;",
        "DECLARE\n" +
            "    v_message VARCHAR2(50) := 'HELLO WORLD';\n" +
            "BEGIN\n" +
            "    DBMS_OUTPUT.PUT_LINE(v_message);\n" +
            "END;");

    // 4. CREATE OR REPLACE PROCEDURE
    testCases.put("4. CREATE OR REPLACE PROCEDURE",
        "CREATE OR REPLACE PROCEDURE proc_test(p_id NUMBER) IS\n" +
            "BEGIN\n" +
            "    DBMS_OUTPUT.PUT_LINE('ID = ' || p_id);\n" +
            "END;");

    // 5. 개행이 있는 블럭
    testCases.put("5. 개행이 있는 블럭",
        "DECLARE\n" +
            "   v_num NUMBER := 3;\n" +
            "BEGIN\n" +
            "   IF v_num > 0 THEN\n" +
            "       DBMS_OUTPUT.PUT_LINE('POSITIVE');\n" +
            "   ELSE\n" +
            "       DBMS_OUTPUT.PUT_LINE('ZERO OR NEGATIVE');\n" +
            "   END IF;\n" +
            "   \n" +
            "   FOR i IN 1..v_num LOOP\n" +
            "       DBMS_OUTPUT.PUT_LINE('I=' || i);\n" +
            "   END LOOP;\n" +
            "   \n" +
            "   CASE v_num\n" +
            "       WHEN 1 THEN DBMS_OUTPUT.PUT_LINE('ONE');\n" +
            "       ELSE DBMS_OUTPUT.PUT_LINE('OTHER');\n" +
            "   END CASE;\n" +
            "END;");


    // 6. 중첩 PL/SQL 블럭
    testCases.put("6. 중첩 PL/SQL 블럭",
        "BEGIN\n" +
            "    DBMS_OUTPUT.PUT_LINE('OUTER');\n" +
            "    DECLARE\n" +
            "        v_inner NUMBER := 1;\n" +
            "    BEGIN\n" +
            "        DBMS_OUTPUT.PUT_LINE('INNER ' || v_inner);\n" +
            "    END;\n" +
            "END;");

    // 7. PL/SQL 블럭 + SQL 문 (JDBC에서 다중 구문 에러 유도)
    testCases.put("7. PL/SQL 블럭 + 일반 SQL 혼합",
        "BEGIN\n" +
            "    DBMS_OUTPUT.PUT_LINE('PLSQL');\n" +
            "END; \n" +
            "SELECT * FROM emp;");

    testCases.put("11. 대소문자 혼용 테이블명 조회",
        "SELECT * FROM emp; SELECT * FROM emp;");

    try (Connection conn = DriverManager.getConnection(url, user, password)) {
      log.info("========== ORACLE PL/SQL BLOCK TEST START ==========");

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

      log.info("========== ORACLE TEST FINISH ==========");

    } catch (SQLException e) {
      log.error(e.getMessage());
    }
  }

  public static void main(String[] args) throws Exception {
    try (ConfigurableApplicationContext ctx =
        SpringApplication.run(Application.class, args)) {

      OracleRunner runner = ctx.getBean(OracleRunner.class);
      runner.run();
    }
  }

}
