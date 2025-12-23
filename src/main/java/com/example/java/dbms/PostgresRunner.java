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
public class PostgresRunner  {

  @Value("${db.postgres.url}")
  private String url;
  @Value("${db.postgres.user}")
  private String user;
  @Value("${db.postgres.password}")
  private String password;

  public void run() throws Exception {
    Map<String, String> testCases = new LinkedHashMap<>();

    // 1. DO $$ ~ $$;
    testCases.put("1. DO $$ ~ $$;",
        "DO $$\n" +
            "BEGIN\n" +
            "    RAISE NOTICE 'HELLO WORLD';\n" +
            "END\n" +
            "$$;");

    // 2. DO $petra$ ~ $petra$;
    testCases.put("2. DO $petra$ ~ $petra$;",
        "DO $petra$\n" +
            "BEGIN\n" +
            "    RAISE NOTICE 'HELLO WORLD';\n" +
            "END;\n" +
            "$petra$;");

    // 3. DO $$ ~  $$ LANGUAGE plpgsql;
    testCases.put("3. DO $$ ~  $$ LANGUAGE plpgsql;",
        "DO $$\n" +
            "BEGIN\n" +
            "    RAISE NOTICE 'HELLO WORLD';\n" +
            "END\n" +
            "$$ LANGUAGE plpgsql;\n");

    // 4. BEGIN ~ END (에러 발생)
    testCases.put("4. BEGIN ~ END (에러 발생)",
        "BEGIN\n" +
            "    RAISE NOTICE 'HELLO WORLD';\n" +
            "END;");

    // 5. CREATE OR REPLACE PROCEDURE (Standard)
    testCases.put("5. CREATE OR REPLACE PROCEDURE (Standard)",
        "CREATE OR REPLACE PROCEDURE test_proc()\n" +
            "LANGUAGE plpgsql\n" +
            "AS $$\n" +
            "BEGIN\n" +
            "    RAISE NOTICE 'procedure';\n" +
            "END\n" +
            "$$;");

    // 6. CREATE OR REPLACE PROCEDURE (Standard)
    testCases.put("6. CREATE OR REPLACE PROCEDURE (Standard)",
        "CREATE OR REPLACE PROCEDURE test_proc()\n" +
            "AS $$\n" +
            "BEGIN\n" +
            "    RAISE NOTICE 'procedure';\n" +
            "END\n" +
            "$$\n" +
            "LANGUAGE plpgsql;");

    // 7. CREATE OR REPLACE PROCEDURE (언어 명시 없음 -> 에러 발생)
    testCases.put("7. CREATE OR REPLACE PROCEDURE (언어 명시 없음 -> 에러 발생)",
        "CREATE OR REPLACE PROCEDURE test_proc()\n" +
            "AS $$\n" +
            "BEGIN\n" +
            "    RAISE NOTICE 'procedure';\n" +
            "END\n" +
            "$$");

    // 8. DO 블럭 + 일반 SQL 혼합
    testCases.put("8. DO 블럭 + 일반 SQL 혼합",
        "DO $$\n" +
            "BEGIN\n" +
            "    RAISE NOTICE 'DO block';\n" +
            "END\n" +
            "$$;\n" +
            "SELECT * FROM emp;");

    testCases.put("9. 일반 SQL 단독 실행",
        "SELECT * FROM emp;select * from emp;" );

    // 실행 로직
    try (Connection conn = DriverManager.getConnection(url, user, password)) {
      log.info("========== POSTGRESQL PL/SQL BLOCK TEST START ==========");

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

      log.info("========== POSTGRESQL TEST FINISH ==========");

    } catch (SQLException e) {
      log.error(e.getMessage());
    }
  }

  public static void main(String[] args) throws Exception {
    try (ConfigurableApplicationContext ctx =
        SpringApplication.run(Application.class, args)) {

      PostgresRunner runner = ctx.getBean(PostgresRunner.class);
      runner.run();
    }
  }
}
