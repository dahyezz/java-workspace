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
public class H2Runner {

  @Value("${db.oracle.url}")
  private String url;
  @Value("${db.oracle.user}")
  private String user;
  @Value("${db.oracle.password}")
  private String password;

  public void run() throws Exception {
    Map<String, String> testCases = new LinkedHashMap<>();

    testCases.put("1. Single Quote 방식",
        "CREATE ALIAS IF NOT EXISTS SAY_HELLO AS '\n" +
            "String sayHello(String name) {\n" +
            "  return \"Hello, \" + name; \n" +
            "}\n" +
            "';");

    testCases.put("2. Dollar Quoting ($$) 방식",
        "CREATE ALIAS IF NOT EXISTS SAY_HELLO_DOLLAR AS $$\n" +
            "String sayHello(String name) {\n" +
            "  return \"It's a beautiful day, \" + name; \n" + // '를 그대로 써도 안전!
            "}\n" +
            "$$;");

    testCases.put("3. Single Quote + @CODE ",
        "CREATE ALIAS IF NOT EXISTS IP_ADDRESS_3 AS '\n" +
            "import java.net.*;\n" +
            "@CODE\n" +
            "String ipAddress(String host) throws Exception {\n" +
            "    return InetAddress.getByName(host).getHostAddress();\n" +
            "}\n" +
            "';");

    testCases.put("4. Dollar Quoting ($$) + @CODE ",
        "CREATE ALIAS IF NOT EXISTS IP_ADDRESS_4 AS $$\n" +
            "import java.net.*;\n" +
            "@CODE\n" +
            "String ipAddress(String host) throws Exception {\n" +
            "    return InetAddress.getByName(host).getHostAddress();\n" +
            "}\n" +
            "$$;");

    testCases.put("5. single quote start + dollar quote end",
        "CREATE ALIAS IF NOT EXISTS IP_ADDRESS_4 AS '\n" +
            "import java.net.*;\n" +
            "@CODE\n" +
            "String ipAddress(String host) throws Exception {\n" +
            "    return InetAddress.getByName(host).getHostAddress();\n" +
            "}\n" +
            "$$;");

    try (Connection conn = DriverManager.getConnection(url, user, password)) {
      log.info("========== H2 PL/SQL BLOCK TEST START ==========");

      testCases.forEach((testName, sql) -> {
        log.info("--------------------------------------------------");
        log.info("TestCase - {}", testName);
        log.info("SQL Text \n{}", sql);

        try (Statement stmt = conn.createStatement()) {
          boolean hasResultSet = stmt.execute(sql);

          if (hasResultSet) {
            try (ResultSet rs = stmt.getResultSet()) {
              if (rs.next()) {
                log.info("[Result] First Column Value: {}", rs.getString(1));
              }
            }
          }
          log.info("[Success] hasResultSet - {}", hasResultSet);
        } catch (Exception e) {
          log.error("[Fail] ErrorMessage - {}", e.getMessage());
        }
        log.info("--------------------------------------------------");
      });

      log.info("========== H2 TEST FINISH ==========");

    } catch (SQLException e) {
      log.error(e.getMessage());
    }
  }

  public static void main(String[] args) throws Exception {
    try (ConfigurableApplicationContext ctx =
        SpringApplication.run(Application.class, args)) {

      H2Runner runner = ctx.getBean(H2Runner.class);
      runner.run();
    }
  }


}
