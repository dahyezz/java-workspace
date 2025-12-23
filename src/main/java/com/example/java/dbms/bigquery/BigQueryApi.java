package com.example.java.dbms.bigquery;

//import com.example.java.dbms.OracleRunner;

import com.example.java.Application;
import com.example.java.dbms.bigquery.util.BigQueryUtil;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;

@Log4j2
public class BigQueryApi  {
// API 이용 테스트
  @Value("${db.bigquery.projectId}")
  private String projectId;
  @Value("${db.bigquery.oauthServiceAccountEmail}")
  private String email;
  @Value("${db.bigquery.OAuthPvtKeyPath}")
  private String keyPath;

  public void run() throws Exception {
    String jdbcUrl =
        "jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443;"
            + "ProjectId=" + projectId + ";"
            + "OAuthType=0;"
            + "OAuthServiceAcctEmail=" + email + ";"
            + "OAuthPvtKeyPath=" + keyPath + ";";

    Map<String, String> testCases = new LinkedHashMap<>();

    // 1. DECLARE / SET / SELECT
    testCases.put("1. DECLARE & SET",
        "DECLARE x INT64 DEFAULT 1;\n" +
            "SET x = x + 10;\n" +
            "SELECT x AS result;");

    // 2. BEGIN ~ END;
    testCases.put("2. BEGIN ~ END;",
        "BEGIN\n" +
            "  DECLARE msg STRING;\n" +
            "  SET msg = 'Hello BigQuery';\n" +
            "  SELECT msg;\n" +
            "END;");

    // 3. DECLARE ~ BEGIN ~ END;
    testCases.put("3. DECLARE ~ BEGIN ~ END;",
        "DECLARE target_id INT64 DEFAULT 10;\n" +
            "DECLARE result_msg STRING;\n" +
            "BEGIN\n" +
            "    SET target_id = target_id + 5;\n" +
            "    IF target_id > 10 THEN\n" +
            "        SET result_msg = 'ID가 10보다 큽니다.';\n" +
            "    ELSE\n" +
            "        SET result_msg = 'ID가 10 이하입니다.';\n" +
            "    END IF;\n" +
            "    SELECT \n" +
            "        target_id AS final_id, \n" +
            "        result_msg AS status;\n" +
            "END;");

    // 4. CREATE TEMP FUNCTION
    testCases.put("4. CREATE TEMP FUNCTION",
        "CREATE TEMP FUNCTION add_one(x FLOAT64)\n" +
            "RETURNS FLOAT64\n" +
            "LANGUAGE js AS \"\"\"\n" +
            "  return x + 1;\n" +
            "\"\"\";");

    // 5. CREATE TEMP FUNCTION & USE
    testCases.put("5. CREATE TEMP FUNCTION & USE",
        "CREATE TEMP FUNCTION add_one(x FLOAT64)\n" +
            "RETURNS FLOAT64\n" +
            "LANGUAGE js AS \"\"\"\n" +
            "  return x + 1;\n" +
            "\"\"\";\n" +
            "SELECT add_one(10);");

    // 6. CREATE OR REPLACE PROCEDURE
    testCases.put("6. CREATE OR REPLACE PROCEDURE",
        "CREATE OR REPLACE PROCEDURE `petra_dataset.test_sp`(val INT64)\n" +
            "BEGIN\n" +
            "  SELECT val * 2;\n" +
            "END;");

    // 7. CREATE TEMP TABLE
    testCases.put("7. CREATE TEMP TABLE",
        "CREATE TEMP TABLE my_temp_table AS\n" +
            "SELECT 1 AS id, 'temp' AS name");

    // 8. CREATE TEMP TABLE + USE
    testCases.put("8. CREATE TEMP TABLE",
        "CREATE TEMP TABLE my_temp_table AS\n" +
            "SELECT 1 AS id, 'temp' AS name;\n" +
            "SELECT * FROM my_temp_table;");

    // 9. 임시 테이블 생명주기 위반
    testCases.put("9. 임시 객체 생명주기 위반",
        "SELECT * FROM petra_dataset.my_temp_table;");

    // 10. PL/SQL 블럭 + 일반 SQL 혼합
    testCases.put("10. PL/SQL 블럭 + 일반 SQL 혼합",
        "DECLARE target_word STRING DEFAULT 'methinks';\n" +
            "DECLARE corpus_count, word_count INT64;\n" +
            "\n" +
            "SET (corpus_count, word_count) = (\n" +
            "    SELECT AS STRUCT COUNT(DISTINCT corpus), SUM(word_count)\n" +
            "    FROM bigquery-public-data.samples.shakespeare\n" +
            "    WHERE LOWER(word) = target_word\n" +
            ");\n" +
            "\n" +
            "SELECT\n" +
            "    FORMAT('Found %d occurrences of \"%s\" across %d Shakespeare works',\n" +
            "        word_count, target_word, corpus_count) AS result;\n" +
            "\n" +
            "SELECT * FROM petra_dataset.emp;");

    // select * from petra_Dataset.emp; select * From petra_dataset.emp;
    testCases.put("11. 대소문자 혼용 테이블명 조회",
        "SELECT * FROM petra_dataset.emp; SELECT * FROM petra_dataset.emp;");


    try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
      BigQuery bigquery = BigQueryUtil.getBigQuery(conn);
      log.info("========== BIGQUERY PL/SQL BLOCK TEST START ==========");

      testCases.forEach((testName, script) -> {
        log.info("--------------------------------------------------");
        log.info("TestCase - {}", testName);
        log.info("SQL Text \n{}", script);

        try {
          QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(script).build();
          TableResult result = bigquery.query(queryConfig);

          log.info("[Success] Script Executed.");

          // 결과 출력 (첫 번째 행만 샘플로)
          for (FieldValueList row : result.iterateAll()) {
            log.info(">> Result Row: {}", row.toString());
            break;
          }
        } catch (Exception e) {
          log.error("[Fail] ErrorMessage - {}", e.getMessage());
        }
      });
      log.info("========== BIGQUERY TESTS FINISH ==========");

    } catch (Exception e) {
      log.error(e.getMessage());
    }
  }

  public static void main(String[] args) throws Exception {
    try (ConfigurableApplicationContext ctx =
        SpringApplication.run(Application.class, args)) {

      BigQueryApi runner = ctx.getBean(BigQueryApi.class);
      runner.run();
    }
  }

}
