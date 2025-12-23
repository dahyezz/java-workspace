//package com.example.java;
//
//import static org.junit.jupiter.api.Assertions.assertEquals;
//
//import java.sql.Connection;
//import java.sql.DriverManager;
//import java.sql.ResultSet;
//import java.sql.SQLException;
//import java.sql.Statement;
//import org.junit.jupiter.api.AfterAll;
//import org.junit.jupiter.api.BeforeAll;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Test;
//import org.testcontainers.containers.PostgreSQLContainer;
//import org.testcontainers.junit.jupiter.Testcontainers;
//
//@Testcontainers
////@Log4j2
//public class PostgresqlAccessTest {
//
//  static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:13.2")
//      .withDatabaseName("test")
//      .withUsername("test")
//      .withPassword("test");
//
//  Connection connection;
//
//  @BeforeAll
//  static void startContainer() {
//    postgres.start();
//    System.out.println("started : " + postgres.isRunning());
//  }
//
//  @AfterAll
//  static void stopContainer() {
//    postgres.stop();
//  }
//
//  @BeforeEach
//  public void setup() throws SQLException {
//    // 데이터베이스 연결
//    connection = DriverManager.getConnection(
//        postgres.getJdbcUrl(),
//        postgres.getUsername(),
//        postgres.getPassword()
//    );
//
//    // emp 테이블 생성
//    try (Statement statement = connection.createStatement()) {
//      statement.execute(
//          "CREATE TABLE emp (id SERIAL PRIMARY KEY, name VARCHAR(100), job VARCHAR(100));");
//      statement.execute("INSERT INTO emp (name, job) VALUES ('John Doe', 'Developer');");
//    }
//  }
//
//  @Test
//  public void test() throws Exception {
//    // emp 테이블에서 데이터 조회
//    try (Statement statement = connection.createStatement()) {
//      ResultSet resultSet = statement.executeQuery("SELECT name, job FROM emp WHERE id = 1;");
//      if (resultSet.next()) {
//        String name = resultSet.getString("name");
//        String job = resultSet.getString("job");
//        System.out.println("name - " + name);
//        System.out.println("job - " + job);
//
//        // 데이터 검증
//        assertEquals("John Doe", name);
//        assertEquals("Developer", job);
//      }
//    }
//  }
//}
