package com.bsn.impalaquery;

import com.bsn.impalaquery.config.ImpalaConnector;
import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;


/*
08001: 클라이언트가 서버에 연결할 수 없음
08003: 연결이 열려 있지 않음
08006: 연결 오류
22000: 데이터 예외
22001: 문자열 데이터, 오른쪽 잘림
22002: NULL 값 없음
23000: 무결성 제약 조건 위반
42000: 구문 오류 또는 접근 권한 위반
42S02: 테이블 또는 뷰가 존재하지 않음
HY000: 일반 오류
*/

@Slf4j
@SpringBootTest
class ImpalaqueryApplicationTests {
	@Value("${connection.url:jdbc:impala://192.168.75.139:21050}")
	private String connectionUrl;
	@Value("${jdbc.driver.class.name:com.cloudera.impala.jdbc41.Driver}")
	private String jdbcDriverName;

	@Value("${connection.max.retries:3}")
	private int maxRetries;

	@Test
	void contextLoads2() {
		log.info("test");
	}
	@Test
	void contextLoads() {
		int retryCount = 0;

		ObjectMapper objectMapper = new ObjectMapper();
		List<Map<String, String>> resultList = new ArrayList<>();
		Map<String, String> parameters = new HashMap<>();
		parameters.put("field_1", "c");

		String sqlStatement = "SELECT field_1, field_2, field_3 FROM bsf.test2 WHERE field_1 = ? order by field_1 desc;";
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		while (retryCount < maxRetries) {
			try {
				DriverManager.setLoginTimeout(20); // 20초
				Class.forName(jdbcDriverName);
				Connection originalConnection = DriverManager.getConnection(connectionUrl);
				//con = DriverManager.getConnection(connectionUrl);
				con = new ImpalaConnector(originalConnection); // 커스텀 Connection 사용

				pstmt = con.prepareStatement(sqlStatement);
				pstmt.setString(1, parameters.get("field_1"));

				rs = pstmt.executeQuery();

				//Statement stmt = con.createStatement();
				//ResultSet rs = stmt.executeQuery(sqlStatement);

				while (rs.next()) {
					Map<String, String> row = new HashMap<>();
					row.put("field_1", rs.getString("field_1"));
					row.put("field_2", rs.getString("field_2"));
					row.put("field_3", rs.getString("field_3"));
					resultList.add(row);
				}
				String jsonResult = objectMapper.writeValueAsString(resultList);
				log.info("[v] jsonResult : {}", jsonResult);
				break;
			} catch (ClassNotFoundException e) {
				// 500 Internal Server Error: JDBC 드라이버 클래스를 찾을 수 없음
				e.printStackTrace();
				break;
			} catch (SQLException e) {
				retryCount++;
				log.info("[v] SQLException occurred. Retrying... (Attempt {})", retryCount);

				// 데이터베이스 관련 예외 처리
				if (e.getMessage().contains("Connection timed out") || e.getMessage().contains("timeout")) {
					// 504 Gateway Timeout: 데이터베이스 연결 시간 초과
					log.info("[v] Connection to the database timed out.");
				} else if (e.getSQLState().equals("08001")) {
					// 503 Service Unavailable: 데이터베이스 서버에 연결할 수 없음
					log.info("[v] Database server is unavailable.");
				} else if (e.getSQLState().equals("28000")) {
					// 401 Unauthorized: 데이터베이스 인증 실패
					log.info("[v] Database authentication failed.");
				} else {
					// 500 Internal Server Error: 기타 SQL 예외 처리
					log.info("[v] Database authentication failed. : {}", e.getMessage());
					//e.printStackTrace();
				}

				if (retryCount >= maxRetries) {
					log.info("[v] Maximum retries exceeded.");
					//e.printStackTrace();
					break;
				}

			} catch (JsonProcessingException e) {
				// 500 Internal Server Error: JSON 처리 예외
				//e.printStackTrace();
				log.info("JsonProcessingException : {}", e.getMessage());
				break;
			} catch (Exception e) {
				// 500 Internal Server Error: 기타 예외 처리
				//e.printStackTrace();
				log.info("Exception : {}", e.getMessage());
				break;
			} finally {
				try {
					if (rs != null) {
						rs.close();
					}
					if (pstmt != null) {
						pstmt.close();
					}
					if (con != null) {
						con.close();
					}
				} catch (SQLException e) {
					// 500 Internal Server Error: 리소스 닫기 예외 처리
					// e.printStackTrace();
					log.info("finally Exception : {}", e.getMessage());
				}
			}
		}
	}
}

