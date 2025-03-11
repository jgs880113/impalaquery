package com.bsn.impalaquery.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class RestApiService {
    @Value("${connection.url:jdbc:impala://192.168.75.139:21050}")
    private String connectionUrl;
    @Value("${jdbc.driver.class.name:com.cloudera.impala.jdbc41.Driver}")
    private String jdbcDriverName;
    @Value("${connection.max.retries:3}")
    private int maxRetries;

    public Map<String, Object> contextLoads(Map<String, Object> parameters) {
        Map<String, Object> returnMap = new HashMap<>();
        String returnFlag = "fail";
        String statusCode = "200";
        String errorMessage = "";

        int retryCount = 0;

        ObjectMapper objectMapper = new ObjectMapper();
        List<Map<String, String>> resultList = new ArrayList<>();
        //Map<String, String> parameters = new HashMap<>();
        //parameters.put("field_1", "c");

        String sqlStatement = "SELECT field_1, field_2, field_3 FROM bsf.test2 WHERE field_1 = ? order by field_1 desc;";
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        while (retryCount < maxRetries) {
            try {
                DriverManager.setLoginTimeout(20); // 20초
                Class.forName(jdbcDriverName);
                con = DriverManager.getConnection(connectionUrl);

                pstmt = con.prepareStatement(sqlStatement);
                pstmt.setString(1, (String) parameters.get("field_1"));

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
                //String jsonResult = objectMapper.writeValueAsString(resultList);
                //log.info("[v] jsonResult : {}", jsonResult);
                returnFlag = "success";
                statusCode = "200";
                break;
            } catch (ClassNotFoundException e) {
                // 500 Internal Server Error: JDBC 드라이버 클래스를 찾을 수 없음
                log.info("[x] ClassNotFoundException : {}", e.getMessage());
                returnFlag = "fail";
                statusCode = "500";
                errorMessage = "JDBC Error : " + e.getMessage();
                e.printStackTrace();
                break;
            } catch (SQLException e) {
                retryCount++;
                returnFlag = "fail";
                log.info("[x] SQLException occurred. Retrying... (Attempt {})", retryCount);

                // 데이터베이스 관련 예외 처리
                if (e.getMessage().contains("Connection timed out") || e.getMessage().contains("timeout")) {
                    // 504 Gateway Timeout: 데이터베이스 연결 시간 초과
                    log.info("[x] Connection to the database timed out : {}", e.getMessage());
                    statusCode = "504";
                    errorMessage = "Connection to the database timed out : " + e.getMessage();
                } else if (e.getSQLState().equals("08001")) {
                    // 503 Service Unavailable: 데이터베이스 서버에 연결할 수 없음
                    log.info("[x] Database server is unavailable : {}", e.getMessage());
                    statusCode = "503";
                    errorMessage = "Database server is unavailable : " + e.getMessage();
                } else if (e.getSQLState().equals("28000")) {
                    // 401 Unauthorized: 데이터베이스 인증 실패
                    log.info("[x] Database authentication failed : {}", e.getMessage());
                    statusCode = "401";
                    errorMessage = "Database authentication failed : " + e.getMessage();
                } else {
                    // 500 Internal Server Error: 기타 SQL 예외 처리
                    log.info("[x] Internal Server Error : {}", e.getMessage());
                    statusCode = "500";
                    errorMessage = "Internal Server Error : " + e.getMessage();
                }

                if (retryCount >= maxRetries) {
                    log.info("[x] Maximum retries exceeded");
                    statusCode = "500";
                    errorMessage = "Maximum retries exceeded";
                    //e.printStackTrace();
                    break;
                }

            }
            /*
            catch (JsonProcessingException e) {
                // 500 Internal Server Error: JSON 처리 예외
                //e.printStackTrace();
                log.info("JsonProcessingException : {}", e.getMessage());
                break;
            }
            */
            catch (Exception e) {
                // 500 Internal Server Error: 기타 예외 처리
                //e.printStackTrace();
                statusCode = "500";
                errorMessage = "Internal Server Error : " + e.getMessage();
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
                    statusCode = "500";
                    errorMessage = "Resource close Error : " + e.getMessage();
                    log.info("finally Exception Resource close Error : {}", e.getMessage());
                }
            }
        }
        returnMap.put("statusCode", statusCode);
        returnMap.put("returnFlag", returnFlag);
        returnMap.put("errorMessage", errorMessage);
        returnMap.put("resultList", resultList);
        return returnMap;
    }
}
