package com.bsn.impalaquery.test2.controller;

import com.bsn.impalaquery.test2.service.Test2Service;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.xml.ws.Action;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@RestController
@Slf4j
public class Test2Controller {

    @Autowired
    Test2Service test2Service;
//    @RequestMapping(value = "/selectTest2", method = RequestMethod.POST)
//    public Map<String, Object> selectTest2(@RequestBody Map<String, Object> requestParam) {
//        log.info("[v] ---------- RestFul API CALL!! , param : {} ----------", requestParam);
//        //Map<String, String> parameters = new HashMap<>();
//        //parameters.put("field_1", "c");
//        Map<String, Object> response = new HashMap<>();
//        List<Map<String, Object>> responses = new ArrayList<>();
//        try {
//            responses = test2Service.selectTest2(requestParam);
//            log.info(responses.toString());
//        } catch (Exception e) {
//            log.error("Error occurred while calling the API: {}", e.getMessage(), e);
//            response = new HashMap<>();
//            response.put("statusCode", 500);
//            response.put("errorMessage", "Internal Server Error: " + e.getMessage());
//        }
//
//        return response;
//    }

    @RequestMapping(value = "/selectTest2", method = RequestMethod.POST)
    public CompletableFuture<List<Map<Object, String>>> selectTest2(@RequestBody Map<String, Object> requestParam) {
        CompletableFuture<List<Map<Object, String>>> data1Future  = test2Service.selectTest2(requestParam);
        CompletableFuture<Integer> countFuture  = test2Service.selectTest3(requestParam);

        return CompletableFuture.allOf(data1Future, countFuture)
                .thenApply(v -> {
                    List<Map<Object, String>> data1 = data1Future.join(); // data1 리스트
                    int count = countFuture.join(); // int count 값 가져오기

                    // 🔹 data1의 모든 항목에 "count" 추가
                    for (Map<Object, String> item : data1) {
                        item.put("count", String.valueOf(count)); // count 추가
                    }

                    return data1; // data1만 반환
                });
    }
}
