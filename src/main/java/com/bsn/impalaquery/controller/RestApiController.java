package com.bsn.impalaquery.controller;

import com.bsn.impalaquery.service.RestApiService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

@RestController
@Slf4j
public class RestApiController {

    @Autowired
    private RestApiService restApiService;
    @RequestMapping(value = "test",method = RequestMethod.GET)
    public Map<String, Object> executor(){
        log.info("test");
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("field_1", "c");

        Map<String, Object> response = restApiService.contextLoads(parameters);
        return response;
    }

    @RequestMapping(value = "/api/impala", method = RequestMethod.POST)
    public Map<String, Object> executor(@RequestBody Map<String, Object> requestParam) {
        log.info("[v] ---------- RestFul API CALL!! , param : {} ----------", requestParam);
        //Map<String, String> parameters = new HashMap<>();
        //parameters.put("field_1", "c");
        Map<String, Object> response = new HashMap<>();
        try {
            response = restApiService.contextLoads(requestParam);
            log.info(response.toString());
        } catch (Exception e) {
            log.error("Error occurred while calling the API: {}", e.getMessage(), e);
            response = new HashMap<>();
            response.put("statusCode", 500);
            response.put("errorMessage", "Internal Server Error: " + e.getMessage());
        }

        return response;
    }
}
