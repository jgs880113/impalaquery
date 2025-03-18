package com.bsn.impalaquery.test2.service;

import com.bsn.impalaquery.test2.mapper.Test2Mapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Service
public class Test2Service {

    @Autowired
    Test2Mapper test2Mapper;

    @Autowired
    private Executor asyncExecutor;

//    @Async("asyncExecutor")
//    public CompletableFuture<List<Map<Object, String>>> selectTest2(Map<String, Object> requestParam) throws Exception {
//        return test2Mapper.selectTest2(requestParam);
//    }
//
//    @Async("asyncExecutor")
//    public CompletableFuture<List<Map<Object, String>>> selectTest3(Map<String, Object> requestParam) throws Exception {
//        return test2Mapper.selectTest3(requestParam);
//    }

    public CompletableFuture<List<Map<Object, String>>> selectTest2(Map<String, Object> requestParam) {
        return CompletableFuture.supplyAsync(() -> { //비동기 실행
            long start = System.currentTimeMillis();
            System.out.println("selectTest2 시작 - 실행 스레드: " + Thread.currentThread().getName());

            List<Map<Object, String>> data = test2Mapper.selectTest2(requestParam); //MyBatis 비동기 실행

            long end = System.currentTimeMillis();
            System.out.println("selectTest2 완료 - 실행 시간: " + (end - start) + "ms");
            return data;
        }, asyncExecutor);
    }

    public CompletableFuture<Integer> selectTest3(Map<String, Object> requestParam) {
        return CompletableFuture.supplyAsync(() -> { //비동기 실행
            long start = System.currentTimeMillis();
            System.out.println("selectTest3 시작 - 실행 스레드: " + Thread.currentThread().getName());

            int data = test2Mapper.selectTest3(requestParam); // MyBatis 비동기 실행
            //int data = 1;
            long end = System.currentTimeMillis();
            System.out.println("selectTest3 완료 - 실행 시간: " + (end - start) + "ms");
            return data;
        }, asyncExecutor);
    }

    public List<Map<String, Object>> selectTest22(Map<String, Object> requestParam) {
        List<Map<String, Object>> data = test2Mapper.selectTest22(requestParam);
        return data;
    }
}
