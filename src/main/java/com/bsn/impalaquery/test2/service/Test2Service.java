package com.bsn.impalaquery.test2.service;

import com.bsn.impalaquery.test2.mapper.Test2Mapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Service
public class Test2Service {

    @Autowired
    Test2Mapper test2Mapper;

//    @Async("asyncExecutor")
//    public CompletableFuture<List<Map<Object, String>>> selectTest2(Map<String, Object> requestParam) throws Exception {
//        return test2Mapper.selectTest2(requestParam);
//    }
//
//    @Async("asyncExecutor")
//    public CompletableFuture<List<Map<Object, String>>> selectTest3(Map<String, Object> requestParam) throws Exception {
//        return test2Mapper.selectTest3(requestParam);
//    }

    @Async("asyncExecutor") // 특정 스레드 풀 사용
    public CompletableFuture<List<Map<Object, String>>> selectTest2(Map<String, Object> requestParam) {
        System.out.println("selectTest2");
        System.out.println("selectTest2 executed on thread: " + Thread.currentThread().getName());
        List<Map<Object, String>> data = test2Mapper.selectTest2(requestParam);
        return CompletableFuture.completedFuture(data);
    }

    @Async("asyncExecutor") // 특정 스레드 풀 사용
    public CompletableFuture<Integer> selectTest3(Map<String, Object> requestParam) {
        System.out.println("selectTest3");
        System.out.println("selectTest3 executed on thread: " + Thread.currentThread().getName());
        int data = test2Mapper.selectTest3(requestParam);
        return CompletableFuture.completedFuture(data);
    }
}
