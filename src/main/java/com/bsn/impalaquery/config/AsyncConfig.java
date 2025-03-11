package com.bsn.impalaquery.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import java.util.concurrent.Executor;

@Configuration
@EnableAsync
public class AsyncConfig {

    @Bean(name = "asyncExecutor") // 별도의 스레드 풀을 사용하도록 설정
    public Executor asyncExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(5);  // 기본적으로 실행할 스레드 개수
        executor.setMaxPoolSize(10);  // 최대 스레드 개수
        executor.setQueueCapacity(100); // 작업 큐 크기 (대기열)
        executor.setThreadNamePrefix("AsyncThread-"); // 스레드 이름 설정
        executor.initialize();
        return executor;
    }
}