package com.bsn.impalaquery;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@EnableAsync
public class ImpalaqueryApplication {

	public static void main(String[] args) {
		SpringApplication.run(ImpalaqueryApplication.class, args);
	}

}
