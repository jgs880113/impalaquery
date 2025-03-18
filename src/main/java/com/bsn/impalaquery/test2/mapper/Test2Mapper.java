package com.bsn.impalaquery.test2.mapper;

import org.apache.ibatis.annotations.Mapper;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Mapper
public interface Test2Mapper {
	List<Map<Object, String>>  selectTest2(Map<String, Object> requestParam);
	int selectTest3(Map<String, Object> requestParam);

	List<Map<String, Object>>  selectTest22(Map<String, Object> requestParam);
}
