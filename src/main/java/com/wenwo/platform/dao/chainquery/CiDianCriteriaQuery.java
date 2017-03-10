package com.wenwo.platform.dao.chainquery;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;

import org.springframework.data.domain.Pageable;

public interface CiDianCriteriaQuery<T,PK extends Serializable> {	
	CiDianChainQueryer<T,PK> forExample(T example) throws Exception;
	CiDianChainQueryer<T,PK> pageable(Pageable pageable);
	CiDianChainQueryer<T,PK> sortAsc(String field);
	CiDianChainQueryer<T,PK> sortDesc(String field);
	CiDianChainQueryer<T,PK> notIn (String field,Collection<?> notInCollection);
	CiDianChainQueryer<T,PK> limit (int limit);
	CiDianChainQueryer<T,PK> lt (String field,Object object);
	CiDianChainQueryer<T,PK> gt (String field,Object object);
	CiDianChainQueryer<T,PK> regex(String field,String re);
	List<T> findList();
	T findOne();
	long count();
}
