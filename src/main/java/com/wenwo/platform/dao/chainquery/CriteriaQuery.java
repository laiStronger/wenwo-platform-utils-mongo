package com.wenwo.platform.dao.chainquery;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;

import org.springframework.data.domain.Pageable;

public interface CriteriaQuery<T,PK extends Serializable> {	
	ChainQueryer<T,PK> forExample(T example) throws Exception;
	ChainQueryer<T,PK> pageable(Pageable pageable);
	ChainQueryer<T,PK> pageable(int page,int size);
	ChainQueryer<T,PK> sortAsc(String field);
	ChainQueryer<T,PK> sortDesc(String field);
	ChainQueryer<T,PK> notIn (String field,Collection<?> notInCollection);
	ChainQueryer<T,PK> limit (int limit);
	ChainQueryer<T,PK> lt (String field,Object object);
	ChainQueryer<T,PK> gt (String field,Object object);
	ChainQueryer<T,PK> regex(String field,String re);
	ChainQueryer<T,PK> notEq(String field,Object o);
	List<T> findList();
	T findOne();
	long count();
}
