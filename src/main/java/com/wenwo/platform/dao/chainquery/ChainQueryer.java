package com.wenwo.platform.dao.chainquery;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import com.wenwo.platform.dao.base.BaseDao;
import com.wenwo.platform.dao.base.CiDianBaseDao;
import com.wenwo.platform.paging.PageableImpl;
import com.wenwo.platform.utils.ReflectionUtil;

public class ChainQueryer<T,PK extends Serializable> implements CriteriaQuery<T,PK>{
	private Query query ;
	private BaseDao<T,PK> dao ;
	public ChainQueryer(BaseDao<T,PK> dao){
		 this.dao = dao;
		 this.query = new Query();
	}
	@Override
	public ChainQueryer<T,PK> forExample(T example) throws Exception {		
		@SuppressWarnings("rawtypes")
		Class clazz = example.getClass();   	
    	Map<String,Object> queryMap = ReflectionUtil.resolve(clazz, example);   	
    	for(Entry<String, Object> entry : queryMap.entrySet()){
    		query.addCriteria( Criteria.where( entry.getKey() ).is( entry.getValue() ) );
    	}
		return this;
	}
	@Override
	public ChainQueryer<T,PK> pageable(Pageable pageable)  {
		query.with(pageable);
		return this;
	}
	@Override
	public ChainQueryer<T,PK> pageable(int page,int size)  {
		query.with(new PageableImpl(page, size));
		return this;
	}
	@Override
	public ChainQueryer<T,PK> sortAsc(String field){
		query.with(new Sort(Direction.ASC, field));
		return this;
	}
	@Override
	public ChainQueryer<T,PK> sortDesc(String field){
		query.with(new Sort(Direction.DESC, field));
		return this;
	}
	@Override
	public ChainQueryer<T, PK> notIn(String field,Collection<?> notInCollection) {
		query.addCriteria(Criteria.where(field).nin(notInCollection));
		return this;
	}
	@Override
	public List<T> findList() {		
		return dao.findList(query);
	}
	@Override
	public T findOne() {
		return dao.findOne(query);
	}
	@Override
	public ChainQueryer<T, PK> limit(int limit) {
		query.limit(limit);
		return this;
	}
	@Override
	public ChainQueryer<T, PK> lt(String field,Object object) {
		query.addCriteria(Criteria.where(field).lt(object));
		return this;
	}
	@Override
	public ChainQueryer<T, PK> gt(String field,Object object) {
		query.addCriteria(Criteria.where(field).gt(object));
		return this;
	}
	@Override
	public long  count(){
		return dao.getCount(query);
	}
	@Override
	public ChainQueryer<T, PK> regex(String field, String re) {
		query.addCriteria(Criteria.where(field).regex(re));
		return this;
	}
	@Override
	public ChainQueryer<T, PK> notEq(String field, Object o) {
		query.addCriteria(Criteria.where(field).ne(o));
		return this;
	}
	
}