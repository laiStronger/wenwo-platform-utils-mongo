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
import com.wenwo.platform.utils.ReflectionUtil;
/**
 * 
 * @author fengyitian
 *
 * @param <T>
 * @param <PK>
 */
public class CiDianChainQueryer<T,PK extends Serializable> implements CiDianCriteriaQuery<T,PK>{
	private Query query ;
	private CiDianBaseDao<T,PK> dao ;
	public CiDianChainQueryer(CiDianBaseDao<T,PK> dao){
		 this.dao = dao;
		 this.query = new Query();
	}
	@Override
	public CiDianChainQueryer<T,PK> forExample(T example) throws Exception {		
		@SuppressWarnings("rawtypes")
		Class clazz = example.getClass();   	
    	Map<String,Object> queryMap = ReflectionUtil.resolve(clazz, example);   	
    	for(Entry<String, Object> entry : queryMap.entrySet()){
    		query.addCriteria( Criteria.where( entry.getKey() ).is( entry.getValue() ) );
    	}
		return this;
	}
	@Override
	public CiDianChainQueryer<T,PK> pageable(Pageable pageable)  {
		query.with(pageable);
		return this;
	}
	@Override
	public CiDianChainQueryer<T,PK> sortAsc(String field){
		query.with(new Sort(Direction.ASC, field));
		return this;
	}
	@Override
	public CiDianChainQueryer<T,PK> sortDesc(String field){
		query.with(new Sort(Direction.DESC, field));
		return this;
	}
	@Override
	public CiDianChainQueryer<T, PK> notIn(String field,Collection<?> notInCollection) {
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
	public CiDianChainQueryer<T, PK> limit(int limit) {
		query.limit(limit);
		return this;
	}
	@Override
	public CiDianChainQueryer<T, PK> lt(String field,Object object) {
		query.addCriteria(Criteria.where(field).lt(object));
		return this;
	}
	@Override
	public CiDianChainQueryer<T, PK> gt(String field,Object object) {
		query.addCriteria(Criteria.where(field).gt(object));
		return this;
	}
	@Override
	public long  count(){
		return dao.getCount(query);
	}
	@Override
	public CiDianChainQueryer<T, PK> regex(String field, String re) {
		query.addCriteria(Criteria.where(field).regex(re));
		return this;
	}
	
}
