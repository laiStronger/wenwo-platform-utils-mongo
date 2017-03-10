package com.wenwo.platform.dao.util;

import java.util.Collection;
import java.util.List;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.mapreduce.GroupBy;
import org.springframework.data.mongodb.core.mapreduce.GroupByResults;
import org.springframework.data.mongodb.core.mapreduce.MapReduceResults;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import com.wenwo.platform.exception.WenwoDaoException;
import com.wenwo.platform.paging.PageHolder;
import com.wenwo.platform.paging.PageInfo;

public interface IMongoUtil {
	/**
	 * 保存MongoDB实体
	 * 
	 * @param entity
	 * @return
	 */
	<T> T save(T entity);

	/**
	 * 分页查询MongoDB实体
	 * 
	 * @param clazz
	 * @param pageInfo
	 * @param sort
	 * @param criterias
	 * @return
	 */
	<T> List<T> getPageList(Class<T> clazz, PageInfo pageInfo, Sort sort, Criteria... criterias);

	/**
	 * 重载分页查询MongoDB实体
	 * 
	 * @param clazz
	 * @param pageInfo
	 * @param sort
	 * @param criterias
	 * @return
	 */
	<T> PageHolder<T> getPage(Class<T> clazz, PageInfo pageInfo, Sort sort, Criteria... criterias);

	/**
	 * 分页查询MongoDB实体
	 * 
	 * @param clazz
	 * @param pageInfo
	 * @param cs
	 * @param sort
	 * @return
	 */
	<T> PageHolder<T> getPage(Class<T> clazz, PageInfo pageInfo, Sort sort, List<Criteria> cs);

	<T> int getCount(Class<T> clazz, Criteria... criterias);

	<T> int getCount(Class<T> clazz, List<Criteria> criterias);

	int getCount(String document, Criteria... criterias);

	<T> List<T> getEntitysByIds(Class<T> clazz, Collection<String> questionIds);

	<T> T getEntityByKeyValue(Class<T> clazz, String key, Object value);

	<T> List<T> getEntitysByKeyValue(Class<T> clazz, String key, Object value);

	<T> List<T> getEntitysByKeyValues(Class<T> clazz, String key, List<Object> values);

	/**
	 * 
	 * @param clazz
	 * @param sort		排序字段，若没有排序需求，可传null
	 * @param criterias
	 * @return
	 */
	<T> List<T> getList(Class<T> clazz, Sort sort, Criteria... criterias);
	
	<T> List<T> getList(Class<T> clazz, Sort sort, int limit, Criteria... criterias);

	/**
	 * 根据ID更新一个实体。原来的update操作会丢失原来有的但实体中不包含的字段，本版本已经修正这个问题
	 * @param <T>
	 * @param entity
	 */
	<T> void update(T entity);

	<T> T getEntityById(Class<T> clazz, String id);

	<T> void remove(T entity);

	<T> void dropCollection(Class<T> entityClass);

	<T> List<T> findAll(Class<T> c);

	<T> List<T> find(Query query, Class<T> clazz);

	List<String> find(Query query, String collName, String fieldName);
	
	List<String> findId(Query query, String collectionName);

	<T> T getEntity(Class<T> clazz, Criteria... criterias);

	<T> int updateMulti(Query query, Update update, Class<T> clazz);
	
	<T> int updateFirst(Query query, Update update, Class<T> clazz);

	<T> GroupByResults<T> group(String inputCollectionName, GroupBy groupBy, Class<T> entityClass);

	<T> GroupByResults<T> group(Criteria criteria, String inputCollectionName, GroupBy groupBy, Class<T> entityClass);

	String getCollectionName(Class<?> entityClass);

	void insertAll(Collection<? extends Object> objectsToSave);
	
	/**
	 * 获取一个子文档的方法
	 * @param <T>
	 * @param id
	 * @param collectionName
	 * @param subDocumentName
	 * @param clazz
	 * @return
	 * @throws WenwoDaoException
	 */
	<T> T getSubEntityById(String id, String collectionName, String subDocumentName, Class<T> clazz);
	
	/**
	 * 保存子文档
	 * @param <T>
	 * @param id
	 * @param collectionName
	 * @param subDocumentName
	 * @param entity
	 * @throws WenwoDaoException
	 */
	<T> void saveSubEntityById(String id, String collectionName, String subDocumentName, T entity);

	public <T> void findAndRemove(Query query, Class<T> clazz);
	
	public <T> MapReduceResults<T> mapReduce(Query query, String inputCollectionName, String mapFunction,
			String reduceFunction, Class<T> entityClass);

	<T> T getEntityByQuery(Class<T> clazz, Query query);

	<T> Page<T> findPage(Class<T> clazz, Query query, Pageable pageable, Sort... sort);
}
