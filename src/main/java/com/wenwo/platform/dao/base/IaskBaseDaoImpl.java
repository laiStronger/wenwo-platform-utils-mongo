package com.wenwo.platform.dao.base;

import java.io.Serializable;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.IaskWenwoMongoTemplate;
import org.springframework.data.mongodb.core.mapreduce.GroupBy;
import org.springframework.data.mongodb.core.mapreduce.GroupByResults;
import org.springframework.data.mongodb.core.mapreduce.MapReduceResults;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.ReadPreference;
import com.mongodb.WriteResult;
import com.wenwo.platform.paging.PagedataImpl;

/**
 * 数据访问基类实现类
 * 
 * @author jy.hu
 * 
 * @param <T>
 *            类
 * @param <PK>
 *            主键
 */
public class IaskBaseDaoImpl<T, PK extends Serializable> implements IaskBaseDao<T, PK> {

    //private static final Logger LOGGER = LoggerFactory.getLogger(IaskBaseDaoImpl.class);

    private static final String ID_FIELD = "_id";

    private IaskWenwoMongoTemplate iaskMongoTemplate;

    public void setIaskMongoTemplate(IaskWenwoMongoTemplate iaskMongoTemplate) {
		this.iaskMongoTemplate = iaskMongoTemplate;
		this.iaskMongoTemplate.setReadPreference(ReadPreference.secondaryPreferred());
	}

	protected Class<T> entityClass;

    @SuppressWarnings("unchecked")
    public IaskBaseDaoImpl() {
        Class<?> c = getClass();
        Type type = c.getGenericSuperclass();
        if (type instanceof ParameterizedType) {
            Type[] parameterizedType = ((ParameterizedType) type).getActualTypeArguments();
            this.entityClass = (Class<T>) parameterizedType[0];
        }
    }

    @Override
    public T findById(PK id) {
        return iaskMongoTemplate.findById(id, entityClass);
    }

    @Override
    public T findOne(Query query) {
        return iaskMongoTemplate.findOne(query, entityClass);
    }

    @Override
    public List<T> findList(Query query) {
        Sort sort = null;
        return findList(query, 0, sort);
    }

    @Override
    public List<T> findList(Query query, Sort... sort) {
        return findList(query, 0, sort);
    }

    @Override
    public List<T> findList(Query query, int limit) {
        Sort sort = null;
        return findList(query, limit, sort);
    }

    @Override
    public List<T> findList(Query query, int limit, Sort... sort) {
        if (query == null)
            query = new Query();
        if (sort != null) {
            for (Sort s : sort) {
                query.with(s);
            }
        }
        if (limit > 0)
            query.limit(limit);
        return iaskMongoTemplate.find(query, entityClass);
    }

    @Override
    public List<T> findListByKeyValue(String key, Object value) {
        return findList(Query.query(Criteria.where(key).is(value)));
    }

    @Override
    public List<T> findListByKeyValues(String key, Collection<String> values) {
        return findList(Query.query(Criteria.where(key).in(values)));
    }

    @Override
    public List<T> findListByIds(Collection<PK> ids) {
        Sort sort = null;
        return findListByIds(ids, sort);
    }

    @Override
    public List<T> findListByIds(Collection<PK> ids, Sort... sort) {
        Query query = Query.query(Criteria.where(ID_FIELD).in(ids));
        if (sort != null) {
            for (Sort s : sort) {
                query.with(s);
            }
        }
        return findList(query);
    }

    @Override
    public List<T> findAll() {
        return iaskMongoTemplate.findAll(entityClass);
    }

    @Override
    public Page<T> findPage(Query query, Pageable pageable, Sort... sort) {
        if (query == null) {
            query = new Query();
        }
        Long total = getCount(query);
        if (sort != null) {
            for (Sort s : sort) {
                query.with(s);
            }
        }
        query.with(pageable);
        List<T> content = findList(query);
        return new PagedataImpl<T>(content, pageable, total.longValue());
    }

    @Override
    public Long getCount(Query query) {
        return iaskMongoTemplate.count(query, entityClass);
    }

    @Override
    public Long getCount(String key, Object val) {
        Query query = Query.query(Criteria.where(key).is(val));
        return getCount(query);
    }

    @Override
    public T insert(T entity) {
        iaskMongoTemplate.insert(entity);
        return entity;
    }
    
    @Override
    public T save(T entity) {
        iaskMongoTemplate.save(entity);
        return entity;
    }

    @Override
    public void save(Collection<T> entitys) {
        iaskMongoTemplate.insertAll(entitys);
    }

    @Override
    public void update(T entity) {
        iaskMongoTemplate.update(entity);
    }

    @Override
    public int updateById(PK id, Update update) {
        Query query = Query.query(Criteria.where(ID_FIELD).is(id));
        return update(query, update, true);
    }

    @Override
    public int updateByIds(Collection<PK> ids, Update update) {
        Query query = Query.query(Criteria.where(ID_FIELD).in(ids));
        return update(query, update, false);
    }

    @Override
    public int upsert(Query query, Update update) {
        WriteResult wr = iaskMongoTemplate.upsert(query, update, entityClass);
        return wr.getN();
    }

    @Override
    public int update(Query query, Update update, boolean isFirst) {
        WriteResult wr = null;
        if (isFirst) {
            wr = iaskMongoTemplate.updateFirst(query, update, entityClass);
        } else {
            wr = iaskMongoTemplate.updateMulti(query, update, entityClass);
        }
        return wr.getN();
    }

    @Override
    public void remove(Query query) {
        iaskMongoTemplate.remove(query, entityClass);
    }

    @Override
    public void remove(PK id) {
        remove(Query.query(Criteria.where(ID_FIELD).is(id)));
    }

    @Override
    public void remove(T entity) {
        iaskMongoTemplate.remove(entity);
    }

    @Override
    public void remove(Collection<PK> ids) {
        for (PK id : ids) {
            remove(id);
        }
    }

    /*
     * @Override public void dropTable(String collectionName) {
     * iaskMongoTemplate.dropCollection(collectionName); }
     * 
     * @Override public void dropTable() {
     * iaskMongoTemplate.dropCollection(entityClass); }
     */

    @Override
    public <O> MapReduceResults<O> mapReduce(Query query, String inputCollectionName, String mapFunction, String reduceFunction, Class<O> clazz) {
        return iaskMongoTemplate.mapReduce(query, inputCollectionName, mapFunction, reduceFunction, clazz);
    }

    @Override
    public <O> GroupByResults<O> group(Criteria criteria, String inputCollectionName, GroupBy groupBy, Class<O> clazz) {
        return iaskMongoTemplate.group(criteria, inputCollectionName, groupBy, clazz);
    }

    @Override
    public String getCollectionName() {
        return iaskMongoTemplate.getCollectionName(entityClass);
    }

    @Override
    public String getCollectionName(Class<?> clazz) {
        return iaskMongoTemplate.getCollectionName(clazz);
    }

    @Override
    public <O> O findSub(PK id, Class<O> subClazz) {
        return findSub(id, subClazz, null);
    }

    @Override
    public <O> O findSub(PK id, Class<O> subClazz, String subName) {
        String collName = getCollectionName();
        if (StringUtils.isBlank(subName)) {
            subName = getCollectionName(subClazz);
        }
        return iaskMongoTemplate.findSubEntityById(id, collName, subClazz, subName);
    }

    @Override
    public List<String> findField(Query query, String fieldName) {
        List<String> fieldList = null;
        String collectionName = getCollectionName();
        if (iaskMongoTemplate.collectionExists(collectionName)) {
            DBObject fld = new BasicDBObject(fieldName, 1);
            DBObject obj = query.getQueryObject();
            DBCursor cursor = iaskMongoTemplate.getCollection(collectionName).find(obj, fld);
            if (query.getSortObject() != null) {
                cursor.sort(query.getSortObject());
            }
            if (query.getLimit() > 0) {
                cursor.limit(query.getLimit());
            }
            fieldList = new ArrayList<String>();
            while (cursor.hasNext()) {
                obj = cursor.next();
                if (obj.containsField(fieldName)) {
                    fieldList.add(obj.get(fieldName).toString());
                }
            }
            cursor.close();
        }
        return fieldList;
    }

    @Override
    public List<String> findId(Query query) {
        return findField(query, ID_FIELD);
    }

    @Override
    public <O> void saveSub(PK id, O o) {
        saveSub(id, o, null);
    }

    @Override
    public <O> void saveSub(PK id, O o, String subName) {
        String collName = getCollectionName();
        if (StringUtils.isBlank(subName)) {
            subName = getCollectionName(o.getClass());
        }
        iaskMongoTemplate.saveSubEntityById(id.toString(), collName, subName, o);
    }

    @Override
    public void findAndRemove(Query query) {
        iaskMongoTemplate.findAndRemove(query, entityClass);

    }

    @Override
    public void findAndUpdate(Query query, Update update) {
        iaskMongoTemplate.findAndModify(query, update, entityClass);
    }

	@Override
	public IaskWenwoMongoTemplate getMongoTemplate() {
		return iaskMongoTemplate;
	}
}
