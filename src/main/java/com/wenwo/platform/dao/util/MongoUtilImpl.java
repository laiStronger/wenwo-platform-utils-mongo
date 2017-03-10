package com.wenwo.platform.dao.util;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.DocumentCallbackHandler;
import org.springframework.data.mongodb.core.WenwoMongoTemplate;
import org.springframework.data.mongodb.core.mapreduce.GroupBy;
import org.springframework.data.mongodb.core.mapreduce.GroupByResults;
import org.springframework.data.mongodb.core.mapreduce.MapReduceResults;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.mongodb.ReadPreference;
import com.mongodb.WriteResult;
import com.wenwo.platform.paging.PageHolder;
import com.wenwo.platform.paging.PageInfo;
import com.wenwo.platform.paging.PagedataImpl;

public class MongoUtilImpl implements IMongoUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoUtilImpl.class);

    private WenwoMongoTemplate mongoTemplate;

    private WenwoMongoTemplate realtimeTemplate;

    public void setMongoTemplate(WenwoMongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
        try {
            this.realtimeTemplate = mongoTemplate.clone();
            realtimeTemplate.setReadPreference(ReadPreference.primary());
        } catch (CloneNotSupportedException e) {
            LOGGER.error("Exception while cloning WenwoMongoTemplate.", e);
        }
    }

    @Override
    public <T> void dropCollection(Class<T> clazz) {
        mongoTemplate.dropCollection(clazz);
    }

    @Override
    public <T> T save(T entity) {
        mongoTemplate.save(entity);
        return entity;
    }

    public <T> List<T> getPageList(Class<T> clazz, PageInfo pageInfo, List<Criteria> criterias, Sort sort) {
        Query query = new Query();
        if (criterias != null) {
            for (Criteria cr : criterias) {
                query.addCriteria(cr);
            }
        }
        query.limit(pageInfo.getPageSize());
        // skip 是int， 是否有问题？？未来数据量肯定很大。。。
        query.skip(pageInfo.getOffset());
        // 排序
        if (sort != null) {
            query.with(sort);
        }
        return mongoTemplate.find(query, clazz);
    }

    @Override
    public <T> List<T> getPageList(Class<T> clazz, PageInfo pageInfo, Sort sort, Criteria... criterias) {
        List<Criteria> cs = new ArrayList<Criteria>();
        if (criterias != null) {
            for (Criteria cr : criterias) {
                cs.add(cr);
            }
        }
        return getPageList(clazz, pageInfo, cs, sort);
    }

    @Override
    public <T> Page<T> findPage(Class<T> clazz, Query query, Pageable pageable, Sort... sort) {
        if (query == null) {
            query = new Query();
        }
        Long total = mongoTemplate.count(query, clazz);
        if (sort != null) {
            for (Sort s : sort) {
                query.with(s);
            }
        }
        query.with(pageable);
        List<T> content = mongoTemplate.find(query, clazz);
        return new PagedataImpl<T>(content, pageable, total.longValue());
    }

    @Override
    public <T> int getCount(Class<T> clazz, Criteria... criterias) {
        if (criterias != null && criterias.length > 0) {
            Query query = new Query();
            for (Criteria cr : criterias) {
                query.addCriteria(cr);
            }
            return (int) mongoTemplate.count(query, clazz);
        } else {
            return (int) mongoTemplate.count(null, clazz);
        }
    }

    @Override
    public <T> int getCount(Class<T> clazz, List<Criteria> criterias) {
        if (criterias != null && criterias.size() > 0) {
            Query query = new Query();
            for (Criteria cr : criterias) {
                query.addCriteria(cr);
            }
            return (int) mongoTemplate.count(query, clazz);
        } else {
            return (int) mongoTemplate.count(null, clazz);
        }
    }

    @Override
    public int getCount(String document, Criteria... criterias) {
        if (criterias != null) {
            Query query = new Query();
            if (criterias != null) {
                for (Criteria cr : criterias) {
                    query.addCriteria(cr);
                }
            }
            return (int) mongoTemplate.count(query, document);
        } else {
            return (int) mongoTemplate.count(null, document);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> List<T> getEntitysByIds(Class<T> clazz, Collection<String> idlist) {
        Query query = new Query();
        query.addCriteria(Criteria.where("_id").in(idlist));
        List<T> entities = mongoTemplate.find(query, clazz);
        List<T> sortList = new ArrayList<T>();
        try {
            if (entities != null) {
                for (String id : idlist) {
                    for (Object entity : entities) {
                        Field field = entity.getClass().getDeclaredField("id");
                        field.setAccessible(true);
                        if (id.equals(field.get(entity).toString())) {
                            sortList.add((T) entity);
                        }
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error("Exception while sort getEntitysByIds.", e);
        }
        return sortList;
    }

    @Override
    public <T> PageHolder<T> getPage(Class<T> clazz, PageInfo pageInfo, Sort sort, Criteria... criterias) {
        List<T> alist = this.getPageList(clazz, pageInfo, sort, criterias);

        long count = this.getCount(clazz, criterias);

        PageHolder<T> page = new PageHolder<T>();
        page.setCurrentPageNum(pageInfo.getCurrentPageNum());
        page.setPageSize(pageInfo.getPageSize());
        page.setDataList(alist);
        page.setTotalCount(count);
        page.setPageCount(calculatePageCount(pageInfo.getPageSize(), count));

        return page;
    }

    @Override
    public <T> void update(T entity) {
        mongoTemplate.update(entity);
    }

    @Override
    public <T> T getEntityById(Class<T> clazz, String id) {
        Query query = new Query();
        query.addCriteria(Criteria.where("_id").is(id));
        return realtimeTemplate.findOne(query, clazz);
    }

    @Override
    public <T> T getEntityByKeyValue(Class<T> clazz, String key, Object value) {
        Query query = new Query();
        query.addCriteria(Criteria.where(key).is(value));
        return getEntityByQuery(clazz, query);
    }

    @Override
    public <T> T getEntityByQuery(Class<T> clazz, Query query) {
        return realtimeTemplate.findOne(query, clazz);
    }

    private long calculatePageCount(int pageSize, long totalCount) {
        if (totalCount <= pageSize) {
            return 1;
        }
        long t = totalCount / pageSize;
        if (totalCount % pageSize != 0) {
            // 不整除
            t++;
        }
        return t;
    }

    @Override
    public <T> void remove(T entity) {
        mongoTemplate.remove(entity);

    }

    @Override
    public <T> List<T> findAll(Class<T> c) {
        return mongoTemplate.findAll(c);
    }

    private Query toQuery(Sort sort, int limit, Criteria... criterias) {
        Query query = null;
        if (criterias != null) {
            query = new Query();
            for (Criteria cr : criterias) {
                query.addCriteria(cr);
            }
        }

        if (sort != null) {
            if (query == null) {
                query = new Query();
            }
            query.with(sort);
        }
        // query 有可能是null。
        if (limit > 0) {
            query.limit(limit);
        }
        return query;
    }

    @Override
    public <T> List<T> getEntitysByKeyValue(Class<T> clazz, String key, Object value) {
        Query query = new Query();
        query.addCriteria(Criteria.where(key).is(value));
        return mongoTemplate.find(query, clazz);
    }

    @Override
    public <T> List<T> getEntitysByKeyValues(Class<T> clazz, String key, List<Object> values) {
        Query query = new Query();
        query.addCriteria(Criteria.where(key).in(values));
        return mongoTemplate.find(query, clazz);
    }

    @Override
    public <T> PageHolder<T> getPage(Class<T> clazz, PageInfo pageInfo, Sort sort, List<Criteria> cs) {
        List<T> alist = this.getPageList(clazz, pageInfo, cs, sort);

        long count = this.getCount(clazz, cs);

        PageHolder<T> page = new PageHolder<T>();
        page.setCurrentPageNum(pageInfo.getCurrentPageNum());
        page.setPageSize(pageInfo.getPageSize());
        page.setDataList(alist);
        page.setTotalCount(count);
        page.setPageCount(calculatePageCount(pageInfo.getPageSize(), count));

        return page;
    }

    @Override
    public <T> List<T> getList(Class<T> clazz, Sort sort, int limit, Criteria... criterias) {
        Query query = this.toQuery(sort, limit, criterias);
        if (query == null) {
            return mongoTemplate.findAll(clazz);
        }
        return mongoTemplate.find(query, clazz);
    }

    @Override
    public <T> T getEntity(Class<T> clazz, Criteria... criterias) {
        Query query = this.toQuery(null, -1, criterias);
        return realtimeTemplate.findOne(query, clazz);
    }

    @Override
    public <T> int updateMulti(Query query, Update update, Class<T> clazz) {
        WriteResult wr = mongoTemplate.updateMulti(query, update, clazz);
        return wr.getN();
    }

    @Override
    public <T> int updateFirst(Query query, Update update, Class<T> clazz) {
        WriteResult wr = mongoTemplate.updateFirst(query, update, clazz);
        return wr.getN();
    }

    @Override
    public <T> List<T> find(Query query, Class<T> clazz) {
        return mongoTemplate.find(query, clazz);
    }

    @Override
    public List<String> find(Query query, String collectionName, String fieldName) {
        List<String> userList = new ArrayList<String>();
        if (mongoTemplate.collectionExists(collectionName)) {
            DBObject keys = new BasicDBObject(fieldName, 1);
            DBObject queryObj = query.getQueryObject();
            DBCursor cursor = mongoTemplate.getCollection(collectionName).find(queryObj, keys);
            if (query.getSortObject() != null) {
                cursor.sort(query.getSortObject());
            }
            while (cursor.hasNext()) {
                Object object = cursor.next().get(fieldName);
                if (object != null) {
                    userList.add(object.toString());
                }
            }
            cursor.close();
        }
        return userList;
    }

    @Override
    public List<String> findId(Query query, String collectionName) {
        List<String> userList = new ArrayList<String>();
        if (mongoTemplate.collectionExists(collectionName)) {
            DBObject keys = new BasicDBObject("_id", 1);
            DBCursor cursor = mongoTemplate.getCollection(collectionName).find(query.getQueryObject(), keys);
            while (cursor.hasNext()) {
                Object id = cursor.next().get("_id");
                if (id == null) {
                    continue;
                }
                userList.add(id.toString());
            }
            cursor.close();
        }
        return userList;
    }

    @Override
    public <T> GroupByResults<T> group(String inputCollectionName, GroupBy groupBy, Class<T> entityClass) {
        return mongoTemplate.group(inputCollectionName, groupBy, entityClass);
    }

    @Override
    public <T> GroupByResults<T> group(Criteria criteria, String inputCollectionName, GroupBy groupBy, Class<T> entityClass) {
        return mongoTemplate.group(criteria, inputCollectionName, groupBy, entityClass);
    }

    @Override
    public String getCollectionName(Class<?> entityClass) {
        return mongoTemplate.getCollectionName(entityClass);
    }

    @Override
    public void insertAll(Collection<? extends Object> objectsToSave) {
        mongoTemplate.insertAll(objectsToSave);

    }

    public <T> T getSubEntityById(String id, String collectionName, final String subDocumentName, final Class<T> clazz) {
        Query query = new Query();
        if (WenwoQueryMapper.QUESTIONS_COLLECTION.equals(collectionName)) {
            query.addCriteria(Criteria.where("_id").is(id));
        } else {
            query.addCriteria(Criteria.where("_id").is(new ObjectId(id)));
        }

        query.fields().include(subDocumentName);

        SubDocumentHandler handler = new SubDocumentHandler(subDocumentName);
        mongoTemplate.executeQuery(query, collectionName, handler);
        return mongoTemplate.getConverter().read(clazz, handler.getObject());
    }

    private class SubDocumentHandler implements DocumentCallbackHandler {
        private String subDocumentName;
        private DBObject object;

        public SubDocumentHandler(String subDocumentName) {
            super();
            this.subDocumentName = subDocumentName;
        }

        @Override
        public void processDocument(DBObject dbObject) throws MongoException, DataAccessException {
            object = (DBObject) dbObject.get(subDocumentName);
        }

        public DBObject getObject() {
            return object;
        }

    }

    public <T> void saveSubEntityById(String id, String collectionName,
			String subDocumentName, T entity) {
		mongoTemplate.saveSubEntityById(id, collectionName, subDocumentName,
				entity);
	}

    @Override
    public <T> List<T> getList(Class<T> clazz, Sort sort, Criteria... criterias) {
        Query query = this.toQuery(sort, -1, criterias);
        if (query == null) {
            return mongoTemplate.findAll(clazz);
        }
        return mongoTemplate.find(query, clazz);
    }

    @Override
    public <T> void findAndRemove(Query query, Class<T> clazz) {
        mongoTemplate.remove(query, clazz);
    }

    @Override
    public <T> MapReduceResults<T> mapReduce(Query query, String inputCollectionName, String mapFunction, String reduceFunction, Class<T> entityClass) {
        return mongoTemplate.mapReduce(query, inputCollectionName, mapFunction, reduceFunction, entityClass);
    }
}
