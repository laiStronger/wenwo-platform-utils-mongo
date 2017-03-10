/*
 * Copyright 2011-2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.wenwo.platform.dao.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.bson.types.ObjectId;
import org.springframework.core.convert.ConversionException;
import org.springframework.core.convert.ConversionService;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.mapping.PropertyReferenceException;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.context.PersistentPropertyPath;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.mapping.BasicMongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.util.Assert;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBRef;


/**
 * @author stanleyding
 *
 * questions的id从ObjectId变成了String以后，spring提供的QueryMapping类对于老的questionId无法进行正常的查询，因为对于24位的合法的id，它强行将其转换为ObjectId了。
 * 这里就是很土鳖地对questions这个collection的ID不进行处理，其它collection不变。
 * 
 * 方法很土鳖，但是暂时没有想到更好的办法。
 */
public class WenwoQueryMapper extends QueryMapper{
	public static final Object QUESTIONS_COLLECTION = "questions";
	private static final List<String> DEFAULT_ID_NAMES = Arrays.asList("id", "_id");
	private static final String N_OR_PATTERN = "\\$.*or";

	private final ConversionService conversionService;
	private final MongoConverter converter;
	private final MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext;

	/**
	 * Creates a new {@link WenwiQueryMapper} with the given {@link MongoConverter}.
	 * 
	 * @param converter must not be {@literal null}.
	 */
	public WenwoQueryMapper(MongoConverter converter) {
		super(converter);
		Assert.notNull(converter);

		this.conversionService = converter.getConversionService();
		this.converter = converter;
		this.mappingContext = converter.getMappingContext();
	}

	/**
	 * Replaces the property keys used in the given {@link DBObject} with the appropriate keys by using the
	 * {@link PersistentEntity} metadata.
	 * 
	 * @param query must not be {@literal null}.
	 * @param entity can be {@literal null}.
	 * @return
	 */
	public DBObject getMappedObject(DBObject query, MongoPersistentEntity<?> entity) {

		if (Keyword.isKeyword(query)) {
			return getMappedKeyword(new Keyword(query), entity);
		}

		DBObject result = new BasicDBObject();

		for (String key : query.keySet()) {

			MongoPersistentProperty targetProperty = getTargetProperty(key, entity);
			String newKey = determineKey(key, entity);
			Object value = query.get(key);

			result.put(newKey, getMappedValue(value, targetProperty, newKey));
		}

		return result;
	}

	/**
	 * Returns the given {@link DBObject} representing a keyword by mapping the keyword's value.
	 * 
	 * @param query the {@link DBObject} representing a keyword (e.g. {@code $ne : … } )
	 * @param entity
	 * @return
	 */
	private DBObject getMappedKeyword(Keyword query, MongoPersistentEntity<?> entity) {

		// $or/$nor
		if (query.key.matches(N_OR_PATTERN)) {

			Iterable<?> conditions = (Iterable<?>) query.value;
			BasicDBList newConditions = new BasicDBList();

			for (Object condition : conditions) {
				newConditions.add(getMappedObject((DBObject) condition, entity));
			}

			return new BasicDBObject(query.key, newConditions);
		}

		return new BasicDBObject(query.key, convertSimpleOrDBObject(query.value, entity));
	}

	/**
	 * Returns the mapped keyword considered defining a criteria for the given property.
	 * 
	 * @param keyword
	 * @param property
	 * @return
	 */
	public DBObject getMappedKeyword(Keyword keyword, MongoPersistentProperty property) {

		if (property.isAssociation()) {
			convertAssociation(keyword.value, property);
		}

		return new BasicDBObject(keyword.key, getMappedValue(keyword.value, property, keyword.key));
	}

	/**
	 * Returns the mapped value for the given source object assuming it's a value for the given
	 * {@link MongoPersistentProperty}.
	 * 
	 * @param source the source object to be mapped
	 * @param property the property the value is a value for
	 * @param newKey the key the value will be bound to eventually
	 * @return
	 */
	private Object getMappedValue(Object source, MongoPersistentProperty property, String newKey) {

		if (property == null) {
			return convertSimpleOrDBObject(source, null);
		}

		if (property.isIdProperty() || "_id".equals(newKey)) {
			PersistentEntity<?, MongoPersistentProperty> entity = property.getOwner();
			
			if (source instanceof DBObject) {
				DBObject valueDbo = (DBObject) source;
				if (valueDbo.containsField("$in") || valueDbo.containsField("$nin")) {
					String inKey = valueDbo.containsField("$in") ? "$in" : "$nin";
					List<Object> ids = new ArrayList<Object>();
					if(entity instanceof BasicMongoPersistentEntity) {
						String collectionName = ((BasicMongoPersistentEntity<?>)entity).getCollection();
						if(collectionName.equals(QUESTIONS_COLLECTION)) {
							for (Object id : (Iterable<?>) valueDbo.get(inKey)) {
								ids.add(id);
							}
						} else {
							for (Object id : (Iterable<?>) valueDbo.get(inKey)) {
								ids.add(convertId(id));
							}
						}
					} else {
						for (Object id : (Iterable<?>) valueDbo.get(inKey)) {
							ids.add(convertId(id));
						}
					}
					
					valueDbo.put(inKey, ids.toArray(new Object[ids.size()]));
				} else if (valueDbo.containsField("$ne")) {
					valueDbo.put("$ne", convertId(valueDbo.get("$ne")));
				} else {
					return getMappedObject((DBObject) source, null);
				}

				return valueDbo;

			} else {
				if(entity instanceof BasicMongoPersistentEntity) {
					String collectionName = ((BasicMongoPersistentEntity<?>)entity).getCollection();
					if(collectionName.equals(QUESTIONS_COLLECTION)) {
						return source;
					}
				}
				
				return convertId(source);
			}
		}

		if (property.isAssociation()) {
			return Keyword.isKeyword(source) ? getMappedKeyword(new Keyword(source), property) : convertAssociation(source,
					property);
		}

		return convertSimpleOrDBObject(source, mappingContext.getPersistentEntity(property));
	}

	private MongoPersistentProperty getTargetProperty(String key, MongoPersistentEntity<?> entity) {

		if (isIdKey(key, entity)) {
			return entity.getIdProperty();
		}

		PersistentPropertyPath<MongoPersistentProperty> path = getPath(key, entity);
		return path == null ? null : path.getLeafProperty();
	}

	private PersistentPropertyPath<MongoPersistentProperty> getPath(String key, MongoPersistentEntity<?> entity) {

		if (entity == null) {
			return null;
		}

		try {
			PropertyPath path = PropertyPath.from(key, entity.getTypeInformation());
			return mappingContext.getPersistentPropertyPath(path);
		} catch (PropertyReferenceException e) {
			return null;
		}
	}

	/**
	 * Returns the translated key assuming the given one is a propert (path) reference.
	 * 
	 * @param key the source key
	 * @param entity the base entity
	 * @return the translated key
	 */
	private String determineKey(String key, MongoPersistentEntity<?> entity) {

		if (entity == null) {
			return key;
		}

		if (!entity.hasIdProperty() && DEFAULT_ID_NAMES.contains(key)) {
			return "_id";
		}

		PersistentPropertyPath<MongoPersistentProperty> path = getPath(key, entity);
		return path == null ? key : path.toDotPath(MongoPersistentProperty.PropertyToFieldNameConverter.INSTANCE);
	}

	/**
	 * Retriggers mapping if the given source is a {@link DBObject} or simply invokes the
	 * 
	 * @param source
	 * @param entity
	 * @return
	 */
	private Object convertSimpleOrDBObject(Object source, MongoPersistentEntity<?> entity) {

		if (source instanceof BasicDBList) {
			return converter.convertToMongoType(source);
		}

		if (source instanceof DBObject) {
			return getMappedObject((DBObject) source, entity);
		}

		return converter.convertToMongoType(source);
	}

	/**
	 * Converts the given source assuming it's actually an association to anoter object.
	 * 
	 * @param source
	 * @param property
	 * @return
	 */
	private Object convertAssociation(Object source, MongoPersistentProperty property) {

		if (property == null || !property.isAssociation()) {
			return source;
		}

		if (source instanceof Iterable) {
			BasicDBList result = new BasicDBList();
			for (Object element : (Iterable<?>) source) {
				result.add(element instanceof DBRef ? element : converter.toDBRef(element, property));
			}
			return result;
		}

		return source == null || source instanceof DBRef ? source : converter.toDBRef(source, property);
	}

	/**
	 * Returns whether the given key will be considered an id key.
	 * 
	 * @param key
	 * @param entity
	 * @return
	 */
	private boolean isIdKey(String key, MongoPersistentEntity<?> entity) {

		if (entity == null) {
			return false;
		}

		MongoPersistentProperty idProperty = entity.getIdProperty();

		if (idProperty != null) {
			return idProperty.getName().equals(key) || idProperty.getFieldName().equals(key);
		}

		return DEFAULT_ID_NAMES.contains(key);
	}

	/**
	 * Converts the given raw id value into either {@link ObjectId} or {@link String}.
	 * 
	 * @param id
	 * @return
	 */
	public Object convertId(Object id) {

		try {
			return conversionService.convert(id, ObjectId.class);
		} catch (ConversionException e) {
			// Ignore
		}

		return converter.convertToMongoType(id);
	}

	/**
	 * Value object to capture a query keyword representation.
	 * 
	 * @author Oliver Gierke
	 */
	private static class Keyword {

		String key;
		Object value;

		Keyword(Object source) {

			Assert.isInstanceOf(DBObject.class, source);

			DBObject value = (DBObject) source;

			Assert.isTrue(value.keySet().size() == 1, "Keyword must have a single key only!");

			this.key = value.keySet().iterator().next();
			this.value = value.get(key);
		}

		/**
		 * Returns whether the given value actually represents a keyword. If this returns {@literal true} it's safe to call
		 * the constructor.
		 * 
		 * @param value
		 * @return
		 */
		static boolean isKeyword(Object value) {

			if (!(value instanceof DBObject)) {
				return false;
			}

			DBObject dbObject = (DBObject) value;
			return dbObject.keySet().size() == 1 && dbObject.keySet().iterator().next().startsWith("$");
		}
	}
}
