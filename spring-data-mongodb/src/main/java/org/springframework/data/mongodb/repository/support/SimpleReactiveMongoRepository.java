/*
 * Copyright 2016 the original author or authors.
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
package org.springframework.data.mongodb.repository.support;

import static org.springframework.data.mongodb.core.query.Criteria.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.reactivestreams.Publisher;
import org.springframework.data.domain.Example;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.reactive.PagingAndSortingQueryPublisher;
import org.springframework.data.domain.reactive.ReactivePage;
import org.springframework.data.domain.reactive.ReactivePageImpl;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.data.mongodb.repository.query.MongoEntityInformation;
import org.springframework.util.Assert;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Reactive repository base implementation for Mongo.
 *
 * @author Mark Paluch
 */
public class SimpleReactiveMongoRepository<T, ID extends Serializable> implements ReactiveMongoRepository<T, ID> {

	private final ReactiveMongoOperations mongoOperations;
	private final MongoEntityInformation<T, ID> entityInformation;

	/**
	 * Creates a new {@link SimpleReactiveMongoRepository} for the given {@link MongoEntityInformation} and
	 * {@link MongoTemplate}.
	 *
	 * @param metadata must not be {@literal null}.
	 * @param mongoOperations must not be {@literal null}.
	 */
	public SimpleReactiveMongoRepository(MongoEntityInformation<T, ID> metadata,
			ReactiveMongoOperations mongoOperations) {

		Assert.notNull(mongoOperations);
		Assert.notNull(metadata);

		this.entityInformation = metadata;
		this.mongoOperations = mongoOperations;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#save(java.lang.Object)
	 */
	public <S extends T> Mono<S> save(S entity) {

		Assert.notNull(entity, "Entity must not be null!");

		if (entityInformation.isNew(entity)) {
			return mongoOperations.insert(entity, entityInformation.getCollectionName()).then(aVoid -> Mono.just(entity));
		} else {
			return mongoOperations.save(entity, entityInformation.getCollectionName()).then(aVoid -> Mono.just(entity));
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#save(java.lang.Iterable)
	 */
	public <S extends T> Flux<S> save(Iterable<S> entities) {

		Assert.notNull(entities, "The given Iterable of entities not be null!");

		List<S> result = convertIterableToList(entities);
		boolean allNew = true;

		for (S entity : entities) {
			if (allNew && !entityInformation.isNew(entity)) {
				allNew = false;
			}
		}

		if (allNew) {
			return Flux.from(mongoOperations.insertAll(result)).switchMap(aVoid -> Flux.fromIterable(entities));
		}

		List<Mono<S>> monos = new ArrayList<>();
		for (S entity : result) {
			monos.add(save(entity));
		}

		return Flux.merge(monos);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#findOne(java.io.Serializable)
	 */
	public Mono<T> findOne(ID id) {
		Assert.notNull(id, "The given id must not be null!");
		return mongoOperations.findById(id, entityInformation.getJavaType(), entityInformation.getCollectionName());
	}

	private Query getIdQuery(Object id) {
		return new Query(getIdCriteria(id));
	}

	private Criteria getIdCriteria(Object id) {
		return where(entityInformation.getIdAttribute()).is(id);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#exists(java.io.Serializable)
	 */
	public Mono<Boolean> exists(ID id) {

		Assert.notNull(id, "The given id must not be null!");
		return mongoOperations.exists(getIdQuery(id), entityInformation.getJavaType(),
				entityInformation.getCollectionName());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#count()
	 */
	public Mono<Long> count() {
		return mongoOperations.count(new Query(), entityInformation.getCollectionName());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#delete(java.io.Serializable)
	 */
	public Mono<Void> delete(ID id) {
		Assert.notNull(id, "The given id must not be null!");
		return mongoOperations
				.remove(getIdQuery(id), entityInformation.getJavaType(), entityInformation.getCollectionName())
				.then(deleteResult -> Mono.empty());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#delete(java.lang.Object)
	 */
	public Mono<Void> delete(T entity) {
		Assert.notNull(entity, "The given entity must not be null!");
		return delete(entityInformation.getId(entity));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#delete(java.lang.Iterable)
	 */
	public Mono<Void> delete(Iterable<? extends T> entities) {

		Assert.notNull(entities, "The given Iterable of entities not be null!");

		List<Mono<Void>> monos = new ArrayList<>();
		for (T entity : entities) {
			monos.add(delete(entity));
		}

		return Flux.merge(monos).after();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#deleteAll()
	 */
	public Mono<Void> deleteAll() {
		return mongoOperations.remove(new Query(), entityInformation.getCollectionName())
				.then(deleteResult -> Mono.empty());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#findAll()
	 */
	// public Flux<T> findAll() {
	// return findAll(new Query());
	// }

	@Override
	public PagingAndSortingQueryPublisher<T> findAll() {
		return null;
	}

	/*
		 * (non-Javadoc)
		 * @see org.springframework.data.repository.CrudRepository#findAll(java.lang.Iterable)
		 */
	/*public Flux<T> findAll(Iterable<ID> ids) {

		Set<ID> parameters = new HashSet<ID>(tryDetermineRealSizeOrReturn(ids, 10));
		for (ID id : ids) {
			parameters.add(id);
		}

		return findAll(new Query(new Criteria(entityInformation.getIdAttribute()).in(parameters)));
	}*/

	@Override
	public PagingAndSortingQueryPublisher<T> findAll(Iterable<ID> ids) {
		return null;
	}

	@Override
	public PagingAndSortingQueryPublisher<T> findAll(Publisher<ID> ids) {
		return null;
	}

	@Override
	public <S extends T> Flux<S> save(Publisher<S> entities) {
		return null;
	}

	@Override
	public Mono<Void> delete(Publisher<? extends T> entities) {
		return null;
	}

	@Override
	public <S extends T> Flux<S> insert(Publisher<S> entities) {
		mongoOperations.insert(entities, entityInformation.getCollectionName());
		// TODO: Return entities.
		return null;
	}

	/*
		 * (non-Javadoc)
		 * @see org.springframework.data.repository.PagingAndSortingRepository#findAll(org.springframework.data.domain.Pageable)
		 */
	public ReactivePage<T> findAll(final Pageable pageable) {

		Mono<Long> count = count();
		Flux<T> list = findAll(new Query().with(pageable));

		return new ReactivePageImpl<>(list, pageable, count);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.PagingAndSortingRepository#findAll(org.springframework.data.domain.Sort)
	 */
	/*public List<T> findAll(Sort sort) {
		return findAll(new Query().with(sort));
	}*/

	@Override
	public PagingAndSortingQueryPublisher<T> findAll(Sort sort) {
		return null;
	}

	/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.repository.MongoRepository#insert(java.lang.Object)
		 */
	@Override
	public <S extends T> Mono<S> insert(S entity) {

		Assert.notNull(entity, "Entity must not be null!");

		return mongoOperations.insert(entity, entityInformation.getCollectionName()).then(aVoid -> Mono.just(entity));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.repository.MongoRepository#insert(java.lang.Iterable)
	 */
	@Override
	public <S extends T> Flux<S> insert(Iterable<S> entities) {

		Assert.notNull(entities, "The given Iterable of entities not be null!");

		List<S> list = convertIterableToList(entities);

		if (list.isEmpty()) {
			return Flux.empty();
		}

		return Flux.from(mongoOperations.insertAll(list)).switchMap(aVoid -> Flux.fromIterable(entities));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.repository.MongoRepository#findAllByExample(org.springframework.data.domain.Example, org.springframework.data.domain.Pageable)
	 */
	// @Override
	public <S extends T> ReactivePage<S> findAll(Example<S> example, Pageable pageable) {

		Assert.notNull(example, "Sample must not be null!");

		Query q = new Query(new Criteria().alike(example)).with(pageable);

		Mono<Long> count = mongoOperations.count(q, example.getProbeType(), entityInformation.getCollectionName());

		return new ReactivePageImpl<>(
				mongoOperations.find(q, example.getProbeType(), entityInformation.getCollectionName()), pageable, count);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.repository.MongoRepository#findAllByExample(org.springframework.data.domain.Example, org.springframework.data.domain.Sort)
	 */
	@Override
	public <S extends T> Flux<S> findAll(Example<S> example, Sort sort) {

		Assert.notNull(example, "Sample must not be null!");

		Query q = new Query(new Criteria().alike(example));

		if (sort != null) {
			q.with(sort);
		}

		return mongoOperations.find(q, example.getProbeType(), entityInformation.getCollectionName());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.repository.MongoRepository#findAllByExample(org.springframework.data.domain.Example)
	 */
	@Override
	public <S extends T> Flux<S> findAll(Example<S> example) {
		return findAll(example, (Sort) null);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.QueryByExampleExecutor#findOne(org.springframework.data.domain.Example)
	 */
	// @Override
	public <S extends T> Mono<S> findOne(Example<S> example) {

		Assert.notNull(example, "Sample must not be null!");

		Query q = new Query(new Criteria().alike(example));
		return mongoOperations.findOne(q, example.getProbeType(), entityInformation.getCollectionName());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.QueryByExampleExecutor#count(org.springframework.data.domain.Example)
	 */
	// @Override
	public <S extends T> Mono<Long> count(Example<S> example) {

		Assert.notNull(example, "Sample must not be null!");

		Query q = new Query(new Criteria().alike(example));
		return mongoOperations.count(q, example.getProbeType(), entityInformation.getCollectionName());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.QueryByExampleExecutor#exists(org.springframework.data.domain.Example)
	 */
	// @Override
	public <S extends T> Mono<Boolean> exists(Example<S> example) {

		Assert.notNull(example, "Sample must not be null!");

		Query q = new Query(new Criteria().alike(example));
		return mongoOperations.exists(q, example.getProbeType(), entityInformation.getCollectionName());
	}

	private Flux<T> findAll(Query query) {

		if (query == null) {
			return Flux.empty();
		}

		return mongoOperations.find(query, entityInformation.getJavaType(), entityInformation.getCollectionName());
	}

	private static <T> List<T> convertIterableToList(Iterable<T> entities) {

		if (entities instanceof List) {
			return (List<T>) entities;
		}

		int capacity = tryDetermineRealSizeOrReturn(entities, 10);

		if (capacity == 0 || entities == null) {
			return Collections.<T> emptyList();
		}

		List<T> list = new ArrayList<T>(capacity);
		for (T entity : entities) {
			list.add(entity);
		}

		return list;
	}

	private static int tryDetermineRealSizeOrReturn(Iterable<?> iterable, int defaultSize) {
		return iterable == null ? 0 : (iterable instanceof Collection) ? ((Collection<?>) iterable).size() : defaultSize;
	}

}
