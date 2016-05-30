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
package org.springframework.data.mongodb.repository;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.Serializable;
import java.util.List;

import org.reactivestreams.Publisher;
import org.springframework.data.domain.Example;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.reactive.PagingAndSortingQueryPublisher;
import org.springframework.data.repository.NoRepositoryBean;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.data.repository.query.QueryByExampleExecutor;
import org.springframework.data.repository.reactive.ReactivePagingAndSortingRepository;

/**
 * Mongo specific {@link org.springframework.data.repository.Repository} interface with reactive support.
 *
 * @author Mark Paluch
 */
@NoRepositoryBean
public interface ReactiveMongoRepository<T, ID extends Serializable>
		extends ReactivePagingAndSortingRepository<T, ID> {

	<S extends T> Flux<S> save(Iterable<S> entites);

	PagingAndSortingQueryPublisher<T> findAll();

	PagingAndSortingQueryPublisher<T> findAll(Sort sort);

	/**
	 * Inserts the given a given entity. Assumes the instance to be new to be able to apply insertion optimizations. Use
	 * the returned instance for further operations as the save operation might have changed the entity instance
	 * completely. Prefer using {@link #save(Object)} instead to avoid the usage of store-specific API.
	 *
	 * @param entity must not be {@literal null}.
	 * @return the saved entity
	 */
	<S extends T> Mono<S> insert(S entity);

	/**
	 * Inserts the given a given entities. Assumes the instance to be new to be able to apply insertion optimizations. Use
	 * the returned instance for further operations as the save operation might have changed the entity instance
	 * completely. Prefer using {@link #save(Object)} instead to avoid the usage of store-specific API.
	 *
	 * @param entity must not be {@literal null}.
	 * @return the saved entity
	 */
	<S extends T> Flux<S> insert(Iterable<S> entity);

	/**
	 * Inserts the given a given entities. Assumes the instance to be new to be able to apply insertion optimizations. Use
	 * the returned instance for further operations as the save operation might have changed the entity instance
	 * completely. Prefer using {@link #save(Object)} instead to avoid the usage of store-specific API.
	 *
	 * @param entities must not be {@literal null}.
	 * @return the saved entity
	 */
	<S extends T> Flux<S> insert(Publisher<S> entities);

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.QueryByExampleExecutor#findAll(org.springframework.data.domain.Example)
	 */
	<S extends T> Flux<S> findAll(Example<S> example);

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.QueryByExampleExecutor#findAll(org.springframework.data.domain.Example, org.springframework.data.domain.Sort)
	 */
	<S extends T> Flux<S> findAll(Example<S> example, Sort sort);

}
