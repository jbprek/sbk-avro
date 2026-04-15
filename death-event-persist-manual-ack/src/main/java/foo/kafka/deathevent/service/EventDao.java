package foo.kafka.deathevent.service;

import foo.kafka.common.MessageCoordinates;

/**
 * Generic DAO that persists an entity of type {@code T}.
 *
 * @param <T> the entity type
 */
@FunctionalInterface
public interface EventDao<T> {

    void save(T entity, MessageCoordinates coordinates);
}
