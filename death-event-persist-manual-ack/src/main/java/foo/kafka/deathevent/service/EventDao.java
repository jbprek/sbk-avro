package foo.kafka.deathevent.service;

import foo.kafka.common.MessageCoordinates;

/**
 * Generic DAO that persists an event of type {@code E}.
 *
 * @param <E> the event type
 */
@FunctionalInterface
public interface EventDao<E> {

    void save(E event, MessageCoordinates coordinates);
}
